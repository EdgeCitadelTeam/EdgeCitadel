from __future__ import annotations

import asyncio
import json
import os
import queue
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path

import nats

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]

_ROOT = Path(__file__).resolve().parents[2]
_TOOLCHAIN = _ROOT / "scripts" / "research" / "toolchain.json"
_OWNER_LABEL = "ai.edgecitadel.owner=test-nats"
_READY_TIMEOUT_SECONDS = 10.0
_ASYNC_READY_BUDGET_SECONDS = 9.5


def _nats_image() -> str:
    value = json.loads(_TOOLCHAIN.read_text(encoding="utf-8"))["nats_image"]
    if not isinstance(value, str) or not value.startswith("nats@sha256:"):
        raise ValueError("toolchain nats_image must be a digest-pinned NATS image")
    return value


@dataclass
class NatsServer:
    token: str
    jetstream: bool
    runner: CommandRunner = field(default=subprocess.run, repr=False)
    _container_id: str | None = field(default=None, init=False, repr=False)
    _container_name: str | None = field(default=None, init=False, repr=False)
    _volume_name: str | None = field(default=None, init=False, repr=False)
    _temp_dir: Path | None = field(default=None, init=False, repr=False)
    _config_path: Path | None = field(default=None, init=False, repr=False)
    _port: int | None = field(default=None, init=False, repr=False)

    def start(self) -> NatsServer:
        if self._temp_dir is not None or self._container_id is not None:
            raise RuntimeError("NATS server is already started")

        self._container_name = f"edgecitadel-test-nats-{uuid.uuid4().hex}"
        try:
            self._create_config()
            if self.jetstream:
                self._create_volume()
            self._launch_container()
            self._wait_until_ready()
        except BaseException:
            self.close()
            raise
        return self

    @property
    def url(self) -> str:
        if self._port is None:
            raise RuntimeError("NATS server is not started")
        return f"nats://127.0.0.1:{self._port}"

    def restart(self, *, preserve_storage: bool) -> None:
        if (
            self._container_id is None
            or self._container_name is None
            or self._config_path is None
        ):
            raise RuntimeError("NATS server is not started")

        self._remove_container()
        if self.jetstream and not preserve_storage:
            self._remove_volume()
            self._create_volume()
        try:
            self._launch_container()
            self._wait_until_ready()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        try:
            self._remove_container()
        finally:
            try:
                self._remove_volume()
            finally:
                if self._temp_dir is not None:
                    shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None
                self._config_path = None
                self._container_name = None
                self._port = None

    def _docker(
        self,
        *arguments: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return self.runner(
            ["docker", *arguments],
            check=check,
            capture_output=True,
            text=True,
        )

    def _create_config(self) -> None:
        directory = Path(tempfile.mkdtemp(prefix="edgecitadel-test-nats-"))
        self._temp_dir = directory
        directory.chmod(0o700)
        config_path = directory / "nats.conf"
        self._config_path = config_path
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        descriptor = os.open(config_path, flags, 0o600)
        jetstream = '\njetstream {\n  store_dir: "/data"\n}\n' if self.jetstream else ""
        config = (
            "port: 4222\n"
            "authorization {\n"
            f"  token: {json.dumps(self.token)}\n"
            "}\n"
            f"{jetstream}"
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as config_file:
            config_file.write(config)
        config_path.chmod(0o600)

    def _create_volume(self) -> None:
        if self._container_name is None:
            raise RuntimeError("container name is not initialized")
        volume_name = f"{self._container_name}-data-{uuid.uuid4().hex}"
        self._volume_name = volume_name
        self._docker(
            "volume",
            "create",
            "--label",
            _OWNER_LABEL,
            volume_name,
        )

    def _launch_container(self) -> None:
        if self._container_name is None or self._config_path is None:
            raise RuntimeError("NATS server configuration is not initialized")

        arguments = [
            "run",
            "--detach",
            "--name",
            self._container_name,
            "--label",
            _OWNER_LABEL,
            "--publish",
            "127.0.0.1::4222",
            "--mount",
            (
                f"type=bind,source={self._config_path},"
                "target=/etc/nats/nats.conf,readonly"
            ),
        ]
        if self.jetstream:
            if self._volume_name is None:
                raise RuntimeError("JetStream volume is not initialized")
            arguments.extend(
                [
                    "--mount",
                    f"type=volume,source={self._volume_name},target=/data",
                ]
            )
        arguments.extend([_nats_image(), "-c", "/etc/nats/nats.conf"])

        result = self._docker(*arguments)
        container_id = result.stdout.strip()
        if not container_id:
            raise RuntimeError("docker run did not return a container ID")
        self._container_id = container_id

        port_result = self._docker("port", container_id, "4222/tcp")
        for line in port_result.stdout.splitlines():
            host, separator, port_text = line.rpartition(":")
            if separator and host == "127.0.0.1" and port_text.isdigit():
                self._port = int(port_text)
                return
        raise RuntimeError(
            f"docker did not publish a loopback NATS port: {port_result.stdout!r}"
        )

    async def _wait_until_ready_async(self) -> None:
        deadline = time.monotonic() + _ASYNC_READY_BUDGET_SECONDS
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            client = None
            remaining = deadline - time.monotonic()
            try:
                client = await asyncio.wait_for(
                    nats.connect(
                        servers=[self.url],
                        token=self.token,
                        connect_timeout=min(1.0, remaining),
                        allow_reconnect=False,
                        max_reconnect_attempts=0,
                    ),
                    timeout=min(1.0, remaining),
                )
                remaining = deadline - time.monotonic()
                await asyncio.wait_for(client.flush(), timeout=max(0.01, remaining))
                return
            except Exception as error:  # noqa: BLE001 - retry nats-py transport errors
                last_error = error
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    await asyncio.sleep(min(0.1, remaining))
            finally:
                if client is not None:
                    with suppress(Exception):
                        remaining = deadline - time.monotonic()
                        await asyncio.wait_for(
                            client.close(),
                            timeout=max(0.01, remaining),
                        )

        raise RuntimeError(
            "NATS server was not ready within ten seconds"
        ) from last_error

    def _wait_until_ready(self) -> None:
        outcome: queue.Queue[BaseException | None] = queue.Queue(maxsize=1)

        def wait_in_thread() -> None:
            try:
                asyncio.run(self._wait_until_ready_async())
            except Exception as error:  # noqa: BLE001 - relay worker failure to caller
                outcome.put(error)
            else:
                outcome.put(None)

        thread = threading.Thread(target=wait_in_thread, daemon=True)
        thread.start()
        thread.join(timeout=_READY_TIMEOUT_SECONDS)
        if thread.is_alive():
            raise RuntimeError("NATS server was not ready within ten seconds")

        error = outcome.get_nowait()
        if error is not None:
            raise RuntimeError(
                "NATS server was not ready within ten seconds"
            ) from error

    def _remove_container(self) -> None:
        if self._container_id is None:
            return
        container_id = self._container_id
        try:
            self._docker("rm", "--force", container_id, check=False)
        finally:
            self._container_id = None
            self._port = None

    def _remove_volume(self) -> None:
        if self._volume_name is None:
            return
        volume_name = self._volume_name
        try:
            self._docker("volume", "rm", "--force", volume_name, check=False)
        finally:
            self._volume_name = None
