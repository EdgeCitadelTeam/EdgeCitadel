"""Per-host lab helper: a run's processes and files on this host, over NATS.

The runner on the controller drives a host listed with ``helper`` in the stack
file through this process instead of SSH. It answers request/reply messages on
its subject of the lab's token-protected experiment broker with a fixed table
of operations, all confined to the host's experiment root (the stack file's
``work_root``) and to the harness's own processes:

- ``start``: a harness process (``<python> -m harness.nodectl ...`` or
  ``<python> -m det_worker ...``) as a transient systemd unit named
  ``firebreak-exp-h-<run>-<process>`` (``--units user`` or ``system``), or as a
  plain process group (``--units none``, for a dry run); its environment is a
  mode-600 file in the run directory, never the command line;
- ``alive`` / ``stop``: those units (or processes) only;
- ``run``: one short ``harness.nodectl`` command, to completion;
- ``write`` / ``read`` (in chunks) / ``exists`` / ``glob``: files under the root
  (``exists`` also sees agentd's own socket directory, where a long state path
  puts the admin socket);
- ``remove``: one run directory directly under the root.

Anything else, a path outside the root or another program is refused. Start it
once per host, for example as a ``firebreak-exp-labhelper`` unit::

    python -m harness.labhelper --host netsys-endurance \\
        --root /home/zhonghao/firebreak-exp/m5/harness-work \\
        --python /home/zhonghao/firebreak-exp/m5/venv/bin/python \\
        --repo /home/zhonghao/firebreak-exp/m5/src \\
        --nats-url nats://<jim-eq tailnet address>:14222 \\
        --token-file /home/zhonghao/firebreak-exp/m5/secrets/nats.token \\
        --units user

and give the host ``"helper": {"nats_url": ..., "token_file": <the token's file
on the controller>, "subject": "harness.lab.<host>"}`` in the stack file.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import glob
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

UNIT_PREFIX = "firebreak-exp-h-"
START_MODULES = ("harness.nodectl", "det_worker")
RUN_COMMANDS = ("configure", "install", "operator", "socket")
READ_CHUNK = 256 * 1024
RUN_TIMEOUT_S = 120.0


def subject_for(host: str) -> str:
    return f"harness.lab.{host}"


class HelperError(ValueError):
    pass


class Helper:
    def __init__(
        self,
        root: Path,
        python: str,
        *,
        repo: Path | None = None,
        units: str = "user",
        modules: Sequence[str] = START_MODULES,
    ) -> None:
        if units not in {"user", "system", "none"}:
            raise HelperError("units: user, system or none")
        self.root = Path(os.path.realpath(root))
        self.python = python
        self.repo = Path(os.path.realpath(repo)) if repo else None
        self.units = units
        self.modules = tuple(modules)
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        # agentd moves a too-long admin socket path here (service.socket_path_for).
        self.socket_dir = (
            Path(tempfile.gettempdir()) / f"edgecitadel-agentd-{os.getuid()}"
        )

    # -- scope ------------------------------------------------------------------
    def _inside(self, path: str, *, extra: Sequence[Path] = ()) -> Path:
        resolved = Path(os.path.realpath(path))
        for base in (self.root, *extra):
            if resolved == base or base in resolved.parents:
                return resolved
        raise HelperError(f"outside the experiment root: {path}")

    def _run_dir(self, path: Path) -> Path:
        """The run directory (a direct child of the root) a path belongs to."""
        relative = path.relative_to(self.root).parts
        if not relative:
            raise HelperError("the experiment root itself is not a run directory")
        return self.root / relative[0]

    def _unit(self, run_dir: Path, name: str) -> str:
        unit = f"{UNIT_PREFIX}{run_dir.name}-{name}"
        if not re.fullmatch(r"[A-Za-z0-9_.\-]+", unit) or len(unit) > 200:
            raise HelperError(f"invalid unit name {unit!r}")
        return unit

    def _check_unit(self, unit: str) -> str:
        if not unit.startswith(UNIT_PREFIX) or not re.fullmatch(
            r"[A-Za-z0-9_.\-]+", unit
        ):
            raise HelperError(f"not a harness unit: {unit!r}")
        return unit

    def _program(self, argv: Sequence[str], modules: Sequence[str]) -> list[str]:
        argv = [str(a) for a in argv]
        if len(argv) < 3 or argv[0] != self.python or argv[1] != "-m":
            raise HelperError("only the harness's python -m <module> is run")
        if argv[2] not in modules:
            raise HelperError(f"module {argv[2]!r} is not a harness process")
        return argv

    def _cwd(self, cwd: str) -> Path:
        allowed = [self.repo / "agent-packages" / "det-worker"] if self.repo else []
        resolved = Path(os.path.realpath(cwd))
        if resolved in allowed:
            return resolved
        return self._inside(cwd)

    def _env_file(self, directory: Path, name: str, env: Mapping[str, str]) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f".env.{name}"
        body = "".join(
            f"{key}={shlex.quote(str(value))}\n" for key, value in env.items()
        )
        self._write_private(path, body.encode(), 0o600)
        return path

    @staticmethod
    def _write_private(path: Path, data: bytes, mode: int) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
        os.chmod(path, mode)

    # -- operations -------------------------------------------------------------
    def start(
        self,
        name: str,
        argv: Sequence[str],
        env: Mapping[str, str],
        cwd: str,
        log: str,
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_.\-]+", name):
            raise HelperError(f"invalid process name {name!r}")
        program = self._program(argv, self.modules)
        log_path = self._inside(log)
        unit = self._unit(self._run_dir(log_path), name)
        directory = self._cwd(cwd)
        env_file = self._env_file(log_path.parent, name, env)
        # The process appends its output to its log; the environment (secrets
        # included) comes from the private file.
        shell = (
            'set -a; . "$1"; set +a; shift; log="$1"; shift; exec "$@" >>"$log" 2>&1'
        )
        command = ["/bin/sh", "-c", shell, "sh", str(env_file), str(log_path), *program]
        if self.units == "none":
            popen = subprocess.Popen(
                command,
                cwd=directory,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            self.processes[unit] = popen
            return {"unit": unit, "pid": popen.pid}
        scope = ["--user"] if self.units == "user" else []
        subprocess.run(
            [
                "systemd-run",
                *scope,
                f"--unit={unit}",
                "--collect",
                f"--working-directory={directory}",
                "--property=KillMode=control-group",
                *command,
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        return {"unit": unit, "pid": self._main_pid(unit)}

    def _systemctl(
        self, *args: str, check: bool = False
    ) -> subprocess.CompletedProcess[str]:
        scope = ["--user"] if self.units == "user" else []
        return subprocess.run(
            ["systemctl", *scope, *args],
            check=check,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def _main_pid(self, unit: str) -> int:
        out = self._systemctl("show", "--property=MainPID", "--value", unit).stdout
        return int(out.strip() or 0)

    def alive(self, unit: str) -> bool:
        unit = self._check_unit(unit)
        if self.units == "none":
            popen = self.processes.get(unit)
            return popen is not None and popen.poll() is None
        return self._systemctl("is-active", "--quiet", unit).returncode == 0

    def stop(self, unit: str, timeout: float = 10) -> bool:
        unit = self._check_unit(unit)
        if self.units == "none":
            popen = self.processes.pop(unit, None)
            if popen is not None and popen.poll() is None:
                os.killpg(popen.pid, signal.SIGTERM)
                try:
                    popen.wait(timeout)
                except subprocess.TimeoutExpired:
                    os.killpg(popen.pid, signal.SIGKILL)
                    popen.wait(5)
            return True
        self._systemctl("stop", unit)
        return True

    def run(
        self,
        argv: Sequence[str],
        env: Mapping[str, str],
        cwd: str | None = None,
        timeout: float = 60,
    ) -> str:
        program = self._program(argv, ("harness.nodectl",))
        if len(program) < 4 or program[3] not in RUN_COMMANDS:
            raise HelperError(f"nodectl {program[3:4]} is not a short command")
        directory = self._cwd(cwd) if cwd else self.root
        result = subprocess.run(
            program,
            cwd=directory,
            env={**os.environ, **{k: str(v) for k, v in env.items()}},
            capture_output=True,
            text=True,
            timeout=min(float(timeout), RUN_TIMEOUT_S),
        )
        if result.returncode != 0:
            raise HelperError(f"{program[2:4]} failed: {result.stderr.strip()[-2000:]}")
        return result.stdout

    def write(self, path: str, data: str, mode: int = 0o600) -> bool:
        if mode & ~0o644:
            raise HelperError("files are written 0600 or 0644")
        self._write_private(self._inside(path), base64.b64decode(data), mode)
        return True

    def read(self, path: str, offset: int = 0) -> dict[str, Any]:
        target = self._inside(path)
        with open(target, "rb") as handle:
            handle.seek(int(offset))
            chunk = handle.read(READ_CHUNK)
            size = os.fstat(handle.fileno()).st_size
        return {
            "data": base64.b64encode(chunk).decode(),
            "size": size,
            "eof": int(offset) + len(chunk) >= size,
        }

    def exists(self, path: str) -> bool:
        return self._inside(path, extra=(self.socket_dir,)).exists()

    def glob(self, directory: str, patterns: Sequence[str]) -> list[str]:
        base = self._inside(directory)
        found: list[str] = []
        for pattern in patterns:
            if pattern.startswith("/") or ".." in Path(pattern).parts:
                raise HelperError(f"invalid pattern {pattern!r}")
            for match in sorted(glob.glob(str(base / pattern))):
                found.append(str(self._inside(match)))
        return found

    def remove(self, path: str) -> bool:
        target = self._inside(path)
        if target.parent != self.root:
            raise HelperError("only a run directory under the root is removed")
        shutil.rmtree(target, ignore_errors=True)
        return True

    def handle(self, request: Mapping[str, Any]) -> Any:
        table = {
            "start": self.start,
            "alive": self.alive,
            "stop": self.stop,
            "run": self.run,
            "write": self.write,
            "read": self.read,
            "exists": self.exists,
            "glob": self.glob,
            "remove": self.remove,
            "ping": lambda: {"root": str(self.root), "units": self.units},
        }
        op = request.get("op")
        if op not in table:
            raise HelperError(f"unknown operation {op!r}")
        return table[op](**(request.get("args") or {}))


def reply_for(helper: Helper, data: bytes) -> bytes:
    try:
        value = helper.handle(json.loads(data))
        reply: dict[str, Any] = {"ok": True, "value": value}
    except (HelperError, OSError, subprocess.SubprocessError, TypeError) as error:
        reply = {"ok": False, "error": f"{type(error).__name__}: {error}"[:4000]}
    return json.dumps(reply).encode()


async def serve(
    helper: Helper,
    nats_url: str,
    token: str,
    subject: str,
    stop: asyncio.Event | None = None,
) -> None:
    """Answer requests until ``stop`` is set (by default: SIGTERM or SIGINT)."""
    from nats.aio.client import Client as NATS

    nc = NATS()
    await nc.connect(
        servers=[nats_url],
        token=token,
        connect_timeout=3,
        reconnect_time_wait=0.5,
        max_reconnect_attempts=-1,
    )

    async def on_request(message: Any) -> None:
        reply = await asyncio.to_thread(reply_for, helper, message.data)
        await message.respond(reply)

    await nc.subscribe(subject, cb=on_request)
    print(f"labhelper serving {subject} under {helper.root}", flush=True)
    if stop is None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop.set)
    await stop.wait()
    await nc.drain()


def read_token(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise SystemExit(f"labhelper: no token file at {path}")
    return path.read_text().strip()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.labhelper")
    parser.add_argument("--host", required=True)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--python", required=True)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--nats-url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--subject")
    parser.add_argument("--units", default="user", choices=("user", "system", "none"))
    args = parser.parse_args(argv)
    args.root.mkdir(parents=True, exist_ok=True)
    helper = Helper(args.root, args.python, repo=args.repo, units=args.units)
    asyncio.run(
        serve(
            helper,
            args.nats_url,
            read_token(args.token_file),
            args.subject or subject_for(args.host),
        )
    )


if __name__ == "__main__":
    main()
