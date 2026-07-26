from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import secrets
import stat
import subprocess
import sys
import warnings
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS_IN = ROOT / "scripts" / "research" / "requirements.in"
REQUIREMENTS_LOCK = ROOT / "scripts" / "research" / "requirements.lock.txt"
TOOLCHAIN = ROOT / "scripts" / "research" / "toolchain.json"
RUN_PYTHON = ROOT / "scripts" / "research" / "run-python"
NATS_HELPER = ROOT / "tests" / "research" / "nats_server.py"

EXPECTED_IMAGE = (
    "nats@sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927"
)
EXPECTED_TOOLCHAIN = {
    "nats_image": EXPECTED_IMAGE,
    "python_version": "3.12",
    "uv_version": "0.8.13",
}
EXPECTED_DIRECT_REQUIREMENTS = [
    "fastapi",
    "httpx",
    "jsonschema",
    "nats-py==2.14.0",
    "pydantic",
    "pydantic-settings",
    "pytest",
    "pytest-asyncio",
    "python-dotenv",
    "pyyaml",
    "respx",
    "sqlite-vec",
    "uvicorn[standard]",
    "websockets",
]
OWNER_LABEL = "ai.edgecitadel.owner=test-nats"


def _assert_file(path: Path) -> None:
    assert path.is_file(), (
        f"required toolchain file is missing: {path.relative_to(ROOT)}"
    )


def _logical_requirements(lock_path: Path) -> list[list[str]]:
    requirements: list[list[str]] = []
    current: list[str] = []

    for raw_line in lock_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        part = stripped.removesuffix("\\").rstrip()
        if raw_line[:1].isspace():
            assert current, f"orphaned lock continuation: {stripped}"
            current.append(part)
            continue

        if current:
            requirements.append(current)
        current = [part]

    if current:
        requirements.append(current)
    return requirements


def _load_nats_helper() -> ModuleType:
    _assert_file(NATS_HELPER)
    module_name = "_edgecitadel_contract_nats_server"
    spec = importlib.util.spec_from_file_location(module_name, NATS_HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_toolchain_metadata_and_direct_requirements_are_exact() -> None:
    _assert_file(TOOLCHAIN)
    _assert_file(REQUIREMENTS_IN)

    assert json.loads(TOOLCHAIN.read_text(encoding="utf-8")) == EXPECTED_TOOLCHAIN
    assert REQUIREMENTS_IN.read_text(encoding="utf-8").splitlines() == (
        EXPECTED_DIRECT_REQUIREMENTS
    )


def test_every_locked_requirement_is_exact_and_hashed() -> None:
    _assert_file(REQUIREMENTS_LOCK)

    requirements = _logical_requirements(REQUIREMENTS_LOCK)
    assert requirements, "the research dependency lock must not be empty"
    for requirement in requirements:
        assert "==" in requirement[0], f"requirement is not exact: {requirement[0]}"
        assert any(part.startswith("--hash=sha256:") for part in requirement), (
            f"requirement is not hash locked: {requirement[0]}"
        )


def test_launcher_uses_exact_uv_semver_and_external_hash_keyed_venv(
    tmp_path: Path,
) -> None:
    _assert_file(RUN_PYTHON)
    _assert_file(REQUIREMENTS_LOCK)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "--version" ]]; then
  printf 'uv %s (contract-build metadata)\\n' "${FAKE_UV_VERSION:-0.8.13}"
elif [[ "$1" == "venv" ]]; then
  printf '%s\\n' "$*" >>"$FAKE_UV_LOG"
  venv="${!#}"
  mkdir -p "$venv/bin"
  ln -s "$FAKE_UV_PYTHON" "$venv/bin/python"
elif [[ "$1" == "pip" && "$2" == "sync" ]]; then
  printf '%s\\n' "$*" >>"$FAKE_UV_LOG"
else
  printf 'unexpected fake uv invocation: %s\\n' "$*" >&2
  exit 64
fi
""",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)

    runtime_tmp = tmp_path / "runtime"
    runtime_tmp.mkdir()
    lock_digest = hashlib.sha256(REQUIREMENTS_LOCK.read_bytes()).hexdigest()
    venv = runtime_tmp / f"edgecitadel-research-py312-{lock_digest[:16]}"
    log = tmp_path / "uv.log"
    env = {
        **os.environ,
        "FAKE_UV_LOG": str(log),
        "FAKE_UV_PYTHON": sys.executable,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "TMPDIR": str(runtime_tmp),
    }
    env.pop("EC_RESEARCH_VENV", None)
    command = [
        str(RUN_PYTHON),
        "-c",
        (
            "import os, sys; assert sys.version_info[:2] == (3, 12); "
            "assert os.environ['PYTHONDONTWRITEBYTECODE'] == '1'; "
            "assert os.environ['PYTHONPYCACHEPREFIX'].startswith("
            "os.environ['TMPDIR']); "
            "assert sys.pycache_prefix == os.environ['PYTHONPYCACHEPREFIX']; "
            "assert sys.argv[1] == 'argument with spaces;$(false)'"
        ),
        "argument with spaces;$(false)",
    ]

    subprocess.run(command, cwd=tmp_path, env=env, check=True)
    subprocess.run(command, cwd=tmp_path, env=env, check=True)

    logged = log.read_text(encoding="utf-8").splitlines()
    assert logged.count(f"venv --managed-python --python 3.12 {venv}") == 1
    expected_sync = (
        f"pip sync --python {venv}/bin/python --require-hashes {REQUIREMENTS_LOCK}"
    )
    assert logged.count(expected_sync) == 2
    assert not any(
        candidate.exists()
        for candidate in (ROOT / ".venv", ROOT / ".uv-cache", ROOT / "venv")
    )

    wrong_version = subprocess.run(
        command,
        cwd=tmp_path,
        env={**env, "FAKE_UV_VERSION": "0.8.14"},
        text=True,
        capture_output=True,
        check=False,
    )
    assert wrong_version.returncode != 0
    assert "0.8.13" in wrong_version.stderr
    assert "0.8.14" in wrong_version.stderr


def test_launcher_executes_managed_python_312() -> None:
    _assert_file(RUN_PYTHON)

    subprocess.run(
        [
            str(RUN_PYTHON),
            "-c",
            "import sys; assert sys.version_info[:2] == (3, 12)",
        ],
        cwd=ROOT,
        check=True,
    )


def test_direct_lock_supports_all_research_suites() -> None:
    _assert_file(RUN_PYTHON)

    imports = (
        "import dotenv, fastapi, httpx, jsonschema, nats, pydantic, "
        "pydantic_settings, pytest, pytest_asyncio, respx, sqlite_vec, "
        "uvicorn, websockets, yaml"
    )
    subprocess.run([str(RUN_PYTHON), "-c", imports], cwd=ROOT, check=True)


class _FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        self.container_count = 0

    def __call__(
        self,
        argv: Sequence[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert isinstance(argv, list), "Docker commands must use argv form"
        assert all(isinstance(part, str) for part in argv)
        normalized = tuple(argv)
        self.calls.append((normalized, kwargs))

        stdout = ""
        if normalized[:2] == ("docker", "run"):
            self.container_count += 1
            stdout = f"container-{self.container_count}\n"
        elif normalized[:2] == ("docker", "port"):
            container_number = int(normalized[2].rsplit("-", 1)[1])
            stdout = f"127.0.0.1:{42000 + container_number}\n"
        elif normalized[:3] == ("docker", "volume", "create"):
            stdout = f"{normalized[-1]}\n"

        return subprocess.CompletedProcess(list(argv), 0, stdout=stdout, stderr="")


class _FakeNatsClient:
    def __init__(self) -> None:
        self.flushed = False
        self.closed = False

    async def flush(self) -> None:
        self.flushed = True

    async def close(self) -> None:
        self.closed = True


def _calls_with_prefix(
    runner: _FakeRunner,
    prefix: tuple[str, ...],
) -> list[tuple[str, ...]]:
    return [argv for argv, _ in runner.calls if argv[: len(prefix)] == prefix]


def _mount_source(run_argv: tuple[str, ...], target: str) -> Path:
    for index, argument in enumerate(run_argv):
        if argument != "--mount":
            continue
        fields = {
            key: value
            for field in run_argv[index + 1].split(",")
            if "=" in field
            for key, value in [field.split("=", 1)]
        }
        if fields.get("target") == target:
            return Path(fields["source"])
    raise AssertionError(f"missing mount target {target}")


def test_nats_server_owns_commands_credentials_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _FakeRunner()
    connect_calls: list[dict[str, Any]] = []
    clients: list[_FakeNatsClient] = []

    async def fake_connect(*args: Any, **kwargs: Any) -> _FakeNatsClient:
        connect_calls.append({"args": args, "kwargs": kwargs})
        client = _FakeNatsClient()
        clients.append(client)
        return client

    monkeypatch.setattr(module.nats, "connect", fake_connect)
    token = "contract-token-never-in-docker-argv"
    server = module.NatsServer(token=token, jetstream=True, runner=runner)

    assert server.start() is server
    run_calls = _calls_with_prefix(runner, ("docker", "run"))
    assert len(run_calls) == 1
    first_run = run_calls[0]
    assert EXPECTED_IMAGE in first_run
    assert first_run[first_run.index("--publish") + 1] == "127.0.0.1::4222"
    assert first_run[first_run.index("--label") + 1] == OWNER_LABEL
    assert token not in " ".join(first_run)

    config_path = _mount_source(first_run, "/etc/nats/nats.conf")
    temp_dir = config_path.parent
    assert stat.S_IMODE(temp_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
    assert token in config_path.read_text(encoding="utf-8")
    assert server.url == "nats://127.0.0.1:42001"
    assert connect_calls[-1]["kwargs"]["token"] == token
    assert clients[-1].flushed and clients[-1].closed

    volume_creates = _calls_with_prefix(runner, ("docker", "volume", "create"))
    assert len(volume_creates) == 1
    first_volume = volume_creates[0][-1]

    server.restart(preserve_storage=True)
    assert len(_calls_with_prefix(runner, ("docker", "run"))) == 2
    assert len(_calls_with_prefix(runner, ("docker", "volume", "create"))) == 1
    assert not _calls_with_prefix(runner, ("docker", "volume", "rm"))
    assert first_volume in " ".join(_calls_with_prefix(runner, ("docker", "run"))[1])

    server.restart(preserve_storage=False)
    run_calls = _calls_with_prefix(runner, ("docker", "run"))
    volume_creates = _calls_with_prefix(runner, ("docker", "volume", "create"))
    assert len(run_calls) == 3
    assert len(volume_creates) == 2
    second_volume = volume_creates[-1][-1]
    assert second_volume != first_volume
    assert _calls_with_prefix(runner, ("docker", "volume", "rm", "--force")) == [
        ("docker", "volume", "rm", "--force", first_volume)
    ]

    assert all(
        token not in " ".join(argv)
        for argv, _ in runner.calls
        if argv and argv[0] == "docker"
    )
    server.close()
    calls_after_first_close = list(runner.calls)
    server.close()
    assert runner.calls == calls_after_first_close
    assert not temp_dir.exists()
    assert _calls_with_prefix(runner, ("docker", "rm", "--force")) == [
        ("docker", "rm", "--force", "container-1"),
        ("docker", "rm", "--force", "container-2"),
        ("docker", "rm", "--force", "container-3"),
    ]
    assert _calls_with_prefix(runner, ("docker", "volume", "rm", "--force"))[-1] == (
        "docker",
        "volume",
        "rm",
        "--force",
        second_volume,
    )
    cleanup_prefixes = {
        ("docker", "rm"),
        ("docker", "volume", "rm"),
    }
    assert all(
        kwargs["check"] is False
        for argv, kwargs in runner.calls
        if argv[:2] in cleanup_prefixes or argv[:3] in cleanup_prefixes
    )


def test_nats_server_start_is_safe_inside_a_running_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _FakeRunner()

    async def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeNatsClient:
        return _FakeNatsClient()

    monkeypatch.setattr(module.nats, "connect", fake_connect)

    async def start_and_close() -> None:
        server = module.NatsServer(
            token="event-loop-token",
            jetstream=False,
            runner=runner,
        )
        try:
            assert server.start() is server
        finally:
            server.close()

    asyncio.run(start_and_close())


def test_nats_server_cleans_a_partial_configuration_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = _load_nats_helper()
    failed_directory = tmp_path / "partial-nats"

    def create_temp_directory(*_args: Any, **_kwargs: Any) -> str:
        failed_directory.mkdir(mode=0o700)
        return str(failed_directory)

    real_os_open: Callable[..., int] = module.os.open

    def fail_config_open(path: Any, *args: Any, **kwargs: Any) -> int:
        if (
            isinstance(path, (str, Path))
            and Path(path) == failed_directory / "nats.conf"
        ):
            raise OSError("injected config creation failure")
        return real_os_open(path, *args, **kwargs)

    monkeypatch.setattr(module.tempfile, "mkdtemp", create_temp_directory)
    monkeypatch.setattr(module.os, "open", fail_config_open)
    server = module.NatsServer(
        token="partial-start-token",
        jetstream=True,
        runner=_FakeRunner(),
    )

    with pytest.raises(OSError, match="injected config creation failure"):
        server.start()
    assert not failed_directory.exists()
    server.close()


def _owned_docker_resources(kind: str) -> set[str]:
    if kind == "container":
        command = [
            "docker",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            f"label={OWNER_LABEL}",
        ]
    elif kind == "volume":
        command = [
            "docker",
            "volume",
            "ls",
            "--quiet",
            "--filter",
            f"label={OWNER_LABEL}",
        ]
    else:
        raise AssertionError(f"unknown Docker resource kind: {kind}")

    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=True,
    )
    return {line for line in result.stdout.splitlines() if line}


async def _prove_nats_authentication(url: str, token: str) -> None:
    from nats.errors import Error as NatsError

    import nats

    async def ignore_error(_error: Exception) -> None:
        return None

    with pytest.raises(NatsError):
        await nats.connect(
            servers=[url],
            token="incorrect-token",
            connect_timeout=1,
            allow_reconnect=False,
            max_reconnect_attempts=0,
            error_cb=ignore_error,
        )

    client = await nats.connect(
        servers=[url],
        token=token,
        connect_timeout=1,
        allow_reconnect=False,
        max_reconnect_attempts=0,
    )
    await client.flush()
    await client.close()


async def _seed_isolated_storage(url: str, token: str, payload: bytes) -> int:
    import nats

    client = await nats.connect(
        servers=[url],
        token=token,
        connect_timeout=1,
        allow_reconnect=False,
        max_reconnect_attempts=0,
    )
    try:
        jetstream = client.jetstream()
        await jetstream.add_stream(name="ISOLATION", subjects=["isolation"])
        await jetstream.publish("isolation", payload)
        info = await jetstream.stream_info("ISOLATION")
        return info.state.messages
    finally:
        await client.close()


with warnings.catch_warnings():
    warnings.simplefilter("ignore", pytest.PytestUnknownMarkWarning)
    docker_test = pytest.mark.docker


@docker_test
def test_two_live_nats_servers_are_authenticated_isolated_and_cleaned(
    request: pytest.FixtureRequest,
) -> None:
    if request.config.option.markexpr != "docker":
        pytest.skip("run explicitly with -m docker")

    module = _load_nats_helper()
    assert not _owned_docker_resources("container")
    assert not _owned_docker_resources("volume")

    first_token = secrets.token_hex(32)
    second_token = secrets.token_hex(32)
    with ExitStack() as stack:
        first = module.NatsServer(token=first_token, jetstream=True)
        second = module.NatsServer(token=second_token, jetstream=True)
        stack.callback(first.close)
        stack.callback(second.close)
        first.start()
        second.start()

        assert first.url != second.url
        assert len(_owned_docker_resources("container")) == 2
        assert len(_owned_docker_resources("volume")) == 2
        asyncio.run(_prove_nats_authentication(first.url, first_token))
        asyncio.run(_prove_nats_authentication(second.url, second_token))
        assert (
            asyncio.run(_seed_isolated_storage(first.url, first_token, b"first")) == 1
        )
        assert (
            asyncio.run(_seed_isolated_storage(second.url, second_token, b"second"))
            == 1
        )

    assert not _owned_docker_resources("container")
    assert not _owned_docker_resources("volume")
