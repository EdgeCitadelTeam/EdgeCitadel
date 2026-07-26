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
from contextlib import ExitStack, suppress
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from packaging.requirements import InvalidRequirement, Requirement

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
LOCK_HASH_PREFIX = "--hash=sha256:"


def _assert_file(path: Path) -> None:
    assert path.is_file(), (
        f"required toolchain file is missing: {path.relative_to(ROOT)}"
    )


def _logical_requirements(lock_path: Path) -> list[list[str]]:
    requirements: list[list[str]] = []
    current: list[str] = []
    expects_continuation = False

    for raw_line in lock_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        continues = stripped.endswith("\\")
        part = stripped.removesuffix("\\").rstrip()
        if raw_line[:1].isspace():
            assert current and expects_continuation, (
                f"orphaned lock continuation: {stripped}"
            )
            current.append(part)
        else:
            assert not current, f"unfinished lock requirement before: {stripped}"
            current = [part]

        expects_continuation = continues
        if not continues:
            _validate_locked_requirement(current)
            requirements.append(current)
            current = []

    assert not current and not expects_continuation, (
        "unfinished lock requirement at EOF"
    )
    return requirements


def _validate_locked_requirement(requirement: list[str]) -> None:
    try:
        parsed = Requirement(requirement[0])
    except InvalidRequirement as error:
        raise AssertionError(f"invalid locked requirement: {requirement[0]}") from error

    specifiers = list(parsed.specifier)
    assert parsed.url is None, f"URL requirement is not an exact pin: {requirement[0]}"
    assert len(specifiers) == 1, f"requirement is not exact: {requirement[0]}"
    assert specifiers[0].operator == "==", (
        f"requirement is not pinned with ==: {requirement[0]}"
    )
    assert "*" not in specifiers[0].version, (
        f"wildcard requirement is not exact: {requirement[0]}"
    )
    assert len(requirement) > 1, f"requirement has no hashes: {requirement[0]}"
    for hash_entry in requirement[1:]:
        assert hash_entry.startswith(LOCK_HASH_PREFIX), (
            f"unsupported lock continuation: {hash_entry}"
        )
        digest = hash_entry.removeprefix(LOCK_HASH_PREFIX)
        assert len(digest) == 64 and all(
            character in "0123456789abcdef" for character in digest
        ), f"invalid SHA-256 hash: {hash_entry}"


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


@pytest.mark.parametrize(
    "lock_text",
    [
        (f"demo==1.0\n    --hash=sha256:{'a' * 64}\n"),
        (f"demo ; python_version == '3.12' \\\n    --hash=sha256:{'a' * 64}\n"),
        (f"demo @ https://example.invalid/demo.whl \\\n    --hash=sha256:{'a' * 64}\n"),
        ("demo==1.0 \\\n    --hash=sha256:ABC123\n"),
    ],
    ids=[
        "continuation-without-backslash",
        "marker-equality-is-not-a-pin",
        "url-is-not-a-pin",
        "hash-must-be-lowercase-64-hex",
    ],
)
def test_lock_parser_rejects_malformed_logical_records(
    tmp_path: Path,
    lock_text: str,
) -> None:
    lock_path = tmp_path / "malformed.lock"
    lock_path.write_text(lock_text, encoding="utf-8")

    with pytest.raises(AssertionError):
        _logical_requirements(lock_path)


def test_lock_parser_accepts_a_well_formed_logical_record(tmp_path: Path) -> None:
    lock_path = tmp_path / "valid.lock"
    lock_path.write_text(
        (
            "demo==1.0 \\\n"
            f"    --hash=sha256:{'a' * 64} \\\n"
            f"    --hash=sha256:{'b' * 64}\n"
        ),
        encoding="utf-8",
    )

    assert _logical_requirements(lock_path) == [
        [
            "demo==1.0",
            f"--hash=sha256:{'a' * 64}",
            f"--hash=sha256:{'b' * 64}",
        ]
    ]


def _write_nonwriting_uv(fake_bin: Path, log: Path) -> None:
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "--version" ]]; then
  printf 'uv 0.8.13 (containment-probe)\\n'
  exit 0
fi
printf '%s\\n' "$*" >>"$FAKE_UV_LOG"
printf 'stateful uv invocation rejected by probe\\n' >&2
exit 97
""",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    assert not log.exists()


@pytest.mark.parametrize(
    "alias_kind",
    [
        "venv-dot-segment",
        "venv-symlink-ancestor",
        "venv-symlink-parent-segment",
        "tmp-dot-segment",
        "tmp-symlink-ancestor",
    ],
)
def test_launcher_rejects_checkout_aliases_without_creating_state(
    tmp_path: Path,
    alias_kind: str,
) -> None:
    fake_bin = tmp_path / "bin"
    log = tmp_path / "uv.log"
    _write_nonwriting_uv(fake_bin, log)
    external_tmp = tmp_path / "runtime"
    external_tmp.mkdir()
    protected_name = f".contract-rejected-{alias_kind}-{tmp_path.name}"
    protected_path = ROOT / protected_name
    env = {
        **os.environ,
        "FAKE_UV_LOG": str(log),
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "TMPDIR": str(external_tmp),
    }
    env.pop("EC_RESEARCH_VENV", None)

    if alias_kind == "venv-dot-segment":
        env["EC_RESEARCH_VENV"] = f"{ROOT.parent}/./{ROOT.name}/{protected_name}"
    elif alias_kind == "venv-symlink-ancestor":
        alias = tmp_path / "checkout-alias"
        alias.symlink_to(ROOT, target_is_directory=True)
        env["EC_RESEARCH_VENV"] = str(alias / protected_name)
    elif alias_kind == "venv-symlink-parent-segment":
        alias = tmp_path / "checkout-subdirectory-alias"
        alias.symlink_to(ROOT / "tests", target_is_directory=True)
        env["EC_RESEARCH_VENV"] = str(alias / ".." / protected_name)
    elif alias_kind == "tmp-dot-segment":
        env["TMPDIR"] = f"{ROOT.parent}/./{ROOT.name}/{protected_name}"
    else:
        alias = tmp_path / "checkout-alias"
        alias.symlink_to(ROOT, target_is_directory=True)
        env["TMPDIR"] = str(alias / protected_name)

    result = subprocess.run(
        [str(RUN_PYTHON), "-c", "raise AssertionError('payload must not run')"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "outside the checkout" in result.stderr
    assert not log.exists(), "rejected path reached a stateful uv command"
    assert not protected_path.exists()


@pytest.mark.parametrize(
    "existing_kind",
    ["not-uv-managed", "wrong-python", "wrong-uv-version"],
)
def test_launcher_rejects_an_invalid_existing_environment_before_sync(
    tmp_path: Path,
    existing_kind: str,
) -> None:
    fake_bin = tmp_path / "bin"
    log = tmp_path / "uv.log"
    fake_bin.mkdir()
    fake_uv = fake_bin / "uv"
    fake_uv.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == "--version" ]]; then
  printf 'uv 0.8.13 (existing-environment-probe)\\n'
else
  printf '%s\\n' "$*" >>"$FAKE_UV_LOG"
fi
""",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)

    venv = tmp_path / "existing-venv"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    if existing_kind == "not-uv-managed":
        python.symlink_to(sys.executable)
    elif existing_kind == "wrong-python":
        (venv / "pyvenv.cfg").write_text("uv = 0.8.13\n", encoding="utf-8")
        python.write_text(
            "#!/usr/bin/env bash\nprintf '3.11\\n'\n",
            encoding="utf-8",
        )
        python.chmod(0o755)
    else:
        (venv / "pyvenv.cfg").write_text("uv = 0.8.12\n", encoding="utf-8")
        python.write_text(
            "#!/usr/bin/env bash\nprintf '3.12\\n'\n",
            encoding="utf-8",
        )
        python.chmod(0o755)

    runtime_tmp = tmp_path / "runtime"
    runtime_tmp.mkdir()
    result = subprocess.run(
        [str(RUN_PYTHON), "-c", "raise AssertionError('payload must not run')"],
        cwd=tmp_path,
        env={
            **os.environ,
            "EC_RESEARCH_VENV": str(venv),
            "FAKE_UV_LOG": str(log),
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "TMPDIR": str(runtime_tmp),
        },
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert not log.exists(), "invalid existing environment was synced or mutated"


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
  printf 'uv = 0.8.13\\n' >"$venv/pyvenv.cfg"
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
        "UV_PYTHON_INSTALL_DIR": str(ROOT / ".forbidden-uv-python-install"),
        "XDG_DATA_HOME": str(ROOT / ".forbidden-xdg-data"),
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
            "assert os.environ['UV_PYTHON_INSTALL_DIR'] == "
            "os.path.join(os.environ['TMPDIR'], "
            "'edgecitadel-research-uv-python'); "
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


class _LifecycleRunner(_FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.containers: dict[str, str] = {}
        self.volumes: set[str] = set()
        self.run_create_then_raise = False
        self.fail_next_volume_create = False
        self.container_remove_outcomes: list[str] = []
        self.volume_remove_outcomes: list[str] = []

    def __call__(
        self,
        argv: Sequence[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert isinstance(argv, list), "Docker commands must use argv form"
        normalized = tuple(argv)
        self.calls.append((normalized, kwargs))

        if normalized[:3] == ("docker", "volume", "create"):
            volume_name = normalized[-1]
            if self.fail_next_volume_create:
                self.fail_next_volume_create = False
                raise subprocess.CalledProcessError(
                    1,
                    list(argv),
                    stderr="injected volume creation failure",
                )
            self.volumes.add(volume_name)
            return subprocess.CompletedProcess(
                list(argv),
                0,
                stdout=f"{volume_name}\n",
                stderr="",
            )

        if normalized[:2] == ("docker", "run"):
            self.container_count += 1
            container_name = normalized[normalized.index("--name") + 1]
            container_id = f"container-{self.container_count}"
            self.containers[container_name] = container_id
            if self.run_create_then_raise:
                self.run_create_then_raise = False
                raise subprocess.CalledProcessError(
                    127,
                    list(argv),
                    stderr="injected OCI start failure after create",
                )
            return subprocess.CompletedProcess(
                list(argv),
                0,
                stdout=f"{container_id}\n",
                stderr="",
            )

        if normalized[:2] == ("docker", "port"):
            container_id = normalized[2]
            container_number = int(container_id.rsplit("-", 1)[1])
            return subprocess.CompletedProcess(
                list(argv),
                0,
                stdout=f"127.0.0.1:{43000 + container_number}\n",
                stderr="",
            )

        if normalized[:3] == ("docker", "rm", "--force"):
            outcome = (
                self.container_remove_outcomes.pop(0)
                if self.container_remove_outcomes
                else "default"
            )
            target = normalized[-1]
            container_name = next(
                (
                    name
                    for name, container_id in self.containers.items()
                    if target in (name, container_id)
                ),
                None,
            )
            if outcome == "error":
                return subprocess.CompletedProcess(
                    list(argv),
                    1,
                    stdout="",
                    stderr="injected daemon removal failure",
                )
            if outcome == "absent" or container_name is None:
                return subprocess.CompletedProcess(
                    list(argv),
                    1,
                    stdout="",
                    stderr=f"Error: No such container: {target}",
                )
            del self.containers[container_name]
            return subprocess.CompletedProcess(list(argv), 0, stdout=target, stderr="")

        if normalized[:4] == ("docker", "volume", "rm", "--force"):
            outcome = (
                self.volume_remove_outcomes.pop(0)
                if self.volume_remove_outcomes
                else "default"
            )
            volume_name = normalized[-1]
            if outcome == "error":
                return subprocess.CompletedProcess(
                    list(argv),
                    1,
                    stdout="",
                    stderr="injected volume removal failure",
                )
            if outcome == "absent" or volume_name not in self.volumes:
                return subprocess.CompletedProcess(
                    list(argv),
                    1,
                    stdout="",
                    stderr=f"Error: No such volume: {volume_name}",
                )
            self.volumes.remove(volume_name)
            return subprocess.CompletedProcess(
                list(argv),
                0,
                stdout=volume_name,
                stderr="",
            )

        return subprocess.CompletedProcess(list(argv), 0, stdout="", stderr="")


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


def test_nats_server_cleans_exact_name_when_run_creates_then_raises() -> None:
    module = _load_nats_helper()
    runner = _LifecycleRunner()
    runner.run_create_then_raise = True
    server = module.NatsServer(
        token="create-then-raise-token",
        jetstream=False,
        runner=runner,
    )

    with pytest.raises(subprocess.CalledProcessError):
        server.start()

    run_call = _calls_with_prefix(runner, ("docker", "run"))[0]
    attempted_name = run_call[run_call.index("--name") + 1]
    assert not runner.containers
    assert _calls_with_prefix(runner, ("docker", "rm", "--force")) == [
        ("docker", "rm", "--force", attempted_name)
    ]
    assert not any(
        argv[:2] in {("docker", "ps"), ("docker", "container")}
        for argv, _ in runner.calls
    )
    calls_after_rollback = list(runner.calls)
    server.close()
    assert runner.calls == calls_after_rollback


def test_nats_server_retries_transient_owned_cleanup_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _LifecycleRunner()

    async def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeNatsClient:
        return _FakeNatsClient()

    monkeypatch.setattr(module.nats, "connect", fake_connect)
    server = module.NatsServer(
        token="transient-cleanup-token",
        jetstream=True,
        runner=runner,
    ).start()
    run_call = _calls_with_prefix(runner, ("docker", "run"))[0]
    temp_dir = _mount_source(run_call, "/etc/nats/nats.conf").parent
    real_rmtree: Callable[..., None] = module.shutil.rmtree
    rmtree_calls = 0

    def flaky_rmtree(path: Any, *args: Any, **kwargs: Any) -> None:
        nonlocal rmtree_calls
        rmtree_calls += 1
        if rmtree_calls == 1:
            raise OSError("injected temporary-directory removal failure")
        real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(module.shutil, "rmtree", flaky_rmtree)
    runner.container_remove_outcomes.append("error")
    runner.volume_remove_outcomes.append("error")

    try:
        with pytest.raises(RuntimeError, match="cleanup"):
            server.close()

        assert runner.containers
        assert runner.volumes
        assert temp_dir.exists()
        assert rmtree_calls == 1

        server.close()
        assert not runner.containers
        assert not runner.volumes
        assert not temp_dir.exists()
        assert rmtree_calls == 2
    finally:
        runner.container_remove_outcomes.clear()
        runner.volume_remove_outcomes.clear()
        with suppress(RuntimeError):
            server.close()
        if temp_dir.exists():
            real_rmtree(temp_dir, ignore_errors=True)


def test_nats_server_treats_exact_external_absence_as_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _LifecycleRunner()

    async def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeNatsClient:
        return _FakeNatsClient()

    monkeypatch.setattr(module.nats, "connect", fake_connect)
    server = module.NatsServer(
        token="external-absence-token",
        jetstream=True,
        runner=runner,
    ).start()
    run_call = _calls_with_prefix(runner, ("docker", "run"))[0]
    temp_dir = _mount_source(run_call, "/etc/nats/nats.conf").parent
    runner.containers.clear()
    runner.volumes.clear()

    server.close()
    assert not temp_dir.exists()
    calls_after_close = list(runner.calls)
    server.close()
    assert runner.calls == calls_after_close


def test_nats_server_rolls_back_a_fresh_volume_restart_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _LifecycleRunner()

    async def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeNatsClient:
        return _FakeNatsClient()

    monkeypatch.setattr(module.nats, "connect", fake_connect)
    server = module.NatsServer(
        token="restart-volume-failure-token",
        jetstream=True,
        runner=runner,
    ).start()
    run_call = _calls_with_prefix(runner, ("docker", "run"))[0]
    temp_dir = _mount_source(run_call, "/etc/nats/nats.conf").parent
    runner.fail_next_volume_create = True

    try:
        with pytest.raises(subprocess.CalledProcessError):
            server.restart(preserve_storage=False)

        assert not runner.containers
        assert not runner.volumes
        assert not temp_dir.exists()
        volume_removals = _calls_with_prefix(
            runner,
            ("docker", "volume", "rm", "--force"),
        )
        assert len(volume_removals) == 2
        server.close()
    finally:
        with suppress(RuntimeError):
            server.close()
        if temp_dir.exists():
            module.shutil.rmtree(temp_dir, ignore_errors=True)


def test_nats_server_rolls_back_a_restart_removal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_nats_helper()
    runner = _LifecycleRunner()

    async def fake_connect(*_args: Any, **_kwargs: Any) -> _FakeNatsClient:
        return _FakeNatsClient()

    monkeypatch.setattr(module.nats, "connect", fake_connect)
    server = module.NatsServer(
        token="restart-removal-failure-token",
        jetstream=True,
        runner=runner,
    ).start()
    run_call = _calls_with_prefix(runner, ("docker", "run"))[0]
    temp_dir = _mount_source(run_call, "/etc/nats/nats.conf").parent
    runner.container_remove_outcomes.append("error")

    try:
        with pytest.raises(RuntimeError, match="remove container"):
            server.restart(preserve_storage=False)

        assert not runner.containers
        assert not runner.volumes
        assert not temp_dir.exists()
        server.close()
    finally:
        runner.container_remove_outcomes.clear()
        with suppress(RuntimeError):
            server.close()
        if temp_dir.exists():
            module.shutil.rmtree(temp_dir, ignore_errors=True)


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


def _container_data_mount_sources(container_ids: set[str]) -> set[str]:
    result = subprocess.run(
        ["docker", "inspect", *sorted(container_ids)],
        text=True,
        capture_output=True,
        check=True,
    )
    inspected = json.loads(result.stdout)
    sources: set[str] = set()
    for container in inspected:
        data_mounts = [
            mount
            for mount in container["Mounts"]
            if mount["Destination"] == "/data" and mount["Type"] == "volume"
        ]
        assert len(data_mounts) == 1
        sources.add(data_mounts[0]["Name"])
    return sources


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
        containers = _owned_docker_resources("container")
        volumes = _owned_docker_resources("volume")
        assert len(containers) == 2
        assert len(volumes) == 2
        data_mount_sources = _container_data_mount_sources(containers)
        assert len(data_mount_sources) == 2
        assert data_mount_sources == volumes
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
