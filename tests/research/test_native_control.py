"""Contract tests for the transport-independent native control fixture."""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import inspect
import json
import os
import sqlite3
import typing
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import FrozenInstanceError, asdict, fields
from pathlib import Path
from typing import TypeVar, cast

import pytest
from jsonschema import Draft202012Validator

from adapters._common.outcome_store import DisabledOutcomeStore
from adapters._common.task_executor import InjectedCrash, TaskExecutor
from adapters._common.task_publisher import EventSink
from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import canonical_json
from scripts.research.fixtures import native_control
from scripts.research.fixtures.native_control import (
    BEHAVIORS,
    CRASH_POINTS,
    NativeControlConfig,
    build_agent_card,
    load_native_config,
    parse_args,
    read_transport_token,
    run_fixture,
    runtime_endpoints,
)
from scripts.research.modes.base import Mode, TaskTransport

WIRE_ID = "10000000-0000-4000-8000-000000000001"
TASK_ID = "20000000-0000-4000-8000-000000000001"
CONTEXT_ID = "30000000-0000-4000-8000-000000000001"
PROGRESS_ID = "40000000-0000-4000-8000-000000000001"
TERMINAL_ID = "50000000-0000-4000-8000-000000000001"
NOW = "2026-07-25T12:00:00.000Z"

_TestFunction = TypeVar("_TestFunction", bound=Callable[..., object])


def _typed_decorator(
    decorator: object,
) -> Callable[[_TestFunction], _TestFunction]:
    return cast(Callable[[_TestFunction], _TestFunction], decorator)


async_test = _typed_decorator(pytest.mark.asyncio)


def cases(
    argnames: str | Sequence[str],
    argvalues: Iterable[object],
) -> Callable[[_TestFunction], _TestFunction]:
    return _typed_decorator(pytest.mark.parametrize(argnames, argvalues))


def _config(tmp_path: Path, **overrides: object) -> NativeControlConfig:
    values: dict[str, object] = {
        "run_id": "run-1",
        "agent_id": "worker-1",
        "mode": "core-only",
        "behavior": "echo",
        "delay_ms": 0,
        "crash_point": None,
        "heartbeat_interval_ms": 1000,
        "outcome_db": str(tmp_path / "outcomes.sqlite3"),
        "side_effect_db": str(tmp_path / "effects.sqlite3"),
    }
    values.update(overrides)
    return NativeControlConfig(
        run_id=cast(str, values["run_id"]),
        agent_id=cast(str, values["agent_id"]),
        mode=cast(str, values["mode"]),
        behavior=cast(str, values["behavior"]),
        delay_ms=cast(int, values["delay_ms"]),
        crash_point=cast(str | None, values["crash_point"]),
        heartbeat_interval_ms=cast(int, values["heartbeat_interval_ms"]),
        outcome_db=cast(str, values["outcome_db"]),
        side_effect_db=cast(str, values["side_effect_db"]),
    )


def _command(*, body: object = "nonce") -> dict[str, object]:
    return {
        "v": 1,
        "id": WIRE_ID,
        "type": "command",
        "sender_id": "requester-1",
        "recipient_id": "worker-1",
        "task_id": TASK_ID,
        "timestamp": NOW,
        "payload": {"body": body},
    }


class _Delivery:
    def __init__(self, envelope: Mapping[str, object]) -> None:
        self.worker_agent_id = "worker-1"
        self.raw = json.dumps(envelope).encode()
        self.delivery_count = 1
        self.stream_sequence: int | None = 7
        self.commits = 0
        self.retries = 0
        self.terminations = 0

    async def in_progress(self) -> None:
        return None

    async def commit(self) -> None:
        self.commits += 1

    async def retry(self) -> None:
        self.retries += 1

    async def terminate(self) -> None:
        self.terminations += 1


class _Clock:
    def __init__(self) -> None:
        self.monotonic = 100

    def monotonic_ns(self) -> int:
        self.monotonic += 1
        return self.monotonic

    def now_iso(self) -> str:
        return NOW


class _UUIDs:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def uuid4(self) -> str:
        if not self.values:
            raise AssertionError("unexpected UUID allocation")
        return self.values.pop(0)


class _CrashHook:
    def __init__(self, crash_point: str | None = None) -> None:
        self.crash_point = crash_point
        self.hits: list[str] = []

    def hit(self, point: str) -> None:
        self.hits.append(point)
        if point == self.crash_point:
            raise InjectedCrash(point)


class _EventSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(event)


class _SleepRecorder:
    def __init__(self, timeline: list[tuple[str, object]] | None = None) -> None:
        self.delays: list[float] = []
        self.timeline = timeline

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        if self.timeline is not None:
            self.timeline.append(("sleep", delay))


class _RecordingTransport:
    def __init__(self, timeline: list[tuple[str, object]] | None = None) -> None:
        self.progress: list[Mapping[str, object]] = []
        self.terminals: list[Mapping[str, object]] = []
        self.submissions: list[Mapping[str, object]] = []
        self.timeline = timeline

    @staticmethod
    def _receipt(envelope: Mapping[str, object]) -> PublicationReceipt:
        return PublicationReceipt(
            envelope_id=cast(str, envelope["id"]),
            accepted=True,
            transport="test",
            stream=None,
            stream_sequence=None,
            duplicate=None,
            accepted_ns=1,
            application_bytes=1,
            wire_bytes=None,
        )

    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.progress.append(envelope)
        if self.timeline is not None:
            self.timeline.append(("progress", envelope["id"]))
        return self._receipt(envelope)

    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.terminals.append(envelope)
        return self._receipt(envelope)

    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.submissions.append(envelope)
        return self._receipt(envelope)


class _LifecycleTransport(_RecordingTransport):
    def __init__(
        self,
        *,
        mode: Mode = Mode.CORE_ONLY,
        outcome_ledger_enabled: bool = False,
    ) -> None:
        super().__init__()
        self.mode = mode
        self.outcome_ledger_enabled = outcome_ledger_enabled
        self.lifecycle: list[tuple[str, object]] = []
        self.executor: TaskExecutor | None = None
        self.closed = 0

    async def start_receiver(
        self,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None:
        self.lifecycle.append(("start", agent_id))
        self.executor = executor

    async def wait_receiver_ready(self, agent_id: str, timeout_s: float) -> None:
        self.lifecycle.append(("ready", (agent_id, timeout_s)))

    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.lifecycle.append(("heartbeat", envelope["id"]))
        return self._receipt(envelope)

    async def close(self) -> None:
        self.closed += 1


class _ClosableStore:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


def test_public_contract_is_exact() -> None:
    assert native_control.__all__ == (
        "NativeControlConfig",
        "BEHAVIORS",
        "CRASH_POINTS",
        "build_agent_card",
        "run_fixture",
        "parse_args",
        "load_native_config",
        "runtime_endpoints",
        "read_transport_token",
    )


def test_public_function_signatures_are_exact() -> None:
    expected_parameters: dict[Callable[..., object], list[str]] = {
        build_agent_card: ["config"],
        run_fixture: ["config", "transport", "event_sink"],
        parse_args: ["argv"],
        load_native_config: ["path"],
        runtime_endpoints: ["config", "environ"],
        read_transport_token: ["path"],
    }
    for function, names in expected_parameters.items():
        signature = inspect.signature(function)
        assert list(signature.parameters) == names
        assert all(
            parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            and parameter.default is inspect.Parameter.empty
            for parameter in signature.parameters.values()
        )
    assert inspect.iscoroutinefunction(run_fixture)

    assert typing.get_type_hints(build_agent_card) == {
        "config": NativeControlConfig,
        "return": dict[str, object],
    }
    assert typing.get_type_hints(run_fixture) == {
        "config": NativeControlConfig,
        "transport": TaskTransport,
        "event_sink": EventSink,
        "return": type(None),
    }
    assert typing.get_type_hints(parse_args) == {
        "argv": Sequence[str],
        "return": argparse.Namespace,
    }
    assert typing.get_type_hints(load_native_config) == {
        "path": str | Path,
        "return": NativeControlConfig,
    }
    assert typing.get_type_hints(runtime_endpoints) == {
        "config": NativeControlConfig,
        "environ": Mapping[str, str],
        "return": dict[str, str],
    }
    assert typing.get_type_hints(read_transport_token) == {
        "path": str | Path,
        "return": str,
    }


def test_fixture_import_direction_and_package_inertness() -> None:
    root = Path(__file__).resolve().parents[2]
    initializer = root / "scripts/research/fixtures/__init__.py"
    module_path = root / "scripts/research/fixtures/native_control.py"
    initializer_tree = ast.parse(initializer.read_text())
    assert len(initializer_tree.body) == 1
    assert isinstance(initializer_tree.body[0], ast.Expr)
    assert isinstance(initializer_tree.body[0].value, ast.Constant)
    assert isinstance(initializer_tree.body[0].value.value, str)

    tree = ast.parse(module_path.read_text())
    imported_modules: set[str] = set()
    imported_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported_modules.add(node.module or "")
            imported_names.update(alias.name for alias in node.names)
    forbidden_modules = {
        "scripts.research.modes.central_relay",
        "scripts.research.modes.core_nats",
        "scripts.research.modes.edgecitadel",
        "scripts.research.modes.all_durable",
        "nats",
        "httpx",
        "aiohttp",
        "requests",
        "signal",
    }
    assert imported_modules.isdisjoint(forbidden_modules)
    assert "PullConsumer" not in imported_names
    defined_functions = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert {"main", "build_transport"}.isdisjoint(defined_functions)
    defined_classes = {
        node.name for node in tree.body if isinstance(node, ast.ClassDef)
    }
    assert {
        "EventSink",
        "PublicationReceipt",
        "TaskTransport",
        "TaskExecutor",
    }.isdisjoint(defined_classes)
    assert not any(
        isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "__name__"
        for node in tree.body
    )


def test_config_contract_is_exact_and_frozen(tmp_path: Path) -> None:
    assert BEHAVIORS == ("echo", "delegate", "progress", "actuator")
    assert CRASH_POINTS == (
        "after-receive-before-handler",
        "after-side-effect-before-ledger-prepare",
        "after-ledger-prepare-before-result-publish",
        "after-result-publish-before-publish-mark",
        "after-publish-mark-before-inbound-commit",
        "during-handler-exception-conversion",
    )
    assert [field.name for field in fields(NativeControlConfig)] == [
        "run_id",
        "agent_id",
        "mode",
        "behavior",
        "delay_ms",
        "crash_point",
        "heartbeat_interval_ms",
        "outcome_db",
        "side_effect_db",
    ]
    config = _config(tmp_path)
    field_name = "behavior"
    with pytest.raises(FrozenInstanceError):
        setattr(config, field_name, "progress")


@cases(
    ("field", "value"),
    [
        ("run_id", ""),
        ("run_id", "UPPER"),
        ("run_id", "line\nbreak"),
        ("run_id", "a" * 65),
        ("run_id", 1),
        ("agent_id", "_worker"),
        ("agent_id", "worker.dot"),
        ("agent_id", None),
        ("mode", "unknown"),
        ("mode", 1),
        ("behavior", "unknown"),
        ("behavior", 1),
        ("delay_ms", -1),
        ("delay_ms", True),
        ("delay_ms", 1.0),
        ("crash_point", "unknown"),
        ("crash_point", 1),
        ("heartbeat_interval_ms", 999),
        ("heartbeat_interval_ms", True),
        ("heartbeat_interval_ms", 1000.0),
        ("outcome_db", ""),
        ("outcome_db", "relative.sqlite3"),
        ("outcome_db", "/tmp/bad\x00name"),
        ("outcome_db", 1),
        ("side_effect_db", ""),
        ("side_effect_db", "relative.sqlite3"),
        ("side_effect_db", "/tmp/bad\x00name"),
        ("side_effect_db", 1),
    ],
)
def test_config_rejects_invalid_field(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match=rf"\b{field}\b") as raised:
        _config(tmp_path, **{field: value})
    assert repr(value) not in str(raised.value)


@cases("mode", [item.value for item in Mode])
@cases("behavior", BEHAVIORS)
@cases("crash_point", [None, *CRASH_POINTS])
def test_config_accepts_every_frozen_choice(
    tmp_path: Path,
    mode: str,
    behavior: str,
    crash_point: str | None,
) -> None:
    config = _config(
        tmp_path,
        mode=mode,
        behavior=behavior,
        crash_point=crash_point,
    )
    assert (config.mode, config.behavior, config.crash_point) == (
        mode,
        behavior,
        crash_point,
    )


def test_config_requires_distinct_database_paths(tmp_path: Path) -> None:
    path = str(tmp_path / "same.sqlite3")
    with pytest.raises(ValueError, match=r"\bside_effect_db\b"):
        _config(tmp_path, outcome_db=path, side_effect_db=path)


@cases(
    "alias",
    [
        "./same.sqlite3",
        "nested/../same.sqlite3",
        "/same.sqlite3",
    ],
)
def test_config_rejects_lexically_aliased_database_paths_without_rendering_them(
    tmp_path: Path,
    alias: str,
) -> None:
    outcome_path = str(tmp_path / "same.sqlite3")
    side_effect_path = f"{tmp_path}/{alias}"

    with pytest.raises(ValueError, match=r"^invalid side_effect_db$") as raised:
        _config(
            tmp_path,
            outcome_db=outcome_path,
            side_effect_db=side_effect_path,
        )

    assert outcome_path not in str(raised.value)
    assert side_effect_path not in str(raised.value)


def test_config_database_alias_check_is_lexical_and_side_effect_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome_path = str(tmp_path / "missing-parent" / "outcome.sqlite3")
    side_effect_path = str(tmp_path / "other-missing-parent" / "effects.sqlite3")

    def reject_filesystem_access(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("unexpected filesystem access")

    with monkeypatch.context() as patch:
        for owner, name in (
            (Path, "resolve"),
            (Path, "stat"),
            (Path, "lstat"),
            (Path, "samefile"),
            (Path, "exists"),
            (Path, "is_file"),
            (Path, "is_dir"),
            (os, "stat"),
            (os, "lstat"),
            (os.path, "realpath"),
            (os.path, "samefile"),
            (os.path, "exists"),
            (os.path, "lexists"),
        ):
            patch.setattr(owner, name, reject_filesystem_access)
        config = _config(
            tmp_path,
            outcome_db=outcome_path,
            side_effect_db=side_effect_path,
        )

    assert config.outcome_db == outcome_path
    assert config.side_effect_db == side_effect_path


def test_parse_args_accepts_only_required_config_path(tmp_path: Path) -> None:
    path = tmp_path / "fixture.json"
    args = parse_args(["--config", str(path)])
    assert vars(args) == {"config": path}


@cases(
    "argv",
    [
        [],
        ["--config"],
        ["--unknown", "secret-argument"],
        ["--config", "one", "--config", "two"],
        ["--config=secret-path", "extra"],
    ],
)
def test_parse_args_rejects_generically_without_output(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(ValueError, match="invalid arguments") as raised:
        parse_args(argv)
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""
    for value in argv:
        assert value not in str(raised.value)


def test_load_native_config_accepts_exact_document(tmp_path: Path) -> None:
    expected = _config(tmp_path, behavior="actuator")
    path = tmp_path / "fixture.json"
    path.write_text(json.dumps(asdict(expected)), encoding="utf-8")
    assert load_native_config(path) == expected


@cases(
    ("name", "contents"),
    [
        ("malformed", b"{"),
        ("bom", b"\xef\xbb\xbf{}"),
        ("invalid-utf8", b"\xff"),
        ("non-object", b"[]"),
        ("nan", b'{"value": NaN}'),
        ("infinity", b'{"value": Infinity}'),
        ("overflow", b'{"value": 1e400}'),
        ("duplicate-root", b'{"run_id": "one", "run_id": "two"}'),
        ("duplicate-nested", b'{"run_id": {"key": 1, "key": 2}}'),
    ],
)
def test_load_native_config_rejects_invalid_json_without_echoing_input(
    tmp_path: Path,
    name: str,
    contents: bytes,
) -> None:
    path = tmp_path / f"{name}.json"
    path.write_bytes(contents)
    with pytest.raises(ValueError) as raised:
        load_native_config(path)
    decoded = contents.decode("utf-8", errors="ignore")
    if decoded:
        assert decoded not in str(raised.value)


def test_load_native_config_normalizes_oversized_integer_error(tmp_path: Path) -> None:
    path = tmp_path / "oversized-integer.json"
    document = json.dumps(asdict(_config(tmp_path)))
    contents = document.replace(
        '"delay_ms": 0',
        f'"delay_ms": {"1" * 5_000}',
    )
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match=r"^invalid config JSON$") as raised:
        load_native_config(path)

    assert "digits" not in str(raised.value)


def test_load_native_config_rejects_unknown_and_missing_keys(tmp_path: Path) -> None:
    document = asdict(_config(tmp_path))
    cases = [
        {**document, "credential": "secret-value"},
        {key: value for key, value in document.items() if key != "run_id"},
    ]
    for index, case in enumerate(cases):
        path = tmp_path / f"keys-{index}.json"
        path.write_text(json.dumps(case), encoding="utf-8")
        with pytest.raises(ValueError, match="config keys") as raised:
            load_native_config(path)
        assert "credential" not in str(raised.value)
        assert "secret-value" not in str(raised.value)


def test_load_native_config_reuses_direct_validation_without_values(
    tmp_path: Path,
) -> None:
    document = asdict(_config(tmp_path))
    document["behavior"] = "secret-invalid-behavior"
    path = tmp_path / "invalid-value.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValueError, match=r"\bbehavior\b") as raised:
        load_native_config(path)
    assert "secret-invalid-behavior" not in str(raised.value)


def test_load_native_config_rejects_missing_symlink_special_and_oversized_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.json"
    target.write_text(json.dumps(asdict(_config(tmp_path))), encoding="utf-8")
    link = tmp_path / "link.json"
    link.symlink_to(target)
    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b" " * 65_537)
    fifo = tmp_path / "config.fifo"
    os.mkfifo(fifo)

    for path in (tmp_path / "missing.json", tmp_path, link, fifo, oversized):
        with pytest.raises(ValueError, match="config file"):
            load_native_config(path)


class _GuardedEnvironment(dict[str, str]):
    def __init__(self, values: dict[str, str], forbidden: str) -> None:
        super().__init__(values)
        self.forbidden = forbidden
        self.reads: list[str] = []

    def __getitem__(self, key: str) -> str:
        if key == self.forbidden:
            raise AssertionError(f"read forbidden key {key}")
        self.reads.append(key)
        return super().__getitem__(key)


@cases(
    ("mode", "selected", "forbidden", "url"),
    [
        ("central-relay", "RELAY_URL", "NATS_URL", "https://relay.local/events"),
        ("core-only", "NATS_URL", "RELAY_URL", "nats://nats.local:4222"),
        ("edgecitadel", "NATS_URL", "RELAY_URL", "nats://nats.local:4222"),
        ("all-durable", "NATS_URL", "RELAY_URL", "nats://nats.local:4222"),
        (
            "core-only",
            "NATS_URL",
            "RELAY_URL",
            "nats://nats.local/path%20segment",
        ),
    ],
)
def test_runtime_endpoints_reads_only_mode_specific_value(
    tmp_path: Path,
    mode: str,
    selected: str,
    forbidden: str,
    url: str,
) -> None:
    environ = _GuardedEnvironment({selected: url, forbidden: "secret"}, forbidden)
    assert runtime_endpoints(_config(tmp_path, mode=mode), environ) == {selected: url}
    assert environ.reads == [selected]


@cases(
    ("mode", "key", "value"),
    [
        ("central-relay", "RELAY_URL", ""),
        ("central-relay", "RELAY_URL", "relay.local"),
        ("central-relay", "RELAY_URL", "nats://relay.local"),
        ("central-relay", "RELAY_URL", "https://user:pass@relay.local"),
        ("central-relay", "RELAY_URL", "https://relay.local?secret=yes"),
        ("central-relay", "RELAY_URL", "https://relay.local#secret"),
        ("core-only", "NATS_URL", "https://nats.local"),
        ("core-only", "NATS_URL", "nats:///missing-host"),
        ("core-only", "NATS_URL", "nats://token@nats.local"),
        ("core-only", "NATS_URL", "nats://nats.local?secret=yes"),
        ("core-only", "NATS_URL", "nats://nats.local#secret"),
        ("core-only", "NATS_URL", " nats://nats.local"),
        ("core-only", "NATS_URL", "nats://nats.local "),
        ("core-only", "NATS_URL", "nats://nats.local/path with space"),
        ("core-only", "NATS_URL", "\u2003nats://nats.local"),
        ("core-only", "NATS_URL", "nats://nats.local\u2003"),
        ("core-only", "NATS_URL", "nats://nats.local/path\u2003segment"),
    ],
)
def test_runtime_endpoints_rejects_invalid_url_without_rendering_it(
    tmp_path: Path,
    mode: str,
    key: str,
    value: str,
) -> None:
    with pytest.raises(ValueError, match=key) as raised:
        runtime_endpoints(_config(tmp_path, mode=mode), {key: value})
    if value:
        assert value not in str(raised.value)


def test_runtime_endpoints_rejects_missing_value_generically(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="RELAY_URL"):
        runtime_endpoints(_config(tmp_path, mode="central-relay"), {})


@cases("mode", [0o600, 0o400])
def test_read_transport_token_accepts_private_exact_file(
    tmp_path: Path,
    mode: int,
) -> None:
    token = "a1" * 32
    path = tmp_path / f"token-{mode:o}"
    path.write_bytes(f"{token}\n".encode())
    path.chmod(mode)
    assert read_transport_token(path) == token


@cases("mode", [0o000, 0o200, 0o440, 0o604, 0o700])
def test_read_transport_token_rejects_nonprivate_mode(
    tmp_path: Path,
    mode: int,
) -> None:
    path = tmp_path / f"bad-mode-{mode:o}"
    path.write_bytes(b"a" * 64 + b"\n")
    path.chmod(mode)
    with pytest.raises(ValueError, match="credential file"):
        read_transport_token(path)


@cases(
    "contents",
    [
        b"",
        b"a" * 63 + b"\n",
        b"a" * 64,
        b"A" * 64 + b"\n",
        b"g" * 64 + b"\n",
        b"a" * 64 + b"\r\n",
        b"a" * 64 + b"\nextra\n",
    ],
)
def test_read_transport_token_rejects_malformed_content_without_echoing_it(
    tmp_path: Path,
    contents: bytes,
) -> None:
    path = tmp_path / "bad-token"
    path.write_bytes(contents)
    path.chmod(0o600)
    with pytest.raises(ValueError, match="credential file") as raised:
        read_transport_token(path)
    decoded = contents.decode("ascii", errors="ignore")
    if decoded:
        assert decoded not in str(raised.value)


def test_read_transport_token_rejects_missing_symlink_and_special_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"a" * 64 + b"\n")
    target.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(target)
    fifo = tmp_path / "token.fifo"
    os.mkfifo(fifo)
    for path in (tmp_path / "missing", link, fifo, tmp_path):
        with pytest.raises(ValueError, match="credential file"):
            read_transport_token(path)


def test_read_transport_token_rejects_replace_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "token"
    replacement = tmp_path / "replacement"
    backup = tmp_path / "backup"
    path.write_bytes(b"a" * 64 + b"\n")
    replacement.write_bytes(b"b" * 64 + b"\n")
    path.chmod(0o600)
    replacement.chmod(0o600)
    real_open = os.open

    def replacing_open(target: str | os.PathLike[str], flags: int) -> int:
        path.rename(backup)
        replacement.rename(path)
        return real_open(target, flags)

    monkeypatch.setattr(os, "open", replacing_open)
    with pytest.raises(ValueError, match="credential file"):
        read_transport_token(path)


def test_runtime_inputs_never_disclose_token_or_print_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    token = "deadbeef" * 8
    config = _config(tmp_path)
    assert token not in repr(config)
    assert token not in json.dumps(build_agent_card(config), sort_keys=True)

    errors: list[str] = []
    with pytest.raises(ValueError) as argument_error:
        parse_args(["--unknown", token])
    errors.append(str(argument_error.value))
    with pytest.raises(ValueError) as endpoint_error:
        runtime_endpoints(
            config,
            {"NATS_URL": f"nats://{token}@nats.local"},
        )
    errors.append(str(endpoint_error.value))
    malformed_token = tmp_path / "malformed-token"
    malformed_token.write_bytes(f"{token}extra\n".encode())
    malformed_token.chmod(0o600)
    with pytest.raises(ValueError) as token_error:
        read_transport_token(malformed_token)
    errors.append(str(token_error.value))

    captured = capsys.readouterr()
    assert captured.out == captured.err == ""
    assert all(token not in error for error in errors)


def test_build_agent_card_is_schema_valid_stable_native_worker(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    card = build_agent_card(config)
    schema_path = Path(__file__).resolve().parents[2] / "schemas/agent-card.v1.json"
    Draft202012Validator(json.loads(schema_path.read_text())).validate(card)

    assert card["name"] == config.agent_id
    metadata = cast(dict[str, object], card["metadata"])
    assert metadata == {
        "runtime.kind": "native",
        "runtime.roles": ["worker"],
        "runtime.conformance": "L1",
        "runtime.heartbeat_interval_sec": 10,
    }
    capabilities = cast(dict[str, object], card["capabilities"])
    assert capabilities["streaming"] is True
    extensions = cast(list[dict[str, object]], capabilities["extensions"])
    assert extensions == [
        {
            "uri": "https://edgecitadel.local/ext/nats-binding/v1",
            "description": "NATS transport binding.",
            "required": True,
            "params": {"subject_prefix": f"agents.{config.agent_id}"},
        }
    ]


def test_build_agent_card_is_fresh_and_uses_only_agent_id(
    tmp_path: Path,
) -> None:
    first_config = _config(tmp_path)
    second_config = _config(
        tmp_path,
        run_id="different-run",
        mode="all-durable",
        behavior="actuator",
        delay_ms=31337,
        crash_point="after-side-effect-before-ledger-prepare",
        outcome_db=str(tmp_path / "private-outcome-sentinel.sqlite3"),
        side_effect_db=str(tmp_path / "private-effect-sentinel.sqlite3"),
    )
    first = build_agent_card(first_config)
    cast(dict[str, object], first["metadata"])["runtime.kind"] = "changed"
    second = build_agent_card(second_config)
    assert cast(dict[str, object], second["metadata"])["runtime.kind"] == "native"
    assert second == build_agent_card(first_config)

    serialized = json.dumps(second, sort_keys=True)
    for forbidden in (
        second_config.run_id,
        second_config.mode,
        second_config.behavior,
        str(second_config.delay_ms),
        cast(str, second_config.crash_point),
        second_config.outcome_db,
        second_config.side_effect_db,
        "RELAY_URL",
        "NATS_URL",
        "EC_CREDENTIAL_FILE",
        "transport-token-sentinel",
    ):
        assert forbidden not in serialized


@async_test
async def test_echo_result_preserves_correlation(tmp_path: Path) -> None:
    config = _config(tmp_path, delay_ms=25)
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(PROGRESS_ID, TERMINAL_ID)
    crash = _CrashHook()
    sleep = _SleepRecorder()
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=sleep,
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )
    delivery = _Delivery(_command())

    result = await executor.execute(delivery)

    assert result.classification == "completed"
    assert sleep.delays == [0.025]
    assert len(transport.progress) == 1
    progress = transport.progress[0]
    assert progress["id"] == PROGRESS_ID
    assert progress["sender_id"] == "worker-1"
    assert progress["recipient_id"] == "requester-1"
    assert progress["task_id"] == TASK_ID
    assert progress["context_id"] == TASK_ID
    assert progress["hop_count"] == 0
    assert progress["task_state"] == "working"
    assert progress["payload"] == {"message": "working"}
    assert len(transport.terminals) == 1
    terminal = transport.terminals[0]
    assert terminal["id"] == TERMINAL_ID
    assert terminal["sender_id"] == "worker-1"
    assert terminal["recipient_id"] == "requester-1"
    assert terminal["task_id"] == TASK_ID
    assert terminal["context_id"] == TASK_ID
    assert terminal["hop_count"] == 0
    assert terminal["task_state"] == "completed"
    assert terminal["payload"] == {"body": "edgecitadel:nonce"}
    assert delivery.commits == 1
    native_events = [
        event for event in sink.events if event["component"] == "native_control"
    ]
    assert [event["event"] for event in native_events] == ["fixture.handler_started"]
    assert "nonce" not in json.dumps(native_events)


@async_test
async def test_progress_frames_preserve_correlation(tmp_path: Path) -> None:
    config = _config(tmp_path, behavior="progress", delay_ms=999)
    timeline: list[tuple[str, object]] = []
    transport = _RecordingTransport(timeline)
    sink = _EventSink()
    clock = _Clock()
    progress_ids = [f"60000000-0000-4000-8000-{index:012x}" for index in range(1, 21)]
    uuids = _UUIDs(*progress_ids, TERMINAL_ID)
    crash = _CrashHook()
    sleep = _SleepRecorder(timeline)
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=sleep,
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    result = await executor.execute(_Delivery(_command()))

    assert result.classification == "completed"
    assert len(transport.progress) == 20
    assert sleep.delays == [0.05] * 19
    assert timeline[0] == ("progress", progress_ids[0])
    assert timeline[1:] == [
        item
        for index in range(1, 20)
        for item in (("sleep", 0.05), ("progress", progress_ids[index]))
    ]
    for index, progress in enumerate(transport.progress, start=1):
        payload = cast(Mapping[str, object], progress["payload"])
        message = cast(str, payload["message"])
        assert message.isascii()
        assert len(message.encode("ascii")) == 256
        assert payload["progress"] == index * 5
        assert progress["sender_id"] == "worker-1"
        assert progress["recipient_id"] == "requester-1"
        assert progress["task_id"] == TASK_ID
        assert progress["context_id"] == TASK_ID
        assert progress["hop_count"] == 0
        assert progress["task_state"] == "working"
    assert len(transport.terminals) == 1
    terminal = transport.terminals[0]
    assert terminal["task_state"] == "completed"
    assert terminal["payload"] == {"body": "edgecitadel:nonce"}


@async_test
async def test_delegate_submits_one_serial_child_with_parent_lineage(
    tmp_path: Path,
) -> None:
    child_task_id = "70000000-0000-4000-8000-000000000001"
    child_wire_id = "70000000-0000-4000-8000-000000000002"
    parent_progress_id = "70000000-0000-4000-8000-000000000003"
    parent_terminal_id = "70000000-0000-4000-8000-000000000004"
    child_progress_id = "70000000-0000-4000-8000-000000000005"
    child_terminal_id = "70000000-0000-4000-8000-000000000006"
    config = _config(tmp_path, behavior="delegate", delay_ms=10)
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(
        parent_progress_id,
        child_task_id,
        child_wire_id,
        parent_terminal_id,
        child_progress_id,
        child_terminal_id,
    )
    crash = _CrashHook()
    sleep = _SleepRecorder()
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=sleep,
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    parent = await executor.execute(_Delivery(_command()))

    assert parent.classification == "completed"
    assert len(transport.submissions) == 1
    child = transport.submissions[0]
    assert child == {
        "v": 1,
        "id": child_wire_id,
        "type": "delegation",
        "sender_id": "requester-1",
        "recipient_id": "worker-1",
        "task_id": child_task_id,
        "context_id": TASK_ID,
        "hop_count": 1,
        "timestamp": NOW,
        "payload": {"body": "nonce", "parent_task_id": TASK_ID},
    }
    assert transport.terminals[0]["payload"] == {
        "body": "edgecitadel:nonce",
        "child_task_id": child_task_id,
    }

    child_result = await executor.execute(_Delivery(child))

    assert child_result.classification == "completed"
    assert len(transport.submissions) == 1
    assert sleep.delays == [0.01, 0.01]
    assert len(transport.progress) == 2
    child_progress = transport.progress[1]
    assert child_progress["recipient_id"] == "requester-1"
    assert child_progress["task_id"] == child_task_id
    assert child_progress["context_id"] == TASK_ID
    assert child_progress["hop_count"] == 1
    assert transport.terminals[1]["payload"] == {
        "body": "edgecitadel:nonce",
        "parent_task_id": TASK_ID,
    }
    native_events = [
        event for event in sink.events if event["component"] == "native_control"
    ]
    assert [event["event"] for event in native_events] == [
        "fixture.handler_started",
        "fixture.delegation_created",
        "fixture.handler_started",
    ]


def _rejected_receipt(envelope: Mapping[str, object]) -> PublicationReceipt:
    accepted = _RecordingTransport._receipt(envelope)
    return PublicationReceipt(
        envelope_id=accepted.envelope_id,
        accepted=False,
        transport=accepted.transport,
        stream=accepted.stream,
        stream_sequence=accepted.stream_sequence,
        duplicate=accepted.duplicate,
        accepted_ns=accepted.accepted_ns,
        application_bytes=accepted.application_bytes,
        wire_bytes=accepted.wire_bytes,
    )


@async_test
async def test_rejected_progress_receipt_becomes_generic_handler_failure(
    tmp_path: Path,
) -> None:
    class RejectingProgressTransport(_RecordingTransport):
        async def publish_progress(
            self,
            envelope: Mapping[str, object],
        ) -> PublicationReceipt:
            self.progress.append(envelope)
            return _rejected_receipt(envelope)

    config = _config(tmp_path, delay_ms=10)
    transport = RejectingProgressTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(PROGRESS_ID, TERMINAL_ID)
    crash = _CrashHook()
    sleep = _SleepRecorder()
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=native_control._NativeHandler(
            config=config,
            transport=transport,
            event_sink=sink,
            clock=clock,
            uuid_factory=uuids,
            crash_hook=crash,
            sleep=sleep,
        ),
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    result = await executor.execute(_Delivery(_command()))

    assert result.classification == "failed"
    assert sleep.delays == []
    assert len(transport.progress) == 1
    assert transport.terminals[0]["payload"] == {"error": "handler_failed"}


@async_test
async def test_rejected_delegation_receipt_becomes_generic_handler_failure(
    tmp_path: Path,
) -> None:
    class RejectingDelegationTransport(_RecordingTransport):
        async def submit_task(
            self,
            envelope: Mapping[str, object],
        ) -> PublicationReceipt:
            self.submissions.append(envelope)
            return _rejected_receipt(envelope)

    config = _config(tmp_path, behavior="delegate", delay_ms=10)
    transport = RejectingDelegationTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(
        "71000000-0000-4000-8000-000000000001",
        "71000000-0000-4000-8000-000000000002",
        "71000000-0000-4000-8000-000000000003",
        "71000000-0000-4000-8000-000000000004",
    )
    crash = _CrashHook()
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=native_control._NativeHandler(
            config=config,
            transport=transport,
            event_sink=sink,
            clock=clock,
            uuid_factory=uuids,
            crash_hook=crash,
            sleep=_SleepRecorder(),
        ),
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    result = await executor.execute(_Delivery(_command()))

    assert result.classification == "failed"
    assert len(transport.submissions) == 1
    assert transport.terminals[0]["payload"] == {"error": "handler_failed"}
    assert "child_task_id" not in cast(
        Mapping[str, object],
        transport.terminals[0]["payload"],
    )


@async_test
async def test_actuator_persists_full_attempts_and_effects_with_counts(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, behavior="actuator", delay_ms=5)
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(
        "80000000-0000-4000-8000-000000000001",
        "80000000-0000-4000-8000-000000000002",
        "80000000-0000-4000-8000-000000000003",
        "80000000-0000-4000-8000-000000000004",
    )
    crash = _CrashHook()
    sleep = _SleepRecorder()
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=sleep,
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    first = await executor.execute(_Delivery(_command()))
    second = await executor.execute(_Delivery(_command()))

    assert first.classification == second.classification == "completed"
    assert sleep.delays == [0.005, 0.005]
    assert not Path(config.outcome_db).exists()
    side_effect_path = Path(config.side_effect_db)
    assert side_effect_path.stat().st_mode & 0o777 == 0o600
    connection = sqlite3.connect(side_effect_path)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        attempts = connection.execute(
            """
            SELECT attempt_id, task_id, wire_id, started_monotonic_ns
            FROM execution_attempts ORDER BY attempt_id
            """
        ).fetchall()
        effects = connection.execute(
            """
            SELECT side_effect_id, attempt_id, task_id, body_sha256,
                   committed_monotonic_ns
            FROM external_side_effects ORDER BY side_effect_id
            """
        ).fetchall()
    finally:
        connection.close()
    assert len(attempts) == len(effects) == 2
    assert [row[1:3] for row in attempts] == [(TASK_ID, WIRE_ID)] * 2
    assert [row[1] for row in effects] == [row[0] for row in attempts]
    assert [row[2] for row in effects] == [TASK_ID, TASK_ID]
    expected_hash = hashlib.sha256(canonical_json("nonce")).hexdigest()
    assert [row[3] for row in effects] == [expected_hash, expected_hash]
    assert all(type(row[3]) is int and row[3] > 0 for row in attempts)
    assert all(type(row[4]) is int and row[4] > 0 for row in effects)
    assert transport.terminals[0]["payload"] == {
        "body": "edgecitadel:nonce",
        "attempt_count": 1,
        "side_effect_count": 1,
    }
    assert transport.terminals[1]["payload"] == {
        "body": "edgecitadel:nonce",
        "attempt_count": 2,
        "side_effect_count": 2,
    }
    native_event_names = [
        event["event"]
        for event in sink.events
        if event["component"] == "native_control"
    ]
    assert native_event_names == [
        "fixture.handler_started",
        "fixture.actuator_attempt_committed",
        "fixture.side_effect_committed",
        "fixture.handler_started",
        "fixture.actuator_attempt_committed",
        "fixture.side_effect_committed",
    ]


def test_actuator_connection_asserts_required_pragmas(tmp_path: Path) -> None:
    path = tmp_path / "effects.sqlite3"
    connection = native_control._open_actuator_database(path)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute("PRAGMA synchronous").fetchone() == (2,)
        assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
        assert connection.execute("PRAGMA busy_timeout").fetchone() == (5000,)
    finally:
        connection.close()


@async_test
async def test_actuator_crash_boundary_follows_durable_effect(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        behavior="actuator",
        crash_point="after-side-effect-before-ledger-prepare",
    )
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs("90000000-0000-4000-8000-000000000001")
    crash = _CrashHook("after-side-effect-before-ledger-prepare")
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=_SleepRecorder(),
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    with pytest.raises(
        InjectedCrash,
        match="after-side-effect-before-ledger-prepare",
    ):
        await executor.execute(_Delivery(_command()))

    connection = sqlite3.connect(config.side_effect_db)
    try:
        attempt_count = connection.execute(
            "SELECT COUNT(*) FROM execution_attempts"
        ).fetchone()
        effect_count = connection.execute(
            "SELECT COUNT(*) FROM external_side_effects"
        ).fetchone()
    finally:
        connection.close()
    assert attempt_count == (1,)
    assert effect_count == (1,)
    assert transport.terminals == []
    assert crash.hits[-1] == "after-side-effect-before-ledger-prepare"
    assert [
        event["event"]
        for event in sink.events
        if event["component"] == "native_control"
    ] == [
        "fixture.handler_started",
        "fixture.actuator_attempt_committed",
        "fixture.side_effect_committed",
    ]


def test_native_policy_accepts_only_direct_and_one_hop_delegation() -> None:
    policy = native_control._NativePolicy()
    direct = {**_command(), "context_id": TASK_ID, "hop_count": 0}
    child = {
        **direct,
        "type": "delegation",
        "hop_count": 1,
        "payload": {"body": "nonce", "parent_task_id": TASK_ID},
    }
    too_deep = {**child, "hop_count": 2}
    cancel = {**direct, "type": "cancel", "payload": {}}

    assert policy.evaluate(direct, "worker-1").accepted is True
    assert policy.evaluate(child, "worker-1").accepted is True
    hop_decision = policy.evaluate(too_deep, "worker-1")
    assert (hop_decision.accepted, hop_decision.reason) == (False, "hop_limit")
    cancel_decision = policy.evaluate(cancel, "worker-1")
    assert (cancel_decision.accepted, cancel_decision.reason) == (
        False,
        "cancel_not_supported",
    )


@async_test
async def test_non_string_body_is_converted_by_executor_without_progress(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(TERMINAL_ID)
    crash = _CrashHook()
    handler = native_control._NativeHandler(
        config=config,
        transport=transport,
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
        sleep=_SleepRecorder(),
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )

    result = await executor.execute(_Delivery(_command(body={"secret": "value"})))

    assert result.classification == "failed"
    assert transport.progress == []
    assert transport.terminals[0]["payload"] == {"error": "handler_failed"}
    assert "during-handler-exception-conversion" in crash.hits
    assert "secret" not in json.dumps(sink.events)
    assert "value" not in json.dumps(sink.events)


def test_configured_crash_hook_exits_only_at_exact_point() -> None:
    exits: list[int] = []
    hook = native_control._ConfiguredCrashHook(
        "after-result-publish-before-publish-mark",
        exit_process=exits.append,
    )
    hook.hit("after-receive-before-handler")
    assert exits == []
    hook.hit("after-result-publish-before-publish-mark")
    assert exits == [86]


@async_test
@cases("crash_point", CRASH_POINTS)
async def test_crash_boundaries_have_exact_executor_vs_actuator_ownership(
    tmp_path: Path,
    crash_point: str,
) -> None:
    actuator_point = "after-side-effect-before-ledger-prepare"
    behavior = "actuator" if crash_point == actuator_point else "echo"
    body: object = (
        {"invalid": "body"}
        if crash_point == "during-handler-exception-conversion"
        else "nonce"
    )
    config = _config(
        tmp_path,
        behavior=behavior,
        crash_point=crash_point,
    )
    transport = _RecordingTransport()
    sink = _EventSink()
    clock = _Clock()
    uuids = _UUIDs(*[f"c0000000-0000-4000-8000-{index:012x}" for index in range(1, 9)])
    crash = _CrashHook(crash_point)
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=native_control._NativeHandler(
            config=config,
            transport=transport,
            event_sink=sink,
            clock=clock,
            uuid_factory=uuids,
            crash_hook=crash,
            sleep=_SleepRecorder(),
        ),
        outcome_store=DisabledOutcomeStore(),
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=native_control._NativePolicy(),
        event_sink=sink,
        clock=clock,
        uuid_factory=uuids,
        crash_hook=crash,
    )
    delivery = _Delivery(_command(body=body))

    with pytest.raises(InjectedCrash, match=crash_point):
        await executor.execute(delivery)

    assert crash.hits.count(crash_point) == 1
    assert (actuator_point in crash.hits) is (crash_point == actuator_point)
    assert delivery.commits == 0
    native_events = [
        event["event"]
        for event in sink.events
        if event["component"] == "native_control"
    ]
    assert ("fixture.side_effect_committed" in native_events) is (
        crash_point == actuator_point
    )


def test_handler_owns_only_side_effect_crash_call() -> None:
    handler_source = inspect.getsource(native_control._NativeHandler)
    executor_source = inspect.getsource(TaskExecutor)
    side_effect_point = "after-side-effect-before-ledger-prepare"
    executor_points = set(CRASH_POINTS) - {side_effect_point}
    assert handler_source.count(f'"{side_effect_point}"') == 1
    assert all(point not in handler_source for point in executor_points)
    assert side_effect_point not in executor_source
    assert all(point in executor_source for point in executor_points)


@async_test
@cases("ledger_enabled", [False, True])
async def test_run_fixture_orders_readiness_and_closes_only_owned_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ledger_enabled: bool,
) -> None:
    config = _config(tmp_path)
    transport = _LifecycleTransport(outcome_ledger_enabled=ledger_enabled)
    sink = _EventSink()
    store = _ClosableStore(ledger_enabled)
    selected: list[tuple[str, object]] = []
    heartbeat_started = asyncio.Event()

    def disabled_store() -> _ClosableStore:
        selected.append(("disabled", None))
        return store

    def sqlite_store(path: Path) -> _ClosableStore:
        selected.append(("sqlite", path))
        return store

    async def dormant_heartbeat(*args: object, **kwargs: object) -> None:
        del args, kwargs
        transport.lifecycle.append(("heartbeat_loop", None))
        heartbeat_started.set()
        await asyncio.Future()

    monkeypatch.setattr(
        native_control,
        "DisabledOutcomeStore",
        disabled_store,
        raising=False,
    )
    monkeypatch.setattr(
        native_control,
        "SQLiteOutcomeStore",
        sqlite_store,
        raising=False,
    )
    monkeypatch.setattr(
        native_control,
        "_heartbeat_loop",
        dormant_heartbeat,
        raising=False,
    )

    fixture_task = asyncio.create_task(
        run_fixture(config, cast(TaskTransport, transport), sink)
    )
    await asyncio.wait_for(heartbeat_started.wait(), timeout=1)
    fixture_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fixture_task

    expected_selection = (
        [("sqlite", Path(config.outcome_db))]
        if ledger_enabled
        else [("disabled", None)]
    )
    assert selected == expected_selection
    assert store.close_count == 1
    assert transport.lifecycle == [
        ("start", config.agent_id),
        ("ready", (config.agent_id, 5.0)),
        ("heartbeat_loop", None),
    ]
    assert transport.closed == 0
    assert not Path(config.outcome_db).exists()
    assert transport.executor is not None
    executor = transport.executor
    handler = cast(native_control._NativeHandler, executor._handler)
    assert cast(object, executor._terminal_publisher) is transport
    assert cast(object, executor._progress_publisher) is transport
    assert handler._transport is transport
    assert handler._clock is executor._clock
    assert handler._uuid_factory is executor._uuid_factory
    assert handler._crash_hook is executor._crash_hook
    native_events = [
        event for event in sink.events if event["component"] == "native_control"
    ]
    assert [event["event"] for event in native_events] == ["fixture.ready"]
    assert native_events[0]["data"] == {"agent_id": config.agent_id}


@async_test
async def test_run_fixture_rejects_mode_mismatch_before_side_effects(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, mode="edgecitadel")
    transport = _LifecycleTransport(mode=Mode.CORE_ONLY)
    sink = _EventSink()

    with pytest.raises(ValueError, match="transport mode"):
        await run_fixture(config, cast(TaskTransport, transport), sink)

    assert transport.lifecycle == []
    assert sink.events == []
    assert not Path(config.outcome_db).exists()
    assert not Path(config.side_effect_db).exists()


@async_test
@cases("failure_stage", ["receiver", "readiness", "heartbeat"])
async def test_run_fixture_closes_store_on_startup_and_heartbeat_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    class FailingTransport(_LifecycleTransport):
        async def start_receiver(
            self,
            agent_id: str,
            executor: TaskExecutor,
        ) -> None:
            await super().start_receiver(agent_id, executor)
            if failure_stage == "receiver":
                raise RuntimeError("receiver failure")

        async def wait_receiver_ready(
            self,
            agent_id: str,
            timeout_s: float,
        ) -> None:
            await super().wait_receiver_ready(agent_id, timeout_s)
            if failure_stage == "readiness":
                raise RuntimeError("readiness failure")

    async def failing_heartbeat(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("heartbeat failure")

    config = _config(tmp_path)
    transport = FailingTransport()
    sink = _EventSink()
    store = _ClosableStore(False)
    monkeypatch.setattr(
        native_control,
        "DisabledOutcomeStore",
        lambda: store,
    )
    if failure_stage == "heartbeat":
        monkeypatch.setattr(native_control, "_heartbeat_loop", failing_heartbeat)

    with pytest.raises(RuntimeError, match=failure_stage):
        await run_fixture(config, cast(TaskTransport, transport), sink)

    assert store.close_count == 1
    assert transport.closed == 0
    ready_events = [
        event
        for event in sink.events
        if event["component"] == "native_control" and event["event"] == "fixture.ready"
    ]
    assert len(ready_events) == (1 if failure_stage == "heartbeat" else 0)


class _DeadlineClock:
    def __init__(self) -> None:
        self.now_ns = 0

    def monotonic_ns(self) -> int:
        return self.now_ns

    def now_iso(self) -> str:
        return NOW


class _StopHeartbeats(RuntimeError):
    pass


class _HeartbeatTransport:
    def __init__(self, *, accepted: bool = True, stop_after: int | None = None) -> None:
        self.accepted = accepted
        self.stop_after = stop_after
        self.envelopes: list[Mapping[str, object]] = []

    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.envelopes.append(envelope)
        if self.stop_after == len(self.envelopes):
            raise _StopHeartbeats
        receipt = _RecordingTransport._receipt(envelope)
        if self.accepted:
            return receipt
        return PublicationReceipt(
            envelope_id=receipt.envelope_id,
            accepted=False,
            transport=receipt.transport,
            stream=receipt.stream,
            stream_sequence=receipt.stream_sequence,
            duplicate=receipt.duplicate,
            accepted_ns=receipt.accepted_ns,
            application_bytes=receipt.application_bytes,
            wire_bytes=receipt.wire_bytes,
        )


@async_test
async def test_heartbeat_loop_uses_monotonic_deadlines_and_canonical_envelopes(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    clock = _DeadlineClock()
    delays: list[float] = []
    transport = _HeartbeatTransport(stop_after=3)
    heartbeat_ids = [f"a0000000-0000-4000-8000-{index:012x}" for index in range(1, 4)]

    async def advance(delay: float) -> None:
        delays.append(delay)
        clock.now_ns += round(delay * 1_000_000_000)

    with pytest.raises(_StopHeartbeats):
        await native_control._heartbeat_loop(
            config,
            transport,
            clock,
            _UUIDs(*heartbeat_ids),
            sleep=advance,
        )

    assert delays == [1.0, 1.0, 1.0]
    assert transport.envelopes == [
        {
            "v": 1,
            "id": heartbeat_id,
            "type": "heartbeat",
            "sender_id": config.agent_id,
            "timestamp": NOW,
            "payload": {},
        }
        for heartbeat_id in heartbeat_ids
    ]


@async_test
async def test_heartbeat_false_receipt_fails_fixture(tmp_path: Path) -> None:
    config = _config(tmp_path)
    clock = _DeadlineClock()

    async def advance(delay: float) -> None:
        clock.now_ns += round(delay * 1_000_000_000)

    with pytest.raises(RuntimeError, match="heartbeat publication failed"):
        await native_control._heartbeat_loop(
            config,
            _HeartbeatTransport(accepted=False),
            clock,
            _UUIDs("b0000000-0000-4000-8000-000000000001"),
            sleep=advance,
        )
