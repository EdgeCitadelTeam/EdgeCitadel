"""Transport-independent deterministic native control fixture."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import math
import os
import re
import sqlite3
import stat
import sys
import time
import urllib.parse
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, cast

from adapters._common.outcome_store import DisabledOutcomeStore, SQLiteOutcomeStore
from adapters._common.task_executor import (
    Clock,
    CrashHook,
    ExecutionContext,
    PolicyDecision,
    TaskExecutor,
    UUIDFactory,
)
from adapters._common.task_publisher import EventSink
from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import canonical_json, default_validator
from scripts.research.modes.base import Mode, TaskTransport

BEHAVIORS = ("echo", "delegate", "progress", "actuator")
CRASH_POINTS = (
    "after-receive-before-handler",
    "after-side-effect-before-ledger-prepare",
    "after-ledger-prepare-before-result-publish",
    "after-result-publish-before-publish-mark",
    "after-publish-mark-before-inbound-commit",
    "during-handler-exception-conversion",
)

_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_MODES = tuple(mode.value for mode in Mode)
_CONFIG_KEYS = (
    "run_id",
    "agent_id",
    "mode",
    "behavior",
    "delay_ms",
    "crash_point",
    "heartbeat_interval_ms",
    "outcome_db",
    "side_effect_db",
)
_MAX_CONFIG_BYTES = 65_536
_PROGRESS_MESSAGE = "x" * 256
_ACTUATOR_SCHEMA_VERSION = 1
_ACTUATOR_BUSY_TIMEOUT_MS = 5_000
_ACTUATOR_ATTEMPTS_SCHEMA = """
CREATE TABLE execution_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    wire_id TEXT NOT NULL,
    started_monotonic_ns INTEGER NOT NULL
)
""".strip()
_ACTUATOR_EFFECTS_SCHEMA = """
CREATE TABLE external_side_effects (
    side_effect_id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id INTEGER NOT NULL,
    task_id TEXT NOT NULL,
    body_sha256 TEXT NOT NULL,
    committed_monotonic_ns INTEGER NOT NULL,
    FOREIGN KEY (attempt_id)
        REFERENCES execution_attempts (attempt_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
)
""".strip()
_ACTUATOR_SCHEMAS = {
    "execution_attempts": _ACTUATOR_ATTEMPTS_SCHEMA,
    "external_side_effects": _ACTUATOR_EFFECTS_SCHEMA,
}


class _InvalidJSON(ValueError):
    pass


class _DuplicateKey(ValueError):
    pass


class _SilentEventSink:
    def emit(self, event: Mapping[str, object]) -> None:
        del event


class _FileEventSink:
    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("invalid event log")
        self._path = path

    def emit(self, event: Mapping[str, object]) -> None:
        encoded = canonical_json(event) + b"\n"
        descriptor = os.open(
            self._path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@dataclass(frozen=True)
class NativeControlConfig:
    run_id: str
    agent_id: str
    mode: str
    behavior: str
    delay_ms: int
    crash_point: str | None
    heartbeat_interval_ms: int
    outcome_db: str
    side_effect_db: str

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or _ID_PATTERN.fullmatch(self.run_id) is None:
            raise ValueError("invalid run_id")
        if (
            type(self.agent_id) is not str
            or _ID_PATTERN.fullmatch(self.agent_id) is None
        ):
            raise ValueError("invalid agent_id")
        if type(self.mode) is not str or self.mode not in _MODES:
            raise ValueError("invalid mode")
        if type(self.behavior) is not str or self.behavior not in BEHAVIORS:
            raise ValueError("invalid behavior")
        if type(self.delay_ms) is not int or self.delay_ms < 0:
            raise ValueError("invalid delay_ms")
        if self.crash_point is not None and (
            type(self.crash_point) is not str or self.crash_point not in CRASH_POINTS
        ):
            raise ValueError("invalid crash_point")
        if (
            type(self.heartbeat_interval_ms) is not int
            or self.heartbeat_interval_ms != 1000
        ):
            raise ValueError("invalid heartbeat_interval_ms")
        if not _valid_database_path(self.outcome_db):
            raise ValueError("invalid outcome_db")
        if not _valid_database_path(self.side_effect_db):
            raise ValueError("invalid side_effect_db")
        if os.path.normcase(os.path.normpath(self.side_effect_db)) == os.path.normcase(
            os.path.normpath(self.outcome_db)
        ):
            raise ValueError("invalid side_effect_db")


def _valid_database_path(value: object) -> bool:
    return (
        type(value) is str
        and bool(value)
        and "\0" not in value
        and Path(value).is_absolute()
    )


class _NativePolicy:
    def evaluate(
        self,
        envelope: Mapping[str, object],
        worker_agent_id: str,
    ) -> PolicyDecision:
        del worker_agent_id
        envelope_type = envelope.get("type")
        hop_count = envelope.get("hop_count")
        if envelope_type == "cancel":
            return PolicyDecision(False, "cancel_not_supported")
        if envelope_type == "command" and hop_count == 0:
            return PolicyDecision(True, None)
        if envelope_type == "delegation" and hop_count == 1:
            return PolicyDecision(True, None)
        if envelope_type in ("command", "delegation"):
            return PolicyDecision(False, "hop_limit")
        return PolicyDecision(False, "unsupported_type")


class _ConfiguredCrashHook:
    def __init__(
        self,
        crash_point: str | None,
        *,
        exit_process: Callable[[int], object] = os._exit,
    ) -> None:
        self._crash_point = crash_point
        self._exit_process = exit_process

    def hit(self, point: str) -> None:
        if point == self._crash_point:
            self._exit_process(86)


class _TaskSubmitter(Protocol):
    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...


class _HeartbeatPublisher(Protocol):
    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...


TransportFactory = Callable[
    [NativeControlConfig, Mapping[str, str], str, EventSink],
    TaskTransport,
]


class _SystemClock:
    def monotonic_ns(self) -> int:
        return time.perf_counter_ns()

    def now_iso(self) -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )


class _SystemUUIDFactory:
    def uuid4(self) -> str:
        return str(uuid.uuid4()).lower()


def _emit_native_event(
    event_sink: EventSink,
    clock: Clock,
    event: str,
    data: Mapping[str, object],
) -> None:
    event_sink.emit(
        {
            "monotonic_ns": clock.monotonic_ns(),
            "epoch_time": clock.now_iso(),
            "component": "native_control",
            "event": event,
            "data": dict(data),
        }
    )


class _NativeHandler:
    def __init__(
        self,
        *,
        config: NativeControlConfig,
        transport: _TaskSubmitter,
        event_sink: EventSink,
        clock: Clock,
        uuid_factory: UUIDFactory,
        crash_hook: CrashHook,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._transport = transport
        self._event_sink = event_sink
        self._clock = clock
        self._uuid_factory = uuid_factory
        self._crash_hook = crash_hook
        self._sleep = sleep

    def _emit(self, event: str, data: Mapping[str, object]) -> None:
        _emit_native_event(self._event_sink, self._clock, event, data)

    async def __call__(
        self,
        envelope: Mapping[str, object],
        context: ExecutionContext,
    ) -> tuple[Mapping[str, object], str]:
        self._emit(
            "fixture.handler_started",
            {
                "agent_id": self._config.agent_id,
                "request_envelope_id": envelope["id"],
                "task_id": envelope["task_id"],
                "behavior": self._config.behavior,
                "hop_count": envelope["hop_count"],
            },
        )
        payload = cast(Mapping[str, object], envelope["payload"])
        body = payload.get("body")
        if type(body) is not str:
            raise ValueError("invalid payload body")
        task_id = cast(str, envelope["task_id"])
        if self._config.behavior == "echo":
            publication = await context.publish_progress(task_id, body="working")
            _require_accepted(publication, publication.envelope_id, "progress")
            await self._sleep(self._config.delay_ms / 1000)
        elif self._config.behavior == "delegate":
            publication = await context.publish_progress(task_id, body="working")
            _require_accepted(publication, publication.envelope_id, "progress")
            await self._sleep(self._config.delay_ms / 1000)
            if envelope["hop_count"] == 0:
                child_task_id = self._uuid_factory.uuid4()
                child_wire_id = self._uuid_factory.uuid4()
                child: dict[str, object] = {
                    "v": 1,
                    "id": child_wire_id,
                    "type": "delegation",
                    "sender_id": envelope["sender_id"],
                    "recipient_id": self._config.agent_id,
                    "task_id": child_task_id,
                    "context_id": envelope["context_id"],
                    "hop_count": 1,
                    "timestamp": self._clock.now_iso(),
                    "payload": {
                        "body": body,
                        "parent_task_id": task_id,
                    },
                }
                default_validator().validate_envelope(child)
                self._emit(
                    "fixture.delegation_created",
                    {
                        "parent_task_id": task_id,
                        "child_task_id": child_task_id,
                        "child_request_envelope_id": child_wire_id,
                        "context_id": envelope["context_id"],
                        "hop_count": 1,
                    },
                )
                receipt = await self._transport.submit_task(child)
                _require_accepted(receipt, child_wire_id, "delegation")
                return (
                    {
                        "body": f"edgecitadel:{body}",
                        "child_task_id": child_task_id,
                    },
                    "completed",
                )
        elif self._config.behavior == "progress":
            for index in range(1, 21):
                if index > 1:
                    await self._sleep(0.05)
                publication = await context.publish_progress(
                    task_id,
                    body=_PROGRESS_MESSAGE,
                    progress=index * 5,
                )
                _require_accepted(
                    publication,
                    publication.envelope_id,
                    "progress",
                )
                self._emit(
                    "fixture.progress_generated",
                    {
                        "task_id": task_id,
                        "envelope_id": publication.envelope_id,
                        "progress": index * 5,
                        "payload_bytes": len(_PROGRESS_MESSAGE.encode("ascii")),
                    },
                )
        elif self._config.behavior == "actuator":
            publication = await context.publish_progress(task_id, body="working")
            _require_accepted(publication, publication.envelope_id, "progress")
            await self._sleep(self._config.delay_ms / 1000)
            attempt_id: int
            side_effect_id: int
            attempt_count: int
            side_effect_count: int
            connection = _open_actuator_database(Path(self._config.side_effect_db))
            try:
                connection.execute("BEGIN IMMEDIATE")
                attempt_cursor = connection.execute(
                    """
                    INSERT INTO execution_attempts(
                        task_id, wire_id, started_monotonic_ns
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        task_id,
                        cast(str, envelope["id"]),
                        self._clock.monotonic_ns(),
                    ),
                )
                attempt_id = cast(int, attempt_cursor.lastrowid)
                connection.execute("COMMIT")

                body_sha256 = hashlib.sha256(canonical_json(body)).hexdigest()
                connection.execute("BEGIN IMMEDIATE")
                effect_cursor = connection.execute(
                    """
                    INSERT INTO external_side_effects(
                        attempt_id, task_id, body_sha256, committed_monotonic_ns
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        attempt_id,
                        task_id,
                        body_sha256,
                        self._clock.monotonic_ns(),
                    ),
                )
                side_effect_id = cast(int, effect_cursor.lastrowid)
                connection.execute("COMMIT")
                attempt_count = cast(
                    int,
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM execution_attempts
                        WHERE task_id = ?
                        """,
                        (task_id,),
                    ).fetchone()[0],
                )
                side_effect_count = cast(
                    int,
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM external_side_effects
                        WHERE task_id = ?
                        """,
                        (task_id,),
                    ).fetchone()[0],
                )
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()
            self._emit(
                "fixture.actuator_attempt_committed",
                {
                    "task_id": task_id,
                    "request_envelope_id": envelope["id"],
                    "attempt_id": attempt_id,
                    "attempt_count": attempt_count,
                },
            )
            self._emit(
                "fixture.side_effect_committed",
                {
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                    "side_effect_id": side_effect_id,
                    "side_effect_count": side_effect_count,
                    "body_sha256": body_sha256,
                },
            )
            self._crash_hook.hit("after-side-effect-before-ledger-prepare")
            return (
                {
                    "body": f"edgecitadel:{body}",
                    "attempt_count": attempt_count,
                    "side_effect_count": side_effect_count,
                },
                "completed",
            )
        else:
            raise NotImplementedError
        return ({"body": f"edgecitadel:{body}"}, "completed")


def _require_accepted(
    receipt: object,
    envelope_id: str,
    publication: str,
) -> None:
    if (
        not isinstance(receipt, PublicationReceipt)
        or receipt.accepted is not True
        or receipt.envelope_id != envelope_id
    ):
        raise RuntimeError(f"{publication} publication failed")


def _normalize_sql(value: str) -> str:
    return " ".join(value.split())


def _prepare_actuator_path(path: Path) -> os.stat_result:
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError:
            raise RuntimeError("actuator database initialization failed") from None
        try:
            os.fchmod(descriptor, 0o600)
            path_stat = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise RuntimeError("actuator database initialization failed") from None
    if (
        not stat.S_ISREG(path_stat.st_mode)
        or stat.S_ISLNK(path_stat.st_mode)
        or stat.S_IMODE(path_stat.st_mode) != 0o600
    ):
        raise RuntimeError("actuator database initialization failed")
    return path_stat


def _open_actuator_database(path: Path) -> sqlite3.Connection:
    before = _prepare_actuator_path(path)
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            path,
            timeout=_ACTUATOR_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        after = path.lstat()
        if (
            not stat.S_ISREG(after.st_mode)
            or stat.S_ISLNK(after.st_mode)
            or stat.S_IMODE(after.st_mode) != 0o600
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        ):
            raise RuntimeError("actuator database initialization failed")
        journal = connection.execute("PRAGMA journal_mode = WAL").fetchone()
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {_ACTUATOR_BUSY_TIMEOUT_MS}")
        if (
            journal is None
            or len(journal) != 1
            or str(journal[0]).casefold() != "wal"
            or connection.execute("PRAGMA synchronous").fetchone() != (2,)
            or connection.execute("PRAGMA foreign_keys").fetchone() != (1,)
            or connection.execute("PRAGMA busy_timeout").fetchone()
            != (_ACTUATOR_BUSY_TIMEOUT_MS,)
        ):
            raise RuntimeError("actuator database initialization failed")
        _initialize_actuator_schema(connection)
        return connection
    except (OSError, sqlite3.Error, RuntimeError):
        if connection is not None:
            connection.close()
        raise RuntimeError("actuator database initialization failed") from None


def _initialize_actuator_schema(connection: sqlite3.Connection) -> None:
    version_row = connection.execute("PRAGMA user_version").fetchone()
    if version_row is None:
        raise RuntimeError("actuator database initialization failed")
    version = cast(int, version_row[0])
    rows = connection.execute(
        """
        SELECT type, name FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    objects = {(cast(str, row[0]), cast(str, row[1])) for row in rows}
    expected_objects = {("table", name) for name in _ACTUATOR_SCHEMAS}
    if version == 0:
        if objects:
            raise RuntimeError("actuator database initialization failed")
        connection.execute("BEGIN IMMEDIATE")
        try:
            for schema in _ACTUATOR_SCHEMAS.values():
                connection.execute(schema)
            connection.execute(f"PRAGMA user_version = {_ACTUATOR_SCHEMA_VERSION}")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    elif version != _ACTUATOR_SCHEMA_VERSION:
        raise RuntimeError("actuator database initialization failed")

    if {
        (cast(str, row[0]), cast(str, row[1]))
        for row in connection.execute(
            """
            SELECT type, name FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
    } != expected_objects:
        raise RuntimeError("actuator database initialization failed")
    actual = {
        cast(str, row[0]): cast(str, row[1])
        for row in connection.execute(
            """
            SELECT name, sql FROM sqlite_master
            WHERE type = 'table'
              AND name IN ('execution_attempts', 'external_side_effects')
            """
        ).fetchall()
    }
    if set(actual) != set(_ACTUATOR_SCHEMAS):
        raise RuntimeError("actuator database initialization failed")
    for name, expected in _ACTUATOR_SCHEMAS.items():
        if _normalize_sql(actual[name]) != _normalize_sql(expected):
            raise RuntimeError("actuator database initialization failed")


def build_agent_card(config: NativeControlConfig) -> dict[str, object]:
    inbox = f"nats://edgecitadel/agents.{config.agent_id}.inbox"
    return {
        "name": config.agent_id,
        "description": "Deterministic EdgeCitadel research worker.",
        "version": "1.0.0",
        "url": inbox,
        "provider": {
            "organization": "EdgeCitadel",
            "url": "https://edgecitadel.local",
        },
        "capabilities": {
            "streaming": True,
            "extensions": [
                {
                    "uri": "https://edgecitadel.local/ext/nats-binding/v1",
                    "description": "NATS transport binding.",
                    "required": True,
                    "params": {
                        "subject_prefix": f"agents.{config.agent_id}",
                    },
                }
            ],
        },
        "securitySchemes": {},
        "additionalInterfaces": [
            {
                "url": inbox,
                "transport": "NATS",
            }
        ],
        "skills": [
            {
                "id": "fixture.execute",
                "name": "fixture-execute",
                "description": "Execute deterministic research tasks.",
                "tags": ["research"],
            }
        ],
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 10,
        },
    }


async def run_fixture(
    config: NativeControlConfig,
    transport: TaskTransport,
    event_sink: EventSink,
) -> None:
    if transport.mode.value != config.mode:
        raise ValueError("transport mode does not match config")
    store = (
        SQLiteOutcomeStore(Path(config.outcome_db))
        if transport.outcome_ledger_enabled
        else DisabledOutcomeStore()
    )
    clock = _SystemClock()
    uuid_factory = _SystemUUIDFactory()
    crash_hook = _ConfiguredCrashHook(config.crash_point)
    policy = _NativePolicy()
    handler = _NativeHandler(
        config=config,
        transport=transport,
        event_sink=event_sink,
        clock=clock,
        uuid_factory=uuid_factory,
        crash_hook=crash_hook,
    )
    executor = TaskExecutor(
        worker_agent_id=config.agent_id,
        handler=handler,
        outcome_store=store,
        terminal_publisher=transport,
        progress_publisher=transport,
        policy=policy,
        event_sink=event_sink,
        clock=clock,
        uuid_factory=uuid_factory,
        crash_hook=crash_hook,
    )
    heartbeat_task: asyncio.Task[None] | None = None
    try:
        await transport.start_receiver(config.agent_id, executor)
        await transport.wait_receiver_ready(config.agent_id, timeout_s=5.0)
        _emit_native_event(
            event_sink,
            clock,
            "fixture.ready",
            {"agent_id": config.agent_id},
        )
        heartbeat_task = asyncio.create_task(
            _heartbeat_loop(config, transport, clock, uuid_factory)
        )
        await heartbeat_task
    finally:
        if heartbeat_task is not None and not heartbeat_task.done():
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        store.close()


async def _heartbeat_loop(
    config: NativeControlConfig,
    transport: _HeartbeatPublisher,
    clock: Clock,
    uuid_factory: UUIDFactory,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    interval_ns = config.heartbeat_interval_ms * 1_000_000
    deadline_ns = clock.monotonic_ns() + interval_ns
    while True:
        while True:
            remaining_ns = deadline_ns - clock.monotonic_ns()
            if remaining_ns <= 0:
                break
            await sleep(remaining_ns / 1_000_000_000)
        heartbeat: dict[str, object] = {
            "v": 1,
            "id": uuid_factory.uuid4(),
            "type": "heartbeat",
            "sender_id": config.agent_id,
            "timestamp": clock.now_iso(),
            "payload": {},
        }
        default_validator().validate_envelope(heartbeat)
        receipt = await transport.publish_heartbeat(heartbeat)
        _require_accepted(
            receipt,
            cast(str, heartbeat["id"]),
            "heartbeat",
        )
        deadline_ns += interval_ns


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    if (
        len(argv) != 2
        or argv[0] != "--config"
        or type(argv[1]) is not str
        or not argv[1]
    ):
        raise ValueError("invalid arguments")
    return argparse.Namespace(config=Path(argv[1]))


def load_native_config(path: str | Path) -> NativeControlConfig:
    contents = _read_regular_file(path, _MAX_CONFIG_BYTES, "config file")
    if contents.startswith(b"\xef\xbb\xbf"):
        raise ValueError("invalid config JSON")
    try:
        decoded = cast(
            object,
            json.loads(
                contents.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            ),
        )
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("invalid config JSON") from None
    if not isinstance(decoded, dict):
        raise ValueError("invalid config JSON")  # noqa: TRY004
    try:
        if _contains_nonfinite(decoded):
            raise ValueError("invalid config JSON")
    except RecursionError:
        raise ValueError("invalid config JSON") from None
    if set(decoded) != set(_CONFIG_KEYS):
        raise ValueError("invalid config keys")
    raw = cast(dict[str, object], decoded)
    return NativeControlConfig(
        run_id=cast(str, raw["run_id"]),
        agent_id=cast(str, raw["agent_id"]),
        mode=cast(str, raw["mode"]),
        behavior=cast(str, raw["behavior"]),
        delay_ms=cast(int, raw["delay_ms"]),
        crash_point=cast(str | None, raw["crash_point"]),
        heartbeat_interval_ms=cast(int, raw["heartbeat_interval_ms"]),
        outcome_db=cast(str, raw["outcome_db"]),
        side_effect_db=cast(str, raw["side_effect_db"]),
    )


def runtime_endpoints(
    config: NativeControlConfig,
    environ: Mapping[str, str],
) -> dict[str, str]:
    if config.mode == Mode.CENTRAL_RELAY.value:
        key = "RELAY_URL"
        schemes = {"http", "https"}
    else:
        key = "NATS_URL"
        schemes = {"nats"}
    try:
        value = environ[key]
    except (KeyError, TypeError):
        raise ValueError(f"invalid {key}") from None
    if not _valid_endpoint(value, schemes):
        raise ValueError(f"invalid {key}")
    return {key: value}


def read_transport_token(path: str | Path) -> str:
    contents = _read_regular_file(
        path,
        65,
        "credential file",
        allowed_modes=frozenset({0o400, 0o600}),
    )
    if re.fullmatch(rb"[0-9a-f]{64}\n", contents) is None:
        raise ValueError("invalid credential file")
    return contents[:64].decode("ascii")


def build_transport(
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
    event_sink: EventSink,
) -> TaskTransport:
    if config.mode == Mode.CENTRAL_RELAY.value:
        module = importlib.import_module("scripts.research.modes.central_relay")
        transport_factory = cast(
            Callable[..., TaskTransport], module.CentralRelayTransport
        )

        return transport_factory(
            relay_url=endpoints["RELAY_URL"],
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
        )

    agent_card = build_agent_card(config)
    nats_url = endpoints["NATS_URL"]
    if config.mode == Mode.CORE_ONLY.value:
        module = importlib.import_module("scripts.research.modes.core_nats")
        transport_factory = cast(Callable[..., TaskTransport], module.CoreNatsTransport)

        return transport_factory(
            nats_url=nats_url,
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            agent_card=agent_card,
        )
    if config.mode == Mode.EDGECITADEL.value:
        module = importlib.import_module("scripts.research.modes.edgecitadel")
        transport_factory = cast(
            Callable[..., TaskTransport], module.EdgeCitadelTransport
        )

        return transport_factory(
            nats_url=nats_url,
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            agent_card=agent_card,
        )
    if config.mode == Mode.ALL_DURABLE.value:
        module = importlib.import_module("scripts.research.modes.all_durable")
        transport_factory = cast(
            Callable[..., TaskTransport], module.AllDurableTransport
        )

        return transport_factory(
            nats_url=nats_url,
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            agent_card=agent_card,
        )
    raise ValueError("invalid mode")


async def main(
    argv: Sequence[str],
    environ: Mapping[str, str] = os.environ,
    transport_factory: TransportFactory = build_transport,
) -> None:
    try:
        arguments = parse_args(argv)
        config = load_native_config(arguments.config)
        endpoints = runtime_endpoints(config, environ)
        credential_path = environ.get("EC_CREDENTIAL_FILE", "")
        token = read_transport_token(credential_path)
    except ValueError:
        raise SystemExit("native control failed") from None
    try:
        event_log = environ.get("EC_EVENT_LOG")
        event_sink: EventSink = (
            _FileEventSink(Path(event_log))
            if event_log is not None
            else _SilentEventSink()
        )
    except (TypeError, ValueError, OSError):
        raise SystemExit("native control failed") from None
    transport: TaskTransport | None = None
    loop: asyncio.AbstractEventLoop | None = None
    installed_signals: list[int] = []
    try:
        transport = transport_factory(config, endpoints, token, event_sink)
        loop = asyncio.get_running_loop()
        current_task = asyncio.current_task()
        signal_module = __import__("signal")

        def cancel_fixture() -> None:
            if current_task is not None:
                current_task.cancel()

        for current_signal in (signal_module.SIGINT, signal_module.SIGTERM):
            try:
                loop.add_signal_handler(current_signal, cancel_fixture)
            except (NotImplementedError, RuntimeError):
                continue
            installed_signals.append(current_signal)
        await run_fixture(config, transport, event_sink)
    except asyncio.CancelledError:
        return
    except Exception:  # noqa: BLE001 - CLI errors must be secret-free and stable.
        raise SystemExit("native control failed") from None
    finally:
        if loop is not None:
            for current_signal in installed_signals:
                loop.remove_signal_handler(current_signal)
        if transport is not None:
            try:
                await transport.close()
            except Exception:  # noqa: BLE001 - close failures share the CLI boundary.
                if sys.exc_info()[0] is None:
                    raise SystemExit("native control failed") from None


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey
        value[key] = item
    return value


def _reject_json_constant(_: str) -> None:
    raise _InvalidJSON


def _contains_nonfinite(value: object) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(_contains_nonfinite(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_nonfinite(item) for item in value)
    return False


def _valid_endpoint(value: object, schemes: set[str]) -> bool:
    if (
        type(value) is not str
        or not value
        or any(character.isspace() or ord(character) == 0x7F for character in value)
    ):
        return False
    try:
        parsed = urllib.parse.urlsplit(value)
        host = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in schemes
        and bool(parsed.netloc)
        and bool(host)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def _read_regular_file(
    path: str | Path,
    limit: int,
    label: str,
    *,
    allowed_modes: frozenset[int] | None = None,
) -> bytes:
    try:
        file_path = Path(path)
        before = file_path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or before.st_size > limit
            or (
                allowed_modes is not None
                and stat.S_IMODE(before.st_mode) not in allowed_modes
            )
        ):
            raise OSError
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(file_path, flags)
    except (OSError, TypeError, ValueError):
        raise ValueError(f"invalid {label}") from None

    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
            or opened.st_size > limit
            or (
                allowed_modes is not None
                and stat.S_IMODE(opened.st_mode) not in allowed_modes
            )
        ):
            raise OSError
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(descriptor, min(remaining, 8192))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        contents = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            len(contents) > limit
            or len(contents) != opened.st_size
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (after.st_dev, after.st_ino, after.st_size)
            or (
                allowed_modes is not None
                and stat.S_IMODE(after.st_mode) not in allowed_modes
            )
        ):
            raise OSError
        return contents
    except OSError:
        raise ValueError(f"invalid {label}") from None
    finally:
        os.close(descriptor)


__all__ = (  # noqa: RUF022 - public contract order is frozen.
    "NativeControlConfig",
    "BEHAVIORS",
    "CRASH_POINTS",
    "build_agent_card",
    "build_transport",
    "main",
    "run_fixture",
    "parse_args",
    "load_native_config",
    "runtime_endpoints",
    "read_transport_token",
)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
