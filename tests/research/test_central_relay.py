"""Contract tests for the run-owned central relay transport."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import inspect
import json
import logging
import os
import sqlite3
import time
import traceback
import typing
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from typing import AsyncContextManager  # noqa: UP035

import httpx
import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.routing import WebSocketRoute
from starlette.types import Message, Scope
from starlette.websockets import WebSocketDisconnect
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close as WebSocketClose

import scripts.research.modes.central_relay as relay_module
import scripts.research.modes.central_relay_server as server_module
from edgecitadel_plugin_runtime.outcome_store import OutcomeKey, SQLiteOutcomeStore
from edgecitadel_plugin_runtime.task_executor import (
    ExecutionContext,
    InjectedCrash,
    PolicyDecision,
    TaskExecutor,
)
from edgecitadel_plugin_runtime.validator import canonical_json
from scripts.research.modes.base import EventSink, Mode, TaskTransport
from scripts.research.modes.central_relay import CentralRelayTransport
from scripts.research.modes.central_relay_server import RelayStore

TOKEN = "a" * 64
NOW = "2026-07-25T12:00:00.000Z"
AUTH_HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

_TestParams = typing.ParamSpec("_TestParams")
_TestReturn = typing.TypeVar("_TestReturn")


def _typed_test_decorator(
    decorator: object,
) -> Callable[
    [Callable[_TestParams, _TestReturn]],
    Callable[_TestParams, _TestReturn],
]:
    return typing.cast(
        Callable[
            [Callable[_TestParams, _TestReturn]],
            Callable[_TestParams, _TestReturn],
        ],
        decorator,
    )


EXPECTED_DDL = """
CREATE TABLE tasks (
    task_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    envelope_id TEXT NOT NULL UNIQUE CHECK (length(envelope_id) > 0),
    task_id TEXT NOT NULL CHECK (length(task_id) > 0),
    recipient_id TEXT NOT NULL CHECK (length(recipient_id) > 0),
    envelope BLOB NOT NULL CHECK (length(envelope) > 0),
    envelope_sha256 TEXT NOT NULL
        CHECK (
            length(envelope_sha256) = 64
            AND envelope_sha256 NOT GLOB '*[^0-9a-f]*'
        ),
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'leased', 'completed', 'terminated')),
    delivery_count INTEGER NOT NULL DEFAULT 0
        CHECK (delivery_count >= 0),
    submitted_ns INTEGER NOT NULL CHECK (submitted_ns > 0),
    completed_ns INTEGER CHECK (completed_ns IS NULL OR completed_ns > 0),
    UNIQUE (task_sequence, recipient_id, task_id),
    CHECK (
        (state IN ('queued', 'leased') AND completed_ns IS NULL)
        OR
        (state IN ('completed', 'terminated') AND completed_ns IS NOT NULL)
    )
) STRICT;

CREATE TABLE leases (
    lease_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_id TEXT NOT NULL UNIQUE CHECK (length(lease_id) > 0),
    task_sequence INTEGER NOT NULL,
    task_id TEXT NOT NULL CHECK (length(task_id) > 0),
    recipient_id TEXT NOT NULL CHECK (length(recipient_id) > 0),
    worker_agent_id TEXT NOT NULL CHECK (length(worker_agent_id) > 0),
    delivery_count INTEGER NOT NULL CHECK (delivery_count > 0),
    state TEXT NOT NULL DEFAULT 'active'
        CHECK (
            state IN (
                'active', 'expired', 'released', 'terminated', 'committed'
            )
        ),
    acquired_ns INTEGER NOT NULL CHECK (acquired_ns > 0),
    deadline_ns INTEGER NOT NULL CHECK (deadline_ns > acquired_ns),
    finalized_ns INTEGER CHECK (finalized_ns IS NULL OR finalized_ns > 0),
    UNIQUE (task_sequence, delivery_count),
    UNIQUE (lease_sequence, lease_id, task_sequence, task_id),
    FOREIGN KEY (task_sequence, recipient_id, task_id)
        REFERENCES tasks (task_sequence, recipient_id, task_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (worker_agent_id = recipient_id),
    CHECK (
        (state = 'active' AND finalized_ns IS NULL)
        OR
        (state != 'active' AND finalized_ns IS NOT NULL)
    )
) STRICT;

CREATE TABLE terminals (
    terminal_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    lease_sequence INTEGER NOT NULL UNIQUE,
    lease_id TEXT NOT NULL UNIQUE CHECK (length(lease_id) > 0),
    task_sequence INTEGER NOT NULL,
    task_id TEXT NOT NULL CHECK (length(task_id) > 0),
    terminal_id TEXT NOT NULL CHECK (length(terminal_id) > 0),
    terminal_sha256 TEXT NOT NULL
        CHECK (
            length(terminal_sha256) = 64
            AND terminal_sha256 NOT GLOB '*[^0-9a-f]*'
        ),
    envelope BLOB NOT NULL CHECK (length(envelope) > 0),
    state TEXT NOT NULL DEFAULT 'prepared'
        CHECK (state IN ('prepared', 'committed')),
    prepared_ns INTEGER NOT NULL CHECK (prepared_ns > 0),
    committed_ns INTEGER CHECK (committed_ns IS NULL OR committed_ns > 0),
    FOREIGN KEY (lease_sequence, lease_id, task_sequence, task_id)
        REFERENCES leases (
            lease_sequence, lease_id, task_sequence, task_id
        )
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CHECK (
        (state = 'prepared' AND committed_ns IS NULL)
        OR
        (state = 'committed' AND committed_ns IS NOT NULL)
    )
) STRICT;

CREATE INDEX tasks_ready
    ON tasks (recipient_id, state, task_sequence);
CREATE INDEX tasks_logical_history
    ON tasks (recipient_id, task_id, task_sequence);
CREATE UNIQUE INDEX leases_one_active_logical_task
    ON leases (worker_agent_id, task_id)
    WHERE state = 'active';
CREATE INDEX leases_expiry
    ON leases (state, deadline_ns);
CREATE INDEX leases_task_history
    ON leases (task_sequence, lease_sequence);
CREATE INDEX terminals_task_cursor
    ON terminals (task_id, state, terminal_sequence);
PRAGMA user_version = 1;
""".strip()


class _EventSink:
    def emit(self, event: Mapping[str, object]) -> None:
        del event


class _RecordingEventSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(event)


class _Clock:
    def __init__(self, value: int = 1_000_000_000) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value


class _EvidenceClock:
    def __init__(self, value: int = 10_000) -> None:
        self.value = value

    def __call__(self) -> int:
        self.value += 1
        return self.value


class _UUIDs:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def __call__(self) -> str:
        if not self.values:
            raise AssertionError("unexpected UUID allocation")
        return self.values.pop(0)

    def uuid4(self) -> str:
        return self()


class _ExecutorClock:
    def __init__(self) -> None:
        self.value = 20_000

    def monotonic_ns(self) -> int:
        self.value += 1
        return self.value

    def now_iso(self) -> str:
        return NOW


class _CrashHook:
    def __init__(self, crash_point: str | None = None) -> None:
        self.crash_point = crash_point
        self.hits: list[str] = []

    def hit(self, point: str) -> None:
        self.hits.append(point)
        if point == self.crash_point:
            raise InjectedCrash(point)


class _AcceptPolicy:
    def evaluate(
        self,
        envelope: Mapping[str, object],
        worker_agent_id: str,
    ) -> PolicyDecision:
        del envelope, worker_agent_id
        return PolicyDecision(accepted=True, reason=None)


class _EchoHandler:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(
        self,
        request: Mapping[str, object],
        context: ExecutionContext,
    ) -> tuple[Mapping[str, object], str]:
        del context
        self.calls += 1
        payload = typing.cast(Mapping[str, object], request["payload"])
        return {"body": f"edgecitadel:{payload['body']}"}, "completed"


def _command(
    *,
    envelope_id: str = "10000000-0000-4000-8000-000000000001",
    task_id: str = "20000000-0000-4000-8000-000000000001",
    sender_id: str = "requester-1",
    recipient_id: str = "worker-1",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "command",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "timestamp": NOW,
        "payload": {"body": "nonce"},
    }


def _terminal(
    *,
    terminal_id: str = "30000000-0000-4000-8000-000000000001",
    task_id: str = "20000000-0000-4000-8000-000000000001",
    sender_id: str = "worker-1",
    recipient_id: str = "requester-1",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": terminal_id,
        "type": "result",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "context_id": task_id,
        "hop_count": 0,
        "task_state": "completed",
        "timestamp": NOW,
        "payload": {"body": "edgecitadel:nonce"},
    }


def _progress(
    *,
    envelope_id: str = "50000000-0000-4000-8000-000000000001",
    progress: int = 5,
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "task.progress",
        "sender_id": "worker-1",
        "recipient_id": "requester-1",
        "task_id": "20000000-0000-4000-8000-000000000001",
        "context_id": "20000000-0000-4000-8000-000000000001",
        "hop_count": 0,
        "task_state": "working",
        "timestamp": NOW,
        "payload": {"message": "x" * 256, "progress": progress},
    }


def _post_canonical(
    client: TestClient,
    path: str,
    value: Mapping[str, object],
) -> httpx.Response:
    return typing.cast(
        httpx.Response,
        client.post(path, content=canonical_json(value), headers=AUTH_HEADERS),
    )


def _assert_canonical_response(
    response: httpx.Response,
    status_code: int,
    body: Mapping[str, object],
) -> None:
    assert response.status_code == status_code
    assert response.content == canonical_json(body)
    assert response.headers["content-type"] == "application/json"


def _assert_store_error(
    raised: pytest.ExceptionInfo[BaseException],
    status_code: int,
    code: str,
) -> None:
    assert type(raised.value) is server_module._RelayStoreError
    assert raised.value.status_code == status_code
    assert raised.value.code == code
    assert str(raised.value) == code


def _normalized(sql: str) -> str:
    return " ".join(sql.split()).rstrip(";")


def _schema_statements(ddl: str) -> dict[str, str]:
    statements = [part.strip() for part in ddl.split(";") if part.strip()]
    result: dict[str, str] = {}
    for statement in statements:
        words = statement.split()
        if words[:2] in (["CREATE", "TABLE"], ["CREATE", "INDEX"]):
            name = words[2]
        elif words[:3] == ["CREATE", "UNIQUE", "INDEX"]:
            name = words[3]
        else:
            continue
        result[name] = _normalized(statement)
    return result


def _accept_task_transport(transport: TaskTransport) -> TaskTransport:
    return transport


def _assert_signature(
    callable_object: Callable[..., object],
    *,
    names: list[str],
    positional: int,
) -> inspect.Signature:
    signature = inspect.signature(callable_object)
    assert list(signature.parameters) == names
    for index, parameter in enumerate(signature.parameters.values()):
        expected_kind = (
            inspect.Parameter.POSITIONAL_OR_KEYWORD
            if index < positional
            else inspect.Parameter.KEYWORD_ONLY
        )
        assert parameter.kind is expected_kind
    return signature


def test_central_public_exports_and_task_transport_contract() -> None:
    assert list(relay_module.__all__) == ["CentralRelayTransport"]
    assert list(server_module.__all__) == [
        "RelayStore",
        "create_app",
        "create_app_from_environment",
    ]
    sink: EventSink = _EventSink()
    transport = CentralRelayTransport(
        relay_url="http://127.0.0.1:8000",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
    )
    assert _accept_task_transport(transport) is transport
    assert transport.mode is Mode.CENTRAL_RELAY
    assert transport.outcome_ledger_enabled is True
    assert transport.faults is transport.faults


def test_central_imports_event_sink_from_base_contract() -> None:
    tree = ast.parse(inspect.getsource(relay_module))
    event_sink_imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and any(alias.name == "EventSink" for alias in node.names)
    }
    assert event_sink_imports == {"scripts.research.modes.base"}


def test_central_public_signatures_annotations_and_defaults_are_exact() -> None:
    signature = _assert_signature(
        CentralRelayTransport,
        names=[
            "relay_url",
            "run_id",
            "token",
            "event_sink",
            "coordinator_restart",
            "worker_stop",
            "worker_start",
            "http_client",
            "websocket_connect",
            "evidence_clock_ns",
            "epoch_now",
            "sleep",
        ],
        positional=0,
    )
    parameters = signature.parameters
    assert [parameters[name].default for name in list(parameters)[:4]] == [
        inspect.Parameter.empty
    ] * 4
    assert parameters["coordinator_restart"].default is None
    assert parameters["worker_stop"].default is None
    assert parameters["worker_start"].default is None
    assert parameters["http_client"].default is None
    assert parameters["websocket_connect"].default is None
    assert parameters["evidence_clock_ns"].default is time.perf_counter_ns
    assert parameters["epoch_now"].default is relay_module._now_iso
    assert parameters["sleep"].default is asyncio.sleep
    assert typing.get_type_hints(CentralRelayTransport.__init__) == {
        "relay_url": str,
        "run_id": str,
        "token": str,
        "event_sink": EventSink,
        "coordinator_restart": Callable[[], Awaitable[str | None]] | None,
        "worker_stop": Callable[[str], Awaitable[None]] | None,
        "worker_start": Callable[[str], Awaitable[None]] | None,
        "http_client": httpx.AsyncClient | None,
        "websocket_connect": (Callable[..., AsyncContextManager[object]] | None),
        "evidence_clock_ns": Callable[[], int],
        "epoch_now": Callable[[], str],
        "sleep": Callable[[float], Awaitable[None]],
        "return": type(None),
    }

    store_signature = _assert_signature(
        RelayStore,
        names=[
            "database_path",
            "run_id",
            "lease_ttl_ms",
            "lease_clock_ns",
            "evidence_clock_ns",
            "uuid4",
        ],
        positional=1,
    )
    store_parameters = store_signature.parameters
    assert store_parameters["database_path"].default is inspect.Parameter.empty
    assert store_parameters["run_id"].default is inspect.Parameter.empty
    assert store_parameters["lease_ttl_ms"].default == 30_000
    assert store_parameters["lease_clock_ns"].default is time.monotonic_ns
    assert store_parameters["evidence_clock_ns"].default is time.perf_counter_ns
    assert store_parameters["uuid4"].default is server_module._uuid4
    assert typing.get_type_hints(RelayStore.__init__) == {
        "database_path": Path,
        "run_id": str,
        "lease_ttl_ms": int,
        "lease_clock_ns": Callable[[], int],
        "evidence_clock_ns": Callable[[], int],
        "uuid4": Callable[[], str],
        "return": type(None),
    }

    app_signature = _assert_signature(
        server_module.create_app,
        names=[
            "database_path",
            "run_id",
            "token",
            "lease_ttl_ms",
            "lease_clock_ns",
            "evidence_clock_ns",
            "uuid4",
        ],
        positional=1,
    )
    app_parameters = app_signature.parameters
    assert app_parameters["database_path"].default is inspect.Parameter.empty
    assert app_parameters["run_id"].default is inspect.Parameter.empty
    assert app_parameters["token"].default is inspect.Parameter.empty
    assert app_parameters["lease_ttl_ms"].default == 30_000
    assert app_parameters["lease_clock_ns"].default is time.monotonic_ns
    assert app_parameters["evidence_clock_ns"].default is time.perf_counter_ns
    assert app_parameters["uuid4"].default is server_module._uuid4
    assert typing.get_type_hints(server_module.create_app) == {
        "database_path": Path,
        "run_id": str,
        "token": str,
        "lease_ttl_ms": int,
        "lease_clock_ns": Callable[[], int],
        "evidence_clock_ns": Callable[[], int],
        "uuid4": Callable[[], str],
        "return": FastAPI,
    }
    assert (
        list(inspect.signature(server_module.create_app_from_environment).parameters)
        == []
    )
    assert typing.get_type_hints(server_module.create_app_from_environment) == {
        "return": FastAPI
    }


@_typed_test_decorator(
    pytest.mark.parametrize(
        "relay_url",
        ["https://relay.invalid?", "https://relay.invalid#"],
    )
)
def test_central_constructor_rejects_empty_query_or_fragment_delimiter(
    relay_url: str,
) -> None:
    with pytest.raises(ValueError, match=r"^invalid relay_url$"):
        CentralRelayTransport(
            relay_url=relay_url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
        )


def test_central_resolved_config_is_exact_fresh_and_read_only() -> None:
    endpoint_sentinel = "http://endpoint-sentinel.invalid:8000"
    run_sentinel = "run-sentinel"
    token_sentinel = "a1" * 32

    async def callback_sentinel() -> str:
        return "callback-sentinel"

    transport = CentralRelayTransport(
        relay_url=endpoint_sentinel,
        run_id=run_sentinel,
        token=token_sentinel,
        event_sink=_EventSink(),
        coordinator_restart=callback_sentinel,
    )
    expected = {
        "mode": "central-relay",
        "ablation": "full-contract",
        "nats_msg_id": False,
        "outcome_ledger": True,
    }
    first = transport.resolved_config
    second = transport.resolved_config
    assert type(first) is MappingProxyType
    assert first == expected
    assert first is not second
    assert {key: type(value) for key, value in first.items()} == {
        "mode": str,
        "ablation": str,
        "nats_msg_id": bool,
        "outcome_ledger": bool,
    }
    with pytest.raises(TypeError):
        first["mode"] = "changed"  # type: ignore[index]
    with pytest.raises((AttributeError, TypeError)):
        first.clear()  # type: ignore[attr-defined]
    mutable_copy = dict(first)
    mutable_copy["mode"] = "changed"
    assert dict(transport.resolved_config) == expected
    assert dict(second) == expected
    assert json.loads(json.dumps(dict(second), sort_keys=True)) == expected
    serialized = json.dumps(dict(second), sort_keys=True)
    for sentinel in (
        endpoint_sentinel,
        run_sentinel,
        token_sentinel,
        "callback-sentinel",
        "agent",
        "credential",
    ):
        assert sentinel not in serialized
    resolved_config_descriptor = inspect.getattr_static(
        type(transport),
        "resolved_config",
    )
    assert isinstance(resolved_config_descriptor, property)
    assert resolved_config_descriptor.fset is None


def test_relay_store_creates_exact_strict_schema_and_pragmas(tmp_path: Path) -> None:
    database_path = tmp_path / "relay.sqlite3"
    store = RelayStore(database_path, run_id="run-1")
    assert store.task("missing") is None
    connection = store._connection
    assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    assert connection.execute("PRAGMA synchronous").fetchone() == (2,)
    assert connection.execute("PRAGMA foreign_keys").fetchone() == (1,)
    assert connection.execute("PRAGMA busy_timeout").fetchone() == (5000,)
    store.close()
    store.close()

    assert database_path.stat().st_mode & 0o777 == 0o600
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        actual = {
            name: _normalized(sql)
            for name, sql in connection.execute(
                """
                SELECT name, sql
                FROM sqlite_schema
                WHERE sql IS NOT NULL
                  AND name NOT LIKE 'sqlite_%'
                """
            )
        }
        assert actual == _schema_statements(EXPECTED_DDL)
        assert set(actual) == {
            "tasks",
            "leases",
            "terminals",
            "tasks_ready",
            "tasks_logical_history",
            "leases_one_active_logical_task",
            "leases_expiry",
            "leases_task_history",
            "terminals_task_cursor",
        }
        assert {
            row[1] for row in connection.execute("PRAGMA index_list('leases')")
        } >= {
            "leases_one_active_logical_task",
            "leases_expiry",
            "leases_task_history",
        }
        assert connection.execute("PRAGMA foreign_key_list('terminals')").fetchall()


@_typed_test_decorator(
    pytest.mark.parametrize(
        "drift_sql",
        [
            "PRAGMA user_version = 2",
            "ALTER TABLE tasks ADD COLUMN unexpected TEXT",
            "CREATE TABLE unexpected(value TEXT) STRICT",
            "DROP INDEX tasks_ready; CREATE INDEX tasks_ready ON tasks(state)",
            (
                "DROP INDEX leases_one_active_logical_task;"
                "CREATE UNIQUE INDEX leases_one_active_logical_task "
                "ON leases(worker_agent_id, task_id) WHERE state = 'released'"
            ),
        ],
    )
)
def test_relay_store_reopens_identical_schema_and_rejects_drift(
    tmp_path: Path,
    drift_sql: str,
) -> None:
    database_path = tmp_path / f"relay-{abs(hash(drift_sql))}.sqlite3"
    RelayStore(database_path, run_id="run-1").close()
    RelayStore(database_path, run_id="run-1").close()

    with sqlite3.connect(database_path) as connection:
        connection.executescript(drift_sql)
    with pytest.raises(RuntimeError, match="relay schema mismatch"):
        RelayStore(database_path, run_id="run-1")


def test_relay_schema_enforces_one_active_lease_per_logical_task(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "relay.sqlite3"
    store = RelayStore(database_path, run_id="run-1")
    connection = store._connection
    try:
        for sequence, envelope_id in enumerate(("wire-1", "wire-2"), start=1):
            connection.execute(
                """
                INSERT INTO tasks(
                    envelope_id, task_id, recipient_id, envelope,
                    envelope_sha256, submitted_ns
                ) VALUES (?, 'task-1', 'worker-1', X'7B7D', ?, ?)
                """,
                (envelope_id, "0" * 64, sequence),
            )
        first_task_sequence = connection.execute(
            "SELECT task_sequence FROM tasks WHERE envelope_id = 'wire-1'"
        ).fetchone()[0]
        second_task_sequence = connection.execute(
            "SELECT task_sequence FROM tasks WHERE envelope_id = 'wire-2'"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO leases(
                lease_id, task_sequence, task_id, recipient_id,
                worker_agent_id, delivery_count, acquired_ns, deadline_ns
            ) VALUES ('lease-1', ?, 'task-1', 'worker-1', 'worker-1', 1, 1, 2)
            """,
            (first_task_sequence,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO leases(
                    lease_id, task_sequence, task_id, recipient_id,
                    worker_agent_id, delivery_count, acquired_ns, deadline_ns
                ) VALUES (
                    'lease-2', ?, 'task-1', 'worker-1', 'worker-1', 1, 1, 2
                )
                """,
                (second_task_sequence,),
            )
    finally:
        store.close()


def test_relay_store_submit_is_wire_idempotent_and_preserves_logical_attempts(
    tmp_path: Path,
) -> None:
    evidence = _EvidenceClock()
    store = RelayStore(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        evidence_clock_ns=evidence,
    )
    first = _command()
    repeated = json.loads(canonical_json(first))
    second = _command(envelope_id="10000000-0000-4000-8000-000000000002")
    try:
        first_status, first_receipt = store._submit_task(first)
        repeat_status, repeat_receipt = store._submit_task(repeated)
        second_status, second_receipt = store._submit_task(second)

        assert (first_status, repeat_status, second_status) == (201, 200, 201)
        assert first_receipt == {
            "envelope_id": first["id"],
            "accepted": True,
            "transport": "central-relay",
            "stream": None,
            "stream_sequence": None,
            "duplicate": None,
            "accepted_ns": first_receipt["accepted_ns"],
            "application_bytes": len(canonical_json(first)),
            "wire_bytes": None,
        }
        assert type(first_receipt["accepted_ns"]) is int
        assert type(repeat_receipt["accepted_ns"]) is int
        assert type(second_receipt["accepted_ns"]) is int
        assert repeat_receipt["accepted_ns"] > first_receipt["accepted_ns"]
        assert second_receipt["accepted_ns"] > repeat_receipt["accepted_ns"]
        assert store.task(str(first["task_id"])) == {
            "envelope_id": first["id"],
            "task_id": first["task_id"],
            "recipient_id": first["recipient_id"],
            "state": "queued",
            "delivery_count": 0,
        }
        assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone() == (
            2,
        )

        changed = {**first, "payload": {"body": "changed"}}
        with pytest.raises(server_module._RelayStoreError) as raised:
            store._submit_task(changed)
        _assert_store_error(raised, 409, "envelope_id_conflict")
        assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone() == (
            2,
        )
    finally:
        store.close()


def test_relay_store_lease_is_oldest_exclusive_and_reopens_for_redelivery(
    tmp_path: Path,
) -> None:
    lease_clock = _Clock()
    uuids = _UUIDs(
        "40000000-0000-4000-8000-000000000001",
        "40000000-0000-4000-8000-000000000002",
    )
    database_path = tmp_path / "relay.sqlite3"
    store = RelayStore(
        database_path,
        run_id="run-1",
        lease_ttl_ms=3,
        lease_clock_ns=lease_clock,
        uuid4=uuids,
    )
    first = _command()
    same_logical = _command(envelope_id="10000000-0000-4000-8000-000000000002")
    store._submit_task(first)
    store._submit_task(same_logical)

    first_lease = store._acquire_lease("worker-1")
    assert first_lease == {
        "delivery_count": 1,
        "envelope": first,
        "lease_deadline_ns": lease_clock.value + 3_000_000,
        "lease_id": "40000000-0000-4000-8000-000000000001",
        "lease_ttl_ms": 3,
        "renewal_interval_ms": 1,
        "stream_sequence": None,
    }
    assert store._acquire_lease("worker-1") is None
    store.close()

    store = RelayStore(
        database_path,
        run_id="run-1",
        lease_ttl_ms=3,
        lease_clock_ns=lease_clock,
        uuid4=uuids,
    )
    try:
        redelivered = store._acquire_lease("worker-1")
        assert redelivered == {
            "delivery_count": 2,
            "envelope": first,
            "lease_deadline_ns": lease_clock.value + 3_000_000,
            "lease_id": "40000000-0000-4000-8000-000000000002",
            "lease_ttl_ms": 3,
            "renewal_interval_ms": 1,
            "stream_sequence": None,
        }
        assert store._acquire_lease("worker-1") is None
        assert store.task(str(first["task_id"])) == {
            "envelope_id": first["id"],
            "task_id": first["task_id"],
            "recipient_id": first["recipient_id"],
            "state": "leased",
            "delivery_count": 2,
        }
    finally:
        store.close()


def test_relay_store_terminal_prepare_is_hidden_until_atomic_commit(
    tmp_path: Path,
) -> None:
    lease_clock = _Clock()
    store = RelayStore(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        lease_clock_ns=lease_clock,
        evidence_clock_ns=_EvidenceClock(),
        uuid4=_UUIDs("40000000-0000-4000-8000-000000000001"),
    )
    command = _command()
    terminal = _terminal()
    try:
        store._submit_task(command)
        lease = store._acquire_lease("worker-1")
        assert lease is not None
        lease_id = str(lease["lease_id"])

        prepare_status, prepare_receipt = store._prepare_terminal(lease_id, terminal)
        repeat_status, repeat_receipt = store._prepare_terminal(lease_id, terminal)
        assert (prepare_status, repeat_status) == (201, 200)
        assert prepare_receipt["envelope_id"] == terminal["id"]
        assert type(prepare_receipt["accepted_ns"]) is int
        assert type(repeat_receipt["accepted_ns"]) is int
        assert repeat_receipt["accepted_ns"] > prepare_receipt["accepted_ns"]
        assert store._terminal_after(str(command["task_id"]), 0) is None
        assert store._health()["committed_terminal_sequence"] == 0

        changed = {**terminal, "id": "30000000-0000-4000-8000-000000000002"}
        with pytest.raises(server_module._RelayStoreError) as conflict:
            store._prepare_terminal(lease_id, changed)
        _assert_store_error(conflict, 409, "terminal_conflict")

        commit_status, committed = store._apply_disposition(lease_id, "commit")
        assert commit_status == 200
        assert committed == {
            "disposition": "commit",
            "envelope": terminal,
            "lease_id": lease_id,
            "state": "completed",
            "terminal_sequence": committed["terminal_sequence"],
        }
        assert type(committed["terminal_sequence"]) is int
        assert committed["terminal_sequence"] > 0
        assert store._terminal_after(str(command["task_id"]), 0) == {
            "delivery_count": 1,
            "envelope": terminal,
            "terminal_sequence": committed["terminal_sequence"],
        }
        assert store._apply_disposition(lease_id, "commit") == (
            200,
            committed,
        )
        completed_task = store.task(str(command["task_id"]))
        assert completed_task is not None
        assert completed_task["state"] == "completed"
    finally:
        store.close()


def test_relay_store_renew_retry_terminate_and_missing_prepare_matrix(
    tmp_path: Path,
) -> None:
    lease_clock = _Clock()
    uuids = _UUIDs(
        "40000000-0000-4000-8000-000000000001",
        "40000000-0000-4000-8000-000000000002",
        "40000000-0000-4000-8000-000000000003",
    )
    store = RelayStore(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        lease_ttl_ms=9,
        lease_clock_ns=lease_clock,
        uuid4=uuids,
    )
    try:
        command = _command()
        store._submit_task(command)
        first = store._acquire_lease("worker-1")
        assert first is not None
        first_id = str(first["lease_id"])

        lease_clock.value += 1
        assert store._apply_disposition(first_id, "renew") == (
            200,
            {
                "disposition": "renew",
                "lease_deadline_ns": lease_clock.value + 9_000_000,
                "lease_id": first_id,
                "lease_ttl_ms": 9,
                "renewal_interval_ms": 3,
                "state": "active",
            },
        )
        with pytest.raises(server_module._RelayStoreError) as missing:
            store._apply_disposition(first_id, "commit")
        _assert_store_error(missing, 409, "terminal_not_prepared")

        assert store._apply_disposition(first_id, "retry") == (
            200,
            {"disposition": "retry", "lease_id": first_id, "state": "queued"},
        )
        assert store._apply_disposition(first_id, "retry") == (
            200,
            {"disposition": "retry", "lease_id": first_id, "state": "queued"},
        )
        for disposition in ("renew", "terminate", "commit"):
            with pytest.raises(server_module._RelayStoreError) as stale:
                store._apply_disposition(first_id, disposition)
            _assert_store_error(stale, 409, "stale_lease")

        second = store._acquire_lease("worker-1")
        assert second is not None
        second_id = str(second["lease_id"])
        assert store._apply_disposition(second_id, "terminate") == (
            200,
            {
                "disposition": "terminate",
                "lease_id": second_id,
                "state": "terminated",
            },
        )
        assert store._apply_disposition(second_id, "terminate") == (
            200,
            {
                "disposition": "terminate",
                "lease_id": second_id,
                "state": "terminated",
            },
        )
        for disposition in ("renew", "retry", "commit"):
            with pytest.raises(server_module._RelayStoreError) as finalized:
                store._apply_disposition(second_id, disposition)
            _assert_store_error(finalized, 409, "lease_finalized")

        for disposition in ("renew", "retry", "terminate", "commit"):
            with pytest.raises(server_module._RelayStoreError) as unknown:
                store._apply_disposition("missing-lease", disposition)
            _assert_store_error(unknown, 404, "lease_not_found")
    finally:
        store.close()


@_typed_test_decorator(
    pytest.mark.parametrize(
        ("history", "historical_status", "historical_code"),
        [
            ("expired", 409, "lease_expired"),
            ("released", 200, "retry"),
        ],
    )
)
def test_historical_lease_operations_cannot_mutate_newer_committed_attempt(
    tmp_path: Path,
    history: str,
    historical_status: int,
    historical_code: str,
) -> None:
    lease_clock = _Clock()
    store = RelayStore(
        tmp_path / f"{history}.sqlite3",
        run_id="run-1",
        lease_ttl_ms=3,
        lease_clock_ns=lease_clock,
        evidence_clock_ns=_EvidenceClock(),
        uuid4=_UUIDs(
            "40000000-0000-4000-8000-000000000001",
            "40000000-0000-4000-8000-000000000002",
        ),
    )
    command = _command()
    terminal = _terminal()
    try:
        store._submit_task(command)
        old = store._acquire_lease("worker-1")
        assert old is not None
        old_id = str(old["lease_id"])
        store._prepare_terminal(old_id, terminal)
        if history == "expired":
            lease_clock.value = typing.cast(int, old["lease_deadline_ns"])
            with pytest.raises(server_module._RelayStoreError) as expired:
                store._apply_disposition(old_id, "renew")
            _assert_store_error(expired, 409, "lease_expired")
        else:
            assert store._apply_disposition(old_id, "retry")[0] == 200

        newer = store._acquire_lease("worker-1")
        assert newer is not None
        newer_id = str(newer["lease_id"])
        store._prepare_terminal(newer_id, terminal)

        def assert_old_history_is_read_only() -> None:
            before = (
                store.task(str(command["task_id"])),
                store._terminal_after(str(command["task_id"]), 0),
                store._health()["committed_terminal_sequence"],
            )
            for disposition in ("renew", "retry", "terminate", "commit"):
                if history == "released" and disposition == "retry":
                    status, body = store._apply_disposition(old_id, disposition)
                    assert status == historical_status
                    assert body == {
                        "disposition": historical_code,
                        "lease_id": old_id,
                        "state": "queued",
                    }
                else:
                    with pytest.raises(server_module._RelayStoreError) as rejected:
                        store._apply_disposition(old_id, disposition)
                    expected_code = (
                        historical_code if history == "expired" else "stale_lease"
                    )
                    _assert_store_error(rejected, 409, expected_code)
            after = (
                store.task(str(command["task_id"])),
                store._terminal_after(str(command["task_id"]), 0),
                store._health()["committed_terminal_sequence"],
            )
            assert after == before

        assert_old_history_is_read_only()
        _, committed = store._apply_disposition(newer_id, "commit")
        terminal_sequence = committed["terminal_sequence"]
        visible = store._terminal_after(str(command["task_id"]), 0)
        assert visible == {
            "delivery_count": 2,
            "envelope": terminal,
            "terminal_sequence": terminal_sequence,
        }
        assert_old_history_is_read_only()
        assert store._terminal_after(str(command["task_id"]), 0) == visible
        assert store._health()["committed_terminal_sequence"] == terminal_sequence
        assert store._connection.execute(
            "SELECT COUNT(*) FROM terminals"
        ).fetchone() == (2,)
    finally:
        store.close()


def test_relay_app_has_exact_routes_and_authenticates_before_routing(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    http_routes = {
        (route.path, method)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods or set()
    }
    websocket_routes = {
        route.path for route in app.routes if isinstance(route, WebSocketRoute)
    }
    assert http_routes == {
        ("/v1/tasks", "POST"),
        ("/v1/workers/{agent_id}/lease", "GET"),
        ("/v1/leases/{lease_id}/terminal", "POST"),
        ("/v1/leases/{lease_id}/commit", "POST"),
        ("/v1/tasks/{task_id}/terminal", "GET"),
        ("/v1/events", "POST"),
        ("/healthz", "GET"),
    }
    assert websocket_routes == {"/v1/events"}
    assert app.docs_url is app.redoc_url is app.openapi_url is None

    with TestClient(app) as client:
        unauthorized_cases = [
            ("GET", "/healthz"),
            ("GET", "/missing"),
            ("DELETE", "/healthz"),
            ("GET", f"/healthz?token={TOKEN}"),
            ("POST", "/v1/tasks"),
            ("GET", "/v1/workers/worker-1/lease?timeout_ms=0"),
            ("POST", "/v1/leases/missing/terminal"),
            ("POST", "/v1/leases/missing/commit"),
            ("GET", "/v1/tasks/missing/terminal"),
            ("POST", "/v1/events"),
        ]
        for method, path in unauthorized_cases:
            response = client.request(method, path)
            _assert_canonical_response(response, 401, {"error": "unauthorized"})

        health = client.get("/healthz", headers=AUTH_HEADERS)
        assert health.status_code == 200
        assert health.content == canonical_json(health.json())

        missing = client.get("/missing", headers=AUTH_HEADERS)
        _assert_canonical_response(missing, 404, {"error": "route_not_found"})
        wrong_method = client.delete("/healthz", headers=AUTH_HEADERS)
        _assert_canonical_response(
            wrong_method,
            405,
            {"error": "method_not_allowed"},
        )

        with (
            pytest.raises(WebSocketDisconnect) as unauthorized_socket,
            client.websocket_connect("/v1/events"),
        ):
            pass
        assert unauthorized_socket.value.code == 4401


@_typed_test_decorator(
    pytest.mark.parametrize(
        ("body", "status_code", "code"),
        [
            (b"{", 400, "invalid_json"),
            (b"\xef\xbb\xbf{}", 400, "invalid_json"),
            (b'{"v":1,"v":1}', 400, "invalid_json"),
            (b'{"value":NaN}', 400, "invalid_json"),
            (b'{"value":1e400}', 400, "invalid_json"),
            (b"[]", 400, "invalid_json"),
            (b'{"value":"\\ud800"}', 400, "invalid_json"),
            (b'{ "value": 1 }', 400, "noncanonical_json"),
            (b"{}", 422, "invalid_envelope"),
        ],
    )
)
def test_relay_app_rejects_invalid_and_noncanonical_json_exactly(
    tmp_path: Path,
    body: bytes,
    status_code: int,
    code: str,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    with TestClient(app) as client:
        response = client.post("/v1/tasks", content=body, headers=AUTH_HEADERS)
        _assert_canonical_response(response, status_code, {"error": code})


def test_relay_app_enforces_media_size_query_path_and_envelope_types(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    with TestClient(app) as client:
        unsupported = client.post(
            "/v1/tasks",
            content=canonical_json(_command()),
            headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "text/plain"},
        )
        _assert_canonical_response(
            unsupported,
            415,
            {"error": "unsupported_media_type"},
        )
        too_large = client.post(
            "/v1/tasks",
            content=b"x" * 1_048_577,
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            too_large,
            413,
            {"error": "request_too_large"},
        )
        wrong_task_type = _post_canonical(client, "/v1/tasks", _terminal())
        _assert_canonical_response(
            wrong_task_type,
            422,
            {"error": "invalid_envelope"},
        )
        wrong_event_type = _post_canonical(client, "/v1/events", _command())
        _assert_canonical_response(
            wrong_event_type,
            422,
            {"error": "invalid_envelope"},
        )
        for path, code in (
            (
                "/v1/workers/worker-1/lease?timeout_ms=0&timeout_ms=1",
                "unexpected_query",
            ),
            ("/v1/workers/worker-1/lease?other=1", "unexpected_query"),
            ("/v1/workers/worker-1/lease?timeout_ms=-1", "invalid_timeout_ms"),
            ("/v1/workers/worker-1/lease?timeout_ms=30001", "invalid_timeout_ms"),
            ("/v1/workers/bad.agent/lease?timeout_ms=0", "invalid_agent_id"),
            (
                (
                    "/v1/tasks/20000000-0000-4000-8000-000000000001/terminal"
                    "?after=0&after=1"
                ),
                "unexpected_query",
            ),
            (
                ("/v1/tasks/20000000-0000-4000-8000-000000000001/terminal?after=-1"),
                "invalid_cursor",
            ),
            ("/v1/tasks/not-a-uuid/terminal?after=0", "invalid_task_id"),
        ):
            response = client.get(path, headers=AUTH_HEADERS)
            _assert_canonical_response(response, 422, {"error": code})


def test_relay_app_rejects_huge_decimal_queries_exactly(tmp_path: Path) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    huge_decimal = "9" * 5_000
    task_id = "20000000-0000-4000-8000-000000000001"
    with TestClient(app) as client:
        timeout = client.get(
            f"/v1/workers/worker-1/lease?timeout_ms={huge_decimal}",
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            timeout,
            422,
            {"error": "invalid_timeout_ms"},
        )
        cursor = client.get(
            f"/v1/tasks/{task_id}/terminal?after={huge_decimal}",
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(cursor, 422, {"error": "invalid_cursor"})


@_typed_test_decorator(pytest.mark.asyncio)
async def test_relay_chunked_body_stops_after_limit_plus_one() -> None:
    chunks = [
        b"x" * server_module._MAX_BODY_BYTES,
        b"x",
        b"unread-private-sentinel",
    ]
    delivered_bytes = 0
    delivered_chunks = 0

    async def receive() -> Message:
        nonlocal delivered_bytes, delivered_chunks
        chunk = chunks.pop(0)
        delivered_bytes += len(chunk)
        delivered_chunks += 1
        return {
            "type": "http.request",
            "body": chunk,
            "more_body": bool(chunks),
        }

    scope = typing.cast(
        Scope,
        {
            "type": "http",
            "headers": [(b"content-type", b"application/json")],
        },
    )
    request = Request(scope, receive)
    with pytest.raises(server_module._HTTPError) as raised:
        await server_module._request_json(request)
    assert type(raised.value).__name__ == "_HTTPError"
    assert raised.value.status_code == 413
    assert raised.value.code == "request_too_large"
    assert delivered_bytes == server_module._MAX_BODY_BYTES + 1
    assert delivered_chunks == 2
    assert chunks == [b"unread-private-sentinel"]


def test_relay_app_routes_complete_task_and_expose_only_committed_terminal(
    tmp_path: Path,
) -> None:
    lease_clock = _Clock()
    evidence = _EvidenceClock()
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        lease_ttl_ms=3,
        lease_clock_ns=lease_clock,
        evidence_clock_ns=evidence,
        uuid4=_UUIDs("40000000-0000-4000-8000-000000000001"),
    )
    command = _command()
    terminal = _terminal()
    task_id = str(command["task_id"])
    with TestClient(app) as client:
        submitted = _post_canonical(client, "/v1/tasks", command)
        assert submitted.status_code == 201
        assert submitted.content == canonical_json(submitted.json())
        repeated = _post_canonical(client, "/v1/tasks", command)
        assert repeated.status_code == 200
        assert repeated.json()["accepted_ns"] > submitted.json()["accepted_ns"]

        changed = {**command, "payload": {"body": "changed"}}
        conflict = _post_canonical(client, "/v1/tasks", changed)
        _assert_canonical_response(
            conflict,
            409,
            {"error": "envelope_id_conflict"},
        )

        leased = client.get(
            "/v1/workers/worker-1/lease?timeout_ms=0",
            headers=AUTH_HEADERS,
        )
        assert leased.status_code == 200
        assert leased.content == canonical_json(leased.json())
        lease_id = leased.json()["lease_id"]
        assert leased.json()["envelope"] == command
        empty = client.get(
            "/v1/workers/worker-1/lease?timeout_ms=0",
            headers=AUTH_HEADERS,
        )
        assert empty.status_code == 204
        assert empty.content == b""

        prepared = _post_canonical(
            client,
            f"/v1/leases/{lease_id}/terminal",
            terminal,
        )
        assert prepared.status_code == 201
        repeated_prepare = _post_canonical(
            client,
            f"/v1/leases/{lease_id}/terminal",
            terminal,
        )
        assert repeated_prepare.status_code == 200
        hidden = client.get(
            f"/v1/tasks/{task_id}/terminal?after=0",
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            hidden,
            404,
            {"error": "terminal_not_found"},
        )

        committed = _post_canonical(
            client,
            f"/v1/leases/{lease_id}/commit",
            {"disposition": "commit"},
        )
        assert committed.status_code == 200
        observation = client.get(
            f"/v1/tasks/{task_id}/terminal?after=0",
            headers=AUTH_HEADERS,
        )
        assert observation.status_code == 200
        assert observation.json()["envelope"] == terminal
        cursor = observation.json()["terminal_sequence"]
        none_after = client.get(
            f"/v1/tasks/{task_id}/terminal?after={cursor}",
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            none_after,
            404,
            {"error": "terminal_not_found"},
        )
        health = client.get("/healthz", headers=AUTH_HEADERS)
        assert health.json() == {
            "ack_pending": 0,
            "committed_terminal_sequence": cursor,
            "message_count": 2,
            "pending": 0,
            "run_id": "run-1",
            "status": "ok",
            "storage_bytes": health.json()["storage_bytes"],
        }
        assert type(health.json()["storage_bytes"]) is int


def test_relay_event_websocket_is_authenticated_live_only_and_canonical(
    tmp_path: Path,
) -> None:
    evidence = _EvidenceClock()
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        evidence_clock_ns=evidence,
    )
    with TestClient(app) as client:
        with client.websocket_connect("/v1/events", headers=AUTH_HEADERS) as socket:
            assert socket.receive_bytes() == canonical_json(
                {"kind": "observer_ready", "run_id": "run-1"}
            )
            first = _progress()
            accepted = _post_canonical(client, "/v1/events", first)
            assert accepted.status_code == 202
            assert socket.receive_bytes() == canonical_json(first)

        lost = _progress(
            envelope_id="50000000-0000-4000-8000-000000000002",
            progress=10,
        )
        assert _post_canonical(client, "/v1/events", lost).status_code == 202

        with client.websocket_connect("/v1/events", headers=AUTH_HEADERS) as socket:
            assert socket.receive_bytes() == canonical_json(
                {"kind": "observer_ready", "run_id": "run-1"}
            )
            live = _progress(
                envelope_id="50000000-0000-4000-8000-000000000003",
                progress=15,
            )
            assert _post_canonical(client, "/v1/events", live).status_code == 202
            assert socket.receive_bytes() == canonical_json(live)


@_typed_test_decorator(pytest.mark.asyncio)
async def test_relay_event_socket_registers_only_after_ready_frame(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    route = next(
        route
        for route in app.routes
        if isinstance(route, WebSocketRoute) and route.path == "/v1/events"
    )
    endpoint = typing.cast(
        Callable[[object], Awaitable[None]],
        route.endpoint,
    )
    ready_send_started = asyncio.Event()
    release_ready_send = asyncio.Event()
    concurrent_send = asyncio.Event()

    class BlockingWebSocket:
        def __init__(self) -> None:
            self.scope = {"query_string": b""}
            self.accepted = False
            self.frames: list[bytes] = []

        async def accept(self) -> None:
            self.accepted = True

        async def send_bytes(self, frame: bytes) -> None:
            if not ready_send_started.is_set():
                ready_send_started.set()
                await release_ready_send.wait()
            else:
                concurrent_send.set()
            self.frames.append(frame)

        async def receive(self) -> Mapping[str, object]:
            return {"type": "websocket.disconnect"}

        async def close(self, code: int = 1000) -> None:
            del code

    socket = BlockingWebSocket()

    async def run_endpoint() -> None:
        await endpoint(socket)

    observer = asyncio.create_task(run_endpoint())
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    event = _progress()
    try:
        await ready_send_started.wait()
        response = await client.post(
            "/v1/events",
            headers=AUTH_HEADERS,
            content=canonical_json(event),
        )
        assert response.status_code == 202
        assert concurrent_send.is_set() is False
        assert socket not in app.state.event_sockets

        release_ready_send.set()
        await observer
        assert socket.accepted is True
        assert socket.frames == [
            canonical_json({"kind": "observer_ready", "run_id": "run-1"})
        ]
        assert socket not in app.state.event_sockets
    finally:
        release_ready_send.set()
        if not observer.done():
            await asyncio.gather(observer, return_exceptions=True)
        await client.aclose()
        typing.cast(RelayStore, app.state.relay_store).close()


@_typed_test_decorator(pytest.mark.parametrize("mode", [0o400, 0o600]))
def test_relay_credential_reader_accepts_exact_private_file(
    tmp_path: Path,
    mode: int,
) -> None:
    path = tmp_path / "credential"
    path.write_bytes(TOKEN.encode() + b"\n")
    path.chmod(mode)
    assert server_module._read_credential_file(path) == TOKEN


@_typed_test_decorator(
    pytest.mark.parametrize(
        ("mode", "contents"),
        [
            (0o644, TOKEN.encode() + b"\n"),
            (0o000, TOKEN.encode() + b"\n"),
            (0o600, b"A" * 64 + b"\n"),
            (0o600, b"a" * 64),
            (0o600, b"a" * 64 + b"\nextra"),
        ],
    )
)
def test_relay_credential_reader_rejects_insecure_or_malformed_file(
    tmp_path: Path,
    mode: int,
    contents: bytes,
) -> None:
    path = tmp_path / "credential"
    path.write_bytes(contents)
    path.chmod(mode)
    with pytest.raises(ValueError, match="invalid credential file") as raised:
        server_module._read_credential_file(path)
    assert TOKEN not in str(raised.value)
    assert str(path) not in str(raised.value)


def test_relay_credential_reader_rejects_symlink_and_replace_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "target"
    target.write_bytes(TOKEN.encode() + b"\n")
    target.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="invalid credential file"):
        server_module._read_credential_file(link)

    path = tmp_path / "credential"
    replacement = tmp_path / "replacement"
    backup = tmp_path / "backup"
    path.write_bytes(TOKEN.encode() + b"\n")
    replacement.write_bytes(("b" * 64).encode() + b"\n")
    path.chmod(0o600)
    replacement.chmod(0o600)
    real_open = os.open

    def replacing_open(target_path: str | os.PathLike[str], flags: int) -> int:
        path.rename(backup)
        replacement.rename(path)
        return real_open(target_path, flags)

    monkeypatch.setattr(os, "open", replacing_open)
    with pytest.raises(ValueError, match="invalid credential file"):
        server_module._read_credential_file(path)


def test_relay_environment_factory_reads_only_named_nonsecret_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential = tmp_path / "credential"
    credential.write_bytes(TOKEN.encode() + b"\n")
    credential.chmod(0o600)
    database = tmp_path / "relay.sqlite3"

    class GuardedEnvironment(dict[str, str]):
        def __getitem__(self, key: str) -> str:
            if key in {"TOKEN", "NATS_TOKEN", "RELAY_TOKEN"}:
                raise AssertionError(f"forbidden environment read: {key}")
            return super().__getitem__(key)

    environment = GuardedEnvironment(
        {
            "EC_RUN_ID": "run-1",
            "RELAY_DB_PATH": str(database),
            "EC_CREDENTIAL_FILE": str(credential),
            "NATS_TOKEN": "secret-sentinel",
        }
    )
    monkeypatch.setattr(os, "environ", environment)
    app = server_module.create_app_from_environment()
    try:
        assert app.state.relay_store.task("missing") is None
        assert TOKEN not in repr(app)
        assert TOKEN not in repr(app.user_middleware)
        assert str(credential) not in repr(app)
    finally:
        app.state.relay_store.close()


def test_relay_create_app_validates_token_before_opening_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "relay.sqlite3"
    with pytest.raises(ValueError, match="invalid token") as raised:
        server_module.create_app(
            database,
            run_id="run-1",
            token="secret-invalid-token",
        )
    assert "secret-invalid-token" not in str(raised.value)
    assert not database.exists()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_relay_http_401_is_exact_secret_free_permission_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "deadbeef" * 8

    def unauthorized(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(401, content=b'{"error":"source-secret-text"}')

    client = httpx.AsyncClient(transport=httpx.MockTransport(unauthorized))
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=secret,
        event_sink=_EventSink(),
        http_client=client,
    )
    caplog.set_level(logging.DEBUG)
    try:
        with pytest.raises(
            PermissionError,
            match=r"^transport authentication failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        rendered = str(raised.value) + repr(raised.value) + caplog.text
        assert secret not in rendered
        assert "source-secret-text" not in rendered
    finally:
        await transport.close()
        await client.aclose()


@_typed_test_decorator(pytest.mark.parametrize("lease_ttl_ms", [True, 2, 300_001, 3.0]))
def test_relay_store_rejects_invalid_lease_ttl(
    tmp_path: Path,
    lease_ttl_ms: object,
) -> None:
    with pytest.raises(ValueError, match="invalid lease_ttl_ms"):
        RelayStore(
            tmp_path / "relay.sqlite3",
            run_id="run-1",
            lease_ttl_ms=typing.cast(int, lease_ttl_ms),
        )


def test_relay_store_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.sqlite3"
    target.touch()
    database_path = tmp_path / "relay.sqlite3"
    os.symlink(target, database_path)
    with pytest.raises(ValueError, match="invalid database_path"):
        RelayStore(database_path, run_id="run-1")


def _mock_json_response(
    request: httpx.Request,
    status_code: int,
    body: Mapping[str, object] | None,
) -> httpx.Response:
    return httpx.Response(
        status_code,
        content=b"" if body is None else canonical_json(body),
        headers=({} if body is None else {"Content-Type": "application/json"}),
        request=request,
    )


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_transport_binds_lease_prepares_commits_and_emits_evidence(
    tmp_path: Path,
) -> None:
    lease_id = "60000000-0000-4000-8000-000000000001"
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        lease_clock_ns=_Clock(),
        evidence_clock_ns=_EvidenceClock(40_000),
        uuid4=_UUIDs(lease_id),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    sink = _RecordingEventSink()
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        http_client=client,
        evidence_clock_ns=_EvidenceClock(80_000),
        epoch_now=lambda: NOW,
    )
    request = _command()
    terminal = _terminal()
    try:
        await transport.start_terminal_observer()
        receipt = await transport.submit_task(request)
        assert receipt.envelope_id == request["id"]

        delivery = await transport.long_poll("worker-1", 0)
        assert delivery is not None
        assert delivery.worker_agent_id == "worker-1"
        assert delivery.raw == canonical_json(request)
        assert delivery.delivery_count == 1
        assert delivery.stream_sequence is None

        await delivery.in_progress()
        terminal_receipt = await transport.publish_terminal(terminal)
        assert terminal_receipt.envelope_id == terminal["id"]
        assert app.state.relay_store.task(str(request["task_id"]))["state"] == "leased"
        await delivery.commit()

        observation = await transport.observe_terminal(
            str(request["task_id"]),
            0,
        )
        assert observation is not None
        assert observation.envelope == terminal
        assert observation.observation_index == 1
        assert observation.stream_sequence is None
        assert observation.delivery_count == 1
        assert observation.replayed is False
        assert observation.delivery is None
        assert await transport.observe_terminal(str(request["task_id"]), 0) is None
        with pytest.raises(
            RuntimeError,
            match=r"^no active relay execution$",
        ):
            await transport.publish_terminal(terminal)

        snapshot = await transport.inspect_state()
        assert snapshot.mode is Mode.CENTRAL_RELAY
        assert snapshot.streams == {}
        assert snapshot.consumers == {}
        assert snapshot.pending == 0
        assert snapshot.ack_pending == 0
        assert snapshot.message_count == 2
        assert snapshot.storage_bytes > 0
        exchanges = [
            event["data"]
            for event in sink.events
            if event["event"] == "transport.http_exchange"
        ]
        assert snapshot.connection_bytes == {
            "request_body_bytes": sum(
                typing.cast(Mapping[str, int], event)["request_body_bytes"]
                for event in exchanges
            ),
            "response_body_bytes": sum(
                typing.cast(Mapping[str, int], event)["response_body_bytes"]
                for event in exchanges
            ),
        }
        assert all(
            set(event)
            == {
                "component",
                "data",
                "epoch_time",
                "event",
                "monotonic_ns",
            }
            for event in sink.events
        )
        worker_event = next(
            event
            for event in sink.events
            if event["event"] == "transport.worker_delivery"
        )
        assert worker_event["data"] == {
            "delivery_count": 1,
            "lease_id": lease_id,
            "raw_sha256": hashlib.sha256(canonical_json(request)).hexdigest(),
            "stream_sequence": None,
            "worker_agent_id": "worker-1",
        }
        terminal_event = next(
            event
            for event in sink.events
            if event["event"] == "transport.terminal_observed"
        )
        assert terminal_event["data"] == {
            "delivery_count": 1,
            "envelope_id": terminal["id"],
            "observation_index": 1,
            "replayed": False,
            "stream_sequence": None,
            "task_id": request["task_id"],
        }
        rendered = json.dumps(sink.events, sort_keys=True)
        assert TOKEN not in rendered
        assert "edgecitadel:nonce" not in rendered
    finally:
        await transport.close()
        assert client.is_closed is False
        await client.aclose()
        app.state.relay_store.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_binding_rejects_absent_foreign_and_mismatched_publication(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        uuid4=_UUIDs("60000000-0000-4000-8000-000000000001"),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    terminal = _terminal()
    try:
        with pytest.raises(
            RuntimeError,
            match=r"^no active relay execution$",
        ):
            await transport.publish_terminal(terminal)
        await transport.submit_task(_command())
        delivery = await transport.long_poll("worker-1", 0)
        assert delivery is not None

        async def publish_from_foreign_task() -> None:
            await transport.publish_terminal(terminal)

        with pytest.raises(
            RuntimeError,
            match=r"^relay execution binding mismatch$",
        ):
            await asyncio.create_task(publish_from_foreign_task())
        with pytest.raises(
            RuntimeError,
            match=r"^relay execution binding mismatch$",
        ):
            await asyncio.create_task(delivery.in_progress())
        mismatched = _terminal(task_id="20000000-0000-4000-8000-000000000002")
        with pytest.raises(
            ValueError,
            match=r"^terminal does not match relay lease$",
        ):
            await transport.publish_terminal(mismatched)
        assert app.state.relay_store._connection.execute(
            "SELECT COUNT(*) FROM terminals"
        ).fetchone() == (0,)

        await transport.publish_terminal(terminal)
        await delivery.retry()
        with pytest.raises(
            RuntimeError,
            match=r"^no active relay execution$",
        ):
            await transport.publish_terminal(terminal)
    finally:
        await transport.close()
        await client.aclose()
        app.state.relay_store.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_context_bindings_are_isolated_between_concurrent_tasks(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        uuid4=_UUIDs(
            "60000000-0000-4000-8000-000000000001",
            "60000000-0000-4000-8000-000000000002",
        ),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    first = _command()
    second = _command(
        envelope_id="10000000-0000-4000-8000-000000000002",
        task_id="20000000-0000-4000-8000-000000000002",
    )
    acquired: list[str] = []
    both_acquired = asyncio.Event()

    async def execute_one(
        request: Mapping[str, object],
        terminal: Mapping[str, object],
    ) -> None:
        delivery = await transport.long_poll("worker-1", 0)
        assert delivery is not None
        acquired.append(str(request["task_id"]))
        if len(acquired) == 2:
            both_acquired.set()
        await both_acquired.wait()
        await transport.publish_terminal(terminal)
        await delivery.commit()

    try:
        await transport.submit_task(first)
        await transport.submit_task(second)
        await asyncio.gather(
            execute_one(first, _terminal()),
            execute_one(
                second,
                _terminal(
                    terminal_id="30000000-0000-4000-8000-000000000002",
                    task_id=str(second["task_id"]),
                ),
            ),
        )
        assert app.state.relay_store.task(str(first["task_id"]))["state"] == "completed"
        assert (
            app.state.relay_store.task(str(second["task_id"]))["state"] == "completed"
        )
    finally:
        await transport.close()
        await client.aclose()
        app.state.relay_store.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_terminal_cursor_classifies_replay_without_advancing_on_404(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        uuid4=_UUIDs(
            "60000000-0000-4000-8000-000000000001",
            "60000000-0000-4000-8000-000000000002",
        ),
    )
    store = app.state.relay_store
    request = _command()
    terminal = _terminal()
    store._submit_task(request)
    first_lease = store._acquire_lease("worker-1")
    assert first_lease is not None
    first_lease_id = typing.cast(str, first_lease["lease_id"])
    store._prepare_terminal(first_lease_id, terminal)
    store._apply_disposition(first_lease_id, "commit")

    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    sink = _RecordingEventSink()
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        http_client=client,
    )
    try:
        await transport.start_terminal_observer()
        replay = await transport.observe_terminal(str(request["task_id"]), 0)
        assert replay is not None
        assert replay.observation_index == 1
        assert replay.replayed is True
        assert await transport.observe_terminal(str(request["task_id"]), 0) is None

        retry_wire = _command(envelope_id="10000000-0000-4000-8000-000000000002")
        store._submit_task(retry_wire)
        second_lease = store._acquire_lease("worker-1")
        assert second_lease is not None
        second_lease_id = typing.cast(str, second_lease["lease_id"])
        store._prepare_terminal(second_lease_id, terminal)
        store._apply_disposition(second_lease_id, "commit")

        live = await transport.observe_terminal(str(request["task_id"]), 0)
        assert live is not None
        assert live.observation_index == 2
        assert live.replayed is False
        assert live.envelope == terminal
    finally:
        await transport.close()
        await client.aclose()
        store.close()


class _FakeRelaySocket:
    def __init__(self) -> None:
        self.frames: asyncio.Queue[bytes] = asyncio.Queue()
        self.frames.put_nowait(
            canonical_json({"kind": "observer_ready", "run_id": "run-1"})
        )
        self.closed = False

    async def recv(self) -> bytes:
        return await self.frames.get()

    async def close(self) -> None:
        self.closed = True


class _FakeSocketContext:
    def __init__(self, socket: _FakeRelaySocket) -> None:
        self.socket = socket

    async def __aenter__(self) -> object:
        return self.socket

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None:
        del exc_type, exc, traceback
        await self.socket.close()


class _FakeWebSocketConnector:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, object]]] = []
        self.sockets: list[_FakeRelaySocket] = []

    def __call__(self, url: str, **kwargs: object) -> _FakeSocketContext:
        self.calls.append((url, dict(kwargs)))
        socket = _FakeRelaySocket()
        self.sockets.append(socket)
        return _FakeSocketContext(socket)


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_progress_socket_is_live_only_and_reconnects_fresh() -> None:
    connector = _FakeWebSocketConnector()
    sink = _RecordingEventSink()
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid/base",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        websocket_connect=connector,
        evidence_clock_ns=_EvidenceClock(),
        epoch_now=lambda: NOW,
    )
    first = _progress()
    lost = _progress(
        envelope_id="50000000-0000-4000-8000-000000000002",
        progress=10,
    )
    second = _progress(
        envelope_id="50000000-0000-4000-8000-000000000003",
        progress=15,
    )
    try:
        await transport.start_progress_observer()
        assert connector.calls == [
            (
                "wss://relay.invalid/base/v1/events",
                {
                    "additional_headers": {"Authorization": f"Bearer {TOKEN}"},
                    "max_size": 1_048_576,
                    "proxy": None,
                },
            )
        ]
        connector.sockets[0].frames.put_nowait(canonical_json(first))
        for _ in range(100):
            if any(
                event["event"] == "transport.transient_observed"
                for event in sink.events
            ):
                break
            await asyncio.sleep(0)

        await transport.faults.disconnect_progress_observer()
        connector.sockets[0].frames.put_nowait(canonical_json(lost))
        await transport.faults.reconnect_progress_observer()
        connector.sockets[1].frames.put_nowait(canonical_json(second))
        for _ in range(100):
            observed_ids = [
                typing.cast(Mapping[str, object], event["data"])["envelope_id"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed_ids) == 2:
                break
            await asyncio.sleep(0)
        assert observed_ids == [first["id"], second["id"]]
        assert all(
            typing.cast(Mapping[str, object], event["data"])["replayed"] is False
            for event in sink.events
            if event["event"] == "transport.transient_observed"
        )
    finally:
        await transport.close()
    assert all(socket.closed for socket in connector.sockets)


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_restart_quiesces_and_restores_progress_observer() -> None:
    connector = _FakeWebSocketConnector()
    sink = _RecordingEventSink()
    transport: CentralRelayTransport

    async def restart() -> str:
        assert len(connector.sockets) == 1
        assert connector.sockets[0].closed is True
        assert transport._background_failure is None
        return "https://replacement.invalid/base"

    transport = CentralRelayTransport(
        relay_url="https://stable.invalid/base",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        coordinator_restart=restart,
        websocket_connect=connector,
        evidence_clock_ns=_EvidenceClock(),
        epoch_now=lambda: NOW,
    )
    first = _progress()
    second = _progress(
        envelope_id="50000000-0000-4000-8000-000000000002",
        progress=10,
    )
    try:
        await transport.start_progress_observer()
        connector.sockets[0].frames.put_nowait(canonical_json(first))
        for _ in range(100):
            if any(
                event["event"] == "transport.transient_observed"
                for event in sink.events
            ):
                break
            await asyncio.sleep(0)

        await transport.faults.restart_coordinator()
        assert [url for url, _ in connector.calls] == [
            "wss://stable.invalid/base/v1/events",
            "wss://replacement.invalid/base/v1/events",
        ]
        assert transport._background_failure is None

        connector.sockets[1].frames.put_nowait(canonical_json(second))
        observed_indexes: list[object] = []
        for _ in range(100):
            observed_indexes = [
                typing.cast(Mapping[str, object], event["data"])["observation_index"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed_indexes) == 2:
                break
            await asyncio.sleep(0)
        assert observed_indexes == [1, 2]
    finally:
        await transport.close()
    assert all(socket.closed for socket in connector.sockets)


@_typed_test_decorator(
    pytest.mark.parametrize(
        "replacement_url",
        ["https://replacement.invalid?", "https://replacement.invalid#"],
    )
)
@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_restart_rejects_empty_query_or_fragment_delimiter(
    replacement_url: str,
) -> None:
    async def restart() -> str:
        return replacement_url

    transport = CentralRelayTransport(
        relay_url="https://stable.invalid/base",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        coordinator_restart=restart,
    )
    try:
        with pytest.raises(ValueError, match=r"^invalid relay_url$"):
            await transport.faults.restart_coordinator()
        assert transport._relay_url == "https://stable.invalid/base"
    finally:
        await transport.close()


class _BlockingExecutor:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.block = asyncio.Event()

    async def execute(self, delivery: object) -> None:
        del delivery
        self.started.set()
        await self.block.wait()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_receiver_reports_ready_and_auto_renews_at_server_cadence() -> (
    None
):
    request = _command()
    lease_id = "60000000-0000-4000-8000-000000000001"
    lease_sent = False
    renewal_seen = asyncio.Event()
    renewal_gate = asyncio.Event()
    delays: list[float] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal lease_sent
        if http_request.method == "GET" and http_request.url.path.endswith("/lease"):
            assert dict(http_request.url.params) == {"timeout_ms": "0"}
            assert lease_sent is False
            lease_sent = True
            return _mock_json_response(
                http_request,
                200,
                {
                    "delivery_count": 1,
                    "envelope": request,
                    "lease_deadline_ns": 999,
                    "lease_id": lease_id,
                    "lease_ttl_ms": 3,
                    "renewal_interval_ms": 1,
                    "stream_sequence": None,
                },
            )
        body = json.loads(http_request.content)
        assert body == {"disposition": "renew"}
        renewal_seen.set()
        return _mock_json_response(
            http_request,
            200,
            {
                "disposition": "renew",
                "lease_deadline_ns": 1_000,
                "lease_id": lease_id,
                "lease_ttl_ms": 3,
                "renewal_interval_ms": 1,
                "state": "active",
            },
        )

    async def controlled_sleep(delay: float) -> None:
        delays.append(delay)
        await renewal_gate.wait()
        renewal_gate.clear()

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    sink = _RecordingEventSink()
    executor = _BlockingExecutor()
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        http_client=client,
        sleep=controlled_sleep,
    )
    typed_executor = typing.cast(TaskExecutor, executor)
    try:
        await transport.start_receiver("worker-1", typed_executor)
        wait_receiver_ready = typing.cast(
            Callable[[str, float], Awaitable[object]],
            transport.wait_receiver_ready,
        )
        assert await wait_receiver_ready("worker-1", 1) is None
        await asyncio.wait_for(executor.started.wait(), timeout=1)
        with pytest.raises(RuntimeError, match="receiver already started"):
            await transport.start_receiver("worker-1", typed_executor)
        with pytest.raises(RuntimeError, match="receiver agent mismatch"):
            await transport.wait_receiver_ready("worker-2", 1)
        renewal_gate.set()
        await asyncio.wait_for(renewal_seen.wait(), timeout=1)
        assert delays[0] == 0.001
        ready = [
            event
            for event in sink.events
            if event["event"] == "transport.receiver_ready"
        ]
        assert len(ready) == 1
        assert ready[0]["data"] == {
            "agent_id": "worker-1",
            "kind": "receiver",
        }
    finally:
        await transport.close()
        await client.aclose()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_receiver_background_failure_is_rethrown() -> None:
    failed = asyncio.Event()

    class FailingExecutor:
        async def execute(self, delivery: object) -> None:
            del delivery
            failed.set()
            raise RuntimeError("receiver exploded")

    def handler(request: httpx.Request) -> httpx.Response:
        return _mock_json_response(
            request,
            200,
            {
                "delivery_count": 1,
                "envelope": _command(),
                "lease_deadline_ns": 999,
                "lease_id": "60000000-0000-4000-8000-000000000001",
                "lease_ttl_ms": 30_000,
                "renewal_interval_ms": 10_000,
                "stream_sequence": None,
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    await transport.start_receiver(
        "worker-1",
        typing.cast(TaskExecutor, FailingExecutor()),
    )
    await transport.wait_receiver_ready("worker-1", 1)
    await asyncio.wait_for(failed.wait(), timeout=1)
    assert transport._receiver_task is not None
    with pytest.raises(RuntimeError, match=r"^receiver exploded$"):
        await transport._receiver_task
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match=r"^receiver exploded$"):
        await transport.inspect_state()
    with pytest.raises(RuntimeError, match=r"^receiver exploded$"):
        await transport.close()
    await client.aclose()


def test_relay_reopen_rejects_forbidden_sqlite_internal_objects(
    tmp_path: Path,
) -> None:
    database = tmp_path / "relay.sqlite3"
    RelayStore(database, run_id="run-1").close()
    connection = sqlite3.connect(database)
    connection.execute("ANALYZE")
    connection.close()
    with pytest.raises(RuntimeError, match=r"^relay schema mismatch$"):
        RelayStore(database, run_id="run-1")


def test_relay_reopen_rejects_null_sql_forbidden_sqlite_object(
    tmp_path: Path,
) -> None:
    database = tmp_path / "relay.sqlite3"
    RelayStore(database, run_id="run-1").close()
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA writable_schema = ON")
    connection.execute(
        """
        INSERT INTO sqlite_schema(type, name, tbl_name, rootpage, sql)
        VALUES ('index', 'sqlite_forbidden', 'tasks', 0, NULL)
        """
    )
    connection.execute("PRAGMA writable_schema = OFF")
    connection.commit()
    connection.close()
    with pytest.raises(RuntimeError, match=r"^relay schema mismatch$"):
        RelayStore(database, run_id="run-1")


def test_relay_auth_precedes_routing_and_queries_fail_closed(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    with TestClient(app) as client:
        query_token = client.get(
            f"/healthz/?token={TOKEN}",
            follow_redirects=False,
        )
        _assert_canonical_response(
            query_token,
            401,
            {"error": "unauthorized"},
        )
        trailing = client.get(
            "/healthz/",
            headers=AUTH_HEADERS,
            follow_redirects=False,
        )
        _assert_canonical_response(
            trailing,
            404,
            {"error": "route_not_found"},
        )
        unexpected = client.post(
            "/v1/tasks?extra=1",
            content=canonical_json(_command()),
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            unexpected,
            422,
            {"error": "unexpected_query"},
        )
        with (
            pytest.raises(WebSocketDisconnect) as unmatched,
            client.websocket_connect("/v1/not-a-route"),
        ):
            pass
        assert unmatched.value.code == 4401


def test_relay_maps_unavailability_and_rejects_cursor_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    with TestClient(app) as client:
        overflow = client.get(
            (
                "/v1/tasks/20000000-0000-4000-8000-000000000001/terminal"
                "?after=9223372036854775808"
            ),
            headers=AUTH_HEADERS,
        )
        _assert_canonical_response(
            overflow,
            422,
            {"error": "invalid_cursor"},
        )

        def unavailable() -> Mapping[str, object]:
            raise sqlite3.OperationalError("source-secret-text")

        monkeypatch.setattr(app.state.relay_store, "_health", unavailable)
        health = client.get("/healthz", headers=AUTH_HEADERS)
        _assert_canonical_response(
            health,
            503,
            {"error": "relay_unavailable"},
        )
        assert "source-secret-text" not in health.text


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_non_auth_http_error_is_sanitized_but_preserves_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    endpoint = "https://private-relay.invalid/private-prefix"
    source_text = "source-secret-text"

    def failed(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            content=source_text.encode(),
            request=request,
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(failed))
    transport = CentralRelayTransport(
        relay_url=endpoint,
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    caplog.set_level(logging.DEBUG)
    try:
        with pytest.raises(httpx.HTTPStatusError) as raised:
            await transport.submit_task(_command())
        error = raised.value
        rendered = "".join(
            (
                str(error),
                repr(error),
                repr(error.request.headers),
                str(error.request.url),
                repr(error.response.content),
                caplog.text,
            )
        )
        for forbidden in (
            TOKEN,
            "private-relay.invalid",
            "private-prefix",
            "/v1/tasks",
            source_text,
            "Authorization",
        ):
            assert forbidden not in rendered
    finally:
        await transport.close()
        await client.aclose()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_transport_error_is_sanitized_and_httpcore_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    endpoint = "https://private-relay.invalid/private-prefix"
    source_text = "source-secret-text /private/socket/path"
    source: httpx.ConnectError | None = None

    def failed(request: httpx.Request) -> httpx.Response:
        nonlocal source
        logging.getLogger("httpcore.connection").debug(
            "connect_tcp host=private-relay.invalid path=/private/socket/path"
        )
        source = httpx.ConnectError(source_text, request=request)
        raise source

    client = httpx.AsyncClient(transport=httpx.MockTransport(failed))
    transport = CentralRelayTransport(
        relay_url=endpoint,
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    caplog.set_level(logging.DEBUG)
    try:
        with pytest.raises(
            httpx.ConnectError,
            match=r"^relay transport failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert source is not None
        assert raised.value is not source
        assert type(raised.value) is type(source)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        production_frames = [
            frame
            for frame, _ in traceback.walk_tb(raised.value.__traceback__)
            if frame.f_code.co_filename.endswith("/central_relay.py")
        ]
        assert production_frames
        assert all(source not in frame.f_locals.values() for frame in production_frames)
        rendered = str(raised.value) + repr(raised.value) + caplog.text
        for forbidden in (
            TOKEN,
            "private-relay.invalid",
            "private-prefix",
            "/v1/tasks",
            source_text,
            "Authorization",
        ):
            assert forbidden not in rendered
    finally:
        await transport.close()
        await client.aclose()


@_typed_test_decorator(pytest.mark.asyncio)
@_typed_test_decorator(pytest.mark.parametrize("failure_phase", ["connector", "recv"]))
async def test_central_websocket_failure_is_sanitized_and_logs_are_silent(
    failure_phase: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    endpoint = "https://private-relay.invalid/private-prefix"
    source_text = (
        f"websocket failure at {endpoint}/v1/events Authorization: Bearer {TOKEN}"
    )
    source = RuntimeError(source_text)

    def log_private_details() -> None:
        logging.getLogger("websockets.client").debug(source_text)

    class FailingSocket:
        async def recv(self) -> object:
            log_private_details()
            raise source

    class FailingContext:
        async def __aenter__(self) -> object:
            return FailingSocket()

        async def __aexit__(
            self,
            exc_type: object,
            exc: object,
            trace: object,
        ) -> None:
            del exc_type, exc, trace

    class FailingConnector:
        def __call__(
            self,
            url: str,
            **kwargs: object,
        ) -> AsyncContextManager[object]:
            del url, kwargs
            if failure_phase == "connector":
                log_private_details()
                raise source
            return FailingContext()

    transport = CentralRelayTransport(
        relay_url=endpoint,
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        websocket_connect=FailingConnector(),
    )
    caplog.set_level(logging.DEBUG)
    close_failure: BaseException | None = None
    try:
        with pytest.raises(
            RuntimeError,
            match=r"^relay websocket failed$",
        ) as started:
            await transport.start_progress_observer()
        assert started.value is not source
        assert started.value.__cause__ is None
        assert started.value.__context__ is None
        await asyncio.sleep(0)
        assert transport._background_failure is started.value

        production_frames = [
            frame
            for frame, _ in traceback.walk_tb(started.value.__traceback__)
            if frame.f_code.co_filename.endswith("/central_relay.py")
        ]
        assert production_frames
        assert all(source not in frame.f_locals.values() for frame in production_frames)

        with pytest.raises(RuntimeError) as operation:
            await transport.inspect_state()
        assert operation.value is started.value

        with pytest.raises(RuntimeError) as closed:
            await transport.close()
        close_failure = closed.value
        assert closed.value is started.value
        rendered = (
            str(started.value)
            + repr(started.value)
            + repr(transport._background_failure)
            + caplog.text
        )
        for forbidden in (
            TOKEN,
            "private-relay.invalid",
            "private-prefix",
            "/v1/events",
            "Authorization",
            source_text,
        ):
            assert forbidden not in rendered
    finally:
        if close_failure is None:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_websocket_context_cannot_suppress_receive_failure() -> None:
    source = RuntimeError("private receive failure")

    class FailingSocket:
        async def recv(self) -> object:
            raise source

    class SuppressingContext:
        async def __aenter__(self) -> object:
            return FailingSocket()

        async def __aexit__(
            self,
            exc_type: object,
            exc: object,
            trace: object,
        ) -> bool:
            del exc_type, exc, trace
            return True

    def connect(
        url: str,
        **kwargs: object,
    ) -> AsyncContextManager[object]:
        del url, kwargs
        return SuppressingContext()

    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        websocket_connect=connect,
    )
    try:
        with pytest.raises(
            RuntimeError,
            match=r"^relay websocket failed$",
        ) as raised:
            await asyncio.wait_for(
                transport.start_progress_observer(),
                timeout=0.1,
            )
        assert raised.value is not source
        assert transport._background_failure is raised.value
    finally:
        await asyncio.gather(transport.close(), return_exceptions=True)


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_cancelled_owned_client_close_remains_retryable() -> None:
    close_started = asyncio.Event()

    class CancelledCloseClient:
        def __init__(self) -> None:
            self.close_calls = 0
            self.closed = False

        async def aclose(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                close_started.set()
                await asyncio.Event().wait()
            self.closed = True

    client = CancelledCloseClient()
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
    )
    transport._http_client = typing.cast(httpx.AsyncClient, client)
    closing = asyncio.create_task(transport.close())
    try:
        await close_started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert transport._closed is False
        assert client.close_calls == 1
        assert client.closed is False

        await transport.close()
        assert transport._closed is True
        assert client.close_calls == 2
        assert client.closed is True
    finally:
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


def test_central_structured_websocket_failure_remains_safe_and_usable() -> None:
    source = ConnectionClosedError(
        WebSocketClose(1008, f"private endpoint and token {TOKEN}"),
        None,
        None,
    )
    sanitized = relay_module._sanitized_failure(
        source,
        "relay websocket failed",
    )
    assert type(sanitized) is type(source)
    assert sanitized is not source
    assert sanitized.__cause__ is None
    assert sanitized.__context__ is None
    rendered = str(sanitized) + repr(sanitized)
    assert "relay websocket failed" in rendered
    assert TOKEN not in rendered
    assert "private endpoint" not in rendered


@_typed_test_decorator(pytest.mark.asyncio)
@_typed_test_decorator(
    pytest.mark.parametrize(
        "timeout_s",
        (0.0009, 30.0009, 1e308, 10**10_000),
        ids=(
            "fractional-millisecond",
            "fractional-over-cap",
            "huge-float",
            "huge-int",
        ),
    )
)
async def test_central_long_poll_rejects_timeout_outside_server_contract(
    timeout_s: float,
) -> None:
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
    )
    try:
        with pytest.raises(ValueError, match=r"^invalid timeout_s$"):
            await transport.long_poll("worker-1", timeout_s)
    finally:
        await transport.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_owned_client_supports_full_long_poll_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class OwnedClient:
        async def request(
            self,
            method: str,
            url: str,
            *,
            content: bytes | None = None,
            headers: Mapping[str, str] | None = None,
        ) -> httpx.Response:
            request = httpx.Request(
                method,
                url,
                content=content,
                headers=headers,
            )
            return httpx.Response(204, request=request)

        async def aclose(self) -> None:
            return None

    def client_factory(**kwargs: object) -> OwnedClient:
        captured.update(kwargs)
        return OwnedClient()

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
    )
    try:
        assert await transport.long_poll("worker-1", 30) is None
        timeout = captured["timeout"]
        assert isinstance(timeout, httpx.Timeout)
        assert timeout.read is not None and timeout.read > 30
        assert captured["trust_env"] is False
    finally:
        await transport.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_receiver_readiness_requires_authenticated_exchange() -> None:
    def unauthorized(request: httpx.Request) -> httpx.Response:
        return _mock_json_response(
            request,
            401,
            {"error": "unauthorized"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(unauthorized))
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    executor = _BlockingExecutor()
    await transport.start_receiver(
        "worker-1",
        typing.cast(TaskExecutor, executor),
    )
    with pytest.raises(
        PermissionError,
        match=r"^transport authentication failed$",
    ):
        await transport.wait_receiver_ready("worker-1", 1)
    assert executor.started.is_set() is False
    with pytest.raises(
        PermissionError,
        match=r"^transport authentication failed$",
    ):
        await transport.close()
    await client.aclose()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_app_401_normalizes_without_source_context(
    tmp_path: Path,
) -> None:
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token="c" * 64,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    try:
        with pytest.raises(
            PermissionError,
            match=r"^transport authentication failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
    finally:
        await transport.close()
        await client.aclose()
        app.state.relay_store.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_accepts_schema_valid_uppercase_task_uuid(
    tmp_path: Path,
) -> None:
    task_id = "ABCDEF00-0000-4000-8000-000000000001"
    lease_id = "60000000-0000-4000-8000-000000000001"
    app = server_module.create_app(
        tmp_path / "relay.sqlite3",
        run_id="run-1",
        token=TOKEN,
        uuid4=_UUIDs(lease_id),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
    )
    request = _command(task_id=task_id)
    terminal = _terminal(task_id=task_id)
    try:
        await transport.start_terminal_observer()
        await transport.submit_task(request)
        delivery = await transport.long_poll("worker-1", 0)
        assert delivery is not None
        await transport.publish_terminal(terminal)
        await delivery.commit()
        observed = await transport.observe_terminal(task_id, 0)
        assert observed is not None
        assert observed.envelope == terminal
    finally:
        await transport.close()
        await client.aclose()
        app.state.relay_store.close()


@_typed_test_decorator(pytest.mark.asyncio)
async def test_central_successful_commit_wins_over_inflight_auto_renew() -> None:
    request = _command()
    terminal = _terminal()
    lease_id = "60000000-0000-4000-8000-000000000001"
    renew_started = asyncio.Event()
    commit_accepted = asyncio.Event()

    async def handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.method == "GET":
            return _mock_json_response(
                http_request,
                200,
                {
                    "delivery_count": 1,
                    "envelope": request,
                    "lease_deadline_ns": 999,
                    "lease_id": lease_id,
                    "lease_ttl_ms": 3,
                    "renewal_interval_ms": 1,
                    "stream_sequence": None,
                },
            )
        disposition = json.loads(http_request.content)["disposition"]
        if disposition == "commit":
            await asyncio.wait_for(renew_started.wait(), timeout=1)
            commit_accepted.set()
            return _mock_json_response(
                http_request,
                200,
                {
                    "disposition": "commit",
                    "envelope": terminal,
                    "lease_id": lease_id,
                    "state": "completed",
                    "terminal_sequence": 1,
                },
            )
        assert disposition == "renew"
        renew_started.set()
        await asyncio.wait_for(commit_accepted.wait(), timeout=1)
        return _mock_json_response(
            http_request,
            409,
            {"error": "lease_finalized"},
        )

    async def yield_once(_: float) -> None:
        await asyncio.sleep(0)

    class CommitExecutor:
        async def execute(self, delivery: object) -> None:
            await asyncio.wait_for(renew_started.wait(), timeout=1)
            await typing.cast(typing.Any, delivery).commit()

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = CentralRelayTransport(
        relay_url="https://relay.invalid",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        http_client=client,
        sleep=yield_once,
    )
    try:
        delivery = await transport.long_poll("worker-1", 0)
        assert isinstance(delivery, relay_module._RelayDelivery)
        binding = transport._binding.get()
        assert binding is not None
        await transport._execute_with_renewal(
            typing.cast(TaskExecutor, CommitExecutor()),
            delivery,
            binding,
        )
        assert binding.active is False
    finally:
        await transport.close()
        await client.aclose()


@_typed_test_decorator(pytest.mark.asyncio)
@_typed_test_decorator(
    pytest.mark.parametrize(
        ("crash_point", "first_publish_state"),
        [
            ("after-result-publish-before-publish-mark", "prepared"),
            ("after-publish-mark-before-inbound-commit", "published"),
        ],
    )
)
async def test_central_real_executor_recovers_across_publication_crashes(
    tmp_path: Path,
    crash_point: str,
    first_publish_state: str,
) -> None:
    task_id = "20000000-0000-4000-8000-000000000001"
    terminal_id = "30000000-0000-4000-8000-000000000001"
    first_lease_id = "60000000-0000-4000-8000-000000000001"
    second_lease_id = "60000000-0000-4000-8000-000000000002"
    lease_clock = _Clock()
    relay_database = tmp_path / "relay.sqlite3"
    outcome_database = tmp_path / "outcomes.sqlite3"
    app = server_module.create_app(
        relay_database,
        run_id="run-1",
        token=TOKEN,
        lease_ttl_ms=3,
        lease_clock_ns=lease_clock,
        uuid4=_UUIDs(first_lease_id, second_lease_id),
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://relay.test",
    )
    sink = _RecordingEventSink()
    executor_clock = _ExecutorClock()
    handler = _EchoHandler()
    policy = _AcceptPolicy()
    first_crash = _CrashHook(crash_point)
    first_store = SQLiteOutcomeStore(outcome_database)
    first_transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        http_client=client,
    )
    first_executor = TaskExecutor(
        worker_agent_id="worker-1",
        handler=handler,
        outcome_store=first_store,
        terminal_publisher=first_transport,
        progress_publisher=first_transport,
        policy=policy,
        event_sink=sink,
        clock=executor_clock,
        uuid_factory=_UUIDs(terminal_id),
        crash_hook=first_crash,
    )
    key = OutcomeKey("worker-1", task_id)
    try:
        await first_transport.start_terminal_observer()
        await first_transport.submit_task(_command())
        first_delivery = await first_transport.long_poll("worker-1", 0)
        assert first_delivery is not None
        assert first_delivery.delivery_count == 1

        with pytest.raises(InjectedCrash, match=crash_point):
            await first_executor.execute(first_delivery)

        first_outcome = first_store.lookup(key)
        assert first_outcome is not None
        assert first_outcome.publish_state == first_publish_state
        assert first_outcome.terminal_envelope == _terminal()
        assert (first_outcome.receipt is not None) is (
            first_publish_state == "published"
        )
        assert first_crash.hits.count(crash_point) == 1
        assert handler.calls == 1
        assert app.state.relay_store.task(task_id) == {
            "delivery_count": 1,
            "envelope_id": _command()["id"],
            "recipient_id": "worker-1",
            "state": "leased",
            "task_id": task_id,
        }
        assert await first_transport.observe_terminal(task_id, 0) is None
    finally:
        first_store.close()
        await first_transport.close()

    lease_clock.value += 3_000_001
    second_store = SQLiteOutcomeStore(outcome_database)
    second_transport = CentralRelayTransport(
        relay_url="http://relay.test",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        http_client=client,
    )
    second_executor = TaskExecutor(
        worker_agent_id="worker-1",
        handler=handler,
        outcome_store=second_store,
        terminal_publisher=second_transport,
        progress_publisher=second_transport,
        policy=policy,
        event_sink=sink,
        clock=executor_clock,
        uuid_factory=_UUIDs(),
        crash_hook=_CrashHook(),
    )
    try:
        await second_transport.start_terminal_observer()
        second_delivery = await second_transport.long_poll("worker-1", 0)
        assert second_delivery is not None
        assert second_delivery.delivery_count == 2

        result = await second_executor.execute(second_delivery)

        assert result.classification == "completed"
        assert result.ledger_decision == "hit"
        assert result.terminal_envelope == _terminal()
        assert result.receipt is not None
        assert result.receipt.accepted is True
        assert handler.calls == 1
        final_outcome = second_store.lookup(key)
        assert final_outcome is not None
        assert final_outcome.publish_state == "published"
        assert final_outcome.terminal_envelope == _terminal()
        assert app.state.relay_store.task(task_id) == {
            "delivery_count": 2,
            "envelope_id": _command()["id"],
            "recipient_id": "worker-1",
            "state": "completed",
            "task_id": task_id,
        }
        observed = await second_transport.observe_terminal(task_id, 0)
        assert observed is not None
        assert observed.envelope == _terminal()
        assert observed.delivery_count == 2
        assert observed.replayed is False

        connection = sqlite3.connect(relay_database)
        try:
            terminal_rows = connection.execute(
                """
                SELECT lease_id, terminal_id, state
                FROM terminals
                ORDER BY terminal_sequence
                """
            ).fetchall()
        finally:
            connection.close()
        assert terminal_rows == [
            (first_lease_id, terminal_id, "prepared"),
            (second_lease_id, terminal_id, "committed"),
        ]
    finally:
        second_store.close()
        await second_transport.close()
        await client.aclose()
        app.state.relay_store.close()
