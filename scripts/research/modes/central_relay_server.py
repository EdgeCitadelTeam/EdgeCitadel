"""Run-owned SQLite relay service."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import stat
import time
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import TypeVar, cast

from fastapi import FastAPI, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect

from adapters._common.validator import (
    ValidationError,
    canonical_json,
    default_validator,
    normalize_task_correlation,
)

_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., object])
_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_SCHEMA_VERSION = 1
_BUSY_TIMEOUT_MS = 5_000
_MAX_BODY_BYTES = 1_048_576
_MAX_JSON_NESTING = 128
_MAX_SQLITE_INTEGER = 2**63 - 1
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_UUID_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\Z"
)


def _typed_decorator(value: object) -> Callable[[_F], _F]:
    return cast(Callable[[_F], _F], value)


_DDL = """
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

_DDL_STATEMENTS = tuple(part.strip() for part in _DDL.split(";") if part.strip())
_TABLES = ("tasks", "leases", "terminals")


def _uuid4() -> str:
    return str(uuid.uuid4()).lower()


def _normalize_sql(value: str) -> str:
    return " ".join(value.split()).rstrip(";")


def _prepare_database_path(path: Path) -> os.stat_result:
    if not isinstance(path, Path) or not path.is_absolute():
        raise ValueError("invalid database_path")
    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError:
            raise ValueError("invalid database_path") from None
        try:
            os.fchmod(descriptor, 0o600)
            path_stat = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise ValueError("invalid database_path") from None
    if (
        not stat.S_ISREG(path_stat.st_mode)
        or stat.S_ISLNK(path_stat.st_mode)
        or stat.S_IMODE(path_stat.st_mode) != 0o600
    ):
        raise ValueError("invalid database_path")
    return path_stat


def _schema_map(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], str | None]:
    return {
        (cast(str, row[0]), cast(str, row[1])): (
            None if row[2] is None else _normalize_sql(cast(str, row[2]))
        )
        for row in connection.execute(
            """
            SELECT type, name, sql
            FROM sqlite_schema
            """
        ).fetchall()
    }


def _table_info(
    connection: sqlite3.Connection,
    table: str,
) -> list[tuple[object, ...]]:
    return [
        (row[1], row[2], row[3], row[4], row[5])
        for row in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    ]


def _foreign_keys(
    connection: sqlite3.Connection,
    table: str,
) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in connection.execute(f"PRAGMA foreign_key_list('{table}')").fetchall()
    ]


def _explicit_indexes(
    connection: sqlite3.Connection,
    table: str,
) -> dict[str, tuple[int, str, int, tuple[str, ...]]]:
    result: dict[str, tuple[int, str, int, tuple[str, ...]]] = {}
    for row in connection.execute(f"PRAGMA index_list('{table}')").fetchall():
        name = cast(str, row[1])
        if name.startswith("sqlite_autoindex_"):
            continue
        columns = tuple(
            cast(str, item[2])
            for item in connection.execute(f"PRAGMA index_info('{name}')").fetchall()
        )
        result[name] = (
            cast(int, row[2]),
            cast(str, row[3]),
            cast(int, row[4]),
            columns,
        )
    return result


def _reference_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    for statement in _DDL_STATEMENTS:
        connection.execute(statement)
    return connection


class _RelayStoreError(RuntimeError):
    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


class _HTTPError(RuntimeError):
    def __init__(self, status_code: int, code: str) -> None:
        super().__init__(code)
        self.status_code = status_code
        self.code = code


class _InvalidJSON(ValueError):
    pass


class _DuplicateKey(ValueError):
    pass


@dataclass(frozen=True)
class _LeaseRecord:
    lease_sequence: int
    lease_id: str
    task_sequence: int
    task_id: str
    recipient_id: str
    worker_agent_id: str
    delivery_count: int
    state: str
    deadline_ns: int
    task_state: str
    request_bytes: bytes


def _canonical_mapping(
    value: Mapping[str, object],
) -> tuple[dict[str, object], bytes]:
    try:
        encoded = canonical_json(value)
        decoded = cast(object, json.loads(encoded))
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("invalid envelope") from None
    if not isinstance(decoded, dict):
        raise TypeError("invalid envelope")
    return cast(dict[str, object], decoded), encoded


def _reject_constant(_: str) -> None:
    raise _InvalidJSON


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKey
        result[key] = value
    return result


def _validate_json_tree(value: object) -> None:
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if isinstance(current, float) and not math.isfinite(current):
            raise _InvalidJSON
        if isinstance(current, Mapping):
            child_depth = depth + 1
            if child_depth > _MAX_JSON_NESTING:
                raise _InvalidJSON
            stack.extend((item, child_depth) for item in current.values())
        elif isinstance(current, list):
            child_depth = depth + 1
            if child_depth > _MAX_JSON_NESTING:
                raise _InvalidJSON
            stack.extend((item, child_depth) for item in current)


def _json_response(status_code: int, body: Mapping[str, object]) -> Response:
    return Response(
        content=canonical_json(body),
        status_code=status_code,
        media_type="application/json",
    )


def _error_response(status_code: int, code: str) -> Response:
    return _json_response(status_code, {"error": code})


class _BearerCredential:
    __slots__ = ("_token",)

    def __init__(self, token: str) -> None:
        self._token = token

    def authorized(self, headers: list[tuple[bytes, bytes]]) -> bool:
        values = [
            value.decode("latin-1")
            for key, value in headers
            if key.lower() == b"authorization"
        ]
        presented = values[0] if len(values) == 1 else ""
        return hmac.compare_digest(presented, f"Bearer {self._token}")

    def __repr__(self) -> str:
        return "_BearerCredential()"


async def _request_json(request: Request) -> dict[str, object]:
    content_type = request.headers.get("content-type", "")
    if content_type.split(";", 1)[0].strip().casefold() != "application/json":
        raise _HTTPError(415, "unsupported_media_type")
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError:
            declared_length = -1
        if declared_length > _MAX_BODY_BYTES:
            raise _HTTPError(413, "request_too_large")
    buffered = bytearray()
    async for chunk in request.stream():
        remaining = _MAX_BODY_BYTES + 1 - len(buffered)
        buffered.extend(chunk[:remaining])
        if len(buffered) > _MAX_BODY_BYTES:
            raise _HTTPError(413, "request_too_large")
    body = bytes(buffered)
    if body.startswith(b"\xef\xbb\xbf"):
        raise _HTTPError(400, "invalid_json")
    try:
        decoded = cast(
            object,
            json.loads(
                body.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            ),
        )
        if not isinstance(decoded, dict):
            raise _InvalidJSON
        _validate_json_tree(decoded)
        canonical = canonical_json(decoded)
    except (
        json.JSONDecodeError,
        UnicodeError,
        TypeError,
        ValueError,
        RecursionError,
        _InvalidJSON,
        _DuplicateKey,
    ):
        raise _HTTPError(400, "invalid_json") from None
    if canonical != body:
        raise _HTTPError(400, "noncanonical_json")
    return cast(dict[str, object], decoded)


def _query_value(
    request: Request,
    name: str,
    *,
    default: str | None = None,
) -> str:
    items = list(request.query_params.multi_items())
    if any(key != name for key, _ in items) or len(items) > 1:
        raise _HTTPError(422, "unexpected_query")
    if not items:
        if default is None:
            raise _HTTPError(422, "unexpected_query")
        return default
    return str(items[0][1])


def _reject_query(request: Request) -> None:
    if request.scope.get("query_string"):
        raise _HTTPError(422, "unexpected_query")


def _bounded_decimal(raw: str, maximum: int, code: str) -> int:
    if re.fullmatch(r"[0-9]+", raw) is None:
        raise _HTTPError(422, code)
    significant = raw.lstrip("0") or "0"
    ceiling = str(maximum)
    if len(significant) > len(ceiling) or (
        len(significant) == len(ceiling) and significant > ceiling
    ):
        raise _HTTPError(422, code)
    return int(significant)


def _validate_envelope(
    envelope: dict[str, object],
    allowed_types: tuple[str, ...],
) -> None:
    candidate = dict(envelope)
    if candidate.get("type") in ("command", "delegation", "cancel", "result"):
        # The envelope schema accepts either UUID hex case; validate correlation
        # against an equivalent lowercase projection without rewriting payloads.
        for field in ("task_id", "context_id"):
            value = candidate.get(field)
            if isinstance(value, str) and _UUID_PATTERN.fullmatch(value):
                candidate[field] = value.lower()
        payload = candidate.get("payload")
        if isinstance(payload, Mapping):
            candidate_payload = dict(payload)
            parent_task_id = candidate_payload.get("parent_task_id")
            if isinstance(parent_task_id, str) and _UUID_PATTERN.fullmatch(
                parent_task_id
            ):
                candidate_payload["parent_task_id"] = parent_task_id.lower()
            candidate["payload"] = candidate_payload
    try:
        default_validator().validate_envelope(candidate)
    except (ValidationError, RecursionError):
        raise _HTTPError(422, "invalid_envelope") from None
    if envelope.get("type") not in allowed_types:
        raise _HTTPError(422, "invalid_envelope")


class _AuthenticationMiddleware:
    def __init__(self, app: ASGIApp, *, credential: _BearerCredential) -> None:
        self._app = app
        self._credential = credential

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return
        headers = cast(list[tuple[bytes, bytes]], scope["headers"])
        if self._credential.authorized(headers):
            await self._app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 4401})
            return
        await _error_response(401, "unauthorized")(scope, receive, send)


class RelayStore:
    def __init__(
        self,
        database_path: Path,
        *,
        run_id: str,
        lease_ttl_ms: int = 30_000,
        lease_clock_ns: Callable[[], int] = time.monotonic_ns,
        evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
        uuid4: Callable[[], str] = _uuid4,
    ) -> None:
        if type(run_id) is not str or _ID_PATTERN.fullmatch(run_id) is None:
            raise ValueError("invalid run_id")
        if type(lease_ttl_ms) is not int or lease_ttl_ms < 3 or lease_ttl_ms > 300_000:
            raise ValueError("invalid lease_ttl_ms")
        before = _prepare_database_path(database_path)
        self._database_path = database_path
        self._run_id = run_id
        self._lease_ttl_ms = lease_ttl_ms
        self._lease_ttl_ns = lease_ttl_ms * 1_000_000
        self._renewal_interval_ms = max(1, lease_ttl_ms // 3)
        self._lease_clock_ns = lease_clock_ns
        self._evidence_clock_ns = evidence_clock_ns
        self._uuid4 = uuid4
        self._lock = RLock()
        self._closed = False
        try:
            self._connection = sqlite3.connect(
                database_path,
                timeout=_BUSY_TIMEOUT_MS / 1000,
                isolation_level=None,
                check_same_thread=False,
            )
            after = database_path.lstat()
            if (
                not stat.S_ISREG(after.st_mode)
                or stat.S_ISLNK(after.st_mode)
                or stat.S_IMODE(after.st_mode) != 0o600
                or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            ):
                raise ValueError("invalid database_path")
            self._configure()
            self._initialize_or_validate_schema()
            self._recover_active_leases()
        except ValueError:
            connection = getattr(self, "_connection", None)
            if isinstance(connection, sqlite3.Connection):
                connection.close()
            self._closed = True
            raise
        except (OSError, sqlite3.Error, RuntimeError):
            connection = getattr(self, "_connection", None)
            if isinstance(connection, sqlite3.Connection):
                connection.close()
            self._closed = True
            raise RuntimeError("relay schema mismatch") from None

    def _configure(self) -> None:
        journal = self._connection.execute("PRAGMA journal_mode = WAL").fetchone()
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        if (
            journal is None
            or str(journal[0]).casefold() != "wal"
            or self._connection.execute("PRAGMA synchronous").fetchone() != (2,)
            or self._connection.execute("PRAGMA foreign_keys").fetchone() != (1,)
            or self._connection.execute("PRAGMA busy_timeout").fetchone()
            != (_BUSY_TIMEOUT_MS,)
        ):
            raise RuntimeError("relay schema mismatch")

    def _initialize_or_validate_schema(self) -> None:
        version = cast(
            int,
            self._connection.execute("PRAGMA user_version").fetchone()[0],
        )
        objects = _schema_map(self._connection)
        if version == 0 and not objects:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                for statement in _DDL_STATEMENTS:
                    self._connection.execute(statement)
                self._connection.execute("COMMIT")
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise
        self._validate_schema()

    def _validate_schema(self) -> None:
        reference = _reference_connection()
        try:
            if self._connection.execute("PRAGMA user_version").fetchone() != (
                _SCHEMA_VERSION,
            ):
                raise RuntimeError("relay schema mismatch")
            if _schema_map(self._connection) != _schema_map(reference):
                raise RuntimeError("relay schema mismatch")
            for table in _TABLES:
                if _table_info(self._connection, table) != _table_info(
                    reference, table
                ):
                    raise RuntimeError("relay schema mismatch")
                if _foreign_keys(self._connection, table) != _foreign_keys(
                    reference, table
                ):
                    raise RuntimeError("relay schema mismatch")
                if _explicit_indexes(self._connection, table) != _explicit_indexes(
                    reference, table
                ):
                    raise RuntimeError("relay schema mismatch")
        finally:
            reference.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("relay store is closed")

    def _transaction(self, operation: Callable[[], _T]) -> _T:
        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                result = operation()
                self._connection.execute("COMMIT")
                return result
            except BaseException:
                if self._connection.in_transaction:
                    self._connection.execute("ROLLBACK")
                raise

    def task(self, task_id: str) -> Mapping[str, object] | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT envelope_id, task_id, recipient_id, state, delivery_count
                FROM tasks
                WHERE task_id = ?
                ORDER BY task_sequence
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "envelope_id": row[0],
            "task_id": row[1],
            "recipient_id": row[2],
            "state": row[3],
            "delivery_count": row[4],
        }

    def _submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> tuple[int, dict[str, object]]:
        decoded, encoded = _canonical_mapping(envelope)
        envelope_id = cast(str, decoded["id"])
        task_id = cast(str, decoded["task_id"])
        recipient_id = cast(str, decoded["recipient_id"])
        digest = hashlib.sha256(encoded).hexdigest()

        def submit() -> bool:
            existing = self._connection.execute(
                "SELECT envelope FROM tasks WHERE envelope_id = ?",
                (envelope_id,),
            ).fetchone()
            if existing is not None:
                if cast(bytes, existing[0]) != encoded:
                    raise _RelayStoreError(409, "envelope_id_conflict")
                return False
            self._connection.execute(
                """
                INSERT INTO tasks(
                    envelope_id, task_id, recipient_id, envelope,
                    envelope_sha256, submitted_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    envelope_id,
                    task_id,
                    recipient_id,
                    encoded,
                    digest,
                    self._evidence_clock_ns(),
                ),
            )
            return True

        created = self._transaction(submit)
        return (
            201 if created else 200,
            self._receipt(envelope_id, len(encoded)),
        )

    def _acquire_lease(self, agent_id: str) -> dict[str, object] | None:
        def acquire() -> dict[str, object] | None:
            now = self._lease_clock_ns()
            self._expire_active_leases(now)
            row = self._connection.execute(
                """
                SELECT
                    task_sequence, envelope, task_id, recipient_id,
                    delivery_count
                FROM tasks AS task
                WHERE recipient_id = ?
                  AND state = 'queued'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM leases AS lease
                      WHERE lease.worker_agent_id = task.recipient_id
                        AND lease.task_id = task.task_id
                        AND lease.state = 'active'
                  )
                ORDER BY task_sequence
                LIMIT 1
                """,
                (agent_id,),
            ).fetchone()
            if row is None:
                return None
            task_sequence = cast(int, row[0])
            delivery_count = cast(int, row[4]) + 1
            lease_id = self._uuid4()
            deadline_ns = now + self._lease_ttl_ns
            updated = self._connection.execute(
                """
                UPDATE tasks
                SET state = 'leased', delivery_count = ?
                WHERE task_sequence = ? AND state = 'queued'
                """,
                (delivery_count, task_sequence),
            )
            if updated.rowcount != 1:
                raise RuntimeError("relay lease acquisition failed")
            self._connection.execute(
                """
                INSERT INTO leases(
                    lease_id, task_sequence, task_id, recipient_id,
                    worker_agent_id, delivery_count, acquired_ns, deadline_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    task_sequence,
                    row[2],
                    row[3],
                    agent_id,
                    delivery_count,
                    now,
                    deadline_ns,
                ),
            )
            return {
                "delivery_count": delivery_count,
                "envelope": cast(object, json.loads(cast(bytes, row[1]))),
                "lease_deadline_ns": deadline_ns,
                "lease_id": lease_id,
                "lease_ttl_ms": self._lease_ttl_ms,
                "renewal_interval_ms": self._renewal_interval_ms,
                "stream_sequence": None,
            }

        return self._transaction(acquire)

    def _prepare_terminal(
        self,
        lease_id: str,
        envelope: Mapping[str, object],
    ) -> tuple[int, dict[str, object]]:
        decoded, encoded = _canonical_mapping(envelope)
        terminal_id = cast(str, decoded["id"])
        digest = hashlib.sha256(encoded).hexdigest()

        def prepare() -> bool | _RelayStoreError:
            lease = self._lease_record(lease_id)
            if lease is None:
                return _RelayStoreError(404, "lease_not_found")
            now = self._lease_clock_ns()
            if lease.state == "active" and lease.deadline_ns <= now:
                self._expire_lease(lease, now)
                return _RelayStoreError(409, "lease_expired")
            if lease.state == "expired":
                return _RelayStoreError(409, "lease_expired")
            if lease.state == "released":
                return _RelayStoreError(409, "stale_lease")
            if lease.state in ("terminated", "committed"):
                return _RelayStoreError(409, "lease_finalized")
            if not self._terminal_matches_lease(decoded, lease):
                return _RelayStoreError(409, "execution_binding_mismatch")
            existing = self._connection.execute(
                """
                SELECT terminal_id, terminal_sha256, envelope
                FROM terminals
                WHERE lease_sequence = ?
                """,
                (lease.lease_sequence,),
            ).fetchone()
            if existing is not None:
                if (
                    existing[0] == terminal_id
                    and existing[1] == digest
                    and cast(bytes, existing[2]) == encoded
                ):
                    return False
                return _RelayStoreError(409, "terminal_conflict")
            self._connection.execute(
                """
                INSERT INTO terminals(
                    lease_sequence, lease_id, task_sequence, task_id,
                    terminal_id, terminal_sha256, envelope, prepared_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.lease_sequence,
                    lease.lease_id,
                    lease.task_sequence,
                    lease.task_id,
                    terminal_id,
                    digest,
                    encoded,
                    self._evidence_clock_ns(),
                ),
            )
            return True

        outcome = self._transaction(prepare)
        if isinstance(outcome, _RelayStoreError):
            raise outcome
        return (
            201 if outcome else 200,
            self._receipt(terminal_id, len(encoded)),
        )

    def _apply_disposition(
        self,
        lease_id: str,
        disposition: str,
    ) -> tuple[int, dict[str, object]]:
        if disposition not in ("renew", "retry", "terminate", "commit"):
            raise _RelayStoreError(422, "invalid_disposition")

        def apply() -> dict[str, object] | _RelayStoreError:
            lease = self._lease_record(lease_id)
            if lease is None:
                return _RelayStoreError(404, "lease_not_found")
            now = self._lease_clock_ns()
            if lease.state == "active" and lease.deadline_ns <= now:
                self._expire_lease(lease, now)
                return _RelayStoreError(409, "lease_expired")
            if lease.state == "expired":
                return _RelayStoreError(409, "lease_expired")
            if lease.state == "released":
                if disposition == "retry":
                    return self._retry_body(lease_id)
                return _RelayStoreError(409, "stale_lease")
            if lease.state == "terminated":
                if disposition == "terminate":
                    return self._terminate_body(lease_id)
                return _RelayStoreError(409, "lease_finalized")
            if lease.state == "committed":
                if disposition == "commit":
                    return self._committed_body(lease)
                return _RelayStoreError(409, "lease_finalized")

            if disposition == "renew":
                deadline_ns = now + self._lease_ttl_ns
                self._connection.execute(
                    "UPDATE leases SET deadline_ns = ? WHERE lease_sequence = ?",
                    (deadline_ns, lease.lease_sequence),
                )
                return {
                    "disposition": "renew",
                    "lease_deadline_ns": deadline_ns,
                    "lease_id": lease_id,
                    "lease_ttl_ms": self._lease_ttl_ms,
                    "renewal_interval_ms": self._renewal_interval_ms,
                    "state": "active",
                }
            if disposition == "retry":
                self._connection.execute(
                    """
                    UPDATE leases
                    SET state = 'released', finalized_ns = ?
                    WHERE lease_sequence = ? AND state = 'active'
                    """,
                    (now, lease.lease_sequence),
                )
                self._connection.execute(
                    """
                    UPDATE tasks
                    SET state = 'queued'
                    WHERE task_sequence = ? AND state = 'leased'
                    """,
                    (lease.task_sequence,),
                )
                return self._retry_body(lease_id)
            if disposition == "terminate":
                completed_ns = self._evidence_clock_ns()
                self._connection.execute(
                    """
                    UPDATE leases
                    SET state = 'terminated', finalized_ns = ?
                    WHERE lease_sequence = ? AND state = 'active'
                    """,
                    (now, lease.lease_sequence),
                )
                self._connection.execute(
                    """
                    UPDATE tasks
                    SET state = 'terminated', completed_ns = ?
                    WHERE task_sequence = ? AND state = 'leased'
                    """,
                    (completed_ns, lease.task_sequence),
                )
                return self._terminate_body(lease_id)

            terminal = self._connection.execute(
                """
                SELECT terminal_sequence, envelope
                FROM terminals
                WHERE lease_sequence = ? AND state = 'prepared'
                """,
                (lease.lease_sequence,),
            ).fetchone()
            if terminal is None:
                return _RelayStoreError(409, "terminal_not_prepared")
            completed_ns = self._evidence_clock_ns()
            self._connection.execute(
                """
                UPDATE terminals
                SET state = 'committed', committed_ns = ?
                WHERE terminal_sequence = ? AND state = 'prepared'
                """,
                (completed_ns, terminal[0]),
            )
            self._connection.execute(
                """
                UPDATE leases
                SET state = 'committed', finalized_ns = ?
                WHERE lease_sequence = ? AND state = 'active'
                """,
                (now, lease.lease_sequence),
            )
            self._connection.execute(
                """
                UPDATE tasks
                SET state = 'completed', completed_ns = ?
                WHERE task_sequence = ? AND state = 'leased'
                """,
                (completed_ns, lease.task_sequence),
            )
            return {
                "disposition": "commit",
                "envelope": cast(object, json.loads(cast(bytes, terminal[1]))),
                "lease_id": lease_id,
                "state": "completed",
                "terminal_sequence": terminal[0],
            }

        outcome = self._transaction(apply)
        if isinstance(outcome, _RelayStoreError):
            raise outcome
        return 200, outcome

    def _terminal_after(
        self,
        task_id: str,
        after: int,
    ) -> dict[str, object] | None:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                """
                SELECT
                    terminal.terminal_sequence,
                    terminal.envelope,
                    lease.delivery_count
                FROM terminals AS terminal
                JOIN leases AS lease
                  ON lease.lease_sequence = terminal.lease_sequence
                WHERE terminal.task_id = ?
                  AND terminal.state = 'committed'
                  AND terminal.terminal_sequence > ?
                ORDER BY terminal.terminal_sequence
                LIMIT 1
                """,
                (task_id, after),
            ).fetchone()
        if row is None:
            return None
        return {
            "delivery_count": row[2],
            "envelope": cast(object, json.loads(cast(bytes, row[1]))),
            "terminal_sequence": row[0],
        }

    def _health(self) -> dict[str, object]:
        with self._lock:
            self._ensure_open()
            pending = cast(
                int,
                self._connection.execute(
                    "SELECT COUNT(*) FROM tasks WHERE state = 'queued'"
                ).fetchone()[0],
            )
            active = cast(
                int,
                self._connection.execute(
                    "SELECT COUNT(*) FROM leases WHERE state = 'active'"
                ).fetchone()[0],
            )
            tasks = cast(
                int,
                self._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0],
            )
            terminals = cast(
                int,
                self._connection.execute("SELECT COUNT(*) FROM terminals").fetchone()[
                    0
                ],
            )
            high_water = cast(
                int,
                self._connection.execute(
                    """
                    SELECT COALESCE(MAX(terminal_sequence), 0)
                    FROM terminals
                    WHERE state = 'committed'
                    """
                ).fetchone()[0],
            )
            storage_bytes = 0
            for candidate in (
                self._database_path,
                Path(f"{self._database_path}-wal"),
                Path(f"{self._database_path}-shm"),
            ):
                try:
                    storage_bytes += candidate.stat().st_size
                except FileNotFoundError:
                    pass
        return {
            "ack_pending": active,
            "committed_terminal_sequence": high_water,
            "message_count": tasks + terminals,
            "pending": pending,
            "run_id": self._run_id,
            "status": "ok",
            "storage_bytes": storage_bytes,
        }

    def _receipt(self, envelope_id: str, application_bytes: int) -> dict[str, object]:
        return {
            "envelope_id": envelope_id,
            "accepted": True,
            "transport": "central-relay",
            "stream": None,
            "stream_sequence": None,
            "duplicate": None,
            "accepted_ns": self._evidence_clock_ns(),
            "application_bytes": application_bytes,
            "wire_bytes": None,
        }

    def _expire_active_leases(self, now: int) -> None:
        rows = self._connection.execute(
            """
            SELECT lease_sequence, task_sequence
            FROM leases
            WHERE state = 'active' AND deadline_ns <= ?
            ORDER BY lease_sequence
            """,
            (now,),
        ).fetchall()
        for row in rows:
            changed = self._connection.execute(
                """
                UPDATE leases
                SET state = 'expired', finalized_ns = ?
                WHERE lease_sequence = ? AND state = 'active' AND deadline_ns <= ?
                """,
                (now, row[0], now),
            )
            if changed.rowcount == 1:
                self._requeue_expired_task(cast(int, row[1]))

    def _recover_active_leases(self) -> None:
        """Requeue leases whose coordinator process was replaced."""

        def recover() -> None:
            now = self._lease_clock_ns()
            rows = self._connection.execute(
                """
                SELECT lease_sequence, task_sequence
                FROM leases
                WHERE state = 'active'
                ORDER BY lease_sequence
                """
            ).fetchall()
            for row in rows:
                changed = self._connection.execute(
                    """
                    UPDATE leases
                    SET state = 'expired', finalized_ns = ?
                    WHERE lease_sequence = ? AND state = 'active'
                    """,
                    (now, row[0]),
                )
                if changed.rowcount == 1:
                    self._requeue_expired_task(cast(int, row[1]))

        self._transaction(recover)

    def _expire_lease(self, lease: _LeaseRecord, now: int) -> None:
        changed = self._connection.execute(
            """
            UPDATE leases
            SET state = 'expired', finalized_ns = ?
            WHERE lease_sequence = ? AND state = 'active' AND deadline_ns <= ?
            """,
            (now, lease.lease_sequence, now),
        )
        if changed.rowcount == 1:
            self._requeue_expired_task(lease.task_sequence)

    def _requeue_expired_task(self, task_sequence: int) -> None:
        self._connection.execute(
            """
            UPDATE tasks
            SET state = 'queued'
            WHERE task_sequence = ?
              AND state = 'leased'
              AND NOT EXISTS (
                  SELECT 1
                  FROM leases
                  WHERE task_sequence = ?
                    AND state = 'active'
              )
            """,
            (task_sequence, task_sequence),
        )

    def _lease_record(self, lease_id: str) -> _LeaseRecord | None:
        row = self._connection.execute(
            """
            SELECT
                lease.lease_sequence,
                lease.lease_id,
                lease.task_sequence,
                lease.task_id,
                lease.recipient_id,
                lease.worker_agent_id,
                lease.delivery_count,
                lease.state,
                lease.deadline_ns,
                task.state,
                task.envelope
            FROM leases AS lease
            JOIN tasks AS task ON task.task_sequence = lease.task_sequence
            WHERE lease.lease_id = ?
            """,
            (lease_id,),
        ).fetchone()
        if row is None:
            return None
        return _LeaseRecord(
            lease_sequence=cast(int, row[0]),
            lease_id=cast(str, row[1]),
            task_sequence=cast(int, row[2]),
            task_id=cast(str, row[3]),
            recipient_id=cast(str, row[4]),
            worker_agent_id=cast(str, row[5]),
            delivery_count=cast(int, row[6]),
            state=cast(str, row[7]),
            deadline_ns=cast(int, row[8]),
            task_state=cast(str, row[9]),
            request_bytes=cast(bytes, row[10]),
        )

    @staticmethod
    def _terminal_matches_lease(
        terminal: Mapping[str, object],
        lease: _LeaseRecord,
    ) -> bool:
        try:
            request = cast(dict[str, object], json.loads(lease.request_bytes))
            request_correlation = normalize_task_correlation(request)
            terminal_correlation = normalize_task_correlation(terminal)
        except (TypeError, ValueError, UnicodeError):
            return False
        request_payload = cast(Mapping[str, object], request_correlation["payload"])
        terminal_payload = cast(Mapping[str, object], terminal_correlation["payload"])
        return (
            terminal.get("type") == "result"
            and terminal_correlation["sender_id"] == lease.worker_agent_id
            and terminal_correlation["recipient_id"] == request_correlation["sender_id"]
            and terminal_correlation["task_id"] == request_correlation["task_id"]
            and terminal_correlation["context_id"] == request_correlation["context_id"]
            and terminal_correlation["hop_count"] == request_correlation["hop_count"]
            and terminal_payload.get("parent_task_id")
            == request_payload.get("parent_task_id")
        )

    @staticmethod
    def _retry_body(lease_id: str) -> dict[str, object]:
        return {
            "disposition": "retry",
            "lease_id": lease_id,
            "state": "queued",
        }

    @staticmethod
    def _terminate_body(lease_id: str) -> dict[str, object]:
        return {
            "disposition": "terminate",
            "lease_id": lease_id,
            "state": "terminated",
        }

    def _committed_body(self, lease: _LeaseRecord) -> dict[str, object]:
        terminal = self._connection.execute(
            """
            SELECT terminal_sequence, envelope
            FROM terminals
            WHERE lease_sequence = ? AND state = 'committed'
            """,
            (lease.lease_sequence,),
        ).fetchone()
        if terminal is None:
            raise RuntimeError("relay committed terminal is missing")
        return {
            "disposition": "commit",
            "envelope": cast(object, json.loads(cast(bytes, terminal[1]))),
            "lease_id": lease.lease_id,
            "state": "completed",
            "terminal_sequence": terminal[0],
        }

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True


def create_app(
    database_path: Path,
    *,
    run_id: str,
    token: str,
    lease_ttl_ms: int = 30_000,
    lease_clock_ns: Callable[[], int] = time.monotonic_ns,
    evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
    uuid4: Callable[[], str] = _uuid4,
) -> FastAPI:
    if type(token) is not str or _TOKEN_PATTERN.fullmatch(token) is None:
        raise ValueError("invalid token")
    store = RelayStore(
        database_path,
        run_id=run_id,
        lease_ttl_ms=lease_ttl_ms,
        lease_clock_ns=lease_clock_ns,
        evidence_clock_ns=evidence_clock_ns,
        uuid4=uuid4,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            store.close()

    app = FastAPI(
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.relay_store = store
    app.state.event_sockets = set()
    app.router.redirect_slashes = False
    app.add_middleware(
        _AuthenticationMiddleware,
        credential=_BearerCredential(token),
    )

    @_typed_decorator(app.exception_handler(_HTTPError))
    async def handle_http_error(_: Request, exc: _HTTPError) -> Response:
        return _error_response(exc.status_code, exc.code)

    @_typed_decorator(app.exception_handler(_RelayStoreError))
    async def handle_store_error(_: Request, exc: _RelayStoreError) -> Response:
        return _error_response(exc.status_code, exc.code)

    @_typed_decorator(app.exception_handler(sqlite3.Error))
    async def handle_database_error(_: Request, __: sqlite3.Error) -> Response:
        return _error_response(503, "relay_unavailable")

    @_typed_decorator(app.exception_handler(RequestValidationError))
    async def handle_validation_error(
        _: Request,
        __: RequestValidationError,
    ) -> Response:
        return _error_response(422, "invalid_envelope")

    @_typed_decorator(app.exception_handler(StarletteHTTPException))
    async def handle_routing_error(
        _: Request,
        exc: StarletteHTTPException,
    ) -> Response:
        if exc.status_code == 404:
            return _error_response(404, "route_not_found")
        if exc.status_code == 405:
            return _error_response(405, "method_not_allowed")
        return _error_response(500, "internal_error")

    @_typed_decorator(app.exception_handler(Exception))
    async def handle_internal_error(_: Request, __: Exception) -> Response:
        return _error_response(500, "internal_error")

    @_typed_decorator(app.post("/v1/tasks"))
    async def submit_task(request: Request) -> Response:
        _reject_query(request)
        envelope = await _request_json(request)
        _validate_envelope(envelope, ("command", "delegation", "cancel"))
        status_code, receipt = store._submit_task(envelope)
        return _json_response(status_code, receipt)

    @_typed_decorator(app.get("/v1/workers/{agent_id}/lease"))
    async def lease_task(agent_id: str, request: Request) -> Response:
        if _ID_PATTERN.fullmatch(agent_id) is None:
            raise _HTTPError(422, "invalid_agent_id")
        raw_timeout = _query_value(request, "timeout_ms")
        timeout_ms = _bounded_decimal(
            raw_timeout,
            30_000,
            "invalid_timeout_ms",
        )
        deadline_ns = lease_clock_ns() + timeout_ms * 1_000_000
        while True:
            lease = store._acquire_lease(agent_id)
            if lease is not None:
                return _json_response(200, lease)
            remaining_ns = deadline_ns - lease_clock_ns()
            if timeout_ms == 0 or remaining_ns <= 0:
                return Response(status_code=204)
            await asyncio.sleep(min(remaining_ns / 1_000_000_000, 0.01))

    @_typed_decorator(app.post("/v1/leases/{lease_id}/terminal"))
    async def prepare_terminal(lease_id: str, request: Request) -> Response:
        _reject_query(request)
        if _UUID_PATTERN.fullmatch(lease_id) is None:
            raise _HTTPError(422, "invalid_lease_id")
        envelope = await _request_json(request)
        _validate_envelope(envelope, ("result",))
        status_code, receipt = store._prepare_terminal(lease_id, envelope)
        return _json_response(status_code, receipt)

    @_typed_decorator(app.post("/v1/leases/{lease_id}/commit"))
    async def apply_disposition(lease_id: str, request: Request) -> Response:
        _reject_query(request)
        if _UUID_PATTERN.fullmatch(lease_id) is None:
            raise _HTTPError(422, "invalid_lease_id")
        body = await _request_json(request)
        if set(body) != {"disposition"} or body["disposition"] not in (
            "renew",
            "retry",
            "terminate",
            "commit",
        ):
            raise _HTTPError(422, "invalid_disposition")
        status_code, result = store._apply_disposition(
            lease_id,
            body["disposition"],
        )
        return _json_response(status_code, result)

    @_typed_decorator(app.get("/v1/tasks/{task_id}/terminal"))
    async def observe_terminal(task_id: str, request: Request) -> Response:
        if _UUID_PATTERN.fullmatch(task_id) is None:
            raise _HTTPError(422, "invalid_task_id")
        raw_cursor = _query_value(request, "after", default="0")
        cursor = _bounded_decimal(
            raw_cursor,
            _MAX_SQLITE_INTEGER,
            "invalid_cursor",
        )
        observation = store._terminal_after(task_id, cursor)
        if observation is None:
            raise _HTTPError(404, "terminal_not_found")
        return _json_response(200, observation)

    @_typed_decorator(app.post("/v1/events"))
    async def publish_event(request: Request) -> Response:
        _reject_query(request)
        envelope = await _request_json(request)
        _validate_envelope(envelope, ("task.progress", "heartbeat", "status"))
        encoded = canonical_json(envelope)
        receipt = store._receipt(cast(str, envelope["id"]), len(encoded))
        failed: list[WebSocket] = []
        sockets = cast(set[WebSocket], app.state.event_sockets)
        for socket in tuple(sockets):
            try:
                await socket.send_bytes(encoded)
            except Exception:  # noqa: BLE001
                failed.append(socket)
        for socket in failed:
            sockets.discard(socket)
        return _json_response(202, receipt)

    @_typed_decorator(app.websocket("/v1/events"))
    async def observe_events(websocket: WebSocket) -> None:
        if websocket.scope.get("query_string"):
            await websocket.close(code=1008)
            return
        await websocket.accept()
        sockets = cast(set[WebSocket], app.state.event_sockets)
        try:
            await websocket.send_bytes(
                canonical_json({"kind": "observer_ready", "run_id": run_id})
            )
            sockets.add(websocket)
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    break
        except WebSocketDisconnect:
            pass
        finally:
            sockets.discard(websocket)

    @_typed_decorator(app.get("/healthz"))
    async def health(request: Request) -> Response:
        _reject_query(request)
        return _json_response(200, store._health())

    return app


def create_app_from_environment() -> FastAPI:
    try:
        run_id = os.environ["EC_RUN_ID"]
        database_path = Path(os.environ["RELAY_DB_PATH"])
        credential_path = Path(os.environ["EC_CREDENTIAL_FILE"])
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid relay environment") from None
    token = _read_credential_file(credential_path)
    return create_app(database_path, run_id=run_id, token=token)


def _read_credential_file(path: Path) -> str:
    descriptor: int | None = None
    try:
        if not isinstance(path, Path) or not path.is_absolute():
            raise OSError
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or stat.S_IMODE(before.st_mode) not in (0o400, 0o600)
        ):
            raise OSError
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) not in (0o400, 0o600)
            or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise OSError
        contents = os.read(descriptor, 66)
        if (
            len(contents) != 65
            or re.fullmatch(rb"[0-9a-f]{64}\n", contents) is None
            or os.read(descriptor, 1)
        ):
            raise OSError
        after = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
        ) or stat.S_IMODE(after.st_mode) not in (0o400, 0o600):
            raise OSError
        return contents[:64].decode("ascii")
    except (OSError, TypeError, ValueError):
        raise ValueError("invalid credential file") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


__all__ = ["RelayStore", "create_app", "create_app_from_environment"]
