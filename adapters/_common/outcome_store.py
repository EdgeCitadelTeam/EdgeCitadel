"""Durable, transport-neutral task outcome ledger."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from threading import RLock
from typing import Literal, Protocol, TypeVar, cast

from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import canonical_json

PublishState = Literal["prepared", "published"]

_SCHEMA_VERSION = 1
_BUSY_TIMEOUT_MS = 5_000
_RETENTION_FORMULA = (
    "max(stream max_age, maximum retry horizon, maximum task deadline) + "
    "duplicate_window + one hour"
)
_METADATA = {
    "schema_version": str(_SCHEMA_VERSION),
    "eviction_policy": "no_eviction_during_run",
    "minimum_retention_formula": _RETENTION_FORMULA,
}
_HASH_PATTERN = re.compile(r"[0-9a-f]{64}\Z")

_METADATA_SCHEMA = """
CREATE TABLE metadata (
    key TEXT NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
)
""".strip()

_OUTCOMES_SCHEMA = """
CREATE TABLE outcomes (
    worker_agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    request_envelope_id TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    terminal_envelope BLOB NOT NULL,
    terminal_payload_hash TEXT NOT NULL,
    publish_state TEXT NOT NULL CHECK (publish_state IN ('prepared', 'published')),
    completed_at TEXT NOT NULL,
    receipt BLOB,
    PRIMARY KEY (worker_agent_id, task_id),
    CHECK (
        (publish_state = 'prepared' AND receipt IS NULL)
        OR (publish_state = 'published' AND receipt IS NOT NULL)
    )
)
""".strip()

_ATTEMPTS_SCHEMA = """
CREATE TABLE request_attempts (
    attempt_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    wire_id TEXT NOT NULL,
    UNIQUE (worker_agent_id, task_id, wire_id),
    FOREIGN KEY (worker_agent_id, task_id)
        REFERENCES outcomes (worker_agent_id, task_id)
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
)
""".strip()

_EXPECTED_SCHEMAS = {
    "metadata": _METADATA_SCHEMA,
    "outcomes": _OUTCOMES_SCHEMA,
    "request_attempts": _ATTEMPTS_SCHEMA,
}
_EXPECTED_SCHEMA_OBJECTS = {("table", name) for name in _EXPECTED_SCHEMAS}

_OUTCOME_SELECT = """
SELECT worker_agent_id, task_id, sender_id, request_envelope_id,
       request_fingerprint, terminal_envelope, terminal_payload_hash,
       publish_state, completed_at, receipt
FROM outcomes
WHERE worker_agent_id = ? AND task_id = ?
"""

_T = TypeVar("_T")


class OutcomeStoreError(RuntimeError):
    """Base class for stable outcome-ledger domain failures."""


class OutcomeValidationError(OutcomeStoreError):
    """An operation received an invalid key, outcome, receipt, or path."""


class OutcomeConflict(OutcomeStoreError):
    """A task key already owns a different immutable outcome."""


class OutcomeNotFound(OutcomeStoreError):
    """Publication marking was requested before outcome preparation."""


class OutcomeStoreClosed(OutcomeStoreError):
    """An operation targeted a closed SQLite store."""


class OutcomeStoreDisabled(OutcomeStoreError):
    """A stateful operation targeted the stateless disabled store."""


class OutcomeSchemaError(OutcomeStoreError):
    """A database does not have the exact supported outcome schema."""


@dataclass(frozen=True)
class OutcomeKey:
    worker_agent_id: str
    task_id: str


@dataclass(frozen=True)
class PreparedOutcome:
    key: OutcomeKey
    sender_id: str
    request_envelope_id: str
    request_fingerprint: str
    terminal_envelope: Mapping[str, object]
    terminal_payload_hash: str
    publish_state: PublishState
    completed_at: str
    receipt: PublicationReceipt | None = None


class OutcomeStore(Protocol):
    @property
    def enabled(self) -> bool: ...

    def lookup(self, key: OutcomeKey) -> PreparedOutcome | None: ...

    def prepare(self, outcome: PreparedOutcome) -> PreparedOutcome: ...

    def mark_published(
        self,
        key: OutcomeKey,
        receipt: PublicationReceipt,
    ) -> PreparedOutcome: ...

    def close(self) -> None: ...


def _nonempty_utf8(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _valid_hash(value: object) -> bool:
    return isinstance(value, str) and _HASH_PATTERN.fullmatch(value) is not None


def _canonical_mapping(
    value: object,
    *,
    failure_message: str,
) -> tuple[dict[str, object], bytes]:
    if not isinstance(value, Mapping):
        raise OutcomeValidationError(failure_message)
    try:
        encoded = canonical_json(value)
        decoded = cast(object, json.loads(encoded))
    except (TypeError, ValueError, UnicodeError):
        raise OutcomeValidationError(failure_message) from None
    if not isinstance(decoded, dict) or not all(
        isinstance(key, str) for key in decoded
    ):
        raise OutcomeValidationError(failure_message)
    return cast(dict[str, object], decoded), encoded


def _receipt_value(receipt: PublicationReceipt) -> dict[str, object]:
    return {
        "envelope_id": receipt.envelope_id,
        "accepted": receipt.accepted,
        "transport": receipt.transport,
        "stream": receipt.stream,
        "stream_sequence": receipt.stream_sequence,
        "duplicate": receipt.duplicate,
        "accepted_ns": receipt.accepted_ns,
        "application_bytes": receipt.application_bytes,
        "wire_bytes": receipt.wire_bytes,
    }


def _strict_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _validate_receipt(receipt: object) -> tuple[PublicationReceipt, bytes]:
    if not isinstance(receipt, PublicationReceipt):
        raise OutcomeValidationError("invalid publication receipt")
    if (
        receipt.accepted is not True
        or not _nonempty_utf8(receipt.envelope_id)
        or not _nonempty_utf8(receipt.transport)
        or (receipt.stream is not None and not isinstance(receipt.stream, str))
        or (
            receipt.stream_sequence is not None
            and (
                type(receipt.stream_sequence) is not int or receipt.stream_sequence <= 0
            )
        )
        or (receipt.duplicate is not None and type(receipt.duplicate) is not bool)
        or not _strict_nonnegative_int(receipt.accepted_ns)
        or not _strict_nonnegative_int(receipt.application_bytes)
        or (
            receipt.wire_bytes is not None
            and not _strict_nonnegative_int(receipt.wire_bytes)
        )
    ):
        raise OutcomeValidationError("invalid publication receipt")
    try:
        encoded = canonical_json(_receipt_value(receipt))
    except (TypeError, ValueError, UnicodeError):
        raise OutcomeValidationError("invalid publication receipt") from None
    return receipt, encoded


def _decode_receipt(value: bytes) -> PublicationReceipt:
    try:
        decoded = cast(object, json.loads(value))
    except (TypeError, ValueError, UnicodeError):
        raise OutcomeSchemaError("unsupported outcome schema") from None
    expected_keys = {
        "envelope_id",
        "accepted",
        "transport",
        "stream",
        "stream_sequence",
        "duplicate",
        "accepted_ns",
        "application_bytes",
        "wire_bytes",
    }
    if not isinstance(decoded, dict) or set(decoded) != expected_keys:
        raise OutcomeSchemaError("unsupported outcome schema")
    raw = cast(dict[str, object], decoded)
    receipt = PublicationReceipt(
        envelope_id=cast(str, raw["envelope_id"]),
        accepted=cast(bool, raw["accepted"]),
        transport=cast(str, raw["transport"]),
        stream=cast(str | None, raw["stream"]),
        stream_sequence=cast(int | None, raw["stream_sequence"]),
        duplicate=cast(bool | None, raw["duplicate"]),
        accepted_ns=cast(int, raw["accepted_ns"]),
        application_bytes=cast(int, raw["application_bytes"]),
        wire_bytes=cast(int | None, raw["wire_bytes"]),
    )
    try:
        validated, canonical = _validate_receipt(receipt)
    except OutcomeValidationError:
        raise OutcomeSchemaError("unsupported outcome schema") from None
    if canonical != value:
        raise OutcomeSchemaError("unsupported outcome schema")
    return validated


def _normalize_outcome(
    outcome: object,
) -> tuple[PreparedOutcome, bytes]:
    if not isinstance(outcome, PreparedOutcome):
        raise OutcomeValidationError("invalid prepared outcome")
    if (
        not isinstance(outcome.key, OutcomeKey)
        or not _nonempty_utf8(outcome.key.worker_agent_id)
        or not _nonempty_utf8(outcome.key.task_id)
        or not _nonempty_utf8(outcome.sender_id)
        or not _nonempty_utf8(outcome.request_envelope_id)
        or not _valid_hash(outcome.request_fingerprint)
        or not _valid_hash(outcome.terminal_payload_hash)
        or outcome.publish_state not in ("prepared", "published")
        or not _nonempty_utf8(outcome.completed_at)
    ):
        raise OutcomeValidationError("invalid prepared outcome")

    terminal, terminal_bytes = _canonical_mapping(
        outcome.terminal_envelope,
        failure_message="invalid prepared outcome",
    )
    terminal_id = terminal.get("id")
    if not _nonempty_utf8(terminal_id):
        raise OutcomeValidationError("invalid prepared outcome")

    if outcome.publish_state == "prepared":
        if outcome.receipt is not None:
            raise OutcomeValidationError("invalid prepared outcome")
        receipt = None
    else:
        try:
            receipt, _ = _validate_receipt(outcome.receipt)
        except OutcomeValidationError:
            raise OutcomeValidationError("invalid prepared outcome") from None
        if receipt.envelope_id != terminal_id:
            raise OutcomeValidationError("invalid prepared outcome")

    return replace(outcome, terminal_envelope=terminal, receipt=receipt), terminal_bytes


def _validate_key(key: object) -> OutcomeKey:
    if (
        not isinstance(key, OutcomeKey)
        or not _nonempty_utf8(key.worker_agent_id)
        or not _nonempty_utf8(key.task_id)
    ):
        raise OutcomeValidationError("invalid outcome key")
    return key


def _normalize_sql(value: str) -> str:
    return " ".join(value.split())


def _prepare_database_path(path: Path) -> None:
    raw_path = os.fspath(path)
    if raw_path in {":memory:", ""} or raw_path.startswith("file:"):
        raise OutcomeValidationError("invalid outcome database path")

    parent = path.parent
    try:
        parent_stat = parent.stat()
    except OSError:
        raise OutcomeValidationError("invalid outcome database path") from None
    if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_mode & 0o222 == 0:
        raise OutcomeValidationError("invalid outcome database path")

    try:
        path_stat = path.lstat()
    except FileNotFoundError:
        try:
            descriptor = os.open(
                path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR,
                0o600,
            )
        except FileExistsError:
            try:
                path_stat = path.lstat()
            except OSError:
                raise OutcomeValidationError("invalid outcome database path") from None
            if not stat.S_ISREG(path_stat.st_mode):
                raise OutcomeValidationError("invalid outcome database path")
        except OSError:
            raise OutcomeValidationError("invalid outcome database path") from None
        else:
            try:
                os.fchmod(descriptor, 0o600)
            finally:
                os.close(descriptor)
            return
    except OSError:
        raise OutcomeValidationError("invalid outcome database path") from None

    if not stat.S_ISREG(path_stat.st_mode) or stat.S_ISLNK(path_stat.st_mode):
        raise OutcomeValidationError("invalid outcome database path")


class DisabledOutcomeStore:
    @property
    def enabled(self) -> bool:
        return False

    def lookup(self, key: OutcomeKey) -> PreparedOutcome | None:
        return None

    def prepare(self, outcome: PreparedOutcome) -> PreparedOutcome:
        normalized, _ = _normalize_outcome(outcome)
        return normalized

    def mark_published(
        self,
        key: OutcomeKey,
        receipt: PublicationReceipt,
    ) -> PreparedOutcome:
        raise OutcomeStoreDisabled("outcome ledger is disabled")

    def close(self) -> None:
        return None


class SQLiteOutcomeStore:
    def __init__(self, path: Path) -> None:
        if not isinstance(path, Path):
            raise OutcomeValidationError("invalid outcome database path")
        _prepare_database_path(path)
        self._lock = RLock()
        self._closed = False
        try:
            self._connection = sqlite3.connect(
                path,
                timeout=_BUSY_TIMEOUT_MS / 1_000,
                isolation_level=None,
                check_same_thread=False,
            )
            self._connection.row_factory = sqlite3.Row
            self._connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._initialize_schema()
            self._configure_connection()
        except OutcomeStoreError:
            connection = getattr(self, "_connection", None)
            if isinstance(connection, sqlite3.Connection):
                connection.close()
            self._closed = True
            raise
        except (OSError, sqlite3.Error):
            connection = getattr(self, "_connection", None)
            if isinstance(connection, sqlite3.Connection):
                connection.close()
            self._closed = True
            raise OutcomeStoreError("outcome store initialization failed") from None

    @property
    def enabled(self) -> bool:
        return True

    def _ensure_open(self) -> None:
        if self._closed:
            raise OutcomeStoreClosed("outcome store is closed")

    def _run_transaction(self, operation: Callable[[], _T]) -> _T:
        try:
            self._connection.execute("BEGIN IMMEDIATE")
            result = operation()
            self._connection.execute("COMMIT")
            return result
        except BaseException as exc:
            if self._connection.in_transaction:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.Error:
                    self._connection.close()
                    self._closed = True
            if isinstance(exc, OutcomeStoreError):
                raise
            if isinstance(exc, Exception):
                raise OutcomeStoreError("outcome store transaction failed") from None
            raise

    def _initialize_schema(self) -> None:
        def initialize() -> None:
            version_row = self._connection.execute("PRAGMA user_version").fetchone()
            if version_row is None:
                raise OutcomeSchemaError("unsupported outcome schema")
            version = cast(int, version_row[0])
            schema_objects = self._application_schema_objects()
            if version == 0:
                if schema_objects:
                    raise OutcomeSchemaError("unsupported outcome schema")
                for schema in _EXPECTED_SCHEMAS.values():
                    self._connection.execute(schema)
                self._connection.executemany(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)",
                    sorted(_METADATA.items()),
                )
                self._connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version != _SCHEMA_VERSION:
                raise OutcomeSchemaError("unsupported outcome schema")
            self._validate_schema()

        self._run_transaction(initialize)

    def _configure_connection(self) -> None:
        journal_row = self._connection.execute("PRAGMA journal_mode = WAL").fetchone()
        self._connection.execute("PRAGMA synchronous = FULL")
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        synchronous_row = self._connection.execute("PRAGMA synchronous").fetchone()
        foreign_keys_row = self._connection.execute("PRAGMA foreign_keys").fetchone()
        busy_timeout_row = self._connection.execute("PRAGMA busy_timeout").fetchone()
        if journal_row != ("wal",) and (
            journal_row is None
            or len(journal_row) != 1
            or str(journal_row[0]).casefold() != "wal"
        ):
            raise OutcomeSchemaError("unsupported outcome schema")
        if (
            synchronous_row is None
            or cast(int, synchronous_row[0]) != 2
            or foreign_keys_row is None
            or cast(int, foreign_keys_row[0]) != 1
            or busy_timeout_row is None
            or cast(int, busy_timeout_row[0]) != _BUSY_TIMEOUT_MS
        ):
            raise OutcomeSchemaError("unsupported outcome schema")

    def _application_schema_objects(self) -> set[tuple[str, str]]:
        rows = self._connection.execute(
            """
            SELECT type, name
            FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
        return {(cast(str, row[0]), cast(str, row[1])) for row in rows}

    def _validate_schema(self) -> None:
        if self._application_schema_objects() != _EXPECTED_SCHEMA_OBJECTS:
            raise OutcomeSchemaError("unsupported outcome schema")

        rows = self._connection.execute(
            """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'table' AND name IN ('metadata', 'outcomes', 'request_attempts')
            """
        ).fetchall()
        actual_schemas = {
            cast(str, row[0]): row[1] for row in rows if isinstance(row[1], str)
        }
        if set(actual_schemas) != set(_EXPECTED_SCHEMAS):
            raise OutcomeSchemaError("unsupported outcome schema")
        for name, expected in _EXPECTED_SCHEMAS.items():
            if _normalize_sql(actual_schemas[name]) != _normalize_sql(expected):
                raise OutcomeSchemaError("unsupported outcome schema")

        metadata_rows = self._connection.execute(
            "SELECT key, value FROM metadata"
        ).fetchall()
        metadata = {cast(str, row[0]): cast(str, row[1]) for row in metadata_rows}
        if metadata != _METADATA:
            raise OutcomeSchemaError("unsupported outcome schema")

    def _select_row(self, key: OutcomeKey) -> sqlite3.Row | None:
        row = self._connection.execute(
            _OUTCOME_SELECT,
            (key.worker_agent_id, key.task_id),
        ).fetchone()
        if row is None:
            return None
        return cast(sqlite3.Row, row)

    def _row_to_outcome(self, row: sqlite3.Row) -> PreparedOutcome:
        try:
            terminal_blob_value = row[5]
            if not isinstance(terminal_blob_value, bytes):
                raise OutcomeSchemaError("unsupported outcome schema")
            terminal_blob = terminal_blob_value
            terminal, canonical_terminal = _canonical_mapping(
                cast(object, json.loads(terminal_blob)),
                failure_message="invalid prepared outcome",
            )
            if canonical_terminal != terminal_blob:
                raise OutcomeSchemaError("unsupported outcome schema")

            receipt_blob_value = row[9]
            if receipt_blob_value is None:
                receipt = None
            elif isinstance(receipt_blob_value, bytes):
                receipt = _decode_receipt(receipt_blob_value)
            else:
                raise OutcomeSchemaError("unsupported outcome schema")

            state_value = row[7]
            if state_value not in ("prepared", "published"):
                raise OutcomeSchemaError("unsupported outcome schema")
            outcome = PreparedOutcome(
                key=OutcomeKey(cast(str, row[0]), cast(str, row[1])),
                sender_id=cast(str, row[2]),
                request_envelope_id=cast(str, row[3]),
                request_fingerprint=cast(str, row[4]),
                terminal_envelope=terminal,
                terminal_payload_hash=cast(str, row[6]),
                publish_state=cast(PublishState, state_value),
                completed_at=cast(str, row[8]),
                receipt=receipt,
            )
            normalized, normalized_terminal = _normalize_outcome(outcome)
        except OutcomeSchemaError:
            raise
        except (OutcomeStoreError, TypeError, ValueError, UnicodeError):
            raise OutcomeSchemaError("unsupported outcome schema") from None
        if normalized_terminal != terminal_blob:
            raise OutcomeSchemaError("unsupported outcome schema")
        return normalized

    def lookup(self, key: OutcomeKey) -> PreparedOutcome | None:
        with self._lock:
            self._ensure_open()
            normalized_key = _validate_key(key)
            try:
                row = self._select_row(normalized_key)
                return None if row is None else self._row_to_outcome(row)
            except OutcomeStoreError:
                raise
            except sqlite3.Error:
                raise OutcomeStoreError("outcome store read failed") from None

    def prepare(self, outcome: PreparedOutcome) -> PreparedOutcome:
        with self._lock:
            self._ensure_open()
            normalized, terminal_blob = _normalize_outcome(outcome)

            def persist() -> PreparedOutcome:
                row = self._select_row(normalized.key)
                if row is None:
                    if (
                        normalized.publish_state != "prepared"
                        or normalized.receipt is not None
                    ):
                        raise OutcomeValidationError("invalid prepared outcome")
                    self._connection.execute(
                        """
                        INSERT INTO outcomes(
                            worker_agent_id, task_id, sender_id,
                            request_envelope_id, request_fingerprint,
                            terminal_envelope, terminal_payload_hash,
                            publish_state, completed_at, receipt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'prepared', ?, NULL)
                        """,
                        (
                            normalized.key.worker_agent_id,
                            normalized.key.task_id,
                            normalized.sender_id,
                            normalized.request_envelope_id,
                            normalized.request_fingerprint,
                            sqlite3.Binary(terminal_blob),
                            normalized.terminal_payload_hash,
                            normalized.completed_at,
                        ),
                    )
                else:
                    persisted = self._row_to_outcome(row)
                    persisted_blob = row[5]
                    if (
                        persisted.sender_id != normalized.sender_id
                        or persisted.request_fingerprint
                        != normalized.request_fingerprint
                        or not isinstance(persisted_blob, bytes)
                        or persisted_blob != terminal_blob
                        or persisted.terminal_payload_hash
                        != normalized.terminal_payload_hash
                        or persisted.completed_at != normalized.completed_at
                    ):
                        raise OutcomeConflict("outcome conflicts with durable record")

                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO request_attempts(
                        worker_agent_id, task_id, wire_id
                    ) VALUES (?, ?, ?)
                    """,
                    (
                        normalized.key.worker_agent_id,
                        normalized.key.task_id,
                        normalized.request_envelope_id,
                    ),
                )
                persisted_row = self._select_row(normalized.key)
                if persisted_row is None:
                    raise OutcomeStoreError("outcome store transaction failed")
                return self._row_to_outcome(persisted_row)

            return self._run_transaction(persist)

    def mark_published(
        self,
        key: OutcomeKey,
        receipt: PublicationReceipt,
    ) -> PreparedOutcome:
        with self._lock:
            self._ensure_open()
            normalized_key = _validate_key(key)
            normalized_receipt, receipt_blob = _validate_receipt(receipt)

            def mark() -> PreparedOutcome:
                row = self._select_row(normalized_key)
                if row is None:
                    raise OutcomeNotFound("outcome is not prepared")
                persisted = self._row_to_outcome(row)
                terminal_id = persisted.terminal_envelope.get("id")
                if normalized_receipt.envelope_id != terminal_id:
                    raise OutcomeValidationError(
                        "receipt envelope does not match terminal"
                    )
                if persisted.publish_state == "published":
                    return persisted

                self._connection.execute(
                    """
                    UPDATE outcomes
                    SET publish_state = 'published', receipt = ?
                    WHERE worker_agent_id = ? AND task_id = ?
                      AND publish_state = 'prepared' AND receipt IS NULL
                    """,
                    (
                        sqlite3.Binary(receipt_blob),
                        normalized_key.worker_agent_id,
                        normalized_key.task_id,
                    ),
                )
                published_row = self._select_row(normalized_key)
                if published_row is None:
                    raise OutcomeStoreError("outcome store transaction failed")
                return self._row_to_outcome(published_row)

            return self._run_transaction(mark)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                self._connection.close()
            except sqlite3.Error:
                raise OutcomeStoreError("outcome store close failed") from None
            finally:
                self._closed = True


__all__ = [
    "DisabledOutcomeStore",
    "OutcomeConflict",
    "OutcomeKey",
    "OutcomeNotFound",
    "OutcomeSchemaError",
    "OutcomeStore",
    "OutcomeStoreClosed",
    "OutcomeStoreDisabled",
    "OutcomeStoreError",
    "OutcomeValidationError",
    "PreparedOutcome",
    "SQLiteOutcomeStore",
]
