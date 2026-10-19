"""Checked, resumable Core payload layout operations.

Collector activation is deliberately separate: callers must adopt mixed-layout
reads and these writes together before invoking prepare on a live database.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any

from edgecitadel_agentd.trace_contract import TraceContractError

from . import trace_capacity, trace_payload_read

VERSION = 1
BATCH_ROWS = 256
FUNCTION = "edgecitadel_core_payload_layout"
GUARDED_TABLES = (
    *trace_capacity.TABLES,
    "trace_capacity_usage",
    "trace_capacity_backfill",
    "trace_collector",
    "trace_poison_counts",
    "trace_retention_state",
    "trace_payloads",
    "trace_payload_layout",
)


def _guards() -> dict[str, str]:
    result = {}
    for table in GUARDED_TABLES:
        for operation in ("INSERT", "UPDATE", "DELETE"):
            name = f"{table}_layout_{operation.lower()}"
            result[name] = (
                f"CREATE TRIGGER {name} BEFORE {operation} ON {table} "
                f"WHEN {FUNCTION}()!={VERSION} BEGIN "
                "SELECT RAISE(ABORT,'unsupported_core_payload_layout'); END"
            )
    return result


def _payload_triggers() -> dict[str, str]:
    result = {}
    for operation, value in (
        ("INSERT", "+length(CAST(NEW.event_json AS BLOB))"),
        ("DELETE", "-length(CAST(OLD.event_json AS BLOB))"),
    ):
        name = f"trace_payload_bytes_{operation.lower()}"
        result[name] = (
            f"CREATE TRIGGER {name} AFTER {operation} ON trace_payloads BEGIN "
            f"UPDATE trace_capacity_usage SET payload_bytes=payload_bytes{value} "
            "WHERE table_name='trace_raw_events'; END"
        )
        name = f"trace_payload_backfill_{operation.lower()}"
        result[name] = (
            f"CREATE TRIGGER {name} BEFORE {operation} ON trace_payloads "
            "WHEN EXISTS(SELECT 1 FROM trace_capacity_backfill WHERE complete=0) "
            "BEGIN SELECT RAISE(ABORT,'core_capacity_backfill_pending'); END"
        )
    result["trace_payload_immutable"] = (
        "CREATE TRIGGER trace_payload_immutable BEFORE UPDATE ON trace_payloads "
        "BEGIN SELECT RAISE(ABORT,'immutable_core_payload'); END"
    )
    result["trace_payload_inline_writer"] = (
        "CREATE TRIGGER trace_payload_inline_writer BEFORE INSERT ON trace_raw_events "
        "WHEN NEW.event_json!='' AND (SELECT complete FROM trace_payload_layout WHERE singleton=1)=1 "
        "BEGIN SELECT RAISE(ABORT,'core_payload_write_required'); END"
    )
    result["trace_payload_expiry_writer"] = (
        "CREATE TRIGGER trace_payload_expiry_writer BEFORE UPDATE OF event_json,payload_expired_at_ms "
        "ON trace_raw_events WHEN (NEW.event_json!='' OR NEW.payload_expired_at_ms IS NOT NULL) "
        "AND EXISTS(SELECT 1 FROM trace_payloads WHERE ingest_seq=OLD.ingest_seq) "
        "BEGIN SELECT RAISE(ABORT,'core_payload_write_required'); END"
    )
    return result


def is_prepared(connection: sqlite3.Connection) -> bool:
    """Detect a durable layout without granting a connection write capability."""
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_payload_layout'"
        ).fetchone()
        is not None
    )


def open_layout(connection: sqlite3.Connection) -> None:
    """Authorize a writer only after checking its durable layout and guards."""
    if connection.in_transaction:
        raise ValueError("payload_layout_requires_idle_connection")
    connection.create_function(FUNCTION, 0, None)
    try:
        version = connection.execute(
            "SELECT version FROM trace_payload_layout WHERE singleton=1"
        ).fetchone()
        actual = dict(
            connection.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
            )
        )
        expected = {**_guards(), **_payload_triggers()}
        expiry_index = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='trace_payload_expiry'"
        ).fetchone()
        if (
            version is None
            or version[0] != VERSION
            or any(actual.get(name) != sql for name, sql in expected.items())
            or expiry_index is None
            or expiry_index[0]
            != "CREATE INDEX trace_payload_expiry ON trace_payloads(received_at_ms,ingest_seq)"
        ):
            raise TraceContractError("core_payload_layout_unavailable")
    except sqlite3.Error as error:
        raise TraceContractError("core_payload_layout_unavailable") from error
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise TraceContractError("core_payload_layout_unavailable")
    connection.create_function(FUNCTION, 0, lambda: VERSION)


def prepare(connection: sqlite3.Connection) -> None:
    """Explicitly fence the old trace writer and prepare an unactivated layout."""
    if connection.in_transaction:
        raise ValueError("payload_layout_requires_idle_connection")
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_payload_layout'"
    ).fetchone():
        open_layout(connection)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS trace_payload_test_expiry ON trace_payloads("
            "(json_extract(event_json,'$.test_run_id') IS NULL),received_at_ms,ingest_seq)"
        )
        return
    connection.execute("PRAGMA foreign_keys=ON")
    connection.create_function(FUNCTION, 0, lambda: VERSION)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        trace_capacity.usage(connection)
        trace_capacity.admit_write(connection)
        connection.execute(
            "CREATE TABLE trace_payload_layout (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "version INTEGER NOT NULL, after_ingest_seq INTEGER NOT NULL CHECK(after_ingest_seq>=0), "
            "complete INTEGER NOT NULL CHECK(complete IN (0,1)))"
        )
        connection.execute(
            "INSERT INTO trace_payload_layout VALUES(1,?,0,0)", (VERSION,)
        )
        connection.execute(
            "CREATE TABLE trace_payloads (ingest_seq INTEGER PRIMARY KEY "
            "REFERENCES trace_raw_events(ingest_seq) DEFERRABLE INITIALLY DEFERRED, "
            "received_at_ms INTEGER NOT NULL, event_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE INDEX trace_payload_expiry ON trace_payloads(received_at_ms,ingest_seq)"
        )
        connection.execute(
            "CREATE INDEX trace_payload_test_expiry ON trace_payloads("
            "(json_extract(event_json,'$.test_run_id') IS NULL),received_at_ms,ingest_seq)"
        )
        for sql in {**_guards(), **_payload_triggers()}.values():
            connection.execute(sql)
    open_layout(connection)


def read_payload(
    connection: sqlite3.Connection, ingest_seq: int
) -> dict[str, Any] | None:
    """Use the shared read-only resolver with the runtime's error contract."""
    try:
        return trace_payload_read.read_payload(connection, ingest_seq)
    except trace_payload_read.PayloadReadError as error:
        raise TraceContractError(str(error)) from error


def migrate_batch(connection: sqlite3.Connection) -> bool:
    """Compact at most 256 identities; content, counters and cursor commit together."""
    open_layout(connection)
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        after, complete = connection.execute(
            "SELECT after_ingest_seq,complete FROM trace_payload_layout WHERE singleton=1"
        ).fetchone()
        if complete:
            return True
        before_usage = trace_capacity.usage(connection)
        trace_capacity.admit_write(connection)
        rows = connection.execute(
            "SELECT rowid,node_id,source_epoch,event_id,source_seq,event_sha256,event_json,"
            "received_at_ms,ingest_seq,payload_expired_at_ms FROM trace_raw_events "
            "WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
            (after, BATCH_ROWS),
        ).fetchall()
        for row in rows:
            existing = connection.execute(
                "SELECT event_json FROM trace_payloads WHERE ingest_seq=?",
                (row[8],),
            ).fetchone()
            if existing:
                if (
                    row[6]
                    or row[9] is not None
                    or hashlib.sha256(existing[0].encode()).hexdigest() != row[5]
                ):
                    raise TraceContractError("core_payload_layout_unavailable")
                continue
            if row[9] is None:
                if not row[6] or hashlib.sha256(row[6].encode()).hexdigest() != row[5]:
                    raise TraceContractError("core_payload_layout_unavailable")
                connection.execute(
                    "INSERT INTO trace_payloads VALUES(?,?,?)", (row[8], row[7], row[6])
                )
            elif row[6]:
                raise TraceContractError("core_payload_layout_unavailable")
            connection.execute("DELETE FROM trace_raw_events WHERE rowid=?", (row[0],))
            slim = list(row)
            slim[6] = ""
            connection.execute(
                "INSERT INTO trace_raw_events(rowid,node_id,source_epoch,event_id,source_seq,"
                "event_sha256,event_json,received_at_ms,ingest_seq,payload_expired_at_ms) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                slim,
            )
        if trace_capacity.usage(connection) != before_usage:
            raise TraceContractError("core_capacity_accounting_unavailable")
        done = len(rows) < BATCH_ROWS
        connection.execute(
            "UPDATE trace_payload_layout SET after_ingest_seq=?,complete=? WHERE singleton=1",
            (rows[-1][8] if rows else after, int(done)),
        )
    return done


def expire_batch(connection: sqlite3.Connection, *, before_ms: int, now_ms: int) -> int:
    """Expire indexed payloads only after the inline migration is complete."""
    if (
        any(
            type(value) is not int or not 0 <= value <= 2**53 - 1
            for value in (before_ms, now_ms)
        )
        or before_ms > now_ms
    ):
        raise ValueError("invalid_retention_time")
    open_layout(connection)
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        if not connection.execute(
            "SELECT complete FROM trace_payload_layout WHERE singleton=1"
        ).fetchone()[0]:
            raise TraceContractError("core_payload_migration_pending")
        trace_capacity.usage(connection)
        rows = []
        for normal in (0, 1):
            rows.extend(
                connection.execute(
                    "SELECT ingest_seq FROM trace_payloads "
                    "WHERE (json_extract(event_json,'$.test_run_id') IS NULL)=? "
                    "AND received_at_ms<? ORDER BY received_at_ms,ingest_seq LIMIT ?",
                    (normal, before_ms, BATCH_ROWS - len(rows)),
                ).fetchall()
            )
            if len(rows) == BATCH_ROWS:
                break
        if rows:
            trace_capacity.admit_write(connection)
            connection.executemany(
                "DELETE FROM trace_payloads WHERE ingest_seq=?", rows
            )
            connection.executemany(
                "UPDATE trace_raw_events SET payload_expired_at_ms=? WHERE ingest_seq=?",
                [(now_ms, row[0]) for row in rows],
            )
    return len(rows)
