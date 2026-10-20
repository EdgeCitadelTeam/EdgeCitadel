"""Transactional Core evidence accounting; encoded limits are not physical limits."""

from __future__ import annotations

import sqlite3

from edgecitadel_agentd.trace_capacity import physical_storage
from edgecitadel_agentd.trace_contract import TraceContractError

RAW_BYTES = 8 * 1024 * 1024 * 1024
PHYSICAL_PRESSURE_BYTES = 16 * 1024 * 1024 * 1024
WAL_PRESSURE_BYTES = 64 * 1024 * 1024
WRITE_HEADROOM_BYTES = 8 * 1024 * 1024
ROW_LIMITS = {
    "trace_raw_events": 10_000_000,
    "trace_ingest_positions": 20_000_000,
    "trace_ingest_conflicts": 1_000_000,
    "trace_loss_ranges": 1_000_000,
}
TABLES = (*ROW_LIMITS, "trace_rejected_positions", "trace_rejected_identities")
BACKFILL_BATCH_ROWS = 256


def initialize(connection: sqlite3.Connection) -> None:
    """Prepare counters and resumable accounting under the caller's transaction."""
    if not connection.in_transaction:
        raise ValueError("capacity_requires_transaction")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_capacity_usage ("
        "table_name TEXT PRIMARY KEY, rows INTEGER NOT NULL CHECK(rows>=0), "
        "payload_bytes INTEGER NOT NULL CHECK(payload_bytes>=0))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trace_capacity_backfill ("
        "table_name TEXT PRIMARY KEY, after_rowid INTEGER, "
        "complete INTEGER NOT NULL CHECK(complete IN (0,1)))"
    )
    for table in TABLES:
        if (
            connection.execute(
                "SELECT 1 FROM trace_capacity_usage WHERE table_name=?", (table,)
            ).fetchone()
            is None
        ):
            connection.execute(
                "INSERT INTO trace_capacity_usage VALUES(?,0,0)",
                (table,),
            )
            connection.execute(
                "INSERT OR REPLACE INTO trace_capacity_backfill VALUES(?,NULL,0)",
                (table,),
            )
        # Existing counters predate resumable migration and are already complete.
        connection.execute(
            "INSERT OR IGNORE INTO trace_capacity_backfill VALUES(?,NULL,1)",
            (table,),
        )
        for operation in ("INSERT", "DELETE", "UPDATE"):
            connection.execute(
                f"CREATE TRIGGER IF NOT EXISTS {table}_backfill_{operation.lower()} "
                f"BEFORE {operation} ON {table} WHEN EXISTS("
                "SELECT 1 FROM trace_capacity_backfill WHERE complete=0) "
                "BEGIN SELECT RAISE(ABORT,'core_capacity_backfill_pending'); END"
            )
        new = (
            "length(CAST(NEW.event_json AS BLOB))"
            if table == "trace_raw_events"
            else "0"
        )
        old = (
            "length(CAST(OLD.event_json AS BLOB))"
            if table == "trace_raw_events"
            else "0"
        )
        for operation, count_delta, byte_delta in (
            ("INSERT", "+1", f"+{new}"),
            ("DELETE", "-1", f"-{old}"),
            ("UPDATE", "+0", f"+{new}-{old}"),
        ):
            connection.execute(
                f"CREATE TRIGGER IF NOT EXISTS {table}_capacity_{operation.lower()} "
                f"AFTER {operation} ON {table} BEGIN "
                f"UPDATE trace_capacity_usage SET rows=rows{count_delta}, "
                f"payload_bytes=payload_bytes{byte_delta} WHERE table_name='{table}'; END"
            )


def backfill_step(connection: sqlite3.Connection) -> bool:
    """Commit at most one indexed rowid batch; True means all counters are ready.

    Evidence writes are fenced by triggers until all tables finish. Each cursor
    and counter update commits together, so restart neither skips nor recounts.
    The row bound does not promise a wall-clock deadline on filesystem access.
    """
    if connection.in_transaction:
        raise ValueError("backfill_requires_idle_connection")
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        pending = connection.execute(
            "SELECT table_name,after_rowid FROM trace_capacity_backfill "
            "WHERE complete=0 ORDER BY table_name LIMIT 1"
        ).fetchone()
        if pending is None:
            return True
        table, after = pending
        if table not in TABLES:
            raise TraceContractError("core_capacity_accounting_unavailable")
        payload = (
            "length(CAST(event_json AS BLOB))" if table == "trace_raw_events" else "0"
        )
        if (
            table == "trace_raw_events"
            and connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_payloads'"
            ).fetchone()
        ):
            payload += (
                "+coalesce((SELECT length(CAST(p.event_json AS BLOB)) FROM trace_payloads p "
                "WHERE p.ingest_seq=trace_raw_events.ingest_seq),0)"
            )
        where, parameters = ("", ()) if after is None else ("WHERE rowid>?", (after,))
        rows = connection.execute(
            f"SELECT rowid,{payload} FROM {table} {where} ORDER BY rowid LIMIT ?",
            (*parameters, BACKFILL_BATCH_ROWS),
        ).fetchall()
        connection.execute(
            "UPDATE trace_capacity_usage SET rows=rows+?,payload_bytes=payload_bytes+? "
            "WHERE table_name=?",
            (len(rows), sum(row[1] for row in rows), table),
        )
        connection.execute(
            "UPDATE trace_capacity_backfill SET after_rowid=?,complete=? WHERE table_name=?",
            (
                rows[-1][0] if rows else after,
                int(len(rows) < BACKFILL_BATCH_ROWS),
                table,
            ),
        )
    return False


def usage(connection: sqlite3.Connection) -> dict[str, dict[str, int]]:
    if connection.execute(
        "SELECT 1 FROM trace_capacity_backfill WHERE complete=0 LIMIT 1"
    ).fetchone():
        raise TraceContractError("core_capacity_accounting_unavailable")
    rows = connection.execute(
        "SELECT table_name,rows,payload_bytes FROM trace_capacity_usage"
    ).fetchall()
    result = {
        name: {"rows": count, "payload_bytes": size} for name, count, size in rows
    }
    if set(result) != set(TABLES):
        raise TraceContractError("core_capacity_accounting_unavailable")
    return result


def check(connection: sqlite3.Connection, *, rejection_limit: int) -> None:
    """Call before commit; capacity failure rolls back data, counters and cursor."""
    if not connection.in_transaction:
        raise ValueError("capacity_requires_transaction")
    values = usage(connection)
    limits = {
        **ROW_LIMITS,
        "trace_rejected_positions": rejection_limit,
        "trace_rejected_identities": rejection_limit,
    }
    if values["trace_raw_events"]["payload_bytes"] > RAW_BYTES or any(
        values[table]["rows"] > limit for table, limit in limits.items()
    ):
        raise TraceContractError("core_capacity_exceeded")


def snapshot(connection: sqlite3.Connection, *, rejection_limit: int) -> dict:
    """Fixed-size operational view; full state remains visible after duplicate ACKs."""
    values = usage(connection)
    limits = {
        **ROW_LIMITS,
        "trace_rejected_positions": rejection_limit,
        "trace_rejected_identities": rejection_limit,
    }
    physical_values = physical(connection)
    return {
        "physical_storage": physical_values,
        "storage_pressure": pressured(physical_values),
        "storage_usage": values,
        "storage_limits": {"raw_payload_bytes": RAW_BYTES, "rows": limits},
        "storage_at_capacity": values["trace_raw_events"]["payload_bytes"] >= RAW_BYTES
        or any(values[table]["rows"] >= limit for table, limit in limits.items()),
    }


def physical(connection: sqlite3.Connection) -> dict[str, int]:
    """Shared-file pressure observation, not a reservation against other writers."""
    try:
        result = physical_storage(connection)
    except OSError as error:
        raise TraceContractError("core_physical_storage_unavailable") from error
    result.pop("optional_pressure_limit_bytes")
    return {
        **result,
        "pressure_limit_bytes": PHYSICAL_PRESSURE_BYTES,
        "wal_pressure_limit_bytes": WAL_PRESSURE_BYTES,
        "write_headroom_bytes": WRITE_HEADROOM_BYTES,
    }


def pressured(values: dict[str, int]) -> bool:
    return (
        values["pressure_bytes"] + WRITE_HEADROOM_BYTES >= PHYSICAL_PRESSURE_BYTES
        or values["wal_file_bytes"] + WRITE_HEADROOM_BYTES >= WAL_PRESSURE_BYTES
    )


def reclaim(connection: sqlite3.Connection) -> None:
    """Try a nonblocking checkpoint outside transactions; never cancel readers."""
    if connection.in_transaction:
        raise ValueError("checkpoint_requires_idle_connection")
    values = physical(connection)
    if not values["wal_file_bytes"] or not pressured(values):
        return
    timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
    try:
        connection.execute("PRAGMA busy_timeout=0")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    finally:
        connection.execute(f"PRAGMA busy_timeout={timeout}")


def admit_write(connection: sqlite3.Connection) -> None:
    """Check again with the write lock held, before making any evidence mutation.

    Headroom is conservative, not proof of a universal transaction size bound.
    Command writers share these files and are not governed by this admission rule.
    """
    if not connection.in_transaction:
        raise ValueError("capacity_requires_transaction")
    if pressured(physical(connection)):
        raise TraceContractError("core_physical_pressure")
