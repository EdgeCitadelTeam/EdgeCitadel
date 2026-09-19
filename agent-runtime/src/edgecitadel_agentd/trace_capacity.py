"""Transactional journal payload accounting and reserved control-record capacity."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .trace_contract import TraceContractError

NORMAL_LIMIT_BYTES = 256 * 1024 * 1024
CONTROL_RESERVE_BYTES = 1024 * 1024
PHYSICAL_PRESSURE_BYTES = 256 * 1024 * 1024

CAPACITY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trace_storage_usage (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    event_bytes INTEGER NOT NULL CHECK(event_bytes>=0),
    event_count INTEGER NOT NULL CHECK(event_count>=0)
);
INSERT OR REPLACE INTO trace_storage_usage
SELECT 1,COALESCE(SUM(event_bytes),0),COUNT(*) FROM trace_journal;
"""

CAPACITY_TRIGGERS = (
    """CREATE TRIGGER IF NOT EXISTS trace_usage_insert AFTER INSERT ON trace_journal
    BEGIN UPDATE trace_storage_usage SET event_bytes=event_bytes+NEW.event_bytes,
        event_count=event_count+1 WHERE singleton=1; END""",
    """CREATE TRIGGER IF NOT EXISTS trace_usage_delete AFTER DELETE ON trace_journal
    BEGIN UPDATE trace_storage_usage SET event_bytes=event_bytes-OLD.event_bytes,
        event_count=event_count-1 WHERE singleton=1; END""",
    """CREATE TRIGGER IF NOT EXISTS trace_usage_update AFTER UPDATE OF event_bytes ON trace_journal
    BEGIN UPDATE trace_storage_usage SET event_bytes=event_bytes-OLD.event_bytes+NEW.event_bytes
        WHERE singleton=1; END""",
)


def admit_event(
    db: sqlite3.Connection,
    *,
    event_bytes: int,
    kind: str,
    reserve_capacity: bool = False,
) -> None:
    """Caller holds the write transaction; retries of existing events bypass this."""
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    usage = db.execute(
        "SELECT event_bytes FROM trace_storage_usage WHERE singleton=1"
    ).fetchone()
    if usage is None:
        raise TraceContractError("storage_unavailable")
    if kind in {"model", "tool"} and not reserve_capacity:
        try:
            pressure = physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure + event_bytes > PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
    limit = NORMAL_LIMIT_BYTES
    if reserve_capacity or kind not in {"model", "tool"}:
        limit += CONTROL_RESERVE_BYTES
    if usage[0] + event_bytes > limit:
        raise TraceContractError("quota_exceeded")


def physical_storage(db: sqlite3.Connection) -> dict[str, int]:
    """Observe trace-owned stores and their named SQLite sidecar files.

    Excludes the declared task_state database; reusable trace pages still count.
    Unknown attached schemas remain conservatively included. Pressure uses
    the larger of file length, filesystem allocation and pending database pages.
    This is not a reservation: super-journals, unlinked temporary files and future
    transaction growth are not covered by this named-file observation.
    """
    result = dict.fromkeys(
        (
            "database_file_bytes",
            "wal_file_bytes",
            "shm_file_bytes",
            "rollback_journal_file_bytes",
            "filesystem_allocated_bytes",
            "allocated_page_bytes",
            "reusable_page_bytes",
            "pressure_bytes",
        ),
        0,
    )
    for _, schema, filename in db.execute("PRAGMA database_list").fetchall():
        if schema == "task_state":
            continue
        # Schema names are SQLite-owned identifiers, not SQL value parameters.
        identifier = '"' + schema.replace('"', '""') + '"'
        page_size = int(db.execute(f"PRAGMA {identifier}.page_size").fetchone()[0])
        pages = int(db.execute(f"PRAGMA {identifier}.page_count").fetchone()[0])
        free_pages = int(
            db.execute(f"PRAGMA {identifier}.freelist_count").fetchone()[0]
        )
        result["allocated_page_bytes"] += pages * page_size
        result["reusable_page_bytes"] += free_pages * page_size
        for suffix, field in (
            ("", "database_file_bytes"),
            ("-wal", "wal_file_bytes"),
            ("-shm", "shm_file_bytes"),
            ("-journal", "rollback_journal_file_bytes"),
        ):
            length = allocated = 0
            if filename:
                try:
                    info = Path(f"{filename}{suffix}").stat()
                    length, allocated = info.st_size, info.st_blocks * 512
                except FileNotFoundError:
                    pass
            result[field] += length
            result["filesystem_allocated_bytes"] += allocated
            result["pressure_bytes"] += max(
                length, allocated, pages * page_size if not suffix else 0
            )
    result["optional_pressure_limit_bytes"] = PHYSICAL_PRESSURE_BYTES
    return result


def reclaim_wal_pressure(db: sqlite3.Connection) -> bool:
    """Attempt WAL reclamation outside transactions, without waiting on readers.

    Caller holds the connection lock. A busy checkpoint leaves the WAL intact;
    optional admission remains closed until a later maintenance pass can reclaim
    it. Return False when a checkpoint is blocked so cache maintenance can defer
    writes that would grow the pinned WAL. No reader is cancelled and no database
    VACUUM is performed.
    """
    if db.in_transaction:
        raise TraceContractError("trace_checkpoint_transaction_active")
    try:
        storage = physical_storage(db)
    except OSError as error:
        raise sqlite3.OperationalError("physical_storage_unavailable") from error
    if (
        storage["pressure_bytes"] < PHYSICAL_PRESSURE_BYTES
        or not storage["wal_file_bytes"]
    ):
        return True
    timeout = int(db.execute("PRAGMA busy_timeout").fetchone()[0])
    try:
        db.execute("PRAGMA busy_timeout=0")
        checkpoint = db.execute("PRAGMA main.wal_checkpoint(TRUNCATE)").fetchone()
        return checkpoint[0] == 0
    finally:
        db.execute(f"PRAGMA busy_timeout={timeout}")
