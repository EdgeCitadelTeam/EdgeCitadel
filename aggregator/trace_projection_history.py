"""Cursor-bound graph/coverage row versions and retained-base compaction.

SQLite capture triggers share the projection transaction and clock. Historical
reads use connection-local views of those versions, reusing the current graph
and coverage queries without replaying future raw inputs or copying the fleet.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING

from .trace_projection_tables import ProjectionTables, select_tables
from .trace_retention import RETENTION_MS, SCAN_ROWS

if TYPE_CHECKING:
    from .trace_projection_store import ProjectionState

READ_TABLES = (
    "trace_projection_runs",
    "trace_projection_run_events",
    "trace_task_outcomes",
    "trace_task_perspectives",
    "trace_entity_observations",
    "trace_projected_tasks",
    "trace_projected_entities",
    "trace_relationship_claims",
    "trace_projection_scope_progress",
    "trace_projection_intervals",
    "trace_projection_run_scopes",
    "trace_projection_run_coverage",
    "trace_projection_source_coverage",
    "trace_projection_run_losses",
)

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS {trace_projection_history_state} (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        floor_cursor INTEGER NOT NULL, clock_cursor INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_history_cursors} (
        cursor INTEGER PRIMARY KEY, ingest_seq INTEGER NOT NULL, received_at_ms INTEGER NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_history_rows} (
        table_name TEXT NOT NULL, row_key TEXT NOT NULL, cursor INTEGER NOT NULL,
        deleted INTEGER NOT NULL, row_json TEXT NOT NULL,
        PRIMARY KEY(table_name,row_key,cursor)
    )""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_history_cursor}
        ON {trace_projection_history_rows}(cursor)""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_history_trace}
        ON {trace_projection_history_rows}(table_name,json_extract(row_json,'$.trace_id'),cursor)""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_history_events}
        ON {trace_projection_history_rows}(json_extract(row_json,'$.trace_id'),
        json_extract(row_json,'$.ingest_seq'),cursor)
        WHERE table_name='trace_projection_run_events'""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_history_creation}
        ON {trace_projection_history_rows}(json_extract(row_json,'$.created_cursor') DESC,
        json_extract(row_json,'$.trace_id') DESC,cursor)
        WHERE table_name='trace_projection_runs'""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_history_source}
        ON {trace_projection_history_rows}(table_name,json_extract(row_json,'$.node_id'),
        json_extract(row_json,'$.source_epoch'),json_extract(row_json,'$.export_generation'),cursor)""",
)


def _columns(tables: ProjectionTables, name: str) -> list:
    return tables.connection.execute(
        f"PRAGMA main.table_info({tables.identifier(name)})"
    ).fetchall()


def install_capture(tables: ProjectionTables) -> None:
    tables.execute("INSERT INTO {trace_projection_history_state} VALUES(1,0,NULL)")
    tables.execute("INSERT INTO {trace_projection_history_cursors} VALUES(0,0,0)")
    for name in READ_TABLES:
        columns = _columns(tables, name)
        keys = [row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5]]
        if not columns or not keys:
            raise ValueError("projection_history_schema_unavailable")
        for operation in ("insert", "update", "delete"):
            alias = "OLD" if operation == "delete" else "NEW"
            key = "json_array(" + ",".join(f'{alias}."{key}"' for key in keys) + ")"
            body = (
                "json_object("
                + ",".join(f"'{row[1]}',{alias}.\"{row[1]}\"" for row in columns)
                + ")"
            )
            trigger = tables.identifier(name + "_history_" + operation)
            current = tables.identifier(name)
            history = tables.identifier("trace_projection_history_rows")
            clock = tables.identifier("trace_projection_history_state")
            tables.connection.execute(f"""CREATE TRIGGER {trigger} AFTER {operation.upper()} ON {current}
                BEGIN
                SELECT CASE WHEN (SELECT clock_cursor FROM {clock} WHERE singleton=1) IS NULL
                    THEN RAISE(ABORT,'projection_history_clock_required') END;
                INSERT INTO {history}(table_name,row_key,cursor,deleted,row_json)
                    VALUES('{name}',{key},(SELECT clock_cursor FROM {clock} WHERE singleton=1),{int(operation == "delete")},{body})
                    ON CONFLICT(table_name,row_key,cursor) DO UPDATE SET deleted=excluded.deleted,row_json=excluded.row_json;
                END""")


def disable_capture(tables: ProjectionTables) -> None:
    # Only retired generations may call this; cleanup does not publish history.
    for name in READ_TABLES:
        for operation in ("insert", "update", "delete"):
            tables.connection.execute(
                f"DROP TRIGGER IF EXISTS {tables.identifier(name + '_history_' + operation)}"
            )


def start_change(
    tables: ProjectionTables, cursor: int, ingest_seq: int, *, received_at_ms: int
) -> None:
    tables.execute(
        "UPDATE {trace_projection_history_state} SET clock_cursor=? WHERE singleton=1",
        (cursor,),
    )
    tables.execute(
        "INSERT INTO {trace_projection_history_cursors} VALUES(?,?,?)",
        (cursor, ingest_seq, received_at_ms),
    )


def finish_changes(tables: ProjectionTables) -> None:
    tables.execute(
        "UPDATE {trace_projection_history_state} SET clock_cursor=NULL WHERE singleton=1 AND clock_cursor IS NOT NULL"
    )


def floor(tables: ProjectionTables) -> int:
    return tables.execute(
        "SELECT floor_cursor FROM {trace_projection_history_state} WHERE singleton=1"
    ).fetchone()[0]


@contextmanager
def at_cursor(
    tables: ProjectionTables,
    state: ProjectionState,
    *,
    generation: str | None,
    cursor: int | None,
):
    if cursor is None:
        if generation is not None and generation != state.generation:
            raise ValueError("projection_generation_mismatch")
        yield state
        return
    if type(cursor) is not int or cursor < 0:
        raise ValueError("invalid_projection_cursor")
    if generation != state.generation:
        raise ValueError("projection_generation_mismatch")
    if cursor < floor(tables):
        raise ValueError("projection_cursor_expired")
    if cursor > state.change_cursor:
        raise ValueError("projection_cursor_ahead")
    position = tables.execute(
        "SELECT ingest_seq FROM {trace_projection_history_cursors} WHERE cursor=?",
        (cursor,),
    ).fetchone()
    if position is None:
        raise ValueError("projection_history_unavailable")
    history = tables.identifier("trace_projection_history_rows")
    created = []
    try:
        for name in READ_TABLES:
            columns = _columns(tables, name)
            projection = ",".join(
                f"json_extract(h.row_json,'$.{row[1]}') AS \"{row[1]}\""
                for row in columns
            )
            view = tables.identifier(name)
            tables.connection.execute(f"""CREATE TEMP VIEW {view} AS
                SELECT {projection} FROM main.{history} h
                WHERE h.table_name='{name}' AND h.cursor<={cursor} AND h.deleted=0
                AND NOT EXISTS(SELECT 1 FROM main.{history} n WHERE n.table_name=h.table_name
                    AND n.row_key=h.row_key AND n.cursor>h.cursor AND n.cursor<={cursor})""")
            created.append(view)
        yield replace(state, change_cursor=cursor, ingest_cursor=position[0])
    finally:
        for view in reversed(created):
            tables.connection.execute(f"DROP VIEW temp.{view}")


# Preserve the latest row at/before the floor plus all later versions. A base
# tombstone may disappear only after every older version of its key is gone.
_OBSOLETE = """h.cursor<=? AND (
    EXISTS(SELECT 1 FROM {trace_projection_history_rows} n
        WHERE n.table_name=h.table_name AND n.row_key=h.row_key AND n.cursor>h.cursor AND n.cursor<=?)
    OR (h.deleted=1 AND NOT EXISTS(SELECT 1 FROM {trace_projection_history_rows} p
        WHERE p.table_name=h.table_name AND p.row_key=h.row_key AND p.cursor<h.cursor)))"""


def compact_batch(
    db: sqlite3.Connection, *, generation: str, through_cursor: int, limit: int = 256
) -> dict:
    """Advance the retained floor atomically; incrementally prune superseded data.

    Every retained cursor remains readable during cleanup. The floor is advanced
    before deleting anything, so an older client gets an expiry error, not a
    partially reconstructed graph. This bounds deleted rows, not physical bytes.
    """
    from .trace_projection_store import _idle, _state

    _idle(db)
    if (
        type(through_cursor) is not int
        or through_cursor < 0
        or type(limit) is not int
        or not 1 <= limit <= 1000
    ):
        raise ValueError("invalid_projection_compaction")
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db)
        state = _state(tables)
        if generation != state.generation:
            raise ValueError("projection_generation_mismatch")
        return _compact(tables, state, through_cursor=through_cursor, limit=limit)


def _compact(
    tables: ProjectionTables, state: ProjectionState, *, through_cursor: int, limit: int
) -> dict:
    if through_cursor < floor(tables) or through_cursor > state.change_cursor:
        raise ValueError("invalid_projection_compaction_cursor")
    tables.execute(
        "UPDATE {trace_projection_history_state} SET floor_cursor=? WHERE singleton=1 AND floor_cursor<>?",
        (through_cursor, through_cursor),
    )
    deleted = tables.execute(
        "DELETE FROM {trace_projection_history_rows} WHERE rowid IN ("
        "SELECT h.rowid FROM {trace_projection_history_rows} h WHERE "
        + _OBSOLETE
        + " LIMIT ?)",
        (through_cursor, through_cursor, limit),
    ).rowcount
    for table, boundary in (
        ("trace_projection_history_cursors", "cursor<?"),
        ("trace_projection_changes", "cursor<=?"),
    ):
        remaining = limit - deleted
        if not remaining:
            break
        deleted += tables.execute(
            "DELETE FROM {"
            + table
            + "} WHERE rowid IN (SELECT rowid FROM {"
            + table
            + "} WHERE "
            + boundary
            + " LIMIT ?)",
            (through_cursor, remaining),
        ).rowcount
    pending = tables.execute(
        "SELECT 1 FROM {trace_projection_history_rows} h WHERE "
        + _OBSOLETE
        + " LIMIT 1",
        (through_cursor, through_cursor),
    ).fetchone()
    pending = (
        pending
        or tables.execute(
            "SELECT 1 FROM {trace_projection_history_cursors} WHERE cursor<? LIMIT 1",
            (through_cursor,),
        ).fetchone()
    )
    pending = (
        pending
        or tables.execute(
            "SELECT 1 FROM {trace_projection_changes} WHERE cursor<=? LIMIT 1",
            (through_cursor,),
        ).fetchone()
    )
    return {
        "floor_cursor": through_cursor,
        "deleted_rows": deleted,
        "complete": not bool(pending),
    }


def retained_range(db: sqlite3.Connection) -> dict:
    """Return the available playback boundary and its raw-observation watermark."""
    from .trace_projection_store import _idle, _state

    _idle(db)
    with db:
        db.execute("BEGIN")
        tables = select_tables(db)
        state = _state(tables)
        lower = floor(tables)
        position = tables.execute(
            "SELECT ingest_seq FROM {trace_projection_history_cursors} WHERE cursor=?",
            (lower,),
        ).fetchone()
        if position is None:
            raise ValueError("projection_history_unavailable")
        return {
            "state": state,
            "from_cursor": lower,
            "from_ingest_seq": position[0],
            "through_cursor": state.change_cursor,
        }


def expire_history_batch(
    db: sqlite3.Connection, *, now_ms: int, limit: int = 256
) -> dict:
    """Apply Core receipt-age policy to a bounded contiguous history prefix.

    Receipt clocks can move backwards. Stop at the first fresh cursor instead of
    advancing across it to an older timestamp later in the log. The retained base
    remains necessary for current state even when all input history is old.
    """
    from .trace_projection_store import _idle, _state

    _idle(db)
    if type(now_ms) is not int or not 0 <= now_ms <= 2**53 - 1:
        raise ValueError("invalid_retention_time")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid_projection_compaction")
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db)
        state = _state(tables)
        through = floor(tables)
        rows = tables.execute(
            "SELECT cursor,received_at_ms FROM {trace_projection_history_cursors} "
            "WHERE cursor>? ORDER BY cursor LIMIT ?",
            (through, SCAN_ROWS),
        ).fetchall()
        cutoff = now_ms - RETENTION_MS
        reached_fresh = False
        for cursor, received_at in rows:
            if received_at >= cutoff:
                reached_fresh = True
                break
            through = cursor
        result = _compact(tables, state, through_cursor=through, limit=limit)
        return {
            **result,
            "scanned_rows": len(rows),
            "eligible_prefix_complete": reached_fresh or len(rows) < SCAN_ROWS,
            "observed_at_ms": now_ms,
            "retention_ms": RETENTION_MS,
            "cutoff_ms": cutoff,
        }
