"""Retire inactive Core graph views without changing execution or raw evidence.

One trace retires at a time. Readers see its tombstone immediately; current
projection pauses until bounded cleanup finishes, preventing late observations
from mixing with partially deleted reducer state. Raw ingestion remains free to
commit. Historical rows are captured under maintenance change cursors.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from . import trace_projection_history as history
from .trace_projection_tables import ProjectionTables, select_tables
from .trace_retention import RETENTION_MS

if TYPE_CHECKING:
    from .trace_projection_store import ProjectionState

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS {trace_projection_runs} (
        trace_id TEXT PRIMARY KEY, last_received_at_ms INTEGER NOT NULL,
        test_only INTEGER NOT NULL, expired_cursor INTEGER,
        cleanup_table INTEGER NOT NULL DEFAULT 0
    )""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_run_age}
        ON {trace_projection_runs}(test_only,last_received_at_ms,trace_id)
        WHERE expired_cursor IS NULL""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_retiring}
        ON {trace_projection_runs}(expired_cursor) WHERE expired_cursor IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS {trace_projection_run_event_scope}
        ON {trace_projection_run_events}(trace_id)""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_retention_state} (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),cutoff_ms INTEGER NOT NULL
    )""",
)

TRACE_TABLES = (
    "trace_projected_tasks",
    "trace_task_perspectives",
    "trace_task_outcomes",
    "trace_entity_observations",
    "trace_projected_entities",
    "trace_relationship_claims",
    "trace_projection_run_events",
    "trace_projection_run_scopes",
    "trace_projection_run_coverage",
    "trace_projection_run_losses",
)


def initialize(tables: ProjectionTables) -> None:
    tables.execute("INSERT INTO {trace_projection_retention_state} VALUES(1,0)")


def touch(tables: ProjectionTables, event: dict, received_at_ms: int) -> None:
    if event["trace_id"] is None:
        return
    tables.execute(
        "INSERT INTO {trace_projection_runs}(trace_id,last_received_at_ms,test_only) VALUES(?,?,?) "
        "ON CONFLICT(trace_id) DO UPDATE SET last_received_at_ms=max(last_received_at_ms,excluded.last_received_at_ms),"
        "test_only=min(test_only,excluded.test_only)",
        (event["trace_id"], received_at_ms, int(event.get("test_run_id") is not None)),
    )


def pending(tables: ProjectionTables) -> tuple | None:
    return tables.execute(
        "SELECT trace_id,cleanup_table FROM {trace_projection_runs} "
        "WHERE expired_cursor IS NOT NULL ORDER BY expired_cursor LIMIT 1"
    ).fetchone()


def require_live(tables: ProjectionTables, trace_id: str) -> None:
    row = tables.execute(
        "SELECT expired_cursor FROM {trace_projection_runs} WHERE trace_id=?",
        (trace_id,),
    ).fetchone()
    if row is not None and row[0] is not None:
        raise ValueError("projection_trace_expired")


def _eligible(tables: ProjectionTables, cutoff: int) -> str | None:
    # Keep test-first cleanup indexed even when many recent test traces exist.
    for test_only in (1, 0):
        row = tables.execute(
            "SELECT trace_id FROM {trace_projection_runs} WHERE expired_cursor IS NULL "
            "AND test_only=? AND last_received_at_ms<? ORDER BY last_received_at_ms,trace_id LIMIT 1",
            (test_only, cutoff),
        ).fetchone()
        if row:
            return row[0]
    return None


def policy_cutoff(tables: ProjectionTables) -> int:
    # Versions before graph expiry had no retirement policy to preserve. Their
    # incompatible state is replayed, never read as a current projection.
    exists = tables.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (tables.namespace + "trace_projection_retention_state",),
    ).fetchone()
    if not exists:
        version = tables.execute(
            "SELECT version FROM {trace_projection_state} WHERE singleton=1"
        ).fetchone()
        if version and 1 <= version[0] < 6:
            return 0
        raise ValueError("projection_retention_state_unavailable")
    row = tables.execute(
        "SELECT cutoff_ms FROM {trace_projection_retention_state} WHERE singleton=1"
    ).fetchone()
    if row is None:
        raise ValueError("projection_retention_state_unavailable")
    return row[0]


def require_activation_ready(tables: ProjectionTables, predecessor_cutoff: int) -> None:
    cutoff = policy_cutoff(tables)
    if (
        cutoff < predecessor_cutoff
        or pending(tables)
        or _eligible(tables, max(cutoff, predecessor_cutoff))
    ):
        raise ValueError("projection_retention_catchup_required")


def _finish(
    tables: ProjectionTables,
    state: ProjectionState,
    trace_id: str,
    kind: str,
    body: dict,
) -> int:
    cursor = state.change_cursor + 1
    tables.execute(
        "INSERT INTO {trace_projection_changes} VALUES(?,NULL,?,?,?)",
        (
            cursor,
            trace_id,
            kind,
            json.dumps(body, sort_keys=True, separators=(",", ":")),
        ),
    )
    tables.execute(
        "UPDATE {trace_projection_state} SET change_cursor=? WHERE singleton=1",
        (cursor,),
    )
    history.finish_changes(tables)
    return cursor


def expire_one(
    db: sqlite3.Connection, *, now_ms: int, build_generation: str | None = None
) -> dict:
    """Atomically hide one age-eligible trace after the projection catches up."""
    from .trace_projection_store import _idle, _state

    _idle(db)
    if type(now_ms) is not int or not 0 <= now_ms <= 2**53 - 1:
        raise ValueError("invalid_retention_time")
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db, build_generation)
        state = _state(tables)
        if pending(tables):
            return {"status": "cleanup_pending"}
        high = tables.execute(
            "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        if state.ingest_cursor != high:
            return {"status": "catching_up"}
        cutoff = max(policy_cutoff(tables), now_ms - RETENTION_MS, 0)
        tables.execute(
            "UPDATE {trace_projection_retention_state} SET cutoff_ms=? WHERE singleton=1 AND cutoff_ms<>?",
            (cutoff, cutoff),
        )
        trace_id = _eligible(tables, cutoff)
        if trace_id is None:
            return {"status": "idle"}
        cursor = state.change_cursor + 1
        history.start_change(tables, cursor, state.ingest_cursor, received_at_ms=now_ms)
        tables.execute(
            "UPDATE {trace_projection_runs} SET expired_cursor=? WHERE trace_id=?",
            (cursor, trace_id),
        )
        _finish(
            tables,
            state,
            trace_id,
            "trace_expired",
            {"trace_id": trace_id, "reason": "retention_expired"},
        )
        return {"status": "retired", "trace_id": trace_id, "cursor": cursor}


def cleanup_batch(
    db: sqlite3.Connection,
    *,
    now_ms: int,
    limit: int = 256,
    build_generation: str | None = None,
) -> dict:
    """Delete at most limit current rows; retain historical images and raw inputs."""
    from .trace_projection_store import _idle, _state

    _idle(db)
    if type(now_ms) is not int or not 0 <= now_ms <= 2**53 - 1:
        raise ValueError("invalid_retention_time")
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid_projection_cleanup_batch")
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db, build_generation)
        state = _state(tables)
        retiring = pending(tables)
        if retiring is None:
            return {"deleted_rows": 0, "complete": True}
        trace_id, index = retiring
        history.start_change(
            tables, state.change_cursor + 1, state.ingest_cursor, received_at_ms=now_ms
        )
        for position in range(index, len(TRACE_TABLES)):
            name = TRACE_TABLES[position]
            deleted = tables.execute(
                "DELETE FROM {" + name + "} WHERE rowid IN ("
                "SELECT rowid FROM {" + name + "} WHERE trace_id=? LIMIT ?)",
                (trace_id, limit),
            ).rowcount
            if not deleted:
                continue
            remaining = tables.execute(
                "SELECT 1 FROM {" + name + "} WHERE trace_id=? LIMIT 1", (trace_id,)
            ).fetchone()
            tables.execute(
                "UPDATE {trace_projection_runs} SET cleanup_table=? WHERE trace_id=?",
                (position if remaining else position + 1, trace_id),
            )
            _finish(
                tables,
                state,
                trace_id,
                "trace_cleanup",
                {"trace_id": trace_id, "cleanup_complete": False},
            )
            return {"deleted_rows": deleted, "complete": False}
        tables.execute(
            "DELETE FROM {trace_projection_runs} WHERE trace_id=?", (trace_id,)
        )
        _finish(
            tables,
            state,
            trace_id,
            "trace_cleanup",
            {"trace_id": trace_id, "cleanup_complete": True},
        )
        return {"deleted_rows": 1, "complete": True}
