"""Crash-safe same-file shadow rebuild and bounded retired-generation cleanup.

Raw ingestion remains authoritative and is never copied, renamed or deleted.
Only two derived generations may exist, so repeated failed builds cannot silently
accumulate more copies. This is a lifecycle bound, not a qualified byte quota.
"""

from __future__ import annotations

import re
import sqlite3
from uuid import uuid4

from . import trace_projection_store as projection
from . import trace_projection_history as history
from .trace_projection_tables import ProjectionTables, select_tables

CATALOG = """CREATE TABLE IF NOT EXISTS trace_projection_generations (
    generation TEXT PRIMARY KEY, namespace TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('active','building','retired')),
    base_generation TEXT, cleanup_started INTEGER NOT NULL DEFAULT 0
)"""


def initialize_catalog(db: sqlite3.Connection) -> None:
    """Called inside initialization's writer transaction, including first install."""
    if not db.in_transaction:
        raise ValueError("projection_transaction_required")
    db.execute(CATALOG)
    for status in ("active", "building"):
        db.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS trace_projection_one_{status} "
            f"ON trace_projection_generations(status) WHERE status='{status}'"
        )
    if db.execute("SELECT 1 FROM trace_projection_generations").fetchone():
        return
    # An existing disabled prototype may need a rebuild, but its cursor must not
    # be reused as if it had already projected the new version.
    exists = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_projection_state'"
    ).fetchone()
    if exists:
        row = db.execute(
            "SELECT generation FROM trace_projection_state WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise ValueError("projection_state_unavailable")
        generation = row[0]
    else:
        generation = str(uuid4())
        projection.create_tables(ProjectionTables(db, ""), generation)
    db.execute(
        "INSERT INTO trace_projection_generations(generation,namespace,status,base_generation) VALUES(?,?,'active',NULL)",
        (generation, ""),
    )


def begin(db: sqlite3.Connection) -> projection.ProjectionState:
    """Create a fresh empty candidate without changing the active reader view."""
    projection._idle(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        initialize_catalog(db)
        if db.execute(
            "SELECT 1 FROM trace_projection_generations WHERE status!='active'"
        ).fetchone():
            raise ValueError("projection_cleanup_or_resume_required")
        active = db.execute(
            "SELECT generation FROM trace_projection_generations WHERE status='active'"
        ).fetchone()
        if active is None:
            raise ValueError("projection_generation_unavailable")
        generation = str(uuid4())
        namespace = "g_" + generation.replace("-", "") + "_"
        state = projection.create_tables(ProjectionTables(db, namespace), generation)
        db.execute(
            "INSERT INTO trace_projection_generations(generation,namespace,status,base_generation) VALUES(?,?,'building',?)",
            (generation, namespace, active[0]),
        )
        return state


def activate(db: sqlite3.Connection, *, generation: str) -> projection.ProjectionState:
    """Switch once caught up, checking epoch/version/parent under one writer lock.

    A concurrent ingestion commit causes a catch-up refusal, never a switch to a
    silently lagging candidate. Existing read transactions keep their old view.
    """
    projection._idle(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        tables = select_tables(db, generation)
        state = projection._state(tables)
        if state.generation != generation:
            raise ValueError("projection_generation_mismatch")
        high = db.execute(
            "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        if state.ingest_cursor != high:
            raise ValueError("projection_rebuild_not_caught_up")
        base = db.execute(
            "SELECT base_generation FROM trace_projection_generations WHERE generation=?",
            (generation,),
        ).fetchone()[0]
        active = db.execute(
            "SELECT generation FROM trace_projection_generations WHERE status='active'"
        ).fetchone()
        if active is None or active[0] != base:
            raise ValueError("projection_rebuild_base_changed")
        db.execute(
            "UPDATE trace_projection_generations SET status='retired' WHERE generation=?",
            (base,),
        )
        db.execute(
            "UPDATE trace_projection_generations SET status='active',base_generation=NULL WHERE generation=?",
            (generation,),
        )
        return state


def cancel(db: sqlite3.Connection, *, generation: str) -> None:
    """Retire a failed candidate for explicit incremental cleanup."""
    projection._idle(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        changed = db.execute(
            "UPDATE trace_projection_generations SET status='retired' WHERE generation=? AND status='building'",
            (generation,),
        ).rowcount
        if changed != 1:
            raise ValueError("projection_build_unavailable")


def cleanup_batch(db: sqlite3.Connection, *, generation: str, limit: int = 256) -> dict:
    """Delete at most limit rows from one retired table; drop only empty tables.

    Cleanup never VACUUMs or claims to reclaim filesystem quota. SQLite freelist
    reuse and physical maintenance remain separate qualified storage concerns.
    """
    projection._idle(db)
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid_projection_cleanup_batch")
    with db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT namespace,cleanup_started FROM trace_projection_generations WHERE generation=? AND status='retired'",
            (generation,),
        ).fetchone()
        if row is None:
            raise ValueError("projection_retired_generation_required")
        db.execute(
            "UPDATE trace_projection_generations SET cleanup_started=1 WHERE generation=?",
            (generation,),
        )
        tables = ProjectionTables(db, row[0])
        if not row[1]:
            history.disable_capture(tables)
        names = [
            re.match(
                r"CREATE TABLE IF NOT EXISTS \{(trace_[a-z_]+)\}", statement
            ).group(1)
            for statement in projection.SCHEMA
            if statement.startswith("CREATE TABLE")
        ]
        for name in names:
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (row[0] + name,),
            ).fetchone()
            if not exists:
                continue
            quoted = tables.identifier(name)
            deleted = db.execute(
                f"DELETE FROM {quoted} WHERE rowid IN (SELECT rowid FROM {quoted} LIMIT ?)",
                (limit,),
            ).rowcount
            if db.execute(f"SELECT 1 FROM {quoted} LIMIT 1").fetchone() is None:
                db.execute(f"DROP TABLE {quoted}")
            return {"deleted_rows": deleted, "complete": False}
        db.execute(
            "DELETE FROM trace_projection_generations WHERE generation=?", (generation,)
        )
        return {"deleted_rows": 0, "complete": True}


def prepare_rollback(
    db: sqlite3.Connection, *, generation: str
) -> projection.ProjectionState:
    """Catch up a compatible retained predecessor under a fresh cursor generation.

    Cleanup irreversibly ends rollback eligibility. No task command, raw event or
    collector checkpoint is rolled back; activation still requires caught-up data.
    """
    projection._idle(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT namespace FROM trace_projection_generations WHERE generation=? "
            "AND status='retired' AND cleanup_started=0 AND base_generation IS NULL",
            (generation,),
        ).fetchone()
        if row is None:
            raise ValueError("projection_rollback_unavailable")
        tables = ProjectionTables(db, row[0])
        projection._state(tables)
        active = db.execute(
            "SELECT generation FROM trace_projection_generations WHERE status='active'"
        ).fetchone()
        if active is None:
            raise ValueError("projection_generation_unavailable")
        fresh = str(uuid4())
        tables.execute(
            "UPDATE {trace_projection_state} SET generation=? WHERE singleton=1",
            (fresh,),
        )
        db.execute(
            "UPDATE trace_projection_generations SET generation=?,status='building',base_generation=? WHERE generation=?",
            (fresh, active[0], generation),
        )
        return projection._state(tables)
