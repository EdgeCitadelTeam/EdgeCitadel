"""Explicit offline file reclamation under daemon and SQLite ownership locks."""

from __future__ import annotations

import sqlite3
from contextlib import closing, ExitStack
from pathlib import Path

from .storage_sqlite import configure_scratch
from .restore import require_startable
from .storage_pair import (
    attach_tasks,
    verify_pair,
    verify_references,
)
from .trace_capacity import physical_storage
from .storage_layout import StorageLayout
from .writer_lock import exclusive_writer


def compact_database(
    state_dir: Path, *, layout: StorageLayout | None = None
) -> dict[str, dict[str, int]]:
    """Reclaim free pages without migrating, rotating or deleting application rows.

    The operator must stop agentd first. No daemon is stopped by this API. Both
    daemon ownership and SQLite exclusive locking are required; active readers
    cause immediate failure. VACUUM uses SQLite's transactional recovery and needs
    memory for the rebuilt database and disk space for rollback journals; an
    error is propagated, never reported as reclamation.
    """
    directory = state_dir.resolve(strict=True)
    layout = layout or StorageLayout(directory)
    if layout.state_directory.resolve() != directory:
        raise ValueError("maintenance layout does not match its state directory")
    with ExitStack() as ownership:
        ownership.enter_context(exclusive_writer(directory))
        require_startable(directory)
        layout.verify()
        if layout.trace_directory.resolve() != directory:
            ownership.enter_context(exclusive_writer(layout.trace_directory))
        uri = layout.trace_path.as_uri() + "?mode=rw"
        with closing(sqlite3.connect(uri, uri=True, timeout=0)) as db:
            configure_scratch(db)
            attach_tasks(db, layout.task_path, existing=True)
            verify_pair(db)
            db.execute("PRAGMA synchronous=EXTRA")
            db.execute("PRAGMA locking_mode=EXCLUSIVE")
            db.execute("BEGIN EXCLUSIVE")
            db.commit()
            # Exclusive locking mode retains ownership through VACUUM and close.
            _verify(db)
            before = physical_storage(db)
            db.execute("VACUUM main")
            db.execute("VACUUM task_state")
            checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint[0]:
                raise sqlite3.OperationalError("offline checkpoint is busy")
            _verify(db)
            after = physical_storage(db)
            return {"before": before, "after": after}


def _verify(db: sqlite3.Connection) -> None:
    for schema in ("main", "task_state"):
        if db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] != "ok":
            raise sqlite3.DatabaseError("database integrity check failed")
        if db.execute(f"PRAGMA {schema}.foreign_key_check").fetchone() is not None:
            raise sqlite3.DatabaseError("database foreign key check failed")
    verify_references(db)
