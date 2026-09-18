"""Explicit offline file reclamation under daemon and SQLite ownership locks."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from .restore import require_startable
from .trace_capacity import physical_storage
from .writer_lock import exclusive_writer


def compact_database(state_dir: Path) -> dict[str, dict[str, int]]:
    """Reclaim free pages without migrating, rotating or deleting application rows.

    The operator must stop agentd first. No daemon is stopped by this API. Both
    daemon ownership and SQLite exclusive locking are required; active readers
    cause immediate failure. VACUUM uses SQLite's transactional recovery and needs
    temporary disk space; an error is propagated, never reported as reclamation.
    """
    directory = state_dir.resolve(strict=True)
    with exclusive_writer(directory):
        require_startable(directory)
        uri = (directory / "agentd.sqlite3").as_uri() + "?mode=rw"
        with closing(sqlite3.connect(uri, uri=True, timeout=0)) as db:
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA locking_mode=EXCLUSIVE")
            db.execute("BEGIN EXCLUSIVE")
            db.commit()
            # Exclusive locking mode retains ownership through VACUUM and close.
            _verify(db)
            before = physical_storage(db)
            db.execute("VACUUM")
            checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint[0]:
                raise sqlite3.OperationalError("offline checkpoint is busy")
            _verify(db)
            after = physical_storage(db)
            return {"before": before, "after": after}


def _verify(db: sqlite3.Connection) -> None:
    if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise sqlite3.DatabaseError("database integrity check failed")
    if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise sqlite3.DatabaseError("database foreign key check failed")
