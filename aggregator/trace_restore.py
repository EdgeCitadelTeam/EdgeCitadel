"""Prepare a new Core database from a SQLite snapshot; never activate it in place."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from uuid import UUID, uuid4

from .trace_store import initialize


def prepare_restore(snapshot: Path, destination: Path) -> dict[str, str]:
    """Copy a consistent snapshot, rotate its epoch, then publish without overwrite.

    Destination must be a new, offline path. Activation requires stopping all Core
    database users; this function neither replaces a live database nor stops them.
    """
    snapshot = snapshot.resolve(strict=True)
    destination = destination.absolute()
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(destination)
    # A same-directory temporary file permits atomic no-replace publication.
    with tempfile.TemporaryDirectory(
        prefix=".core-restore-", dir=destination.parent
    ) as tmp:
        staging = Path(tmp) / "core.db"
        staging.touch(mode=0o600)
        source = sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)
        target = sqlite3.connect(staging)
        try:
            source.backup(target, pages=256)
            if target.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("core_restore_integrity_failed")
            rows = target.execute(
                "SELECT singleton,collector_epoch,ingest_seq FROM trace_collector"
            ).fetchall()
            if len(rows) != 1 or rows[0][0] != 1 or rows[0][2] < 0:
                raise ValueError("core_restore_invalid_collector")
            old_epoch = rows[0][1]
            UUID(old_epoch)
            # Upgrade accounting only in the private copy, preserving all evidence.
            initialize(target)
            new_epoch = str(uuid4())
            with target:
                target.execute(
                    "UPDATE trace_collector SET collector_epoch=? WHERE singleton=1",
                    (new_epoch,),
                )
            target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            target.execute("PRAGMA journal_mode=DELETE")
        finally:
            target.close()
            source.close()
        with staging.open("rb") as file:
            os.fsync(file.fileno())
        # link() is atomic and fails even for dangling destination symlinks.
        os.link(staging, destination)
        directory = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return {"previous_collector_epoch": old_epoch, "collector_epoch": new_epoch}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    print(json.dumps(prepare_restore(args.snapshot, args.destination), sort_keys=True))


if __name__ == "__main__":
    main()
