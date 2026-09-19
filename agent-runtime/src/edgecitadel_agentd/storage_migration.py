"""Offline, identity-preserving move of a closed schema-29 trace database."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

from .storage_sqlite import configure_scratch
from .restore import RESTORE_BARRIER, _barrier
from .storage_layout import StorageLayout
from .storage_pair import verify_pair, verify_references
from .store import StoreError
from .trace_quota import verify_trace_quota
from .writer_lock import exclusive_writer

_MIGRATION_STATE = "quota_layout_migration"


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _regular(path: Path) -> None:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or info.st_uid != os.geteuid()
        or info.st_mode & 0o077
    ):
        raise StoreError("migration requires private, singly linked owned files")


def _digest(path: Path) -> str:
    _regular(path)
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def _inventory(trace: Path, layout: StorageLayout) -> dict[str, str]:
    return {
        "trace": _digest(trace),
        "tasks": _digest(layout.task_path),
        "key": _digest(layout.key_path),
    }


def _read_marker(path: Path) -> dict[str, str]:
    _regular(path)
    try:
        with path.open() as source:
            value = json.loads(source.read(4097))
        hashes = value["sha256"]
        if (
            value["version"] != 1
            or value["state"] != _MIGRATION_STATE
            or set(hashes) != {"trace", "tasks", "key"}
            or any(
                not isinstance(digest, str)
                or len(digest) != 64
                or any(char not in "0123456789abcdef" for char in digest)
                for digest in hashes.values()
            )
        ):
            raise ValueError
        return hashes
    except (KeyError, TypeError, ValueError) as error:
        raise StoreError(
            "migration barrier is invalid or belongs to another operation"
        ) from error


def _verify_database(db: sqlite3.Connection) -> None:
    verify_pair(db)
    for schema in ("main", "task_state"):
        if db.execute(f"PRAGMA {schema}.journal_mode").fetchone()[0] != "delete":
            raise StoreError("migration requires a closed rollback-journal pair")
        if db.execute(f"PRAGMA {schema}.integrity_check").fetchall() != [("ok",)]:
            raise StoreError("migration database integrity check failed")
        if db.execute(f"PRAGMA {schema}.foreign_key_check").fetchone():
            raise StoreError("migration database foreign key check failed")
    verify_references(db)


def migrate_storage(state_dir: Path) -> dict[str, str]:
    """Move only trace main; retain exact task/key bytes and all source identities.

    Run as the dedicated service UID with agentd stopped and the target trace
    filesystem already provisioned. Only a clean schema-29 DELETE pair is accepted.
    The durable barrier fences old and new daemons before any copy. A retry with
    the same unchanged inputs replaces a partial copy or finishes an interrupted
    handoff. Malformed barriers/changed inputs require operator investigation.
    No service is stopped, quota changed, UID changed, or source epoch rotated.
    """
    directory = state_dir.resolve(strict=True)
    layout = StorageLayout(directory)
    old = directory / "agentd.sqlite3"
    marker = directory / RESTORE_BARRIER
    with exclusive_writer(directory):
        # The ordinary layout verifier intentionally refuses the old source path.
        verify_trace_quota(layout.trace_directory, directory)
        with exclusive_writer(layout.trace_directory):
            expected = (
                _read_marker(marker) if marker.exists() or marker.is_symlink() else None
            )
            allowed = {"writer.lock"}
            if expected is not None:
                allowed.add(layout.trace_path.name)
            if any(
                path.name not in allowed for path in layout.trace_directory.iterdir()
            ):
                raise StoreError("migration trace destination contains unrelated state")
            source = old if old.exists() or old.is_symlink() else layout.trace_path
            if source == layout.trace_path and expected is None:
                raise StoreError(
                    "migration requires an existing source or its recovery barrier"
                )
            for path in (source, layout.task_path, layout.key_path):
                _regular(path)
            for path in (source, layout.task_path):
                # Includes rollback/WAL sidecars and super-journals. Migration
                # must not strand old trace allocations outside the quota volume.
                if any(path.parent.glob(path.name + "-*")):
                    raise StoreError(
                        "migration requires offline SQLite recovery before copying"
                    )
            with closing(
                sqlite3.connect(source.as_uri() + "?mode=rw", uri=True, timeout=0)
            ) as db:
                configure_scratch(db)
                db.execute(
                    "ATTACH DATABASE ? AS task_state",
                    (layout.task_path.as_uri() + "?mode=rw",),
                )
                db.execute("BEGIN EXCLUSIVE")
                _verify_database(db)
                actual = _inventory(source, layout)
                if expected is not None and actual != expected:
                    raise StoreError(
                        "migration input changed while fenced; refusing handoff"
                    )
                if expected is None:
                    _barrier(
                        directory,
                        {"version": 1, "state": _MIGRATION_STATE, "sha256": actual},
                    )
                if source == old:
                    if layout.trace_path.exists() or layout.trace_path.is_symlink():
                        _regular(layout.trace_path)
                    descriptor = os.open(
                        layout.trace_path,
                        os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW,
                        0o600,
                    )
                    with (
                        os.fdopen(descriptor, "wb") as target,
                        old.open("rb") as origin,
                    ):
                        shutil.copyfileobj(origin, target, length=1024 * 1024)
                        target.flush()
                        os.fsync(target.fileno())
                    if _inventory(layout.trace_path, layout) != actual:
                        raise StoreError("migration copy verification failed")
                    # Persist the new file and its directory entry before retiring
                    # the only previous authority. Both may exist only while fenced.
                    _sync_directory(layout.trace_directory)
                    old.unlink()
                    _sync_directory(directory)
                # A resumed handoff rechecks the full pair before clearing its fence.
                marker.unlink()
                _sync_directory(directory)
                db.rollback()
                return actual


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    arguments = parser.parse_args()
    migrate_storage(arguments.state_dir)
    print("Trace layout migration complete; source identity and task state preserved.")


if __name__ == "__main__":
    main()
