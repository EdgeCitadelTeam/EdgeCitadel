"""Crash-resumable offline conversion of legacy shared task/trace state."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

from .restore import RESTORE_BARRIER
from .storage_layout import StorageLayout
from .storage_pair import verify_pair, verify_references
from .storage_sqlite import configure_scratch
from .store import AgentdStore, StoreError

UPGRADE_STATE = "shared_storage_upgrade"


def _save(state: Path, record: dict) -> None:
    from .storage_migration import _sync_directory

    temporary = state / "storage-upgrade-marker.tmp"
    fd = os.open(
        temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(fd, "w") as target:
        json.dump(record, target, sort_keys=True)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, state / RESTORE_BARRIER)
    _sync_directory(state)


def _validate(trace: Path, tasks: Path) -> None:
    with closing(sqlite3.connect(trace.as_uri() + "?mode=rw", uri=True)) as db:
        configure_scratch(db)
        db.execute("ATTACH DATABASE ? AS task_state", (tasks.as_uri() + "?mode=rw",))
        verify_pair(db)
        verify_references(db)
        for schema in ("main", "task_state"):
            if (
                db.execute(f"PRAGMA {schema}.integrity_check").fetchall() != [("ok",)]
                or db.execute(f"PRAGMA {schema}.foreign_key_check").fetchone()
                or db.execute(f"PRAGMA {schema}.journal_mode").fetchone()[0] != "delete"
            ):
                raise StoreError("upgraded database pair failed validation")


def upgrade_shared(layout: StorageLayout) -> dict:
    """Caller holds both storage locks and has verified native storage.

    The original database remains the source until a fully validated pair has
    durable hashes. Temporary copies are transaction working state, retired only
    after cutover. Every retry either rebuilds staging or verifies the recorded
    output bytes; no source identity or encrypted payload is regenerated.
    """
    from .storage_migration import _digest, _regular, _sync_directory

    state = layout.state_directory
    source, marker = state / "agentd.sqlite3", state / RESTORE_BARRIER
    staging = state / "storage-upgrade"
    staged_trace, staged_tasks = (
        staging / "agentd.sqlite3",
        staging / "agentd-tasks.sqlite3",
    )
    if marker.exists() or marker.is_symlink():
        _regular(marker)
        try:
            record = json.loads(marker.read_text())
            if (
                record["version"] != 1
                or record["state"] != UPGRADE_STATE
                or record["phase"] not in {"staging", "ready"}
            ):
                raise ValueError
        except (ValueError, KeyError, TypeError) as error:
            raise StoreError("invalid shared migration barrier") from error
    else:
        _regular(source)
        _regular(layout.key_path)
        destinations = (staging, layout.task_path, layout.trace_path)
        if any(path.exists() or path.is_symlink() for path in destinations):
            raise StoreError("shared migration destinations contain unrelated state")
        allowed = {
            "writer.lock",
            ".fseventsd",
            ".Spotlight-V100",
            ".Trashes",
            ".metadata_never_index",
        }
        if any(path.name not in allowed for path in layout.trace_directory.iterdir()):
            raise StoreError(
                "shared migration trace destination contains unrelated state"
            )
        record = {
            "version": 1,
            "state": UPGRADE_STATE,
            "phase": "staging",
            "key": _digest(layout.key_path),
        }
        _save(state, record)
    if _digest(layout.key_path) != record["key"]:
        raise StoreError("migration encryption key changed")
    if record["phase"] == "staging":
        _regular(source)
        # SQLite performs WAL/hot-journal recovery while the service is fenced.
        # A competing connection prevents the journal-mode transition.
        with closing(
            sqlite3.connect(source.as_uri() + "?mode=rw", uri=True, timeout=0)
        ) as db:
            configure_scratch(db)
            if db.execute("PRAGMA user_version").fetchone()[0] != 6:
                raise StoreError("shared migration requires schema 6")
            if db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                raise StoreError("shared source cannot leave WAL mode")
            db.execute("BEGIN EXCLUSIVE")
            if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise StoreError("shared source integrity check failed")
            source_hash = _digest(source)
            if "source" in record and record["source"] != source_hash:
                raise StoreError("shared migration source changed")
            record["source"] = source_hash
            _save(state, record)
            if staging.exists():
                if staging.is_symlink() or staging.stat().st_uid != os.geteuid():
                    raise StoreError("migration staging ownership changed")
                for path in staging.iterdir():
                    if not path.name.startswith(
                        ("agentd.sqlite3", "agentd-tasks.sqlite3")
                    ):
                        raise StoreError("unrelated migration staging contents")
                    _regular(path)
                    path.unlink()
            else:
                staging.mkdir(mode=0o700)
            # Copy only after WAL recovery and while holding SQLite ownership.
            fd = os.open(staged_trace, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "wb") as target, source.open("rb") as origin:
                shutil.copyfileobj(origin, target)
                target.flush()
                os.fsync(target.fileno())
            db.rollback()
        store = AgentdStore(
            staged_trace, task_path=staged_tasks, payload_key_path=layout.key_path
        )
        store.close()
        _validate(staged_trace, staged_tasks)
        for path in (staged_trace, staged_tasks):
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        _sync_directory(staging)
        record.update(
            phase="ready", trace=_digest(staged_trace), tasks=_digest(staged_tasks)
        )
        _save(state, record)
    if source.exists() and _digest(source) != record["source"]:
        raise StoreError("shared source changed before activation")
    tasks = staged_tasks if staged_tasks.exists() else layout.task_path
    if _digest(tasks) != record["tasks"]:
        raise StoreError("upgraded task database changed")
    if staged_trace.exists():
        if _digest(staged_trace) != record["trace"]:
            raise StoreError("upgraded trace database changed")
        if layout.trace_path.exists() or layout.trace_path.is_symlink():
            _regular(layout.trace_path)
        fd = os.open(
            layout.trace_path,
            os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW,
            0o600,
        )
        with os.fdopen(fd, "wb") as target, staged_trace.open("rb") as origin:
            shutil.copyfileobj(origin, target)
            target.flush()
            os.fsync(target.fileno())
        _sync_directory(layout.trace_directory)
    if _digest(layout.trace_path) != record["trace"]:
        raise StoreError("upgraded trace copy changed")
    _validate(layout.trace_path, tasks)
    if tasks == staged_tasks:
        if layout.task_path.exists() or layout.task_path.is_symlink():
            raise StoreError("unrelated task destination appeared")
        os.replace(tasks, layout.task_path)
        _sync_directory(state)
        _sync_directory(staging)
    _validate(layout.trace_path, layout.task_path)
    # Retire authority only after both durable outputs pass pair/reference checks.
    if source.exists():
        source.unlink()
        _sync_directory(state)
    if staged_trace.exists():
        staged_trace.unlink()
        _sync_directory(staging)
    if staging.exists():
        staging.rmdir()
        _sync_directory(state)
    marker.unlink()
    _sync_directory(state)
    return {name: record[name] for name in ("trace", "tasks", "key")}
