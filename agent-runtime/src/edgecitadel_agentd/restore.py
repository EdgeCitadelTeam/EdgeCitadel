"""Offline restore staging. Staged state stays closed pending reconciliation."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import time
from contextlib import ExitStack, closing
from pathlib import Path
from typing import Any

from .store import AgentdStore, StoreError
from .storage_pair import attach_task_snapshot, task_database_path
from .trace_restore import rotate_restored_source
from .writer_lock import exclusive_writer

RESTORE_BARRIER = "restore-barrier.json"


class RestorePendingError(RuntimeError):
    """This directory cannot serve until restore reconciliation is resolved."""


def require_startable(state_dir: Path) -> None:
    barrier = state_dir / RESTORE_BARRIER
    # Presence alone fences startup, including incomplete writes and symlinks.
    if barrier.exists() or barrier.is_symlink():
        raise RestorePendingError("agentd restore reconciliation is required")


def _barrier(state_dir: Path, value: dict[str, Any]) -> None:
    descriptor = os.open(
        state_dir / RESTORE_BARRIER, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
    )
    with os.fdopen(descriptor, "w") as output:
        json.dump(value, output, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    directory = os.open(state_dir, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def stage_restore(
    *,
    snapshot_dir: Path,
    previous_state_dir: Path,
    destination_dir: Path,
    node_id: str,
    expected_source_epoch: str,
) -> dict[str, Any]:
    """Create a fenced restored copy; never resume or discard saved work.

    Inputs are trusted operator paths to agentd directories. The snapshot must
    include its matched DB/key pair. Old/snapshot owners must be stopped. A failed
    destination is retained behind its barrier for diagnosis; never reuse it.
    This internal API does not replace enrollment or manage remote producers.
    """
    snapshot = snapshot_dir.resolve(strict=True)
    previous = previous_state_dir.resolve(strict=True)
    destination = destination_dir.resolve()
    if any(
        destination == p or destination.is_relative_to(p) for p in (snapshot, previous)
    ):
        raise StoreError("restore destination must be a separate new directory")
    with ExitStack() as locks:
        for directory in sorted({snapshot, previous}):
            locks.enter_context(exclusive_writer(directory))
        require_startable(previous)
        if (
            not (snapshot / "agentd.sqlite3").is_file()
            or not (snapshot / "payload.key").is_file()
        ):
            raise StoreError("restore requires a matched database and payload key")
        destination.mkdir(mode=0o700)
        locks.enter_context(exclusive_writer(destination))
        _barrier(
            destination,
            {
                "version": 1,
                "state": "reconciliation_required",
                "previous_state_dir": str(previous),
            },
        )
        with (
            closing(
                sqlite3.connect(
                    (snapshot / "agentd.sqlite3").as_uri() + "?mode=ro", uri=True
                )
            ) as source,
            closing(sqlite3.connect(destination / "agentd.sqlite3")) as target,
        ):
            source.execute("BEGIN")
            paired = attach_task_snapshot(source, snapshot / "agentd.sqlite3")
            # Hold both read locks until both backups finish. A live source
            # transaction cannot commit between the two snapshots.
            source.execute("SELECT count(*) FROM sqlite_schema").fetchone()
            source.backup(target)
            if paired:
                with closing(
                    sqlite3.connect(task_database_path(destination / "agentd.sqlite3"))
                ) as task_target:
                    source.backup(task_target, name="task_state")
            source.rollback()
        shutil.copyfile(snapshot / "payload.key", destination / "payload.key")
        (destination / "payload.key").chmod(0o600)
        store = AgentdStore(destination / "agentd.sqlite3")
        try:
            db = store._connection
            for schema in ("main", "task_state"):
                if (
                    db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] != "ok"
                    or db.execute(f"PRAGMA {schema}.foreign_key_check").fetchone()
                    is not None
                ):
                    raise StoreError("restored database integrity check failed")
            # Validate every saved encrypted payload against the supplied key,
            # without emitting contents or assuming that a syntactically valid
            # key belongs to this snapshot.
            for row in db.execute("SELECT payload_json, result_json FROM tasks"):
                store._decode_content(row[0])
                if row[1] is not None:
                    store._decode_content(row[1])
            for row in db.execute("SELECT envelope_json FROM transport_outbox"):
                store._decode_content(row[0])
            with store._lock, db:
                db.execute("BEGIN IMMEDIATE")
                marker = rotate_restored_source(
                    db, node_id=node_id, expected_source_epoch=expected_source_epoch
                )
            hold_restored_execution(store, source_epoch=marker["source_epoch"])
        finally:
            store.close()
        # Destination remains barred. Retire the old directory only after the
        # copy/key and transition have succeeded; a crash here leaves both closed
        # or the old directory usable, never an automatically serving new copy.
        _barrier(
            previous,
            {"version": 1, "state": "retired", "replacement": str(destination)},
        )
        return marker


def hold_restored_execution(store: AgentdStore, *, source_epoch: str) -> dict[str, int]:
    """Hold a staged snapshot before activation; caller owns directory ownership.

    Saved tasks and command payloads keep their actual last observed state. Closed
    sessions cannot renew; unlike ordinary lease recovery, no task is requeued.
    This does not release the startup barrier or attest an external outcome.
    """
    from .trace_finish import close_session_bindings_locked

    barrier = store.path.parent / RESTORE_BARRIER
    try:
        with barrier.open() as source:
            status = json.loads(source.read(65537))
        if status.get("state") != "reconciliation_required":
            raise ValueError
    except (OSError, ValueError, AttributeError) as error:
        raise StoreError("restored staging barrier is required") from error
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        if (
            db.execute(
                "SELECT 1 FROM trace_sources s JOIN trace_journal j "
                "ON j.node_id=s.node_id AND j.source_epoch=s.source_epoch "
                "WHERE s.source_epoch=? AND s.active=1 AND j.source_seq=1 "
                "AND json_extract(j.event_json,'$.kind')='source' "
                "AND json_extract(j.event_json,'$.phase')='restored'",
                (source_epoch,),
            ).fetchone()
            is None
        ):
            raise StoreError("active restored source was not found")
        now = time.time_ns() // 1_000_000
        db.execute(
            "INSERT OR IGNORE INTO restore_holds(kind,object_id,source_epoch,held_at_ms) "
            "SELECT 'task',task_id,?,? FROM tasks WHERE state NOT IN "
            "('completed','failed','rejected','cancelled','expired','undeliverable')",
            (source_epoch, now),
        )
        db.execute(
            "INSERT OR IGNORE INTO restore_holds(kind,object_id,source_epoch,held_at_ms) "
            "SELECT 'transport',message_id,?,? FROM transport_outbox WHERE published_at_ms IS NULL",
            (source_epoch, now),
        )
        sessions = db.execute(
            "SELECT session_id FROM sessions WHERE closed_at_ms IS NULL"
        ).fetchall()
        for row in sessions:
            db.execute(
                "UPDATE sessions SET closed_at_ms=? WHERE session_id=?", (now, row[0])
            )
            close_session_bindings_locked(store, str(row[0]), now)
        return {
            kind: int(
                db.execute(
                    "SELECT COUNT(*) FROM restore_holds WHERE kind=?", (kind,)
                ).fetchone()[0]
            )
            for kind in ("task", "transport")
        }
