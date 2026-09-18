"""Reviewed activation of a restored source while uncertain work remains held."""

from __future__ import annotations

import hashlib
import json
import os
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .restore import RESTORE_BARRIER
from .store import AgentdStore, StoreError
from .trace_journal import TraceJournal
from .writer_lock import exclusive_writer


def review_inventory(store: AgentdStore) -> dict[str, Any]:
    """Return a bounded review summary; hash saved rows without exposing content."""
    digest = hashlib.sha256()
    counts = {"task": 0, "transport": 0}
    with store._lock:
        for row in store._connection.execute(
            "SELECT kind,object_id,source_epoch,held_at_ms FROM restore_holds ORDER BY kind,object_id"
        ):
            kind, object_id = str(row[0]), str(row[1])
            table, key = (
                ("tasks", "task_id")
                if kind == "task"
                else ("transport_outbox", "message_id")
            )
            saved = store._connection.execute(
                f"SELECT * FROM {table} WHERE {key}=?", (object_id,)
            ).fetchone()
            if saved is None:
                raise StoreError("restored hold has no saved object")
            digest.update(
                json.dumps(
                    [list(row), list(saved)], separators=(",", ":"), ensure_ascii=True
                ).encode()
            )
            digest.update(b"\n")
            counts[kind] += 1
    return {
        "inventory_sha256": digest.hexdigest(),
        "held_tasks": counts["task"],
        "held_messages": counts["transport"],
    }


def _read_barrier(directory: Path) -> dict[str, Any]:
    try:
        with (directory / RESTORE_BARRIER).open() as source:
            value = json.loads(source.read(65537))
        if not isinstance(value, dict):
            raise TypeError
        return value
    except (OSError, ValueError, TypeError) as error:
        raise StoreError("restore barrier is missing or invalid") from error


def _remove_barrier(directory: Path) -> None:
    (directory / RESTORE_BARRIER).unlink()
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def activate_restored_state(
    *,
    state_dir: Path,
    previous_state_dir: Path,
    node_id: str,
    source_epoch: str,
    inventory_sha256: str,
) -> dict[str, Any]:
    """Keep every reviewed hold; durably declare uncertainty before serving.

    Trusted operator API. This acknowledges an exact hold inventory; it does not
    declare held effects safe to retry, release a hold, or start the daemon.
    """
    state, previous = (
        state_dir.resolve(strict=True),
        previous_state_dir.resolve(strict=True),
    )
    if state == previous:
        raise StoreError("restore directories must differ")
    with ExitStack() as locks:
        for directory in sorted((state, previous)):
            locks.enter_context(exclusive_writer(directory))
        retired = _read_barrier(previous)
        if retired.get("state") != "retired" or retired.get("replacement") != str(
            state
        ):
            raise StoreError("previous restore writer is not retired")
        if (
            not (state / "agentd.sqlite3").is_file()
            or not (state / "payload.key").is_file()
        ):
            raise StoreError("restored database and key are required")
        store = AgentdStore(state / "agentd.sqlite3")
        try:
            db = store._connection
            with store._lock, db:
                db.execute("BEGIN IMMEDIATE")
                source = db.execute(
                    "SELECT j.event_json FROM trace_journal j JOIN trace_sources s "
                    "ON s.node_id=j.node_id AND s.source_epoch=j.source_epoch "
                    "WHERE s.node_id=? AND s.source_epoch=? AND s.active=1 AND j.source_seq=1",
                    (node_id, source_epoch),
                ).fetchone()
                if source is None:
                    raise StoreError("active restored source was not found")
                root = json.loads(source[0])
                if root["kind"] != "source" or root["phase"] != "restored":
                    raise StoreError("active restored source was not found")
                receipt = db.execute(
                    "SELECT inventory_sha256,event_id FROM restore_activations WHERE node_id=? AND source_epoch=?",
                    (node_id, source_epoch),
                ).fetchone()
                if (state / RESTORE_BARRIER).exists():
                    barrier = _read_barrier(state)
                    if barrier.get("state") != "reconciliation_required" or barrier.get(
                        "previous_state_dir"
                    ) != str(previous):
                        raise StoreError("restore staging lineage does not match")
                    if review_inventory(store)["inventory_sha256"] != inventory_sha256:
                        raise StoreError("restore inventory changed since review")
                    unheld = db.execute(
                        "SELECT 1 FROM tasks WHERE state NOT IN ('completed','failed','rejected','cancelled','expired','undeliverable') "
                        "AND task_id NOT IN (SELECT object_id FROM restore_holds WHERE kind='task') LIMIT 1"
                    ).fetchone()
                    unpublished = db.execute(
                        "SELECT 1 FROM transport_outbox WHERE published_at_ms IS NULL AND message_id NOT IN (SELECT object_id FROM restore_holds WHERE kind='transport') LIMIT 1"
                    ).fetchone()
                    active = db.execute(
                        "SELECT 1 FROM sessions WHERE closed_at_ms IS NULL LIMIT 1"
                    ).fetchone()
                    if unheld or unpublished or active:
                        raise StoreError("restored execution state is not fully held")
                elif receipt is None:
                    raise StoreError("restore barrier is missing")
                if receipt is not None:
                    if receipt[0] != inventory_sha256:
                        raise StoreError("restore review does not match activation")
                    saved = db.execute(
                        "SELECT event_json FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                        (node_id, source_epoch, receipt[1]),
                    ).fetchone()
                    if saved is None:
                        raise StoreError("restore activation evidence is missing")
                    event = json.loads(saved[0])
                else:
                    journal = TraceJournal(db)
                    _epoch, generation = journal.initialize(node_id)
                    through = db.execute(
                        "SELECT next_export_seq-1 FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                        (node_id, source_epoch, generation),
                    ).fetchone()[0]
                    event = journal.record(
                        node_id,
                        {
                            **root,
                            "event_id": str(uuid4()),
                            "kind": "coverage",
                            "phase": "unknown",
                            "causes": [
                                {
                                    key: root[key]
                                    for key in ("node_id", "source_epoch", "event_id")
                                }
                            ],
                            "occurred_at": datetime.now(UTC)
                            .isoformat(timespec="milliseconds")
                            .replace("+00:00", "Z"),
                            "attributes": {
                                "export_generation": generation,
                                "through_export_seq": through,
                                "reason": "unknown",
                            },
                        },
                        selected=True,
                    )
                    db.execute(
                        "INSERT INTO restore_activations(node_id,source_epoch,inventory_sha256,event_id) VALUES (?,?,?,?)",
                        (node_id, source_epoch, inventory_sha256, event["event_id"]),
                    )
            # A crash after COMMIT leaves the barrier intact; a matching retry
            # reuses the receipt/event and completes only this filesystem step.
            if (state / RESTORE_BARRIER).exists():
                barrier = _read_barrier(state)
                if barrier.get("state") != "reconciliation_required" or barrier.get(
                    "previous_state_dir"
                ) != str(previous):
                    raise StoreError("restore staging lineage does not match")
                _remove_barrier(state)
            return event
        finally:
            store.close()
