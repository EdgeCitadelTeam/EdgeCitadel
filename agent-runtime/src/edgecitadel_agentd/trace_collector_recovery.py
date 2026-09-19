"""Restartable collector-change recovery, isolated from executable work."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .trace_contract import TraceContractError
from .trace_journal import TraceJournal
from .trace_completed import export_page

if TYPE_CHECKING:
    from .store import AgentdStore

RECOVERY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trace_collector_recovery (
    node_id TEXT NOT NULL, source_epoch TEXT NOT NULL, export_generation TEXT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('scanning','ready','live')),
    scanned_through INTEGER NOT NULL, assigned_through INTEGER NOT NULL,
    blocked_epochs_json TEXT NOT NULL,
    PRIMARY KEY(node_id,source_epoch,export_generation),
    FOREIGN KEY(node_id,source_epoch,export_generation)
        REFERENCES trace_export_generations(node_id,source_epoch,export_generation)
);
"""
MAX_BLOCKED_EPOCHS = 64
_SCOPE = "node_id=? AND source_epoch=? AND export_generation=?"


def begin_recovery(
    store: AgentdStore, scope: tuple[str, str, str], *, expected_epoch: str
) -> None:
    """Caller confirmed collector change; fence settlement before replay work."""
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("recovery_requires_committed_store")
        with store._connection:
            db = store._connection
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(
                f"SELECT collector_epoch FROM trace_source_settlements WHERE {_SCOPE}",
                scope,
            ).fetchone()
            if cursor is None or cursor[0] != expected_epoch:
                raise TraceContractError("recovery_epoch_mismatch")
            existing = db.execute(
                f"SELECT phase,blocked_epochs_json FROM trace_collector_recovery WHERE {_SCOPE}",
                scope,
            ).fetchone()
            blocked = json.loads(existing[1]) if existing else []
            if existing and existing[0] != "live":
                return
            if expected_epoch not in blocked:
                if len(blocked) >= MAX_BLOCKED_EPOCHS:
                    raise TraceContractError("recovery_epoch_capacity_exceeded")
                blocked.append(expected_epoch)
            through = db.execute(
                f"SELECT next_export_seq-1 FROM trace_export_generations WHERE {_SCOPE}",
                scope,
            ).fetchone()[0]
            db.execute(
                "INSERT INTO trace_collector_recovery VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(node_id,source_epoch,export_generation) DO UPDATE SET "
                "phase='scanning',scanned_through=0,assigned_through=excluded.assigned_through,blocked_epochs_json=excluded.blocked_epochs_json",
                (*scope, "scanning", 0, through, json.dumps(blocked)),
            )


def recover_batch(store: AgentdStore, scope: tuple[str, str, str]) -> bool:
    """Process at most64 rows; loss marker, replay marks and progress commit together."""
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("recovery_requires_committed_store")
        with store._connection:
            db = store._connection
            db.execute("BEGIN IMMEDIATE")
            recovery = db.execute(
                f"SELECT phase,scanned_through,assigned_through FROM trace_collector_recovery WHERE {_SCOPE}",
                scope,
            ).fetchone()
            if recovery is None:
                raise TraceContractError("recovery_not_started")
            if recovery[0] != "scanning":
                return True
            after, through = recovery[1], recovery[2]
            rows = [
                (
                    row["export_seq"],
                    row["journal_event_id"] if row["event_json"] is not None else None,
                )
                for row in export_page(
                    db, scope, after=after, through=through, limit=64
                )
            ]
            end = rows[-1][0] if len(rows) == 64 else through
            missing, next_position = [], after + 1
            for position, event_id in rows:
                if event_id is not None:
                    if position > next_position:
                        missing.append({"first": next_position, "last": position - 1})
                    next_position = position + 1
            if next_position <= end:
                missing.append({"first": next_position, "last": end})
            if missing:
                event: dict[str, Any] = {
                    key: None
                    for key in (
                        "agent_id",
                        "trace_id",
                        "context_id",
                        "task_id",
                        "parent_task_id",
                        "parent_run_id",
                        "execution_attempt_id",
                        "span_id",
                        "parent_span_id",
                        "duration_ms",
                        "supersedes_event_id",
                    )
                }
                event.update(
                    schema_version=1,
                    event_id=str(uuid4()),
                    kind="coverage",
                    phase="lost",
                    occurred_at=datetime.now(UTC)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    evidence_kind="source_observed",
                    causes=[],
                    attributes={
                        "affected_source_epoch": scope[1],
                        "export_generation": scope[2],
                        "through_export_seq": through,
                        "lost_ranges": missing,
                        "reason": "retention_expired",
                    },
                )
                TraceJournal(db).record(
                    scope[0], event, selected=True, reserve_capacity=True
                )
            for position, event_id in rows:
                db.execute(
                    "UPDATE trace_spool_all SET state=?,collector_epoch=NULL,core_outcome=NULL,journal_event_id=? "
                    f"WHERE {_SCOPE} AND export_seq=?",
                    (
                        "pending" if event_id else "lost_with_marker",
                        event_id,
                        *scope,
                        position,
                    ),
                )
            ready = end == through
            db.execute(
                f"UPDATE trace_collector_recovery SET scanned_through=?,phase=? WHERE {_SCOPE}",
                (end, "ready" if ready else "scanning", *scope),
            )
    return ready
