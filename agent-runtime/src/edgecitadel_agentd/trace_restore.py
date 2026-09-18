"""Atomic source identity transition for an offline, fenced restore workflow."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .trace_contract import TraceContractError
from .trace_journal import TraceJournal


def rotate_restored_source(
    connection: sqlite3.Connection, *, node_id: str, expected_source_epoch: str
) -> dict[str, Any]:
    """Rotate a restored source without rewriting retained replay identities.

    Internal only: caller owns the directory writer lock, store lock and active
    transaction, and must fence old producers/reconcile saved execution state
    before service resumes. This function neither copies files nor starts work.
    The expected epoch makes retry after a lost commit result idempotent.
    """
    if not connection.in_transaction:
        raise TraceContractError("trace_transaction_required")
    current = connection.execute(
        "SELECT source_epoch FROM trace_sources WHERE node_id=? AND active=1",
        (node_id,),
    ).fetchone()
    if current is None:
        raise TraceContractError("restore_source_missing")
    epoch = str(current[0])
    if epoch != expected_source_epoch:
        first = connection.execute(
            "SELECT event_json FROM trace_journal WHERE node_id=? "
            "AND source_epoch=? AND source_seq=1",
            (node_id, epoch),
        ).fetchone()
        if first is not None:
            marker = json.loads(first[0])
            if (
                marker["kind"] == "source"
                and marker["phase"] == "restored"
                and marker["attributes"].get("previous_source_epoch")
                == expected_source_epoch
            ):
                return marker
        raise TraceContractError("restore_source_changed")
    generation = connection.execute(
        "SELECT export_generation FROM trace_export_generations "
        "WHERE node_id=? AND source_epoch=? AND active=1",
        (node_id, epoch),
    ).fetchone()
    if generation is None:
        raise TraceContractError("missing_export_generation")
    connection.execute(
        "UPDATE trace_export_generations SET active=0 "
        "WHERE node_id=? AND source_epoch=? AND active=1",
        (node_id, epoch),
    )
    connection.execute(
        "UPDATE trace_sources SET active=0 WHERE node_id=? AND source_epoch=?",
        (node_id, epoch),
    )
    journal = TraceJournal(connection)
    _new_epoch, new_generation = journal.initialize(node_id)
    return journal.record(
        node_id,
        {
            "schema_version": 1,
            "event_id": str(uuid4()),
            "agent_id": None,
            "trace_id": None,
            "context_id": None,
            "task_id": None,
            "parent_task_id": None,
            "parent_run_id": None,
            "execution_attempt_id": None,
            "span_id": None,
            "parent_span_id": None,
            "kind": "source",
            "phase": "restored",
            "occurred_at": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "duration_ms": None,
            "evidence_kind": "source_observed",
            "causes": [],
            "attributes": {
                "export_generation": new_generation,
                "previous_source_epoch": epoch,
                "previous_export_generation": str(generation[0]),
            },
            "supersedes_event_id": None,
        },
        selected=True,
    )
