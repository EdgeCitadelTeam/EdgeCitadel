"""Atomic binding closure and unknown endings for unclosed observations."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_finish_request,
    validate_rpc_reply,
)
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore


def finish_trace(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_finish_request(params)).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='finish' AND scope=? AND request_id=?",
            (connector_id, params["binding_id"], params["request_id"]),
        ).fetchone()
        binding, task = authorized_binding_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
            operation="finish",
            allow_closed_receipt=previous is not None,
        )
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        outcome = params["outcome"]
        if task is not None and outcome != "unknown":
            expected = {
                "completed": "completed",
                "failed": "failed",
                "cancelled": "canceled",
            }.get(task.state)
            if outcome != expected:
                raise TraceContractError("task_outcome_mismatch")
        event = close_binding_locked(
            store,
            binding,
            node_id=node_id,
            now=now,
            outcome=outcome,
            reason=params["reason"],
            evidence_kind="source_observed"
            if task is not None
            else "integration_reported",
        )
        reply = {
            "schema_version": 1,
            "operation": "finish",
            "request_id": params["request_id"],
            "status": "ok",
            "result": {
                key: event[key] for key in ("event_id", "source_epoch", "source_seq")
            },
        }
        validate_rpc_reply(reply, operation="finish", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'finish',?,?,?,?,?)",
            (
                connector_id,
                binding["binding_id"],
                params["request_id"],
                digest,
                binding["binding_id"],
                canonical_bytes(reply).decode(),
            ),
        )
        return reply


def close_binding_locked(
    store: AgentdStore,
    binding: sqlite3.Row,
    *,
    now: int,
    outcome: str,
    reason: str,
    evidence_kind: str,
    node_id: str | None = None,
) -> dict[str, Any]:
    """Internal closure; caller owns authorization, lock and transaction."""
    db = store._connection
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    root = db.execute(
        "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND json_extract(event_json,'$.execution_attempt_id')=? "
        "AND json_extract(event_json,'$.kind')='run' AND json_extract(event_json,'$.phase')='started'",
        (binding["trace_id"], binding["execution_attempt_id"]),
    ).fetchone()
    if root is None:
        raise TraceContractError("binding_evidence_missing")
    original = json.loads(root[0])
    node_id = node_id or original["node_id"]
    base = {
        **original,
        "event_id": str(uuid4()),
        "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "duration_ms": None,
        "phase": outcome,
        "attributes": {"reason": reason},
        "evidence_kind": evidence_kind,
    }
    journal = TraceJournal(db)
    # Closure marks missing terminal evidence; it never claims the tool/model
    # actually failed or causes its external effect to run again.
    for operation in db.execute(
        "SELECT * FROM trace_operations WHERE binding_id=? AND terminal_event_id IS NULL",
        (binding["binding_id"],),
    ).fetchall():
        attrs = {"name": operation["name"], "reason": "unknown"}
        if operation["kind"] == "model":
            attrs.update(
                input_tokens=None,
                output_tokens=None,
                usage_unavailable_reason="interrupted",
            )
        interrupted = journal.record(
            node_id,
            {
                **base,
                "event_id": str(uuid4()),
                "kind": operation["kind"],
                "phase": "interrupted",
                "span_id": operation["span_id"],
                "parent_span_id": operation["parent_span_id"],
                "evidence_kind": "source_observed",
                "attributes": attrs,
            },
            selected=True,
            # Internal missing-terminal evidence is mandatory closure metadata,
            # even though its kind matches the interrupted optional operation.
            reserve_capacity=True,
        )
        db.execute(
            "UPDATE trace_operations SET phase='interrupted',terminal_event_id=? WHERE span_id=?",
            (interrupted["event_id"], operation["span_id"]),
        )
    event = journal.record(node_id, base, selected=True)
    db.execute(
        "UPDATE trace_bindings SET closed_at_ms=? WHERE binding_id=?",
        (now, binding["binding_id"]),
    )
    return event


def close_session_bindings_locked(
    store: AgentdStore, session_id: str, now: int
) -> None:
    for binding in store._connection.execute(
        "SELECT * FROM trace_bindings WHERE session_id=? AND closed_at_ms IS NULL",
        (session_id,),
    ).fetchall():
        close_binding_locked(
            store,
            binding,
            now=now,
            outcome="interrupted",
            reason="session_unavailable",
            evidence_kind="source_observed",
        )
