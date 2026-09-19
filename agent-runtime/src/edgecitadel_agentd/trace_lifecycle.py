"""Mandatory task boundaries recorded inside the task store transaction."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .trace_contract import TraceContractError
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

_PHASES = {
    "queued",
    "offered",
    "accepted",
    "running",
    "completed",
    "failed",
    "rejected",
    "cancelled",
    "expired",
    "undeliverable",
    "requeued",
}
_REASONS = {
    "deadline_exceeded": "deadline_expired",
    "executor_session_lost": "session_unavailable",
    "session_closed_before_execution": "session_closed",
    "permission_denied": "permission_denied",
}


def record_task_boundary(
    store: AgentdStore,
    *,
    event_type: str,
    event_id: str,
    actor_id: str | None,
    task_id: str | None,
    now: int,
    reason: object = None,
    node_id: str | None = None,
    source_role: str | None = None,
    evidence_kind: str = "source_observed",
) -> None:
    phase = event_type.removeprefix("task.")
    if not event_type.startswith("task.") or phase not in _PHASES or task_id is None:
        return
    identity_path = store.state_directory.parent / "node.json"
    try:
        configured = json.loads(identity_path.read_text())["agent_id"]
    except FileNotFoundError:
        configured = None
    except (ValueError, KeyError, TypeError) as error:
        raise TraceContractError("invalid_source_node") from error
    if node_id is not None and configured is not None and node_id != configured:
        raise TraceContractError("source_node_mismatch")
    node_id = node_id or configured
    if node_id is None:
        # Standalone stores have no enrolled host identity. Never invent one.
        return
    db = store._connection
    task = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if task is None:
        raise TraceContractError("task_evidence_missing")
    saved = db.execute(
        "SELECT context_json FROM trace_task_contexts WHERE task_id=?", (task_id,)
    ).fetchone()
    if saved is not None:
        context = json.loads(saved[0])
    else:
        payload = store._decode_content(task["payload_json"])
        metadata = payload.get("execution_context", {})
        context = {
            "context_id": task["context_id"],
            "parent_task_id": payload.get("parent_task_id"),
            "parent_run_id": metadata.get("parent_run_id")
            if isinstance(metadata, dict)
            else None,
        }
    binding = db.execute(
        "SELECT execution_attempt_id FROM trace_bindings WHERE task_id=? AND session_id=?",
        (task_id, task["claimed_session_id"]),
    ).fetchone()
    if source_role is None:
        if actor_id == "edgecitadel-system":
            source_role = "daemon"
        elif actor_id == task["sender_id"] and store._agent_is_local_locked(
            task["sender_id"]
        ):
            source_role = "sender"
        elif store._agent_is_local_locked(task["recipient_id"]):
            source_role = "recipient"
        elif store._agent_is_local_locked(task["sender_id"]):
            source_role = "sender"
        else:
            source_role = "daemon"
    attributes: dict[str, Any] = {"source_role": source_role}
    if reason is not None:
        attributes["reason"] = (
            _REASONS.get(reason, "unknown") if isinstance(reason, str) else "unknown"
        )
    TraceJournal(db).record(
        node_id,
        {
            "schema_version": 1,
            "event_id": event_id,
            "agent_id": actor_id,
            "trace_id": task["trace_id"],
            "task_id": task_id,
            "context_id": context["context_id"],
            "parent_task_id": context["parent_task_id"],
            "parent_run_id": context["parent_run_id"],
            "execution_attempt_id": binding[0] if binding else None,
            "span_id": None,
            "parent_span_id": None,
            "kind": "task",
            "phase": "queued" if phase == "requeued" else phase,
            "occurred_at": store._iso_timestamp(now),
            "duration_ms": None,
            "evidence_kind": evidence_kind,
            "causes": [],
            "attributes": attributes,
            "supersedes_event_id": None,
        },
        selected=True,
    )
