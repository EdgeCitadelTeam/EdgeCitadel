"""Shared store-owned binding authorization for transactional trace operations."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from .trace_authority import (
    BindingAuthority,
    ExecutionAuthority,
    SessionAuthority,
    authorize_binding_operation,
)
from .trace_contract import TraceContractError

if TYPE_CHECKING:
    from .store import AgentdStore


def authorized_binding_locked(
    store: AgentdStore,
    *,
    connector_id: str,
    token: str,
    binding_id: str,
    now: int,
    operation: str,
    allow_closed_receipt: bool = False,
    delegation_allowed: bool = False,
) -> tuple[sqlite3.Row, ExecutionAuthority | None]:
    db = store._connection
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    connector = store.authenticate(connector_id, token)
    binding = db.execute(
        "SELECT * FROM trace_bindings WHERE binding_id=? AND connector_id=?",
        (binding_id, connector_id),
    ).fetchone()
    if binding is None:
        raise TraceContractError("binding_not_owned")
    session = db.execute(
        "SELECT * FROM sessions WHERE session_id=? AND connector_id=?",
        (binding["session_id"], connector_id),
    ).fetchone()
    if session is None:
        raise TraceContractError("session_unavailable")
    task = None
    if binding["task_id"]:
        row = db.execute(
            "SELECT * FROM tasks WHERE task_id=?", (binding["task_id"],)
        ).fetchone()
        if row is None:
            raise TraceContractError("binding_task_mismatch")
        task = ExecutionAuthority(
            row["task_id"],
            row["recipient_id"],
            row["claimed_session_id"],
            row["state"],
        )
    authorize_binding_operation(
        SessionAuthority(
            connector_id,
            connector["agent_id"],
            session["session_id"],
            False,
            session["closed_at_ms"] is not None,
            session["lease_expires_at_ms"],
            connector["host_type"] == "managed-agent"
            or "edgecitadel_trace"
            in json.loads(connector["capabilities_json"])["items"],
        ),
        BindingAuthority(
            connector_id,
            binding["agent_id"],
            binding["session_id"],
            binding["task_id"],
            binding["closed_at_ms"] is not None and not allow_closed_receipt,
        ),
        now_ms=now,
        task=task,
        operation=operation,
        delegation_allowed=delegation_allowed,
    )
    return binding, task
