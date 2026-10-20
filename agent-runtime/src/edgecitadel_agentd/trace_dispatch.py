"""Binding-authorized child dispatch with durable decision and retry evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from . import trace_capacity
from . import trace_lineage as lineage
from .delegation_authority import GRANT_POLICY_VERSION, delegation_grant
from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_dispatch_request,
    validate_rpc_reply,
)
from .trace_correlation import TaskTraceContext
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore


@dataclass(frozen=True)
class Admission:
    """One dispatch decision; ``code`` is None when the dispatch is allowed."""

    policy: str
    policy_version: str
    grant_version: str | None
    reason: str | None
    code: str | None
    scope: frozenset[str] = frozenset()


def _admit(
    store: AgentdStore,
    connector: sqlite3.Row,
    *,
    hold: sqlite3.Row | None,
    inherited: tuple[str, ...] | None,
    recipient_id: str,
) -> Admission:
    """Admission after binding authority: containment, grant, then inherited scope.

    A held invocation cannot delegate whatever its grant says. The policy
    version of a containment denial is the hold's fencing token.
    """
    if hold is not None:
        return Admission(
            f"{hold['reason']}_hold",
            str(hold["policy_version"]),
            None,
            "execution_held",
            "execution_held",
        )
    grant = delegation_grant(store, connector)
    if not grant.usable or recipient_id not in grant.recipients:
        return Admission(
            grant.policy,
            GRANT_POLICY_VERSION,
            grant.version,
            "permission_denied",
            "not_authorized",
        )
    if inherited is not None and recipient_id not in inherited:
        return Admission(
            grant.policy,
            GRANT_POLICY_VERSION,
            grant.version,
            "outside_authority_scope",
            "outside_authority_scope",
        )
    return Admission(
        grant.policy,
        GRANT_POLICY_VERSION,
        grant.version,
        None,
        None,
        grant.scope(inherited),
    )


def dispatch_trace(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_dispatch_request(params)).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        binding, task = authorized_binding_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
            operation="dispatch",
            delegation_allowed=True,
        )
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='dispatch' AND scope=? AND request_id=?",
            (connector_id, binding["binding_id"], params["request_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        # Every fresh decision retains a receipt, including permission denials.
        # Resolve authorized retries above before closing new admission.
        try:
            pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
        connector = db.execute(
            "SELECT * FROM connectors WHERE connector_id=?", (connector_id,)
        ).fetchone()
        saved = (
            db.execute(
                "SELECT context_json FROM trace_task_contexts WHERE task_id=?",
                (task.task_id,),
            ).fetchone()
            if task is not None
            else None
        )
        parent = TaskTraceContext(**json.loads(saved[0])) if saved is not None else None
        admission = _admit(
            store,
            connector,
            hold=store._task_hold_locked(task.task_id) if task is not None else None,
            inherited=parent.authority_scope if parent is not None else None,
            recipient_id=params["recipient_id"],
        )
        allowed = admission.code is None
        root = db.execute(
            "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND json_extract(event_json,'$.execution_attempt_id')=? AND json_extract(event_json,'$.kind')='run' AND json_extract(event_json,'$.phase')='started'",
            (binding["trace_id"], binding["execution_attempt_id"]),
        ).fetchone()
        if root is None:
            raise TraceContractError("binding_evidence_missing")
        base = {
            **json.loads(root[0]),
            "event_id": str(uuid4()),
            "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }
        journal = TraceJournal(db)
        decision = "allowed" if allowed else "denied"
        child_id = (params.get("child_task_id") or str(uuid4())) if allowed else None
        journal.record(
            node_id,
            {
                **base,
                "kind": "permission",
                "phase": decision,
                "attributes": {
                    "dispatch_id": params["request_id"],
                    "policy": admission.policy,
                    "policy_version": admission.policy_version,
                    "grant_version": admission.grant_version,
                    "reason": admission.reason,
                },
            },
            selected=True,
        )
        dispatched = journal.record(
            node_id,
            {
                **base,
                "event_id": str(uuid4()),
                "kind": "dispatch",
                "phase": decision,
                "attributes": {
                    "dispatch_id": params["request_id"],
                    "recipient_id": params["recipient_id"],
                    "skill_id": params["skill_id"],
                    "grant_version": admission.grant_version,
                    "reason": admission.reason,
                    **({"child_task_id": child_id} if child_id else {}),
                },
            },
            selected=True,
        )
        if child_id is not None:
            if task is None:
                context = TaskTraceContext(
                    child_id,
                    binding["context_id"] or binding["binding_id"],
                    binding["trace_id"],
                    parent_run_id=binding["trace_id"],
                    context_origin="source_explicit"
                    if binding["context_id"]
                    else "legacy_default",
                    authority_scope=tuple(admission.scope),
                )
            else:
                if parent is not None:
                    parent = replace(parent, trace_id=binding["trace_id"])
                else:
                    current = store.get_task(task.task_id)
                    if cast(dict[str, Any], current["payload"]).get("parent_task_id"):
                        raise TraceContractError("correlation_unavailable")
                    parent = TaskTraceContext(
                        task.task_id,
                        binding["context_id"] or task.task_id,
                        binding["trace_id"],
                        context_origin="source_explicit"
                        if binding["context_id"]
                        else "legacy_default",
                    )
                context = parent.child(child_id, admission.scope)
            payload = {
                "body": params["request"],
                "execution_context": context.execution_context(),
            }
            if context.parent_task_id is not None:
                payload["parent_task_id"] = context.parent_task_id
            store.create_task(
                task_id=child_id,
                sender_id=binding["agent_id"],
                recipient_id=params["recipient_id"],
                payload=payload,
                skill_id=params["skill_id"],
                deadline_at_ms=params["deadline_at_ms"],
                trace_id=context.trace_id,
                context_id=context.context_id,
                correlation=context,
                trace_node_id=node_id,
                trace_causes=[lineage.reference(dispatched)],
            )
            lineage.record(
                db,
                node_id,
                base,
                phase="generated",
                entity="request",
                entity_task_id=child_id,
                version=lineage.version_id("request", child_id, payload),
                external=False,
                causes=[lineage.reference(dispatched)],
                occurred_at=base["occurred_at"],
            )
            reply = {
                "schema_version": 1,
                "operation": "dispatch",
                "request_id": params["request_id"],
                "status": "ok",
                "result": {
                    "task_id": child_id,
                    "trace_id": context.trace_id,
                    "context_id": context.context_id,
                    "parent_task_id": context.parent_task_id,
                    "parent_run_id": context.parent_run_id,
                    "state": "queued",
                },
            }
        else:
            reply = {
                "schema_version": 1,
                "operation": "dispatch",
                "request_id": params["request_id"],
                "status": "error",
                "code": admission.code,
                "retryable": False,
            }
        validate_rpc_reply(reply, operation="dispatch", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'dispatch',?,?,?,?,?)",
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
