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
from .delegation_authority import (
    GRANT_POLICY_VERSION,
    EffectRule,
    Lease,
    LeaseExceeded,
    LeaseRequest,
    charge,
    child_lease,
    delegation_grant,
    remaining_effects,
)
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
    """One dispatch decision; ``code`` is None when the dispatch is allowed.

    ``label``: allowed, but the sender carries an egress label, so the
    dispatch is held as an event (the child is created, its request waits).
    """

    policy: str
    policy_version: str
    grant_version: str | None
    reason: str | None
    code: str | None
    recipients: frozenset[str] = frozenset()
    effects: frozenset[EffectRule] = frozenset()
    lease: Lease | None = None
    lease_dimension: str | None = None
    label: Any = None


def _denied(row: sqlite3.Row, policy: str, code: str) -> Admission:
    return Admission(policy, str(row["policy_version"]), None, code, code)


def _admit(
    store: AgentdStore,
    connector: sqlite3.Row,
    *,
    task_id: str | None,
    binding_id: str,
    parent: TaskTraceContext | None,
    recipient_id: str,
    lease: LeaseRequest,
    now_ms: int,
) -> Admission:
    """Admission after binding authority; the first failing check decides.

    Containment comes first (a paused invocation, or a revoked authority,
    cannot delegate whatever its grant says), then the grant, the inherited
    scope, ingress filters and the lease. The policy version of a containment
    denial is the deciding owner's fencing token. A dispatch that passes every
    check from an invocation under an egress label is held as an event; a
    restore egress hold denies it (``egress_held``).
    """
    # containment imports the store, which imports this module.
    from .containment import (
        authority_keys,
        egress_hold_locked,
        ingress_filter_locked,
        revocation_locked,
    )

    db = store._connection
    authorities = parent.authorities if parent else ((task_id,) if task_id else ())
    if task_id is not None:
        hold = store._task_hold_locked(task_id)
        if hold is not None:
            return _denied(hold, f"{hold['reason']}_hold", "execution_held")
    revoked = revocation_locked(
        db,
        authorities=authorities,
        invocation=(task_id, binding_id),
        agent_id=str(connector["agent_id"]),
    )
    if revoked is not None:
        return _denied(revoked, f"{revoked['kind']}_revocation", "authority_revoked")
    grant = delegation_grant(store, connector)
    if not grant.usable or recipient_id not in grant.recipients:
        return Admission(
            grant.policy,
            GRANT_POLICY_VERSION,
            grant.version,
            "permission_denied",
            "not_authorized",
        )
    inherited = parent.authority_scope if parent is not None else None
    if inherited is not None and recipient_id not in inherited:
        return Admission(
            grant.policy,
            GRANT_POLICY_VERSION,
            grant.version,
            "outside_authority_scope",
            "outside_authority_scope",
        )
    filtered = ingress_filter_locked(
        db,
        authority_keys(authorities, str(connector["agent_id"])),
        agent_id=recipient_id,
    )
    if filtered is not None:
        return _denied(filtered, "ingress_filter", "ingress_filtered")
    parent_lease = parent.lease if parent is not None else None
    try:
        child = child_lease(
            parent_lease,
            remaining_effects(db, task_id, parent_lease),
            lease,
            now_ms,
        )
    except LeaseExceeded as exceeded:
        return Admission(
            "lease",
            GRANT_POLICY_VERSION,
            grant.version,
            "lease_exceeded",
            "lease_exceeded",
            lease_dimension=exceeded.dimension,
        )
    label = egress_hold_locked(db, task_id, binding_id)
    if label is not None and label["reason"] != "containment":
        return _denied(label, f"{label['reason']}_egress_hold", "egress_held")
    return Admission(
        grant.policy,
        GRANT_POLICY_VERSION,
        grant.version,
        None,
        None,
        grant.scope(inherited),
        grant.effect_scope(parent.effect_scope if parent is not None else None),
        child,
        label=label,
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
        requested = params.get("lease") or {}
        admission = _admit(
            store,
            connector,
            task_id=task.task_id if task is not None else None,
            binding_id=binding["binding_id"],
            parent=parent,
            recipient_id=params["recipient_id"],
            lease=LeaseRequest(**requested),
            now_ms=now,
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
        if admission.code == "lease_exceeded":
            from .containment import record_alert_locked

            record_alert_locked(
                db,
                node_id,
                base,
                alert="lease_exceeded",
                request_id=params["request_id"],
                lease_dimension=admission.lease_dimension,
                causes=[lineage.reference(dispatched)],
                occurred_at=base["occurred_at"],
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
                    authority_scope=tuple(admission.recipients),
                    effect_scope=tuple(admission.effects),
                    lease=admission.lease,
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
                context = parent.child(
                    child_id,
                    recipients=admission.recipients,
                    effects=admission.effects,
                    lease=admission.lease,
                )
                # The child's effect budget is reserved from the parent's.
                if (
                    admission.lease is not None
                    and admission.lease.max_effects is not None
                    and parent.lease is not None
                    and parent.lease.max_effects is not None
                ):
                    charge(db, task.task_id, reserved=admission.lease.max_effects)
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
            if admission.label is not None:
                from .containment import hold_event_locked

                hold_event_locked(
                    store,
                    node_id,
                    json.loads(root[0]),
                    kind="dispatch",
                    subject_id=child_id,
                    task_id=task.task_id if task is not None else None,
                    binding_id=binding["binding_id"],
                    label=admission.label,
                    causes=[lineage.reference(dispatched)],
                    recipient_id=params["recipient_id"],
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
