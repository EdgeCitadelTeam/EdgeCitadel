"""The effect gateway: authorize one external effect of a bound invocation.

A harness calls ``effect.authorize`` before any external effect (a tool call that
writes, an HTTP request, a device command) and performs the effect only when the
reply is ``ok``. The decision is made from agentd state in one transaction and
recorded as provenance in the invocation's run:

- ``effect/attempted``: the harness asked, with the effect class and target;
- ``effect/allowed`` or ``effect/denied``: the decision, citing the attempt, with
  the deciding policy, its version and the reason.

Checks, in order; the first failing one decides:

1. the invocation's task is not held (``execution_held``) or egress-held
   (``egress_held``);
2. neither the task nor any task in its authority chain is revoked
   (``authority_revoked``);
3. when the controller lease is configured, it has been renewed recently
   (``controller_lease_expired``, fail closed);
4. the effect is within the invocation's effect scope: its connector's effect
   grant intersected with the scope its task inherited (``outside_effect_scope``);
5. no ingress filter denies the effect under the invocation's authorities
   (``ingress_filtered``);
6. the task's lease has not expired and has an effect left (``lease_exceeded``).

An allowed effect is charged to the task's lease. A lease or controller-lease
denial also records a ``containment/alert`` event. Retries with the same request
ID return the stored decision without charging again. The gateway is
cooperative: it gates the harnesses that call it and is not an OS sandbox.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from . import trace_capacity
from . import trace_lineage as lineage
from .delegation_authority import (
    GRANT_POLICY_VERSION,
    EffectRule,
    charge,
    delegation_grant,
    effect_allowed,
    remaining_effects,
)
from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_effect_request,
    validate_rpc_reply,
)
from .trace_correlation import TaskTraceContext
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

EFFECT_VERSION = 1


@dataclass(frozen=True)
class EffectDecision:
    policy: str
    policy_version: str | None
    reason: str | None = None
    alert: str | None = None
    lease_dimension: str | None = None

    @property
    def allowed(self) -> bool:
        return self.reason is None


def _decide(
    store: AgentdStore,
    connector: Any,
    *,
    task_id: str | None,
    context: TaskTraceContext | None,
    effect: EffectRule,
    now_ms: int,
) -> EffectDecision:
    from .containment import (
        authority_keys,
        egress_hold_locked,
        ingress_filter_locked,
        revocation_locked,
    )

    db = store._connection
    authorities = context.authorities if context else ((task_id,) if task_id else ())
    if task_id is not None:
        hold = store._task_hold_locked(task_id)
        if hold is not None:
            return EffectDecision(
                f"{hold['reason']}_hold", str(hold["policy_version"]), "execution_held"
            )
        egress = egress_hold_locked(db, task_id)
        if egress is not None:
            return EffectDecision(
                "containment_egress_hold", str(egress["policy_version"]), "egress_held"
            )
    revoked = revocation_locked(db, authorities)
    if revoked is not None:
        return EffectDecision(
            "revocation", str(revoked["policy_version"]), "authority_revoked"
        )
    if not store.controller_lease_valid(now_ms):
        return EffectDecision(
            "controller_lease",
            None,
            "controller_lease_expired",
            alert="controller_lease_expired",
        )
    grant = delegation_grant(store, connector)
    scope = grant.effect_scope(context.effect_scope if context else None)
    if not effect_allowed(scope, effect):
        return EffectDecision(grant.policy, grant.version, "outside_effect_scope")
    filtered = ingress_filter_locked(
        db, authority_keys(authorities, str(connector["agent_id"])), effect=effect
    )
    if filtered is not None:
        return EffectDecision(
            "ingress_filter", str(filtered["policy_version"]), "ingress_filtered"
        )
    lease = context.lease if context else None
    if lease is not None:
        if lease.expires_at_ms is not None and now_ms >= lease.expires_at_ms:
            return EffectDecision(
                "lease",
                GRANT_POLICY_VERSION,
                "lease_exceeded",
                alert="lease_exceeded",
                lease_dimension="wall_time",
            )
        if remaining_effects(db, task_id, lease) == 0:
            return EffectDecision(
                "lease",
                GRANT_POLICY_VERSION,
                "lease_exceeded",
                alert="lease_exceeded",
                lease_dimension="effects",
            )
    return EffectDecision(grant.policy, grant.version)


def authorize_effect(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_effect_request(params)).hexdigest()
    try:
        effect = EffectRule(params["effect_class"], params["target"])
    except TraceContractError as error:
        raise TraceContractError("invalid_effect_target") from error
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
            operation="effect",
        )
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='effect' "
            "AND scope=? AND request_id=?",
            (connector_id, binding["binding_id"], params["request_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        try:
            pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
        connector = db.execute(
            "SELECT * FROM connectors WHERE connector_id=?", (connector_id,)
        ).fetchone()
        task_id = task.task_id if task is not None else None
        saved = (
            db.execute(
                "SELECT context_json FROM trace_task_contexts WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if task_id is not None
            else None
        )
        context = TaskTraceContext(**json.loads(saved[0])) if saved else None
        decision = _decide(
            store,
            connector,
            task_id=task_id,
            context=context,
            effect=effect,
            now_ms=now,
        )
        if decision.allowed and task_id is not None:
            charge(db, task_id, used=1)
        remaining = remaining_effects(db, task_id, context.lease) if context else None
        invocation = db.execute(
            "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND "
            "json_extract(event_json,'$.execution_attempt_id')=? AND "
            "json_extract(event_json,'$.kind')='run' AND "
            "json_extract(event_json,'$.phase')='started'",
            (binding["trace_id"], binding["execution_attempt_id"]),
        ).fetchone()
        if invocation is None:
            raise TraceContractError("binding_evidence_missing")
        base = {
            **json.loads(invocation[0]),
            "span_id": None,
            "parent_span_id": None,
            "kind": "effect",
            "duration_ms": None,
            "causes": [],
            "supersedes_event_id": None,
            "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }
        identity = {
            "effect_version": EFFECT_VERSION,
            "effect_id": params["request_id"],
            "effect_class": effect.effect_class,
            "target": effect.target,
            "tool_call_id": params.get("tool_call_id"),
        }
        journal = TraceJournal(db)
        attempted = journal.record(
            node_id,
            {
                **base,
                "event_id": str(uuid4()),
                "phase": "attempted",
                "attributes": {
                    **identity,
                    "policy": None,
                    "policy_version": None,
                    "reason": None,
                    "effects_remaining": None,
                },
            },
            selected=True,
        )
        decided = journal.record(
            node_id,
            {
                **base,
                "event_id": str(uuid4()),
                "phase": "allowed" if decision.allowed else "denied",
                "causes": [lineage.reference(attempted)],
                "attributes": {
                    **identity,
                    "policy": decision.policy,
                    "policy_version": decision.policy_version,
                    "reason": decision.reason,
                    "effects_remaining": remaining,
                },
            },
            selected=True,
        )
        if decision.alert is not None:
            from .containment import record_alert_locked

            record_alert_locked(
                db,
                node_id,
                base,
                alert=decision.alert,
                request_id=params["request_id"],
                lease_dimension=decision.lease_dimension,
                causes=[lineage.reference(decided)],
                occurred_at=base["occurred_at"],
            )
        if decision.allowed:
            reply: dict[str, Any] = {
                "schema_version": 1,
                "operation": "effect",
                "request_id": params["request_id"],
                "status": "ok",
                "result": {
                    "event_id": decided["event_id"],
                    "effects_remaining": remaining,
                    "lease_expires_at_ms": context.lease.expires_at_ms
                    if context and context.lease
                    else None,
                },
            }
        else:
            reply = {
                "schema_version": 1,
                "operation": "effect",
                "request_id": params["request_id"],
                "status": "error",
                "code": decision.reason,
                "retryable": False,
            }
        validate_rpc_reply(reply, operation="effect", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,"
            "request_sha256,binding_id,result_json) VALUES (?,'effect',?,?,?,?,?)",
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
