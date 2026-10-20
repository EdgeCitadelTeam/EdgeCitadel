"""The effect gateway: authorize one external effect of a bound invocation.

A harness calls ``effect.authorize`` before any external effect (a tool call that
writes, an HTTP request, a device command) and performs the effect only when the
reply is ``ok`` with ``state: committed``. The decision is made from agentd state
in one transaction and recorded as provenance in the invocation's run:

- ``effect/requested``: the harness asked, with the effect class, target and
  optional ``scheduled_at_ms``;
- ``effect/committed`` or ``effect/denied``: the decision, citing the request,
  with the deciding policy, its version and the reason.

An effect scheduled for later (``scheduled_at_ms`` in the future) is checked now
and, if permitted, only requested (``state: scheduled``). The harness commits it
with ``effect.commit`` when it is due; the checks run again then, and a
containment hold on the effect ID (``holds.kind = effect``) keeps it uncommitted
(``effect_held``, no event) until released. Between request and commit an
effect can therefore be held, which an immediate effect cannot.

Checks, in order; the first failing one decides:

1. the invocation's task is not held (``execution_held``) or egress-held
   (``egress_held``);
2. no authority, egress or credential revocation applies (``authority_revoked``);
3. when the controller lease is configured, it has been renewed recently
   (``controller_lease_expired``, fail closed);
4. the effect is within the invocation's effect scope: its connector's effect
   grant intersected with the scope its task inherited (``outside_effect_scope``);
5. no ingress filter denies the effect under the invocation's authorities
   (``ingress_filtered``);
6. the task's lease has not expired and has an effect left (``lease_exceeded``).

A committed effect is charged to the task's lease. A lease or controller-lease
denial also records a ``containment/alert`` event. Retries with the same request
ID return the stored reply without charging again. The gateway is cooperative:
it gates the harnesses that call it and is not an OS sandbox.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
import sqlite3
from typing import TYPE_CHECKING, Any, cast
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
    validate_effect_commit_request,
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
    binding_id: str,
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
    egress = egress_hold_locked(db, task_id, binding_id)
    if egress is not None:
        return EffectDecision(
            "containment_egress_hold", str(egress["policy_version"]), "egress_held"
        )
    revoked = revocation_locked(
        db,
        authorities=authorities,
        invocation=(task_id, binding_id),
        agent_id=str(connector["agent_id"]),
    )
    if revoked is not None:
        return EffectDecision(
            f"{revoked['kind']}_revocation",
            str(revoked["policy_version"]),
            "authority_revoked",
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


@dataclass(frozen=True)
class _Invocation:
    binding: sqlite3.Row
    connector: sqlite3.Row
    task_id: str | None
    context: TaskTraceContext | None
    run_started: dict[str, Any]


def _invocation_locked(
    store: AgentdStore,
    *,
    connector_id: str,
    token: str,
    binding_id: str,
    now: int,
) -> _Invocation:
    db = store._connection
    binding, task = authorized_binding_locked(
        store,
        connector_id=connector_id,
        token=token,
        binding_id=binding_id,
        now=now,
        operation="effect",
    )
    task_id = task.task_id if task is not None else None
    saved = (
        db.execute(
            "SELECT context_json FROM trace_task_contexts WHERE task_id=?", (task_id,)
        ).fetchone()
        if task_id is not None
        else None
    )
    started = db.execute(
        "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND "
        "json_extract(event_json,'$.execution_attempt_id')=? AND "
        "json_extract(event_json,'$.kind')='run' AND "
        "json_extract(event_json,'$.phase')='started'",
        (binding["trace_id"], binding["execution_attempt_id"]),
    ).fetchone()
    if started is None:
        raise TraceContractError("binding_evidence_missing")
    return _Invocation(
        binding,
        db.execute(
            "SELECT * FROM connectors WHERE connector_id=?", (connector_id,)
        ).fetchone(),
        task_id,
        TaskTraceContext(**json.loads(saved[0])) if saved else None,
        json.loads(started[0]),
    )


def _receipt_locked(
    db: sqlite3.Connection,
    connector_id: str,
    operation: str,
    binding_id: str,
    request_id: str,
    digest: str,
) -> dict[str, Any] | None:
    previous = db.execute(
        "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation=? "
        "AND scope=? AND request_id=?",
        (connector_id, operation, binding_id, request_id),
    ).fetchone()
    if previous is None:
        return None
    if previous["request_sha256"] != digest:
        raise TraceContractError("idempotency_conflict")
    return cast(dict[str, Any], json.loads(previous["result_json"]))


def _admit_storage(db: sqlite3.Connection) -> None:
    try:
        pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
    except OSError as error:
        raise TraceContractError("storage_unavailable") from error
    if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
        raise TraceContractError("quota_exceeded")


def _record(
    db: sqlite3.Connection,
    node_id: str,
    invocation: _Invocation,
    *,
    phase: str,
    identity: dict[str, Any],
    decision: EffectDecision | None,
    remaining: int | None,
    causes: list[dict[str, str]],
    now: int,
) -> dict[str, Any]:
    return TraceJournal(db).record(
        node_id,
        {
            **invocation.run_started,
            "event_id": str(uuid4()),
            "span_id": None,
            "parent_span_id": None,
            "kind": "effect",
            "phase": phase,
            "duration_ms": None,
            "causes": causes,
            "supersedes_event_id": None,
            "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "attributes": {
                **identity,
                "policy": decision.policy if decision else None,
                "policy_version": decision.policy_version if decision else None,
                "reason": decision.reason if decision else None,
                "effects_remaining": remaining,
            },
        },
        selected=True,
    )


def _settle_locked(
    store: AgentdStore,
    node_id: str,
    invocation: _Invocation,
    *,
    effect: EffectRule,
    identity: dict[str, Any],
    request_id: str,
    requested: dict[str, Any],
    now: int,
    final: bool,
) -> tuple[EffectDecision, dict[str, Any] | None, int | None]:
    """Decide; record committed or denied (if final, or denied) and alerts."""
    db = store._connection
    context = invocation.context
    decision = _decide(
        store,
        invocation.connector,
        task_id=invocation.task_id,
        binding_id=invocation.binding["binding_id"],
        context=context,
        effect=effect,
        now_ms=now,
    )
    if decision.allowed and final and invocation.task_id is not None:
        charge(db, invocation.task_id, used=1)
    remaining = (
        remaining_effects(db, invocation.task_id, context.lease) if context else None
    )
    decided = None
    if final or not decision.allowed:
        decided = _record(
            db,
            node_id,
            invocation,
            phase="committed" if decision.allowed else "denied",
            identity=identity,
            decision=decision,
            remaining=remaining,
            causes=[lineage.reference(requested)],
            now=now,
        )
    if decision.alert is not None:
        from .containment import record_alert_locked

        record_alert_locked(
            db,
            node_id,
            invocation.run_started,
            alert=decision.alert,
            request_id=request_id,
            lease_dimension=decision.lease_dimension,
            causes=[lineage.reference(decided or requested)],
            occurred_at=requested["occurred_at"],
        )
    return decision, decided, remaining


def _reply(
    request_id: str,
    decision: EffectDecision,
    *,
    state: str,
    event: dict[str, Any] | None,
    remaining: int | None,
    context: TaskTraceContext | None,
) -> dict[str, Any]:
    if not decision.allowed:
        return {
            "schema_version": 1,
            "operation": "effect",
            "request_id": request_id,
            "status": "error",
            "code": decision.reason,
            "retryable": False,
        }
    return {
        "schema_version": 1,
        "operation": "effect",
        "request_id": request_id,
        "status": "ok",
        "result": {
            "event_id": event["event_id"] if event else None,
            "state": state,
            "effects_remaining": remaining,
            "lease_expires_at_ms": context.lease.expires_at_ms
            if context and context.lease
            else None,
        },
    }


def _store_receipt_locked(
    db: sqlite3.Connection,
    connector_id: str,
    operation: str,
    binding_id: str,
    request_id: str,
    digest: str,
    reply: dict[str, Any],
) -> None:
    validate_rpc_reply(reply, operation="effect", request_id=request_id)
    db.execute(
        "INSERT INTO trace_requests(connector_id,operation,scope,request_id,"
        "request_sha256,binding_id,result_json) VALUES (?,?,?,?,?,?,?)",
        (
            connector_id,
            operation,
            binding_id,
            request_id,
            digest,
            binding_id,
            canonical_bytes(reply).decode(),
        ),
    )


def authorize_effect(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_effect_request(params)).hexdigest()
    effect = EffectRule(params["effect_class"], params["target"])
    scheduled_at = params.get("scheduled_at_ms")
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        invocation = _invocation_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
        )
        binding_id = invocation.binding["binding_id"]
        stored = _receipt_locked(
            db, connector_id, "effect", binding_id, params["request_id"], digest
        )
        if stored is not None:
            return stored
        _admit_storage(db)
        identity = {
            "effect_version": EFFECT_VERSION,
            "effect_id": params["request_id"],
            "effect_class": effect.effect_class,
            "target": effect.target,
            "tool_call_id": params.get("tool_call_id"),
            "scheduled_at_ms": scheduled_at,
        }
        requested = _record(
            db,
            node_id,
            invocation,
            phase="requested",
            identity=identity,
            decision=None,
            remaining=None,
            causes=[],
            now=now,
        )
        deferred = scheduled_at is not None and scheduled_at > now
        decision, decided, remaining = _settle_locked(
            store,
            node_id,
            invocation,
            effect=effect,
            identity=identity,
            request_id=params["request_id"],
            requested=requested,
            now=now,
            final=not deferred,
        )
        if decision.allowed and deferred:
            db.execute(
                "INSERT INTO scheduled_effects(effect_id,binding_id,effect_class,target,"
                "identity_json,requested_json,state) VALUES (?,?,?,?,?,?,'scheduled')",
                (
                    params["request_id"],
                    binding_id,
                    effect.effect_class,
                    effect.target,
                    json.dumps(identity),
                    json.dumps(lineage.reference(requested)),
                ),
            )
        reply = _reply(
            params["request_id"],
            decision,
            state="scheduled" if deferred else "committed",
            event=decided or requested,
            remaining=remaining,
            context=invocation.context,
        )
        _store_receipt_locked(
            db, connector_id, "effect", binding_id, params["request_id"], digest, reply
        )
        return reply


def commit_effect(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Commit a scheduled effect; the same checks run again at commit time."""
    from .containment import effect_hold_locked

    digest = hashlib.sha256(validate_effect_commit_request(params)).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        invocation = _invocation_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
        )
        binding_id = invocation.binding["binding_id"]
        stored = _receipt_locked(
            db, connector_id, "effect_commit", binding_id, params["request_id"], digest
        )
        if stored is not None:
            return stored
        row = db.execute(
            "SELECT * FROM scheduled_effects WHERE effect_id=? AND binding_id=? "
            "AND state='scheduled'",
            (params["effect_id"], binding_id),
        ).fetchone()
        if row is None:
            raise TraceContractError("effect_not_scheduled")
        _admit_storage(db)
        hold = effect_hold_locked(db, params["effect_id"])
        if hold is not None:
            # Still requested, not denied, and no receipt: the same request may
            # commit once the hold is released.
            return _reply(
                params["request_id"],
                EffectDecision(
                    "containment_effect_hold",
                    str(hold["policy_version"]),
                    "effect_held",
                ),
                state="scheduled",
                event=None,
                remaining=None,
                context=invocation.context,
            )
        decision, decided, remaining = _settle_locked(
            store,
            node_id,
            invocation,
            effect=EffectRule(row["effect_class"], row["target"]),
            identity=json.loads(row["identity_json"]),
            request_id=params["effect_id"],
            requested={
                **json.loads(row["requested_json"]),
                "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            },
            now=now,
            final=True,
        )
        db.execute(
            "UPDATE scheduled_effects SET state=? WHERE effect_id=?",
            ("committed" if decision.allowed else "denied", params["effect_id"]),
        )
        reply = _reply(
            params["request_id"],
            decision,
            state="committed",
            event=decided,
            remaining=remaining,
            context=invocation.context,
        )
        _store_receipt_locked(
            db,
            connector_id,
            "effect_commit",
            binding_id,
            params["request_id"],
            digest,
            reply,
        )
        return reply
