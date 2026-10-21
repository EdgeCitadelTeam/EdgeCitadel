"""The effect gateway: decide, record and (for registered effects) perform effects.

Two paths, recorded as provenance in the invocation's run:

``effect.execute`` (fenced at commit). The worker names a registered adapter
(``effect_adapters``), an effect class, a target and bounded data. The gateway
re-checks admission and records ``effect/committed`` in one transaction under
``store.control_lock``, the lock every containment command takes, and only then
performs the effect through the adapter, which holds the target's credential.
A command confirmed before that transaction therefore always wins, and every
effect committed before a command took effect is journaled at or below the
journal position its confirmation reports. An effect between its commit and the
end of its adapter call is listed as in flight in confirmations (it can no
longer be recalled). With ``scheduled_at_ms`` in the future the effect is
checked and requested now, kept by the gateway, and at its time checked again,
committed and performed by the gateway's scheduler. An effect hold placed on it
(``holds.kind = effect``) cancels it: if a hold is in place when it is due, it is
denied ``effect_held`` and never performed later. A hold released before the
due time lets it run on time.

``effect.authorize`` (not fenced). For effects only the harness can perform
(bash, file writes in its sandbox) the gateway decides immediately and the
harness performs the effect after an ``ok`` reply. Nothing stops a harness that
received ``committed`` from acting after a later containment command, or from
acting without asking: the gateway is cooperative and not an OS sandbox. This
bypass is a stated limitation, measured at the destination (ledger), not here.

Events: ``effect/requested`` (effect class, target, ``scheduled_at_ms``, the
``adapter`` or null), then ``effect/committed`` (with ``committed_at_ms``) or
``effect/denied``, citing the request, with the deciding policy, its version and
the reason. Checks, in order; the first failing one decides:

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
ID return the stored reply without charging or performing again.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
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
from .effect_adapters import EffectAdapter, EffectAdapterError
from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_effect_execute_request,
    validate_effect_request,
    validate_rpc_reply,
)
from .trace_correlation import TaskTraceContext
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

EFFECT_VERSION = 2
SCHEDULER_BATCH = 64
IDLE_POLL_SECONDS = 1.0


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _iso(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


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


def _invocation_of_binding_locked(
    store: AgentdStore, binding: sqlite3.Row
) -> _Invocation:
    db = store._connection
    task_id = binding["task_id"]
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
            "SELECT * FROM connectors WHERE connector_id=?", (binding["connector_id"],)
        ).fetchone(),
        task_id,
        TaskTraceContext(**json.loads(saved[0])) if saved else None,
        json.loads(started[0]),
    )


def _invocation_locked(
    store: AgentdStore,
    *,
    connector_id: str,
    token: str,
    binding_id: str,
    now: int,
) -> _Invocation:
    """The caller's live invocation, authenticated by its connector token."""
    binding, _ = authorized_binding_locked(
        store,
        connector_id=connector_id,
        token=token,
        binding_id=binding_id,
        now=now,
        operation="effect",
    )
    return _invocation_of_binding_locked(store, binding)


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
            "occurred_at": _iso(now),
            "attributes": {
                **identity,
                "committed_at_ms": now if phase == "committed" else None,
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
    adapter: str | None = None,
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
    result: dict[str, Any] = {
        "event_id": event["event_id"] if event else None,
        "state": state,
        "effects_remaining": remaining,
        "lease_expires_at_ms": context.lease.expires_at_ms
        if context and context.lease
        else None,
    }
    if adapter is not None:
        result.update(adapter=adapter, outcome=None)
    return {
        "schema_version": 1,
        "operation": "effect",
        "request_id": request_id,
        "status": "ok",
        "result": result,
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


def _identity(
    params: Mapping[str, Any], effect: EffectRule, adapter: str | None
) -> dict[str, Any]:
    return {
        "effect_version": EFFECT_VERSION,
        "effect_id": params["request_id"],
        "effect_class": effect.effect_class,
        "target": effect.target,
        "tool_call_id": params.get("tool_call_id"),
        "scheduled_at_ms": params.get("scheduled_at_ms"),
        "adapter": adapter,
    }


def authorize_effect(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Decide an effect the harness performs itself (not fenced at commit)."""
    digest = hashlib.sha256(validate_effect_request(params)).hexdigest()
    effect = EffectRule(params["effect_class"], params["target"])
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = _now_ms()
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
        identity = _identity(params, effect, None)
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
        decision, decided, remaining = _settle_locked(
            store,
            node_id,
            invocation,
            effect=effect,
            identity=identity,
            request_id=params["request_id"],
            requested=requested,
            now=now,
            final=True,
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
            db, connector_id, "effect", binding_id, params["request_id"], digest, reply
        )
        return reply


class EffectExecutor:
    """Performs registered effects for the gateway, now or at their time.

    The store owns one executor (``store.effects``); the daemon configures its
    adapters and runs its scheduler thread. ``in_flight`` is guarded by
    ``store.control_lock``: an effect enters it in its commit transaction and
    leaves it when its adapter call returned.
    """

    def __init__(
        self, store: AgentdStore, adapters: Mapping[str, EffectAdapter] | None = None
    ) -> None:
        self.store = store
        self.adapters: dict[str, EffectAdapter] = dict(adapters or {})
        self._in_flight: set[str] = set()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def in_flight(self) -> frozenset[str]:
        """Committed effects whose adapter call has not returned. Callers hold
        the control lock."""
        return frozenset(self._in_flight)

    # -- RPC ---------------------------------------------------------------------
    def execute(
        self,
        *,
        node_id: str,
        connector_id: str,
        token: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        store, db = self.store, self.store._connection
        digest = hashlib.sha256(validate_effect_execute_request(params)).hexdigest()
        adapter = self.adapters.get(params["adapter"])
        if adapter is None:
            raise TraceContractError("effect_adapter_unavailable")
        effect = EffectRule(params["effect_class"], params["target"])
        data = dict(params.get("data") or {})
        adapter.validate(effect, data)
        scheduled_at = params.get("scheduled_at_ms")
        with store.control_lock, store._lock, db:
            db.execute("BEGIN IMMEDIATE")
            now = _now_ms()
            invocation = _invocation_locked(
                store,
                connector_id=connector_id,
                token=token,
                binding_id=params["binding_id"],
                now=now,
            )
            binding_id = invocation.binding["binding_id"]
            stored = _receipt_locked(
                db,
                connector_id,
                "effect_execute",
                binding_id,
                params["request_id"],
                digest,
            )
            if stored is not None:
                return stored
            _admit_storage(db)
            deferred = scheduled_at is not None and scheduled_at > now
            identity = _identity(params, effect, adapter.name)
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
                    "INSERT INTO scheduled_effects(effect_id,binding_id,effect_class,"
                    "target,identity_json,requested_json,state,connector_id,adapter,"
                    "data_text,due_at_ms) VALUES (?,?,?,?,?,?,'scheduled',?,?,?,?)",
                    (
                        params["request_id"],
                        binding_id,
                        effect.effect_class,
                        effect.target,
                        json.dumps(identity),
                        json.dumps(lineage.reference(requested)),
                        connector_id,
                        adapter.name,
                        store._encode_content({"data": data}),
                        scheduled_at,
                    ),
                )
            elif decision.allowed:
                self._in_flight.add(params["request_id"])
            reply = _reply(
                params["request_id"],
                decision,
                state="scheduled" if deferred else "committed",
                event=decided or requested,
                remaining=remaining,
                context=invocation.context,
                adapter=adapter.name,
            )
            _store_receipt_locked(
                db,
                connector_id,
                "effect_execute",
                binding_id,
                params["request_id"],
                digest,
                reply,
            )
        if deferred:
            self._wake.set()
            return reply
        if not decision.allowed:
            return reply
        assert decided is not None
        outcome = self._perform(adapter, effect, data, decided)
        performed = {**reply, "result": {**reply["result"], "outcome": outcome}}
        validate_rpc_reply(
            performed, operation="effect", request_id=params["request_id"]
        )
        return performed

    # -- scheduled effects ---------------------------------------------------------
    def run_due(self, node_id: str, now_ms: int | None = None) -> int:
        """Check, commit and perform every scheduled effect due by ``now_ms``."""
        now = _now_ms() if now_ms is None else now_ms
        with self.store._lock:
            due = [
                row[0]
                for row in self.store._connection.execute(
                    "SELECT effect_id FROM scheduled_effects WHERE state='scheduled' "
                    "AND adapter IS NOT NULL AND due_at_ms<=? "
                    "ORDER BY due_at_ms,effect_id LIMIT ?",
                    (now, SCHEDULER_BATCH),
                )
            ]
        for effect_id in due:
            self._run_scheduled(node_id, effect_id)
        return len(due)

    def _run_scheduled(self, node_id: str, effect_id: str) -> None:
        from .containment import effect_hold_locked

        store, db = self.store, self.store._connection
        with store.control_lock, store._lock, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM scheduled_effects WHERE effect_id=? AND state='scheduled'",
                (effect_id,),
            ).fetchone()
            if row is None:
                return
            _admit_storage(db)
            now = _now_ms()
            binding = db.execute(
                "SELECT * FROM trace_bindings_all WHERE binding_id=?",
                (row["binding_id"],),
            ).fetchone()
            try:
                if binding is None:
                    raise TraceContractError("binding_evidence_missing")
                invocation = _invocation_of_binding_locked(store, binding)
            except TraceContractError:
                # Without its invocation's evidence the effect cannot be checked
                # or recorded again: it is dropped, never performed.
                db.execute(
                    "UPDATE scheduled_effects SET state='denied',outcome_json=? "
                    "WHERE effect_id=?",
                    (
                        json.dumps(
                            {
                                "performed": False,
                                "code": "invocation_unavailable",
                                "http_status": None,
                            },
                            sort_keys=True,
                        ),
                        effect_id,
                    ),
                )
                return
            identity = json.loads(row["identity_json"])
            requested = {**json.loads(row["requested_json"]), "occurred_at": _iso(now)}
            effect = EffectRule(row["effect_class"], row["target"])
            hold = effect_hold_locked(db, effect_id)
            if hold is not None:
                # Held when due: cancelled, never performed later.
                decision = EffectDecision(
                    "containment_effect_hold",
                    str(hold["policy_version"]),
                    "effect_held",
                )
                decided = _record(
                    db,
                    node_id,
                    invocation,
                    phase="denied",
                    identity=identity,
                    decision=decision,
                    remaining=None,
                    causes=[lineage.reference(requested)],
                    now=now,
                )
            else:
                decision, decided, _ = _settle_locked(
                    store,
                    node_id,
                    invocation,
                    effect=effect,
                    identity=identity,
                    request_id=effect_id,
                    requested=requested,
                    now=now,
                    final=True,
                )
            db.execute(
                "UPDATE scheduled_effects SET state=? WHERE effect_id=?",
                ("committed" if decision.allowed else "denied", effect_id),
            )
            if decision.allowed:
                self._in_flight.add(effect_id)
        if decision.allowed:
            assert decided is not None
            data = store._decode_content(row["data_text"])["data"]
            outcome = self._perform(
                self.adapters.get(row["adapter"]), effect, data, decided
            )
            with store._task_transaction():
                db.execute(
                    "UPDATE scheduled_effects SET outcome_json=? WHERE effect_id=?",
                    (json.dumps(outcome, sort_keys=True), effect_id),
                )

    def _perform(
        self,
        adapter: EffectAdapter | None,
        effect: EffectRule,
        data: Mapping[str, Any],
        committed: Mapping[str, Any],
    ) -> dict[str, Any]:
        effect_id = committed["attributes"]["effect_id"]
        record = {
            "schema_version": 1,
            "effect_id": effect_id,
            "node_id": committed["node_id"],
            "agent_id": committed["agent_id"],
            "task_id": committed["task_id"],
            "execution_attempt_id": committed["execution_attempt_id"],
            "effect_class": effect.effect_class,
            "target": effect.target,
            "scheduled_at_ms": committed["attributes"]["scheduled_at_ms"],
            "committed_at_ms": committed["attributes"]["committed_at_ms"],
        }
        try:
            if adapter is None:  # the configuration changed since it was scheduled
                raise EffectAdapterError("effect_adapter_unavailable")
            status = adapter.perform(effect, data, record)
            outcome = {"performed": True, "code": None, "http_status": status}
        except EffectAdapterError as error:
            outcome = {
                "performed": False,
                "code": error.code,
                "http_status": error.http_status,
            }
        finally:
            with self.store.control_lock:
                self._in_flight.discard(effect_id)
        return outcome

    # -- scheduler thread ------------------------------------------------------------
    def start(self, node_id: str) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, args=(node_id,), name="effect-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None

    def _next_due(self) -> int | None:
        with self.store._lock:
            row = self.store._connection.execute(
                "SELECT MIN(due_at_ms) FROM scheduled_effects "
                "WHERE state='scheduled' AND adapter IS NOT NULL"
            ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def _run(self, node_id: str) -> None:
        while not self._stop.is_set():
            try:
                due = self._next_due()
                wait = (
                    IDLE_POLL_SECONDS
                    if due is None
                    else min(IDLE_POLL_SECONDS, (due - _now_ms()) / 1000)
                )
                if wait > 0:
                    self._wake.wait(wait)
                    self._wake.clear()
                    continue
                self.run_due(node_id)
            except (sqlite3.Error, TraceContractError) as error:
                code = getattr(error, "code", type(error).__name__)
                print(f"agentd effect scheduler: {code}", file=sys.stderr)
                self._stop.wait(IDLE_POLL_SECONDS)
