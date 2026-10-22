"""Per-run metrics from ground truth, the ledger and the monitor's decision log.

Definitions mirror the simulator's ``contain.metrics`` so runtime and simulator
rows are comparable (times in seconds from the run's start ``t0``):

- **taint at alert**: the fault's seeds plus every task whose consumed content
  carried a fault label before the alert (undeclared contamination);
  **normal** tasks are outside its declared closure (the plan's dependents).
- **at risk**: normal tasks whose own work was not complete at the alert. A
  task's work is complete when it output its items, every push and creating
  input it hands on was dispatched, and its declared effects committed.
- **kept**: an at-risk task whose work completed with fault-free content
  (values equal the workload's expected values); reported as a count share, a
  compute-weighted share, GPU-seconds and tokens. **contaminated**: an at-risk
  task that consumed faulty content; **collateral**: at risk, neither kept nor
  contaminated; **interrupted**: at risk and paused at least once.
- **violation**: a tainted effect of a protected class that the sink
  committed. Taint is the commit's lineage (``groundtruth.commit_lineage``:
  the runtime task the gateway committed for, its planned task, and whether
  the fault reaches that task), so a commit without the worker's
  ``attempted`` record (no fault labels) is still classified; one whose
  issuer cannot be resolved counts as tainted. ``commits_unlabelled`` (no
  ``attempted`` record), ``commits_unresolved`` (no issuer) and
  ``commits_label_mismatch`` (labels and lineage disagree) must be 0.

Containment outcomes are split (no single "fully contained" headline):

- (a) ``clean_after_alert``: no violation committed after the alert (a run
  that timed out is not clean: its observation is incomplete);
- (b) ``tainted_after_established``: violations committed after containment
  was established (NaN when it never was); ``tainted_after_tau1``: violations
  committed after tau_1 (the monitor's ``converged`` record: phase 1's literal
  convergence check passed), each ordered by its commit time at its own site
  (the gateway's, from its post or Core's record; the ledger's time when
  neither has it); NaN with
  ``converged`` 0 when the check never passed (no tau_1: not converged);
- (c) ``established``: every action of the monitor's initial response (its
  first decision that chose actions) was confirmed, and
  ``time_to_established_s`` from the alert to the last confirmation;
- (d) ``unconfirmed``, ``control_failed``, ``infeasible`` and ``timeout`` flags.

Every run stays in every denominator. Control accounting per run (logical
actions, control messages per node, confirmations) comes from the decision
log; ``first_action_s`` / ``last_action_s`` are the first and last
confirmation over the whole incident.

``alert_source`` says who raised the alert: the harness at its trigger
(``injected``) or, in a detector-triggered run (the alert's trigger is
``none``), the monitor's own detectors (``detection``). There the alert time
is the monitor's receipt of the first detector alert on the fault's lineage,
and the incident it opened is the response; ``detection_latency_ms`` runs
from the lineage's first boundary violation (node clock mapped onto the
services' clock) to that alert, and every time above is measured from it. A
run no detector alert reached is ``detected`` 0: scored from its start, with
NaN alert-relative outcomes.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator, Mapping, Set
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from .groundtruth import (
    RunRecords,
    commit_lineage,
    declared_closure,
    expected_contents,
    fault_seeds,
    ledger_effects,
    protected_classes,
    read_records,
    runtime_tasks,
)
from .spec import Spec, derive, fault_origin

NAN = float("nan")
# Action states (the monitor's ActionRecord) that leave an action not in force:
# rejected or undeliverable, or sent without a confirmation (m3/integration
# ab174ed on) so it may or may not have landed.
CONTROL_FAILURES = frozenset({"failed", "unconfirmed"})


def _seconds(t_ms: float | None, t0_ms: float) -> float:
    return NAN if t_ms is None else (t_ms - t0_ms) / 1000


def completion_times(
    spec: Spec, records: RunRecords, tainted: Set[str] = frozenset()
) -> dict[str, int | None]:
    """When each task's own work was complete (services clock), or None.

    A push or creation whose target is in ``tainted`` (the declared closure)
    is not an obligation: the target's work is discarded anyway, so refusing
    that hand-off loses none of the producer's work (amendment of 2026-10-22).
    """
    derived = derive(spec)
    effects = ledger_effects(records)
    done: dict[str, int | None] = {}
    for t in spec.tasks:
        rec = records.tasks[t.id]
        done[t.id] = None
        if rec.produced_at is None:
            continue
        times = [rec.produced_at]
        for e in derived.effects_of.get(t.id, ()):
            entry = effects.get(e.id)
            if entry is None or entry["state"] != "committed":
                break
            times.append(entry["t_committed"])
        else:
            pushed = [
                rec.dispatches.get(("deliver", f"{d.item}->{d.target}"))
                for d in derived.deliveries_out.get(t.id, ())
                if d.target not in tainted
            ]
            created = [
                rec.dispatches.get(("create", f"{derived.creation[c].item}->{c}"))
                for c in derived.creates.get(t.id, ())
                if c not in tainted
            ]
            sent = pushed + created
            if any(p is None or p[1] is not None for p in sent):
                continue
            times += [p[0] for p in sent]
            done[t.id] = max(times)
    return done


def correct_content(
    spec: Spec, records: RunRecords, expected: Mapping[str, Any]
) -> dict[str, bool]:
    derived = derive(spec)
    effects = ledger_effects(records)
    out = {}
    for t in spec.tasks:
        rec = records.tasks[t.id]
        ok = all(rec.produced[i][1] == expected[i]["value"] for i in rec.produced)
        for e in derived.effects_of.get(t.id, ()):
            entry = effects.get(e.id)
            if entry is not None and entry.get("value") != expected[e.id]["value"]:
                ok = False
        out[t.id] = ok and rec.tainted_at is None
    return out


def operator_incidents(decisions: Iterable[Mapping[str, Any]]) -> set[str]:
    """The incidents the harness's operator alert opened (or joined)."""
    return {
        str(d.get("incident_id"))
        for d in decisions
        if d.get("type") == "decision"
        and (d.get("alert") or {}).get("kind") == "operator"
    }


def _responses(
    decisions: list[Mapping[str, Any]], incidents: Set[str]
) -> tuple[list[Mapping[str, Any]], int]:
    """The decision and re-solve records of the response's ``incidents``, and
    how many other incidents the monitor's own detectors opened."""
    opened = [d for d in decisions if d.get("type") == "decision"]
    responses = [
        d
        for d in decisions
        if d.get("type") in {"decision", "resolve"}
        and str(d.get("incident_id")) in incidents
    ]
    return responses, sum(
        1 for d in opened if str(d.get("incident_id")) not in incidents
    )


def tau1_ms(
    decisions: Iterable[Mapping[str, Any]], incidents: Set[str] | None = None
) -> float | None:
    """tau_1 of the response: the first ``converged`` record of its
    ``incidents`` (default: the operator alert's), None if phase 1 never
    converged."""
    decisions = list(decisions)
    if incidents is None:
        incidents = operator_incidents(decisions)
    times = [
        float(d["tau1"])
        for d in decisions
        if d.get("type") == "converged" and str(d.get("incident_id")) in incidents
    ]
    return min(times) if times else None


def _records(d: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [*d.get("phase1", ()), *d.get("phase2", ())]


def _sites(record: Mapping[str, Any]) -> list[tuple[str, str, Mapping[str, Any]]]:
    """(node, message key, outcome) per node the action was sent to.

    From m3/integration ab174ed on, a record lists its per-node outcomes
    (``sites``, each with its ``command_id``); earlier logs have one outcome,
    the record itself, at the action's node.
    """
    label = _action_label(record["action"])
    sites = record.get("sites")
    if isinstance(sites, Mapping) and sites:
        return [
            (node, str(o.get("command_id") or f"{label}@{node}@{o.get('t_sent')}"), o)
            for node, o in sites.items()
        ]
    return [(record["action"]["node_id"], f"{label}@{record.get('t_sent')}", record)]


def sent_to_confirmed(
    decisions: Iterable[Mapping[str, Any]],
) -> dict[str, list[float]]:
    """Per site, every control command's time from sent to confirmed (ms, both
    on the monitor's clock), over every incident of a decision log. A command
    listed again by later records counts once; one never confirmed (failed or
    unconfirmed) has no time."""
    seen: dict[tuple[str, str], float] = {}
    for d in decisions:
        for record in [*_records(d), *d.get("released", ())]:
            for node, key, outcome in _sites(record):
                sent, confirmed = outcome.get("t_sent"), outcome.get("t_confirmed")
                if sent is None or confirmed is None:
                    continue
                seen.setdefault((node, key), float(confirmed) - float(sent))
    out: dict[str, list[float]] = {}
    for (node, _), ms in seen.items():
        out.setdefault(node, []).append(ms)
    return out


def control_accounting(responses: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Logical actions, control messages per node and confirmations.

    A logical action is one monitor action (op, kind, object, deciding node).
    Every command a node was sent is one message (a revocation may go to
    several nodes), and so is every release; a confirmation is a message the
    node confirmed. Retransmissions of one command (NATS retries keep its
    ``command_id``) and the monitor's controller-lease renewals are not in the
    decision log and not counted.
    """
    logical: set[str] = set()
    messages: dict[tuple[str, str], tuple[str, bool]] = {}
    for d in responses:
        for record in [*_records(d), *d.get("released", ())]:
            logical.add(_action_label(record["action"]))
            for node, key, outcome in _sites(record):
                if outcome.get("t_sent") is None:
                    continue  # planned, never sent
                state = outcome.get("state", record.get("state"))
                messages[("send", key)] = (
                    node,
                    outcome.get("t_confirmed") is not None
                    and state not in CONTROL_FAILURES,
                )
                if record.get("t_release_sent") is not None:
                    messages[("release", key)] = (node, record["state"] == "released")
    by_node: dict[str, int] = {}
    for node, _ in messages.values():
        by_node[node] = by_node.get(node, 0) + 1
    return {
        "n_logical_actions": len(logical),
        "n_control_messages": len(messages),
        "n_release_messages": sum(1 for kind, _ in messages if kind == "release"),
        "n_confirmations": sum(1 for _, ok in messages.values() if ok),
        "control_messages_by_node": json.dumps(by_node, sort_keys=True),
    }


def decision_metrics(
    decisions: list[Mapping[str, Any]],
    t_alert_ms: float,
    incidents: Set[str] | None = None,
) -> dict[str, Any]:
    """The monitor's response, from its decision log.

    Only the response's ``incidents`` are the strategy's response: by default
    those the harness's operator alert opened (or joined); in a
    detector-triggered run the one its detection opened (``detection``).
    Incidents the monitor's own detectors opened otherwise (for example a
    lease violation when a worker loses its session) are counted in
    ``n_detector_decisions`` and otherwise left out.

    The monitor re-solves on every taint advance (``resolve`` records) for
    every strategy, so its initial response is the first record that chose
    actions; containment is **established** when every one of those actions
    was confirmed, at the last confirmation. Its solve and G_C size describe
    the decision. The flags count, never exclude: ``unconfirmed`` (an action
    sent without a confirmation, or a phase 2 left unconfirmed),
    ``control_failed`` (an action or release a node rejected or never got)
    and ``infeasible`` (the strategy's solver found no cut, or the applied cut
    did not verify).
    """
    if incidents is None:
        incidents = operator_incidents(decisions)
    responses, detector = _responses(decisions, incidents)
    row: dict[str, Any] = {
        "n_decisions": sum(1 for d in responses if d["type"] == "decision"),
        "n_resolves": sum(1 for d in responses if d["type"] == "resolve"),
        "n_detector_decisions": detector,
        **control_accounting(responses),
    }
    latest: dict[str, Mapping[str, Any]] = {}
    released: dict[str, Mapping[str, Any]] = {}
    for d in responses:
        for record in _records(d):
            latest[_action_label(record["action"])] = record
        for record in d.get("released", ()):
            released[_action_label(record["action"])] = record
    states = [r["state"] for r in latest.values()]
    release_states = [r["state"] for r in released.values()]
    confirmed_all = [
        r["t_confirmed"]
        for r in latest.values()
        if r.get("t_confirmed") is not None and r["state"] not in CONTROL_FAILURES
    ]
    row.update(
        n_actions=len(latest),
        n_phase1=sum(1 for r in latest.values() if r["phase"] == 1),
        n_released=sum(1 for s in release_states if s == "released"),
        n_failed_actions=sum(1 for s in states if s in CONTROL_FAILURES),
        n_unconfirmed_actions=sum(1 for s in states if s == "unconfirmed"),
        unconfirmed=int(
            "unconfirmed" in states
            or "release_unconfirmed" in release_states
            or any(d.get("status") == "phase2_unconfirmed" for d in responses)
        ),
        control_failed=int("failed" in states or "release_failed" in release_states),
        first_action_s=(min(confirmed_all) - t_alert_ms) / 1000
        if confirmed_all
        else NAN,
        last_action_s=(max(confirmed_all) - t_alert_ms) / 1000
        if confirmed_all
        else NAN,
    )
    first = next((d for d in responses if _records(d)), None)
    first = first or next((d for d in responses if d["type"] == "decision"), None)
    if first is None:
        row.update(
            decision_status="none",
            root_basis="",
            roots="[]",
            ranking="[]",
            t_received_s=NAN,
            decision_s=NAN,
            established=0,
            time_to_established_s=NAN,
            t_established_ms=None,
            infeasible=0,
            solve_ms=NAN,
            solver_ms=NAN,
            gc_nodes=NAN,
            gc_edges=NAN,
            unavoidable=NAN,
            cut_valid=NAN,
        )
        return row
    records = _records(first)
    decided = [r["t_decision"] for r in records]
    established = bool(records) and all(
        r["state"] not in CONTROL_FAILURES and r.get("t_confirmed") is not None
        for r in records
    )
    t_established = max(r["t_confirmed"] for r in records) if established else None
    solve = first.get("solve") or {}
    graph = first.get("graph") or {}
    opened = next((d for d in responses if d["type"] == "decision"), first)
    received = next(d["t_received"] for d in responses if "t_received" in d)
    row.update(
        decision_status=first.get("status"),
        root_basis=opened.get("root_basis", ""),
        roots=json.dumps(list(opened.get("roots") or [])),
        ranking=json.dumps(
            [
                c.get("node")
                for c in opened.get("ranking") or []
                if isinstance(c, Mapping)
            ]
        ),
        t_received_s=(received - t_alert_ms) / 1000,
        decision_s=(min(decided) - t_alert_ms) / 1000 if decided else NAN,
        established=int(established),
        time_to_established_s=(t_established - t_alert_ms) / 1000
        if t_established is not None
        else NAN,
        t_established_ms=t_established,
        # The strategy could not produce a valid cut: its solver found none,
        # or the cut it applied did not verify. (``unavoidable`` sinks alone
        # are not infeasibility: they include potential future flows.)
        infeasible=int(
            str(solve.get("status") or "") == "infeasible"
            or first.get("cut_valid") is False
            or any(d.get("status") == "phase2_check_failed" for d in responses)
        ),
        solve_ms=solve.get("ms", NAN),
        solver_ms=solve.get("solver_ms", NAN),
        gc_nodes=graph.get("nodes", NAN),
        gc_edges=graph.get("edges", NAN),
        unavoidable=len(first.get("unavoidable", ())),
        cut_valid=NAN
        if first.get("cut_valid") is None
        else int(bool(first["cut_valid"])),
    )
    return row


def admission_metrics(decisions: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Per-event admission of labelled units (the monitor's ``admission``
    records): events admitted and kept, and the latency from the hold to the
    admission at the node (node clock)."""
    admissions = [d for d in decisions if d.get("type") == "admission"]
    latency = sorted(
        float(d["latency_ms"])
        for d in admissions
        if d.get("decision") == "admit" and d.get("latency_ms") is not None
    )

    def quantile(q: float) -> float:
        return latency[min(len(latency) - 1, int(q * len(latency)))] if latency else NAN

    return {
        "n_events_admitted": sum(1 for d in admissions if d.get("decision") == "admit"),
        "n_events_kept": len(
            {d["event_id"] for d in admissions if d.get("decision") == "keep"}
            - {d["event_id"] for d in admissions if d.get("decision") == "admit"}
        ),
        "admission_latency_ms_median": quantile(0.5),
        "admission_latency_ms_p95": quantile(0.95),
    }


def _worker_clock(
    events: Iterable[Mapping[str, Any]],
) -> Callable[[Mapping[str, Any]], float]:
    """An event's time in ms: the worker's own clock (``t_local_ms``) shifted
    onto the services clock by that worker's smallest ``t_ms - t_local_ms``
    (the offset plus the fastest report), so the time a report spends on its
    way to the services, up to a second under load, does not count. Events
    without a local time keep ``t_ms``."""
    offset: dict[str, float] = {}
    for e in events:
        if e.get("t_local_ms") is not None:
            gap = float(e["t_ms"]) - float(e["t_local_ms"])
            agent = str(e.get("agent"))
            offset[agent] = min(offset.get(agent, gap), gap)

    def at(e: Mapping[str, Any]) -> float:
        if e.get("t_local_ms") is None:
            return float(e["t_ms"])
        # A dispatch counts from when it was sent (``sent_local_ms``), not
        # from when its call returned.
        local = e.get("sent_local_ms", e["t_local_ms"])
        return float(local) + offset[str(e.get("agent"))]

    return at


def _handoff_index(records: RunRecords) -> dict[tuple[str, str], float]:
    """First start of each invocation, first successful dispatch of each
    (purpose, item, target), and the start dispatch of each created task, on
    the workers' clocks (``_worker_clock``)."""
    at = _worker_clock(records.events)
    first: dict[tuple[str, str], float] = {}
    for e in records.events:
        name, kind = str(e.get("task")), e["kind"]
        if kind == "start":
            first.setdefault(("start", name), at(e))
        elif kind == "dispatch" and e.get("code") is None:
            key = f"{e.get('purpose')}:{e.get('item')}->{e.get('target')}"
            first.setdefault(("dispatch", key), at(e))
            if e.get("purpose") == "start":  # a pending creation starts its task
                first.setdefault(("started_by", str(e.get("target"))), at(e))
    return first


def _dispatches(
    first: Mapping[tuple[str, str], float],
) -> Iterator[tuple[str, str, str, float]]:
    for (what, key), t in first.items():
        if what == "dispatch":
            purpose, rest = key.split(":", 1)
            item, target = rest.split("->", 1)
            yield purpose, item, target, t


def handoff_latencies(records: RunRecords) -> list[float]:
    """The run's per-hop hand-off latencies in ms: for each message through
    the runtime, from when its dispatch was sent to the start of the
    invocation it delivered (a created task's pending creation, a created
    task, a pushed item's delivery). A dispatch recorded before workers sent
    ``sent_local_ms`` counts from its report, once its call returned.
    Spawned and rogue messages (faults) are not hand-offs.
    """
    first = _handoff_index(records)
    invocation = {
        "create": lambda item, target: f"create:{target}",
        "start": lambda item, target: target,
        "deliver": lambda item, target: f"deliver:{item}->{target}",
    }
    out = []
    for purpose, item, target, t in _dispatches(first):
        if purpose in invocation:
            arrived = first.get(("start", invocation[purpose](item, target)))
            if arrived is not None:
                out.append(arrived - t)
    return sorted(out)


def task_handoff_latencies(records: RunRecords) -> list[float]:
    """Each created task's hand-off in ms, from its creator's dispatch to its
    start, without the time its pending creation waited for the task's pushed
    inputs: the two hops (to the pending creation, and from it to the task)."""
    first = _handoff_index(records)
    out = []
    for purpose, _item, target, t in _dispatches(first):
        if purpose == "create":
            arrived = first.get(("start", f"create:{target}"))
            handed = first.get(("started_by", target))
            started = first.get(("start", target))
            if arrived is not None and handed is not None and started is not None:
                out.append((arrived - t) + (started - handed))
    return sorted(out)


def _quantile(values: list[float], q: float) -> float:
    return values[min(len(values) - 1, int(q * len(values)))] if values else NAN


def handoff_metrics(records: RunRecords) -> dict[str, Any]:
    hops = handoff_latencies(records)
    tasks = task_handoff_latencies(records)
    return {
        "n_handoffs": len(hops),
        "handoff_ms_median": _quantile(hops, 0.5),
        "handoff_ms_p95": _quantile(hops, 0.95),
        "handoff_ms_max": hops[-1] if hops else NAN,
        # Every hop of the run, for pooled statistics (the lab gate).
        "handoff_ms": json.dumps(hops),
        "n_task_handoffs": len(tasks),
        "task_handoff_ms_median": _quantile(tasks, 0.5),
        "task_handoff_ms_p95": _quantile(tasks, 0.95),
        "task_handoff_ms": json.dumps(tasks),
    }


def _action_label(action: Mapping[str, Any]) -> str:
    return f"{action['op']}:{action['kind']}:{action['object_id']}@{action['node_id']}"


# ---------------------------------------------------------------------------
# Detector-triggered runs
# ---------------------------------------------------------------------------
# What agentd refuses or ends at a boundary (``coreq boundary``'s reasons): a
# dispatch or effect outside the grant or the scope the task inherited (the
# authority boundary); a lease's effect quota or wall time used up, a session
# lease lost, a deadline passed (the resource boundary). Refusals that are
# containment's own doing (holds, revocations, filters) are not violations.
VIOLATION_REASONS = frozenset(
    {
        "outside_authority_scope",
        "permission_denied",
        "outside_effect_scope",
        "lease_exceeded",
        "session_unavailable",
        "deadline_expired",
        "quota_exceeded",
        "expired",
    }
)


@dataclass(frozen=True)
class Detection:
    """A detector-triggered run's alert: the first detector alert whose
    subject is on the fault's lineage, and the first boundary violation there.

    ``t_alert_ms`` is when the monitor's response to the alert began (the
    decision's ``t_received``), the alert's time at the monitor; None when no
    detector alert reached the lineage (not detected). ``observed_ms`` (the
    alert's triggering event) and ``t_violation_ms`` are node times mapped
    onto the services' clock (``_node_offsets``); the monitor shares the
    services' host and clock.
    """

    incident_id: str | None = None
    t_alert_ms: float | None = None
    kind: str | None = None
    reason: str | None = None
    subject_task: str | None = None
    observed_ms: float | None = None
    t_violation_ms: float | None = None
    violation_reason: str | None = None
    violation_task: str | None = None
    n_violations: int = 0
    origin_attempt: str | None = None


def _node_offsets(events: Iterable[Mapping[str, Any]]) -> dict[str, float]:
    """Per node, its clock onto the services' clock: the smallest
    ``t_ms - t_local_ms`` of the events its workers reported (as
    ``_worker_clock`` per worker). A node without such events gets 0: its
    clock is taken as the services' (as for the gateway's commit times)."""
    offset: dict[str, float] = {}
    for e in events:
        if e.get("t_local_ms") is not None and e.get("node") is not None:
            gap = float(e["t_ms"]) - float(e["t_local_ms"])
            node = str(e["node"])
            offset[node] = min(offset.get(node, gap), gap)
    return offset


def _occurred_ms(value: str) -> float:
    """An event time of the trace contract (UTC ISO 8601, ms), in epoch ms."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000


def _lineage_task(invocation: tuple[str, str] | None, lineage: Set[str]) -> str | None:
    """The planned task a worker invocation ``(inv_kind, name)`` serves, if it
    is on the fault's ``lineage``. A pending creation and a delivery serve
    their target task; an undeclared (spawned or rogue) invocation exists
    only because a tainted one made it, so it is on the lineage."""
    if invocation is None:
        return None
    kind, name = invocation
    if kind in {"rogue", "rogue_msg"}:
        return name
    if kind == "create":
        name = name.removeprefix("create:")
    elif kind == "deliver":
        name = name.split("->", 1)[-1]
    return name if name in lineage else None


def detection(
    spec: Spec,
    events: Iterable[Mapping[str, Any]],
    decisions: Iterable[Mapping[str, Any]],
    boundary: Iterable[Mapping[str, Any]],
    t_end_ms: float | None = None,
) -> Detection:
    """The alert of a detector-triggered run (``spec.AlertTrigger.by_detector``).

    The fault's lineage is its declared closure from its seeds (the faulty
    invocation and every task that depends on it). A detector alert's
    subject (``inv:<attempt>`` or ``task:<runtime task>``) is mapped through
    Core's ``run/started`` records (``boundary``) to the worker invocation and
    its planned task. The violation is the lineage's first boundary record
    with a ``VIOLATION_REASONS`` reason before the run's end (later ones are
    the teardown's), or the alert's own event when that is earlier.
    """
    events = list(events)
    boundary = list(boundary)
    lineage = declared_closure(spec, fault_seeds(spec))
    invocations: dict[str, tuple[str, str]] = {}
    for e in events:
        if e.get("rt"):
            invocations.setdefault(
                str(e["rt"]), (str(e.get("inv_kind")), str(e["task"]))
            )
    started = {
        str(b["attempt_id"]): b
        for b in boundary
        if b["kind"] == "run" and b["phase"] == "started" and b.get("attempt_id")
    }
    offsets = _node_offsets(events)

    def at(node: Any, t_ms: float) -> float:
        return t_ms + offsets.get(str(node), 0.0)

    violations = sorted(
        (
            (at(b["node_id"], _occurred_ms(b["occurred_at"])), b)
            for b in boundary
            if b.get("reason") in VIOLATION_REASONS
            and _lineage_task(invocations.get(str(b.get("task_id"))), lineage)
        ),
        key=lambda pair: pair[0],
    )
    if t_end_ms is not None:
        violations = [(t, b) for t, b in violations if t <= t_end_ms]
    origin = fault_origin(spec)
    origin_attempt = next(
        (
            attempt
            for attempt, b in started.items()
            if invocations.get(str(b.get("task_id"))) == ("task", origin)
        ),
        None,
    )
    first = violations[0] if violations else None
    found = Detection(
        t_violation_ms=first[0] if first else None,
        violation_reason=first[1]["reason"] if first else None,
        violation_task=_lineage_task(
            invocations.get(str(first[1].get("task_id"))), lineage
        )
        if first
        else None,
        n_violations=len(violations),
        origin_attempt=origin_attempt,
    )
    alerts = sorted(
        (
            d
            for d in decisions
            if d.get("type") == "decision"
            and (d.get("alert") or {}).get("origin") == "detector"
        ),
        key=lambda d: float(d["t_received"]),
    )
    for d in alerts:
        alert = d["alert"]
        prefix, _, identity = str(alert.get("subject")).partition(":")
        rt, node = None, None
        if prefix == "inv" and identity in started:
            rt, node = started[identity].get("task_id"), started[identity]["node_id"]
        elif prefix == "task":
            rt = identity
        task = _lineage_task(invocations.get(str(rt)), lineage)
        if task is None:
            continue
        observed = at(node, float(alert["observed_at_ms"]))
        t_violation = (
            observed
            if found.t_violation_ms is None
            else min(observed, found.t_violation_ms)
        )
        return replace(
            found,
            incident_id=str(d["incident_id"]),
            t_alert_ms=float(d["t_received"]),
            kind=alert.get("kind"),
            reason=(alert.get("detail") or {}).get("reason"),
            subject_task=task,
            observed_ms=observed,
            t_violation_ms=t_violation,
        )
    return found


def detection_metrics(found: Detection, t0_ms: float) -> dict[str, Any]:
    """The detection's columns (times in seconds from the run's start)."""
    detected = found.t_alert_ms is not None
    return {
        "detected": int(detected),
        "detector_kind": found.kind or "",
        "detector_reason": found.reason or "",
        "detector_task": found.subject_task or "",
        "t_violation_s": _seconds(found.t_violation_ms, t0_ms),
        "violation_reason": found.violation_reason or "",
        "violation_task": found.violation_task or "",
        "n_violations": found.n_violations,
        "detector_observed_s": _seconds(found.observed_ms, t0_ms),
        # Violation to the alert at the monitor (the decision's t_received).
        "detection_latency_ms": (
            found.t_alert_ms - found.t_violation_ms
            if detected and found.t_violation_ms is not None
            else NAN
        ),
    }


def evaluate(
    spec: Spec,
    meta: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
    ledger: Iterable[Mapping[str, Any]],
    decisions: list[Mapping[str, Any]] | None = None,
    status: Mapping[str, Any] | None = None,
    gateway: Iterable[Mapping[str, Any]] = (),
    marks: Iterable[Mapping[str, Any]] = (),
    boundary: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """One row for one run. ``meta`` holds ``t0_ms``, ``t_alert_ms`` (None for a
    no-fault reference run, where every task is at risk from the start) and
    ``status`` (complete / quiescent / timeout); ``gateway`` is Core's record
    of the gateway's effects and ``marks`` the harness's request marks.

    A run whose alert (``meta["alert"]``) is left to the detectors takes its
    alert time and response incident from ``detection`` (``boundary``: Core's
    boundary records); undetected, it is scored from its start like a
    reference run, and the alert-relative outcomes are NaN.
    """
    records = read_records(spec, events, ledger)
    gateway = list(gateway or ())
    derived = derive(spec)
    t0 = float(meta["t0_ms"])
    trigger = spec.alerts.get(meta.get("alert") or "")
    found = None
    if trigger is not None and trigger.by_detector:
        found = detection(
            spec, records.events, decisions or [], boundary, meta.get("t_end_ms")
        )
        t_alert = found.t_alert_ms
        incidents = {found.incident_id} if found.incident_id else set()
    else:
        t_alert = meta.get("t_alert_ms")
        incidents = operator_incidents(decisions or [])
        if t_alert is None and spec.fault.cls != "none":
            raise ValueError("a faulted run without an alert time cannot be scored")
    horizon = t0 if t_alert is None else float(t_alert)
    seeds = fault_seeds(spec)
    early_taint = {
        t
        for t, rec in records.tasks.items()
        if rec.tainted_at is not None and rec.tainted_at <= horizon
    }
    tainted = (
        declared_closure(spec, seeds | early_taint) if seeds or early_taint else set()
    )
    normal = {t.id for t in spec.tasks} - tainted
    done = completion_times(spec, records, tainted)
    at_risk = {t for t in normal if done[t] is None or done[t] > horizon}
    correct = correct_content(spec, records, expected_contents(spec))
    kept = {t for t in at_risk if done[t] is not None and correct[t]}
    contaminated = {t for t in at_risk if records.tasks[t].tainted_at is not None}
    collateral = at_risk - kept - contaminated
    interrupted = {t for t in at_risk if records.tasks[t].pauses}

    def weight(tasks: set[str], attr: str) -> float:
        total = 0.0
        for tid in tasks:
            task = spec.task(tid)
            total += {
                "compute": task.compute,
                "gpu": task.compute * task.gpu,
                "tokens": task.tokens,
            }[attr]
        return total

    def share(part: float, whole: float) -> float:
        return part / whole if whole else NAN

    lineage = commit_lineage(
        spec, records, gateway, runtime_tasks(records.events, marks)
    )
    effects = ledger_effects(records, lineage)
    committed = [e for e in effects.values() if e["state"] == "committed"]
    classes = {c.id for c in spec.effect_classes}
    protected = protected_classes(spec)
    violations = [
        e
        for e in committed
        if e["tainted"] is not False
        and (e["class"] in protected or e["class"] not in classes)
    ]
    # Each commit's time at its own site: the gateway's commit time, carried
    # by its post to the sink or by Core's record of the same commit.
    core_commits = {
        str(g["effect_id"]): g
        for g in gateway
        if g.get("phase") == "committed" and g.get("committed_at_ms") is not None
    }
    at_site: dict[str, float] = {}
    for r in records.ledger:
        if r.get("phase") != "committed":
            continue
        post = r.get("gateway") or {}
        site_time = post.get("committed_at_ms")
        if site_time is None:
            site_time = core_commits.get(str(post.get("effect_id")), {}).get(
                "committed_at_ms"
            )
        if site_time is not None:
            at_site.setdefault(str(r["effect_id"]), float(site_time))
    violation_ids = {
        effect_id for effect_id, e in effects.items() if any(e is v for v in violations)
    }
    tau1 = tau1_ms(decisions or [], incidents) if t_alert is not None else None
    decision = (
        decision_metrics(decisions or [], horizon, incidents)
        if t_alert is not None or found is not None
        else {}
    )
    t_established = decision.get("t_established_ms")
    after_alert = [e for e in violations if e["t_committed"] > horizon]
    timed_out = meta.get("status") == "timeout"
    item = spec.fault.item
    readers = set(derived.readers.get(item, ())) if item else set()
    consumed_by_alert = {
        reader
        for t, reader in records.consumed.get(item, [])
        if reader in readers and t <= horizon
    }
    ends = [rec.end for rec in records.tasks.values() if rec.end is not None]
    row: dict[str, Any] = {
        "status": meta.get("status"),
        "timeout": int(timed_out),
        "t_alert_s": _seconds(t_alert, t0),
        # Who raised the alert: the harness at its trigger ("injected"; under
        # No containment only its time is marked) or the monitor's detectors.
        "alert_source": "detector"
        if found is not None
        else ("injected" if trigger is not None else ""),
        "detection_latency_ms": NAN,
        "consumed_at_alert": share(len(consumed_by_alert), len(readers))
        if readers
        else NAN,
        "makespan_s": _seconds(max(ends), t0) if ends else NAN,
        "n_tasks": len(spec.tasks),
        "n_normal": len(normal),
        "n_at_risk": len(at_risk),
        "n_kept": len(kept),
        "n_contaminated": len(contaminated),
        "n_collateral": len(collateral),
        "n_interrupted": len(interrupted),
        "kept_frac": share(len(kept), len(at_risk)),
        "kept_cfrac": share(weight(kept, "compute"), weight(at_risk, "compute")),
        "kept_compute_s": weight(kept, "compute"),
        "at_risk_compute_s": weight(at_risk, "compute"),
        "kept_gpu_s": weight(kept, "gpu"),
        "kept_tokens": weight(kept, "tokens"),
        "lost_compute_s": weight(at_risk, "compute") - weight(kept, "compute"),
        "redo_compute_s": sum(rec.wasted_s for rec in records.tasks.values()),
        "violations": len(after_alert),
        "violations_total": len(violations),
        # Scorer checks, each 0 in a sound run: sink commits without the
        # worker's attempted record, without a resolvable issuer, and whose
        # fault labels disagree with their lineage.
        "commits_unlabelled": sum(1 for e in committed if not e["attributed"]),
        "commits_unresolved": sum(1 for e in committed if e["tainted"] is None),
        "commits_label_mismatch": sum(
            1
            for e in committed
            if e["attributed"]
            and e["tainted"] is not None
            and bool(e["labels"]) != e["tainted"]
        ),
        "viol_declared": sum(1 for e in after_alert if e["declared"]),
        "viol_undeclared": sum(1 for e in after_alert if not e["declared"]),
        # (a) no tainted effect committed after the alert (a timed-out run's
        # observation is incomplete, so it does not count as clean)
        "clean_after_alert": int(not after_alert and not timed_out),
        # (b) tainted effects committed after containment was established
        "tainted_after_established": (
            sum(1 for e in violations if e["t_committed"] > t_established)
            if t_established is not None
            else NAN
        ),
        # ... and after tau_1 (phase 1 converged), at the commit's site
        "converged": int(tau1 is not None),
        "tau1_s": (tau1 - float(t_alert)) / 1000 if tau1 is not None else NAN,
        "tainted_after_tau1": (
            sum(
                1
                for effect_id in violation_ids
                if at_site.get(effect_id, effects[effect_id]["t_committed"]) > tau1
            )
            if tau1 is not None
            else NAN
        ),
        "n_spawned": len(records.spawned),
        "rogue_effects_committed": sum(
            1
            for e in effects.values()
            if e["origin"] == "rogue" and e["state"] == "committed"
        ),
        # Refused when asked (the worker's record) ...
        "effects_denied": sum(1 for e in effects.values() if e["state"] == "denied"),
        # ... or scheduled in the gateway and never performed (held, revoked,
        # or the run ended first), and committed but not performed.
        "effects_not_performed": sum(
            1 for e in effects.values() if e["state"] == "pending"
        ),
        "effects_sink_failed": sum(
            1 for e in effects.values() if e["state"] == "sink_failed"
        ),
        "slot_restarts": sum(1 for e in records.events if e["kind"] == "slot_restart"),
        "rpc_retries": sum(1 for e in records.events if e["kind"] == "rpc_retry"),
    }
    row.update(decision)
    row.update(admission_metrics(decisions or []))
    row.update(handoff_metrics(records))
    row.pop("t_established_ms", None)
    origin = meta.get("origin_attempt")
    if found is not None:
        row.update(detection_metrics(found, t0))
        origin = found.origin_attempt
        if t_alert is None:  # not detected: no alert to measure from
            for key in (
                "clean_after_alert",
                "violations",
                "viol_declared",
                "viol_undeclared",
                "consumed_at_alert",
            ):
                row[key] = NAN
    roots = json.loads(row.get("roots") or "[]")
    # Whether the response started from the faulty source: always when the
    # harness names it (root mode "given"), the ranking's pick otherwise (a
    # detector's alert names no root either).
    row["root_mode"] = (
        "detector" if found is not None else meta.get("root_mode", "given")
    )
    row["root_correct"] = (
        int(any(origin in str(r) for r in roots)) if origin and roots else NAN
    )
    ranking = json.loads(row.pop("ranking", None) or "[]")
    # 1-based position of the faulty invocation among the ranked candidates.
    row["root_rank"] = next(
        (i + 1 for i, node in enumerate(ranking) if origin and origin in str(node)),
        NAN,
    )
    if status:
        row["gp_nodes"] = status.get("g_p", {}).get("nodes", NAN)
        row["gp_edges"] = status.get("g_p", {}).get("edges", NAN)
    return row
