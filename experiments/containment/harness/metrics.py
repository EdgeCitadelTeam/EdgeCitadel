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
- **violation**: a tainted (fault-labelled) effect of a protected class that
  the ledger committed after the alert; also counted after the last
  confirmation of the first decision. **fully contained**: no violation after
  the alert and the run did not time out. Timeouts and failed control actions
  stay in every denominator.
- **time to containment**: alert to the last confirmation of the monitor's
  initial response (its first decision that issued actions; NaN when one of
  them failed or stayed unconfirmed, or none was ever issued); ``first_action_s`` / ``last_action_s``
  are the first and last confirmation over the whole incident.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .groundtruth import (
    RunRecords,
    declared_closure,
    expected_contents,
    fault_seeds,
    ledger_effects,
    protected_classes,
    read_records,
)
from .spec import Spec, derive

NAN = float("nan")
# Action states (the monitor's ActionRecord) that leave an action not in force:
# rejected or undeliverable, or sent without a confirmation (m3/integration
# ab174ed on) so it may or may not have landed.
CONTROL_FAILURES = frozenset({"failed", "unconfirmed"})


def _seconds(t_ms: float | None, t0_ms: float) -> float:
    return NAN if t_ms is None else (t_ms - t0_ms) / 1000


def completion_times(spec: Spec, records: RunRecords) -> dict[str, int | None]:
    """When each task's own work was complete (services clock), or None."""
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
            ]
            created = [
                rec.dispatches.get(("create", f"{derived.creation[c].item}->{c}"))
                for c in derived.creates.get(t.id, ())
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


def decision_metrics(
    decisions: list[Mapping[str, Any]], t_alert_ms: float
) -> dict[str, Any]:
    """Timing and size of the monitor's response, from its decision log.

    The monitor re-solves on every taint advance (``resolve`` records) for
    every strategy, so the initial response is the first record that issued
    actions; its solve, G_C size and timing describe the decision, and its
    last confirmation is when the response was in force.

    Only the incidents the harness's operator alert opened (or joined) are the
    strategy's response. Incidents the monitor's own detectors opened (for
    example a lease violation when a worker loses its session) are counted in
    ``n_detector_decisions`` and otherwise left out.
    """
    opened = [d for d in decisions if d.get("type") == "decision"]
    operator = {
        d.get("incident_id")
        for d in opened
        if (d.get("alert") or {}).get("kind") == "operator"
    }
    responses = [
        d
        for d in decisions
        if d.get("type") in {"decision", "resolve"} and d.get("incident_id") in operator
    ]
    row: dict[str, Any] = {
        "n_decisions": sum(1 for d in responses if d["type"] == "decision"),
        "n_resolves": sum(1 for d in responses if d["type"] == "resolve"),
        "n_detector_decisions": sum(
            1 for d in opened if d.get("incident_id") not in operator
        ),
    }
    actions: dict[str, Mapping[str, Any]] = {}
    failed = unconfirmed = phase1 = released = 0
    confirmed_all = []
    for d in responses:
        for record in [*d.get("phase1", ()), *d.get("phase2", ())]:
            actions.setdefault(_action_label(record["action"]), record)
            failed += record["state"] in CONTROL_FAILURES
            unconfirmed += record["state"] == "unconfirmed"
            phase1 += record["phase"] == 1
            if (
                record.get("t_confirmed") is not None
                and record["state"] not in CONTROL_FAILURES
            ):
                confirmed_all.append(record["t_confirmed"])
        released += sum(1 for r in d.get("released", ()) if r["state"] == "released")
    row.update(
        n_actions=len(actions),
        n_phase1=phase1,
        n_released=released,
        n_failed_actions=failed,
        n_unconfirmed_actions=unconfirmed,
        first_action_s=(min(confirmed_all) - t_alert_ms) / 1000
        if confirmed_all
        else NAN,
        last_action_s=(max(confirmed_all) - t_alert_ms) / 1000
        if confirmed_all
        else NAN,
    )
    first = next((d for d in responses if d.get("phase1") or d.get("phase2")), None)
    first = first or next((d for d in responses if d["type"] == "decision"), None)
    if first is None:
        row.update(
            decision_status="none",
            t_received_s=NAN,
            decision_s=NAN,
            time_to_containment_s=NAN,
            t_confirmed_ms=None,
            solve_ms=NAN,
            solver_ms=NAN,
            gc_nodes=NAN,
            gc_edges=NAN,
            unavoidable=NAN,
            cut_valid=NAN,
        )
        return row
    records = [*first.get("phase1", ()), *first.get("phase2", ())]
    decided = [r["t_decision"] for r in records]
    confirmed = [r["t_confirmed"] for r in records if r.get("t_confirmed") is not None]
    # In force only when every action was confirmed (an unconfirmed one may or
    # may not have landed, so it counts as a control failure).
    complete = records and all(
        r["state"] not in CONTROL_FAILURES and r.get("t_confirmed") is not None
        for r in records
    )
    t_confirmed = max(confirmed) if complete and confirmed else None
    solve = first.get("solve") or {}
    graph = first.get("graph") or {}
    received = next(d["t_received"] for d in responses if "t_received" in d)
    row.update(
        decision_status=first.get("status"),
        t_received_s=(received - t_alert_ms) / 1000,
        decision_s=(min(decided) - t_alert_ms) / 1000 if decided else NAN,
        time_to_containment_s=(t_confirmed - t_alert_ms) / 1000 if t_confirmed else NAN,
        t_confirmed_ms=t_confirmed,
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


def _action_label(action: Mapping[str, Any]) -> str:
    return f"{action['op']}:{action['kind']}:{action['object_id']}@{action['node_id']}"


def evaluate(
    spec: Spec,
    meta: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
    ledger: Iterable[Mapping[str, Any]],
    decisions: list[Mapping[str, Any]] | None = None,
    status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """One row for one run. ``meta`` holds ``t0_ms``, ``t_alert_ms`` (None for a
    no-fault reference run, where every task is at risk from the start) and
    ``status`` (complete / quiescent / timeout)."""
    records = read_records(spec, events, ledger)
    derived = derive(spec)
    t0 = float(meta["t0_ms"])
    t_alert = meta.get("t_alert_ms")
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
    done = completion_times(spec, records)
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

    effects = ledger_effects(records)
    classes = {c.id for c in spec.effect_classes}
    protected = protected_classes(spec)
    violations = [
        e
        for e in effects.values()
        if e["state"] == "committed"
        and e["labels"]
        and (e["class"] in protected or e["class"] not in classes)
    ]
    decision = decision_metrics(decisions or [], horizon) if t_alert is not None else {}
    t_confirmed = decision.get("t_confirmed_ms")
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
        "viol_declared": sum(1 for e in after_alert if e["declared"]),
        "viol_undeclared": sum(1 for e in after_alert if not e["declared"]),
        "violations_after_confirm": (
            sum(1 for e in violations if e["t_committed"] > t_confirmed)
            if t_confirmed is not None
            else NAN
        ),
        "contained": int(not after_alert and not timed_out),
        "n_spawned": len(records.spawned),
        "rogue_effects_committed": sum(
            1
            for e in effects.values()
            if e["origin"] == "rogue" and e["state"] == "committed"
        ),
        "effects_denied": sum(1 for e in effects.values() if e["state"] == "denied"),
        "slot_restarts": sum(1 for e in records.events if e["kind"] == "slot_restart"),
        "rpc_retries": sum(1 for e in records.events if e["kind"] == "rpc_retry"),
    }
    row["contained_after_confirm"] = (
        NAN
        if t_confirmed is None
        else int(row["violations_after_confirm"] == 0 and not timed_out)
    )
    row.update(decision)
    row.pop("t_confirmed_ms", None)
    if status:
        row["gp_nodes"] = status.get("g_p", {}).get("nodes", NAN)
        row["gp_edges"] = status.get("g_p", {}).get("edges", NAN)
    return row
