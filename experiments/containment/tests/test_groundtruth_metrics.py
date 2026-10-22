"""Ground truth from the spec and metrics from a run's records (no runtime).

The records are what the services would hold for the mini workload with a
wrong value in X@v1 (produced by A.t0): A.t1 and B.t0 are tainted, R.plan and
B.n are normal work, and the alert comes after B.t0 consumed X.
"""

from __future__ import annotations

import json
import math

import pytest
from conftest import Records, mini_document

from harness import groundtruth as G
from harness import metrics as M
from harness import spec as S

FAULT = "fault:wrong_value:X@v1"
ALERT = 6000


@pytest.fixture
def run(mini_spec) -> Records:
    r = Records(mini_spec)
    r.compute("R.plan", 0)
    r.produce("R.plan", 1000)
    r.event("end", "R.plan", 1000)
    r.compute("A.t0", 1100, (FAULT,))
    r.produce("A.t0", 4100, (FAULT,))
    r.event("end", "A.t0", 4100)
    r.compute("B.n", 1100)
    r.compute("A.t1", 4200, (FAULT,))
    r.compute("B.t0", 4200, (FAULT,))
    r.event("consume", "B.t0", 4200, item="X@v1", labels=[FAULT])
    r.event("paused", "A.t1", 6100)
    r.event("paused", "B.t0", 6100)
    return r


def finish_b_n(r: Records, t: int = 7100) -> None:
    r.produce("B.n", t)
    r.effect("E_Bn", "B.n", t + 1000)
    r.event("end", "B.n", t + 1000)


def decision(confirmed: int = 6090, failed: bool = False) -> dict:
    def record(node: str) -> dict:
        return {
            "action": {
                "op": "hold",
                "kind": "task",
                "node_id": node,
                "object_id": f"o-{node}",
            },
            "incident_id": "i1",
            "phase": 1,
            "policy_version": 1,
            "t_alert": ALERT,
            "t_decision": 6080,
            "t_sent": 6081,
            "t_applied": 6085,
            "t_confirmed": confirmed,
            "state": "failed" if failed and node == "edge-2" else "in_force",
            "error": None,
        }

    return {
        "type": "decision",
        "incident_id": "i1",
        "alert": {"kind": "operator", "origin": "external"},
        "status": "contained",
        "t_received": 6020,
        "phase1": [record("edge-1"), record("edge-2")],
        "phase2": [],
        "released": [],
        "unavoidable": [],
        "cut_valid": True,
        "solve": {"ms": 1.5, "solver_ms": 0.4},
        "graph": {"nodes": 51, "edges": 48},
    }


def evaluate(
    r: Records, *, status: str = "quiescent", decisions=None, t_alert=ALERT
) -> dict:
    meta = {"t0_ms": 0, "t_alert_ms": t_alert, "status": status}
    return M.evaluate(
        r.spec,
        meta,
        r.events,
        r.ledger,
        [decision()] if decisions is None else decisions,
    )


# -- ground truth from the spec -------------------------------------------------------
def test_expected_contents_are_deterministic_and_fault_sensitive(mini_spec):
    first, second = G.expected_contents(mini_spec), G.expected_contents(mini_spec)
    assert first == second
    assert all(not c["labels"] for c in first.values())
    from harness._worker import content

    inputs = [first["dB"], first["X@v1"]]
    assert content.item_content("rB", inputs, ())["value"] == first["rB"]["value"]
    assert content.item_content("rB", inputs, [FAULT])["value"] != first["rB"]["value"]


def test_declared_closure_follows_creation_pushes_and_sessions(mini_spec):
    assert G.declared_closure(mini_spec, {"A.t0"}) == {"A.t0", "A.t1", "B.t0", "R.agg"}
    assert G.declared_closure(mini_spec, {"B.n"}) == {"B.n", "R.agg"}


@pytest.mark.parametrize(
    ("fault", "seeds"),
    [
        ({"class": "wrong_value", "item": "X@v1"}, {"A.t0"}),
        ({"class": "lease_runaway", "origin": "B.t0"}, {"B.t0"}),
        ({"class": "message_duplicate", "item": "rB"}, {"R.agg"}),
        ({"class": "message_delay", "item": "rB", "params": {"delay_s": 2}}, set()),
        (
            {
                "class": "message_delay",
                "item": "rB",
                "params": {"delay_s": 2, "stale_after_s": 1},
            },
            {"R.agg"},
        ),
        ({"class": "none"}, set()),
    ],
)
def test_fault_seeds_per_class(fault, seeds):
    assert G.fault_seeds(S.from_dict(mini_document(**fault))) == seeds


# -- metrics ---------------------------------------------------------------------------
def test_contained_run_keeps_the_independent_work(run):
    finish_b_n(run)
    row = evaluate(run)
    assert (row["n_normal"], row["n_at_risk"], row["n_kept"]) == (
        2,
        1,
        1,
    )  # R.plan done before the alert
    assert (
        row["kept_cfrac"] == 1.0
        and row["n_collateral"] == 0
        and row["n_interrupted"] == 0
    )
    assert row["violations"] == 0 and row["clean_after_alert"] == 1
    assert row["established"] == 1 and row["tainted_after_established"] == 0
    assert row["time_to_established_s"] == pytest.approx(0.09)
    assert row["decision_s"] == pytest.approx(0.08)
    assert row["consumed_at_alert"] == 1.0
    assert row["n_actions"] == 2 and row["gc_nodes"] == 51


def test_holding_normal_work_is_collateral(run):
    run.event("paused", "B.n", 6100)
    row = evaluate(run)
    assert (
        row["n_at_risk"],
        row["n_kept"],
        row["n_collateral"],
        row["n_interrupted"],
    ) == (1, 0, 1, 1)
    assert row["kept_cfrac"] == 0.0


def refuse(r: Records, task: str, target: str, code: str = "ingress_filtered") -> None:
    hits = [
        e
        for e in r.events
        if e["kind"] == "dispatch" and e["task"] == task and e.get("target") == target
    ]
    assert hits, (task, target)
    for e in hits:
        e["code"] = code


def test_a_refused_hand_off_into_the_tainted_closure_does_not_lose_the_work(run):
    """Amendment of 2026-10-22: R.agg is in the tainted closure, so refusing
    B.n's push to it (as an admission revocation does) keeps B.n's work."""
    finish_b_n(run)
    refuse(run, "B.n", "R.agg")
    row = evaluate(run)
    assert (row["n_at_risk"], row["n_kept"], row["n_collateral"]) == (1, 1, 0)
    assert row["kept_cfrac"] == 1.0


def test_a_refused_hand_off_to_normal_work_still_loses_the_producer(mini_spec):
    """B.n is normal: R.plan's refused push to it leaves R.plan incomplete."""
    r = Records(mini_spec)
    r.compute("R.plan", 0)
    r.produce("R.plan", 1000)
    r.event("end", "R.plan", 1000)
    refuse(r, "R.plan", "B.n")
    done = M.completion_times(mini_spec, G.read_records(mini_spec, r.events, r.ledger))
    assert done["R.plan"] is None


def test_paused_then_resumed_work_is_kept_but_counted_as_interrupted(run):
    run.event("paused", "B.n", 6100)
    run.event("resumed", "B.n", 6500)
    finish_b_n(run, 9000)
    row = evaluate(run)
    assert (row["n_kept"], row["n_interrupted"], row["n_collateral"]) == (1, 1, 0)


def test_tainted_effect_after_the_alert_is_a_violation(run):
    finish_b_n(run)
    run.effect("E_B", "B.t0", 9000, (FAULT,))
    row = evaluate(run)
    assert row["violations"] == 1 and row["viol_declared"] == 1
    assert row["clean_after_alert"] == 0 and row["tainted_after_established"] == 1


def converged(tau1: int, incident: str = "i1") -> dict:
    return {"type": "converged", "incident_id": incident, "tau1": tau1, "phase": 1}


def test_tainted_after_tau1_orders_commits_at_their_site(run):
    """tau_1 is the operator incident's first converged record; a violation
    counts after it by the gateway's commit time at its site, which the sink
    may record later. Without tau_1 the run is reported not converged, and
    tainted_after_established stays as it was."""
    finish_b_n(run)
    run.effect("E_B", "B.t0", 9000, (FAULT,))
    row = evaluate(run, decisions=[decision(), converged(8000), converged(7000, "i9")])
    assert (row["converged"], row["tau1_s"], row["tainted_after_tau1"]) == (1, 2.0, 1)
    assert row["tainted_after_established"] == 1
    assert (
        evaluate(run, decisions=[decision(), converged(9500)])["tainted_after_tau1"]
        == 0
    )
    # The gateway committed at its site before tau_1; the sink heard of it later.
    for entry in run.ledger:
        if entry["effect_id"] == "E_B" and entry["phase"] == "committed":
            entry["gateway"] = {"effect_id": "g-1", "committed_at_ms": 7900}
    assert (
        evaluate(run, decisions=[decision(), converged(8000)])["tainted_after_tau1"]
        == 0
    )
    row = evaluate(run)
    assert row["converged"] == 0
    assert math.isnan(row["tau1_s"]) and math.isnan(row["tainted_after_tau1"])
    assert row["tainted_after_established"] == 1


def unattributed_commit(effect_id: str, t: int, **post) -> dict:
    """A sink commit as the services record it when the worker's attempted
    record is missing: no class, no fault labels, ``attributed: false``."""
    return {
        "run": "r1",
        "effect_id": effect_id,
        "phase": "committed",
        "sink": "http",
        "gateway": {"effect_id": f"g-{effect_id}", **post},
        "attributed": False,
        "t_ms": t,
    }


def test_a_tainted_commit_without_an_attempted_record_is_a_violation(run):
    """The sink's copy of the worker's labels is empty here; the commit's
    lineage (the gateway's runtime task -> B.t0, which consumed X@v1) still
    makes it a leak, and the missing record is counted apart."""
    finish_b_n(run)
    run.ledger.append(
        unattributed_commit(
            "E_B", 9000, task_id="rt-B.t0", target="actuator://act_b/E_B"
        )
    )
    row = evaluate(run)
    assert row["violations"] == 1 and row["clean_after_alert"] == 0
    assert row["commits_unlabelled"] == 1
    assert row["commits_unresolved"] == 0 and row["commits_label_mismatch"] == 0


def test_the_commit_lineage_joins_cores_gateway_record(run):
    """A post naming only the gateway's effect id is resolved through Core's
    record of the same commit: B.n's clean command is no violation, B.t0's is.
    Without Core's record neither issuer is known, and both count as leaks."""
    run.ledger.append(unattributed_commit("E_Bn", 9000))
    run.ledger.append(unattributed_commit("E_B", 9100))
    gateway = [
        {"phase": "committed", "effect_id": "g-E_Bn", "task_id": "rt-B.n"},
        {"phase": "committed", "effect_id": "g-E_B", "task_id": "rt-B.t0"},
    ]
    records = G.read_records(run.spec, run.events, run.ledger)
    lineage = G.commit_lineage(run.spec, records, gateway)
    assert {k: (v["issuer"], v["tainted"]) for k, v in lineage.items()} == {
        "E_Bn": ("B.n", False),
        "E_B": ("B.t0", True),
    }
    meta = {"t0_ms": 0, "t_alert_ms": ALERT, "status": "quiescent"}
    row = M.evaluate(
        run.spec, meta, run.events, run.ledger, [decision()], gateway=gateway
    )
    assert row["violations"] == 1 and row["commits_unlabelled"] == 2
    assert row["commits_unresolved"] == 0
    unjoined = M.evaluate(run.spec, meta, run.events, run.ledger, [decision()])
    assert unjoined["commits_unresolved"] == 2 and unjoined["violations"] == 2


def test_tainted_effect_before_the_alert_is_not_a_violation(run):
    finish_b_n(run)
    run.effect("E_B", "B.t0", 5000, (FAULT,))
    row = evaluate(run)
    assert (
        row["violations_total"] == 1
        and row["violations"] == 0
        and row["clean_after_alert"] == 1
    )


def test_a_timeout_stays_in_the_denominator_as_not_contained(run):
    finish_b_n(run)
    row = evaluate(run, status="timeout")
    assert row["timeout"] == 1 and row["clean_after_alert"] == 0


def test_a_failed_control_action_leaves_containment_time_undefined(run):
    finish_b_n(run)
    row = evaluate(run, decisions=[decision(failed=True)])
    assert row["n_failed_actions"] == 1 and row["control_failed"] == 1
    assert row["established"] == 0 and math.isnan(row["time_to_established_s"])
    assert math.isnan(row["tainted_after_established"])


def test_an_unconfirmed_action_is_a_control_failure(run):
    finish_b_n(run)
    unanswered = decision()
    unanswered["phase1"][1].update(state="unconfirmed", t_confirmed=None)
    row = evaluate(run, decisions=[unanswered])
    assert row["n_failed_actions"] == 1 and row["n_unconfirmed_actions"] == 1
    assert row["unconfirmed"] == 1 and row["control_failed"] == 0
    assert row["established"] == 0 and math.isnan(row["time_to_established_s"])
    assert row["first_action_s"] == pytest.approx(0.09)  # the confirmed one


def test_a_detector_incident_is_not_the_strategys_response(run):
    finish_b_n(run)
    detector = {
        **decision(confirmed=4090),
        "incident_id": "i0",
        "alert": {"kind": "lease_violation", "origin": "detector"},
        "t_received": 4000,
    }
    row = evaluate(run, decisions=[detector, decision()])
    assert row["n_detector_decisions"] == 1 and row["n_decisions"] == 1
    assert row["time_to_established_s"] == pytest.approx(0.09)
    alone = evaluate(run, decisions=[detector])
    assert alone["decision_status"] == "none" and alone["n_actions"] == 0


def test_undeclared_contamination_before_the_alert_removes_work_from_normal(run):
    run.event("polluted", "B.n", 5000, labels=[FAULT])
    finish_b_n(run)
    row = evaluate(run)
    assert row["n_normal"] == 1  # only R.plan; B.n and its dependents are tainted


def test_contamination_after_the_alert_is_counted_against_kept_work(run):
    run.event("polluted", "B.n", 7000, labels=[FAULT])
    finish_b_n(run)
    row = evaluate(run)
    assert (row["n_at_risk"], row["n_kept"], row["n_contaminated"]) == (1, 0, 1)


def test_reference_run_counts_every_task_as_at_risk(mini_spec):
    spec = S.from_dict(mini_document(**{"class": "none"}))
    r = Records(spec)
    for task, start in (
        ("R.plan", 0),
        ("A.t0", 1100),
        ("B.n", 1100),
        ("A.t1", 4200),
        ("B.t0", 4200),
        ("R.agg", 8200),
    ):
        r.compute(task, start)
        r.produce(task, start + int(spec.task(task).compute * 1000))
        r.event("end", task, start + int(spec.task(task).compute * 1000) + 1000)
    r.effect("E_B", "B.t0", 9200)
    r.effect("E_Bn", "B.n", 8100)
    r.effect("FINAL", "R.agg", 9700, cls="final")
    row = M.evaluate(
        spec,
        {"t0_ms": 0, "t_alert_ms": None, "status": "complete"},
        r.events,
        r.ledger,
        [],
    )
    assert (row["n_at_risk"], row["n_kept"], row["kept_cfrac"]) == (6, 6, 1.0)


def test_control_accounting_counts_messages_per_node_and_confirmations(run):
    finish_b_n(run)
    d = decision()
    # ab174ed-style records: one revocation sent to two nodes, one unanswered
    d["phase2"] = [
        {
            **d["phase1"][0],
            "phase": 2,
            "action": {
                "op": "revoke",
                "kind": "grant",
                "node_id": "edge-1",
                "object_id": "b1>fx:act_b",
            },
            "sites": {
                "edge-1": {
                    "command_id": "c1",
                    "t_sent": 6081,
                    "t_confirmed": 6090,
                    "state": "in_force",
                },
                "edge-2": {
                    "command_id": "c2",
                    "t_sent": 6081,
                    "t_confirmed": None,
                    "state": "unconfirmed",
                },
            },
            "state": "unconfirmed",
            "t_confirmed": None,
        }
    ]
    d["phase1"][0].update(state="released", t_release_sent=6200)
    row = evaluate(run, decisions=[d])
    assert row["n_logical_actions"] == 3
    # 2 holds + 1 release + the revocation's 2 node messages
    assert row["n_control_messages"] == 5 and row["n_release_messages"] == 1
    assert row["n_confirmations"] == 4  # all but the unanswered revocation copy
    assert json.loads(row["control_messages_by_node"]) == {"edge-1": 3, "edge-2": 2}
    assert row["unconfirmed"] == 1 and row["established"] == 0


def test_infeasible_decisions_are_flagged_not_dropped(run):
    finish_b_n(run)
    row = evaluate(run, decisions=[{**decision(), "unavoidable": ["Fout:ctx:c>b1"]}])
    assert row["infeasible"] == 0 and row["unavoidable"] == 1
    no_cut = {**decision(), "solve": {"status": "infeasible", "ms": 1.0}}
    row = evaluate(run, decisions=[no_cut])
    assert row["infeasible"] == 1 and row["established"] == 1
    unverified = {**decision(), "cut_valid": False, "status": "phase2_check_failed"}
    assert evaluate(run, decisions=[unverified])["infeasible"] == 1


def test_root_correct_compares_the_monitors_roots_with_the_faulty_attempt(run):
    finish_b_n(run)
    meta = {
        "t0_ms": 0,
        "t_alert_ms": ALERT,
        "status": "quiescent",
        "origin_attempt": "a-1",
        "root_mode": "ranked",
    }
    picked = {
        **decision(),
        "roots": ["inv:a-2"],
        "root_basis": "session",
        "ranking": [{"node": "inv:a-2"}, {"node": "inv:a-1"}],
    }
    row = M.evaluate(run.spec, meta, run.events, run.ledger, [picked])
    assert row["root_mode"] == "ranked" and row["root_correct"] == 0
    assert row["root_rank"] == 2
    assert row["root_basis"] == "session"
    picked["roots"] = ["inv:a-1"]
    assert (
        M.evaluate(run.spec, meta, run.events, run.ledger, [picked])["root_correct"]
        == 1
    )


def test_handoffs_leave_out_data_waits_and_report_delays():
    """Hops of a created task (to its pending creation, then to the task) and
    of a pushed item, timed on each worker's clock: a0's clock is 1 s behind
    the services', a1's 5 s ahead, and a0's dispatch reaches the services
    400 ms late. The task's hand-off is its two hops, without the creation's
    wait for its data. The pushed item's send returned only after its
    delivery started (as A2A's blocking send does): it counts from its send."""
    skew = {"a0": -1000, "a1": 5000, "a2": 0}

    def ev(kind, agent, task, t, late=0, **data):
        return {
            "kind": kind,
            "agent": agent,
            "task": task,
            "t_ms": t + late,
            "t_local_ms": t + skew[agent],
            **data,
        }

    events = [
        ev("start", "a0", "T0", 0),
        ev(
            "dispatch",
            "a0",
            "T0",
            100,
            late=400,
            purpose="create",
            item="h1",
            target="T1",
        ),
        ev("end", "a0", "T0", 120),
        ev("start", "a1", "create:T1", 140),
        # the creation waits for T1's pushed inputs until 200
        ev("dispatch", "a1", "create:T1", 200, purpose="start", item="h1", target="T1"),
        ev("start", "a1", "T1", 230),
        ev("start", "a2", "deliver:X->T2", 310),
        # sent at 300, its blocking call returned after the delivery started
        ev(
            "dispatch",
            "a0",
            "T0",
            335,
            purpose="deliver",
            item="X",
            target="T2",
            sent_local_ms=300 + skew["a0"],
        ),
        # a refused dispatch is no hand-off
        ev(
            "dispatch",
            "a0",
            "T0",
            320,
            purpose="deliver",
            item="Y",
            target="T2",
            code="denied",
        ),
    ]
    records = G.RunRecords(tasks={}, ledger=[], spawned={}, consumed={}, events=events)
    assert M.handoff_latencies(records) == [10.0, 30.0, 40.0]
    assert M.task_handoff_latencies(records) == [70.0]
    row = M.handoff_metrics(records)
    assert row["n_handoffs"] == 3 and row["handoff_ms_median"] == 30.0
    assert row["handoff_ms_max"] == 40.0
    assert json.loads(row["handoff_ms"]) == [10.0, 30.0, 40.0]
    assert row["n_task_handoffs"] == 1 and row["task_handoff_ms_median"] == 70.0


# -- detector-triggered runs -------------------------------------------------------------
OOS = "fault:out_of_scope_effect:A.t0"


def iso(ms: int) -> str:
    from datetime import UTC, datetime

    moment = datetime.fromtimestamp(ms / 1000, UTC)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def boundary(kind: str, phase: str, reason, task: str, node: str, ms: int) -> dict:
    return {
        "kind": kind,
        "phase": phase,
        "reason": reason,
        "node_id": node,
        "agent_id": None,
        "task_id": f"rt-{task}",
        "attempt_id": f"att-{task}",
        "occurred_at": iso(ms),
    }


def detector_decision(incident: str, subject: str, received: int, observed: int):
    return {
        "type": "decision",
        "incident_id": incident,
        "alert": {
            "kind": "admission_denied",
            "origin": "detector",
            "subject": subject,
            "observed_at_ms": observed,
            "detail": {"reason": "outside_authority_scope"},
        },
        "status": "contained",
        "t_received": received,
        "roots": [subject],
        "phase1": [
            {
                "action": {
                    "op": "hold",
                    "kind": "task",
                    "node_id": "edge-1",
                    "object_id": incident,
                },
                "incident_id": incident,
                "phase": 1,
                "t_decision": received + 60,
                "t_sent": received + 61,
                "t_confirmed": received + 90,
                "state": "in_force",
            }
        ],
        "phase2": [],
        "released": [],
    }


@pytest.fixture
def detector_run() -> Records:
    """A.t0 attempts an effect class it was never granted; no alert is
    injected. Its worker on edge-1 reports 10 ms after its own clock."""
    document = mini_document(
        **{
            "class": "out_of_scope_effect",
            "origin": "A.t0",
            "params": {"target_class": "act_b"},
        }
    )
    document["alerts"] = {"detector": {"event": "none"}}
    r = Records(S.from_dict(document))
    r.compute("R.plan", 0)
    r.produce("R.plan", 1000)
    r.event("end", "R.plan", 1000)
    r.compute("A.t0", 1100, (OOS,))
    r.event("fault", "A.t0", 1110, node="edge-1", t_local_ms=1100)
    r.produce("A.t0", 4100, (OOS,))
    r.event("end", "A.t0", 4100)
    r.compute("B.n", 1100)
    r.compute("A.t1", 4200, (OOS,))
    finish_b_n(r)
    return r


DETECTOR_BOUNDARY = [
    boundary("run", "started", None, "A.t0", "edge-1", 1100),
    boundary("run", "started", None, "B.n", "edge-2", 1100),
    # Containment's own refusal is not a violation, however early.
    boundary("effect", "denied", "execution_held", "A.t0", "edge-1", 1500),
    # agentd's own lease alert repeats the denial it cites: not counted again.
    boundary("containment", "alert", "lease_exceeded", "A.t0", "edge-1", 2000),
    # A violation by normal work is not the fault's.
    boundary("dispatch", "denied", "permission_denied", "B.n", "edge-2", 1800),
    boundary("effect", "denied", "outside_effect_scope", "A.t0", "edge-1", 2100),
    boundary("effect", "denied", "outside_effect_scope", "A.t0", "edge-1", 4000),
    # The teardown's interrupted session, after the run's end.
    boundary("run", "failed", "session_unavailable", "A.t1", "edge-1", 30_000),
]


def evaluate_detector(r: Records, decisions: list[dict]) -> dict:
    meta = {
        "t0_ms": 0,
        "t_alert_ms": None,
        "t_end_ms": 20_000,
        "status": "complete",
        "alert": "detector",
    }
    return M.evaluate(
        r.spec, meta, r.events, r.ledger, decisions, boundary=DETECTOR_BOUNDARY
    )


def test_a_detector_alert_on_the_lineage_starts_the_response(detector_run):
    """The first detector alert whose subject is on the fault's lineage is the
    alert: its receipt at the monitor is the alert time, every time is
    measured from it, and the latency runs from the first violation."""
    decisions = [
        detector_decision("i0", "inv:att-B.n", 1900, 1800),  # normal work's
        detector_decision("i1", "inv:att-A.t0", 2600, 2300),
        converged(3000),
    ]
    row = evaluate_detector(detector_run, decisions)
    assert (row["alert_source"], row["detected"], row["root_mode"]) == (
        "detector",
        1,
        "detector",
    )
    assert (row["detector_kind"], row["detector_task"]) == ("admission_denied", "A.t0")
    assert row["t_alert_s"] == pytest.approx(2.6)
    # The first violation at edge-1's 2100 ms is 2110 on the services' clock.
    assert (row["violation_reason"], row["violation_task"]) == (
        "outside_effect_scope",
        "A.t0",
    )
    assert row["t_violation_s"] == pytest.approx(2.11) and row["n_violations"] == 2
    assert row["detector_observed_s"] == pytest.approx(2.31)
    assert row["detection_latency_ms"] == pytest.approx(490)
    assert row["decision_s"] == pytest.approx(0.06)
    assert row["time_to_established_s"] == pytest.approx(0.09)
    assert row["tau1_s"] == pytest.approx(0.4) and row["converged"] == 1
    assert (row["n_decisions"], row["n_detector_decisions"]) == (1, 1)
    assert row["root_correct"] == 1
    assert row["clean_after_alert"] == 1 and row["violations"] == 0


def test_a_run_no_detector_alert_reached_is_not_detected(detector_run):
    """Not detected: nothing is measured from an alert, the violations are
    still reported, and the run is scored from its start."""
    row = evaluate_detector(
        detector_run, [detector_decision("i0", "inv:att-B.n", 1900, 1800)]
    )
    assert (row["alert_source"], row["detected"]) == ("detector", 0)
    for key in (
        "t_alert_s",
        "detection_latency_ms",
        "clean_after_alert",
        "violations",
        "tau1_s",
        "tainted_after_tau1",
        "decision_s",
    ):
        assert math.isnan(row[key]), key
    assert row["t_violation_s"] == pytest.approx(2.11) and row["n_violations"] == 2
    assert row["decision_status"] == "none" and row["n_detector_decisions"] == 1
    assert (row["n_at_risk"], row["n_kept"]) == (2, 2)  # R.plan and B.n
    assert row["violations_total"] == 0


def test_a_detector_alert_on_a_task_counts_from_its_own_event(detector_run):
    """A subject named by its task (no attempt yet) maps directly; with no
    earlier violation the alert's own event is the violation."""
    alert = detector_decision("i1", "task:rt-A.t1", 2600, 1950)
    alert["alert"]["kind"] = "lease_violation"
    meta = {"t0_ms": 0, "t_end_ms": 20_000, "status": "complete", "alert": "detector"}
    r = detector_run
    row = M.evaluate(r.spec, meta, r.events, r.ledger, [alert], boundary=[])
    assert (row["detected"], row["detector_task"]) == (1, "A.t1")
    assert row["detection_latency_ms"] == pytest.approx(650)
    assert row["n_violations"] == 0


def test_injected_alerts_are_labelled_and_still_required(run):
    row = M.evaluate(
        run.spec,
        {"t0_ms": 0, "t_alert_ms": ALERT, "status": "quiescent", "alert": "late"},
        run.events,
        run.ledger,
        [decision()],
    )
    assert row["alert_source"] == "injected" and math.isnan(row["detection_latency_ms"])
    assert "detected" not in row
    with pytest.raises(ValueError, match="without an alert time"):
        M.evaluate(
            run.spec,
            {"t0_ms": 0, "t_alert_ms": None, "alert": "late"},
            run.events,
            run.ledger,
        )
