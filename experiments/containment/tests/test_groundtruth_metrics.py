"""Ground truth from the spec and metrics from a run's records (no runtime).

The records are what the services would hold for the mini workload with a
wrong value in X@v1 (produced by A.t0): A.t1 and B.t0 are tainted, R.plan and
B.n are normal work, and the alert comes after B.t0 consumed X.
"""

from __future__ import annotations

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
    assert (
        row["violations"] == 0
        and row["contained"] == 1
        and row["contained_after_confirm"] == 1
    )
    assert row["time_to_containment_s"] == pytest.approx(0.09)
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
    assert row["contained"] == 0 and row["contained_after_confirm"] == 0


def test_tainted_effect_before_the_alert_is_not_a_violation(run):
    finish_b_n(run)
    run.effect("E_B", "B.t0", 5000, (FAULT,))
    row = evaluate(run)
    assert (
        row["violations_total"] == 1
        and row["violations"] == 0
        and row["contained"] == 1
    )


def test_a_timeout_stays_in_the_denominator_as_not_contained(run):
    finish_b_n(run)
    row = evaluate(run, status="timeout")
    assert row["timeout"] == 1 and row["contained"] == 0


def test_a_failed_control_action_leaves_containment_time_undefined(run):
    finish_b_n(run)
    row = evaluate(run, decisions=[decision(failed=True)])
    assert row["n_failed_actions"] == 1
    assert math.isnan(row["time_to_containment_s"]) and math.isnan(
        row["contained_after_confirm"]
    )


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
    assert row["time_to_containment_s"] == pytest.approx(0.09)
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
