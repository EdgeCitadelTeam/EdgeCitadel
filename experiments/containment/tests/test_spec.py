"""Spec loading, validation at the boundary, and the derived runtime realization."""

from __future__ import annotations

import copy
import json

import pytest
from conftest import mini_document

from harness import spec as S


def load(document: dict) -> S.Spec:
    return S.from_dict(json.loads(json.dumps(document)))


def test_document_round_trips_through_json(mini_spec):
    again = load(S.to_dict(mini_spec))
    assert S.to_dict(again) == S.to_dict(mini_spec)


def test_yaml_spec_loads_like_json(tmp_path, mini_spec):
    yaml = pytest.importorskip("yaml")
    path = tmp_path / "mini.yaml"
    path.write_text(yaml.safe_dump(S.to_dict(mini_spec)))
    assert S.to_dict(S.load(path)) == S.to_dict(mini_spec)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d["tasks"][0].update(color="red"), "unknown fields"),
        (lambda d: d["agents"][1].update(id="A1"), "runtime agent identity"),
        (lambda d: d["tasks"][3].update(reads=["Y@v9"]), "unknown item"),
        (
            lambda d: d["messages"].append(
                {"id": "dB", "sender": "R.plan", "receiver": "B.n"}
            ),
            "duplicate message",
        ),
        (lambda d: d["effects"][0].update(delay=0.1), "below its class minimum"),
        (
            lambda d: d["fault"].update(item="dA", **{"class": "message_delay"}),
            "pushed item",
        ),
        (
            lambda d: d["fault"].update(item="rA", **{"class": "message_delay"}),
            "delay_s",
        ),
        (lambda d: d["alerts"]["early"].update(item="nope"), "unknown item"),
        (
            lambda d: d["alerts"]["early"].update(posted_on_receipt=True),
            "posted_on_receipt: only for a produce event with no delay",
        ),
        (
            lambda d: d["alerts"]["late"].update(posted_on_receipt=True, delay_s=0),
            "posted_on_receipt: only for a produce event with no delay",
        ),
        (lambda d: d["placement"].pop("s-b"), "no node for sites"),
        (lambda d: d["rogue"].update(rate=1.5), "probability"),
    ],
)
def test_invalid_documents_are_rejected_with_the_field(mutate, message):
    document = copy.deepcopy(mini_document())
    mutate(document)
    with pytest.raises(S.SpecError, match=message):
        load(document)


def test_a_task_nobody_creates_is_an_external_request_at_its_release():
    from harness import deploy, runner

    document = mini_document()
    document["messages"] = [m for m in document["messages"] if m["receiver"] != "B.n"]
    [task] = [t for t in document["tasks"] if t["id"] == "B.n"]
    task["release"] = 7.0
    spec = load(document)
    assert set(S.derive(spec).roots) == {"R.plan", "B.n"}
    plan = S.compile_plan(spec, run_id="run", time_scale=2.0)
    schedule = runner.request_schedule(spec, plan, "run", 1000, 2.0)
    entries = {e["task"]: (node, e) for node, es in schedule.items() for e in es}
    node, entry = entries["B.n"]
    assert (node, entry["recipient"], entry["at_ms"]) == ("edge-2", "b1", 15_000)
    assert entry["body"]["kind"] == "task" and entry["body"]["t0_ms"] == 1000
    assert entries["R.plan"][1]["at_ms"] == 1000
    assert deploy.external_recipients(spec) == {"core": ["r"], "edge-2": ["b1"]}


def test_dependency_cycles_are_rejected():
    document = mini_document()
    document["tasks"][1]["reads"] = [
        "rB"
    ]  # A.t0 would wait for B.t0, which reads A.t0's X
    document["messages"].append({"id": "rB2", "sender": "B.t0", "receiver": "A.t0"})
    with pytest.raises(S.SpecError, match="cycle|unknown item"):
        load(document)


def test_creation_and_deliveries_follow_the_realization_rule(mini_spec):
    derived = S.derive(mini_spec)
    creation = {t: (c.dispatcher, c.via, c.item) for t, c in derived.creation.items()}
    assert creation == {
        "R.plan": (None, "root", None),
        "A.t0": ("R.plan", "message", "dA"),
        "A.t1": ("A.t0", "session", "session:A.t0"),
        "B.t0": ("R.plan", "message", "dB"),
        "B.n": ("R.plan", "message", "dBn"),
        "R.agg": ("R.plan", "session", "session:R.plan"),
    }
    pushed = {(d.item, d.source, d.target) for d in derived.deliveries}
    assert pushed == {
        ("X@v1", "A.t0", "B.t0"),
        ("rA", "A.t1", "R.agg"),
        ("rB", "B.t0", "R.agg"),
        ("rBn", "B.n", "R.agg"),
    }
    assert derived.roots == ("R.plan",)


def test_scopes_are_least_privilege_and_closed_upward(mini_spec):
    derived = S.derive(mini_spec)
    assert derived.scope_agents["a1"] == ("b1", "r")  # X to b1, rA to r
    assert derived.scope_agents["b1"] == ("r",)
    assert derived.scope_agents["r"] == ("a1", "b1", "r")
    assert derived.grants["a1"] == ("a1", "b1", "r")  # plus the session self-dispatch
    assert derived.scope_classes["b1"] == ("act_b",)
    assert derived.effect_grant["r"] == ("act_b", "final")


def test_scope_order_follows_simulator_labels():
    document = mini_document()
    for agent, label in (("a1", "Zeta"), ("b1", "Alpha")):
        next(a for a in document["agents"] if a["id"] == agent)["label"] = label
    derived = S.derive(load(document))
    # The simulator sorts its own ids: "Alpha" < "Zeta" < "r" (not a1 < b1 < r).
    assert derived.scope_agents["r"] == ("b1", "a1", "r")


def test_nominal_schedule_uses_latencies_and_session_order(mini_spec):
    start, end = S.nominal_schedule(mini_spec)
    assert start["A.t0"] == pytest.approx(1.1)
    assert start["A.t1"] == pytest.approx(4.1)
    assert start["B.t0"] == pytest.approx(4.1)  # X at 4.1 (read latency 0), dB at 1.1
    assert start["R.agg"] == pytest.approx(8.2)  # rB at 8.1 + 0.1
    assert S.makespan(mini_spec) == pytest.approx(9.7)  # FINAL at R.agg end + 0.5


def test_compiled_plan_carries_what_workers_need(mini_spec):
    plan = S.compile_plan(mini_spec, run_id="r1", time_scale=2.0)
    b = plan["tasks"]["B.t0"]
    assert b["deliveries_in"] == [{"item": "X@v1", "source": "A.t0"}]
    assert b["effects"] == [
        {"id": "E_B", "class": "act_b", "delay": 1.0, "target": "actuator://act_b/E_B"}
    ]
    assert plan["tasks"]["R.plan"]["creates"][0]["via"] == "message"
    assert plan["agents"]["b1"] == {
        "label": "b1",
        "node": "edge-2",
        "tasks": ["B.t0", "B.n"],
        "scope_agents": ["r"],
        "scope_classes": ["act_b"],
        "shared_memory": False,
    }
    # A protected class acts as a device command at the runtime.
    assert plan["effect_classes"]["act_b"]["runtime_class"] == "device.command"
    assert plan["fault"]["origin"] == "A.t0"


def test_lease_runaway_gives_the_faulty_task_a_lease_for_its_subtree():
    spec = load(mini_document(**{"class": "lease_runaway", "origin": "B.t0"}))
    plan = S.compile_plan(spec, run_id="r1", time_scale=1.0)
    [create] = [c for c in plan["tasks"]["R.plan"]["creates"] if c["task"] == "B.t0"]
    assert create["lease"] == {"max_effects": 1}


def test_an_alert_posted_on_receipt_round_trips_and_is_left_out_by_default():
    document = copy.deepcopy(mini_document())
    document["alerts"]["early"].update(delay_s=0, posted_on_receipt=True)
    spec = load(document)
    assert spec.alerts["early"].posted_on_receipt
    assert S.to_dict(spec)["alerts"]["early"]["posted_on_receipt"] is True
    assert "posted_on_receipt" not in S.to_dict(spec)["alerts"]["late"]


def test_an_alert_left_to_the_detectors_has_no_trigger_and_round_trips():
    document = copy.deepcopy(mini_document())
    document["alerts"] = {"detector": {"event": "none"}}
    spec = load(document)
    assert spec.alerts["detector"].by_detector
    assert not load(mini_document()).alerts["early"].by_detector
    assert S.to_dict(spec)["alerts"] == {"detector": {"event": "none"}}
    for extra in ({"item": "X@v1"}, {"task": "A.t0"}, {"delay_s": 1}, {"count": 2}):
        document["alerts"] = {"detector": {"event": "none", **extra}}
        with pytest.raises(S.SpecError, match="no trigger"):
            load(document)


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        # b1 is granted act_b: attempting it is not out of scope.
        (
            {
                "class": "out_of_scope_effect",
                "origin": "B.n",
                "params": {"target_class": "act_b"},
            },
            "inside the origin's scope",
        ),
        # The harness submits R.plan: no dispatch, so no lease to exceed.
        ({"class": "lease_runaway", "origin": "R.plan"}, "created task"),
        # a1 has no protected class to actuate: the runaway would do nothing.
        ({"class": "lease_runaway", "origin": "A.t1"}, "no protected class"),
    ],
)
def test_boundary_faults_must_reach_their_boundary(fault, message):
    with pytest.raises(S.SpecError, match=message):
        load(mini_document(**fault))


def test_the_detector_workloads_reach_their_boundaries():
    """The detect-boundary grid's workloads: alert left to the detectors, and
    each fault reaches its boundary (validated on load)."""
    root = S.Path(__file__).resolve().parents[1] / "workloads"
    for name, cls in (
        ("c2-oos", "out_of_scope_effect"),
        ("rand-n7-s1-oos", "out_of_scope_effect"),
        ("c2-runaway", "lease_runaway"),
        ("rand-n7-s1-runaway", "lease_runaway"),
    ):
        spec = S.load(root / f"{name}.json")
        assert spec.fault.cls == cls and list(spec.alerts) == ["detector"]
        assert spec.alerts["detector"].by_detector
        if cls == "lease_runaway":
            # Its lease fits exactly the declared effects of the work it creates.
            origin = spec.fault.origin
            plan = S.compile_plan(spec, run_id="r", time_scale=2.0)
            [create] = [
                c
                for t in plan["tasks"].values()
                for c in t["creates"]
                if c["task"] == origin
            ]
            assert create["lease"] == {
                "max_effects": S._subtree_effects(S.derive(spec), origin)
            }


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        # b1 may hand off to r: that dispatch is inside its grant.
        (
            {
                "class": "out_of_scope_dispatch",
                "origin": "B.t0",
                "params": {"target_agent": "r"},
            },
            "inside the origin's grant",
        ),
        (
            {
                "class": "out_of_scope_dispatch",
                "origin": "B.t0",
                "params": {"target_agent": "nobody"},
            },
            "must name an agent",
        ),
        ({"class": "deadline_runaway", "origin": "R.plan"}, "created task"),
        (
            {
                "class": "deadline_runaway",
                "origin": "B.t0",
                "params": {"deadline_factor": 3.0, "overrun_factor": 2.0},
            },
            "deadline_factor < overrun_factor",
        ),
        (
            {
                "class": "deadline_runaway",
                "origin": "B.t0",
                "params": {"deadline_factor": 0.8},
            },
            "deadline_factor < overrun_factor",
        ),
    ],
)
def test_dispatch_and_deadline_faults_must_reach_their_boundary(fault, message):
    with pytest.raises(S.SpecError, match=message):
        load(mini_document(**fault))


def test_a_deadline_runaway_gets_its_deadline_from_its_dispatcher():
    spec = load(
        mini_document(
            **{
                "class": "deadline_runaway",
                "origin": "B.t0",
                "params": {"deadline_factor": 2.0},
            }
        )
    )
    plan = S.compile_plan(spec, run_id="r1", time_scale=1.0)
    creates = {c["task"]: c for c in plan["tasks"]["R.plan"]["creates"]}
    assert creates["B.t0"]["deadline_s"] == 2.0 * 4.0
    assert "deadline_s" not in creates["B.n"] and "lease" not in creates["B.t0"]
