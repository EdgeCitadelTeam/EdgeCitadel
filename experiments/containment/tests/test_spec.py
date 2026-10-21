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
