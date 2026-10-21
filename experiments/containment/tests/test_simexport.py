"""Simulator <-> spec conversion: one instance, identical in both worlds.

Needs the offline simulator (``CONTAIN_SIM_ROOT``); skipped otherwise.
"""

from __future__ import annotations

import json

import pytest

from harness import simexport
from harness import spec as S
from harness.spec import Rogue

ROGUE = Rogue(rate=0.3, seed=7, p_spawn=0.5)


def same_instance(wf, fault, over, spec: S.Spec, alert: str) -> None:
    from contain.simulator import nominal_schedule, simulate

    wf2, fault2, over2 = simexport.to_sim(spec, alert)
    assert set(wf.tasks) == set(wf2.tasks)
    s1, _ = nominal_schedule(wf)
    s2, _ = nominal_schedule(wf2)
    assert all(abs(s1[t] - s2[t]) < 1e-9 for t in s1)
    for agent in wf.agents.values():
        assert (
            wf.witnesses[agent.witness].scope == wf2.witnesses[agent.witness].scope
        ), agent.id
    assert fault2.z == fault.z and fault2.origin_task == fault.origin_task
    assert fault2.t_alert == pytest.approx(fault.t_alert)
    first = simulate(wf, fault, [], 0.05, over)
    second = simulate(wf2, fault2, [], 0.05, over2)
    assert first.tstate == second.tstate
    assert first.taint_time == second.taint_time
    assert first.violations == second.violations
    assert set(first.dyn) == set(second.dyn)


def test_c2_round_trips_with_both_alert_times(sim_root):
    contain = simexport.import_contain(sim_root)
    from contain.simulator import Overreach

    spec = simexport.scenario("c2", sim_root=sim_root, rogue=ROGUE)
    spec = S.from_dict(
        json.loads(json.dumps(S.to_dict(spec)))
    )  # through the file format
    over = Overreach(0.3, seed=7, p_spawn=0.5)
    for alert in ("early", "late"):
        wf, fault = contain.scenarios.c2(alert)
        same_instance(wf, fault, over, spec, alert)
    # The late alert keeps its state: A consumed once, R1/R2 about to be in flight.
    late = spec.alerts["late"]
    assert (late.event, late.item, late.count) == ("consume", "A@v1", 1)
    assert late.delay_s == pytest.approx(5.4)
    assert spec.alerts["early"].event == "produce"


@pytest.mark.parametrize("which", ["n2", "n3", "c3"])
def test_hand_built_scenarios_round_trip(sim_root, which):
    contain = simexport.import_contain(sim_root)
    from contain.simulator import Overreach

    wf, fault = getattr(contain.scenarios, which)()
    spec = simexport.scenario(which, sim_root=sim_root, rogue=ROGUE)
    same_instance(wf, fault, Overreach(0.3, seed=7, p_spawn=0.5), spec, "sim")


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_generated_workflows_round_trip(sim_root, seed):
    contain = simexport.import_contain(sim_root)
    from contain.simulator import Overreach

    wf = contain.scenarios.random_workflow(15, 0.5, seed=seed)
    fault = contain.scenarios.random_fault(wf, 0.5, seed=seed)
    spec = simexport.export(wf, fault, rogue=ROGUE)
    same_instance(wf, fault, Overreach(0.3, seed=7, p_spawn=0.5), spec, "sim")
    assert {a.label for a in spec.agents} == set(wf.agents)
    assert all(S.AGENT_ID.fullmatch(a.id) for a in spec.agents)


def test_scope_slack_is_kept(sim_root):
    contain = simexport.import_contain(sim_root)
    wf = contain.scenarios.random_workflow(7, 0.5, seed=4, scope_slack=0.5)
    fault = contain.scenarios.random_fault(wf, 0.5, seed=4)
    spec = simexport.export(wf, fault)
    assert spec.scope_extra
    wf2, _, _ = simexport.to_sim(spec, "sim")
    for agent in wf.agents.values():
        assert wf.witnesses[agent.witness].scope == wf2.witnesses[agent.witness].scope


def test_worker_draws_are_the_simulators(sim_root):
    simexport.import_contain(sim_root)
    from contain.simulator import Overreach

    from harness._worker import content

    over = Overreach(0.4, p_eff=0.5, seed=11)
    for task in ("L1.a", "nv|TARS-L1|ovM:L2.a:3"):
        for k in range(1, 6):
            assert content.rogue_draw(11, task, k, 0.4, 0.5) == over.draw(task, k)


def test_default_placement_puts_each_team_on_its_own_edge_node(sim_root):
    spec = simexport.scenario("c2", sim_root=sim_root)
    node = {a.label: S.node_of_agent(spec, a.id) for a in spec.agents}
    assert node["KIPP"] == "core"
    assert node["TARS"] == node["TARS-L1"] == node["TARS-L2"] != node["CASE"]
    assert node["CASE"] == node["CASE-L1"] == node["CASE-L2"]
