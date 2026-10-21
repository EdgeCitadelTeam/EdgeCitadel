"""Overhead workloads are valid and stay in the workloads' slot range; the
overhead rows measure handoffs and resources from the run's own records."""

from __future__ import annotations

from harness import benchspec, deploy, overhead
from harness import spec as S


def test_benchmark_workloads_are_valid_and_light_on_slots():
    for name, make in benchspec.SHAPES.items():
        spec = S.from_dict(make())
        S.validate(spec)
        assert spec.fault.cls == "none", name
        # More slots per agent overflowed agentd's socket backlog locally.
        assert max(deploy.slots(spec, a.id) for a in spec.agents) <= 6, name


def test_a_chain_alternates_agents_and_creates_each_task_by_message():
    spec = S.from_dict(benchspec.chain("c", 4, ("core", "edge-1")))
    plan = S.compile_plan(spec, run_id="r", time_scale=1.0)
    agents = [plan["tasks"][t]["agent"] for t in sorted(plan["tasks"])]
    assert agents == ["r", "b", "r", "b", "r"]
    vias = {plan["tasks"][t]["creation"]["via"] for t in plan["tasks"]}
    assert vias == {"root", "message"}


def test_handoffs_and_resources_come_from_events_and_samples():
    spec = S.from_dict(benchspec.chain("c", 2, ("core", "edge-1")))
    plan = S.compile_plan(spec, run_id="r", time_scale=1.0)
    events = [
        {"inv_kind": "task", "task": "t000", "kind": "start", "t_ms": 1000},
        {"inv_kind": "task", "task": "t000", "kind": "produced", "t_ms": 1010},
        {"inv_kind": "task", "task": "t001", "kind": "start", "t_ms": 1030},
        {"inv_kind": "task", "task": "t001", "kind": "produced", "t_ms": 1040},
        {"inv_kind": "task", "task": "t002", "kind": "start", "t_ms": 1075},
    ]
    rows = overhead.hops(plan, overhead.task_times(events))
    assert [
        (r["task"], r["from_node"], r["to_node"], r["handoff_ms"]) for r in rows
    ] == [
        ("t001", "core", "edge-1", 20),
        ("t002", "edge-1", "core", 35),
    ]
    samples = [
        {"t_ms": 900, "groups": {"agentd-core": {"cpu_s": 1.0, "rss_kb": 2048}}},
        {"t_ms": 1000, "groups": {"agentd-core": {"cpu_s": 2.0, "rss_kb": 4096}}},
        {"t_ms": 1100, "groups": {"agentd-core": {"cpu_s": 2.5, "rss_kb": 3072}}},
    ]
    used = overhead.resources(samples, 1000, 1100)
    assert used == {"runtime": {"cpu_s": 0.5, "peak_rss_mb": 4.0}}
