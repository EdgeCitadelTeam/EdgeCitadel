"""The paired grid, alert triggers and lab latency profiles (no runtime)."""

from __future__ import annotations

import json

import pytest
from conftest import mini_document

from harness import netem, runner
from harness import spec as S


def experiment(tmp_path, **overrides) -> runner.Experiment:
    (tmp_path / "mini.json").write_text(json.dumps(mini_document()))
    document = {
        "name": "t",
        "instances": ["mini.json"],
        "strategies": ["firebreak", "trace_and_stop", "none"],
        "alerts": ["early", "late"],
        "latencies": [
            {"name": "fast", "control_latency_ms": {"*": 50}},
            {"name": "slow", "control_latency_ms": {"*": 2000}},
        ],
        "repetitions": 2,
        "rogue_rates": [0.0, 0.3],
        **overrides,
    }
    path = tmp_path / "exp.json"
    path.write_text(json.dumps(document))
    return runner.Experiment.load(path)


def test_grid_keeps_paired_strategies_adjacent(tmp_path):
    cells = runner.grid(experiment(tmp_path))
    # 2 rogue rates x 2 latencies x 2 reps x (1 reference + 2 alerts x 3 strategies)
    assert len(cells) == 2 * 2 * 2 * 7
    block = cells[1:4]
    assert [c.strategy for c in block] == ["firebreak", "trace_and_stop", "none"]
    assert (
        len({(c.instance, c.rogue_rate, c.alert, c.latency, c.rep) for c in block}) == 1
    )
    assert len({c.run_id for c in cells}) == len(cells)


def test_unknown_strategies_are_rejected(tmp_path):
    with pytest.raises(ValueError, match="unknown strategy"):
        experiment(tmp_path, strategies=["firebreak", "kill_all"])


def test_cells_override_rogue_rate_and_drop_the_fault_for_reference(tmp_path):
    cells = runner.grid(experiment(tmp_path))
    rogue = next(c for c in cells if c.rogue_rate == 0.3 and c.strategy == "firebreak")
    assert runner.cell_spec(rogue).rogue.rate == 0.3
    reference = next(c for c in cells if c.strategy == "reference")
    spec = runner.cell_spec(reference)
    assert spec.fault.cls == "none" and not spec.alerts


def test_latency_setting_per_node():
    latency = runner.Latency("x", {"*": 100, "edge-1": 300})
    assert latency.per_node(["core", "edge-1"]) == {"core": 100.0, "edge-1": 300.0}


def feed(watch: runner.TriggerWatch, kind: str, task: str, t: int, **data) -> None:
    watch.observe({"kind": kind, "task": task, "inv_kind": "task", "t_ms": t, **data})


def test_consume_trigger_counts_distinct_readers_and_scales_its_delay(mini_spec):
    trigger = S.AlertTrigger("consume", item="X@v1", count=1, delay_s=0.5)
    watch = runner.TriggerWatch(mini_spec, trigger, t0_ms=0, time_scale=2.0)
    feed(watch, "consume", "B.t0", 4000, item="dB")
    assert watch.fire_at() is None
    feed(watch, "consume", "B.t0", 4200, item="X@v1")
    assert watch.fire_at() == 4200 + 1000  # 0.5 spec seconds at time scale 2


def test_produce_and_time_triggers(mini_spec):
    produce = runner.TriggerWatch(
        mini_spec, S.AlertTrigger("produce", item="X@v1", delay_s=0.2), 0, 1.0
    )
    feed(produce, "produce", "A.t0", 4100, item="X@v1")
    assert produce.fire_at() == 4300
    at = runner.TriggerWatch(
        mini_spec, S.AlertTrigger("at", delay_s=3.0), t0_ms=1000, time_scale=1.0
    )
    assert at.fire_at() == 4000


def test_netem_profiles_only_delay_experiment_ports():
    config = {
        "interface": {"core": "tailscale0"},
        "ports": [14222],
        "profiles": {
            "wan": {"delay_ms": 50, "jitter_ms": 5, "distribution": "normal"},
            "measured": {"delay_ms": None},
        },
    }
    [lines] = netem.commands(config, "wan").values()
    assert "netem delay 50ms 5ms distribution normal" in lines[2]
    assert "priomap " + " ".join(["1"] * 16) in lines[1]
    assert sum("u32 match ip" in line and "14222" in line for line in lines) == 2
    with pytest.raises(ValueError, match="placeholder"):
        netem.commands(config, "measured")
    assert netem.commands(config, "", clear=True) == {
        "core": ["tc qdisc del dev tailscale0 root 2>/dev/null || true"]
    }


@pytest.mark.parametrize(
    ("meta", "failed"),
    [
        ({"t0_ms": 1, "errors": [], "slot_restarts": 0}, False),
        ({"t0_ms": 1, "errors": [], "slot_restarts": 2}, True),
        ({"t0_ms": 1, "errors": ["processes exited early: ['worker-a']"]}, True),
        ({"t0_ms": None, "errors": ["StackError: NATS did not become ready"]}, True),
        ({"t0_ms": 1, "errors": ["alert trigger never fired"]}, False),
    ],
)
def test_testbed_failures_are_told_apart_from_outcomes(meta, failed):
    assert (runner.infrastructure_failure(meta) is not None) == failed
