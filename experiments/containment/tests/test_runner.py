"""The paired grid, alert triggers and lab latency profiles (no runtime)."""

from __future__ import annotations

import json
from types import SimpleNamespace

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
            {"name": "fast", "control_latency_ms": {"*": 2}},
            {"name": "stress-slow", "control_latency_ms": {"*": 2000}},
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
        ({"t0_ms": 1, "errors": [], "rpc_retries": 3, "rpc_failures": 0}, False),
        ({"t0_ms": 1, "errors": [], "rpc_failures": 1}, True),
        ({"t0_ms": 1, "errors": ["processes exited early: ['worker-a']"]}, True),
        ({"t0_ms": None, "errors": ["StackError: NATS did not become ready"]}, True),
        ({"t0_ms": 1, "errors": ["alert trigger never fired"]}, True),
        ({"t0_ms": 1, "errors": [], "status": "timeout"}, False),
    ],
)
def test_testbed_failures_are_told_apart_from_outcomes(meta, failed):
    assert (runner.infrastructure_failure(meta) is not None) == failed


def test_testbed_counts_separate_lost_calls_from_refusals(tmp_path):
    events = [
        {"kind": "rpc_retry", "code": "rpc:agentd is unavailable: timed out"},
        {"kind": "dispatch", "code": "lease_exhausted"},
        {"kind": "slot_restart"},
    ]
    ledger = [
        {"phase": "denied", "code": "rpc:agentd is unavailable: timed out"},
        {"phase": "denied", "code": "rpc:effect_scope_denied"},
        {"phase": "committed"},
    ]
    for name, records in (("events.jsonl", events), ("ledger.jsonl", ledger)):
        (tmp_path / name).write_text("".join(json.dumps(r) + "\n" for r in records))
    counts = runner.testbed_counts(tmp_path)
    # the retried call's event names its transport code but is not a failure
    assert counts == {"slot_restarts": 1, "rpc_retries": 1, "rpc_failures": 1}


def test_latency_above_the_measured_round_trip_must_be_labelled_stress(tmp_path):
    with pytest.raises(ValueError, match="stress"):
        experiment(
            tmp_path,
            latencies=[{"name": "L1", "control_latency_ms": {"*": 1000}}],
        )
    loaded = experiment(tmp_path)
    regimes = [lat.regime(loaded.measured_rtt_ms) for lat in loaded.latencies]
    assert regimes == ["measured", "stress"]
    # a netem profile counts with the round trip it adds (twice its delay)
    assert runner.Latency("lan", {"*": 2}).regime(4.0, netem_rtt_ms=6.0) == "stress"


def test_netem_profiles_above_the_measured_round_trip_must_be_labelled_stress():
    netem.check_profiles(
        {"profiles": {"lan": {"delay_ms": 1}, "stress-x": {"delay_ms": 50}}}
    )
    with pytest.raises(ValueError, match="stress"):
        netem.check_profiles({"profiles": {"wan-50": {"delay_ms": 50}}})


def test_ranked_root_mode_runs_alongside_with_its_own_run_ids(tmp_path):
    cells = runner.grid(experiment(tmp_path, root_modes=["given", "ranked"]))
    ranked = [c for c in cells if c.root_mode == "ranked"]
    assert len(ranked) == len([c for c in cells if c.alert]) // 2
    assert all(".ranked__" in c.run_id for c in ranked)
    given = next(c for c in cells if c.root_mode == "given" and c.alert)
    assert ".given" not in given.run_id  # main-run ids are unchanged
    with pytest.raises(ValueError, match="root mode"):
        experiment(tmp_path, root_modes=["guess"])


def test_the_symptom_is_the_latest_reader_that_consumed_the_item(mini_spec):
    trigger = S.AlertTrigger("consume", item="X@v1", count=1, delay_s=0.0)
    watch = runner.TriggerWatch(mini_spec, trigger, t0_ms=0, time_scale=1.0)
    assert watch.symptom() is None
    watch.observe(
        {
            "kind": "consume",
            "task": "B.t0",
            "inv_kind": "task",
            "t_ms": 5,
            "item": "X@v1",
            "rt": "rt-b",
        }
    )
    assert watch.symptom() == "rt-b"


def test_new_strategies_are_accepted_by_name(tmp_path):
    loaded = experiment(
        tmp_path, strategies=["trace_and_quarantine", "sink_guard", "none"]
    )
    assert loaded.strategies == ("trace_and_quarantine", "sink_guard", "none")


def test_profiles_options_and_parts_extend_the_paired_grid(tmp_path):
    loaded = experiment(
        tmp_path,
        repetitions=1,
        rogue_rates=[0.0],
        latencies=[{"name": "fast", "control_latency_ms": {"*": 2}}],
        profiles=["deployed"],
        phase1_modes=["freeze", "adaptive"],
        # A part inherits every dimension it does not set.
        parts=[
            {
                "profiles": ["shared"],
                "phase1_modes": ["freeze"],
                "receding_during_waits": [True],
                "strategies": ["firebreak"],
                "reference": False,
            }
        ],
    )
    cells = runner.grid(loaded)
    # 1 reference + 2 modes x 2 alerts x 3 strategies, then 2 alerts x 1 shared.
    assert len(cells) == 1 + 12 + 2
    main = [c for c in cells if c.profile == "deployed" and c.phase1_mode == "freeze"]
    assert all(c.run_id.endswith(f"__r0__{c.strategy}") for c in main if c.alert)
    adaptive = [c for c in cells if c.phase1_mode == "adaptive"]
    assert len(adaptive) == 6 and all("__r0__adaptive__" in c.run_id for c in adaptive)
    shared = [c for c in cells if c.profile == "shared"]
    assert all(c.receding for c in shared)
    assert [c.run_id.rsplit("__", 3)[1:] for c in shared] == [
        ["shared", "receding", "firebreak"]
    ] * 2
    with pytest.raises(ValueError, match="profile"):
        experiment(tmp_path, profiles=["everything"])
    with pytest.raises(ValueError, match="phase-1 mode"):
        experiment(tmp_path, phase1_modes=["eager"])
    with pytest.raises(ValueError, match="phase-1 labels"):
        experiment(tmp_path, phase1_labels=["everywhere"])
    closure = runner.grid(
        experiment(tmp_path, phase1_labels=["forward_closure"], rogue_rates=[0.0])
    )
    assert all("__forward_closure__" in c.run_id for c in closure if c.alert)
    with pytest.raises(ValueError, match="repeats"):
        runner.grid(experiment(tmp_path, parts=[{"strategies": ["firebreak"]}]))


def test_the_lab_grids_have_their_planned_sizes():
    root = runner.Path(__file__).resolve().parents[1] / "experiments"
    sizes = {}
    for name in ("lab-smoke", "lab-pilot", "lab-main"):
        loaded = runner.Experiment.load(root / f"{name}.json")
        cells = runner.grid(loaded)
        sizes[name] = len(cells)
        assert {c.profile for c in cells} <= {"deployed", "shared"}
        assert {c.phase1_mode for c in cells} == {"freeze"}
        assert {c.phase1_labels for c in cells} == {"frontier"}
        assert not any(c.receding for c in cells)
    # 7 instances x (reference + 2 alerts x (9 strategies + none)), plus C2's
    # 2 alerts x 9 strategies under the shared profile.
    assert sizes["lab-smoke"] == 7 * (1 + 2 * 10) + 2 * 9
    assert sizes["lab-pilot"] == 3 * sizes["lab-smoke"]


def test_a_resumed_experiment_skips_finished_runs_and_their_retries(
    tmp_path, monkeypatch
):
    loaded = experiment(tmp_path, repetitions=1, rogue_rates=[0.0])
    cells = runner.grid(loaded)[:3]
    out = tmp_path / "out"
    # The first cell finished earlier, after a testbed retry it could not fix.
    finished = out / "runs" / cells[0].run_id
    finished.mkdir(parents=True)
    meta = {"status": "complete", "errors": [], "attempts": 2, "slot_restarts": 3}
    (finished / "run.json").write_text(json.dumps(meta))
    (out / "runs" / f"{cells[0].run_id}.infra0").mkdir()
    ran = []

    def run_cell(stack_config, experiment_, cell, out_, **_options):
        ran.append(cell.run_id)
        run_dir = out_ / "runs" / cell.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run.json").write_text(
            json.dumps({"status": "complete", "errors": [], "t0_ms": 1})
        )
        return run_dir

    monkeypatch.setattr(runner, "run_cell", run_cell)
    stack = SimpleNamespace(system="edgecitadel")
    monkeypatch.setattr(runner, "checkouts", lambda _config: {})
    done = runner.run_experiment(stack, loaded, out, limit=3)  # type: ignore[arg-type]
    assert ran == [c.run_id for c in cells[1:]]
    assert len(done) == 3
    assert json.loads((finished / "run.json").read_text())["attempts"] == 2


def test_each_system_runs_only_its_own_controls():
    runner.check_system("edgecitadel", {"firebreak", "none", "reference"})
    runner.check_system("a2a", {"a2a_cancel_agent", "a2a_block_input", "none"})
    runner.check_system("edgecitadel_bare", {"none", "reference"})
    with pytest.raises(ValueError, match="cannot run"):
        runner.check_system("a2a", {"firebreak"})
    with pytest.raises(ValueError, match="cannot run"):
        runner.check_system("edgecitadel", {"a2a_cancel_all"})
    with pytest.raises(ValueError, match="cannot run"):
        runner.check_system("edgecitadel_bare", {"stop_team"})
    runner.check_system("langgraph", {"lg_time_travel", "none", "reference"})
    runner.check_system("langgraph_functional", {"none", "reference"})
    with pytest.raises(ValueError, match="cannot run"):
        runner.check_system("langgraph_functional", {"lg_cancel_run"})
