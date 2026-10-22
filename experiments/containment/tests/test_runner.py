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


def test_planned_L_is_measured_up_to_the_control_bound_injection_up_to_the_rtt(
    tmp_path,
):
    # The native lab setting: L = 400 ms planned, nothing injected.
    loaded = experiment(
        tmp_path, latencies=[{"name": "native", "control_latency_ms": {"*": 400}}]
    )
    assert loaded.measured_control_ms == runner.MEASURED_CONTROL_MS == 400.0
    assert loaded.latencies[0].regime(loaded.measured_rtt_ms) == "measured"
    for latency in (
        {"name": "native", "control_latency_ms": {"*": 401}},
        {
            "name": "native",
            "control_latency_ms": {"*": 400},
            "control_delay_ms": {"*": 5},
        },
    ):
        with pytest.raises(ValueError, match="stress"):
            experiment(tmp_path, latencies=[latency])
    # An unset site plans with the measured bound.
    assert runner.Latency("x").per_node(["core"]) == {"core": 400.0}


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
    # The deployed profile's own phase 1 (adaptive) keeps plain ids; the
    # freeze ablation is named.
    main = [
        c for c in cells if c.alert and c.profile == "deployed" and c.mode == "adaptive"
    ]
    assert len(main) == 6 and all(c.main_design for c in main)
    assert all(c.run_id.endswith(f"__r0__{c.strategy}") for c in main if c.alert)
    frozen = [c for c in cells if c.profile == "deployed" and c.mode == "freeze"]
    assert len(frozen) == 6 and all("__r0__freeze__" in c.run_id for c in frozen)
    shared = [c for c in cells if c.profile == "shared"]
    # The shared ablation's own phase 1 is the freeze: not named a variant.
    assert all(c.mode == "freeze" and "freeze" not in c.run_id for c in shared)
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


def test_the_tainted_output_value_ablation_is_named_and_recorded(tmp_path, monkeypatch):
    """1.0 (pre-registered) keeps plain ids and an unchanged monitor
    configuration; another value is a named variant, recorded in run.json and
    passed in the monitor's settings."""
    loaded = experiment(
        tmp_path,
        repetitions=1,
        rogue_rates=[0.0],
        latencies=[{"name": "fast", "control_latency_ms": {"*": 2}}],
        alerts=["early"],
        strategies=["firebreak"],
        reference=False,
        tainted_output_values=[1.0, 0.5],
    )
    cells = runner.grid(loaded)
    assert [c.tainted_output_value for c in cells] == [1.0, 0.5]
    plain, ablated = cells
    assert plain.main_design and not ablated.main_design
    assert plain.run_id.endswith("__r0__firebreak")
    assert ablated.run_id == plain.run_id.replace("__firebreak", "__tov0.5__firebreak")
    for bad in ([1.5], [], ["half"], [True]):
        with pytest.raises(ValueError, match="tainted_output_values"):
            experiment(tmp_path, tainted_output_values=bad)

    settings = {}

    class Stack:
        def __init__(self, config, run_id, run_dir):
            self.run_id = run_id

        def prepare(self):
            return {}

        def apply_netem(self, profile):
            return False

        def up(self, *args, monitor=None, **kwargs):
            settings[self.run_id] = dict(monitor or {})
            raise runner.StackError("not starting a stack in a unit test")

        def down(self):
            return []

    monkeypatch.setattr(runner, "make_stack", Stack)
    monkeypatch.setattr(runner, "_collect", lambda *args, **kwargs: None)

    class Config:
        mode = "dryrun"
        system = "edgecitadel"
        runtime_env: dict = {}
        effect_sink = {"kind": "ledger"}
        controller_lease_ms = 30_000

        def netem_rtt_ms(self, profile):
            return 0.0

        def plan_sink(self):
            return {"adapter": "http"}

    for cell in cells:
        run_dir = runner.run_cell(Config(), loaded, cell, tmp_path / "out")  # type: ignore[arg-type]
        meta = json.loads((run_dir / "run.json").read_text())
        assert meta["tainted_output_value"] == cell.tainted_output_value
    assert "tainted_output_value" not in settings[plain.run_id]
    assert settings[ablated.run_id]["tainted_output_value"] == 0.5


def test_the_lab_grids_have_their_planned_sizes():
    root = runner.Path(__file__).resolve().parents[1] / "experiments"
    sizes = {}
    for name in ("lab-smoke", "lab-pilot", "lab-main"):
        loaded = runner.Experiment.load(root / f"{name}.json")
        cells = runner.grid(loaded)
        sizes[name] = len(cells)
        assert {c.profile for c in cells} <= {"deployed", "shared"}
        # Each profile's own phase 1 (deployed: adaptive; shared: the
        # freeze), and the deployed freeze ablations on C2 with Firebreak.
        assert {(c.profile, c.mode) for c in cells if c.main_design} <= {
            ("deployed", "adaptive"),
            ("shared", "freeze"),
        }
        ablations = [c for c in cells if c.profile == "deployed" and not c.main_design]
        assert {c.strategy for c in ablations} <= {"firebreak"}
        assert {c.phase1_labels for c in cells} == {"frontier"}
        assert not any(c.receding for c in cells)
    # 7 instances x (reference + 2 alerts x (10 strategies + none)), plus C2's
    # 2 alerts x 10 strategies under the shared profile, plus Firebreak on C2
    # under the freeze and freeze_plus_cut phase 1 (2 alerts each).
    assert sizes["lab-smoke"] == 7 * (1 + 2 * 11) + 2 * 10 + 2 * 2 == 185
    assert sizes["lab-pilot"] == 2 * sizes["lab-smoke"] == 370
    # G-Safeguard is in every grid the lab runs, under both profiles.
    for name in ("regression", "svc-smoke", "svc-pilot", "lab-smoke", "lab-pilot"):
        loaded = runner.Experiment.load(root / f"{name}.json")
        cells = runner.grid(loaded)
        assert {c.profile for c in cells if c.strategy == "gsafeguard"} == (
            {"deployed", "shared"} if name.startswith("lab-") else {"deployed"}
        )
    assert len(runner.grid(runner.Experiment.load(root / "regression.json"))) == 69


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


def test_a_run_ends_only_after_its_scheduled_effects_are_due_plus_grace():
    class Stack:
        def __init__(self, now: int) -> None:
            self.now = now

        def services(self, method: str, path: str) -> dict:
            assert (method, path) == ("GET", "/effects/pending")
            return {"due_ms": [10_000, 12_000], "t_ms": self.now}

    grace = runner.EFFECT_GRACE_MS
    assert not runner._effects_settled(Stack(12_000 + grace - 1))
    assert runner._effects_settled(Stack(12_000 + grace))


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


def test_the_root_wait_adds_nothing_when_core_knows_the_invocation(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(runner.time, "sleep", slept.append)

    class Known:
        def attempt_of(self, rt: str) -> str:
            return f"attempt-of-{rt}"

    meta: dict = {}
    assert runner._wait_for_root(Known(), "rt-1", 10.0, meta) == "attempt-of-rt-1"
    assert slept == [] and meta["alert_root_wait_ms"] == 0


def test_the_root_wait_polls_until_core_knows_it_and_is_bounded(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(runner.time, "monotonic", lambda: clock[0])

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr(runner.time, "sleep", sleep)

    class Late:
        def __init__(self, known_after_s: float) -> None:
            self.known_after_s = known_after_s

        def attempt_of(self, rt: str) -> str | None:
            return "a-1" if clock[0] >= self.known_after_s else None

    meta: dict = {}
    assert runner._wait_for_root(Late(1.0), "rt-1", 10.0, meta) == "a-1"
    assert (
        1000 <= meta["alert_root_wait_ms"] < 1000 + 1000 * runner.ATTEMPT_LOOKUP_FAST_S
    )
    clock[0], meta = 0.0, {}
    # Never known: the alert names the task after the bound.
    assert runner._wait_for_root(Late(float("inf")), "rt-1", 10.0, meta) is None
    assert (
        10_000
        <= meta["alert_root_wait_ms"]
        < 10_000 + 1000 * runner.ATTEMPT_LOOKUP_FAST_S
    )


def test_the_main_and_comparison_grids_wait_for_the_root():
    root = runner.Path(__file__).resolve().parents[1] / "experiments"
    for name in (
        "main-smoke",
        "lab-main",
        "svc-main",
        "compare-ec",
        "compare-a2a",
        "overhead",
        "compare-langgraph",
        "timing-langgraph",
        "dryrun-langgraph-c2",
    ):
        loaded = runner.Experiment.load(root / f"{name}.json")
        assert loaded.root_wait_s == 10.0, name
    # The pilot grids keep what the lab runs now (no wait).
    for name in ("regression", "svc-smoke", "lab-pilot", "svc-pilot"):
        assert runner.Experiment.load(root / f"{name}.json").root_wait_s == 0.0
    # C2 early/late x 4 strategies and svc early/late x 2, once each.
    smoke = runner.grid(runner.Experiment.load(root / "main-smoke.json"))
    assert len(smoke) == 2 * 4 + 2 * 2 == 12


def test_the_main_grids_have_the_decided_sizes_and_their_own_run_ids():
    """Decided 2026-10-22 about 13:30 London: lab-main option C, svc-main 5
    repetitions. Their run ids name the rogue rate, so they never share an id
    with a pilot-chain run."""
    root = runner.Path(__file__).resolve().parents[1] / "experiments"

    def cells(name: str) -> list:
        return runner.grid(runner.Experiment.load(root / f"{name}.json"))

    lab, svc = cells("lab-main"), cells("svc-main")
    # 7 instances x (reference + 2 alerts x 11 strategies) x 3 repetitions at
    # rogue 0, plus C2 and rand-n7-s1 at rogue 0.3; svc-pilot's 23 cells x 5.
    assert len(lab) == 7 * 23 * 3 + 2 * 23 * 3 == 621
    assert {runner.Path(c.instance).stem for c in lab if c.rogue_rate == 0.3} == {
        "c2",
        "rand-n7-s1",
    }
    assert len([c for c in lab if c.rogue_rate == 0.0]) == 483
    assert len(svc) == 23 * 5 == 115
    pilot_chain = {
        c.run_id
        for name in (
            "regression",
            "svc-smoke",
            "lab-pilot",
            "svc-pilot",
            "handoff-chain",
        )
        for c in cells(name)
    }
    main = {c.run_id for c in lab + svc}
    assert len(main) == len(lab) + len(svc) and not main & pilot_chain
    assert all("__p0" in run_id for run_id in main)
