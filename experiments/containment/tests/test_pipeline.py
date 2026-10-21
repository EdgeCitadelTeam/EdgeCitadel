"""The lab chain's gate before the pilots."""

from __future__ import annotations

import json

from harness import pipeline


def row(**overrides):
    values = {
        "run_id": "c2__late__native__r0__firebreak",
        "status": "complete",
        "strategy_code": "firebreak",
        "fault": "wrong_value",
        "clean_after_alert": 1,
        "slot_restarts": 0,
        "commits_unlabelled": 0,
        "commits_unresolved": 0,
        "commits_label_mismatch": 0,
        "handoff_ms": json.dumps([20.0, 30.0, 40.0]),
        "n_errors": 0,
    }
    values.update(overrides)
    return values


def test_a_clean_part_passes_the_gate():
    verdict = pipeline.gate(
        [row(), row(run_id="ref", strategy_code="reference", fault="none")]
    )
    assert verdict["passed"] and verdict["handoff_ms_median"] == 30.0


def test_each_gate_condition_fails_on_its_own():
    cases = {
        "not complete or quiescent": row(status="timeout"),
        "commits_unresolved is not 0": row(commits_unresolved=1),
        "Firebreak not clean": row(clean_after_alert=0),
        "worker slot restarts": row(slot_restarts=4),
        "median hand-off latency": row(handoff_ms=json.dumps([150.0, 160.0])),
    }
    for reason, bad in cases.items():
        verdict = pipeline.gate([bad])
        assert not verdict["passed"]
        assert any(reason in r for r in verdict["reasons"]), (reason, verdict)
    # An uncontained or no-fault run need not be clean; slot losses add up.
    assert pipeline.gate([row(strategy_code="none", clean_after_alert=0)])["passed"]
    assert not pipeline.gate([row(slot_restarts=2), row(slot_restarts=2)])["passed"]


CONFIRMED = {"core": [18.0], "edge-1": [40.0]}


def test_a_failed_gate_stops_before_the_pilots(tmp_path, monkeypatch):
    ran = []
    monkeypatch.setattr(pipeline, "_control_latency", lambda out: CONFIRMED)

    def stage(stack_path, name, experiment, out):
        ran.append(name)
        return [row(slot_restarts=5)] if name == "svc-smoke" else [row()]

    monkeypatch.setattr(pipeline, "_stage", stage)
    assert pipeline.run_pipeline(tmp_path / "stack.json", tmp_path / "out") == 1
    assert ran == ["regression", "svc-smoke"]
    assert "slot restarts" in (tmp_path / "out" / "GATE-FAILED").read_text()
    assert json.loads((tmp_path / "out" / "gate.json").read_text())["passed"] is False

    ran.clear()
    monkeypatch.setattr(
        pipeline, "_stage", lambda *args: ran.append(args[1]) or [row()]
    )
    assert pipeline.run_pipeline(tmp_path / "stack.json", tmp_path / "out") == 0
    assert ran == ["regression", "svc-smoke", "lab-pilot", "svc-pilot"]
    assert not (tmp_path / "out" / "GATE-FAILED").exists()
    # The storage cost behind the hand-offs is recorded with the verdict.
    cost = json.loads((tmp_path / "out" / "gate.json").read_text())["commit_cost_ms"]
    assert set(cost) == {"one_db", "attached_pair"} and min(cost.values()) > 0


def test_a_hop_median_of_exactly_100_ms_passes():
    assert pipeline.gate([row(handoff_ms=json.dumps([90.0, 100.0, 110.0]))])["passed"]


def test_the_gate_reports_whether_svcs_early_alert_preceded_the_read():
    svc = row(run_id="svc__early__native__r0__none", instance="svc", alert="early")
    verdict = pipeline.gate(
        [row(), {**svc, "consumed_at_alert": 1.0}, {**svc, "alert": "late"}]
    )
    assert verdict["passed"]  # informational only
    assert verdict["svc_early_consumed_at_alert"] == {svc["run_id"]: 1.0}


def staged(monkeypatch, chain_ec, chain_a2a, *, gated_hops=(150.0,), fail=None):
    """Stages answered from memory: the gated parts with ``gated_hops``, the
    chain on each system with its own hops (``fail``: that stage raises)."""
    ran = []

    def stage(stack_path, name, experiment, out):
        ran.append((name, str(stack_path)))
        if name == fail:
            raise ValueError(f"{name} could not start")
        hops = {"handoff-ec": chain_ec, "handoff-a2a-sqlite": chain_a2a}.get(
            name, gated_hops
        )
        return [row(handoff_ms=json.dumps(list(hops)))]

    monkeypatch.setattr(pipeline, "_stage", stage)
    monkeypatch.setattr(pipeline, "_a2a_sqlite_stack", lambda path: None)
    monkeypatch.setattr(pipeline, "_control_latency", lambda out: CONFIRMED)
    return ran


def test_a_slow_hop_median_falls_back_to_the_a2a_sqlite_ratio(tmp_path, monkeypatch):
    ran = staged(monkeypatch, [110.0, 120.0, 130.0], [40.0, 50.0])
    out = tmp_path / "out"
    assert pipeline.run_pipeline(tmp_path / "ec.json", out, tmp_path / "a2a.json") == 0
    verdict = json.loads((out / "gate.json").read_text())
    assert verdict["passed"] and verdict["handoff_criterion"] == "a2a_sqlite_ratio"
    assert verdict["handoff_ms_median"] == 150.0
    assert verdict["chain_handoff_ms_median"] == {
        "edgecitadel": 120.0,
        "a2a_sqlite": 45.0,
    }
    assert verdict["chain_handoff_ratio"] == 120.0 / 45.0
    # The chain runs on each system's own stack, then the pilots.
    assert ran[2:4] == [
        ("handoff-ec", str(tmp_path / "ec.json")),
        ("handoff-a2a-sqlite", str(tmp_path / "a2a.json")),
    ]
    assert [name for name, _ in ran[4:]] == ["lab-pilot", "svc-pilot"]


def test_the_fallback_fails_above_three_times_or_without_a_measurement(
    tmp_path, monkeypatch
):
    cases = {
        "times A2A-SQLite": dict(chain_ec=[200.0], chain_a2a=[50.0]),
        "could not run: ValueError: handoff-a2a-sqlite could not start": dict(
            chain_ec=[60.0], chain_a2a=[50.0], fail="handoff-a2a-sqlite"
        ),
        "could not run: ValueError: no hand-offs": dict(chain_ec=[60.0], chain_a2a=[]),
    }
    for index, (reason, case) in enumerate(cases.items()):
        staged(monkeypatch, **case)
        out = tmp_path / f"out{index}"
        assert (
            pipeline.run_pipeline(tmp_path / "ec.json", out, tmp_path / "a.json") == 1
        )
        failed = (out / "GATE-FAILED").read_text()
        assert reason in failed and pipeline.HANDOFF_REASON not in failed, failed
    ran = staged(monkeypatch, [60.0], [50.0])
    assert pipeline.run_pipeline(tmp_path / "ec.json", tmp_path / "none") == 1
    assert (
        "no A2A-SQLite stack given" in (tmp_path / "none" / "GATE-FAILED").read_text()
    )
    assert [name for name, _ in ran] == ["regression", "svc-smoke"]


def test_other_failures_skip_the_fallback(tmp_path, monkeypatch):
    ran = staged(monkeypatch, [60.0], [50.0])
    monkeypatch.setattr(
        pipeline,
        "_stage",
        lambda stack, name, exp, out: ran.append((name, "")) or [row(slot_restarts=9)],
    )
    assert (
        pipeline.run_pipeline(tmp_path / "ec.json", tmp_path / "o", tmp_path / "a") == 1
    )
    verdict = json.loads((tmp_path / "o" / "gate.json").read_text())
    assert verdict["handoff_criterion"] == "absolute"
    assert [name for name, _ in ran] == ["regression", "svc-smoke"]


def test_only_the_sqlite_a2a_stack_can_decide_the_fallback(monkeypatch):
    import re

    import pytest

    configs = pipeline.Path(pipeline.__file__).resolve().parents[1] / "configs"
    for name in ("lab-a2a-sqlite-gate.json", "lab-a2a.json", "lab.json"):
        for var in re.findall(r"\$\{(\w+)\}", (configs / name).read_text()):
            monkeypatch.setenv(var, f"/x/{var.lower()}")
    pipeline._a2a_sqlite_stack(configs / "lab-a2a-sqlite-gate.json")
    for name in ("lab-a2a.json", "lab.json"):
        with pytest.raises(ValueError, match="SQLite task store"):
            pipeline._a2a_sqlite_stack(configs / name)


def decision(t_decided, *records, kind="decision"):
    return {
        "type": kind,
        "incident_id": "inc-1",
        "t_decided": t_decided,
        "phase1": list(records),
        "phase2": [],
        "released": [],
    }


def record(label, **sites):
    """An action record sent to ``sites`` (node: (t_sent, t_confirmed or None))."""
    return {
        "action": {"op": "hold", "kind": "task", "object_id": label, "node_id": "core"},
        "incident_id": "inc-1",
        "phase": 1,
        "state": "in_force",
        "sites": {
            node: {
                "state": "in_force" if confirmed is not None else "unconfirmed",
                "command_id": f"{label}@{node}",
                "t_sent": sent,
                "t_confirmed": confirmed,
            }
            for node, (sent, confirmed) in sites.items()
        },
    }


def write_run(stage, run_id, decisions, t_end_ms=10_000):
    run = stage / "runs" / run_id
    run.mkdir(parents=True)
    (run / "run.json").write_text(json.dumps({"run_id": run_id, "t_end_ms": t_end_ms}))
    (run / "decisions.jsonl").write_text(
        "".join(json.dumps(d) + "\n" for d in decisions)
    )


def test_control_latency_is_read_per_site_from_the_regression_decision_logs(
    tmp_path,
):
    stage = tmp_path / "regression"
    hold = record("a", core=(1000, 1018), **{"edge-1": (1000, 1082)})
    write_run(
        stage,
        "c2__early__native__r0__firebreak",
        [
            decision(1000, hold),
            # A re-solve lists the hold again: one command, counted once.
            decision(1500, hold, record("b", core=(1500, 1520)), kind="resolve"),
            # Never confirmed: no time.
            decision(1600, record("c", core=(1600, None))),
            # The monitor's answer to the teardown is not part of the run.
            decision(20_000, record("d", core=(20_000, 25_000))),
        ],
    )
    # A failed testbed attempt is kept for inspection only.
    write_run(
        stage,
        "c2__early__native__r0__firebreak.infra1",
        [decision(1000, record("e", core=(1000, 9000)))],
    )
    assert pipeline._control_latency(tmp_path) == {
        "core": [18.0, 20.0],
        "edge-1": [82.0],
    }


def test_the_gate_fails_when_a_sites_p99_is_above_L():
    fast = [float(ms) for ms in range(1, 100)] + [124.0]
    verdict = pipeline.gate([row()], {"core": fast, "edge-1": [30.0]})
    assert verdict["passed"]
    assert verdict["control_latency_ms"] == 150.0
    assert verdict["control_sent_to_confirmed_ms"]["core"] == {
        "n": 100,
        "p50": 50.0,
        "p95": 95.0,
        "p99": 99.0,
        "max": 124.0,
    }
    slow = fast[:-2] + [151.0, 300.0]
    verdict = pipeline.gate([row()], {"core": [10.0], "edge-1": slow})
    assert not verdict["passed"]
    assert verdict["reasons"] == [
        "control latency: sent-to-confirmed p99 above L = 150 ms at "
        "{'edge-1': 151.0} (ms), so L is no longer an upper bound"
    ]
    verdict = pipeline.gate([row()], {})
    assert not verdict["passed"] and "unchecked" in verdict["reasons"][0]


def test_a_control_latency_failure_skips_the_handoff_fallback(tmp_path, monkeypatch):
    ran = staged(monkeypatch, [60.0], [50.0])
    monkeypatch.setattr(pipeline, "_control_latency", lambda out: {"core": [400.0]})
    out = tmp_path / "out"
    assert pipeline.run_pipeline(tmp_path / "ec.json", out, tmp_path / "a.json") == 1
    assert [name for name, _ in ran] == ["regression", "svc-smoke"]
    failed = (out / "GATE-FAILED").read_text()
    assert pipeline.CONTROL_REASON in failed and pipeline.HANDOFF_REASON in failed


def test_every_native_lab_setting_plans_with_the_gated_L():
    from harness import runner

    assert runner.MEASURED_CONTROL_MS == pipeline.GATE["max_control_p99_ms"]
    experiments = pipeline.Path(pipeline.__file__).resolve().parents[1] / "experiments"
    native = 0
    for path in sorted(experiments.glob("*.json")):
        loaded = runner.Experiment.load(path)
        for latency in loaded.latencies:
            if latency.name == "native":
                native += 1
                assert dict(latency.control_latency_ms) == {"*": 150}, path.name
                assert not latency.control_delay_ms, path.name
                assert latency.regime(loaded.measured_rtt_ms) == "measured"
            else:
                assert latency.regime(loaded.measured_rtt_ms) == "stress", path.name
    assert native == 16
