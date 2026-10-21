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


def test_a_failed_gate_stops_before_the_pilots(tmp_path, monkeypatch):
    ran = []

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
