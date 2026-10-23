"""Scoring of collected runs, statistics and the presentation constants."""

from __future__ import annotations

import csv
import json
import math

import pytest
from conftest import Records, mini_document

from harness import names, report
from harness import spec as S


def test_names_match_the_simulators_figures(sim_root):
    from harness.simexport import import_contain

    import_contain(sim_root)
    from contain import names as sim_names

    assert names.MAIN == sim_names.MAIN
    assert set(names.CODE_NAME.values()) >= set(sim_names.MAIN_NAMES)


def test_simulator_pipeline_codes_exist(sim_root):
    from harness.simexport import import_contain

    import_contain(sim_root)
    pytest.importorskip("networkx")
    from contain.receding import registry

    registered = registry()
    for code, sim_code in names.SIM_CODE.items():
        assert sim_code in registered, code
        if code in names.MAIN_NAMES or names.CODE_NAME[code] in names.MAIN_NAMES:
            assert registered[sim_code].name == names.CODE_NAME[code]


def test_mcnemar_exact_matches_the_binomial_tail():
    assert report.mcnemar_exact(0, 0) == 1.0
    assert report.mcnemar_exact(6, 0) == pytest.approx(2 / 64)
    assert report.mcnemar_exact(3, 3) == 1.0


def test_sign_flip_detects_a_consistent_paired_difference():
    pytest.importorskip("numpy")
    assert report.sign_flip_p([10.0] * 8) == pytest.approx(2 / 256)
    assert report.sign_flip_p([1.0, -1.0] * 4) == 1.0


def write_run(
    directory, *, strategy: str, rep: int, kept: bool, slot_restarts: int = 0
) -> None:
    """A collected run of the mini workload: R.plan done before the alert, B.n
    kept (completed with its effect) or stopped."""
    spec = S.from_dict(mini_document())
    run_id = f"mini__late__native__r{rep}__{strategy}"
    run = directory / "runs" / run_id
    run.mkdir(parents=True)
    (run / "spec.json").write_text(json.dumps(S.to_dict(spec)))
    meta = {
        "run_id": run_id,
        "experiment": "t",
        "mode": "dryrun",
        "instance": "mini",
        "spec_name": "mini",
        "fault": "wrong_value",
        "rogue_rate": 0.0,
        "strategy": strategy,
        "alert": "late",
        "latency": "native",
        "control_latency_ms": {"core": 50.0},
        "rep": rep,
        "time_scale": 1.0,
        "t0_ms": 0,
        "t_alert_ms": 6000,
        "status": "quiescent",
        "errors": [],
        "slot_restarts": slot_restarts,
    }
    (run / "run.json").write_text(json.dumps(meta))
    records = Records(spec)
    records.compute("R.plan", 0)
    records.produce("R.plan", 1000)
    records.compute("B.n", 1100)
    if kept:
        records.produce("B.n", 7100)
        records.effect("E_Bn", "B.n", 8100)
    (run / "events.jsonl").write_text(
        "".join(json.dumps(e) + "\n" for e in records.events)
    )
    (run / "ledger.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in records.ledger)
    )
    (run / "decisions.jsonl").write_text("")


def test_score_writes_one_row_per_run_with_identity_and_metrics(tmp_path):
    write_run(tmp_path, strategy="firebreak", rep=0, kept=True)
    write_run(tmp_path, strategy="stop_team", rep=0, kept=False, slot_restarts=1)
    rows = report.score(tmp_path)
    by = {r["strategy_code"]: r for r in rows}
    assert by["firebreak"]["strategy"] == "Firebreak (ours)"
    # a run the testbed failed is flagged, and still scored and counted
    assert by["firebreak"]["testbed_failure"] == ""
    assert "lost agentd" in by["stop_team"]["testbed_failure"]
    assert by["stop_team"]["clean_after_alert"] in (0, 1)
    assert by["firebreak"]["family"] == names.FAMILY_FUT
    assert by["firebreak"]["kept_cfrac"] == 1.0 and by["stop_team"]["kept_cfrac"] == 0.0
    with (tmp_path / "runs.csv").open() as handle:
        header = next(csv.reader(handle))
    assert header[: len(report.IDENTITY)] == list(report.IDENTITY)


def test_report_pairs_strategies_on_matched_runs(tmp_path):
    pytest.importorskip("pandas")
    for rep in range(3):
        write_run(tmp_path, strategy="firebreak", rep=rep, kept=True)
        write_run(tmp_path, strategy="stop_team", rep=rep, kept=False)
    report.score(tmp_path)
    report.report(tmp_path, figures=False)
    import pandas as pd

    paired = pd.read_csv(tmp_path / "paired.csv")
    [row] = paired.to_dict("records")
    assert row["against"] == "Stop team" and row["pairs"] == 3
    assert row["d_kept"] == pytest.approx(100.0)
    assert "mcnemar_p_clean_after_alert" in row and "mcnemar_p_established" in row
    summary = pd.read_csv(tmp_path / "summary.csv").set_index("strategy_code")
    assert summary.loc["firebreak", "kept"] == pytest.approx(100.0)
    assert summary.loc["stop_team", "runs"] == 3
    assert not math.isnan(summary.loc["firebreak", "clean_after_alert"])
    for column in ("established", "unconfirmed_runs", "infeasible_runs", "timeouts"):
        assert column in summary.columns


def test_decisions_after_the_runs_end_are_not_scored(tmp_path):
    """The monitor answers the teardown's interrupted sessions; those decisions
    come after the run's end and stay out of its row."""
    write_run(tmp_path, strategy="firebreak", rep=0, kept=True)
    run = tmp_path / "runs" / "mini__late__native__r0__firebreak"
    meta = json.loads((run / "run.json").read_text())
    meta["t_end_ms"] = 20_000
    (run / "run.json").write_text(json.dumps(meta))
    decision = {
        "type": "decision",
        "incident_id": "i9",
        "alert": {"kind": "lease_violation", "origin": "detector"},
        "status": "contained",
        "t_received": 21_000,
        "phase1": [],
        "phase2": [],
        "released": [],
    }
    (run / "decisions.jsonl").write_text(json.dumps(decision) + "\n")
    [row] = report.score(tmp_path)
    assert row["n_decisions"] == 0


def test_the_alerts_root_wait_is_a_column(tmp_path):
    write_run(tmp_path, strategy="firebreak", rep=0, kept=True)
    write_run(tmp_path, strategy="stop_team", rep=0, kept=False)
    run = tmp_path / "runs" / "mini__late__native__r0__firebreak"
    meta = json.loads((run / "run.json").read_text())
    meta["alert_root_wait_ms"] = 0
    (run / "run.json").write_text(json.dumps(meta))
    by = {r["strategy_code"]: r for r in report.score(tmp_path)}
    assert by["firebreak"]["alert_root_wait_ms"] == 0.0
    # Not recorded (a grid without the wait): NaN, not 0.
    assert math.isnan(by["stop_team"]["alert_root_wait_ms"])


def test_a_detector_triggered_run_is_scored_without_an_injected_alert(tmp_path):
    """No harness alert time: not a failed run. With no detector alert on the
    fault's lineage it is a scored, not-detected run."""
    write_run(tmp_path, strategy="firebreak", rep=0, kept=True)
    run = tmp_path / "runs" / "mini__late__native__r0__firebreak"
    document = mini_document(
        **{
            "class": "out_of_scope_effect",
            "origin": "A.t0",
            "params": {"target_class": "act_b"},
        }
    )
    document["alerts"] = {"detector": {"event": "none"}}
    (run / "spec.json").write_text(json.dumps(document))
    meta = json.loads((run / "run.json").read_text())
    meta.update(
        alert="detector",
        root_mode="detector",
        fault="out_of_scope_effect",
        t_alert_ms=None,
        t_end_ms=20_000,
    )
    (run / "run.json").write_text(json.dumps(meta))
    [row] = report.score(tmp_path)
    assert row["status"] == "quiescent" and row["alert_source"] == "detector"
    assert row["detected"] == 0 and row["root_mode"] == "detector"
    assert math.isnan(row["clean_after_alert"]) and math.isnan(row["t_alert_s"])


def test_earliest_alert_scores_a_run_whose_injected_alert_never_fired(tmp_path):
    """Failed by default; with earliest_alert timed from the detector alert."""
    write_run(tmp_path, strategy="stop_team", rep=0, kept=False)
    run = tmp_path / "runs" / "mini__late__native__r0__stop_team"
    meta = json.loads((run / "run.json").read_text())
    meta.update(t_alert_ms=None, t_end_ms=20_000, errors=["alert trigger never fired"])
    (run / "run.json").write_text(json.dumps(meta))
    start = {"kind": "start", "task": "A.t0", "rt": "rt-A.t0", "inv_kind": "task"}
    with (run / "events.jsonl").open("a") as handle:
        handle.write(json.dumps({**start, "t_ms": 1090}) + "\n")
    alert = {
        "type": "decision",
        "incident_id": "i1",
        "alert": {
            "kind": "admission_denied",
            "origin": "detector",
            "subject": "inv:att-A.t0",
            "observed_at_ms": 2000,
        },
        "t_received": 2100,
        "ranking": [{"node": "inv:att-A.t0", "path": ["task:rt-A.t0", "inv:att-A.t0"]}],
        "phase1": [],
        "phase2": [],
    }
    (run / "decisions.jsonl").write_text(json.dumps(alert) + "\n")
    [row] = report.score(tmp_path)
    assert row["clean_after_alert"] == 0 and "t_alert_s" not in row
    [row] = report.score(tmp_path, earliest_alert=True)
    assert (row["alert_source"], row["t_alert_s"]) == ("detector", 2.1)
    assert row["detection_latency_ms"] == pytest.approx(100)
    assert math.isnan(row["t_injected_alert_s"])
