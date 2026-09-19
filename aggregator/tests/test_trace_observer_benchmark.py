import asyncio
import json
import sys
from copy import deepcopy

import pytest

from aggregator import trace_collector
from e2e.helpers.trace_observer_benchmark import main, records, run_arm, summarize


def test_matched_real_ingestion_includes_warmup_and_restores_wiring(tmp_path):
    cohort = records(100)
    original_sqlite = trace_collector.sqlite3
    original_delivery = trace_collector.ingest_delivery
    pair = {}
    for observed in (False, True):
        name = "observed" if observed else "control"
        pair[name] = asyncio.run(
            run_arm(tmp_path / name, cohort, observed=observed, warmup=50)
        )
        assert trace_collector.sqlite3 is original_sqlite
        assert trace_collector.ingest_delivery is original_delivery
        assert pair[name]["verified_events"] == 100
        assert pair[name]["measured_events"] == 50
        assert pair[name]["exact_ack_and_callback_counts"]
    assert pair["observed"]["report_bytes"] > 0
    assert pair["control"]["report_bytes"] == 0
    summary = summarize([pair])
    assert len(summary["cpu_percent_deltas"]) == 1
    damaged = deepcopy(pair)
    damaged["observed"]["content_digest"] = "different"
    with pytest.raises(ValueError, match="unmatched"):
        summarize([damaged])


def test_ingest_failure_invalidates_arm_and_restores_wiring(tmp_path):
    cohort = records(2)
    cohort[1]["event_sha256"] = "0" * 64
    original_sqlite = trace_collector.sqlite3
    original_delivery = trace_collector.ingest_delivery
    with pytest.raises(RuntimeError, match="ingestion rejected"):
        asyncio.run(run_arm(tmp_path / "failed", cohort, observed=True, warmup=1))
    assert trace_collector.sqlite3 is original_sqlite
    assert trace_collector.ingest_delivery is original_delivery


def test_summary_preserves_negative_deltas_and_pair_population():
    common = dict(
        content_digest="same",
        measured_events=10,
        warmup_events=2,
        pragmas={"journal_mode": "wal"},
        verified_events=12,
    )
    pairs = [
        dict(
            control=dict(common, wall_ns=100, cpu_ns=100),
            observed=dict(common, wall_ns=80, cpu_ns=120),
        ),
        dict(
            control=dict(common, wall_ns=100, cpu_ns=100),
            observed=dict(common, wall_ns=110, cpu_ns=90),
        ),
    ]
    report = summarize(pairs)
    assert report["wall_percent_deltas"] == pytest.approx([-20, 10])
    assert report["wall_median_percent_delta"] == pytest.approx(-5)
    assert report["cpu_percent_deltas"] == pytest.approx([20, -10])
    assert report["cpu_median_percent_delta"] == pytest.approx(5)
    with pytest.raises(ValueError, match="no benchmark"):
        summarize([])


def test_cli_releases_owned_databases_and_refuses_existing_content(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark",
            str(tmp_path),
            "--samples",
            "50",
            "--warmup",
            "50",
            "--pairs",
            "2",
        ],
    )
    main()
    assert [p.name for p in tmp_path.iterdir()] == ["result.json"]
    original = (tmp_path / "result.json").read_bytes()
    report = json.loads(original)
    assert len(report["pairs"]) == 2
    assert all(pair["observed"]["verified_events"] == 100 for pair in report["pairs"])
    with pytest.raises(SystemExit):
        main()
    assert (tmp_path / "result.json").read_bytes() == original
