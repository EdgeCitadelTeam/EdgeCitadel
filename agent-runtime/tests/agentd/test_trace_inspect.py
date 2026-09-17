import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_trace_retention import prune, recorded  # noqa: F401

from edgecitadel_agentd import trace_inspect
from edgecitadel_agentd.trace_journal import TraceJournal


def scope_of(store):
    return tuple(
        store._connection.execute(
            "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
        ).fetchone()
    )


def test_inspection_separates_ack_from_settlement_and_never_writes(recorded):  # noqa: F811
    store = recorded
    before = list(store._connection.iterdump())
    scope = scope_of(store)
    listing = trace_inspect.inspect_source(store.path)
    assert listing["scopes"][0]["assigned_through"] == 3
    report = trace_inspect.inspect_source(store.path, scope=scope, limit=2)
    assert report["settlement"] is None
    assert report["coverage"] == "partial_local_evidence"
    assert [r["state"] for r in report["records"]] == ["pending", "broker_acked"]
    assert report["next_export_seq"] == 2
    stats = report["summary"]["by_state"]
    assert set(stats) == {"pending", "broker_acked", "core_settled"}
    assert all(
        s["positions"] == 1
        and s["retained_payload_bytes"] > 0
        and s["oldest_retained_age_ms"] >= 0
        for s in stats.values()
    )
    tail = trace_inspect.inspect_source(store.path, scope=scope, after=2)
    assert [r["export_seq"] for r in tail["records"]] == [3]
    assert tail["next_export_seq"] is None
    assert list(store._connection.iterdump()) == before


def test_retention_marker_is_inspectable_after_sparse_compaction(recorded):  # noqa: F811
    store = recorded
    scope = scope_of(store)
    prune(store)
    # Production reconcile may compact lost spool positions; retain marker evidence.
    store.reconcile()
    report = trace_inspect.inspect_source(store.path, scope=scope)
    markers = [
        r["event"]
        for r in report["records"]
        if r["event"] and r["event"]["kind"] == "coverage"
    ]
    assert len(markers) == 1
    assert markers[0]["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
    assert report["settlement"] is None
    assert report["generation"]["assigned_through"] == 4


def test_scope_pagination_and_uncommitted_records(recorded):  # noqa: F811
    store = recorded
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        TraceJournal(store._connection).initialize("edge-b")
    first = trace_inspect.inspect_source(store.path, limit=1)
    assert first["next_scope"] == list(scope_of(store))
    second = trace_inspect.inspect_source(
        store.path, after_scope=tuple(first["next_scope"]), limit=1
    )
    assert second["scopes"][0]["node_id"] == "edge-b"
    assert second["next_scope"] is None
    store._connection.execute("BEGIN IMMEDIATE")
    store._connection.execute(
        "UPDATE trace_spool SET state='broker_acked' WHERE export_seq=1"
    )
    try:
        report = trace_inspect.inspect_source(store.path, scope=scope_of(store))
        assert report["records"][0]["state"] == "pending"
    finally:
        store._connection.rollback()


def test_summary_budget_does_not_hide_record_page(recorded, monkeypatch):  # noqa: F811
    store = recorded
    original = trace_inspect._budget
    calls = 0

    def budget(db):
        nonlocal calls
        calls += 1
        if calls == 2:
            db.set_progress_handler(lambda: 1, 1)
        else:
            original(db)

    monkeypatch.setattr(trace_inspect, "_budget", budget)
    report = trace_inspect.inspect_source(store.path, scope=scope_of(store))
    assert len(report["records"]) == 3
    assert report["summary"]["state"] == "unavailable"
    assert "by_state" not in report["summary"]


def test_overall_budget_fails_with_fixed_diagnostic(recorded, monkeypatch):  # noqa: F811
    monkeypatch.setattr(
        trace_inspect, "_budget", lambda db: db.set_progress_handler(lambda: 1, 1)
    )
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_source(recorded.path)


def test_missing_database_is_not_created(tmp_path):
    path = tmp_path / "never-created.db"
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_source(path)
    assert not path.exists()


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 33},
        {"limit": True},
        {"after": 1},
        {"scope": ("a", "b", "c"), "after_scope": ("a", "b", "c")},
    ],
)
def test_invalid_request_is_rejected_before_open(tmp_path, params):
    with pytest.raises(
        trace_inspect.InspectionError, match="^invalid_inspection_request$"
    ):
        trace_inspect.inspect_source(tmp_path / "missing.db", **params)


def test_real_cli_reads_existing_database(recorded):  # noqa: F811
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "edgecitadel_agentd.trace_inspect",
            str(recorded.path),
            "--scope",
            *scope_of(recorded),
            "--limit",
            "1",
        ],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2] / "src")},
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["records"][0]["state"] == "pending"
    assert report["next_export_seq"] == 1


def test_durable_settlement_and_recovery_are_distinct(recorded):  # noqa: F811
    from test_trace_settlement_apply import reply

    from edgecitadel_agentd.trace_collector_recovery import begin_recovery
    from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request

    scope = scope_of(recorded)
    request = page_request(recorded, scope)
    response = reply(request, through=3)
    apply_page(recorded, request=request, reply=response)
    report = trace_inspect.inspect_source(recorded.path, scope=scope)
    assert report["settlement"] == {
        "collector_epoch": response["page"]["collector_epoch"],
        "applied_through": 3,
    }
    assert report["recovery"] is None
    begin_recovery(recorded, scope, expected_epoch=response["page"]["collector_epoch"])
    recovering = trace_inspect.inspect_source(recorded.path, scope=scope)
    assert recovering["settlement"] == report["settlement"]
    assert recovering["recovery"]["phase"] == "scanning"


@pytest.mark.parametrize(
    "received_ms,expected,state",
    [(1000, 1000, "observed"), (2000, 0, "observed"), (3000, None, "clock_skew")],
)
def test_queue_age_reports_future_receipt_as_unknown(
    recorded,  # noqa: F811
    monkeypatch,
    received_ms,
    expected,
    state,  # noqa: F811
):  # noqa: F811
    monkeypatch.setattr(trace_inspect.time, "time", lambda: 2.0)
    with recorded._connection:
        recorded._connection.execute(
            "UPDATE trace_journal SET received_at_ms=?", (received_ms,)
        )
    before = list(recorded._connection.iterdump())
    report = trace_inspect.inspect_source(recorded.path, scope=scope_of(recorded))
    for summary in report["summary"]["by_state"].values():
        assert summary["oldest_retained_age_ms"] == expected
        assert summary["oldest_retained_age_state"] == state
    assert list(recorded._connection.iterdump()) == before


def test_decode_releases_snapshot_but_preserves_page_and_summary(recorded, monkeypatch):  # noqa: F811
    store = recorded
    scope = scope_of(store)
    db = store._connection
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert db.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
    db.execute("PRAGMA busy_timeout=0")
    original = trace_inspect.json.loads
    decoded = 0

    def decode_after_writer(value):
        nonlocal decoded
        if decoded == 0:
            with db:
                db.execute(
                    "UPDATE trace_spool SET state='broker_acked' WHERE export_seq=1"
                )
        decoded += 1
        return original(value)

    monkeypatch.setattr(trace_inspect.json, "loads", decode_after_writer)
    report = trace_inspect.inspect_source(store.path, scope=scope)
    assert decoded == 3
    assert report["records"][0]["state"] == "pending"
    assert report["summary"]["by_state"]["pending"]["positions"] == 1
    assert (
        db.execute("SELECT state FROM trace_spool WHERE export_seq=1").fetchone()[0]
        == "broker_acked"
    )
