import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import event_sha256
from test_trace_ingest import connection, deliver, encode, record  # noqa: F401
from test_trace_settlement import loss

from aggregator import trace_inspect
from aggregator.trace_restore import prepare_restore


def test_committed_views_preserve_first_raw_record_and_rejection_privacy(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    path = tmp_path / "core.db"
    deliver(connection, encode(record))
    replay = deepcopy(record)
    replay["export_generation"] = str(uuid4())
    deliver(connection, encode(replay))
    changed = deepcopy(record)
    changed["event"]["occurred_at"] = "2026-09-16T12:00:01.000Z"
    deliver(connection, encode(changed))
    rejected = deepcopy(record)
    rejected["export_seq"] = 5
    rejected["event"]["event_id"] = str(uuid4())
    rejected["event"]["private_unknown_field"] = "must-not-be-inspected"
    deliver(connection, encode(rejected))
    deliver(connection, b"private-invalid-wire")
    before = list(connection.iterdump())
    raw = trace_inspect.inspect_core(path)
    assert len(raw["records"]) == 1
    assert raw["records"][0]["event"] == record["event"]
    assert raw["records"][0]["event_sha256"] == event_sha256(record["event"])
    assert raw["coverage"] == "partial_committed_evidence"
    positions = trace_inspect.inspect_core(path, view="positions")
    assert [row["outcome"] for row in positions["records"]] == ["accepted", "duplicate"]
    conflicts = trace_inspect.inspect_core(path, view="conflicts")
    assert conflicts["records"][0]["reason"] == "export_position"
    rejection = trace_inspect.inspect_core(path, view="rejected")
    assert rejection["records"][0]["export_seq"] == 5
    combined = json.dumps([raw, positions, conflicts, rejection])
    assert (
        "must-not-be-inspected" not in combined
        and "private-invalid-wire" not in combined
    )
    assert raw["poison"][0]["reason"] == "wire"
    assert list(connection.iterdump()) == before


def test_indexed_pages_and_actual_restore_epoch_fencing(connection, record, tmp_path):  # noqa: F811
    path = tmp_path / "core.db"
    deliver(connection, encode(record))
    record["export_seq"] = 9
    deliver(connection, encode(record))
    first = trace_inspect.inspect_core(path, view="positions", limit=1)
    assert first["records"][0]["export_seq"] == 1
    cursor = first["next_ingest_seq"]
    epoch = first["collector"]["collector_epoch"]
    second = trace_inspect.inspect_core(
        path, view="positions", after=cursor, collector_epoch=epoch
    )
    assert [row["export_seq"] for row in second["records"]] == [9]
    assert second["next_ingest_seq"] is None
    restored = tmp_path / "restored.db"
    prepare_restore(path, restored)
    with pytest.raises(
        trace_inspect.InspectionError, match="^collector_epoch_changed$"
    ):
        trace_inspect.inspect_core(
            restored, view="positions", after=cursor, collector_epoch=epoch
        )
    assert trace_inspect.inspect_core(restored)["collector"]["collector_epoch"] != epoch


def test_loss_marker_retains_affected_epoch_and_exact_ranges(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    scope = {k: record[k] for k in ("node_id", "source_epoch", "export_generation")}
    writer = loss(connection, scope, [(2, 4), (7, 8)], through=8)
    report = trace_inspect.inspect_core(tmp_path / "core.db")
    event = report["records"][0]["event"]
    assert event["source_epoch"] == writer["source_epoch"]
    assert event["attributes"]["affected_source_epoch"] == scope["source_epoch"]
    assert event["attributes"]["lost_ranges"] == [
        {"first": 2, "last": 4},
        {"first": 7, "last": 8},
    ]
    assert (
        next(
            row["rows"]
            for row in report["usage"]
            if row["table_name"] == "trace_loss_ranges"
        )
        == 2
    )


def test_live_snapshot_excludes_uncommitted_writes(connection, record, tmp_path):  # noqa: F811
    connection.execute("PRAGMA journal_mode=WAL")
    deliver(connection, encode(record))
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("DELETE FROM trace_raw_events")
    try:
        assert len(trace_inspect.inspect_core(tmp_path / "core.db")["records"]) == 1
    finally:
        connection.rollback()


def test_budget_and_missing_file_fail_without_mutation(
    connection,  # noqa: F811
    tmp_path,
    monkeypatch,  # noqa: F811
):
    missing = tmp_path / "missing.db"
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_core(missing)
    assert not missing.exists()
    before = list(connection.iterdump())
    monkeypatch.setattr(
        trace_inspect, "_budget", lambda db: db.set_progress_handler(lambda: 1, 1)
    )
    with pytest.raises(trace_inspect.InspectionError, match="^inspection_unavailable$"):
        trace_inspect.inspect_core(tmp_path / "core.db")
    assert list(connection.iterdump()) == before


@pytest.mark.parametrize(
    "params",
    [{"after": 1}, {"limit": True}, {"limit": 33}, {"view": "tasks"}, {"after": -1}],
)
def test_invalid_request_rejected_before_open(tmp_path, params):
    with pytest.raises(
        trace_inspect.InspectionError, match="^invalid_inspection_request$"
    ):
        trace_inspect.inspect_core(tmp_path / "missing.db", **params)


@pytest.mark.parametrize("separated", [False, True])
def test_real_cli_and_nonzero_error(connection, record, tmp_path, separated):  # noqa: F811
    deliver(connection, encode(record))
    if separated:
        from aggregator import trace_payloads

        trace_payloads.prepare(connection)
        while not trace_payloads.migrate_batch(connection):
            pass
    root = Path(__file__).parents[2]
    env = {**os.environ, "PYTHONPATH": str(root)}
    args = [sys.executable, "-m", "aggregator.trace_inspect", str(tmp_path / "core.db")]
    result = subprocess.run(
        args, cwd=root, env=env, capture_output=True, text=True, timeout=10, check=False
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["records"][0]["event"] == record["event"]
    failed = subprocess.run(
        [*args, "--after", "1"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert failed.returncode == 1
    assert json.loads(failed.stdout) == {"error": "invalid_inspection_request"}


@pytest.mark.parametrize("view", list(trace_inspect.VIEWS))
def test_partial_backfill_is_not_reported_as_complete_accounting(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    view,  # noqa: F811
):
    from aggregator import trace_capacity, trace_store

    deliver(connection, encode(record))
    with connection:
        for table in trace_capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                connection.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        connection.execute("DROP TABLE trace_capacity_usage")
    trace_store.initialize(connection, backfill=False)
    assert (
        connection.execute("SELECT count(*) FROM trace_capacity_usage").fetchone()[0]
        == 6
    )
    before = list(connection.iterdump())
    with pytest.raises(
        trace_inspect.InspectionError, match="^core_accounting_unavailable$"
    ):
        trace_inspect.inspect_core(tmp_path / "core.db", view=view)
    assert list(connection.iterdump()) == before
    trace_store.initialize(connection)
    report = trace_inspect.inspect_core(tmp_path / "core.db", view=view)
    assert (
        next(
            x["rows"] for x in report["usage"] if x["table_name"] == "trace_raw_events"
        )
        == 1
    )


def test_legacy_complete_accounting_remains_readable_without_migration(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    from aggregator import trace_capacity

    deliver(connection, encode(record))
    with connection:
        for table in trace_capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                connection.execute(f"DROP TRIGGER {table}_backfill_{operation}")
        connection.execute("DROP TABLE trace_capacity_backfill")
    before = list(connection.iterdump())
    report = trace_inspect.inspect_core(tmp_path / "core.db")
    assert report["records"][0]["event"] == record["event"]
    assert list(connection.iterdump()) == before


@pytest.mark.parametrize("separated", [False, True])
def test_payload_decoding_releases_snapshot_before_shared_write_checkpoint(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    monkeypatch,
    separated,  # noqa: F811
):
    from aggregator import trace_payload_read, trace_payloads

    connection.execute("PRAGMA journal_mode=WAL")
    deliver(connection, encode(record))
    if separated:
        trace_payloads.prepare(connection)
        while not trace_payloads.migrate_batch(connection):
            pass
    with connection:
        connection.execute("CREATE TABLE owned_writer (value INTEGER)")
        connection.execute("INSERT INTO owned_writer VALUES(0)")
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.execute("PRAGMA busy_timeout=0")
    original = trace_payload_read.json.loads
    checkpoints = []

    def decode_after_write(value):
        with connection:
            connection.execute("UPDATE owned_writer SET value=value+1")
        checkpoints.append(
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        )
        assert checkpoints[-1][0] == 0, (
            "JSON decoding still pins the inspection snapshot"
        )
        return original(value)

    monkeypatch.setattr(trace_payload_read.json, "loads", decode_after_write)
    path = Path(connection.execute("PRAGMA database_list").fetchone()[2])
    result = trace_inspect.inspect_core(path)
    assert result["records"][0]["event"] == record["event"]
    assert len(checkpoints) == 1
