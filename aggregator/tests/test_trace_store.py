import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import TraceContractError, event_sha256

from aggregator import database, trace_store


@pytest.fixture
def record():
    fixture = (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    )
    event = json.loads(fixture.read_text())["fixtures"][0]["event"]
    return {
        "schema_version": 1,
        "node_id": event["node_id"],
        "source_epoch": event["source_epoch"],
        "export_generation": str(uuid4()),
        "export_seq": 1,
        "event_sha256": event_sha256(event),
        "event": event,
    }


@pytest.fixture
def connection(tmp_path):
    with sqlite3.connect(tmp_path / "core.db") as conn:
        trace_store.initialize(conn)
        yield conn


def ingest(connection, record):
    return trace_store.ingest(
        connection, "edgecitadel.telemetry.v1.edge-a", record, received_at_ms=1000
    )


def scalar(connection, sql):
    return connection.execute(sql).fetchone()[0]


def test_restart_redelivery_and_new_generation_share_immutable_event(
    tmp_path, record, monkeypatch
):
    path = str(tmp_path / "core.db")
    monkeypatch.setenv("DB_PATH", path)
    database.init_db(path)
    with sqlite3.connect(path) as conn:
        trace_store.initialize(conn)
        first = ingest(conn, record)
        assert first.outcome == "accepted"
    with sqlite3.connect(path) as conn:
        trace_store.initialize(conn)
        assert ingest(conn, record) == first
        record["export_generation"] = str(uuid4())
        replay = ingest(conn, record)
        assert replay.collector_epoch == first.collector_epoch
        assert replay.ingest_seq == first.ingest_seq + 1
        assert replay.outcome == "duplicate"
        assert scalar(conn, "SELECT count(*) FROM trace_raw_events") == 1
        assert scalar(conn, "SELECT count(*) FROM trace_ingest_positions") == 2
        assert scalar(conn, "SELECT count(*) FROM messages") == 0


def test_sparse_out_of_order_positions_are_not_filled(connection, record):
    record["export_seq"] = 9
    ingest(connection, record)
    record["export_seq"] = 2
    ingest(connection, record)
    assert connection.execute(
        "SELECT export_seq FROM trace_ingest_positions ORDER BY export_seq"
    ).fetchall() == [(2,), (9,)]
    assert scalar(connection, "SELECT ingest_seq FROM trace_collector") == 2


@pytest.mark.parametrize(
    "kind", ["event_identity", "source_position", "export_position"]
)
def test_conflicts_preserve_first_event_and_bound_repeated_diagnostics(
    connection, record, kind
):
    ingest(connection, record)
    original = scalar(connection, "SELECT event_json FROM trace_raw_events")
    changed = deepcopy(record)
    changed["event"]["occurred_at"] = "2026-09-16T12:00:01.000Z"
    if kind != "export_position":
        changed["export_seq"] = 2
    if kind == "source_position":
        changed["event"]["event_id"] = str(uuid4())
    changed["event_sha256"] = event_sha256(changed["event"])
    result = ingest(connection, changed)
    assert result.outcome == "conflict"
    assert ingest(connection, changed) == result
    changed["event"]["occurred_at"] = "2026-09-16T12:00:02.000Z"
    changed["event_sha256"] = event_sha256(changed["event"])
    assert ingest(connection, changed).outcome == "conflict"
    assert scalar(connection, "SELECT count(*) FROM trace_ingest_conflicts") == 1
    assert scalar(connection, "SELECT reason FROM trace_ingest_conflicts") == kind
    assert scalar(connection, "SELECT event_json FROM trace_raw_events") == original
    assert scalar(connection, "SELECT ingest_seq FROM trace_collector") == 2


@pytest.mark.parametrize(
    "table",
    [
        "trace_raw_events",
        "trace_ingest_positions",
        "trace_collector",
        "trace_ingest_conflicts",
    ],
)
def test_failure_rolls_back_event_ledger_conflict_and_cursor(connection, record, table):
    if table == "trace_ingest_conflicts":
        ingest(connection, record)
        record["export_seq"] = 2
        record["event"]["occurred_at"] = "2026-09-16T12:00:01.000Z"
        record["event_sha256"] = event_sha256(record["event"])
    before = list(connection.iterdump())
    operation = "UPDATE" if table == "trace_collector" else "INSERT"
    connection.execute(
        f"CREATE TEMP TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        ingest(connection, record)
    assert list(connection.iterdump()) == before


@pytest.mark.parametrize("invalid", ["subject", "hash", "origin", "schema"])
def test_invalid_record_never_advances_position(connection, record, invalid):
    subject = "edgecitadel.telemetry.v1.edge-a"
    if invalid == "subject":
        subject = "edgecitadel.telemetry.v1.someone-else"
    elif invalid == "hash":
        record["event_sha256"] = "0" * 64
    elif invalid == "origin":
        record["node_id"] = "someone-else"
    else:
        record["schema_version"] = 2
    with pytest.raises(TraceContractError):
        trace_store.ingest(connection, subject, record, received_at_ms=1000)
    assert scalar(connection, "SELECT ingest_seq FROM trace_collector") == 0
    assert scalar(connection, "SELECT count(*) FROM trace_ingest_positions") == 0


def test_concurrent_redelivery_commits_one_position(tmp_path, record):
    path = tmp_path / "core.db"
    with sqlite3.connect(path) as conn:
        trace_store.initialize(conn)

    def deliver(_):
        with sqlite3.connect(path) as conn:
            return ingest(conn, record)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(deliver, range(24)))
    assert len(set(results)) == 1
    with sqlite3.connect(path) as conn:
        assert scalar(conn, "SELECT ingest_seq FROM trace_collector") == 1
        assert scalar(conn, "SELECT count(*) FROM trace_raw_events") == 1
