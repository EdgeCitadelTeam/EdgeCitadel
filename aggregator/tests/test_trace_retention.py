import json
import sqlite3
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import TraceContractError, event_sha256
from test_trace_capacity import case, ingest, next_event  # noqa: F401

from aggregator import trace_capacity, trace_retention, trace_store
from aggregator.trace_inspect import inspect_core
from aggregator.trace_settlement import settlement_reply


def test_expiry_preserves_settlement_replay_conflicts_and_identity_limit(
    case,  # noqa: F811
    monkeypatch,  # noqa: F811
):  # noqa: F811
    db, record = case
    first = ingest(db, record)
    request = {
        key: record[key] for key in ("node_id", "source_epoch", "export_generation")
    }
    request.update(schema_version=1, request_id=str(uuid4()))
    settled = settlement_reply(db, request)
    boundary = 1 + trace_retention.RETENTION_MS
    assert trace_retention.expire_payloads(db, now_ms=boundary)["expired_payloads"] == 0
    assert (
        trace_retention.expire_payloads(db, now_ms=boundary + 1)["expired_payloads"]
        == 1
    )
    assert settlement_reply(db, request) == settled
    assert trace_capacity.usage(db)["trace_raw_events"] == {
        "rows": 1,
        "payload_bytes": 0,
    }
    path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    inspected = inspect_core(path)["records"][0]
    assert inspected["event"] is None and inspected["payload_state"] == "expired"
    assert inspected["event_sha256"] == record["event_sha256"]
    assert inspected["payload_expired_at_ms"] == boundary + 1
    assert ingest(db, record) == first
    assert (
        ingest(db, {**record, "export_generation": str(uuid4())}).outcome == "duplicate"
    )
    changed = deepcopy(record)
    changed["event"]["occurred_at"] = "2026-09-17T01:00:00.000Z"
    changed["event_sha256"] = event_sha256(changed["event"])
    changed["export_generation"] = str(uuid4())
    assert ingest(db, changed).outcome == "conflict"
    changed["event"]["event_id"] = str(uuid4())
    changed["event_sha256"] = event_sha256(changed["event"])
    changed["export_generation"] = str(uuid4())
    assert ingest(db, changed).outcome == "conflict"
    assert db.execute("SELECT event_json FROM trace_raw_events").fetchone()[0] == ""
    monkeypatch.setitem(trace_capacity.ROW_LIMITS, "trace_raw_events", 1)
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_capacity_exceeded"):
        ingest(db, next_event(record))
    assert list(db.iterdump()) == before


def test_bounded_scan_reopens_wraps_and_revisits_recent_prefix(case, monkeypatch):  # noqa: F811
    db, record = case
    monkeypatch.setattr(trace_retention, "SCAN_ROWS", 2)
    for _ in range(5):
        ingest(db, record)
        record = next_event(record)
    with db:
        db.execute(
            "UPDATE trace_raw_events SET received_at_ms=100000 WHERE ingest_seq<=2"
        )
    now = trace_retention.RETENTION_MS + 2
    assert trace_retention.expire_payloads(db, now_ms=now) == {
        "scanned_rows": 2,
        "expired_payloads": 0,
        "after_ingest_seq": 2,
        "observed_at_ms": now,
        "retention_ms": trace_retention.RETENTION_MS,
    }
    with closing(
        sqlite3.connect(db.execute("PRAGMA database_list").fetchone()[2])
    ) as reopened:
        trace_store.initialize(reopened)
        assert (
            trace_retention.expire_payloads(reopened, now_ms=now)["expired_payloads"]
            == 2
        )
        final = trace_retention.expire_payloads(reopened, now_ms=now)
        assert final["expired_payloads"] == 1 and final["after_ingest_seq"] == 0
        assert (
            trace_retention.expire_payloads(reopened, now_ms=now + 100000)[
                "expired_payloads"
            ]
            == 2
        )
    assert (
        db.execute(
            "SELECT count(*) FROM trace_raw_events WHERE payload_expired_at_ms IS NOT NULL"
        ).fetchone()[0]
        == 5
    )


def test_expiry_cursor_failure_rolls_back_payload_and_accounting(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    with db:
        db.execute(
            "CREATE TRIGGER retention_fault BEFORE UPDATE ON trace_retention_state BEGIN SELECT RAISE(ABORT,'owned failure'); END"
        )
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="owned failure"):
        trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 2)
    assert list(db.iterdump()) == before


def test_loss_dispositions_survive_expiring_the_coverage_payload(case):  # noqa: F811
    from test_trace_settlement import loss

    db, record = case
    scope = {
        key: record[key] for key in ("node_id", "source_epoch", "export_generation")
    }
    loss(db, scope, [(1, 5)], through=5)
    request = {**scope, "schema_version": 1, "request_id": str(uuid4())}
    before = settlement_reply(db, request)
    assert (
        trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 1001)[
            "expired_payloads"
        ]
        == 1
    )
    assert settlement_reply(db, request) == before
    assert before["checkpoint"]["lost_ranges"] == [{"first": 1, "last": 5}]


def test_legacy_payload_column_upgrade_preserves_evidence_and_epoch(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    epoch = db.execute("SELECT collector_epoch FROM trace_collector").fetchone()[0]
    with db:
        db.execute("DROP INDEX trace_inline_test_expiry")
        db.execute("ALTER TABLE trace_raw_events DROP COLUMN payload_expired_at_ms")
        db.execute("DROP TABLE trace_retention_state")
    trace_store.initialize(db)
    assert (
        db.execute("SELECT collector_epoch FROM trace_collector").fetchone()[0] == epoch
    )
    assert (
        json.loads(db.execute("SELECT event_json FROM trace_raw_events").fetchone()[0])
        == record["event"]
    )
    assert (
        db.execute("SELECT payload_expired_at_ms FROM trace_raw_events").fetchone()[0]
        is None
    )


@pytest.mark.asyncio
async def test_idle_collector_runs_expiry_without_a_delivery(case):  # noqa: F811
    from types import SimpleNamespace

    from nats.errors import TimeoutError

    from aggregator.trace_collector import TraceCollectorService

    db, record = case
    ingest(db, record)
    service = TraceCollectorService(
        Path(db.execute("PRAGMA database_list").fetchone()[2]),
        "nats://127.0.0.1:9",
        "owned",
    )

    class IdleConsumer:
        async def fetch(self, **kwargs):
            service._stop.set()
            raise TimeoutError

    await service._ingest(db, SimpleNamespace(is_connected=True), IdleConsumer())
    assert service.status()["retention"]["expired_payloads"] == 1
    assert service.status()["retention"]["state"] == "observed"
    assert service.status()["storage_usage"]["trace_raw_events"]["payload_bytes"] == 0


def test_physical_pressure_refuses_expiry_without_advancing_cursor(case, monkeypatch):  # noqa: F811
    db, record = case
    ingest(db, record)
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 0)
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_physical_pressure"):
        trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 2)
    assert list(db.iterdump()) == before
    plan = db.execute(
        "EXPLAIN QUERY PLAN SELECT ingest_seq,received_at_ms,payload_expired_at_ms "
        "FROM trace_raw_events WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
        (0, 256),
    ).fetchall()
    assert any("SEARCH" in row[3] and "ingest_seq>?" in row[3] for row in plan)
    assert not any("TEMP B-TREE" in row[3] for row in plan)
