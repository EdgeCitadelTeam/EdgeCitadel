import sqlite3
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import event_sha256
from test_trace_capacity import case, ingest, next_event  # noqa: F401

from aggregator import trace_payloads, trace_retention, trace_store
from aggregator.trace_inspect import inspect_core
from aggregator.trace_restore import prepare_restore


def adopt(db):
    trace_payloads.prepare(db)
    while not trace_payloads.migrate_batch(db):
        pass


def test_adopted_ingest_retry_conflict_inspection_and_expiry(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    adopt(db)
    fresh = next_event(record)
    first = ingest(db, fresh)
    before = list(db.iterdump())
    assert ingest(db, fresh) == first
    assert list(db.iterdump()) == before
    assert (
        ingest(db, {**fresh, "export_generation": str(uuid4())}).outcome == "duplicate"
    )
    changed = deepcopy(fresh)
    changed["event"]["occurred_at"] = "2026-09-17T01:00:00.000Z"
    changed["event_sha256"] = event_sha256(changed["event"])
    assert ingest(db, changed).outcome == "conflict"
    assert (
        db.execute(
            "SELECT count(*) FROM trace_raw_events WHERE event_json!=''"
        ).fetchone()[0]
        == 0
    )
    assert db.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 2
    path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    assert [row["event"] for row in inspect_core(path)["records"]] == [
        record["event"],
        fresh["event"],
    ]
    assert (
        trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 2)[
            "expired_payloads"
        ]
        == 2
    )
    assert all(
        row["payload_state"] == "expired" for row in inspect_core(path)["records"]
    )
    assert ingest(db, fresh) == first
    assert db.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 0


def test_payload_insert_fault_cannot_commit_identity_or_disposition(case):  # noqa: F811
    db, record = case
    adopt(db)
    with db:
        db.execute(
            "CREATE TRIGGER owned_fault BEFORE INSERT ON trace_payloads BEGIN SELECT RAISE(ABORT,'owned fault'); END"
        )
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="owned fault"):
        ingest(db, record)
    assert list(db.iterdump()) == before


def test_preexisting_connection_and_restored_layout_are_authorized_before_writes(
    case,  # noqa: F811
    tmp_path,  # noqa: F811
):  # noqa: F811
    db, record = case
    path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    with closing(sqlite3.connect(path)) as other:
        trace_store.initialize(other)
        adopt(db)
        ingest(other, record)
    restored = tmp_path / "restored.db"
    prepare_restore(path, restored)
    with closing(sqlite3.connect(restored)) as target:
        trace_store.initialize(target)
        assert trace_payloads.read_payload(target, 1)["event"] == record["event"]
        assert ingest(target, next_event(record)).outcome == "accepted"
        assert target.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 2
        assert target.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.asyncio
async def test_collector_stop_during_payload_migration_retains_resumable_cursor(
    case,  # noqa: F811
    monkeypatch,  # noqa: F811
):  # noqa: F811
    from aggregator import trace_collector

    db, record = case
    for _ in range(300):
        ingest(db, record)
        record = next_event(record)
    path = Path(db.execute("PRAGMA database_list").fetchone()[2])
    service = trace_collector.TraceCollectorService(path, "nats://127.0.0.1:9", "owned")
    step = trace_payloads.migrate_batch

    def stop_after_batch(connection):
        done = step(connection)
        assert not done
        service._stop.set()
        return done

    def unexpected_network():
        raise AssertionError("Subscribed before completing migration")

    with monkeypatch.context() as patch:
        patch.setattr(trace_payloads, "migrate_batch", stop_after_batch)
        patch.setattr(trace_collector, "NATS", unexpected_network)
        await service._run()
    assert db.execute(
        "SELECT after_ingest_seq,complete FROM trace_payload_layout"
    ).fetchone() == (256, 0)
    assert len(inspect_core(path)["records"]) == 32
    trace_store.initialize(db)
    assert db.execute("SELECT complete FROM trace_payload_layout").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 300
