import json
import sqlite3
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    canonical_bytes,
    event_sha256,
)

from aggregator import trace_capacity as capacity
from aggregator import trace_store

FIXTURES = json.loads(
    (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    ).read_text()
)["fixtures"]


@pytest.fixture
def case(tmp_path):
    db = sqlite3.connect(tmp_path / "core.db")
    trace_store.initialize(db)
    event = deepcopy(FIXTURES[0]["event"])
    record = {
        "schema_version": 1,
        "node_id": event["node_id"],
        "source_epoch": event["source_epoch"],
        "export_generation": str(uuid4()),
        "export_seq": 1,
        "event_sha256": event_sha256(event),
        "event": event,
    }
    yield db, record
    db.close()


def ingest(db, record):
    return trace_store.ingest(
        db, f"edgecitadel.telemetry.v1.{record['node_id']}", record, received_at_ms=1
    )


def next_event(record):
    record = deepcopy(record)
    record["event"]["event_id"] = str(uuid4())
    record["event"]["source_seq"] += 1
    record["export_seq"] += 1
    record["event_sha256"] = event_sha256(record["event"])
    return record


@pytest.mark.parametrize("budget", ["bytes", "rows"])
def test_raw_budget_rolls_back_all_evidence_and_cursor_but_allows_exact_retry(
    case, monkeypatch, budget
):
    db, record = case
    if budget == "bytes":
        monkeypatch.setattr(
            capacity, "RAW_BYTES", len(canonical_bytes(record["event"]))
        )
    else:
        monkeypatch.setitem(capacity.ROW_LIMITS, "trace_raw_events", 1)
    first = ingest(db, record)
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_capacity_exceeded"):
        ingest(db, next_event(record))
    assert list(db.iterdump()) == before
    assert ingest(db, record) == first
    assert list(db.iterdump()) == before


def test_generation_replay_cannot_grow_position_metadata_past_limit(case, monkeypatch):
    db, record = case
    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_ingest_positions", 1)
    first = ingest(db, record)
    before = list(db.iterdump())
    replay = {**record, "export_generation": str(uuid4())}
    with pytest.raises(TraceContractError, match="core_capacity_exceeded"):
        ingest(db, replay)
    assert list(db.iterdump()) == before
    assert ingest(db, record) == first


def test_conflict_metadata_is_capped_without_replacing_original(case, monkeypatch):
    db, record = case
    ingest(db, record)
    conflict = next_event(record)
    conflict["event"]["source_seq"] = record["event"]["source_seq"]
    conflict["event_sha256"] = event_sha256(conflict["event"])
    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_ingest_conflicts", 1)
    result = ingest(db, conflict)
    assert result.outcome == "conflict"
    before = list(db.iterdump())
    another = {**conflict, "export_seq": 3}
    with pytest.raises(TraceContractError, match="core_capacity_exceeded"):
        ingest(db, another)
    assert list(db.iterdump()) == before
    assert ingest(db, conflict) == result
    assert capacity.usage(db)["trace_raw_events"]["rows"] == 1


def test_loss_range_limit_rolls_back_marker_raw_and_entire_fragment(case, monkeypatch):
    db, record = case
    event = deepcopy(next(x["event"] for x in FIXTURES if x["name"] == "coverage"))
    event.update(
        node_id=record["node_id"],
        source_epoch=record["source_epoch"],
        event_id=str(uuid4()),
    )
    event["attributes"] = {
        "affected_source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
        "through_export_seq": 3,
        "lost_ranges": [{"first": 1, "last": 1}, {"first": 3, "last": 3}],
    }
    marker = {**record, "event": event, "event_sha256": event_sha256(event)}
    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_loss_ranges", 1)
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_capacity_exceeded"):
        ingest(db, marker)
    assert list(db.iterdump()) == before
    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_loss_ranges", 2)
    ingest(db, marker)
    assert capacity.usage(db)["trace_loss_ranges"]["rows"] == 2


def test_accounting_backfills_legacy_data_once_and_tracks_deletion(case):
    db, record = case
    first = ingest(db, record)
    expected = capacity.usage(db)
    with db:
        for table in capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                db.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        db.execute("DROP TABLE trace_capacity_usage")
    trace_store.initialize(db)
    assert capacity.usage(db) == expected
    assert ingest(db, record) == first
    statements = []
    db.set_trace_callback(statements.append)
    trace_store.initialize(db)
    db.set_trace_callback(None)
    assert not any("coalesce(sum(" in sql.lower() for sql in statements)
    # Accounting must follow future safe retention work; this test does not assert
    # that deleting raw evidence alone is an authorized retention policy.
    with db:
        db.execute("DELETE FROM trace_raw_events")
    assert capacity.usage(db)["trace_raw_events"] == {"rows": 0, "payload_bytes": 0}


def test_counter_write_failure_and_missing_counter_cannot_commit_growth(case):
    db, record = case
    with db:
        db.execute(
            "CREATE TRIGGER fail_usage BEFORE UPDATE ON trace_capacity_usage BEGIN SELECT RAISE(ABORT, 'injected'); END"
        )
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError):
        ingest(db, record)
    assert list(db.iterdump()) == before
    with db:
        db.execute("DROP TRIGGER fail_usage")
        db.execute(
            "DELETE FROM trace_capacity_usage WHERE table_name='trace_raw_events'"
        )
    before = list(db.iterdump())
    with pytest.raises(
        TraceContractError, match="core_capacity_accounting_unavailable"
    ):
        ingest(db, record)
    assert list(db.iterdump()) == before


def test_saturated_raw_storage_still_accepts_fixed_poison_counter(case, monkeypatch):
    db, record = case
    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_raw_events", 1)
    ingest(db, record)
    for _ in range(10):
        trace_store.record_poison(db, "wire", received_at_ms=1)
    assert db.execute(
        "SELECT count(*),max(observations) FROM trace_poison_counts"
    ).fetchone() == (1, 10)
    assert capacity.usage(db)["trace_raw_events"]["rows"] == 1


def test_legacy_accounting_upgrade_rolls_back_without_changing_epoch_or_evidence(
    case, monkeypatch
):
    db, record = case
    first = ingest(db, record)
    with db:
        for table in capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                db.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        db.execute("DROP TABLE trace_capacity_usage")
    before = list(db.iterdump())
    initialize = capacity.initialize

    def fail(connection):
        initialize(connection)
        raise RuntimeError("injected migration failure")

    with monkeypatch.context() as patch:
        patch.setattr(capacity, "initialize", fail)
        with pytest.raises(RuntimeError, match="injected migration"):
            trace_store.initialize(db)
    assert list(db.iterdump()) == before
    trace_store.initialize(db)
    assert ingest(db, record) == first


@pytest.mark.asyncio
async def test_owned_broker_capacity_refusal_does_not_ack_or_advance_settlement(
    tmp_path, monkeypatch
):
    import asyncio
    import os
    import secrets

    from edgecitadel_agentd.store import AgentdStore
    from edgecitadel_agentd.trace_journal import TraceJournal
    from edgecitadel_agentd.trace_sync_service import TraceSyncService
    from edgecitadel_plugin_runtime.telemetry_stream import CONSUMER_NAME, STREAM_NAME
    from nats.aio.client import Client as NATS

    from aggregator.trace_collector import TraceCollectorService
    from tests.nats_server import NatsServer

    if os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1":
        pytest.skip("owned NATS opt-in required")
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    core = TraceCollectorService(tmp_path / "core.db", server.url, server.token)
    node = tmp_path / "source"
    node.mkdir()
    (node / "node.json").write_text(
        json.dumps(
            {
                "version": 1,
                "mode": "core",
                "nats_url": server.url,
                "nats_token": server.token,
            }
        )
    )
    source = AgentdStore(node / "agentd" / "agentd.sqlite3")
    event = deepcopy(FIXTURES[0]["event"])
    with source._connection:
        source._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(source._connection)
        journal.initialize("edge-a")
        journal.record("edge-a", event, selected=True)
        event["event_id"] = str(uuid4())
        journal.record("edge-a", event, selected=True)
    sync = TraceSyncService(node, source.path, enabled=True)
    nc = NATS()

    async def until(predicate):
        async with asyncio.timeout(8):
            while not predicate():
                await asyncio.sleep(0.02)

    monkeypatch.setitem(capacity.ROW_LIMITS, "trace_raw_events", 1)
    try:
        await nc.connect(servers=[server.url], token=server.token)
        core.start()
        await until(lambda: core.status()["state"] == "running")
        sync.start()
        await until(lambda: core.status()["fault"] == "collector_capacity_exceeded")
        await until(
            lambda: (
                source._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                ).fetchone()[0]
                == 1
            )
        )
        assert (
            await nc.jetstream().consumer_info(STREAM_NAME, CONSUMER_NAME)
        ).num_ack_pending >= 1
        assert (
            source._connection.execute(
                "SELECT max(applied_through) FROM trace_source_settlements"
            ).fetchone()[0]
            == 1
        )
        assert (
            source._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 2
        )
        assert core.status()["storage_usage"]["trace_raw_events"]["rows"] == 1
        assert core.status()["storage_at_capacity"] is True
        assert core.status()["storage_limits"]["rows"]["trace_raw_events"] == 1
        # Test-only budget enlargement; no receipts or evidence are deleted.
        monkeypatch.setitem(capacity.ROW_LIMITS, "trace_raw_events", 2)
        await until(
            lambda: core.status()["storage_usage"]["trace_raw_events"]["rows"] == 2
        )
        await asyncio.to_thread(sync.stop)
        sync.start()
        await until(
            lambda: (
                source._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                ).fetchone()[0]
                == 2
            )
        )
        assert (
            source._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
        with sqlite3.connect(tmp_path / "core.db") as observer:
            assert {
                row[0]
                for row in observer.execute("SELECT event_id FROM trace_raw_events")
            } == {
                row[0]
                for row in source._connection.execute(
                    "SELECT event_id FROM trace_journal"
                )
            }
    finally:
        await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(core.stop)
        await nc.close()
        source.close()
        await asyncio.to_thread(server.close)


def test_pinned_reader_pressure_stops_counter_growth_then_checkpoint_resumes(
    case, monkeypatch
):
    db, record = case
    path = db.execute("PRAGMA database_list").fetchone()[2]
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    reader = sqlite3.connect(path)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM trace_raw_events").fetchone()
    monkeypatch.setattr(capacity, "WAL_PRESSURE_BYTES", 128 * 1024)
    monkeypatch.setattr(capacity, "WRITE_HEADROOM_BYTES", 32 * 1024)
    ingest(db, record)
    try:
        for _ in range(100):
            try:
                trace_store.record_poison(db, "wire", received_at_ms=1)
            except TraceContractError as error:
                assert error.code == "core_physical_pressure"
                break
        else:
            pytest.fail("pinned WAL did not trigger pressure admission")
        before = list(db.iterdump())
        wal_size = Path(path + "-wal").stat().st_size
        for _ in range(10):
            with pytest.raises(TraceContractError, match="core_physical_pressure"):
                trace_store.record_poison(db, "wire", received_at_ms=1)
        assert list(db.iterdump()) == before
        assert Path(path + "-wal").stat().st_size == wal_size
        assert wal_size < capacity.WAL_PRESSURE_BYTES
        assert capacity.snapshot(db, rejection_limit=4096)["storage_pressure"]
        # Already committed identities can still ACK without an evidence mutation.
        assert ingest(db, record).outcome == "accepted"
    finally:
        reader.rollback()
        reader.close()
    trace_store.record_poison(db, "wire", received_at_ms=1)
    assert Path(path + "-wal").stat().st_size < wal_size
    assert not capacity.snapshot(db, rejection_limit=4096)["storage_pressure"]


def test_shared_database_pressure_refuses_new_positions_but_preserves_known_ack(
    case, monkeypatch
):
    db, record = case
    first = ingest(db, record)
    current = capacity.physical(db)["pressure_bytes"]
    monkeypatch.setattr(
        capacity, "PHYSICAL_PRESSURE_BYTES", current + capacity.WRITE_HEADROOM_BYTES
    )
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_physical_pressure"):
        ingest(db, {**record, "export_generation": str(uuid4())})
    with pytest.raises(TraceContractError, match="core_physical_pressure"):
        trace_store.record_poison(db, "wire", received_at_ms=1)
    assert list(db.iterdump()) == before
    assert ingest(db, record) == first
    assert capacity.snapshot(db, rejection_limit=4096)["storage_pressure"]


def test_backfill_batches_resume_and_fence_evidence_until_complete(case):
    db, record = case
    ingest(db, record)
    with db:
        db.executemany(
            "INSERT INTO trace_raw_events "
            "(node_id,source_epoch,event_id,source_seq,event_sha256,event_json,received_at_ms,ingest_seq) "
            "SELECT node_id,source_epoch,?,? ,"
            "event_sha256,event_json,received_at_ms,? FROM trace_raw_events "
            "WHERE event_id=?",
            [
                (f"owned-{index}", index + 2, index + 2, record["event"]["event_id"])
                for index in range(600)
            ],
        )
    expected = capacity.usage(db)
    epoch = db.execute("SELECT collector_epoch FROM trace_collector").fetchone()[0]
    with db:
        for table in capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                db.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        db.execute("DROP TABLE trace_capacity_usage")
    trace_store.initialize(db, backfill=False)
    while True:
        assert not capacity.backfill_step(db)
        state = db.execute(
            "SELECT rows FROM trace_capacity_usage WHERE table_name='trace_raw_events'"
        ).fetchone()[0]
        if state:
            assert state == capacity.BACKFILL_BATCH_ROWS
            break
    with pytest.raises(
        TraceContractError, match="core_capacity_accounting_unavailable"
    ):
        capacity.usage(db)
    before = list(db.iterdump())
    with (
        pytest.raises(sqlite3.IntegrityError, match="core_capacity_backfill_pending"),
        db,
    ):
        db.execute("DELETE FROM trace_raw_events")
    assert list(db.iterdump()) == before
    path = db.execute("PRAGMA database_list").fetchone()[2]
    # A fresh connection simulates process reopening persisted migration state.
    with closing(sqlite3.connect(path)) as reopened:
        trace_store.initialize(reopened, backfill=False)
        assert (
            reopened.execute(
                "SELECT rows FROM trace_capacity_usage WHERE table_name='trace_raw_events'"
            ).fetchone()[0]
            == state
        )
        with reopened:
            reopened.execute(
                "CREATE TRIGGER fail_batch BEFORE UPDATE ON trace_capacity_backfill "
                "BEGIN SELECT RAISE(ABORT,'owned backfill fault'); END"
            )
        before = list(reopened.iterdump())
        with pytest.raises(sqlite3.IntegrityError, match="owned backfill fault"):
            capacity.backfill_step(reopened)
        assert list(reopened.iterdump()) == before
        with reopened:
            reopened.execute("DROP TRIGGER fail_batch")
        while not capacity.backfill_step(reopened):
            pass
        assert capacity.usage(reopened) == expected
        assert (
            reopened.execute("SELECT collector_epoch FROM trace_collector").fetchone()[
                0
            ]
            == epoch
        )
        assert ingest(reopened, record).outcome == "accepted"
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.asyncio
async def test_collector_stop_between_backfill_batches_preserves_resume(
    case, monkeypatch
):
    from aggregator import trace_collector

    db, record = case
    ingest(db, record)
    expected = capacity.usage(db)
    with db:
        for table in capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                db.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        db.execute("DROP TABLE trace_capacity_usage")
    path = db.execute("PRAGMA database_list").fetchone()[2]
    service = trace_collector.TraceCollectorService(
        Path(path), "nats://127.0.0.1:9", "owned"
    )
    step = capacity.backfill_step

    def stop_after_batch(connection):
        ready = step(connection)
        assert not ready
        service._stop.set()
        return ready

    def unexpected_network():
        raise AssertionError("Network consumer started before accounting completed")

    with monkeypatch.context() as patch:
        patch.setattr(capacity, "backfill_step", stop_after_batch)
        patch.setattr(trace_collector, "NATS", unexpected_network)
        await service._run()
    assert (
        db.execute(
            "SELECT count(*) FROM trace_capacity_backfill WHERE complete=1"
        ).fetchone()[0]
        == 1
    )
    with pytest.raises(
        TraceContractError, match="core_capacity_accounting_unavailable"
    ):
        capacity.usage(db)
    trace_store.initialize(db)
    assert capacity.usage(db) == expected
    assert ingest(db, record).outcome == "accepted"
