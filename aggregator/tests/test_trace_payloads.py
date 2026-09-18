import sqlite3
from contextlib import closing

import pytest
from edgecitadel_agentd.trace_contract import TraceContractError
from test_trace_capacity import case, ingest, next_event  # noqa: F401

from aggregator import trace_capacity, trace_payloads, trace_retention


def finish(db):
    while not trace_payloads.migrate_batch(db):
        pass


def seed(db, record, count):
    for _ in range(count):
        ingest(db, record)
        record = next_event(record)


def test_mixed_live_and_expired_payloads_resume_with_exact_accounting(case):  # noqa: F811
    db, record = case
    seed(db, record, 300)
    with db:
        db.execute(
            "UPDATE trace_raw_events SET received_at_ms=100000 WHERE ingest_seq>10"
        )
    trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 2)
    expected = [trace_payloads.read_payload(db, i) for i in range(1, 301)]
    positions = list(
        db.execute("SELECT * FROM trace_ingest_positions ORDER BY ingest_seq")
    )
    usage = trace_capacity.usage(db)
    trace_payloads.prepare(db)
    with pytest.raises(TraceContractError, match="core_payload_migration_pending"):
        trace_payloads.expire_batch(db, before_ms=200000, now_ms=200001)
    assert not trace_payloads.migrate_batch(db)
    assert (
        db.execute("SELECT after_ingest_seq FROM trace_payload_layout").fetchone()[0]
        == 256
    )
    assert [trace_payloads.read_payload(db, i) for i in range(1, 301)] == expected
    with closing(
        sqlite3.connect(db.execute("PRAGMA database_list").fetchone()[2])
    ) as reopened:
        trace_payloads.open_layout(reopened)
        finish(reopened)
        assert [
            trace_payloads.read_payload(reopened, i) for i in range(1, 301)
        ] == expected
        assert (
            list(
                reopened.execute(
                    "SELECT * FROM trace_ingest_positions ORDER BY ingest_seq"
                )
            )
            == positions
        )
        assert trace_capacity.usage(reopened) == usage
        assert (
            reopened.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 290
        )
        before = list(reopened.iterdump())
        assert trace_payloads.migrate_batch(reopened)
        assert list(reopened.iterdump()) == before
        assert (
            trace_payloads.expire_batch(reopened, before_ms=200000, now_ms=200001)
            == 256
        )
        assert (
            trace_payloads.expire_batch(reopened, before_ms=200000, now_ms=200001) == 34
        )
        assert trace_capacity.usage(reopened)["trace_raw_events"] == {
            "rows": 300,
            "payload_bytes": 0,
        }
        assert reopened.execute("PRAGMA foreign_key_check").fetchall() == []
        assert reopened.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_migration_fault_rolls_back_body_identity_accounting_and_cursor(case):  # noqa: F811
    db, record = case
    seed(db, record, 5)
    trace_payloads.prepare(db)
    with db:
        db.execute(
            "CREATE TRIGGER owned_fail BEFORE UPDATE ON trace_payload_layout BEGIN SELECT RAISE(ABORT,'owned fault'); END"
        )
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="owned fault"):
        trace_payloads.migrate_batch(db)
    assert list(db.iterdump()) == before
    with db:
        db.execute("DROP TRIGGER owned_fail")
    finish(db)
    assert trace_payloads.read_payload(db, 1)["event"] == record["event"]


def test_old_connection_refused_and_missing_accounting_trigger_revokes_capability(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    trace_payloads.prepare(db)
    with closing(
        sqlite3.connect(db.execute("PRAGMA database_list").fetchone()[2])
    ) as old:
        before = list(old.iterdump())
        with pytest.raises(sqlite3.OperationalError, match="no such function"), old:
            old.execute(
                "INSERT OR IGNORE INTO trace_collector(singleton,collector_epoch) VALUES(1,'legacy')"
            )
        assert list(old.iterdump()) == before
    with db:
        db.execute("DROP TRIGGER trace_payload_bytes_insert")
    with pytest.raises(TraceContractError, match="core_payload_layout_unavailable"):
        trace_payloads.open_layout(db)
    with pytest.raises(sqlite3.OperationalError), db:
        db.execute("UPDATE trace_raw_events SET event_json='{}'")


def test_accounting_rebuild_includes_separate_payload_bytes(case):  # noqa: F811
    db, record = case
    seed(db, record, 3)
    expected = trace_capacity.usage(db)
    trace_payloads.prepare(db)
    finish(db)
    with db:
        for table in trace_capacity.TABLES:
            for operation in ("insert", "delete", "update"):
                db.execute(f"DROP TRIGGER {table}_capacity_{operation}")
        db.execute("DROP TABLE trace_capacity_usage")
    with db:
        db.execute("BEGIN IMMEDIATE")
        trace_capacity.initialize(db)
    with (
        pytest.raises(sqlite3.IntegrityError, match="core_capacity_backfill_pending"),
        db,
    ):
        db.execute("DELETE FROM trace_payloads")
    while not trace_capacity.backfill_step(db):
        pass
    assert trace_capacity.usage(db) == expected


def test_pressure_and_corrupt_inline_digest_refuse_without_partial_migration(
    case,  # noqa: F811
    monkeypatch,  # noqa: F811
):
    db, record = case
    ingest(db, record)
    trace_payloads.prepare(db)
    before = list(db.iterdump())
    with monkeypatch.context() as patch:
        patch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 0)
        with pytest.raises(TraceContractError, match="core_physical_pressure"):
            trace_payloads.migrate_batch(db)
    assert list(db.iterdump()) == before
    with db:
        db.execute("UPDATE trace_raw_events SET event_json='{}'")
    before = list(db.iterdump())
    with pytest.raises(TraceContractError, match="core_payload_layout_unavailable"):
        trace_payloads.migrate_batch(db)
    assert list(db.iterdump()) == before


def test_legacy_reader_does_not_require_expiry_schema(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    with db:
        db.execute("DROP INDEX trace_inline_test_expiry")
        db.execute("ALTER TABLE trace_raw_events DROP COLUMN payload_expired_at_ms")
    before = list(db.iterdump())
    assert trace_payloads.read_payload(db, 1)["event"] == record["event"]
    assert trace_payloads.read_payload(db, 2) is None
    assert list(db.iterdump()) == before


def test_missing_legacy_counter_trigger_cannot_commit_migration(case):  # noqa: F811
    db, record = case
    ingest(db, record)
    trace_payloads.prepare(db)
    with db:
        db.execute("DROP TRIGGER trace_raw_events_capacity_delete")
    before = list(db.iterdump())
    with pytest.raises(
        TraceContractError, match="core_capacity_accounting_unavailable"
    ):
        trace_payloads.migrate_batch(db)
    assert list(db.iterdump()) == before


def test_pinned_wal_reader_stops_migration_then_release_allows_resume(
    case,  # noqa: F811
    monkeypatch,  # noqa: F811
):  # noqa: F811
    db, record = case
    seed(db, record, 600)
    trace_payloads.prepare(db)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    with closing(
        sqlite3.connect(db.execute("PRAGMA database_list").fetchone()[2])
    ) as reader:
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM trace_raw_events").fetchone()
        monkeypatch.setattr(trace_capacity, "WAL_PRESSURE_BYTES", 128 * 1024)
        monkeypatch.setattr(trace_capacity, "WRITE_HEADROOM_BYTES", 32 * 1024)
        assert not trace_payloads.migrate_batch(db)
        before = list(db.iterdump())
        with pytest.raises(TraceContractError, match="core_physical_pressure"):
            trace_payloads.migrate_batch(db)
        assert list(db.iterdump()) == before
        reader.rollback()
    finish(db)
    assert db.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 600
    assert trace_payloads.read_payload(db, 1)["event"] == record["event"]


@pytest.mark.parametrize("after_commit", [False, True])
def test_process_kill_recovers_only_complete_batches(case, after_commit):  # noqa: F811
    import signal
    import subprocess
    import sys

    db, record = case
    seed(db, record, 300)
    expected = [trace_payloads.read_payload(db, i) for i in range(1, 301)]
    trace_payloads.prepare(db)
    path = db.execute("PRAGMA database_list").fetchone()[2]
    script = """
import os, signal, sqlite3, sys
from aggregator.trace_payloads import migrate_batch
connection = sqlite3.connect(sys.argv[1])
if sys.argv[2] == 'before':
    def trace(sql):
        if sql == 'COMMIT':
            os.kill(os.getpid(), signal.SIGKILL)
    connection.set_trace_callback(trace)
migrate_batch(connection)
os.kill(os.getpid(), signal.SIGKILL)
"""
    job = subprocess.run(
        [sys.executable, "-c", script, path, "after" if after_commit else "before"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert job.returncode == -signal.SIGKILL, job.stderr
    assert db.execute("SELECT after_ingest_seq FROM trace_payload_layout").fetchone()[
        0
    ] == (256 if after_commit else 0)
    assert [trace_payloads.read_payload(db, i) for i in range(1, 301)] == expected
    finish(db)
    assert [trace_payloads.read_payload(db, i) for i in range(1, 301)] == expected


def test_authorized_connection_cannot_use_legacy_write_paths_after_migration(
    case,  # noqa: F811
    monkeypatch,  # noqa: F811
):  # noqa: F811
    db, record = case
    ingest(db, record)
    trace_payloads.prepare(db)
    finish(db)
    before = list(db.iterdump())
    with monkeypatch.context() as patch:
        patch.setattr(trace_payloads, "is_prepared", lambda connection: False)
        with pytest.raises(sqlite3.IntegrityError, match="core_payload_write_required"):
            ingest(db, next_event(record))
        assert list(db.iterdump()) == before
        with pytest.raises(sqlite3.IntegrityError, match="core_payload_write_required"):
            trace_retention.expire_payloads(db, now_ms=trace_retention.RETENTION_MS + 2)
    assert list(db.iterdump()) == before
    assert trace_payloads.expire_batch(db, before_ms=2, now_ms=3) == 1
