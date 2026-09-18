"""Physical pressure uses real owned SQLite files and a pinned WAL reader."""

import sqlite3
import time
from copy import deepcopy
from uuid import uuid4

import pytest
from test_trace_crash import snapshot
from test_trace_journal import FIXTURES, event, write

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError


def tool():
    value = deepcopy(next(f["event"] for f in FIXTURES if f["name"] == "tool"))
    value["event_id"] = str(uuid4())
    return value


def test_pinned_wal_stops_optional_admission_preserves_retry_and_recovers(
    tmp_path, monkeypatch
):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    db = store._connection
    reader = sqlite3.connect(store.path)
    try:
        original = tool()
        committed = write(store, original)
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        baseline = trace_capacity.physical_storage(db)
        limit = baseline["pressure_bytes"] + 192 * 1024
        monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", limit)
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM trace_journal").fetchone()
        for _ in range(128):
            before = snapshot(store)
            try:
                write(store, tool())
            except TraceContractError as error:
                assert error.code == "quota_exceeded"
                assert snapshot(store) == before
                break
        else:
            pytest.fail("pinned reader did not cause optional storage pressure")
        pressure = trace_capacity.physical_storage(db)
        assert pressure["wal_file_bytes"] > 0
        assert pressure["pressure_bytes"] >= limit
        assert (
            db.execute("SELECT event_bytes FROM trace_storage_usage").fetchone()[0]
            < limit
        )
        health = store.health()
        assert health["status"] == "degraded"
        assert health["trace_storage"] == "physical_pressure"
        assert health["physical_storage"] == pressure
        # Idempotent replay still succeeds without spending more capacity.
        assert write(store, original) == committed
        assert snapshot(store) == before
        # Mandatory lifecycle data retains its existing quota/failure semantics.
        mandatory = write(store, event())
        assert mandatory["kind"] == "task"
        checkpoint = db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        assert checkpoint[1] > checkpoint[2]
        with pytest.raises(TraceContractError, match="quota_exceeded"):
            write(store, tool())
        # Scheduled maintenance must not wait out the connection's 5-second
        # busy timeout or evict a reader. Its snapshot remains usable.
        started = time.monotonic()
        store.reconcile()
        assert time.monotonic() - started < 2
        assert reader.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0] == 1
        assert store.health()["trace_storage"] == "physical_pressure"
        assert db.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        reader.rollback()
        store.reconcile()
        assert trace_capacity.physical_storage(db)["wal_file_bytes"] == 0
        assert db.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        assert trace_capacity.physical_storage(db)["pressure_bytes"] < limit
        assert store.health()["status"] == "ready"
        assert "trace_storage" not in store.health()
        # Physical-pressure cleanup now writes durable coverage before deleting
        # optional payloads, so it legitimately consumes source positions.
        next_sequence = db.execute(
            "SELECT next_source_seq FROM trace_sources WHERE active=1"
        ).fetchone()[0]
        assert write(store, tool())["source_seq"] == next_sequence
        assert (
            db.execute(
                "SELECT count(*) FROM trace_journal WHERE event_id=?",
                (mandatory["event_id"],),
            ).fetchone()[0]
            == 1
        )
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        reader.close()
        store.close()


def test_pending_pages_and_free_pages_remain_accounted_without_checkpoint(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    db = store._connection
    try:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        baseline = trace_capacity.physical_storage(db)
        with db:
            db.execute("CREATE TABLE owned_bulk(value BLOB)")
            db.execute("INSERT INTO owned_bulk VALUES (zeroblob(262144))")
            pending = trace_capacity.physical_storage(db)
            assert (
                pending["allocated_page_bytes"]
                >= baseline["allocated_page_bytes"] + 262144
            )
            assert pending["pressure_bytes"] >= pending["allocated_page_bytes"]
            assert db.in_transaction  # Measurement does not commit or checkpoint.
        with db:
            db.execute("DELETE FROM owned_bulk")
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        freed = trace_capacity.physical_storage(db)
        assert freed["reusable_page_bytes"] >= 262144
        assert (
            freed["pressure_bytes"]
            == freed["database_file_bytes"] + freed["shm_file_bytes"]
        )
        # Reusable pages still occupy disk; reporting must not subtract them.
        assert freed["pressure_bytes"] >= freed["allocated_page_bytes"]
    finally:
        store.close()


def test_checkpoint_refuses_active_transaction(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            with pytest.raises(
                TraceContractError, match="trace_checkpoint_transaction_active"
            ):
                trace_capacity.reclaim_wal_pressure(store._connection)
            assert store._connection.in_transaction
    finally:
        store.close()
