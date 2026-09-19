"""Local SQLite component tests; deployed E2E runs only on jim-eq."""

import asyncio
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes

from aggregator import trace_collector, trace_store
from e2e.helpers.trace_commit_observer import (
    CommitObserver,
    TimedConnection,
    instrument_collector,
)


def test_transaction_brackets_commit_not_rollback_or_idle(tmp_path):
    path = tmp_path / "commit.db"
    db = sqlite3.connect(path, factory=TimedConnection)
    other = sqlite3.connect(path)
    db.execute("CREATE TABLE value (n INTEGER)")
    try:
        before = time.monotonic_ns()
        with db:
            db.execute("INSERT INTO value VALUES(1)")
        bracket = db.last_commit
        assert before <= bracket.before_ns <= bracket.after_ns <= time.monotonic_ns()
        assert other.execute("SELECT n FROM value").fetchall() == [(1,)]
        with pytest.raises(ValueError), db:
            db.execute("INSERT INTO value VALUES(2)")
            raise ValueError("abort")
        assert db.last_commit is None
        with db:
            pass
        assert db.last_commit is None
        with db:
            db.execute("INSERT INTO value VALUES(3)")
        assert db.last_commit.before_ns >= bracket.after_ns
        assert other.execute("SELECT n FROM value").fetchall() == [(1,), (3,)]
    finally:
        other.close()
        db.close()


def test_failed_commit_does_not_leave_marker(tmp_path):
    db = sqlite3.connect(tmp_path / "failed.db", factory=TimedConnection)
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        db.execute(
            "CREATE TABLE child(id INTEGER REFERENCES parent(id) "
            "DEFERRABLE INITIALLY DEFERRED)"
        )
        with pytest.raises(sqlite3.IntegrityError), db:
            db.execute("INSERT INTO child VALUES(7)")
        assert db.last_commit is None
        assert not db.in_transaction
        assert db.execute("SELECT count(*) FROM child").fetchone() == (0,)
    finally:
        db.close()


def event_record():
    path = (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    )
    event = json.loads(path.read_text())["fixtures"][0]["event"]
    return {
        "schema_version": 1,
        "node_id": event["node_id"],
        "source_epoch": event["source_epoch"],
        "export_generation": str(uuid4()),
        "export_seq": 1,
        "event_sha256": hashlib.sha256(canonical_bytes(event)).hexdigest(),
        "event": event,
    }


class Message:
    def __init__(self, record):
        self.subject = "edgecitadel.telemetry.v1." + record["node_id"]
        self.data = json.dumps(record).encode()
        self.acks = 0

    async def ack_sync(self, timeout):
        self.acks += 1


@pytest.mark.parametrize("observer_failure", [False, "internal", "callback"])
def test_real_ingestion_replay_callback_and_restoration(tmp_path, observer_failure):
    record = event_record()
    event = record["event"]
    observer = CommitObserver(
        node_id=event["node_id"],
        source_epoch=event["source_epoch"],
        trace_id=event["trace_id"],
    )
    if observer_failure == "internal":
        # Deliberately damage only the observer to exercise fail-open ingestion.
        class BrokenRecords(dict):
            def __contains__(self, key):
                raise RuntimeError("observer only")

        observer._records = BrokenRecords()
    elif observer_failure == "callback":

        def broken_observe(*args):
            raise RuntimeError("observer callback")

        observer.observe = broken_observe
    original_delivery = trace_collector.ingest_delivery
    path = tmp_path / "ingest.db"
    with instrument_collector(trace_collector, observer):
        ordinary = sqlite3.connect(":memory:")
        assert type(ordinary) is sqlite3.Connection
        ordinary.close()
        db = trace_collector.sqlite3.connect(path)
        trace_store.initialize(db)
        message = Message(record)
        callbacks = []

        def committed(result):
            with sqlite3.connect(path) as reader:
                assert reader.execute(
                    "SELECT ingest_seq FROM trace_raw_events WHERE event_id=?",
                    (event["event_id"],),
                ).fetchone() == (result.ingest_seq,)
            callbacks.append(result)

        try:
            asyncio.run(
                trace_collector.ingest_delivery(db, message, on_commit=committed)
            )
            first = observer.report()
            asyncio.run(
                trace_collector.ingest_delivery(db, message, on_commit=committed)
            )
            assert message.acks == 2
            assert len(callbacks) == 2
            assert callbacks[0].ingest_seq == callbacks[1].ingest_seq
            if not observer_failure:
                assert observer.report()["records"] == first["records"]
                assert len(first["records"]) == 1
                assert first["valid"]
            else:
                assert observer._failure == "observer_error"
        finally:
            db.close()
    assert trace_collector.sqlite3 is sqlite3
    assert trace_collector.ingest_delivery is original_delivery


def test_bounded_scope_overflow_and_nested_wiring():
    record = event_record()
    event = record["event"]
    observer = CommitObserver(
        node_id=event["node_id"],
        source_epoch=event["source_epoch"],
        trace_id=event["trace_id"],
        capacity=1,
    )
    from e2e.helpers.trace_commit_observer import CommitBracket

    db = SimpleNamespace(last_commit=CommitBracket(1, 2))
    result = SimpleNamespace(outcome="accepted", collector_epoch="epoch", ingest_seq=1)
    observer.observe(db, Message(record), result)
    event["event_id"] = str(uuid4())
    event["trace_id"] = "f" * 32
    observer.observe(db, Message(record), result)
    assert observer.report()["valid"]
    event["trace_id"] = observer.scope[2]
    observer.observe(db, Message(record), result)
    report = observer.report()
    assert len(report["records"]) == 1
    assert report["failure"] == "capacity_exceeded"
    with instrument_collector(trace_collector, observer):
        with pytest.raises(RuntimeError, match="already instrumented"):
            with instrument_collector(trace_collector, observer):
                pass
    assert trace_collector.sqlite3 is sqlite3


def test_ingestion_failure_has_no_ack_and_restores_wiring(tmp_path, monkeypatch):
    record = event_record()
    event = record["event"]
    observer = CommitObserver(
        node_id=event["node_id"],
        source_epoch=event["source_epoch"],
        trace_id=event["trace_id"],
    )
    message = Message(record)
    original_delivery = trace_collector.ingest_delivery

    def full(*args, **kwargs):
        raise sqlite3.OperationalError("database or disk is full")

    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        with instrument_collector(trace_collector, observer):
            db = trace_collector.sqlite3.connect(tmp_path / "full.db")
            trace_store.initialize(db)
            monkeypatch.setattr(trace_store.trace_capacity, "check", full)
            try:
                asyncio.run(trace_collector.ingest_delivery(db, message))
            finally:
                assert db.last_commit is None
                assert db.execute(
                    "SELECT count(*) FROM trace_raw_events"
                ).fetchone() == (0,)
                db.close()
    assert message.acks == 0
    assert observer.report()["records"] == []
    assert trace_collector.sqlite3 is sqlite3
    assert trace_collector.ingest_delivery is original_delivery
