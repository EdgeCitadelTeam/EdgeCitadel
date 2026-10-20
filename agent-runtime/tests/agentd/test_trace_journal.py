import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_journal import TraceJournal

FIXTURES = json.loads(
    (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
)["fixtures"]


def event():
    value = deepcopy(next(f["event"] for f in FIXTURES if f["name"] == "task"))
    value["event_id"] = str(uuid4())
    return value


@pytest.fixture
def store(tmp_path):
    value = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        yield value
    finally:
        value.close()


def write(store, value, selected=True):
    with store._lock, store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return TraceJournal(store._connection).record(
            "edge-a", value, selected=selected
        )


def counts(db):
    return tuple(
        db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("trace_sources", "trace_journal", "trace_spool")
    )


def test_selected_and_local_only_sequences_survive_reopen(store):
    a = write(store, event(), selected=False)
    b = write(store, event())
    c = write(store, event())
    assert [a["source_seq"], b["source_seq"], c["source_seq"]] == [1, 2, 3]
    assert len({a["source_epoch"], b["source_epoch"], c["source_epoch"]}) == 1
    assert [
        r[0]
        for r in store._connection.execute(
            "SELECT export_seq FROM trace_spool ORDER BY export_seq"
        )
    ] == [1, 2]
    reopened = AgentdStore(store.path)
    try:
        d = write(reopened, event())
        assert d["source_epoch"] == a["source_epoch"]
        assert d["source_seq"] == 4
        assert counts(reopened._connection) == (1, 4, 3)
    finally:
        reopened.close()


def test_retry_has_no_new_identity_or_position_and_conflict_rolls_back(store):
    value = event()
    original = write(store, value)
    assert write(store, value) == original
    assert counts(store._connection) == (1, 1, 1)
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        write(store, {**value, "phase": "completed"})
    assert counts(store._connection) == (1, 1, 1)
    assert write(store, event())["source_seq"] == 2


def test_spool_failure_rolls_back_task_journal_and_both_positions(store):
    task = store.create_task(
        sender_id="a", recipient_id="b", payload={}, queue_transport=False
    )
    db = store._connection
    db.execute(
        "CREATE TRIGGER owned_spool_fault BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT, 'owned spool fault'); END"
    )
    with (
        pytest.raises(sqlite3.IntegrityError, match="owned spool fault"),
        store._lock,
        db,
    ):
        db.execute(
            "UPDATE tasks SET state='running' WHERE task_id=?", (task["task_id"],)
        )
        TraceJournal(db).record("edge-a", event(), selected=True)
    assert store.get_task(task["task_id"])["state"] == task["state"]
    assert counts(db) == (0, 0, 0)
    db.execute("DROP TRIGGER owned_spool_fault")
    assert write(store, event())["source_seq"] == 1
    assert db.execute("SELECT export_seq FROM trace_spool").fetchone()[0] == 1


def test_invalid_metadata_and_transactionless_calls_cannot_persist(store):
    journal = TraceJournal(store._connection)
    with pytest.raises(TraceContractError, match="trace_transaction_required"):
        journal.record("edge-a", event(), selected=True)
    value = event()
    value["attributes"]["secret"] = "OWNED_SECRET_SENTINEL"
    with pytest.raises(TraceContractError):
        write(store, value)
    assert counts(store._connection) == (0, 0, 0)


def test_pending_spool_prevents_unmarked_journal_deletion(store):
    value = write(store, event())
    with pytest.raises(sqlite3.IntegrityError), store._connection:
        store._connection.execute(
            "DELETE FROM trace_journal WHERE event_id=?", (value["event_id"],)
        )
    assert counts(store._connection) == (1, 1, 1)


def test_populated_v6_upgrade_is_additive_and_failure_rolls_back(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    original = AgentdStore(path)
    task = original.create_task(
        sender_id="a", recipient_id="b", payload={"body": "owned"}
    )
    original.close()
    with sqlite3.connect(path) as db:
        for table in (
            "trace_collector_recovery",
            "trace_source_settlements",
            "trace_storage_usage",
            "trace_task_contexts",
            "trace_operations",
            "trace_requests",
            "trace_bindings",
            "trace_spool",
            "trace_journal",
            "trace_export_generations",
            "trace_sources",
        ):
            db.execute(f"DROP TABLE {table}")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(db)
        db.execute("PRAGMA user_version=6")
        outbox_before = db.execute("SELECT * FROM transport_outbox").fetchall()
    captured = []

    class FaultyStore(AgentdStore):
        def _execute_migration_sql(self, source):
            captured.append(self._connection)
            super()._execute_migration_sql(source)
            raise RuntimeError("owned schema failure")

    try:
        with pytest.raises(RuntimeError, match="owned schema failure"):
            FaultyStore(path)
    finally:
        for db in captured:
            db.close()
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 6
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'trace_%'"
        ).fetchall()
        assert db.execute("SELECT * FROM transport_outbox").fetchall() == outbox_before
    migrated = AgentdStore(path)
    try:
        assert migrated.get_task(task["task_id"]) == task
        assert counts(migrated._connection) == (0, 0, 0)
        assert migrated._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        assert migrated._connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert (
            migrated._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        )
    finally:
        migrated.close()
