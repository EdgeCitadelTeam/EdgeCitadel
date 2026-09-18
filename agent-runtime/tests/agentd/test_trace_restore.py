import json
import shutil
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_restore import rotate_restored_source
from edgecitadel_agentd.writer_lock import exclusive_writer


def event():
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    value = next(f["event"] for f in fixtures if f["name"] == "task")
    value["event_id"] = str(uuid4())
    return value


def write(store):
    with store._lock, store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return TraceJournal(store._connection).record("edge-a", event(), selected=True)


def rotate(store, epoch):
    with store._lock, store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return rotate_restored_source(
            store._connection, node_id="edge-a", expected_source_epoch=epoch
        )


def snapshot(db, epoch=None):
    tables = (
        ["trace_journal", "trace_spool"]
        if epoch
        else [
            "trace_sources",
            "trace_export_generations",
            "trace_journal",
            "trace_spool",
            "trace_storage_usage",
        ]
    )
    return {
        table: [
            tuple(row)
            for row in db.execute(
                f"SELECT * FROM {table}" + (" WHERE source_epoch=?" if epoch else ""),
                (epoch,) if epoch else (),
            )
        ]
        for table in tables
    }


def test_backup_rotation_preserves_replay_and_retry_across_restart(tmp_path):
    original_dir, restored_dir = tmp_path / "original", tmp_path / "restored"
    with exclusive_writer(original_dir), exclusive_writer(restored_dir):
        original = AgentdStore(original_dir / "agentd.sqlite3")
        try:
            first = write(original)
            epoch = first["source_epoch"]
            before = snapshot(original._connection, epoch)
            with sqlite3.connect(restored_dir / "agentd.sqlite3") as target:
                original._connection.backup(target)
            shutil.copy2(original_dir / "payload.key", restored_dir / "payload.key")
        finally:
            original.close()
        restored = AgentdStore(restored_dir / "agentd.sqlite3")
        try:
            marker = rotate(restored, epoch)
            assert marker["source_epoch"] != epoch
            assert marker["source_seq"] == 1
            assert marker["phase"] == "restored"
            assert marker["attributes"]["previous_source_epoch"] == epoch
            assert (
                marker["attributes"]["export_generation"]
                != marker["attributes"]["previous_export_generation"]
            )
            assert snapshot(restored._connection, epoch) == before
            fresh = write(restored)
            assert fresh["source_epoch"] == marker["source_epoch"]
            assert fresh["source_seq"] == 2
            assert [
                r[0]
                for r in restored._connection.execute(
                    "SELECT export_seq FROM trace_spool WHERE source_epoch=? ORDER BY export_seq",
                    (marker["source_epoch"],),
                )
            ] == [1, 2]
            after = snapshot(restored._connection)
        finally:
            restored.close()
        reopened = AgentdStore(restored_dir / "agentd.sqlite3")
        try:
            assert rotate(reopened, epoch) == marker
            assert snapshot(reopened._connection) == after
            assert (
                reopened._connection.execute("PRAGMA foreign_key_check").fetchall()
                == []
            )
            assert (
                reopened._connection.execute("PRAGMA integrity_check").fetchone()[0]
                == "ok"
            )
            next_marker = rotate(reopened, marker["source_epoch"])
            with pytest.raises(TraceContractError, match="restore_source_changed"):
                rotate(reopened, epoch)
            assert rotate(reopened, marker["source_epoch"]) == next_marker
        finally:
            reopened.close()


def test_failed_restore_marker_rolls_back_epoch_and_replay(tmp_path):
    with exclusive_writer(tmp_path):
        store = AgentdStore(tmp_path / "agentd.sqlite3")
        try:
            first = write(store)
            before = snapshot(store._connection)
            store._connection.execute(
                "CREATE TRIGGER owned_restore_fault BEFORE INSERT ON trace_spool "
                "BEGIN SELECT RAISE(ABORT, 'owned restore fault'); END"
            )
            with pytest.raises(sqlite3.IntegrityError, match="owned restore fault"):
                rotate(store, first["source_epoch"])
            assert snapshot(store._connection) == before
            store._connection.execute("DROP TRIGGER owned_restore_fault")
            marker = rotate(store, first["source_epoch"])
            assert marker["source_seq"] == 1
        finally:
            store.close()


def test_missing_source_and_transactionless_calls_do_not_allocate(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        with pytest.raises(TraceContractError, match="trace_transaction_required"):
            rotate_restored_source(
                store._connection, node_id="edge-a", expected_source_epoch=str(uuid4())
            )
        with pytest.raises(TraceContractError, match="restore_source_missing"):
            rotate(store, str(uuid4()))
        assert (
            store._connection.execute("SELECT COUNT(*) FROM trace_sources").fetchone()[
                0
            ]
            == 0
        )
    finally:
        store.close()
