import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

from storage_test_support import flatten_connection, paired_connect, stage_restore

import pytest

from edgecitadel_agentd.restore import (
    RESTORE_BARRIER,
    hold_restored_execution,
)
from edgecitadel_agentd.service import PROCESS_STATE_NAME, socket_path_for
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer


def seed(directory):
    with exclusive_writer(directory):
        store = AgentdStore(directory / "agentd.sqlite3")
        try:
            token = store.register_connector(
                connector_id="worker",
                host_type="codex",
                agent_id="worker",
                capabilities=[],
            )
            session = store.open_session(connector_id="worker", token=token)
            task = store.create_task(
                sender_id="worker",
                recipient_id="remote",
                payload={"body": "owned restore content"},
            )
            with store._connection:
                store._connection.execute(
                    "UPDATE tasks SET state='accepted', claimed_session_id=? WHERE task_id=?",
                    (session["session_id"], task["task_id"]),
                )
                epoch, _ = TraceJournal(store._connection).initialize("edge-a")
            return epoch
        finally:
            store.close()


def rows(directory):
    with paired_connect(directory / "agentd.sqlite3") as db:
        return {
            t: db.execute(f"SELECT * FROM {t}").fetchall()
            for t in ("sessions", "tasks", "transport_outbox")
        }


def rejected_start(directory):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "edgecitadel_agentd.service",
            "--state-dir",
            str(directory),
        ],
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).parents[2] / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 1
    assert "restore reconciliation is required" in result.stderr
    assert not socket_path_for(directory).exists()
    assert not (directory / PROCESS_STATE_NAME).exists()


def test_staged_restore_preserves_uncertain_work_and_fences_both_directories(tmp_path):
    previous, destination = tmp_path / "old", tmp_path / "new"
    epoch = seed(previous)
    before = rows(previous)
    marker = stage_restore(
        snapshot_dir=previous,
        previous_state_dir=previous,
        destination_dir=destination,
        node_id="edge-a",
        expected_source_epoch=epoch,
    )
    assert marker["source_epoch"] != epoch
    assert rows(previous) == before
    restored = rows(destination)
    assert restored["tasks"] == before["tasks"]
    assert restored["transport_outbox"] == before["transport_outbox"]
    assert all(row[-1] is not None for row in restored["sessions"])
    assert json.loads((previous / RESTORE_BARRIER).read_text())["state"] == "retired"
    assert (
        json.loads((destination / RESTORE_BARRIER).read_text())["state"]
        == "reconciliation_required"
    )
    rejected_start(previous)
    rejected_start(destination)
    assert rows(previous) == before
    restored = rows(destination)
    assert restored["tasks"] == before["tasks"]
    assert restored["transport_outbox"] == before["transport_outbox"]
    assert all(row[-1] is not None for row in restored["sessions"])


def test_active_writer_prevents_any_restore_destination(tmp_path):
    previous, destination = tmp_path / "old", tmp_path / "new"
    epoch = seed(previous)
    with exclusive_writer(previous), pytest.raises(WriterActiveError):
        stage_restore(
            snapshot_dir=previous,
            previous_state_dir=previous,
            destination_dir=destination,
            node_id="edge-a",
            expected_source_epoch=epoch,
        )
    assert not destination.exists()
    assert not (previous / RESTORE_BARRIER).exists()


def test_wrong_key_keeps_old_directory_usable_and_failed_copy_barred(tmp_path):
    previous, snapshot, unrelated, destination = [
        tmp_path / n for n in ("old", "snapshot", "unrelated", "new")
    ]
    epoch = seed(previous)
    seed(unrelated)
    shutil.copytree(previous, snapshot)
    shutil.copyfile(unrelated / "payload.key", snapshot / "payload.key")
    before = rows(previous)
    with pytest.raises(StoreError, match="could not be decrypted"):
        stage_restore(
            snapshot_dir=snapshot,
            previous_state_dir=previous,
            destination_dir=destination,
            node_id="edge-a",
            expected_source_epoch=epoch,
        )
    assert not (previous / RESTORE_BARRIER).exists()
    assert rows(previous) == before
    rejected_start(destination)


@pytest.mark.parametrize("damage", ["missing", "mismatched"])
def test_incomplete_pair_keeps_previous_authority_and_destination_barred(
    tmp_path, damage
):
    previous, snapshot, unrelated, destination = [
        tmp_path / n for n in ("old", "snapshot", "unrelated", "new")
    ]
    epoch = seed(previous)
    shutil.copytree(previous, snapshot)
    task_file = snapshot / "agentd-tasks.sqlite3"
    if damage == "missing":
        task_file.unlink()
    else:
        seed(unrelated)
        shutil.copyfile(unrelated / task_file.name, task_file)
    before = rows(previous)
    with pytest.raises(sqlite3.DatabaseError):
        stage_restore(
            snapshot_dir=snapshot,
            previous_state_dir=previous,
            destination_dir=destination,
            node_id="edge-a",
            expected_source_epoch=epoch,
        )
    assert not (previous / RESTORE_BARRIER).exists()
    assert rows(previous) == before
    rejected_start(destination)
    if damage == "missing":
        assert not task_file.exists()


def test_incomplete_barrier_fails_closed_before_store_open(tmp_path):
    state = tmp_path / "barred"
    state.mkdir()
    (state / RESTORE_BARRIER).touch()
    rejected_start(state)
    assert not (state / "agentd.sqlite3").exists()


def test_holds_survive_reopen_and_block_old_work_but_allow_fresh_work(tmp_path):
    previous, destination = tmp_path / "old", tmp_path / "new"
    epoch = seed(previous)
    marker = stage_restore(
        snapshot_dir=previous,
        previous_state_dir=previous,
        destination_dir=destination,
        node_id="edge-a",
        expected_source_epoch=epoch,
    )
    store = AgentdStore(destination / "agentd.sqlite3")
    try:
        token = store.register_connector(
            connector_id="local", host_type="codex", agent_id="local", capabilities=[]
        )
        session = store.open_session(connector_id="local", token=token)
        queued = store.create_task(
            sender_id="local", recipient_id="local", payload={}, queue_transport=False
        )
        old_task = store._connection.execute(
            "SELECT task_id FROM tasks WHERE state='accepted'"
        ).fetchone()[0]
        old_message = store._connection.execute(
            "SELECT message_id FROM transport_outbox"
        ).fetchone()[0]
        assert hold_restored_execution(store, source_epoch=marker["source_epoch"]) == {
            "task": 2,
            "transport": 1,
        }
        assert hold_restored_execution(store, source_epoch=marker["source_epoch"]) == {
            "task": 2,
            "transport": 1,
        }
        with pytest.raises(StoreError, match="active session"):
            store.renew_session(
                connector_id="local", token=token, session_id=session["session_id"]
            )
        assert store.get_task(old_task)["restore_status"] == "reconciliation_required"
        with pytest.raises(StoreError, match="restore reconciliation"):
            store.transition_task(task_id=old_task, state="running", actor_id="remote")
        assert store.pending_transport() == []
        with pytest.raises(StoreError, match="pending transport"):
            store.mark_transport_published(old_message)
        with store._connection:
            store._connection.execute(
                "UPDATE tasks SET deadline_at_ms=1 WHERE task_id=?", (old_task,)
            )
        store.reconcile()
        assert store.get_task(old_task)["state"] == "accepted"
        assert store.get_task(str(queued["task_id"]))["state"] == "queued"
    finally:
        store.close()
    store = AgentdStore(destination / "agentd.sqlite3")
    try:
        assert store.pending_transport() == []
        fresh_session = store.open_session(connector_id="local", token=token)
        assert (
            store.claim_next_task(
                connector_id="local",
                token=token,
                session_id=fresh_session["session_id"],
            )
            is None
        )
        fresh = store.create_task(
            sender_id="local", recipient_id="local", payload={}, queue_transport=False
        )
        claimed = store.claim_next_task(
            connector_id="local", token=token, session_id=fresh_session["session_id"]
        )
        assert claimed["task_id"] == fresh["task_id"]
        assert "restore_status" not in claimed
        outbound = store.create_task(
            sender_id="local", recipient_id="other", payload={}
        )
        assert [row["task_id"] for row in store.pending_transport()] == [
            outbound["task_id"]
        ]
    finally:
        store.close()


def test_execution_hold_requires_a_staged_restore(tmp_path):
    epoch = seed(tmp_path)
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        with pytest.raises(StoreError, match="staging barrier"):
            hold_restored_execution(store, source_epoch=epoch)
        assert (
            store._connection.execute("SELECT COUNT(*) FROM restore_holds").fetchone()[
                0
            ]
            == 0
        )
    finally:
        store.close()


def test_hold_failure_rolls_back_sessions_and_keeps_old_directory_unretired(tmp_path):
    previous, destination = tmp_path / "old", tmp_path / "new"
    epoch = seed(previous)
    before = rows(previous)
    with paired_connect(previous / "agentd.sqlite3") as db:
        db.execute(
            "CREATE TRIGGER task_state.owned_hold_failure BEFORE UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'owned hold fault'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="owned hold fault"):
        stage_restore(
            snapshot_dir=previous,
            previous_state_dir=previous,
            destination_dir=destination,
            node_id="edge-a",
            expected_source_epoch=epoch,
        )
    assert not (previous / RESTORE_BARRIER).exists()
    assert rows(previous) == rows(destination) == before
    with paired_connect(destination / "agentd.sqlite3") as db:
        assert db.execute("SELECT COUNT(*) FROM restore_holds").fetchone()[0] == 0
    rejected_start(destination)


def test_v12_upgrade_preserves_work_and_creates_empty_holds(tmp_path):
    seed(tmp_path)
    before = rows(tmp_path)
    with paired_connect(tmp_path / "agentd.sqlite3") as db:
        db.execute("DROP TABLE restore_holds")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(db)
        db.execute("PRAGMA user_version=12")
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 28
        assert (
            store._connection.execute("SELECT COUNT(*) FROM restore_holds").fetchone()[
                0
            ]
            == 0
        )
        assert rows(tmp_path) == before
        assert len(store.pending_transport()) == 1
    finally:
        store.close()
