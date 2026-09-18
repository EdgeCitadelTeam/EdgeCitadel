import sqlite3

import pytest
from test_restore import rejected_start, seed
from test_writer_lock import start, stop

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.restore import RESTORE_BARRIER, stage_restore
from edgecitadel_agentd.restore_activation import (
    activate_restored_state,
    review_inventory,
)
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.store import AgentdStore, StoreError


def staged(tmp_path):
    old, new = tmp_path / "old", tmp_path / "new"
    epoch = seed(old)
    marker = stage_restore(
        snapshot_dir=old,
        previous_state_dir=old,
        destination_dir=new,
        node_id="edge-a",
        expected_source_epoch=epoch,
    )
    store = AgentdStore(new / "agentd.sqlite3")
    try:
        review = review_inventory(store)
    finally:
        store.close()
    return old, new, marker, review


def activate(old, new, marker, review):
    return activate_restored_state(
        state_dir=new,
        previous_state_dir=old,
        node_id="edge-a",
        source_epoch=marker["source_epoch"],
        inventory_sha256=review["inventory_sha256"],
    )


def test_activated_daemon_accepts_fresh_work_and_keeps_old_work_held(tmp_path):
    old, new, marker, review = staged(tmp_path)
    event = activate(old, new, marker, review)
    assert event["kind"] == "coverage" and event["phase"] == "unknown"
    assert "lost_export_ranges" not in event["attributes"]
    assert event["source_epoch"] == marker["source_epoch"]
    assert event["causes"][0]["event_id"] == marker["event_id"]
    assert activate(old, new, marker, review) == event
    rejected_start(old)
    process = start(new)
    try:
        admin = AgentdClient(
            socket_path_for(new), admin_token=(new / "admin.token").read_text().strip()
        )
        registration = admin.call(
            "connector.register",
            connector_id="remote",
            host_type="codex",
            agent_id="remote",
            capabilities=[
                "edgecitadel_delegate",
                "edgecitadel_task_status",
                "edgecitadel_task_update",
            ],
        )
        client = AgentdClient(
            socket_path_for(new), connector_id="remote", token=registration["token"]
        )
        client.call("session.open")
        fresh = client.call(
            "task.create", recipient_id="remote", payload={"body": "owned fresh work"}
        )
        assert fresh["state"] == "queued" and "restore_status" not in fresh
        with sqlite3.connect(new / "agentd.sqlite3") as db:
            old_task = db.execute(
                "SELECT object_id FROM restore_holds WHERE kind='task'"
            ).fetchone()[0]
            assert (
                db.execute(
                    "SELECT published_at_ms FROM transport_outbox WHERE message_id IN (SELECT object_id FROM restore_holds WHERE kind='transport')"
                ).fetchone()[0]
                is None
            )
        held = client.call("task.get", task_id=old_task)
        assert held["restore_status"] == "reconciliation_required"
        with pytest.raises(AgentdClientError, match="restore reconciliation"):
            client.call("task.transition", task_id=old_task, state="cancelled")
    finally:
        stop(process)


def test_changed_review_or_unretired_source_cannot_activate(tmp_path):
    old, new, marker, review = staged(tmp_path)
    with pytest.raises(StoreError, match="inventory changed"):
        activate(old, new, marker, {**review, "inventory_sha256": "0" * 64})
    assert (new / RESTORE_BARRIER).exists()
    (old / RESTORE_BARRIER).write_text('{"state":"reconciliation_required"}')
    with pytest.raises(StoreError, match="not retired"):
        activate(old, new, marker, review)
    rejected_start(new)


def test_post_commit_failure_retries_one_coverage_event(tmp_path, monkeypatch):
    old, new, marker, review = staged(tmp_path)
    from edgecitadel_agentd import restore_activation

    remove = restore_activation._remove_barrier

    def fail(_directory):
        raise OSError("owned barrier removal failure")

    monkeypatch.setattr(restore_activation, "_remove_barrier", fail)
    with pytest.raises(OSError, match="owned barrier"):
        activate(old, new, marker, review)
    rejected_start(new)
    with sqlite3.connect(new / "agentd.sqlite3") as db:
        before = db.execute(
            "SELECT event_id,event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchall()
        assert len(before) == 1
        assert db.execute("SELECT COUNT(*) FROM restore_activations").fetchone()[0] == 1
    monkeypatch.setattr(restore_activation, "_remove_barrier", remove)
    assert activate(old, new, marker, review)["event_id"] == before[0][0]
    with sqlite3.connect(new / "agentd.sqlite3") as db:
        assert (
            db.execute(
                "SELECT event_id,event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            ).fetchall()
            == before
        )


def test_coverage_failure_preserves_barrier_and_has_no_activation_receipt(tmp_path):
    old, new, marker, review = staged(tmp_path)
    with sqlite3.connect(new / "agentd.sqlite3") as db:
        db.execute(
            "CREATE TRIGGER owned_activation_failure BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT,'owned activation fault'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="owned activation fault"):
        activate(old, new, marker, review)
    rejected_start(new)
    with sqlite3.connect(new / "agentd.sqlite3") as db:
        assert db.execute("SELECT COUNT(*) FROM restore_activations").fetchone()[0] == 0
        assert (
            db.execute(
                "SELECT COUNT(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("kind", ["task", "session"])
def test_unheld_execution_state_prevents_activation(tmp_path, kind):
    old, new, marker, review = staged(tmp_path)
    store = AgentdStore(new / "agentd.sqlite3")
    try:
        if kind == "task":
            store.create_task(
                sender_id="worker",
                recipient_id="worker",
                payload={},
                queue_transport=False,
            )
        else:
            token = store.register_connector(
                connector_id="new", host_type="codex", agent_id="new", capabilities=[]
            )
            store.open_session(connector_id="new", token=token)
    finally:
        store.close()
    with pytest.raises(StoreError, match="not fully held"):
        activate(old, new, marker, review)
    rejected_start(new)


def test_v13_migration_preserves_holds_and_adds_empty_activation_receipts(tmp_path):
    _old, new, _marker, review = staged(tmp_path)
    with sqlite3.connect(new / "agentd.sqlite3") as db:
        before = db.execute("SELECT * FROM restore_holds").fetchall()
        db.execute("DROP TABLE restore_activations")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        db.execute("PRAGMA user_version=13")
    store = AgentdStore(new / "agentd.sqlite3")
    try:
        assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 23
        assert [
            tuple(r) for r in store._connection.execute("SELECT * FROM restore_holds")
        ] == before
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM restore_activations"
            ).fetchone()[0]
            == 0
        )
        assert review_inventory(store) == review
    finally:
        store.close()
