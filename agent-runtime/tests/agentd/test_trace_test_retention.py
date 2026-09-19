import json
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_restore import rotate_restored_source
from edgecitadel_agentd.trace_retention import maintain_capacity


def event():
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    return {
        **next(f["event"] for f in fixtures if f["name"] == "task"),
        "event_id": str(uuid4()),
    }


def test_provenance_is_trusted_immutable_and_survives_restart_restore(tmp_path):
    path = tmp_path / "store.db"
    run = str(uuid4())
    store = AgentdStore(path)
    try:
        store.configure_test_source(node_id="test-node", test_run_id=run)
        with store._task_transaction():
            marked = TraceJournal(store._connection).record(
                "test-node", {**event(), "test_run_id": str(uuid4())}, selected=True
            )
            normal = TraceJournal(store._connection).record(
                "normal-node", {**event(), "test_run_id": run}, selected=True
            )
        assert marked["test_run_id"] == run
        assert "test_run_id" not in normal
        with pytest.raises(StoreError, match="cannot reclassify"):
            store.configure_test_source(node_id="normal-node", test_run_id=run)
        with pytest.raises(StoreError, match="cannot reclassify"):
            store.configure_test_source(node_id="test-node", test_run_id=str(uuid4()))
    finally:
        store.close()
    store = AgentdStore(path)
    try:
        store.configure_test_source(node_id="test-node", test_run_id=run)
        with store._task_transaction():
            restored = rotate_restored_source(
                store._connection,
                node_id="test-node",
                expected_source_epoch=marked["source_epoch"],
            )
        assert restored["test_run_id"] == run
        saved = json.loads(
            store._connection.execute(
                "SELECT event_json FROM trace_journal WHERE event_id=?",
                (marked["event_id"],),
            ).fetchone()[0]
        )
        assert saved == marked
    finally:
        store.close()


def test_settled_test_payloads_precede_older_normal_payloads(tmp_path, monkeypatch):
    store = AgentdStore(tmp_path / "store.db")
    try:
        store.configure_test_source(node_id="z-test", test_run_id=str(uuid4()))
        db = store._connection
        with store._task_transaction():
            for node in ("a-normal", "z-test"):
                for _ in range(70):
                    TraceJournal(db).record(node, event(), selected=True)
            db.execute(
                "UPDATE trace_journal SET received_at_ms=CASE node_id WHEN 'a-normal' THEN 1 ELSE 2 END"
            )
            collector = str(uuid4())
            db.execute(
                "UPDATE trace_spool SET state='core_settled',collector_epoch=?",
                (collector,),
            )
            db.execute(
                "INSERT INTO trace_source_settlements SELECT node_id,source_epoch,export_generation,?,next_export_seq-1,'{}' FROM trace_export_generations",
                (collector,),
            )
        # Exercise the physical trigger independently of encoded payload usage.
        monkeypatch.setattr(
            trace_capacity,
            "physical_storage",
            lambda db: {"pressure_bytes": trace_capacity.PHYSICAL_PRESSURE_BYTES},
        )
        with store._task_transaction():
            assert maintain_capacity(db, now_ms=10) == 64
        counts = dict(
            db.execute(
                "SELECT node_id,count(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='task' GROUP BY node_id"
            )
        )
        assert counts == {"a-normal": 70, "z-test": 6}
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


def test_pressure_preserves_active_and_unsettled_mandatory_test_events(
    tmp_path, monkeypatch
):
    (tmp_path / "node.json").write_text('{"agent_id":"test-node"}')
    store = AgentdStore(tmp_path / "agentd/store.db")
    try:
        store.configure_test_source(node_id="test-node", test_run_id=str(uuid4()))
        task = store.create_task(sender_id="sender", recipient_id="worker", payload={})
        db = store._connection
        original = db.execute("SELECT event_json FROM trace_journal").fetchone()[0]
        monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 1)
        with store._task_transaction():
            assert maintain_capacity(db, now_ms=100) == 0
        assert (
            db.execute("SELECT event_json FROM trace_journal").fetchone()[0] == original
        )
        store.transition_task(
            task_id=task["task_id"], state="cancelled", actor_id="sender"
        )
        with store._task_transaction():
            assert maintain_capacity(db, now_ms=100) == 0
        assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 2
    finally:
        store.close()


def test_test_payloads_expire_after_one_day_without_expiring_normal(tmp_path):
    from edgecitadel_agentd.trace_retention import TEST_RETENTION_MS

    store = AgentdStore(tmp_path / "store.db")
    try:
        store.configure_test_source(node_id="test-node", test_run_id=str(uuid4()))
        with store._task_transaction():
            for node in ("test-node", "normal-node"):
                # Non-exported closed fixture observations need no settlement.
                TraceJournal(store._connection).record(node, event(), selected=False)
            store._connection.execute("UPDATE trace_journal SET received_at_ms=1")
        with store._task_transaction():
            assert (
                maintain_capacity(
                    store._connection, now_ms=TEST_RETENTION_MS + 1, expire_before_ms=0
                )
                == 0
            )
        with store._task_transaction():
            assert (
                maintain_capacity(
                    store._connection, now_ms=TEST_RETENTION_MS + 2, expire_before_ms=0
                )
                == 1
            )
        assert (
            store._connection.execute(
                "SELECT node_id FROM trace_journal WHERE json_extract(event_json,'$.kind')='task'"
            ).fetchone()[0]
            == "normal-node"
        )
    finally:
        store.close()


def test_cleanup_failure_does_not_undo_task_recovery(tmp_path, monkeypatch):
    import sqlite3
    from edgecitadel_agentd import store as store_module

    store = AgentdStore(tmp_path / "store.db")
    try:
        task = store.create_task(
            sender_id="sender",
            recipient_id="worker",
            payload={},
            deadline_at_ms=store_module._now_ms() + 1000,
        )

        def fail(*args, **kwargs):
            raise sqlite3.OperationalError("owned cleanup failure")

        monkeypatch.setattr(store_module, "maintain_capacity", fail)
        with pytest.raises(sqlite3.OperationalError, match="owned cleanup failure"):
            store.reconcile(now_ms=store_module._now_ms() + 2000)
        assert store.get_task(task["task_id"])["state"] == "expired"
    finally:
        store.close()


def test_daemon_environment_configures_provenance_without_rpc_authority(
    tmp_path, monkeypatch
):
    import sqlite3
    import threading
    from edgecitadel_agentd.client import AgentdClient, AgentdClientError
    from edgecitadel_agentd.service import serve, socket_path_for

    run = str(uuid4())
    monkeypatch.setenv("EDGECITADEL_TRACE_TEST_RUN_ID", run)
    (tmp_path / "node.json").write_text('{"agent_id":"dev-node"}')
    directory = tmp_path / "agentd"
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(directory, stop), daemon=True)
    thread.start()
    try:
        socket = socket_path_for(directory)
        for _ in range(200):
            if socket.exists():
                break
            stop.wait(0.01)
        else:
            pytest.fail("service did not start")
        with sqlite3.connect(directory / "agentd.sqlite3") as db:
            assert (
                db.execute(
                    "SELECT test_run_id FROM trace_sources WHERE active=1"
                ).fetchone()[0]
                == run
            )
        client = AgentdClient(
            socket, admin_token=(directory / "admin.token").read_text().strip()
        )
        with pytest.raises(AgentdClientError):
            client.call(
                "trace.configure_test_source",
                node_id="dev-node",
                test_run_id=str(uuid4()),
            )
    finally:
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_v22_upgrade_keeps_existing_data_normal(tmp_path):
    path = tmp_path / "store.db"
    store = AgentdStore(path)
    try:
        with store._task_transaction():
            original = TraceJournal(store._connection).record(
                "ordinary", event(), selected=True
            )
            store._connection.execute(
                "ALTER TABLE trace_sources DROP COLUMN test_run_id"
            )
            flatten_connection(store._connection)
            store._connection.execute("PRAGMA user_version=22")
    finally:
        store.close()
    store = AgentdStore(path)
    try:
        assert (
            store._connection.execute(
                "SELECT test_run_id FROM trace_sources"
            ).fetchone()[0]
            is None
        )
        assert (
            json.loads(
                store._connection.execute(
                    "SELECT event_json FROM trace_journal"
                ).fetchone()[0]
            )
            == original
        )
        with pytest.raises(StoreError, match="cannot reclassify"):
            store.configure_test_source(node_id="ordinary", test_run_id=str(uuid4()))
    finally:
        store.close()


def test_fresh_test_sources_do_not_hide_eligible_ordinary_data(tmp_path):
    store = AgentdStore(tmp_path / "store.db")
    try:
        for i in range(9):
            store.configure_test_source(node_id=f"test-{i}", test_run_id=str(uuid4()))
        with store._task_transaction():
            for i in range(9):
                TraceJournal(store._connection).record(
                    f"test-{i}", event(), selected=False
                )
            old = TraceJournal(store._connection).record(
                "ordinary", event(), selected=False
            )
            store._connection.execute(
                "UPDATE trace_journal SET received_at_ms=1 WHERE event_id=?",
                (old["event_id"],),
            )
        with store._task_transaction():
            assert (
                maintain_capacity(store._connection, now_ms=100, expire_before_ms=2)
                == 1
            )
        assert (
            store._connection.execute(
                "SELECT count(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='task'"
            ).fetchone()[0]
            == 9
        )
    finally:
        store.close()
