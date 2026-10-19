import sqlite3
import threading
import time

import pytest
from test_restore import seed

from edgecitadel_agentd import service, storage_layout
from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.restore import stage_restore
from edgecitadel_agentd.restore_activation import (
    activate_restored_state,
    review_inventory,
)
from edgecitadel_agentd.storage_layout import StorageLayout
from edgecitadel_agentd.trace_quota import TraceQuotaError


@pytest.fixture(autouse=True)
def fixture_mount(monkeypatch):
    monkeypatch.setattr(StorageLayout, "mount", lambda self: None)


@pytest.fixture
def verified_layout(tmp_path, monkeypatch):
    state = tmp_path / "state/agentd"
    trace = state / "trace"
    trace.mkdir(parents=True, mode=0o700)
    calls = []

    def verify(trace_dir, task_dir):
        calls.append((trace_dir, task_dir))
        assert trace_dir == task_dir / "trace"

    monkeypatch.setattr(storage_layout, "verify_storage", verify)
    return StorageLayout(state), calls


def test_absent_enforcement_refuses_before_database_or_endpoint(tmp_path, monkeypatch):
    state = tmp_path / "agentd"

    def refuse(*args):
        raise TraceQuotaError("owned enforcement refusal")

    monkeypatch.setattr(storage_layout, "verify_storage", refuse)
    with pytest.raises(TraceQuotaError, match="owned enforcement refusal"):
        service.serve(state)
    assert {p.name for p in state.iterdir()} == {"writer.lock"}


def test_shared_state_requires_offline_migration(verified_layout):
    layout, calls = verified_layout
    old = layout.state_directory / "agentd.sqlite3"
    old.write_bytes(b"preserve existing authority")
    with pytest.raises(TraceQuotaError, match="offline migration"):
        layout.open()
    assert old.read_bytes() == b"preserve existing authority"
    assert not layout.trace_path.exists()
    assert calls == []


def test_daemon_and_sync_reopen_the_same_provisioned_pair(verified_layout, monkeypatch):
    layout, calls = verified_layout
    monkeypatch.setenv("EDGECITADEL_TRACE_SYNC", "1")
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve, args=(layout.state_directory, stop), daemon=True
    )
    thread.start()
    try:
        client = AgentdClient(service.socket_path_for(layout.state_directory))
        deadline = time.monotonic() + 5
        while not service.socket_path_for(layout.state_directory).exists():
            assert thread.is_alive() and time.monotonic() < deadline
            time.sleep(0.01)
        while client.call("health")["telemetry"]["state"] != "unconfigured":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        assert len(calls) >= 3  # Ownership check, command handle, telemetry handle.
        assert all(
            pair == (layout.trace_directory, layout.state_directory) for pair in calls
        )
        store = layout.open()
        try:
            task = store.create_task(
                sender_id="owned",
                recipient_id="remote",
                payload={"body": "encrypted task"},
            )
            assert (
                store.get_task(task["task_id"])["payload"]["body"] == "encrypted task"
            )
        finally:
            store.close()
        assert layout.key_path.is_file()
        assert not (layout.trace_directory / "payload.key").exists()
        with sqlite3.connect(layout.trace_path) as db:
            assert (
                db.execute(
                    "SELECT count(*) FROM sqlite_schema WHERE name='tasks'"
                ).fetchone()[0]
                == 0
            )
        with sqlite3.connect(layout.task_path) as db:
            assert db.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
    finally:
        stop.set()
        thread.join(timeout=10)
    assert not thread.is_alive()


def test_restore_and_activation_use_provisioned_paths(verified_layout, tmp_path):
    layout, _ = verified_layout
    old = tmp_path / "snapshot"
    epoch = seed(old)
    marker = stage_restore(
        snapshot_dir=old,
        previous_state_dir=old,
        destination_dir=layout.state_directory,
        node_id="edge-a",
        expected_source_epoch=epoch,
    )
    store = layout.open()
    try:
        review = review_inventory(store)
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        )
    finally:
        store.close()
    activate_restored_state(
        state_dir=layout.state_directory,
        previous_state_dir=old,
        node_id="edge-a",
        source_epoch=marker["source_epoch"],
        inventory_sha256=review["inventory_sha256"],
    )
    assert not (layout.state_directory / "restore-barrier.json").exists()
    assert not (layout.state_directory / "agentd.sqlite3").exists()
    assert (
        layout.trace_path.is_file()
        and layout.task_path.is_file()
        and layout.key_path.is_file()
    )
