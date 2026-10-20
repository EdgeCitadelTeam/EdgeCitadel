"""Opt-in native macOS qualification; fault injection touches only owned images."""

import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from test_storage_upgrade import legacy, verify
from test_trace_filesystem_full import exhaust

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import serve, socket_path_for
from edgecitadel_agentd.storage_layout import StorageLayout
from edgecitadel_agentd.storage_macos import (
    IMAGE,
    MANIFEST,
    IMAGE_BYTES,
    setup_macos_storage,
    mount_macos_storage,
)
from edgecitadel_agentd.storage_migration import migrate_storage
from edgecitadel_agentd.trace_quota import TraceQuotaError
from edgecitadel_agentd.writer_lock import exclusive_writer

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin" or os.environ.get("RUN_AGENTD_MACOS_STORAGE") != "1",
    reason="requires explicit owned macOS native storage opt-in",
)


@pytest.fixture
def volume(tmp_path):
    state = tmp_path.resolve() / "agentd"
    state.mkdir(mode=0o700)
    with exclusive_writer(state):
        setup_macos_storage(state)
    try:
        yield StorageLayout(state)
    finally:
        if os.path.ismount(state / "trace"):
            subprocess.run(
                ["/usr/bin/hdiutil", "detach", str(state / "trace")],
                check=True,
                capture_output=True,
                timeout=30,
            )


def detach(layout):
    subprocess.run(
        ["/usr/bin/hdiutil", "detach", str(layout.trace_directory)],
        check=True,
        capture_output=True,
        timeout=30,
    )


def test_native_reuse_remount_missing_and_substitution_refusal(volume):
    state = volume.state_directory
    original = volume.verify()
    assert setup_macos_storage(state)["volume_uuid"] == original["volume_uuid"]
    detach(volume)
    with pytest.raises(TraceQuotaError, match="not mounted"):
        volume.open()
    assert not volume.trace_path.exists()
    mount_macos_storage(state)
    assert volume.verify()["volume_uuid"] == original["volume_uuid"]
    manifest = state / MANIFEST
    record = json.loads(manifest.read_text())
    changed = dict(record, volume_uuid="wrong-owned-fixture")
    manifest.write_text(json.dumps(changed))
    with pytest.raises(TraceQuotaError, match="identity"):
        volume.open()
    manifest.write_text(json.dumps(record))
    image = state / IMAGE
    image.chmod(0o644)
    with pytest.raises(TraceQuotaError, match="private"):
        volume.open()
    image.chmod(0o600)
    detach(volume)
    with image.open("ab") as target:
        target.write(b"x" * 4096)
    with pytest.raises(TraceQuotaError, match="sized"):
        mount_macos_storage(state)
    with image.open("r+b") as target:
        target.truncate(IMAGE_BYTES)
    mount_macos_storage(state)


def test_native_shared_upgrade_and_daemon_restart(volume):
    state = volume.state_directory
    _, before = legacy(state)
    migrate_storage(state)
    verify(volume, before)
    detach(volume)
    for _ in range(2):
        stop = threading.Event()
        thread = threading.Thread(target=serve, args=(state, stop), daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 15
            while not socket_path_for(state).exists():
                assert thread.is_alive() and time.monotonic() < deadline
                time.sleep(0.02)
            health = AgentdClient(socket_path_for(state)).call("health")
            assert health["schema_version"] == 29
            assert health["storage_backend"]["mount_verified"] is True
            assert health["storage_backend"]["physical_limit_bytes"] == IMAGE_BYTES
        finally:
            stop.set()
            thread.join(15)
        assert not thread.is_alive()


def test_native_full_attached_transaction_rolls_back_and_recovers(volume):
    store = volume.open()
    try:
        task = store.create_task(
            sender_id="owner", recipient_id="worker", payload={"body": "owned"}
        )
        before = store.get_task(task["task_id"])
        outbox = store._connection.execute(
            "SELECT count(*) FROM transport_outbox"
        ).fetchone()[0]
        filler = exhaust(volume.trace_directory)
        assert volume.verify()["available_bytes"] < 4096
        # A failed task+trace commit must not acknowledge a task result or enqueue
        # outbound work. The caller retries only after releasing owned pressure.
        with pytest.raises(sqlite3.Error) as error:
            store.transition_task(
                task_id=task["task_id"],
                state="cancelled",
                actor_id="owner",
                reason="owned",
            )
        assert error.value.sqlite_errorcode in {
            sqlite3.SQLITE_FULL,
            sqlite3.SQLITE_CANTOPEN,
        }
        assert store.get_task(task["task_id"]) == before
        assert (
            store._connection.execute(
                "SELECT count(*) FROM transport_outbox"
            ).fetchone()[0]
            == outbox
        )
        filler.unlink()
        store.transition_task(
            task_id=task["task_id"], state="cancelled", actor_id="owner", reason="owned"
        )
        assert store.get_task(task["task_id"])["state"] == "cancelled"
        for schema in ("main", "task_state"):
            assert (
                store._connection.execute(
                    f"PRAGMA {schema}.integrity_check"
                ).fetchone()[0]
                == "ok"
            )
    finally:
        store.close()


def test_native_wrong_backing_volume_refuses_without_database_creation(
    volume, tmp_path
):
    other = tmp_path.resolve() / "other"
    other.mkdir(mode=0o700)
    setup_macos_storage(other)
    subprocess.run(
        ["/usr/bin/hdiutil", "detach", str(other / "trace")],
        check=True,
        capture_output=True,
        timeout=30,
    )
    detach(volume)
    subprocess.run(
        [
            "/usr/bin/hdiutil",
            "attach",
            "-nobrowse",
            "-owners",
            "on",
            "-mountpoint",
            str(volume.trace_directory),
            str(other / IMAGE),
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    with pytest.raises(TraceQuotaError, match="backing image"):
        volume.open()
    assert not volume.trace_path.exists()
    detach(volume)
    mount_macos_storage(volume.state_directory)


def test_native_process_interruption_rolls_back_attached_pair(volume):
    store = volume.open()
    task = store.create_task(
        sender_id="owner", recipient_id="worker", payload={"body": "owned"}
    )
    store.close()
    code = """
import os
from pathlib import Path
from edgecitadel_agentd.storage_layout import StorageLayout
store = StorageLayout(Path(os.environ["OWNED_STATE"])).open()
db = store._connection
db.execute("BEGIN IMMEDIATE")
db.execute("UPDATE tasks SET state='completed'")
db.execute("UPDATE events SET event_type='uncommitted'")
os._exit(79)
"""
    child = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        env={
            **os.environ,
            "OWNED_STATE": str(volume.state_directory),
            "PYTHONPATH": str(Path(__file__).parents[2] / "src"),
        },
        capture_output=True,
        timeout=30,
    )
    assert child.returncode == 79, child.stderr
    store = volume.open()
    try:
        assert store.get_task(task["task_id"])["state"] == task["state"]
        assert (
            store._connection.execute(
                "SELECT count(*) FROM events WHERE event_type='uncommitted'"
            ).fetchone()[0]
            == 0
        )
        for schema in ("main", "task_state"):
            assert (
                store._connection.execute(
                    f"PRAGMA {schema}.integrity_check"
                ).fetchone()[0]
                == "ok"
            )
    finally:
        store.close()
