import json
import sqlite3
from pathlib import Path

import pytest
from storage_test_support import flatten_connection
from test_restore import seed

from edgecitadel_agentd import storage_migration, storage_upgrade
from edgecitadel_agentd.restore import (
    RESTORE_BARRIER,
    require_startable,
    RestorePendingError,
)
from edgecitadel_agentd.storage_layout import StorageLayout
from edgecitadel_agentd.store import AgentdStore, StoreError


def legacy(state):
    seed(state)
    layout = StorageLayout(state)
    # A real shared schema-6 fixture with sessions, encrypted tasks/results and
    # queued transport messages. Keep the exact old ciphertext and row identities.
    store = AgentdStore(state / "agentd.sqlite3")
    with store._connection:
        store._connection.execute("UPDATE tasks SET result_json=payload_json")
    store.close()
    with sqlite3.connect(state / "agentd.sqlite3") as db:
        flatten_connection(db)
        for (name,) in db.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'trace_%'"
        ).fetchall():
            db.execute(f'DROP TABLE "{name}"')
        db.execute("PRAGMA user_version=6")
        before = {
            name: db.execute(f'SELECT * FROM "{name}"').fetchall()
            for name in ("tasks", "sessions", "connectors", "transport_outbox")
        }
    layout.task_path.unlink()
    return layout, before


@pytest.fixture
def shared(tmp_path, monkeypatch):
    layout, before = legacy(tmp_path / "agentd")
    layout.trace_directory.mkdir(mode=0o700)
    monkeypatch.setattr(storage_migration, "verify_storage", lambda *args: None)
    return layout, before


def verify(layout, before):
    store = AgentdStore(
        layout.trace_path, task_path=layout.task_path, payload_key_path=layout.key_path
    )
    try:
        for name, rows in before.items():
            assert [
                tuple(row)
                for row in store._connection.execute(f'SELECT * FROM "{name}"')
            ] == rows
        task_id = store._connection.execute("SELECT task_id FROM tasks").fetchone()[0]
        task = store.get_task(task_id)
        assert task["payload"] == task["result"] == {"body": "owned restore content"}
        assert store.health()["schema_version"] == 29
    finally:
        store.close()
    require_startable(layout.state_directory)
    assert not (layout.state_directory / "agentd.sqlite3").exists()
    assert not (layout.state_directory / "storage-upgrade").exists()


def test_populated_shared_upgrade_preserves_content_and_identity(shared):
    layout, before = shared
    key = layout.key_path.read_bytes()
    storage_migration.migrate_storage(layout.state_directory)
    assert layout.key_path.read_bytes() == key
    verify(layout, before)


@pytest.mark.parametrize("point", ["staging", "ready", "tasks", "source", "cleanup"])
def test_interrupted_shared_upgrade_resumes(shared, monkeypatch, point):
    layout, before = shared
    save, replace, unlink = (
        storage_upgrade._save,
        storage_upgrade.os.replace,
        Path.unlink,
    )

    def interrupted_save(state, record):
        save(state, record)
        if record["phase"] == point:
            raise RuntimeError("owned interruption")

    def interrupted_replace(source, target):
        replace(source, target)
        if point == "tasks" and target == layout.task_path:
            raise RuntimeError("owned interruption")

    def interrupted_unlink(path, *args, **kwargs):
        unlink(path, *args, **kwargs)
        if (
            point == "source" and path == layout.state_directory / "agentd.sqlite3"
        ) or (
            point == "cleanup"
            and path == layout.state_directory / "storage-upgrade/agentd.sqlite3"
        ):
            raise RuntimeError("owned interruption")

    with monkeypatch.context() as patch:
        patch.setattr(storage_upgrade, "_save", interrupted_save)
        patch.setattr(storage_upgrade.os, "replace", interrupted_replace)
        patch.setattr(Path, "unlink", interrupted_unlink)
        with pytest.raises(RuntimeError, match="owned interruption"):
            storage_migration.migrate_storage(layout.state_directory)
    with pytest.raises(RestorePendingError):
        require_startable(layout.state_directory)
    storage_migration.migrate_storage(layout.state_directory)
    verify(layout, before)


def test_changed_staged_pair_stays_fenced(shared, monkeypatch):
    layout, _ = shared
    original = storage_upgrade._save

    def interrupt(state, record):
        original(state, record)
        if record["phase"] == "ready":
            raise RuntimeError("owned interruption")

    with monkeypatch.context() as patch:
        patch.setattr(storage_upgrade, "_save", interrupt)
        with pytest.raises(RuntimeError):
            storage_migration.migrate_storage(layout.state_directory)
    with sqlite3.connect(
        layout.state_directory / "storage-upgrade/agentd-tasks.sqlite3"
    ) as db:
        db.execute(
            "UPDATE storage_pair SET pair_id='00000000-0000-0000-0000-000000000000'"
        )
    with pytest.raises(StoreError, match="task database changed"):
        storage_migration.migrate_storage(layout.state_directory)
    assert (
        json.loads((layout.state_directory / RESTORE_BARRIER).read_text())["phase"]
        == "ready"
    )
