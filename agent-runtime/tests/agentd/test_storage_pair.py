import os
import select
import shutil
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from storage_test_support import flatten_connection
from test_trace_crash import prepare

from edgecitadel_agentd import storage_pair
from edgecitadel_agentd.store import AgentdStore


def inventory(db):
    result = {}
    for schema in ("main", "task_state"):
        for (name,) in db.execute(
            f"SELECT name FROM {schema}.sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'storage_pair'"
        ):
            result[name] = [
                tuple(row)
                for row in db.execute(
                    f'SELECT rowid,* FROM {schema}."{name}" ORDER BY rowid'
                )
            ]
    return result


def test_task_content_is_separate_and_reopen_preserves_both_files(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    task_path = tmp_path / "outside-trace.sqlite3"
    store = AgentdStore(path, task_path=task_path)
    task = store.create_task(
        sender_id="sender", recipient_id="worker", payload={"body": "private"}
    )
    expected = inventory(store._connection)
    assert (
        store._connection.execute(
            "SELECT 1 FROM main.sqlite_schema WHERE name='tasks'"
        ).fetchone()
        is None
    )
    assert (
        store._connection.execute("SELECT count(*) FROM task_state.tasks").fetchone()[0]
        == 1
    )
    assert (
        store._connection.execute("SELECT count(*) FROM main.events").fetchone()[0] > 0
    )
    store.close()
    reopened = AgentdStore(path, task_path=task_path)
    try:
        assert inventory(reopened._connection) == expected
        assert reopened.get_task(task["task_id"])["payload"] == {"body": "private"}
        for schema in ("main", "task_state"):
            assert (
                reopened._connection.execute(
                    f"PRAGMA {schema}.journal_mode"
                ).fetchone()[0]
                == "delete"
            )
            assert (
                reopened._connection.execute(f"PRAGMA {schema}.synchronous").fetchone()[
                    0
                ]
                == 3
            )
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "damage", ["missing", "other_pair", "other_pair_wal", "version"]
)
def test_missing_or_mismatched_pair_refuses_without_recreating(tmp_path, damage):
    path = tmp_path / "one.sqlite3"
    store = AgentdStore(path)
    task_path = store.task_path
    store.close()
    if damage == "missing":
        task_path.rename(tmp_path / "saved-task.sqlite3")
    elif damage in {"other_pair", "other_pair_wal"}:
        other = AgentdStore(tmp_path / "two.sqlite3")
        other_path = other.task_path
        other.close()
        shutil.copyfile(other_path, task_path)
        if damage == "other_pair_wal":
            with sqlite3.connect(task_path) as db:
                db.execute("PRAGMA journal_mode=WAL").fetchone()
    else:
        with sqlite3.connect(task_path) as db:
            db.execute("PRAGMA user_version=23")
    before = path.read_bytes()
    task_before = task_path.read_bytes() if task_path.exists() else None
    with pytest.raises(sqlite3.DatabaseError):
        AgentdStore(path)
    assert path.read_bytes() == before
    if damage == "missing":
        assert not task_path.exists()
    else:
        assert task_path.read_bytes() == task_before


def test_cross_store_references_guard_child_and_parent_mutations(tmp_path):
    store, _, _, binding = prepare(tmp_path / "state/agentd.sqlite3", managed=True)
    db = store._connection
    try:
        before = inventory(db)
        for statement, params in [
            (
                "UPDATE trace_bindings SET task_id='missing' WHERE binding_id=?",
                (binding["binding_id"],),
            ),
            ("DELETE FROM tasks WHERE task_id=?", (binding["task_id"],)),
            (
                "UPDATE tasks SET task_id='changed' WHERE task_id=?",
                (binding["task_id"],),
            ),
            ("INSERT INTO trace_task_contexts VALUES ('missing','{}')", ()),
        ]:
            with pytest.raises(sqlite3.IntegrityError), db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(statement, params)
            assert inventory(db) == before
        assert db.execute("PRAGMA main.foreign_key_list(trace_operations)").fetchall()
    finally:
        store.close()


def test_shared_migration_preserves_exact_rows_and_rowids(tmp_path):
    path = tmp_path / "state/agentd.sqlite3"
    store, _, _, _ = prepare(path, managed=True)
    expected = inventory(store._connection)
    with store._connection:
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=23")
    store.close()
    migrated = AgentdStore(path)
    try:
        assert inventory(migrated._connection) == expected
        for schema in ("main", "task_state"):
            assert (
                migrated._connection.execute(
                    f"PRAGMA {schema}.integrity_check"
                ).fetchone()[0]
                == "ok"
            )
            assert not migrated._connection.execute(
                f"PRAGMA {schema}.foreign_key_check"
            ).fetchall()
    finally:
        migrated.close()


def test_migration_refusal_keeps_shared_authority_and_retries(tmp_path, monkeypatch):
    path = tmp_path / "state/agentd.sqlite3"
    store, _, _, _ = prepare(path, managed=True)
    expected = inventory(store._connection)
    with store._connection:
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=23")
    store.close()
    copy = storage_pair._copy_rows

    def fail_after_copy(db, table, target):
        copy(db, table, target)
        if table == "trace_bindings":
            raise sqlite3.OperationalError("owned migration refusal")

    with monkeypatch.context() as patch:
        patch.setattr(storage_pair, "_copy_rows", fail_after_copy)
        with pytest.raises(sqlite3.OperationalError, match="owned migration refusal"):
            AgentdStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 23
        assert db.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
    reopened = AgentdStore(path)
    try:
        assert inventory(reopened._connection) == expected
    finally:
        reopened.close()


@pytest.mark.parametrize("point", ["during_copy", "before_commit", "after_commit"])
def test_migration_sigkill_recovers_one_complete_authority(tmp_path, point):
    path = tmp_path / "state/agentd.sqlite3"
    store, _, _, _ = prepare(path, managed=True)
    expected = inventory(store._connection)
    with store._connection:
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=23")
    store.close()
    script = """
import signal, sys
from pathlib import Path
from edgecitadel_agentd import storage_pair
from edgecitadel_agentd.store import AgentdStore
def pause():
    print('ready', flush=True)
    signal.pause()
point = sys.argv[2]
if point == 'during_copy':
    original = storage_pair._copy_rows
    def copy(db, table, target):
        original(db, table, target)
        if table == 'trace_bindings':
            pause()
    storage_pair._copy_rows = copy
else:
    method = '_migrate_locked' if point == 'before_commit' else '_migrate'
    original = getattr(AgentdStore, method)
    def migrate(self):
        original(self)
        pause()
    setattr(AgentdStore, method, migrate)
AgentdStore(Path(sys.argv[1]))
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(path), point],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    try:
        assert select.select([child.stdout], [], [], 15)[0]
        assert child.stdout.readline() == "ready\n"
        child.kill()
        assert child.wait(timeout=10) == -signal.SIGKILL
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)
    # A read-write connection performs any hot-journal recovery before inspecting
    # authority; reopening Agentd then retries an interrupted migration normally.
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == (
            26 if point == "after_commit" else 23
        )
    reopened = AgentdStore(path)
    try:
        assert inventory(reopened._connection) == expected
    finally:
        reopened.close()


def test_task_payload_allocation_is_not_trace_pressure(tmp_path):
    from edgecitadel_agentd.trace_capacity import physical_storage

    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        before = physical_storage(store._connection)["pressure_bytes"]
        for _ in range(8):
            store.create_task(
                sender_id="sender",
                recipient_id="worker",
                payload={"body": "x" * 40_000},
            )
        measured = physical_storage(store._connection)
        assert store.task_path.stat().st_size > 400_000
        assert measured["pressure_bytes"] - before < 100_000
        assert measured["database_file_bytes"] == store.path.stat().st_size
    finally:
        store.close()
