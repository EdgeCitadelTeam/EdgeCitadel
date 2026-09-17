"""Populated pre-tracing migration/rollback fixture. No future migration claimed."""

import shutil
import sqlite3

import pytest

from edgecitadel_agentd.store import SCHEMA_VERSION, AgentdStore, StoreError


def snapshot(path):
    with sqlite3.connect(path) as db:
        tables = [
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'trace_%' ORDER BY name"
            )
        ]
        result = {}
        for table in tables:
            # Names come exclusively from this owned database's sqlite_master.
            columns = [
                r[1]
                for r in db.execute(f'PRAGMA table_info("{table}")')
                if r[1] != "context_id"
            ]
            selected = ",".join(f'"{c}"' for c in columns)
            result[table] = db.execute(
                f'SELECT {selected} FROM "{table}" ORDER BY rowid'
            ).fetchall()
        return result


@pytest.fixture
def populated_v5(tmp_path):
    path = tmp_path / "original" / "agentd.sqlite3"
    store = AgentdStore(path)
    try:
        completed = store.create_task(
            sender_id="fixture-a",
            recipient_id="fixture-b",
            payload={"body": "owned encrypted input"},
            queue_transport=False,
        )
        for state, actor in [
            ("offered", "edgecitadel-system"),
            ("accepted", "fixture-b"),
            ("running", "fixture-b"),
            ("completed", "fixture-b"),
        ]:
            store.transition_task(
                task_id=completed["task_id"],
                state=state,
                actor_id=actor,
                result={"body": "owned exact output"} if state == "completed" else None,
                queue_transport=False,
            )
        pending = store.create_task(
            sender_id="fixture-a",
            recipient_id="remote-b",
            payload={"body": "pending owned command"},
            queue_transport=True,
        )
        expected = [store.get_task(t["task_id"]) for t in (completed, pending)]
    finally:
        store.close()
    with sqlite3.connect(path) as db:
        for table in (
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
        db.execute("ALTER TABLE tasks DROP COLUMN context_id")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        db.execute("PRAGMA user_version=5")
        assert (
            db.execute(
                "SELECT COUNT(*) FROM transport_outbox WHERE published_at_ms IS NULL"
            ).fetchone()[0]
            == 1
        )
    return path, expected, snapshot(path)


def test_populated_upgrade_preserves_journal_outbox_and_encrypted_content(populated_v5):
    path, expected, before = populated_v5
    store = AgentdStore(path)
    try:
        assert (
            snapshot(path) == before
        )  # Includes ciphertext, attempts, events and outbox.
        for original in expected:
            actual = store.get_task(original["task_id"])
            assert actual == {**original, "context_id": None}
        assert (
            store._connection.execute("PRAGMA user_version").fetchone()[0]
            == SCHEMA_VERSION
        )
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        store.close()


def test_failure_after_migration_statements_rolls_back_every_change(populated_v5):
    path, _, before = populated_v5
    connections = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source):
            connections.append(self._connection)
            super()._execute_migration_sql(source)
            raise RuntimeError("owned post-statement failure")

    try:
        with pytest.raises(RuntimeError, match="owned post-statement failure"):
            FailingStore(path)
    finally:
        for db in connections:
            db.close()
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 5
        assert "context_id" not in {
            r[1] for r in db.execute("PRAGMA table_info(tasks)")
        }
    assert snapshot(path) == before
    recovered = AgentdStore(path)
    recovered.close()
    assert snapshot(path) == before


def test_fenced_matched_backup_restores_pending_work_and_exact_results(
    populated_v5, tmp_path
):
    path, expected, _ = populated_v5
    store = AgentdStore(path)
    backup = tmp_path / "backup"
    backup.mkdir()
    try:
        with sqlite3.connect(backup / "agentd.sqlite3") as destination:
            store._connection.backup(destination)
        shutil.copy2(path.parent / "payload.key", backup / "payload.key")
        before = snapshot(path)
    finally:
        store.close()
    # A version fence exercises old-binary behavior, not a real future migration.
    with sqlite3.connect(path) as db:
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION + 1}")
    refused = AgentdStore.__new__(AgentdStore)
    try:
        with pytest.raises(StoreError, match="newer than supported"):
            refused.__init__(path)
    finally:
        refused.close()
    assert snapshot(path) == before
    restored_path = tmp_path / "restored"
    shutil.copytree(backup, restored_path)
    restored = AgentdStore(restored_path / "agentd.sqlite3")
    try:
        assert snapshot(restored.path) == before
        for original in expected:
            assert restored.get_task(original["task_id"]) == {
                **original,
                "context_id": None,
            }
    finally:
        restored.close()


def test_mismatched_valid_key_cannot_silently_decode_backup(populated_v5, tmp_path):
    from cryptography.fernet import Fernet

    path, expected, _ = populated_v5
    destination = tmp_path / "wrong-key"
    shutil.copytree(path.parent, destination)
    (destination / "payload.key").write_bytes(Fernet.generate_key() + b"\n")
    store = AgentdStore(destination / "agentd.sqlite3")
    try:
        with pytest.raises(StoreError, match="could not be decrypted"):
            store.get_task(expected[0]["task_id"])
    finally:
        store.close()
