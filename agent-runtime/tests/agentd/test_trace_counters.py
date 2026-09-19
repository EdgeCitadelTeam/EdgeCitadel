import sqlite3

import pytest

from edgecitadel_agentd import trace_counters
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_journal import TRACE_SCHEMA_SQL
from test_trace_journal import event, write
from storage_test_support import stage_restore
from edgecitadel_agentd.storage_pair import task_database_path


def legacy_counters(path, task_path):
    """Build genuine v24 counter tables in an owned paired fixture."""
    with sqlite3.connect(path) as db:
        db.execute("ATTACH DATABASE ? AS task_state", (str(task_path),))
        db.execute("BEGIN IMMEDIATE")
        for table in ("trace_sources", "trace_export_generations"):
            sql = next(
                part.strip()
                for part in TRACE_SCHEMA_SQL.split(";")
                if part.strip().startswith(f"CREATE TABLE {table} (")
            )
            scratch = table + "_legacy"
            db.execute(
                sql.replace(f"CREATE TABLE {table}", f"CREATE TABLE {scratch}", 1)
            )
            if table == "trace_export_generations":
                db.execute(f"ALTER TABLE {scratch} ADD COLUMN sync_fault TEXT")
            columns = ",".join(
                row[1] for row in db.execute(f"PRAGMA table_info({scratch})")
            )
            indexes = db.execute(
                "SELECT sql FROM sqlite_schema WHERE tbl_name=? AND type='index' AND sql IS NOT NULL",
                (table,),
            ).fetchall()
            db.execute(
                f"INSERT INTO {scratch}(rowid,{columns}) SELECT rowid,{columns} FROM {table}"
            )
            db.execute(f"DROP TABLE {table}")
            db.execute(f"ALTER TABLE {scratch} RENAME TO {table}")
            for (sql,) in indexes:
                db.execute(sql)
        db.execute("PRAGMA main.user_version=24")
        db.execute("PRAGMA task_state.user_version=24")


def test_real_v24_migration_preserves_identity_positions_and_retry(
    tmp_path, monkeypatch
):
    path = tmp_path / "agentd.sqlite3"
    store = AgentdStore(path)
    value = event()
    original = write(store, value)
    pair = store._connection.execute("SELECT pair_id FROM storage_pair").fetchone()[0]
    task_path = store.task_path
    store.close()
    legacy_counters(path, task_path)
    migrate = trace_counters.migrate_counters

    def fail_after_rebuild(db):
        migrate(db)
        raise RuntimeError("owned migration refusal")

    with monkeypatch.context() as patch:
        patch.setattr(trace_counters, "migrate_counters", fail_after_rebuild)
        with pytest.raises(RuntimeError, match="owned migration refusal"):
            AgentdStore(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 24
        assert db.execute(
            "SELECT typeof(next_source_seq),next_source_seq FROM trace_sources"
        ).fetchone() == ("integer", 2)
        assert not any(
            row[1] == "next_source_seq_bytes"
            for row in db.execute("PRAGMA table_info(trace_sources)")
        )
    with sqlite3.connect(task_path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 24
    migrated = AgentdStore(path)
    try:
        assert (
            migrated._connection.execute("SELECT pair_id FROM storage_pair").fetchone()[
                0
            ]
            == pair
        )
        assert write(migrated, value) == original
        assert write(migrated, event())["source_seq"] == 2
        assert (
            migrated._connection.execute(
                "SELECT next_export_seq FROM trace_export_generations"
            ).fetchone()[0]
            == 3
        )
        assert not migrated._connection.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        migrated.close()


def test_counter_increment_boundaries_preserve_stored_width(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        write(store, event())
        db = store._connection
        for boundary in (127, 32767, 8388607, 2147483647, 140737488355327):
            with db:
                for table, name in (
                    ("trace_sources", "next_source_seq"),
                    ("trace_export_generations", "next_export_seq"),
                ):
                    db.execute(
                        f"UPDATE {table} SET {name}_bytes=?",
                        (trace_counters.encode_counter(boundary),),
                    )
            assert write(store, event())["source_seq"] == boundary
            for table, name in (
                ("trace_sources", "next_source_seq"),
                ("trace_export_generations", "next_export_seq"),
            ):
                assert tuple(
                    db.execute(
                        f"SELECT {name},typeof({name}_bytes),length({name}_bytes) FROM {table}"
                    ).fetchone()
                ) == (boundary + 1, "blob", 20)
    finally:
        store.close()


def test_v24_paired_snapshot_restores_then_migrates_before_rotation(tmp_path):
    previous = tmp_path / "previous"
    path = previous / "agentd.sqlite3"
    store = AgentdStore(path)
    original = write(store, event())
    store.close()
    legacy_counters(path, task_database_path(path))
    destination = tmp_path / "restored"
    marker = stage_restore(
        snapshot_dir=previous,
        previous_state_dir=previous,
        destination_dir=destination,
        node_id="edge-a",
        expected_source_epoch=original["source_epoch"],
    )
    assert marker["source_epoch"] != original["source_epoch"]
    with sqlite3.connect(destination / "agentd.sqlite3") as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 28
        assert (
            db.execute(
                "SELECT source_seq FROM trace_journal WHERE event_id=?",
                (original["event_id"],),
            ).fetchone()[0]
            == original["source_seq"]
        )
        assert (
            db.execute(
                "SELECT next_source_seq FROM trace_sources WHERE active=1"
            ).fetchone()[0]
            == 2
        )
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 24


def test_exhaustion_and_invalid_storage_cannot_consume_positions(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        write(store, event())
        db = store._connection
        for invalid in (
            b"1",
            b"x" * 20,
            b"0000000000000001\x00xxx",
            b"0" * 20,
            b"9" * 20,
            "00000000000000000001",
        ):
            with pytest.raises(sqlite3.IntegrityError), db:
                db.execute(
                    "UPDATE trace_sources SET next_source_seq_bytes=?", (invalid,)
                )
        with db:
            db.execute(
                "UPDATE trace_export_generations SET next_export_seq_bytes=?",
                (trace_counters.encode_counter(trace_counters.MAX_COUNTER),),
            )
        with pytest.raises(TraceContractError, match="trace_sequence_exhausted"):
            write(store, event())
        assert (
            db.execute("SELECT next_source_seq FROM trace_sources").fetchone()[0] == 2
        )
        assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM trace_spool").fetchone()[0] == 1
    finally:
        store.close()


def test_last_wire_position_commits_and_remains_retryable(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        write(store, event())
        db = store._connection
        last = trace_counters.MAX_COUNTER - 1
        with db:
            db.execute(
                "UPDATE trace_sources SET next_source_seq_bytes=?",
                (trace_counters.encode_counter(last),),
            )
            db.execute(
                "UPDATE trace_export_generations SET next_export_seq_bytes=?",
                (trace_counters.encode_counter(last),),
            )
        value = event()
        final = write(store, value)
        assert final["source_seq"] == last
        assert write(store, value) == final
        with pytest.raises(TraceContractError, match="trace_sequence_exhausted"):
            write(store, event())
        assert (
            db.execute("SELECT max(export_seq) FROM trace_spool").fetchone()[0] == last
        )
        assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 2
    finally:
        store.close()
