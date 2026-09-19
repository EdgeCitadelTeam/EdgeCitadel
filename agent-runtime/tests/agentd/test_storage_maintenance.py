"""Offline reclamation preserves records and respects live ownership."""

import sqlite3

import pytest
from test_trace_crash import prepare, snapshot

from edgecitadel_agentd.restore import RESTORE_BARRIER, RestorePendingError
from edgecitadel_agentd.storage_maintenance import compact_database
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer


def prepared(directory):
    store, *_ = prepare(directory / "agentd.sqlite3")
    db = store._connection
    with db:
        db.execute("CREATE TABLE owned_bulk(value BLOB)")
        db.execute("INSERT INTO owned_bulk VALUES (zeroblob(1048576))")
    with db:
        db.execute("DELETE FROM owned_bulk")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert db.execute("PRAGMA freelist_count").fetchone()[0] > 0
    before = snapshot(store)
    store.close()
    return before


def test_offline_vacuum_shrinks_file_preserving_state_and_key(tmp_path):
    directory = tmp_path / "state/agentd"
    before = prepared(directory)
    with sqlite3.connect(directory / "agentd.sqlite3") as db:
        # Compare every persisted application column, including ciphertext and
        # source/export counters, independently of the trace snapshot helper.
        tables = [
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        ]
        all_rows = {
            name: list(db.execute('SELECT * FROM "' + name.replace('"', '""') + '"'))
            for name in tables
        }
    db.close()
    key = (directory / "payload.key").read_bytes()
    result = compact_database(directory)
    assert (
        result["after"]["database_file_bytes"] < result["before"]["database_file_bytes"]
    )
    assert result["after"]["reusable_page_bytes"] == 0
    assert result["after"]["wal_file_bytes"] == 0
    assert (directory / "payload.key").read_bytes() == key
    with sqlite3.connect(directory / "agentd.sqlite3") as db:
        for name, rows in all_rows.items():
            actual = list(db.execute('SELECT * FROM "' + name.replace('"', '""') + '"'))
            assert sorted(actual, key=repr) == sorted(rows, key=repr)
    db.close()
    store = AgentdStore(directory / "agentd.sqlite3")
    try:
        assert snapshot(store) == before
    finally:
        store.close()


def test_active_writer_and_restore_barrier_exclude_compaction(tmp_path):
    tmp_path = tmp_path / "state/agentd"
    prepared(tmp_path)
    with exclusive_writer(tmp_path), pytest.raises(WriterActiveError):
        compact_database(tmp_path)
    (tmp_path / RESTORE_BARRIER).write_text("{}")
    with pytest.raises(RestorePendingError):
        compact_database(tmp_path)


def test_active_reader_fails_without_modifying_records_then_retry_succeeds(tmp_path):
    tmp_path = tmp_path / "state/agentd"
    before = prepared(tmp_path)
    reader = sqlite3.connect(tmp_path / "agentd.sqlite3")
    try:
        reader.execute("BEGIN")
        count = reader.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0]
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            compact_database(tmp_path)
        assert (
            reader.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0] == count
        )
    finally:
        reader.close()
    compact_database(tmp_path)
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        assert snapshot(store) == before
    finally:
        store.close()


def test_missing_database_is_not_created(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        compact_database(tmp_path)
    assert not (tmp_path / "agentd.sqlite3").exists()


def test_sqlite_interrupt_during_vacuum_preserves_reopen_and_retry(
    tmp_path, monkeypatch
):
    tmp_path = tmp_path / "state/agentd"
    before = prepared(tmp_path)
    connect = sqlite3.connect

    class InterruptVacuum(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if not sql.startswith("VACUUM"):
                return super().execute(sql, *args, **kwargs)
            self.set_progress_handler(lambda: 1, 1)
            try:
                return super().execute(sql, *args, **kwargs)
            finally:
                self.set_progress_handler(None, 0)

    with monkeypatch.context() as patch:
        patch.setattr(
            sqlite3,
            "connect",
            lambda *args, **kwargs: connect(*args, **kwargs, factory=InterruptVacuum),
        )
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            compact_database(tmp_path)
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        assert snapshot(store) == before
        assert store.health()["database"] == "ok"
    finally:
        store.close()
    assert compact_database(tmp_path)["after"]["reusable_page_bytes"] == 0
