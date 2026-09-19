import sqlite3

import pytest

from edgecitadel_agentd import storage_geometry, storage_workspace
from edgecitadel_agentd.trace_journal import TraceJournal
import test_trace_seed as seed_tests
import test_trace_task_completion as task_tests

existing = seed_tests.existing
installed = task_tests.installed


def test_new_source_and_export_identity_limits_roll_back_admission(
    existing, monkeypatch
):
    store, *_ = existing
    monkeypatch.setattr(storage_geometry, "MAX_IDENTITIES", 2)
    seed_tests.install(store)
    db = store._connection
    with db:
        db.execute("BEGIN IMMEDIATE")
        TraceJournal(db).initialize("second-source")
    before = task_tests.snapshot(store)
    with pytest.raises(sqlite3.IntegrityError, match="geometry admission limit"), db:
        db.execute("BEGIN IMMEDIATE")
        TraceJournal(db).initialize("third-source")
    assert task_tests.snapshot(store) == before
    epoch = db.execute(
        "SELECT source_epoch FROM trace_sources WHERE node_id='owned-edge'"
    ).fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="geometry admission limit"), db:
        db.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,active) VALUES (?,?,?,0)",
            ("owned-edge", epoch, "owned-generation"),
        )
    assert task_tests.snapshot(store) == before


def test_utf8_byte_width_refuses_ordinary_update_without_changing_counter(installed):
    store, *_ = installed
    task_tests.running(installed)
    db = store._connection
    before = task_tests.snapshot(store)
    # Existing schema counts characters; geometry must bound actual UTF-8 bytes.
    with pytest.raises(sqlite3.IntegrityError, match="geometry admission limit"), db:
        db.execute("UPDATE trace_export_generations SET sync_fault=?", ("é" * 33,))
    assert task_tests.snapshot(store) == before


@pytest.mark.parametrize("change", ["index", "width", "count", "without_rowid"])
def test_installation_refuses_unqualified_restored_geometry(
    existing, monkeypatch, change
):
    store, *_ = existing
    db = store._connection
    with db:
        db.execute("BEGIN IMMEDIATE")
        if change == "index":
            db.execute(
                "CREATE INDEX owned_counter_index ON trace_sources(next_source_seq)"
            )
        elif change == "width":
            db.execute("UPDATE trace_export_generations SET sync_fault=?", ("é" * 33,))
        elif change == "without_rowid":
            counter = db.execute(
                "SELECT next_id FROM trace_presence_counter"
            ).fetchone()[0]
            db.execute("DROP TABLE trace_presence_counter")
            db.execute(
                "CREATE TABLE trace_presence_counter(singleton INTEGER PRIMARY KEY,next_id BLOB) WITHOUT /* syntax */ ROWID"
            )
            db.execute("INSERT INTO trace_presence_counter VALUES (1,?)", (counter,))
        else:
            monkeypatch.setattr(storage_geometry, "MAX_IDENTITIES", 1)
            TraceJournal(db).initialize("second-source")
    before = task_tests.snapshot(store)
    physical = storage_workspace.CompletionWorkspace(
        store.path.parent / "completion.reserve"
    )
    try:
        error = (
            sqlite3.NotSupportedError
            if change in {"index", "without_rowid"}
            else sqlite3.IntegrityError
        )
        with pytest.raises(error, match="completion geometry"):
            db.install_workspace(physical)
        assert db.workspace is None
        assert not physical.owned and not physical.borrowed
        assert task_tests.snapshot(store) == before
    finally:
        physical.close()


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE INDEX owned_counter_index ON trace_sources(next_source_seq)",
        "ALTER TABLE trace_completion_slots ADD COLUMN hidden BLOB",
        "DROP INDEX trace_completion_owner",
        "DROP TABLE trace_presence_counter",
        "DROP TRIGGER temp.completion_geometry_trace_sources_INSERT",
    ],
)
def test_installed_handle_preserves_schema_and_geometry_guards(installed, sql):
    store, *_ = installed
    db = store._connection
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        db.execute(sql)
    task = task_tests.running(installed)
    assert task_tests.complete(installed, task)["state"] == "completed"


def test_unqualified_vfs_sector_refuses_installation_without_editing_journal(
    existing, monkeypatch
):
    from io import BytesIO

    store, *_ = existing
    db = store._connection
    before = task_tests.snapshot(store)
    header = bytearray(28)
    header[20:24] = (8192).to_bytes(4, "big")
    header[24:28] = (4096).to_bytes(4, "big")
    # Replace only the observer's read; the real SQLite journal is untouched.
    monkeypatch.setattr(
        storage_geometry, "open", lambda *args: BytesIO(header), raising=False
    )
    physical = storage_workspace.CompletionWorkspace(
        store.path.parent / "completion.reserve"
    )
    try:
        with pytest.raises(sqlite3.NotSupportedError, match="journal sector"):
            db.install_workspace(physical)
        assert db.workspace is None and not physical.owned
        assert task_tests.snapshot(store) == before
    finally:
        physical.close()
