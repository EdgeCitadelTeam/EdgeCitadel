import os
import sqlite3

import pytest

from edgecitadel_agentd import storage_workspace as workspace
from edgecitadel_agentd.storage_sqlite import configure_scratch
from edgecitadel_agentd.trace_journal import TRACE_SCHEMA_SQL
from edgecitadel_agentd.trace_reservations import (
    SCHEMA_SQL,
    Obligation,
    fill,
    read,
    reserve,
)


@pytest.fixture
def db(tmp_path, monkeypatch):
    # Small local file verifies ownership/control flow only. Real allocation,
    # quota pressure and process death are qualified on the owned jim-eq volume.
    monkeypatch.setattr(workspace, "WORKSPACE_BYTES", 256 * 1024)
    if not hasattr(os, "posix_fallocate"):

        def allocate(fd, offset, size):
            assert os.pwrite(fd, b"\0" * size, offset) == size

        monkeypatch.setattr(os, "posix_fallocate", allocate, raising=False)
    connection = sqlite3.connect(
        tmp_path / "trace.sqlite3", factory=workspace.ReservedConnection
    )
    configure_scratch(connection)
    connection.executescript(TRACE_SCHEMA_SQL + SCHEMA_SQL)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        reserve(connection, Obligation("run", "owned", "terminal"))
    physical = workspace.CompletionWorkspace(tmp_path / "completion.reserve")
    try:
        connection.install_workspace(physical)
        yield connection
    finally:
        connection.close()


def assert_restored(db):
    info = os.fstat(db.workspace.descriptor)
    assert info.st_size == workspace.WORKSPACE_BYTES + 1
    assert info.st_blocks * 512 >= workspace.WORKSPACE_BYTES
    assert not db.workspace.owned and not db.workspace.borrowed


def test_fixed_completion_borrows_then_restores_under_one_owner(db):
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        assert db.workspace.owned
        assert os.fstat(db.workspace.descriptor).st_size == 0
        fill(db, Obligation("run", "owned", "terminal"), {"done": True})
        assert db.workspace.owned
    assert read(db, 1) == {"done": True}
    assert_restored(db)


def test_failed_completion_restores_both_record_and_allocation(db):
    with pytest.raises(ValueError), db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        fill(db, Obligation("run", "owned", "terminal"), {"done": True})
        raise ValueError("before commit")
    assert read(db, 1) is None
    assert_restored(db)


def test_completion_cannot_commit_database_growth(db):
    with pytest.raises(sqlite3.IntegrityError, match="changed database allocation"), db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        reserve(db, Obligation("run", "must-not-be-admitted", "terminal"))
    assert db.execute("SELECT count(*) FROM trace_completion_slots").fetchone()[0] == 1
    assert_restored(db)


def test_implicit_writes_and_explicit_commit_retain_owner(db):
    db.execute("UPDATE trace_completion_slots SET purpose='changed' WHERE slot_id=1")
    assert db.workspace.owned and db.in_transaction
    db.commit()
    assert_restored(db)
    db.execute("BEGIN IMMEDIATE")
    with pytest.raises(sqlite3.ProgrammingError, match="commit"):
        db.execute("COMMIT")
    assert db.workspace.owned and db.in_transaction
    db.rollback()
    assert_restored(db)


def test_context_without_transaction_and_repeated_close_release_owner(db):
    with pytest.raises(ValueError), db:
        raise ValueError("before SQLite transaction")
    assert_restored(db)
    db.close()
    db.close()


def test_failed_reserve_refill_marks_committed_state_and_next_owner_recovers(
    db, monkeypatch
):
    allocate = os.posix_fallocate

    def refuse(fd, offset, size):
        os.ftruncate(fd, size)
        raise OSError("owned refill failure")

    with monkeypatch.context() as patch:
        patch.setattr(os, "posix_fallocate", refuse)
        with pytest.raises(OSError, match="owned refill failure"), db:
            db.execute("BEGIN IMMEDIATE")
            db.use_completion_workspace()
            fill(db, Obligation("run", "owned", "terminal"), {"committed": True})
    assert not db.workspace.owned
    assert os.fstat(db.workspace.descriptor).st_size == workspace.WORKSPACE_BYTES
    assert not db.workspace.reserved
    assert read(db, 1) == {"committed": True}
    assert os.posix_fallocate is allocate
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert db.workspace.reserved
        assert read(db, 1) == {"committed": True}
    assert_restored(db)


def test_outer_savepoint_cannot_bypass_completion_commit_guard(db):
    with pytest.raises(sqlite3.ProgrammingError, match="outer transaction"):
        db.execute("SAVEPOINT bypass")
    assert not db.in_transaction
    assert_restored(db)
