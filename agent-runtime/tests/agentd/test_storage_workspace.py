import os
import sqlite3

import pytest

from edgecitadel_agentd import storage_workspace as workspace
from edgecitadel_agentd.storage_sqlite import configure_scratch
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_reservations import (
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
    AgentdStore(tmp_path / "trace.sqlite3").close()
    connection = sqlite3.connect(
        tmp_path / "trace.sqlite3", factory=workspace.ReservedConnection
    )
    configure_scratch(connection)
    connection.execute("PRAGMA synchronous=EXTRA")
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
    assert db.workspace.reserved
    assert all(
        path.is_file() and path.stat().st_size == 0 for path in db.workspace.inode_paths
    )
    assert not db.workspace.owned and not db.workspace.borrowed


def test_fixed_completion_borrows_then_restores_under_one_owner(db):
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        assert db.workspace.owned
        assert os.fstat(db.workspace.descriptor).st_size == 0
        assert not any(path.exists() for path in db.workspace.inode_paths)
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
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"), db:
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


@pytest.mark.parametrize(
    "command",
    [
        "/* boundary */ COMMIT",
        "-- boundary\nEND",
        "; ; /* boundary */ COMMIT",
        "\ufeffCOMMIT",
    ],
)
def test_sql_spelling_cannot_bypass_completion_commit_guard(db, command):
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        with pytest.raises(sqlite3.ProgrammingError, match="commit"):
            db.execute(command)
        assert db.in_transaction and db.workspace.owned
        fill(db, Obligation("run", "owned", "terminal"), {"guarded": True})
    assert_restored(db)


@pytest.mark.parametrize(
    "method,arguments",
    [
        ("execute", ("COMMIT",)),
        ("executemany", ("UPDATE trace_completion_slots SET purpose=?", [("bypass",)])),
        ("executescript", ("COMMIT;",)),
    ],
)
def test_returned_cursor_cannot_run_unowned_sql(db, method, arguments):
    cursor = db.execute("SELECT 1")
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        with pytest.raises(sqlite3.ProgrammingError, match="connection execution"):
            getattr(cursor, method)(*arguments)
        assert db.in_transaction and db.workspace.owned
    assert cursor.fetchone()[0] == 1
    assert_restored(db)


@pytest.mark.parametrize(
    "command",
    [
        "PRAGMA cache_spill=ON",
        "PRAGMA main.journal_mode=WAL",
        "PRAGMA synchronous=OFF",
        "PRAGMA temp_store=FILE",
        "PRAGMA optimize",
        "PRAGMA writable_schema=ON",
        "PRAGMA main.cache_spill(ON)",
        "EXPLAIN PRAGMA cache_spill=ON",
        "EXPLAIN /* bypass */ PRAGMA temp_store=FILE",
    ],
)
def test_installed_workspace_rejects_changes_to_qualified_sqlite_policy(db, command):
    with pytest.raises(sqlite3.ProgrammingError, match="qualified SQLite policy"):
        db.execute(command)
    assert db.execute("PRAGMA cache_spill").fetchone()[0] == 0
    assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    assert_restored(db)


@pytest.mark.parametrize(
    "command",
    [
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=OFF",
        "PRAGMA temp_store=FILE",
        "PRAGMA cache_spill=ON",
    ],
)
def test_workspace_installation_refuses_unqualified_policy_before_borrow(
    tmp_path, command
):
    connection = sqlite3.connect(
        tmp_path / "refused.sqlite3", factory=workspace.ReservedConnection
    )
    configure_scratch(connection)
    connection.execute("PRAGMA synchronous=EXTRA")
    connection.execute(command)
    physical = workspace.CompletionWorkspace(tmp_path / "refused.reserve")
    try:
        with pytest.raises(
            sqlite3.NotSupportedError, match="completion workspace requires"
        ):
            connection.install_workspace(physical)
        assert connection.workspace is None
        assert not physical.borrowed and not physical.owned
        assert os.fstat(physical.descriptor).st_size == 0
    finally:
        connection.close()
        physical.close()


@pytest.mark.parametrize("cached", [False, True])
def test_ordinary_write_before_borrow_refuses_without_releasing_reserve(db, cached):
    sql = "UPDATE trace_completion_slots SET purpose=? WHERE slot_id=1"
    if cached:
        with db:
            db.execute(sql, ("terminal",))
    with (
        pytest.raises(sqlite3.ProgrammingError, match="after ordinary trace writes"),
        db,
    ):
        db.execute("BEGIN IMMEDIATE")
        db.execute(sql, ("changed",))
        before = os.fstat(db.workspace.descriptor).st_blocks
        try:
            db.use_completion_workspace()
        finally:
            assert not db.workspace.borrowed
            assert os.fstat(db.workspace.descriptor).st_blocks == before
    assert (
        db.execute("SELECT purpose FROM trace_completion_slots").fetchone()[0]
        == "terminal"
    )
    assert_restored(db)


@pytest.mark.parametrize("cached", [False, True])
def test_cached_ordinary_write_after_borrow_cannot_commit_partial_completion(
    db, cached
):
    sql = "UPDATE trace_completion_slots SET purpose=? WHERE slot_id=1"
    if cached:
        with db:
            db.execute(sql, ("terminal",))
    with (
        pytest.raises(
            sqlite3.IntegrityError, match="attempted an ordinary trace write"
        ),
        db,
    ):
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        fill(db, Obligation("run", "owned", "terminal"), {"must_rollback": True})
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            db.execute(sql, ("forbidden",))
        # Even swallowing the statement error cannot commit a partial result.
    assert read(db, 1) is None
    assert_restored(db)
    with db:
        db.execute(sql, ("terminal",))
    assert_restored(db)


def test_trigger_cannot_hide_an_ordinary_trace_write_from_completion_guard(db):
    db.execute(
        "CREATE TRIGGER owned_hidden_write AFTER UPDATE OF filled ON trace_completion_slots BEGIN UPDATE trace_sources SET active=0; END"
    )
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"), db:
        db.execute("BEGIN IMMEDIATE")
        db.use_completion_workspace()
        fill(db, Obligation("run", "owned", "terminal"), {"must_rollback": True})
    assert read(db, 1) is None
    assert_restored(db)


def test_installed_workspace_keeps_its_authorizer(db):
    with pytest.raises(sqlite3.ProgrammingError, match="owns SQL authorization"):
        db.set_authorizer(None)


def test_fixed_column_write_before_borrow_is_still_ordinary(db):
    with (
        pytest.raises(sqlite3.ProgrammingError, match="after ordinary trace writes"),
        db,
    ):
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE trace_completion_slots SET record=record")
        db.use_completion_workspace()
    assert not db.workspace.borrowed
    assert_restored(db)


def test_missing_inode_reservation_recovers_before_admission(db):
    db.workspace.inode_paths[0].unlink()
    assert not db.workspace.reserved
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert db.workspace.reserved
    assert_restored(db)


@pytest.mark.parametrize("invalid", ["content", "symlink", "hardlink", "mode"])
def test_invalid_inode_reservation_refuses_without_deleting_files(
    db, tmp_path, invalid
):
    first, second = db.workspace.inode_paths
    if invalid == "content":
        second.write_bytes(b"foreign")
    elif invalid == "symlink":
        second.unlink()
        second.symlink_to(first)
    elif invalid == "hardlink":
        os.link(second, tmp_path / "other-link")
    else:
        second.chmod(0o644)
    with pytest.raises(sqlite3.OperationalError, match="inode reservation"):
        db.execute("BEGIN IMMEDIATE")
    assert first.exists() and second.lstat()
    assert not db.workspace.owned and not db.in_transaction


def test_failed_borrow_invalidates_cached_ordinary_statements(db, monkeypatch):
    sql = "UPDATE trace_completion_slots SET purpose=purpose WHERE slot_id=-1"
    with db:
        db.execute(sql)

    def fail_sync(self):
        raise OSError("release failed")

    with db:
        db.execute("BEGIN IMMEDIATE")
        with monkeypatch.context() as patch:
            patch.setattr(workspace.CompletionWorkspace, "_sync_directory", fail_sync)
            with pytest.raises(OSError, match="release failed"):
                db.use_completion_workspace()
        assert db.workspace.borrowed
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            db.execute(sql)
        db.rollback()
    assert_restored(db)


def test_failed_inode_refill_cannot_publish_ready_marker(db, monkeypatch):
    original = os.open

    def refuse(path, flags, *args, **kwargs):
        if path == db.workspace.inode_paths[1] and flags & os.O_CREAT:
            raise OSError("inode refill failed")
        return original(path, flags, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "open", refuse)
        with pytest.raises(OSError, match="inode refill failed"), db:
            db.execute("BEGIN IMMEDIATE")
            db.use_completion_workspace()
            fill(db, Obligation("run", "owned", "terminal"), {"committed": True})
    assert not db.workspace.reserved
    assert os.fstat(db.workspace.descriptor).st_size == workspace.WORKSPACE_BYTES
    assert read(db, 1) == {"committed": True}
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert db.workspace.reserved
    assert_restored(db)
