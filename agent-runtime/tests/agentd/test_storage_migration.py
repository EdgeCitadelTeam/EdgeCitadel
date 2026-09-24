import errno
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from test_restore import seed

from edgecitadel_agentd import storage_migration as migration
from edgecitadel_agentd.restore import (
    RESTORE_BARRIER,
    RestorePendingError,
    require_startable,
)
from edgecitadel_agentd.storage_layout import StorageLayout
from edgecitadel_agentd.store import StoreError
from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer


@pytest.fixture
def source(tmp_path, monkeypatch):
    state = tmp_path / "agentd"
    seed(state)
    layout = StorageLayout(state)
    layout.trace_directory.mkdir(mode=0o700)
    for path in (state / "agentd.sqlite3", layout.task_path, layout.key_path):
        path.chmod(0o600)
    monkeypatch.setattr(migration, "verify_storage", lambda *args: None)
    return layout


def inventory(layout):
    return migration._inventory(layout.state_directory / "agentd.sqlite3", layout)


def test_move_preserves_exact_pair_key_and_accepted_task(source):
    before = inventory(source)
    assert migration.migrate_storage(source.state_directory) == before
    assert migration._inventory(source.trace_path, source) == before
    assert not (source.state_directory / "agentd.sqlite3").exists()
    require_startable(source.state_directory)
    with closing(sqlite3.connect(source.task_path)) as db:
        assert db.execute("SELECT state FROM tasks").fetchone() == ("accepted",)


def test_quota_refusal_precedes_any_migration_write(source, monkeypatch):
    before = inventory(source)

    def refuse(*args):
        raise RuntimeError("quota absent")

    monkeypatch.setattr(migration, "verify_storage", refuse)
    with pytest.raises(RuntimeError, match="quota absent"):
        migration.migrate_storage(source.state_directory)
    assert inventory(source) == before
    assert list(source.trace_directory.iterdir()) == []
    require_startable(source.state_directory)


def test_partial_copy_is_fenced_and_retry_preserves_authority(source, monkeypatch):
    before = inventory(source)
    original = migration.shutil.copyfileobj

    def full(origin, target, **kwargs):
        target.write(origin.read(4096))
        raise OSError(errno.EDQUOT, "owned quota exhaustion")

    monkeypatch.setattr(migration.shutil, "copyfileobj", full)
    with pytest.raises(OSError) as error:
        migration.migrate_storage(source.state_directory)
    assert error.value.errno == errno.EDQUOT
    assert inventory(source) == before
    with pytest.raises(RestorePendingError):
        require_startable(source.state_directory)
    monkeypatch.setattr(migration.shutil, "copyfileobj", original)
    assert migration.migrate_storage(source.state_directory) == before
    assert migration._inventory(source.trace_path, source) == before
    require_startable(source.state_directory)


@pytest.mark.parametrize(
    "point", ["before_retirement", "after_retirement", "before_unfence"]
)
def test_interrupted_handoff_resumes_without_rotating_identity(
    source, monkeypatch, point
):
    before = inventory(source)
    original_unlink = Path.unlink
    original_sync = migration._sync_directory
    old = source.state_directory / "agentd.sqlite3"
    marker = source.state_directory / RESTORE_BARRIER

    def unlink(path, *args, **kwargs):
        if (point == "before_retirement" and path == old) or (
            point == "before_unfence" and path == marker
        ):
            raise RuntimeError("owned interruption")
        return original_unlink(path, *args, **kwargs)

    def sync(path):
        if point == "after_retirement" and path == source.state_directory:
            raise RuntimeError("owned interruption")
        original_sync(path)

    monkeypatch.setattr(Path, "unlink", unlink)
    monkeypatch.setattr(migration, "_sync_directory", sync)
    with pytest.raises(RuntimeError, match="owned interruption"):
        migration.migrate_storage(source.state_directory)
    with pytest.raises(RestorePendingError):
        require_startable(source.state_directory)
    monkeypatch.setattr(Path, "unlink", original_unlink)
    monkeypatch.setattr(migration, "_sync_directory", original_sync)
    assert migration.migrate_storage(source.state_directory) == before
    require_startable(source.state_directory)


def test_changed_task_state_during_recovery_refuses(source, monkeypatch):
    def interrupt(*args, **kwargs):
        raise OSError(errno.EDQUOT, "owned quota exhaustion")

    monkeypatch.setattr(migration.shutil, "copyfileobj", interrupt)
    with pytest.raises(OSError):
        migration.migrate_storage(source.state_directory)
    with closing(sqlite3.connect(source.task_path)) as db:
        db.execute("UPDATE tasks SET state='completed'")
        db.commit()
    with pytest.raises(StoreError, match="input changed"):
        migration.migrate_storage(source.state_directory)
    assert (source.state_directory / "agentd.sqlite3").exists()
    with pytest.raises(RestorePendingError):
        require_startable(source.state_directory)


def test_active_writer_is_not_stopped(source):
    before = inventory(source)
    with exclusive_writer(source.state_directory), pytest.raises(WriterActiveError):
        migration.migrate_storage(source.state_directory)
    assert inventory(source) == before


@pytest.mark.parametrize(
    "artifact",
    ["foreign_target", "journal", "super_journal", "restore_barrier", "symlink"],
)
def test_unowned_or_unrecovered_inputs_refuse(source, artifact):
    before = inventory(source)
    if artifact == "foreign_target":
        source.trace_path.write_bytes(b"unrelated")
    elif artifact == "journal":
        (source.state_directory / "agentd.sqlite3-journal").write_bytes(
            b"recover first"
        )
    elif artifact == "super_journal":
        (source.state_directory / "agentd.sqlite3-mj123").write_bytes(b"recover first")
    elif artifact == "restore_barrier":
        marker = source.state_directory / RESTORE_BARRIER
        marker.write_text('{"version":1,"state":"reconciliation_required"}')
        marker.chmod(0o600)
    else:
        source.trace_path.symlink_to(source.task_path)
    with pytest.raises(StoreError):
        migration.migrate_storage(source.state_directory)
    assert inventory(source) == before


def test_corrupt_barrier_remains_fenced(source):
    marker = source.state_directory / RESTORE_BARRIER
    marker.write_text('{"version":')
    marker.chmod(0o600)
    before = inventory(source)
    with pytest.raises(StoreError, match="barrier is invalid"):
        migration.migrate_storage(source.state_directory)
    assert inventory(source) == before
    with pytest.raises(RestorePendingError):
        require_startable(source.state_directory)


def test_sqlite_reader_prevents_migration_before_copy(source):
    before = inventory(source)
    with closing(sqlite3.connect(source.task_path)) as db:
        db.execute("BEGIN")
        db.execute("SELECT * FROM tasks").fetchall()
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            migration.migrate_storage(source.state_directory)
    assert inventory(source) == before
    assert not source.trace_path.exists()
    require_startable(source.state_directory)
