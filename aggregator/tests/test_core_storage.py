import json
import os
import select
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from aggregator.core_storage import StorageUnavailable, read_manifest, storage_lease


@pytest.fixture
def provisioned(tmp_path):
    for name in ("lifecycle.lock", "collector.lock", "trace.db", "mirror.db"):
        (tmp_path / name).touch(mode=0o600)
    record = {
        "version": 1,
        "state": "active",
        "generation": str(uuid4()),
        "trace": "trace.db",
        "mirror": "mirror.db",
    }
    (tmp_path / "storage.json").write_text(json.dumps(record))
    return tmp_path, record


def snapshot(root):
    return {
        p.name: (
            p.lstat().st_ino,
            p.lstat().st_mode,
            os.readlink(p) if p.is_symlink() else p.read_bytes(),
        )
        for p in root.iterdir()
    }


@pytest.mark.parametrize(
    "change",
    [
        {"version": 2},
        {"version": True},
        {"state": "prepared"},
        {"generation": "invalid"},
        {"trace": "../elsewhere.db"},
        {"trace": "/elsewhere.db"},
        {"trace": "x/../trace.db"},
        {"trace": "x//trace.db"},
        {"trace": "mirror.db"},
        {"unknown": 1},
    ],
)
def test_invalid_manifest_does_not_mutate(provisioned, change):
    root, record = provisioned
    (root / "storage.json").write_text(json.dumps({**record, **change}))
    before = snapshot(root)
    with storage_lease(root, "reader"), pytest.raises(StorageUnavailable):
        read_manifest(root)
    assert snapshot(root) == before


@pytest.mark.parametrize("text", ["{", "[]", '{"version":1,"version":1}', " " * 4097])
def test_corrupt_duplicate_and_oversize_records(provisioned, text):
    root, _ = provisioned
    (root / "storage.json").write_text(text)
    before = snapshot(root)
    with pytest.raises(StorageUnavailable, match="storage_manifest_invalid"):
        read_manifest(root)
    assert snapshot(root) == before


def test_manifest_is_immutable_and_roles_exist(provisioned):
    from dataclasses import FrozenInstanceError

    root, record = provisioned
    with storage_lease(root, "reader"):
        manifest = read_manifest(root)
        assert manifest.generation == record["generation"]
        assert manifest.path(root, "trace") == root / "trace.db"
        assert manifest.path(root, "mirror") == root / "mirror.db"
        with pytest.raises(FrozenInstanceError):
            manifest.trace = "wrong.db"
        (root / "trace.db").unlink()
        before = snapshot(root)
        with pytest.raises(StorageUnavailable):
            manifest.path(root, "trace")
        assert snapshot(root) == before


@pytest.mark.parametrize(
    "name", ["storage.json", "lifecycle.lock", "collector.lock", "trace.db"]
)
def test_symlinks_refused_without_mutation(provisioned, name):
    root, _ = provisioned
    path = root / name
    held = root / "held"
    path.rename(held)
    path.symlink_to(held)
    before = snapshot(root)
    with pytest.raises(StorageUnavailable), storage_lease(root, "collector"):
        read_manifest(root).path(root, "trace")
    assert snapshot(root) == before


def test_missing_lock_never_creates_a_directory(tmp_path):
    absent = tmp_path / "absent"
    with pytest.raises(StorageUnavailable), storage_lease(absent, "collector"):
        pass
    assert not absent.exists()
    with pytest.raises(StorageUnavailable), storage_lease(tmp_path, "collector"):
        pass
    assert list(tmp_path.iterdir()) == []


def test_readers_coexist_with_single_collector_and_block_maintenance(provisioned):
    root, _ = provisioned
    before = snapshot(root)
    with storage_lease(root, "collector"), storage_lease(root, "reader"):
        for kind in ("collector", "maintenance"):
            with (
                pytest.raises(StorageUnavailable, match="storage_owner_busy"),
                storage_lease(root, kind),
            ):
                pass
    with storage_lease(root, "maintenance"):
        for kind in ("reader", "collector", "maintenance"):
            with (
                pytest.raises(StorageUnavailable, match="storage_owner_busy"),
                storage_lease(root, kind),
            ):
                pass
    assert snapshot(root) == before


def test_process_death_releases_both_leases(provisioned):
    root, _ = provisioned
    code = """import sys
from pathlib import Path
from aggregator.core_storage import storage_lease
with storage_lease(Path(sys.argv[1]), 'collector'):
    print('ready', flush=True)
    sys.stdin.read()
"""
    job = subprocess.Popen(
        [sys.executable, "-c", code, str(root)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=Path(__file__).resolve().parents[2],
    )
    try:
        assert select.select([job.stdout], [], [], 10)[0], "child startup timeout"
        assert job.stdout.readline().strip() == "ready"
        with storage_lease(root, "reader"):
            pass
        with (
            pytest.raises(StorageUnavailable, match="storage_owner_busy"),
            storage_lease(root, "maintenance"),
        ):
            pass
        job.kill()
        job.wait(timeout=10)
        with storage_lease(root, "maintenance"):
            pass
        with storage_lease(root, "collector"):
            pass
    finally:
        if job.poll() is None:
            job.kill()
        job.communicate(timeout=10)


def test_missing_writer_lock_releases_lifecycle_lease(provisioned):
    root, _ = provisioned
    (root / "collector.lock").unlink()
    with pytest.raises(StorageUnavailable), storage_lease(root, "collector"):
        pass
    with storage_lease(root, "maintenance"):
        pass


def test_body_exception_is_preserved_and_leases_released(provisioned):
    root, _ = provisioned
    error = BlockingIOError("caller failure")
    with pytest.raises(BlockingIOError) as caught, storage_lease(root, "collector"):
        raise error
    assert caught.value is error
    with storage_lease(root, "maintenance"):
        pass


def test_nested_symlink_is_refused(provisioned):
    root, record = provisioned
    (root / "alias").symlink_to(root, target_is_directory=True)
    (root / "storage.json").write_text(
        json.dumps({**record, "trace": "alias/trace.db"})
    )
    with storage_lease(root, "reader"), pytest.raises(StorageUnavailable):
        read_manifest(root).path(root, "trace")


@pytest.mark.parametrize(
    "name", ["storage.json", "lifecycle.lock", "collector.lock", "trace.db"]
)
def test_fifo_refused_without_blocking(provisioned, name):
    root, _ = provisioned
    (root / name).unlink()
    os.mkfifo(root / name)
    with pytest.raises(StorageUnavailable), storage_lease(root, "collector"):
        read_manifest(root).path(root, "trace")


def test_deeply_nested_manifest_has_fixed_refusal(provisioned):
    root, _ = provisioned
    (root / "storage.json").write_text("[" * 2000 + "]" * 2000)
    with pytest.raises(StorageUnavailable, match="storage_manifest_invalid"):
        read_manifest(root)
