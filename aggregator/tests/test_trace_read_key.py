import os
from concurrent.futures import ThreadPoolExecutor

import pytest

from aggregator.trace_read_key import load_or_create_key


def test_concurrent_key_creation_is_private_persistent_and_leaves_no_temporary_files(
    tmp_path,
):
    path = tmp_path / "cursor.key"
    with ThreadPoolExecutor(max_workers=8) as workers:
        keys = list(workers.map(lambda _: load_or_create_key(path), range(32)))
    assert len(set(keys)) == 1 and len(keys[0]) == 32
    assert path.read_bytes() == keys[0] == load_or_create_key(path)
    assert path.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "kind", ["public", "short", "long", "symlink", "directory", "fifo"]
)
def test_invalid_key_fails_without_replacing_it(tmp_path, kind):
    path = tmp_path / "key"
    target = tmp_path / "target"
    if kind == "symlink":
        target.write_bytes(b"PRIVATE_SENTINEL" * 3)
        path.symlink_to(target)
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path, 0o600)
    else:
        path.write_bytes(
            b"x" * (31 if kind == "short" else 33 if kind == "long" else 32)
        )
        path.chmod(0o644 if kind == "public" else 0o600)
    original = path.lstat()
    with pytest.raises(ValueError, match="^trace_cursor_key_unavailable$"):
        load_or_create_key(path)
    assert path.lstat().st_ino == original.st_ino
    if kind == "symlink":
        assert target.read_bytes() == b"PRIVATE_SENTINEL" * 3
