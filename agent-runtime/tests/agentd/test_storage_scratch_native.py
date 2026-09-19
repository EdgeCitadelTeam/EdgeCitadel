"""Owned jim-eq quota-volume check for unlinked SQLite scratch files."""

import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys


def scratch_files():
    result = []
    for path in Path("/proc/self/fd").iterdir():
        try:
            target = path.readlink()
        except FileNotFoundError:
            continue
        if "etilqs" in str(target):
            result.append({"target": str(target), "bytes": path.stat().st_size})
    return result


def worker(state, control):
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.storage_layout import StorageLayout

    layout = StorageLayout(state)
    store = layout.open()
    db = store._connection
    try:
        if control:
            # Deliberate previous policy: prove this same workload actually
            # creates unlinked files rather than merely checking pragma values.
            db.execute("PRAGMA temp_store=FILE")
            db.execute("PRAGMA cache_spill=ON")
        db.execute("PRAGMA main.cache_size=8")
        with db:
            db.execute(
                "CREATE TABLE scratch_probe (id INTEGER PRIMARY KEY, value BLOB)"
            )
            db.executemany(
                "INSERT INTO scratch_probe VALUES (?,?)",
                ((number, bytes([number % 251]) * 16384) for number in range(1024)),
            )
        before = hashlib.sha256(layout.trace_path.read_bytes()).hexdigest()
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE scratch_probe SET value=zeroblob(16384)")
        db.execute("SAVEPOINT rewrite")
        db.execute("UPDATE scratch_probe SET value=?", (b"x" * 16384,))
        savepoint_files = scratch_files()
        cursor = db.execute("SELECT id,value FROM scratch_probe ORDER BY value,id DESC")
        assert cursor.fetchone() is not None
        sort_files = scratch_files()
        cursor.close()
        changed = hashlib.sha256(layout.trace_path.read_bytes()).hexdigest() != before
        db.execute("ROLLBACK TO rewrite")
        db.execute("RELEASE rewrite")
        db.rollback()
        assert hashlib.sha256(layout.trace_path.read_bytes()).hexdigest() == before
        assert changed is control
        if control:
            assert savepoint_files and sort_files
            assert any(item["bytes"] > 1024 * 1024 for item in savepoint_files)
        else:
            assert not savepoint_files and not sort_files
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        with db:
            db.execute("DROP TABLE scratch_probe")
        print(
            json.dumps(
                {
                    "control": control,
                    "sqlite_version": sqlite3.sqlite_version,
                    "compile_options": [
                        row[0] for row in db.execute("PRAGMA compile_options")
                    ],
                    "savepoint_scratch": savepoint_files,
                    "sort_scratch": sort_files,
                    "database_changed_before_commit": changed,
                    "exact_rollback": True,
                    "quota_allocated_bytes": layout.verify().allocated_bytes,
                }
            ),
            flush=True,
        )
    finally:
        store.close()


def main(output=None):
    from test_trace_linux_quota import owned_volume

    reports = []
    with owned_volume() as (root, state, _, _):
        state.chmod(0o700)
        (state / "trace").unlink()
        (state / "trace").mkdir(mode=0o700)
        os.chown(state / "trace", 65534, 65534)
        script = root / "scratch-probe.py"
        shutil.copyfile(__file__, script)
        script.chmod(0o644)
        subprocess.run(
            ["mount", "--bind", str(root / "fs/trace"), str(state / "trace")],
            check=True,
        )
        try:
            for control in (False, True):
                result = subprocess.run(
                    [
                        sys.executable,
                        str(script),
                        "--worker",
                        str(state),
                        str(int(control)),
                    ],
                    user=65534,
                    group=65534,
                    extra_groups=[],
                    cwd=root,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                    capture_output=True,
                    text=True,
                    timeout=45,
                )
                assert result.returncode == 0, result.stderr
                reports.append(json.loads(result.stdout))
        finally:
            subprocess.run(["umount", str(state / "trace")], check=True)
    source = Path(__file__).resolve().parents[2] / "src/edgecitadel_agentd/store.py"
    report = {
        "cases": reports,
        "scratch_removed": not root.exists(),
        "runtime_sha256": {
            str(path.relative_to(source.parent)): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in sorted(source.parent.glob("*.py"))
        },
        "test_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "Owned quota-gated store; no completion reservation or full-pressure claim.",
    }
    assert report["scratch_removed"]
    encoded = json.dumps(report, indent=2) + "\n"
    if output:
        Path(output).write_text(encoded)
    print(encoded)


def test_native_scratch():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq root quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), bool(int(sys.argv[3])))
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else None)
