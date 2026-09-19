"""Owned jim-eq quota exhaustion and process-death migration qualification."""

import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import subprocess
import sys
import time


def worker(state, operation, filler):
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.storage_layout import StorageLayout
    from edgecitadel_agentd.storage_migration import _inventory, migrate_storage
    from edgecitadel_agentd.store import AgentdStore
    from edgecitadel_agentd.restore import require_startable, RestorePendingError

    layout = StorageLayout(state)
    if operation == "seed":
        store = AgentdStore(state / "agentd.sqlite3")
        try:
            store.create_task(
                sender_id="owned",
                recipient_id="remote",
                payload={"body": "owned migration"},
            )
        finally:
            store.close()
        print(json.dumps(_inventory(state / "agentd.sqlite3", layout)), flush=True)
    elif operation == "fill":
        with filler.open("wb", buffering=0) as target:
            # A failed MiB allocation can leave enough quota for a small database.
            # Exhaust the final filesystem blocks as well.
            for width in (1024 * 1024, 4096):
                try:
                    while True:
                        target.write(b"x" * width)
                except OSError as error:
                    assert error.errno == errno.EDQUOT
    elif operation == "full":
        before = _inventory(state / "agentd.sqlite3", layout)
        try:
            migrate_storage(state)
        except OSError as error:
            assert error.errno == errno.EDQUOT
        else:
            raise AssertionError("quota exhaustion did not refuse migration")
        assert _inventory(state / "agentd.sqlite3", layout) == before
        try:
            require_startable(state)
        except RestorePendingError:
            pass
        else:
            raise AssertionError("failed copy was not fenced")
    elif operation == "kill":
        unlink = Path.unlink

        def boundary(path, *args, **kwargs):
            result = unlink(path, *args, **kwargs)
            if path == state / "agentd.sqlite3":
                print("old authority retired", flush=True)
                while True:
                    time.sleep(1)
            return result

        Path.unlink = boundary
        migrate_storage(state)
    else:
        hashes = migrate_storage(state)
        assert _inventory(layout.trace_path, layout) == hashes
        require_startable(state)
        store = layout.open()
        try:
            assert (
                store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
                == 1
            )
        finally:
            store.close()
        print(json.dumps(hashes), flush=True)


def main(output=None):
    from test_trace_linux_quota import owned_volume

    report = {}
    with owned_volume() as (root, state, _, _):
        state.chmod(0o700)
        (state / "trace").unlink()
        (state / "trace").mkdir(mode=0o700)
        os.chown(state / "trace", 65534, 65534)
        filler = root / "fs/filler"
        filler.touch(mode=0o600)
        os.chown(filler, 65534, 65534)
        script = root / "migration-probe.py"
        shutil.copyfile(__file__, script)
        script.chmod(0o644)
        subprocess.run(
            ["mount", "--bind", str(root / "fs/trace"), str(state / "trace")],
            check=True,
        )
        child = None
        options = dict(
            user=65534,
            group=65534,
            extra_groups=[],
            cwd=root,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        def command(operation):
            return [
                sys.executable,
                str(script),
                "--worker",
                str(state),
                operation,
                str(filler),
            ]

        def run(operation):
            result = subprocess.run(command(operation), **options, timeout=30)
            assert result.returncode == 0, result.stderr
            return result.stdout

        try:
            hashes = json.loads(run("seed"))
            run("fill")
            run("full")
            report["real_edquot_preserves_source_and_fences_startup"] = True
            filler.unlink()
            child = subprocess.Popen(command("kill"), **options)
            assert select.select([child.stdout], [], [], 30)[0], (
                "handoff boundary not reached"
            )
            assert child.stdout.readline().strip() == "old authority retired"
            child.kill()
            child.communicate(timeout=10)
            assert child.returncode == -9
            child = None
            assert not (state / "agentd.sqlite3").exists()
            assert (state / "restore-barrier.json").exists()
            assert json.loads(run("resume")) == hashes
            report["sigkill_after_retirement_resumes_exact_pair_and_key"] = True
            report["quota_gated_store_reopens_with_task"] = True
        finally:
            if child is not None:
                child.kill()
                child.communicate(timeout=10)
            subprocess.run(["umount", str(state / "trace")], check=True)
    report["scratch_removed"] = not root.exists()
    report["test_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["scope"] = (
        "Owned schema-27 migration only; no live rollout or completion-capacity claim."
    )
    value = json.dumps(report, indent=2, sort_keys=True)
    if output:
        Path(output).write_text(value + "\n")
    print(value)


def test_native_migration():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq root quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else None)
