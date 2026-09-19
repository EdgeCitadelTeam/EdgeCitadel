"""Existing work seeding/refusal and recovery on an owned jim-eq quota volume."""

import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys


def worker(state, near_limit=False, inode_pressure=False):
    os.umask(0o077)
    state.chmod(0o700)
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.storage_workspace import CompletionWorkspace
    from edgecitadel_agentd.store import AgentdStore
    from edgecitadel_agentd.trace_seed import seed_existing_work
    from edgecitadel_agentd.trace_quota import verify_trace_quota
    from edgecitadel_agentd.trace_counters import MAX_COUNTER, encode_counter

    trace = (state / "trace").resolve(strict=True)
    (state / "agentd").mkdir(mode=0o700)
    (state / "node.json").write_text('{"agent_id":"edge-a"}')
    physical = CompletionWorkspace(trace / "completion.reserve")
    store = AgentdStore(
        trace / "agentd.sqlite3",
        task_path=state / "agentd/tasks.sqlite3",
        payload_key_path=state / "agentd/payload.key",
    )
    try:
        token = store.register_connector(
            connector_id="native", host_type="codex", agent_id="worker", capabilities=[]
        )
        session = store.open_session(connector_id="native", token=token)["session_id"]
        for _ in range(16):
            task = store.create_task(
                sender_id="origin", recipient_id="worker", payload={}
            )
            assert (
                store.claim_next_task(
                    connector_id="native", token=token, session_id=session
                )["task_id"]
                == task["task_id"]
            )
        db = store._connection
        db.install_workspace(physical)

        def facts():
            return {
                name: [
                    tuple(row) for row in db.execute(f"SELECT * FROM {name} ORDER BY 1")
                ]
                for name in (
                    "tasks",
                    "task_attempts",
                    "events_all",
                    "trace_journal_all",
                    "trace_sources",
                    "trace_export_generations",
                    "sessions",
                    "presence_history_all",
                )
            }

        pressure = trace / "owned-pressure"

        inode_files = []
        inode_counts = []

        def fill():
            with pressure.open("xb", buffering=0) as handle:
                if inode_pressure:
                    for index in range(256):
                        path = trace / f"i{index}"
                        try:
                            path.touch(mode=0o600, exist_ok=False)
                        except OSError as error:
                            assert error.errno == errno.EDQUOT
                            break
                        inode_files.append(path)
                    else:
                        raise AssertionError("inode quota was not enforced")
                    quota = verify_trace_quota(trace, state)
                    assert quota.allocated_inodes == quota.hard_inodes == 128
                    inode_counts.append(quota.allocated_inodes)
                for size in (1024 * 1024, 4096):
                    while True:
                        try:
                            handle.write(b"x" * size)
                        except OSError as error:
                            assert error.errno == errno.EDQUOT
                            break
                os.fsync(handle.fileno())
            return verify_trace_quota(trace, state).allocated_bytes

        before = facts()
        at_refusal = fill()
        try:
            seed_existing_work(store)
        except sqlite3.OperationalError as error:
            expected = {sqlite3.SQLITE_FULL, sqlite3.SQLITE_IOERR}
            if inode_pressure:
                expected.add(sqlite3.SQLITE_CANTOPEN)
            assert error.sqlite_errorcode & 255 in expected
        else:
            raise AssertionError("seeding admitted without quota capacity")
        assert facts() == before
        assert (
            db.execute("SELECT count(*) FROM trace_completion_slots").fetchone()[0] == 0
        )
        assert not physical.borrowed
        pressure.unlink()
        for path in inode_files:
            path.unlink()
        inode_files.clear()
        seeded = seed_existing_work(store)
        assert seeded == {
            "required_pending": 34,
            "newly_reserved": 34,
            "occupied": 34,
            "completed": 0,
        }
        assert facts() == before
        assert seed_existing_work(store)["newly_reserved"] == 0
        if near_limit:
            # Synthetic near-exhaustion identities isolate the arithmetic bound;
            # quota, allocation and all recovery transactions remain native.
            with db:
                db.execute(
                    "UPDATE trace_sources SET next_source_seq_bytes=?",
                    (encode_counter(MAX_COUNTER - 32),),
                )
                db.execute(
                    "UPDATE trace_export_generations SET next_export_seq_bytes=?",
                    (encode_counter(MAX_COUNTER - 32),),
                )
                db.execute(
                    "UPDATE trace_presence_counter SET next_id=?",
                    (encode_counter(MAX_COUNTER - 1),),
                )
        at_recovery = fill()
        pages = db.execute("PRAGMA page_count").fetchone()[0]
        store.close_session(connector_id="native", token=token, session_id=session)
        assert db.execute("PRAGMA page_count").fetchone()[0] == pages
        assert (
            db.execute(
                "SELECT count(*) FROM tasks WHERE state='queued' AND claimed_session_id IS NULL"
            ).fetchone()[0]
            == 16
        )
        assert (
            db.execute(
                "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
            ).fetchone()[0]
            == 17
        )
        assert (
            db.execute("SELECT count(*) FROM presence_history_all").fetchone()[0] == 2
        )
        assert physical.reserved
        quota_after = verify_trace_quota(trace, state)
        if inode_pressure:
            assert quota_after.allocated_inodes == 128
        positions = None
        if near_limit:
            for row in db.execute("SELECT task_id FROM tasks").fetchall():
                store.transition_task(
                    task_id=row[0], state="cancelled", actor_id="origin"
                )
            positions = {
                "source": db.execute(
                    "SELECT next_source_seq FROM trace_sources"
                ).fetchone()[0],
                "export": db.execute(
                    "SELECT next_export_seq FROM trace_export_generations"
                ).fetchone()[0],
                "presence": int(
                    db.execute("SELECT next_id FROM trace_presence_counter").fetchone()[
                        0
                    ]
                ),
            }
            assert set(positions.values()) == {MAX_COUNTER}
            assert (
                db.execute(
                    "SELECT count(*) FROM tasks WHERE state='cancelled'"
                ).fetchone()[0]
                == 16
            )
            assert db.execute("PRAGMA page_count").fetchone()[0] == pages
        for schema in ("main", "task_state"):
            assert db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] == "ok"
        print(
            json.dumps(
                {
                    "inode_pressure": inode_pressure,
                    "inode_counts_at_pressure": inode_counts,
                    "inodes_after_completion": quota_after.allocated_inodes,
                    "quota_at_seed_refusal": at_refusal,
                    "quota_at_recovery": at_recovery,
                    "seeded": seeded,
                    "recovered_tasks": 16,
                    "filled": 33 if near_limit else 17,
                    "exhausted_positions": positions,
                    "integrity": "ok",
                }
            )
        )
    finally:
        installed = store._connection.workspace is physical
        store.close()
        if not installed:
            physical.close()


def main(output=None, near_limit=False, inode_pressure=False):
    from test_trace_linux_quota import owned_volume

    with owned_volume() as (root, state, _, _):
        script = root / "seed-probe.py"
        shutil.copyfile(__file__, script)
        result = subprocess.run(
            [
                sys.executable,
                str(script),
                "--worker",
                str(state),
                str(int(near_limit)),
                str(int(inode_pressure)),
            ],
            user=65534,
            group=65534,
            extra_groups=[],
            cwd=root,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
    assert not root.exists()
    report["scratch_removed"] = True
    source = Path(__file__).resolve().parents[2] / "src/edgecitadel_agentd"
    report["runtime_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(source.glob("*.py"))
    }
    report["test_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    value = json.dumps(report, indent=2) + "\n"
    if output:
        Path(output).write_text(value)
    print(value)


def test_native_existing_work_seed():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), bool(int(sys.argv[3])), bool(int(sys.argv[4])))
    else:
        main(
            sys.argv[1] if len(sys.argv) > 1 else None,
            near_limit=len(sys.argv) > 2 and sys.argv[2] == "headroom",
            inode_pressure=len(sys.argv) > 2 and sys.argv[2] == "inodes",
        )
