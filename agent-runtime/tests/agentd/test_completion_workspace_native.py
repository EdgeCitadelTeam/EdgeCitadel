"""Owned jim-eq allocator qualification, before task/trace integration.

This exercises physical slot/workspace primitives and attached atomicity. It does
not run AgentdStore lifecycle admission, real trace export or production recovery.
"""

import ctypes
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import sqlite3
import subprocess
import sys


def worker(state, operation):
    os.umask(0o077)
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.storage_sqlite import configure_scratch
    from edgecitadel_agentd.storage_workspace import (
        CompletionWorkspace,
        ReservedConnection,
    )
    from edgecitadel_agentd.trace_reservations import (
        MAX_SLOTS,
        SLOT_BYTES,
        SCHEMA_SQL,
        Obligation,
        fill,
        read,
        reserve,
    )
    from edgecitadel_agentd.trace_contract import canonical_bytes
    from edgecitadel_agentd.trace_quota import verify_trace_quota

    trace = (state / "trace").resolve(strict=True)
    if operation == "wait_writer":
        descriptor = os.open(trace / "completion.reserve", os.O_RDONLY)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print(json.dumps({"barrier": "writer_owner_still_held"}), flush=True)
            else:
                raise AssertionError(
                    "writer ownership ended before reserve restoration"
                )
        finally:
            os.close(descriptor)
    physical = CompletionWorkspace(trace / "completion.reserve")
    db = None
    try:
        db = sqlite3.connect(trace / "trace.sqlite3", factory=ReservedConnection)
        configure_scratch(db)
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA synchronous=EXTRA")
        db.execute("ATTACH DATABASE ? AS task_state", (str(state / "tasks.sqlite3"),))
        db.execute("PRAGMA task_state.journal_mode=DELETE")
        db.execute("PRAGMA task_state.synchronous=EXTRA")
        # Force both members' recovery while the external writer owner is held.
        db.execute("SELECT count(*) FROM main.sqlite_schema").fetchone()
        db.execute("SELECT count(*) FROM task_state.sqlite_schema").fetchone()
        db.install_workspace(physical)
        obligations = [
            Obligation("operation", str(i).zfill(128), "t" * 32)
            for i in range(MAX_SLOTS)
        ]

        def record(i):
            value = {"slot": i, "phase": "completed", "detail": ""}
            value["detail"] = "x" * (
                SLOT_BYTES - len(canonical_bytes(value, limit=SLOT_BYTES))
            )
            assert len(canonical_bytes(value, limit=SLOT_BYTES)) == SLOT_BYTES
            return value

        def quota_bytes():
            return verify_trace_quota(trace, state).allocated_bytes

        def snapshot():
            return {
                "pages": db.execute("PRAGMA main.page_count").fetchone()[0],
                "filled": db.execute(
                    "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
                ).fetchone()[0],
                "tasks": db.execute(
                    "SELECT phase,count(*) FROM task_state.work GROUP BY phase"
                ).fetchall(),
                "counters": [
                    (bytes(v).decode(), count)
                    for v, count in db.execute(
                        "SELECT value,count(*) FROM counters GROUP BY value"
                    )
                ],
                "workspace_bytes": os.fstat(physical.descriptor).st_blocks * 512,
                "quota_bytes": quota_bytes(),
            }

        if operation == "seed":
            # The schema is owned by this probe; no service store is touched.
            for statement in SCHEMA_SQL.split(";"):
                if statement.strip():
                    db.execute(statement)
            with db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "CREATE TABLE counters (id INTEGER PRIMARY KEY, value BLOB NOT NULL CHECK(length(value)=20), padding BLOB NOT NULL)"
                )
                db.execute("CREATE TABLE pressure (id INTEGER PRIMARY KEY, value BLOB)")
                db.execute(
                    "CREATE TABLE task_state.work (id INTEGER PRIMARY KEY, phase TEXT NOT NULL)"
                )
                for i, obligation in enumerate(obligations):
                    reserve(db, obligation)
                    db.executemany(
                        "INSERT INTO counters VALUES (?,?,?)",
                        [
                            (2 * i + offset, b"00000000000000000001", b"z" * 3000)
                            for offset in (0, 1)
                        ],
                    )
                    db.execute("INSERT INTO task_state.work VALUES (?,'pending')", (i,))
            assert db.execute("PRAGMA freelist_count").fetchone()[0] == 0
            failure = None
            for _ in range(1024):
                try:
                    with db:
                        db.execute("BEGIN IMMEDIATE")
                        db.executemany(
                            "INSERT INTO pressure(value) VALUES (?)",
                            [(b"p" * 65536,)] * 8,
                        )
                except sqlite3.OperationalError as error:
                    assert error.sqlite_errorcode in {
                        sqlite3.SQLITE_FULL,
                        sqlite3.SQLITE_IOERR_WRITE,
                    }, (
                        error.sqlite_errorcode,
                        str(error),
                    )
                    failure = error.sqlite_errorname
                    break
            assert failure in {"SQLITE_FULL", "SQLITE_IOERR_WRITE"}
            # SQLite reports EDQUOT as IOERR_WRITE on this runtime. Establish
            # actual kernel quota exhaustion independently, not by message text.
            probe = trace / "quota-probe"
            try:
                with probe.open("xb") as target:
                    try:
                        os.posix_fallocate(target.fileno(), 0, 8 * 1024 * 1024)
                    except OSError as error:
                        assert error.errno == errno.EDQUOT
                    else:
                        raise AssertionError("pressure was not kernel quota exhaustion")
            finally:
                probe.unlink()
            before = snapshot()
            try:
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    db.execute(
                        "INSERT INTO task_state.work VALUES (99999,'must-rollback')"
                    )
                    db.execute(
                        "INSERT INTO pressure(value) VALUES (?)",
                        (b"q" * (1024 * 1024),),
                    )
            except sqlite3.OperationalError as error:
                assert error.sqlite_errorcode in {
                    sqlite3.SQLITE_FULL,
                    sqlite3.SQLITE_IOERR_WRITE,
                }
            else:
                raise AssertionError("new paired admission unexpectedly fitted")
            assert snapshot() == before
            print(
                json.dumps(
                    {
                        "operation": operation,
                        "actual_full": failure,
                        "kernel_edquot_confirmed": True,
                        "paired_refusal_unchanged": True,
                        **before,
                    }
                ),
                flush=True,
            )
        elif operation in {"before_commit", "after_commit", "hold_restore"}:
            initial_pages = db.execute("PRAGMA page_count").fetchone()[0]
            if operation != "before_commit":
                restore = physical.restore

                def stopped_restore():
                    if physical.borrowed:
                        assert not db.in_transaction
                        print(
                            json.dumps(
                                {
                                    "barrier": "committed_before_refill",
                                    "sqlite_commit_returned": True,
                                    "workspace_bytes": os.fstat(
                                        physical.descriptor
                                    ).st_blocks
                                    * 512,
                                    "quota_bytes": quota_bytes(),
                                }
                            ),
                            flush=True,
                        )
                        assert sys.stdin.readline().strip() == "resume"
                    restore()

                physical.restore = stopped_restore
            with db:
                db.execute("BEGIN IMMEDIATE")
                db.use_completion_workspace()
                if operation == "hold_restore":
                    db.execute(
                        "UPDATE counters SET value=?", (b"00000000000000000003",)
                    )
                    db.execute("UPDATE task_state.work SET phase='closed'")
                else:
                    for i, obligation in enumerate(obligations):
                        fill(db, obligation, record(i))
                    db.execute(
                        "UPDATE counters SET value=?", (b"00000000000000000002",)
                    )
                    db.execute("UPDATE task_state.work SET phase='completed'")
                assert db.execute("PRAGMA page_count").fetchone()[0] == initial_pages
                if operation == "before_commit":
                    journal = trace / "trace.sqlite3-journal"
                    print(
                        json.dumps(
                            {
                                "barrier": "before_commit",
                                "journal_bytes": journal.stat().st_size,
                                "journal_allocated": journal.stat().st_blocks * 512,
                                **snapshot(),
                            }
                        ),
                        flush=True,
                    )
                    assert sys.stdin.readline().strip() == "resume"
            # Capture a consistent final observation under a new owned read
            # transaction; another connection may otherwise borrow during this
            # diagnostic's independent SELECTs after our commit released ownership.
            with db:
                db.execute("BEGIN IMMEDIATE")
                completed = snapshot()
            print(json.dumps({"operation": operation, **completed}), flush=True)
        else:
            if operation in {"survive_rollback", "survive_complete"}:
                print(json.dumps({"barrier": "existing_connection_ready"}), flush=True)
                assert sys.stdin.readline().strip() == "resume"
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    value = snapshot()
                value["same_connection_survived"] = True
            else:
                value = snapshot()
            if operation in {"verify_rollback", "survive_rollback"}:
                assert value["filled"] == 0 and value["tasks"] == [
                    ("pending", MAX_SLOTS)
                ]
                assert value["counters"] == [("00000000000000000001", 2 * MAX_SLOTS)]
            elif operation in {"verify_complete", "survive_complete"}:
                assert value["filled"] == MAX_SLOTS and value["tasks"] == [
                    ("completed", MAX_SLOTS)
                ]
                assert value["counters"] == [("00000000000000000002", 2 * MAX_SLOTS)]
                for i in range(MAX_SLOTS):
                    assert read(db, i + 1) == record(i)
            elif operation == "wait_writer":
                assert value["tasks"] == [("closed", MAX_SLOTS)]
                assert value["counters"] == [("00000000000000000003", 2 * MAX_SLOTS)]
            else:
                raise AssertionError(operation)
            assert value["workspace_bytes"] >= 32 * 1024 * 1024
            for schema in ("main", "task_state"):
                assert (
                    db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] == "ok"
                )
            print(
                json.dumps({"operation": operation, "integrity": "ok", **value}),
                flush=True,
            )
    finally:
        if db is not None:
            installed = db.workspace is physical
            db.close()
            if not installed:
                physical.close()
        else:
            physical.close()


def main(output=None):
    from test_trace_linux_quota import owned_volume

    report = {
        "scope": "Slot/workspace primitives only; production task/trace admission remains unimplemented."
    }
    with owned_volume() as (root, state, _, _):
        state.chmod(0o700)
        script = root / "completion-probe.py"
        shutil.copyfile(__file__, script)
        script.chmod(0o644)
        options = dict(
            user=65534,
            group=65534,
            extra_groups=[],
            cwd=root,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        children = []

        def command(operation):
            return [sys.executable, str(script), "--worker", str(state), operation]

        def run(operation):
            result = subprocess.run(command(operation), **options, timeout=60)
            assert result.returncode == 0, result.stderr
            return json.loads(result.stdout)

        def pause(operation):
            child = subprocess.Popen(command(operation), **options)
            children.append(child)
            assert select.select([child.stdout], [], [], 30)[0], (
                "completion boundary not reached"
            )
            line = child.stdout.readline()
            if not line:
                child.wait(timeout=5)
                raise AssertionError(child.stderr.read())
            return child, json.loads(line)

        try:
            report["pressure"] = run("seed")
            child, report["before_commit"] = pause("before_commit")
            assert report["before_commit"]["barrier"] == "before_commit"
            child.kill()
            child.communicate(timeout=10)
            assert child.returncode == -9
            report["rollback_recovery"] = run("verify_rollback")
            survivor, ready = pause("survive_rollback")
            assert ready["barrier"] == "existing_connection_ready"
            child, report["existing_handle_before_commit"] = pause("before_commit")
            child.kill()
            child.communicate(timeout=10)
            assert child.returncode == -9
            survivor.stdin.write("resume\n")
            survivor.stdin.flush()
            remaining, error = survivor.communicate(timeout=15)
            assert survivor.returncode == 0, error
            report["existing_handle_rollback"] = json.loads(remaining)
            survivor, ready = pause("survive_complete")
            assert ready["barrier"] == "existing_connection_ready"
            child, report["after_commit"] = pause("after_commit")
            assert report["after_commit"]["barrier"] == "committed_before_refill"
            child.kill()
            child.communicate(timeout=10)
            assert child.returncode == -9
            survivor.stdin.write("resume\n")
            survivor.stdin.flush()
            remaining, error = survivor.communicate(timeout=15)
            assert survivor.returncode == 0, error
            report["existing_handle_commit"] = json.loads(remaining)
            report["committed_recovery"] = run("verify_complete")
            first, report["ownership_barrier"] = pause("hold_restore")
            second = subprocess.Popen(command("wait_writer"), **options)
            children.append(second)
            assert select.select([second.stdout], [], [], 15)[0]
            report["second_writer_blocked"] = json.loads(second.stdout.readline())
            assert (
                report["second_writer_blocked"]["barrier"] == "writer_owner_still_held"
            )
            assert not select.select([second.stdout], [], [], 0.25)[0]
            assert second.poll() is None, second.stderr.read()
            first.stdin.write("resume\n")
            first.stdin.flush()
            remaining, error = first.communicate(timeout=15)
            assert first.returncode == 0, error
            report["restored_first_writer"] = json.loads(remaining)
            remaining, error = second.communicate(timeout=15)
            assert second.returncode == 0, error
            report["next_writer"] = json.loads(remaining)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=10)
    report["scratch_removed"] = not root.exists()
    assert report["scratch_removed"]
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


def test_native_completion_workspace():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), sys.argv[3])
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else None)
