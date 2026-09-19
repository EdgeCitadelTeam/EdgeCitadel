"""Owned jim-eq full session reconciliation at Linux user quota.

Closes a session with accepted tasks, an open run and an open operation,
including local presence in the same paired transaction. Workspace is fixture-installed.
"""

import ctypes
import errno
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from uuid import UUID, uuid4


def worker(state, operation, fixture, mode, inode_pressure=False):
    os.umask(0o077)
    state.chmod(0o700)
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.store import AgentdStore, StoreError
    from edgecitadel_agentd.storage_workspace import CompletionWorkspace
    from edgecitadel_agentd.trace_contract import canonical_bytes
    from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
    from edgecitadel_agentd.trace_quota import verify_trace_quota
    from edgecitadel_agentd.trace_reservations import MAX_SLOTS

    task_count = (MAX_SLOTS - 2) // 4
    trace = (state / "trace").resolve(strict=True)
    physical = CompletionWorkspace(trace / "completion.reserve")
    store = None
    try:
        if operation == "seed":
            (state / "agentd").mkdir(mode=0o700)
            (state / "node.json").write_text('{"agent_id":"edge-a"}')
        store = AgentdStore(
            trace / "agentd.sqlite3",
            task_path=state / "agentd/tasks.sqlite3",
            payload_key_path=state / "agentd/payload.key",
        )
        db = store._connection
        db.install_workspace(physical)
        task_ids = [
            str(
                UUID(
                    bytes=hashlib.sha256(f"owned-task-{i}".encode()).digest()[:16],
                    version=4,
                )
            )
            for i in range(task_count)
        ]

        def recover():
            if mode == "revoke":
                return store.revoke_connector("native")
            return store.reconcile(now_ms=time.time_ns() // 1_000_000 + 600_000)

        def snapshot():
            return {
                "allocated_inodes": verify_trace_quota(trace, state).allocated_inodes,
                "inode_reserve_ready": physical.reserved,
                "connector_revoked": db.execute(
                    "SELECT revoked_at_ms IS NOT NULL FROM connectors WHERE connector_id='native'"
                ).fetchone()[0],
                "closed_sessions": db.execute(
                    "SELECT count(*) FROM sessions WHERE closed_at_ms IS NOT NULL"
                ).fetchone()[0],
                "presence": [
                    list(row)
                    for row in db.execute(
                        "SELECT presence_id,state,reason FROM presence_history_all ORDER BY presence_id"
                    )
                ],
                "run_closed": db.execute(
                    "SELECT closed_at_ms IS NOT NULL FROM trace_bindings_all"
                ).fetchone()[0],
                "operation_phase": db.execute(
                    "SELECT phase FROM trace_operations_all"
                ).fetchone()[0],
                "claimed": db.execute(
                    "SELECT count(*) FROM tasks WHERE claimed_session_id IS NOT NULL"
                ).fetchone()[0],
                "task_states": dict(
                    db.execute("SELECT state,count(*) FROM tasks GROUP BY state")
                ),
                "attempts": db.execute("SELECT count(*) FROM task_attempts").fetchone()[
                    0
                ],
                "outbox": db.execute(
                    "SELECT count(*) FROM transport_outbox"
                ).fetchone()[0],
                "legacy_events": db.execute(
                    "SELECT count(*) FROM events_all"
                ).fetchone()[0],
                "indexed_legacy_events": db.execute(
                    "SELECT count(*) FROM events"
                ).fetchone()[0],
                "pages": db.execute("PRAGMA page_count").fetchone()[0],
                "filled": db.execute(
                    "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
                ).fetchone()[0],
                "events": db.execute(
                    "SELECT count(*) FROM trace_journal_all"
                ).fetchone()[0],
                "indexed_events": db.execute(
                    "SELECT count(*) FROM trace_journal"
                ).fetchone()[0],
                "next_source_seq": db.execute(
                    "SELECT next_source_seq FROM trace_sources"
                ).fetchone()[0],
                "next_export_seq": db.execute(
                    "SELECT next_export_seq FROM trace_export_generations"
                ).fetchone()[0],
                "workspace_bytes": os.fstat(physical.descriptor).st_blocks * 512,
                "quota_bytes": verify_trace_quota(trace, state).allocated_bytes,
            }

        if operation == "seed":
            token = store.register_connector(
                connector_id="native",
                host_type="codex",
                agent_id="worker",
                capabilities=["edgecitadel_trace"],
            )
            session = store.open_session(
                connector_id="native", token=token, lease_seconds=300
            )["session_id"]
            for task_id in task_ids:
                store.create_task(
                    task_id=task_id,
                    sender_id="origin",
                    recipient_id="worker",
                    payload={"owned": True},
                )
                assert (
                    store.claim_next_task(
                        connector_id="native", token=token, session_id=session
                    )["task_id"]
                    == task_id
                )
            binding = store.bind_trace(
                node_id="edge-a",
                connector_id="native",
                token=token,
                params={
                    "schema_version": 1,
                    "request_id": str(uuid4()),
                    "session_id": session,
                    "task_id": None,
                    "context_id": None,
                },
            )["result"]
            store.append_trace(
                node_id="edge-a",
                connector_id="native",
                token=token,
                params={
                    "schema_version": 1,
                    "binding_id": binding["binding_id"],
                    "observation_id": str(uuid4()),
                    "observation": {
                        "schema_version": 1,
                        "kind": "tool",
                        "phase": "started",
                        "span_id": str(uuid4()),
                        "parent_span_id": None,
                        "occurred_at": "2026-09-19T12:00:00.000Z",
                        "duration_ms": None,
                        "attributes": {"name": "owned-operation"},
                    },
                },
            )
            with db:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "CREATE TABLE owned_pressure(id INTEGER PRIMARY KEY,payload BLOB)"
                )
            for _ in range(1024):
                try:
                    with db:
                        db.execute("BEGIN IMMEDIATE")
                        db.executemany(
                            "INSERT INTO owned_pressure(payload) VALUES (?)",
                            [(b"p" * 65536,)] * 8,
                        )
                except sqlite3.OperationalError as error:
                    assert error.sqlite_errorcode in (
                        sqlite3.SQLITE_FULL,
                        sqlite3.SQLITE_IOERR_WRITE,
                    )
                    break
            else:
                raise AssertionError("kernel quota was not reached")
            probe = trace / "owned-probe"
            try:
                with probe.open("xb") as handle:
                    try:
                        os.posix_fallocate(handle.fileno(), 0, 8 * 1024 * 1024)
                    except OSError as error:
                        assert error.errno == errno.EDQUOT
                    else:
                        raise AssertionError("expected EDQUOT")
            finally:
                probe.unlink()
            if inode_pressure:
                for index in range(256):
                    try:
                        (trace / f"i{index}").touch(mode=0o600, exist_ok=False)
                    except OSError as error:
                        assert error.errno == errno.EDQUOT
                        break
                else:
                    raise AssertionError("inode quota was not enforced")
                quota = verify_trace_quota(trace, state)
                assert quota.allocated_inodes == quota.hard_inodes == 128
            print(json.dumps({"kernel_edquot": True, **snapshot()}), flush=True)
        elif operation in ("before_commit", "after_commit"):
            if operation == "after_commit":
                restore = physical.restore

                def pause_refill():
                    if physical.borrowed:
                        assert not db.in_transaction
                        print(
                            json.dumps({"barrier": "committed_before_refill"}),
                            flush=True,
                        )
                        assert sys.stdin.readline().strip() == "resume"
                    restore()

                physical.restore = pause_refill
            if operation == "before_commit":
                commit = db.commit

                def pause_commit():
                    if physical.borrowed:
                        print(
                            json.dumps(
                                {
                                    "barrier": "before_commit",
                                    "journal_allocated": (
                                        trace / "agentd.sqlite3-journal"
                                    )
                                    .stat()
                                    .st_blocks
                                    * 512,
                                    **snapshot(),
                                }
                            ),
                            flush=True,
                        )
                        assert sys.stdin.readline().strip() == "resume"
                    commit()

                db.commit = pause_commit
            recover()
        else:
            result = snapshot()
            complete = operation == "verify_complete"
            expected = task_count * (4 if complete else 3) + (4 if complete else 2)
            assert result["filled"] == task_count * (3 if complete else 2) + (
                (4 if mode == "revoke" else 3) if complete else 0
            )
            assert result["events"] == expected
            assert result["legacy_events"] == task_count * (
                4 if complete else 3
            ) + 1 + int(complete and mode == "revoke")
            assert result["indexed_events"] == task_count + 2
            assert result["indexed_legacy_events"] == task_count + 1
            assert result["task_states"] == {
                "queued" if complete else "accepted": task_count
            }
            assert result["claimed"] == (0 if complete else task_count)
            assert result["attempts"] == task_count * 2
            assert result["outbox"] == 0
            assert result["connector_revoked"] == int(complete and mode == "revoke")
            assert result["closed_sessions"] == int(complete)
            assert result["run_closed"] == int(complete)
            assert result["operation_phase"] == (
                "interrupted" if complete else "started"
            )
            assert result["presence"] == [[1, "online", "native_session_opened"]] + (
                [[2, "unavailable", "session_lease_expired"]]
                if complete and mode == "reconcile"
                else []
            )
            assert (
                result["next_source_seq"] == result["next_export_seq"] == expected + 1
            )
            assert result["workspace_bytes"] >= 32 * 1024 * 1024
            if complete:
                scope = ExportScope(
                    *tuple(
                        db.execute(
                            "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
                        ).fetchone()
                    )
                )
                wanted = db.execute(
                    "SELECT event_json FROM trace_journal_all ORDER BY source_seq"
                ).fetchall()
                after = 0
                observed = []
                while after < expected:
                    batch = selected_batch(store, scope, after=after)
                    assert batch
                    for record in batch:
                        decoded = json.loads(record.payload)["event"]
                        assert canonical_bytes(decoded) == canonical_bytes(
                            json.loads(wanted[after][0])
                        )
                        if decoded["kind"] == "task":
                            observed.append((decoded["task_id"], decoded["phase"]))
                        assert decoded["source_seq"] == after + 1
                        assert record.export_seq == after + 1
                        after += 1
                assert selected_batch(store, scope, after=after) == []
                assert Counter(observed) == Counter(
                    (task_id, phase)
                    for task_id in task_ids
                    for phase in ("queued", "offered", "accepted", "queued")
                )
                if mode == "revoke":
                    try:
                        recover()
                    except StoreError as error:
                        assert "already revoked" in str(error)
                    else:
                        raise AssertionError("revoked connector was revoked twice")
                else:
                    recover()
                assert snapshot() == result
                result["empty_maintenance_completed"] = mode == "reconcile"
                result["exact_exported_events"] = after
            for schema in ("main", "task_state"):
                assert (
                    db.execute(f"PRAGMA {schema}.integrity_check").fetchone()[0] == "ok"
                )
            print(json.dumps({"integrity": "ok", **result}), flush=True)
    finally:
        if store is not None:
            installed = store._connection.workspace is physical
            store.close()
            if not installed:
                physical.close()
        else:
            physical.close()


def main(output=None, mode="reconcile", inode_pressure=False):
    assert mode in {"reconcile", "revoke"}
    from test_trace_linux_quota import owned_volume

    report = {
        "inode_pressure": inode_pressure,
        "scope": f"Production {mode} recovery at Linux user quota: accepted-task requeue, run/operation closure and local presence/audit in one paired commit. Fixture-installed workspace; no live deployment.",
    }
    fixture = Path(__file__).resolve().parents[1] / "fixtures/traces/events.v1.json"
    with owned_volume() as (root, state, _, _):
        script = root / "session-recovery-probe.py"
        shutil.copyfile(__file__, script)
        children = []

        def start(operation):
            child = subprocess.Popen(
                [
                    sys.executable,
                    str(script),
                    "--worker",
                    str(state),
                    operation,
                    str(fixture),
                    mode,
                    str(int(inode_pressure)),
                ],
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
            children.append(child)
            return child

        def run(operation):
            child = start(operation)
            out, error = child.communicate(timeout=120)
            assert child.returncode == 0, error
            return json.loads(out)

        try:
            report["pressure"] = run("seed")
            for operation, verify in (
                ("before_commit", "verify_rollback"),
                ("after_commit", "verify_complete"),
            ):
                child = start(operation)
                assert select.select([child.stdout], [], [], 120)[0], (
                    "boundary not reached"
                )
                line = child.stdout.readline()
                if not line:
                    _, error = child.communicate(timeout=10)
                    raise AssertionError(error)
                report[operation] = json.loads(line)
                child.kill()
                child.communicate(timeout=10)
                assert child.returncode == -9
                report[verify] = run(verify)
                if inode_pressure:
                    assert report[verify]["allocated_inodes"] == 128
                    assert report[verify]["inode_reserve_ready"]
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=10)
    assert not root.exists()
    report["scratch_removed"] = True
    source = Path(__file__).resolve().parents[2] / "src/edgecitadel_agentd"
    report["runtime_sha256"] = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(source.glob("*.py"))
    }
    report["test_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report["fixture_sha256"] = hashlib.sha256(fixture.read_bytes()).hexdigest()
    value = json.dumps(report, indent=2) + "\n"
    if output:
        Path(output).write_text(value)
    print(value)


def test_native_session_recovery():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main()


def test_native_connector_recovery():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main(mode="revoke")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(
            Path(sys.argv[2]),
            sys.argv[3],
            Path(sys.argv[4]),
            sys.argv[5],
            bool(int(sys.argv[6])),
        )
    else:
        main(
            sys.argv[1] if len(sys.argv) > 1 else None,
            sys.argv[2] if len(sys.argv) > 2 else "reconcile",
            inode_pressure=len(sys.argv) > 3 and sys.argv[3] == "inodes",
        )
