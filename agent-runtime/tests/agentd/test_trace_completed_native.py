"""Owned jim-eq quota/crash qualification of production completed-event reads.

Reservations are explicitly installed by this fixture. Production lifecycle
admission and binding/task terminal-state reservation are not exercised here.
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
from uuid import UUID


def worker(state, operation, fixture):
    os.umask(0o077)
    state.chmod(0o700)
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.store import AgentdStore
    from edgecitadel_agentd.storage_workspace import CompletionWorkspace
    from edgecitadel_agentd.trace_contract import canonical_bytes
    from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
    from edgecitadel_agentd.trace_journal import TraceJournal
    from edgecitadel_agentd.trace_quota import verify_trace_quota
    from edgecitadel_agentd.trace_reservations import MAX_SLOTS, Obligation, reserve

    trace = (state / "trace").resolve(strict=True)
    physical = CompletionWorkspace(trace / "completion.reserve")
    store = None
    try:
        store = AgentdStore(
            trace / "agentd.sqlite3",
            task_path=state / "tasks.sqlite3",
            payload_key_path=state / "payload.key",
        )
        db = store._connection
        db.install_workspace(physical)
        base = json.loads(fixture.read_text())["fixtures"][0]["event"]
        obligations = [
            Obligation("task", f"owned-{i}", "terminal") for i in range(MAX_SLOTS)
        ]

        def event(i):
            return {
                **base,
                "event_id": str(
                    UUID(
                        bytes=hashlib.sha256(
                            f"owned-native-completion-{i}".encode()
                        ).digest()[:16],
                        version=4,
                    )
                ),
            }

        def snapshot():
            return {
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
            with db:
                db.execute("BEGIN IMMEDIATE")
                TraceJournal(db).initialize("edge-a")
                db.execute(
                    "CREATE TABLE owned_pressure(id INTEGER PRIMARY KEY, payload BLOB)"
                )
                for obligation in obligations:
                    reserve(db, obligation)
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
            with db:
                db.execute("BEGIN IMMEDIATE")
                pages = db.execute("PRAGMA page_count").fetchone()[0]
                for i, obligation in enumerate(obligations):
                    TraceJournal(db).record(
                        "edge-a", event(i), selected=True, completion=obligation
                    )
                assert db.execute("PRAGMA page_count").fetchone()[0] == pages
                if operation == "before_commit":
                    journal = trace / "agentd.sqlite3-journal"
                    print(
                        json.dumps(
                            {
                                "barrier": "before_commit",
                                "journal_allocated": journal.stat().st_blocks * 512,
                                **snapshot(),
                            }
                        ),
                        flush=True,
                    )
                    assert sys.stdin.readline().strip() == "resume"
        else:
            result = snapshot()
            expected = MAX_SLOTS if operation == "verify_complete" else 0
            assert result["filled"] == result["events"] == expected
            assert result["indexed_events"] == 0
            assert (
                result["next_source_seq"] == result["next_export_seq"] == expected + 1
            )
            assert result["workspace_bytes"] >= 32 * 1024 * 1024
            if expected:
                scope = ExportScope(
                    *tuple(
                        db.execute(
                            "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
                        ).fetchone()
                    )
                )
                after = 0
                while after < expected:
                    batch = selected_batch(store, scope, after=after)
                    assert batch
                    for record in batch:
                        decoded = json.loads(record.payload)["event"]
                        wanted = {
                            **event(after),
                            "node_id": scope.node_id,
                            "source_epoch": scope.source_epoch,
                            "source_seq": after + 1,
                        }
                        assert canonical_bytes(decoded) == canonical_bytes(wanted)
                        assert record.export_seq == after + 1
                        after += 1
                assert selected_batch(store, scope, after=after) == []
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    for i in (0, MAX_SLOTS - 1):
                        repeated = TraceJournal(db).record(
                            "edge-a", event(i), selected=True, completion=obligations[i]
                        )
                        assert repeated["source_seq"] == i + 1
                assert snapshot() == result
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


def main(output=None):
    from test_trace_linux_quota import owned_volume

    report = {
        "scope": "Production completed-event writer/read/export with fixture-installed reservations; no lifecycle admission or live deployment."
    }
    fixture = Path(__file__).resolve().parents[1] / "fixtures/traces/events.v1.json"
    with owned_volume() as (root, state, _, _):
        script = root / "completed-probe.py"
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


def test_native_completed_events():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else None)
