"""Owned jim-eq actual bind/append/finish quota and crash qualification.

The fixture installs the workspace; production startup and task/recovery
admission are deliberately outside this gate's scope.
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
from uuid import uuid4


def worker(state, operation, fixture):
    os.umask(0o077)
    state.chmod(0o700)
    assert ctypes.CDLL(None).prctl(38, 1, 0, 0, 0) == 0
    from edgecitadel_agentd.store import AgentdStore
    from edgecitadel_agentd.storage_workspace import CompletionWorkspace
    from edgecitadel_agentd.trace_contract import TraceContractError, canonical_bytes
    from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
    from edgecitadel_agentd.trace_quota import verify_trace_quota
    from edgecitadel_agentd.trace_reservations import MAX_SLOTS

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
        control = {} if operation == "seed" else json.loads(sys.stdin.readline())
        template = next(
            item["event"]
            for item in json.loads(fixture.read_text())["fixtures"]
            if item["name"] == "tool"
        )

        def request(span=None, phase="started"):
            observation = {
                key: template[key]
                for key in (
                    "schema_version",
                    "kind",
                    "phase",
                    "span_id",
                    "parent_span_id",
                    "occurred_at",
                    "duration_ms",
                    "attributes",
                )
            }
            observation.update(
                span_id=span or str(uuid4()), parent_span_id=None, phase=phase
            )
            return {
                "schema_version": 1,
                "binding_id": control["binding_id"],
                "observation_id": str(uuid4()),
                "observation": observation,
            }

        def call(method, params):
            return getattr(store, method)(
                node_id="edge-a",
                connector_id="native",
                token=control["token"],
                params=params,
            )

        def binding_request():
            return {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": control["session"],
                "task_id": None,
                "context_id": None,
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
                "closed": db.execute(
                    "SELECT closed_at_ms IS NOT NULL FROM trace_bindings_all"
                ).fetchone()[0],
                "indexed_closed": db.execute(
                    "SELECT closed_at_ms IS NOT NULL FROM trace_bindings"
                ).fetchone()[0],
                "phases": dict(
                    db.execute(
                        "SELECT phase,count(*) FROM trace_operations_all GROUP BY phase"
                    )
                ),
                "indexed_phases": dict(
                    db.execute(
                        "SELECT phase,count(*) FROM trace_operations GROUP BY phase"
                    )
                ),
                "workspace_bytes": os.fstat(physical.descriptor).st_blocks * 512,
                "quota_bytes": verify_trace_quota(trace, state).allocated_bytes,
            }

        if operation == "seed":
            control["token"] = store.register_connector(
                connector_id="native",
                host_type="codex",
                agent_id="agent-a",
                capabilities=["edgecitadel_trace"],
            )
            control["session"] = store.open_session(
                connector_id="native", token=control["token"], lease_seconds=300
            )["session_id"]
            control["binding_id"] = call("bind_trace", binding_request())["result"][
                "binding_id"
            ]
            first = request()
            call("append_trace", first)
            control["append"] = request(first["observation"]["span_id"], "finished")
            control["finish"] = {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": control["binding_id"],
                "outcome": "unknown",
                "reason": "unknown",
            }
            for _ in range(MAX_SLOTS - 2):
                call("append_trace", request())
            before = snapshot()
            for method, params in (
                ("bind_trace", binding_request()),
                ("append_trace", request()),
            ):
                try:
                    call(method, params)
                except TraceContractError as error:
                    assert "quota_exceeded" in str(error)
                else:
                    raise AssertionError("admission exceeded reserved capacity")
                assert snapshot() == before
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
                raise AssertionError("kernel quota not reached")
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
            pages = snapshot()["pages"]
            reply = call("append_trace", control["append"])
            assert call("append_trace", control["append"]) == reply
            result = snapshot()
            assert result["pages"] == pages and result["filled"] == 1
            assert result["phases"] == {"finished": 1, "started": MAX_SLOTS - 2}
            print(
                json.dumps(
                    {
                        "control": control,
                        "kernel_edquot": True,
                        "admission_refused": True,
                        **result,
                    }
                ),
                flush=True,
            )
        elif operation in ("before_commit", "after_commit"):
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
            else:
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
            call("finish_trace", control["finish"])
        else:
            result = snapshot()
            complete = operation == "verify_complete"
            expected = MAX_SLOTS if complete else 1
            assert result["filled"] == expected
            assert result["events"] == MAX_SLOTS + expected
            assert result["indexed_events"] == MAX_SLOTS
            assert (
                result["next_source_seq"]
                == result["next_export_seq"]
                == MAX_SLOTS + expected + 1
            )
            assert result["closed"] == int(complete) and result["indexed_closed"] == 0
            assert result["indexed_phases"] == {"started": MAX_SLOTS - 1}
            assert result["phases"] == {
                "finished": 1,
                "interrupted" if complete else "started": MAX_SLOTS - 2,
            }
            assert result["workspace_bytes"] >= 32 * 1024 * 1024
            if complete:
                reply = call("finish_trace", control["finish"])
                assert call("finish_trace", control["finish"]) == reply
                assert snapshot() == result
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
                while after < result["events"]:
                    batch = selected_batch(store, scope, after=after)
                    assert batch
                    for record in batch:
                        event = json.loads(record.payload)["event"]
                        assert canonical_bytes(event) == canonical_bytes(
                            json.loads(wanted[after][0])
                        )
                        assert record.export_seq == event["source_seq"] == after + 1
                        after += 1
                assert selected_batch(store, scope, after=after) == []
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
        "scope": "Actual bind/append/finish and retry/export under Linux user quota with fixture-installed workspace; no task/recovery admission or live deployment."
    }
    fixture = Path(__file__).resolve().parents[1] / "fixtures/traces/events.v1.json"
    control = None
    with owned_volume() as (root, state, _, _):
        script = root / "terminal-probe.py"
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
            if control is not None:
                child.stdin.write(json.dumps(control) + "\n")
                child.stdin.flush()
            return child

        def run(operation):
            child = start(operation)
            out, error = child.communicate(timeout=120)
            assert child.returncode == 0, error
            return json.loads(out)

        try:
            report["pressure"] = run("seed")
            control = report["pressure"].pop("control")
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


def test_native_terminal_events():
    import pytest

    if os.environ.get("RUN_AGENTD_USER_QUOTA") != "1":
        pytest.skip("requires explicitly owned jim-eq quota fixture")
    main()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
    else:
        main(sys.argv[1] if len(sys.argv) > 1 else None)
