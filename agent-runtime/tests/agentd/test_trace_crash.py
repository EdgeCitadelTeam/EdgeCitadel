"""Real SIGKILL gates at local commit and external-effect boundaries."""

import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_retention import prune_active_history

TABLES = (
    "tasks",
    "transport_outbox",
    "trace_journal",
    "trace_spool",
    "trace_sources",
    "trace_export_generations",
    "trace_storage_usage",
    "trace_requests",
    "trace_operations",
    "trace_bindings",
)


def snapshot(store):
    return {
        name: [
            tuple(row)
            for row in store._connection.execute(f"SELECT * FROM {name} ORDER BY rowid")
        ]
        for name in TABLES
    }


def prepare(path, *, managed=False):
    path.parent.parent.mkdir(parents=True, exist_ok=True)
    (path.parent.parent / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(path)
    token = store.register_connector(
        connector_id="owner",
        host_type="managed-agent" if managed else "codex",
        agent_id="owner",
        capabilities=["edgecitadel_trace", "edgecitadel_delegate"],
    )
    session = store.open_session(connector_id="owner", token=token)
    task_id = None
    if managed:
        task = store.create_task(
            sender_id="initiator",
            recipient_id="owner",
            payload={},
            queue_transport=False,
        )
        task_id = task["task_id"]
        claimed = store.claim_next_task(
            connector_id="owner", token=token, session_id=session["session_id"]
        )
        assert claimed["task_id"] == task_id
        store.transition_task(
            task_id=task_id,
            state="running",
            actor_id="owner",
            session_id=session["session_id"],
            queue_transport=False,
        )
    binding = store.bind_trace(
        node_id="owned-edge",
        connector_id="owner",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session["session_id"],
            "task_id": task_id,
            "context_id": None,
        },
    )["result"]
    return store, token, session, binding


def append_request(binding):
    return {
        "schema_version": 1,
        "binding_id": binding["binding_id"],
        "observation_id": str(uuid4()),
        "observation": {
            "schema_version": 1,
            "kind": "tool",
            "phase": "started",
            "span_id": str(uuid4()),
            "parent_span_id": None,
            "occurred_at": "2026-09-16T12:00:00.000Z",
            "duration_ms": None,
            "attributes": {"name": "owned-effect"},
        },
    }


def execute(store, config):
    operation = config["operation"]
    if operation == "prune":
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            return prune_active_history(
                store._connection, node_id="owned-edge", now_ms=int(time.time() * 1000)
            )
    method = store.dispatch_trace if operation == "dispatch" else store.append_trace
    return method(
        node_id="owned-edge",
        connector_id="owner",
        token=config["token"],
        params=config["params"],
    )


def killed_at(path, config, point):
    config_path = path.parent / "owned-crash.json"
    config_path.write_text(json.dumps(config))
    config_path.chmod(0o600)
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(path),
            str(config_path),
            point,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            **os.environ,
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    try:
        assert select.select([process.stdout], [], [], 15)[0], (
            "child did not reach fault point"
        )
        line = process.stdout.readline()
        assert line == "ready\n", (
            process.stderr.read() if process.poll() is not None else line
        )
        process.kill()
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()
        config_path.unlink()


@pytest.mark.parametrize("operation", ["dispatch", "append", "prune"])
@pytest.mark.parametrize("point", ["mid_transaction", "before_commit", "after_commit"])
def test_sigkill_preserves_atomic_journal_and_retry_identity(
    tmp_path, operation, point
):
    path = tmp_path / "state/agentd/agentd.sqlite3"
    store, token, _session, binding = prepare(path)
    params = append_request(binding)
    if operation == "dispatch":
        params = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "binding_id": binding["binding_id"],
            "recipient_id": "owned-worker",
            "request": "owned work",
            "skill_id": None,
            "deadline_at_ms": None,
        }
    elif operation == "prune":
        store.append_trace(
            node_id="owned-edge", connector_id="owner", token=token, params=params
        )
    config = {"operation": operation, "token": token, "params": params}
    before = snapshot(store)
    store.close()
    killed_at(path, config, point)
    store = AgentdStore(path)
    try:
        after = snapshot(store)
        if point != "after_commit":
            assert after == before
        else:
            assert after != before
        if operation != "prune":
            first = execute(store, config)
            committed = snapshot(store)
            assert execute(store, config) == first
            assert snapshot(store) == committed
            if point == "after_commit":
                assert committed == after
            if operation == "dispatch":
                assert (
                    len(committed["tasks"]) == len(committed["transport_outbox"]) == 1
                )
        else:
            if point != "after_commit":
                assert execute(store, config) == 1
            assert execute(store, config) == 0
            markers = [
                json.loads(r[0])
                for r in store._connection.execute(
                    "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
                )
            ]
            assert len(markers) == 1
            assert markers[0]["attributes"]["lost_ranges"] == [{"first": 2, "last": 2}]
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


def test_kill_after_external_effect_recovers_interrupted_without_reclaim(tmp_path):
    path = tmp_path / "state/agentd/agentd.sqlite3"
    store, token, session, binding = prepare(path, managed=True)
    effect_path = tmp_path / "owned-effects"
    config = {
        "operation": "effect",
        "token": token,
        "params": append_request(binding),
        "effect_path": str(effect_path),
    }
    store.close()
    killed_at(path, config, "after_effect")
    store = AgentdStore(path)
    try:
        store.reconcile(now_ms=session["lease_expires_at_ms"] + 1)
        assert store.get_task(binding["task_id"])["state"] == "failed"
        renewed = store.open_session(connector_id="owner", token=token)
        assert (
            store.claim_next_task(
                connector_id="owner", token=token, session_id=renewed["session_id"]
            )
            is None
        )
        assert effect_path.read_text() == "once\n"
        operations = store._connection.execute(
            "SELECT phase FROM trace_operations"
        ).fetchall()
        assert [r[0] for r in operations] == ["interrupted"]
    finally:
        store.close()


if __name__ == "__main__":
    store = AgentdStore(Path(sys.argv[1]))
    config = json.loads(Path(sys.argv[2]).read_text())
    point = sys.argv[3]

    def ready():
        print("ready", flush=True)
        signal.pause()
        raise RuntimeError("fault point resumed unexpectedly")

    if point == "before_commit":
        store._connection.set_trace_callback(
            lambda statement: ready() if statement.strip().upper() == "COMMIT" else None
        )
    elif point == "mid_transaction":
        boundary = {
            "dispatch": "INSERT INTO TRACE_TASK_CONTEXTS",
            "append": "INSERT INTO TRACE_SPOOL",
            "prune": "DELETE FROM TRACE_JOURNAL",
        }[config["operation"]]
        store._connection.set_trace_callback(
            lambda statement: (
                ready() if statement.strip().upper().startswith(boundary) else None
            )
        )
    execute(store, config)
    if point == "after_effect":
        with Path(config["effect_path"]).open("a") as output:
            output.write("once\n")
            output.flush()
            os.fsync(output.fileno())
    ready()
