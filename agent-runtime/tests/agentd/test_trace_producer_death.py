"""Producer SIGKILL after an effect with loss counters still only in RAM."""

import asyncio
import json
import os
import select
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from test_trace_crash import append_request, prepare, snapshot

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.trace_history import read_history


@pytest.mark.parametrize("pressure", ["physical", "logical"])
def test_pressure_does_not_block_daemon_interrupted_operation_evidence(
    tmp_path, monkeypatch, pressure
):
    store, token, session, binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    try:
        store.append_trace(
            node_id="owned-edge",
            connector_id="owner",
            token=token,
            params=append_request(binding),
        )
        monkeypatch.setattr(
            trace_capacity,
            "PHYSICAL_PRESSURE_BYTES"
            if pressure == "physical"
            else "NORMAL_LIMIT_BYTES",
            1,
        )
        from edgecitadel_agentd.trace_contract import TraceContractError

        before = snapshot(store)
        with pytest.raises(TraceContractError, match="quota_exceeded"):
            store.append_trace(
                node_id="owned-edge",
                connector_id="owner",
                token=token,
                params=append_request(binding),
            )
        assert snapshot(store) == before
        store.reconcile(now_ms=session["lease_expires_at_ms"] + 1)
        operation = store._connection.execute(
            "SELECT phase,terminal_event_id FROM trace_operations"
        ).fetchone()
        assert operation[0] == "interrupted" and operation[1]
        root = store._connection.execute(
            "SELECT closed_at_ms FROM trace_bindings"
        ).fetchone()
        assert root[0] is not None
    finally:
        store.close()


@pytest.mark.parametrize("managed", [False, True])
def test_killed_producer_keeps_unknown_loss_and_never_repeats_effect(tmp_path, managed):
    store, token, session, binding = prepare(
        tmp_path / "state/agentd/agentd.sqlite3", managed=managed
    )
    effect = tmp_path / "effect.txt"
    child = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            binding["binding_id"],
            str(effect),
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
        assert select.select([child.stdout], [], [], 15)[0], (
            "producer did not reach fault point"
        )
        acknowledgement = json.loads(child.stdout.readline())
        assert acknowledgement == {"effect_fsynced": True, "volatile_drops": 2}
        assert effect.read_text() == "once\n"
        child.kill()
        assert child.wait(timeout=10) == -signal.SIGKILL
        store.reconcile(now_ms=session["lease_expires_at_ms"] + 1)
        history = read_history(
            store,
            connector_id="owner",
            token=token,
            params={"trace_id": binding["trace_id"]},
        )
        assert history["coverage"]["producer_loss"] == "unknown"
        assert history["coverage"]["partial"] is True
        assert not any(
            "dropped_observations" in e["attributes"] for e in history["events"]
        )
        assert any(
            e["kind"] == "run" and e["phase"] == "interrupted"
            for e in history["events"]
        )
        assert not any(e["kind"] == "tool" for e in history["events"])
        if managed:
            task = store._connection.execute("SELECT state FROM tasks").fetchone()
            assert task[0] == "failed"
            fresh = store.open_session(connector_id="owner", token=token)
            assert (
                store.claim_next_task(
                    connector_id="owner", token=token, session_id=fresh["session_id"]
                )
                is None
            )
        assert effect.read_text() == "once\n"
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        child.stdout.close()
        child.stderr.close()
        store.close()


def test_daemon_survives_exhausted_closure_reserve_and_recovers(tmp_path, monkeypatch):
    from edgecitadel_agentd import service
    from edgecitadel_agentd.client import AgentdClient, AgentdClientError

    store, token, _session, binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    store.append_trace(
        node_id="owned-edge",
        connector_id="owner",
        token=token,
        params=append_request(binding),
    )
    with store._connection:
        store._connection.execute("UPDATE sessions SET lease_expires_at_ms=1")
    before = snapshot(store)
    normal = trace_capacity.NORMAL_LIMIT_BYTES
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 0)
    monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
    errors = []
    monkeypatch.setattr(
        threading, "excepthook", lambda args: errors.append(args.exc_value)
    )
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve,
        args=(store.path.parent, stop),
        kwargs={"open_store": lambda: store},
        daemon=True,
    )
    thread.start()
    client = AgentdClient(service.socket_path_for(store.path.parent), timeout=0.2)

    def wait_health(degraded):
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            assert not errors, errors
            try:
                health = client.call("health")
                if (health.get("reconciliation") == "storage_unavailable") == degraded:
                    return health
            except AgentdClientError:
                pass
            time.sleep(0.02)
        raise AssertionError("reconciliation did not reach expected health")

    try:
        assert wait_health(True)["status"] == "degraded"
        assert snapshot(store) == before
        monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", normal)
        assert wait_health(False)["status"] == "ready"
        assert (
            store._connection.execute("SELECT phase FROM trace_operations").fetchone()[
                0
            ]
            == "interrupted"
        )
    finally:
        stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()


async def producer(binding_id, effect):
    from edgecitadel_agentd.client import AgentdClient
    from edgecitadel_agentd.trace_producer import RuntimeTrace

    # A real unavailable private-socket endpoint, not a fabricated success reply.
    trace = RuntimeTrace(AgentdClient(effect.parent / "unavailable.sock", timeout=0.1))
    trace.binding_id = binding_id
    async with trace.operation("tool", "owned-effect"):
        with effect.open("x") as output:
            output.write("once\n")
            output.flush()
            os.fsync(output.fileno())
    print(
        json.dumps(
            {"effect_fsynced": True, "volatile_drops": trace.dropped_observations}
        ),
        flush=True,
    )
    signal.pause()


if __name__ == "__main__":
    asyncio.run(producer(sys.argv[1], Path(sys.argv[2])))
