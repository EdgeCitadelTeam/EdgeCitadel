"""Explicit real-NATS proof for the agentd connector transport."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest
from nats.aio.client import Client as NATS

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import serve, socket_path_for
from edgecitadel_plugin_runtime.jetstream import ensure_consumer, ensure_stream


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1",
    reason="set RUN_AGENTD_NATS_INTEGRATION=1 to run owned NATS integration",
)


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def _wait_for(predicate, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.05)
    pytest.fail("condition did not become ready")


@pytest.mark.asyncio
@pytest.mark.parametrize("node_mode", ["edge", "core", "legacy-core"])
async def test_connector_round_trip_through_owned_real_nats(
    tmp_path: Path, node_mode: str
) -> None:
    executable = shutil.which("nats-server")
    if executable is None:
        pytest.skip("nats-server is not installed")
    port = _unused_port()
    monitor_port = _unused_port()
    token = secrets.token_urlsafe(32)
    server_dir = tmp_path / "nats"
    server_dir.mkdir()
    config = server_dir / "nats.conf"
    config.write_text(
        f"port: {port}\nhttp_port: {monitor_port}\nauthorization {{ token: {json.dumps(token)} }}\n"
        f"jetstream {{ store_dir: {server_dir / 'js'} }}\n"
    )
    process = subprocess.Popen(
        [executable, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    node = {
        "agent_id": "owned-edge" if node_mode == "edge" else "core",
        "version": 2 if node_mode == "edge" else 1,
        "mode": "edge" if node_mode == "edge" else "core",
        "messaging_mode": "single-client",
        "nats_url": f"nats://127.0.0.1:{port}",
        "nats_token": token,
    }
    if node_mode != "legacy-core":
        node.update(plugin_nats_url=node["nats_url"], plugin_nats_token=token)
    (state_dir / "node.json").write_text(json.dumps(node))
    stop = threading.Event()
    service_thread = threading.Thread(
        target=serve, args=(state_dir / "agentd", stop), daemon=True
    )
    service_thread.start()
    socket_path = socket_path_for(state_dir / "agentd")
    nc = NATS()
    try:

        async def nats_ready() -> bool:
            if process.poll() is not None:
                pytest.fail("owned nats-server exited during startup")
            try:
                await asyncio.wait_for(
                    nc.connect(
                        servers=[f"nats://127.0.0.1:{port}"],
                        token=token,
                        connect_timeout=0.2,
                        allow_reconnect=False,
                        max_reconnect_attempts=0,
                    ),
                    timeout=1,
                )
            except Exception:  # noqa: BLE001
                return False
            return True

        await _wait_for(nats_ready)
        for _ in range(200):
            if socket_path.exists():
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("agentd socket did not become ready")

        anonymous = AgentdClient(socket_path)
        admin = AgentdClient(
            socket_path,
            admin_token=(state_dir / "agentd/admin.token").read_text().strip(),
        )
        registration = admin.call(
            "connector.register",
            connector_id="pi-local",
            host_type="pi",
            agent_id="edge-one-pi",
            capabilities=[
                "edgecitadel_delegate",
                "edgecitadel_inbox",
                "edgecitadel_task_status",
            ],
        )
        client = AgentdClient(
            socket_path,
            connector_id="pi-local",
            token=str(registration["token"]),
        )
        client.call("session.open")

        async def transport_ready() -> bool:
            health = anonymous.call("health")
            return bool(
                health["transport"]["connected"]
                and health["transport"].get("ready_inbox_count") == 1
            )

        await _wait_for(transport_ready)
        js = nc.jetstream()
        await ensure_stream(js, "remote-agent")
        await ensure_consumer(js, "remote-agent")
        remote = await js.pull_subscribe(
            "agents.remote-agent.inbox", durable="remote-agent_inbox"
        )
        task = client.call(
            "task.create",
            recipient_id="remote-agent",
            payload={"request": "perform the remote task"},
        )
        deliveries = await remote.fetch(batch=1, timeout=10)
        inbound = json.loads(deliveries[0].data)
        assert inbound["task_id"] == task["task_id"]
        await deliveries[0].ack()

        result = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": "result",
            "sender_id": "remote-agent",
            "recipient_id": "edge-one-pi",
            "task_id": task["task_id"],
            "task_state": "completed",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "payload": {},
        }
        await js.publish(
            "agents.edge-one-pi.inbox",
            json.dumps(result).encode(),
            headers={"Nats-Msg-Id": result["id"]},
        )

        async def completed() -> bool:
            return client.call("task.get", task_id=task["task_id"])["state"] == (
                "completed"
            )

        await _wait_for(completed)
        assert len(client.call("task.list")) == 1

        # Same-host connectors deliberately route through agentd's local store.
        # Keep this proof separate from the authenticated NATS round trip above.
        registration_two = admin.call(
            "connector.register",
            connector_id="codex-local",
            host_type="codex",
            agent_id="core-local-codex",
            capabilities=["edgecitadel_delegate", "edgecitadel_inbox"],
        )
        client_two = AgentdClient(
            socket_path,
            connector_id="codex-local",
            token=str(registration_two["token"]),
        )
        client_two.call("session.open")

        async def two_connectors_ready() -> bool:
            return anonymous.call("health")["transport"].get("ready_inbox_count") == 2

        await _wait_for(two_connectors_ready)
        local_request = client.call(
            "task.create",
            recipient_id="core-local-codex",
            payload={"request": "test-owned local round trip"},
        )
        received = client_two.call("task.list", recipient_id="core-local-codex")
        assert [task["task_id"] for task in received] == [local_request["task_id"]]
        local_reply = client_two.call(
            "task.create",
            recipient_id="edge-one-pi",
            payload={"reply_to": local_request["task_id"]},
        )
        received_reply = client.call("task.get", task_id=local_reply["task_id"])
        assert received_reply["sender_id"] == "core-local-codex"
        assert received_reply["payload"]["reply_to"] == local_request["task_id"]
    finally:
        stop.set()
        service_thread.join(timeout=10)
        if not nc.is_closed:
            await nc.close()
        process.terminate()
        process.wait(timeout=5)
    assert not service_thread.is_alive()


@pytest.mark.asyncio
async def test_trace_storage_failure_retains_result_for_broker_redelivery(
    tmp_path, monkeypatch
):
    from test_trace_crash import snapshot

    from edgecitadel_agentd import trace_capacity
    from edgecitadel_agentd.store import AgentdStore, StoreError
    from edgecitadel_agentd.transport import AgentdNatsTransport

    executable = shutil.which("nats-server")
    assert executable is not None, "explicit NATS qualification requires nats-server"
    port = _unused_port()
    token = secrets.token_urlsafe(32)
    config = tmp_path / "owned-nats.conf"
    config.write_text(
        f'host: "127.0.0.1"\nport: {port}\nauthorization {{ token: {json.dumps(token)} }}\njetstream {{ store_dir: {json.dumps(str(tmp_path / "js"))} }}\n'
    )
    config.chmod(0o600)
    process = await asyncio.to_thread(
        subprocess.Popen,
        [executable, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    nc = NATS()
    store = None
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                assert process.poll() is None and time.monotonic() < deadline
                await asyncio.sleep(0.01)
        await nc.connect(
            servers=[f"nats://127.0.0.1:{port}"], token=token, allow_reconnect=False
        )
        js = nc.jetstream()
        await ensure_stream(js, "worker")
        # Shorten only this owned fixture's retry wait; production remains 300s.
        await ensure_consumer(js, "worker", ack_wait_sec=1)
        subscription = await js.pull_subscribe(
            "agents.worker.inbox", durable="worker_inbox"
        )
        state = tmp_path / "state"
        state.mkdir()
        (state / "node.json").write_text('{"agent_id":"owned-edge"}')
        store = AgentdStore(state / "agentd" / "agentd.sqlite3")
        transport = AgentdNatsTransport(state, store)
        task = store.create_task(
            sender_id="worker", recipient_id="remote", payload={}, queue_transport=False
        )
        envelope = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": "result",
            "task_id": task["task_id"],
            "sender_id": "remote",
            "recipient_id": "worker",
            "task_state": "completed",
            "timestamp": "2026-09-16T12:00:00.000Z",
            "payload": {"body": "done"},
        }
        await js.publish(
            "agents.worker.inbox",
            json.dumps(envelope).encode(),
            headers={"Nats-Msg-Id": envelope["id"]},
        )
        (first,) = await subscription.fetch(1, timeout=3)
        assert first.metadata.num_delivered == 1
        before = snapshot(store)
        with monkeypatch.context() as pressure:
            pressure.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 0)
            pressure.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
            with pytest.raises(StoreError, match="quota_exceeded"):
                await transport._ingest_message(first, "worker")
        await nc.flush()
        assert snapshot(store) == before
        assert (
            await js.consumer_info("AGENT_INBOX", "worker_inbox")
        ).num_ack_pending == 1
        assert (await js.stream_info("AGENT_INBOX")).state.messages == 1
        (redelivered,) = await subscription.fetch(1, timeout=3)
        assert redelivered.metadata.num_delivered == 2
        assert redelivered.metadata.sequence.stream == first.metadata.sequence.stream
        assert redelivered.data == first.data
        await transport._ingest_message(redelivered, "worker")
        await nc.flush()
        assert store.get_task(task["task_id"])["state"] == "completed"
        assert store.get_task(task["task_id"])["result"] == {"body": "done"}
        assert (
            await js.consumer_info("AGENT_INBOX", "worker_inbox")
        ).num_ack_pending == 0
        assert (await js.stream_info("AGENT_INBOX")).state.messages == 0
        phases = [
            json.loads(row[0])["phase"]
            for row in store._connection.execute(
                "SELECT event_json FROM trace_journal ORDER BY source_seq"
            )
        ]
        assert phases == ["queued", "offered", "accepted", "running", "completed"]
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        if store is not None:
            store.close()
        if not nc.is_closed:
            await nc.close()
        process.terminate()
        await asyncio.to_thread(process.wait, timeout=5)
