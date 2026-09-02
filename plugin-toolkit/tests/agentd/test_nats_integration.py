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
async def test_connector_round_trip_through_owned_real_nats(tmp_path: Path) -> None:
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
        f"port: {port}\nhttp_port: {monitor_port}\nauthorization {{ token: {token} }}\n"
        f"jetstream {{ store_dir: {server_dir / 'js'} }}\n"
    )
    process = subprocess.Popen(
        [executable, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "node.json").write_text(
        json.dumps(
            {
                "version": 2,
                "mode": "edge",
                "messaging_mode": "single-client",
                "plugin_nats_url": f"nats://127.0.0.1:{port}",
                "plugin_nats_token": token,
            }
        )
    )
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
                await nc.connect(
                    servers=[f"nats://127.0.0.1:{port}"],
                    token=token,
                    connect_timeout=0.2,
                    allow_reconnect=False,
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
    finally:
        stop.set()
        service_thread.join(timeout=10)
        if not nc.is_closed:
            await nc.close()
        process.terminate()
        process.wait(timeout=5)
    assert not service_thread.is_alive()
