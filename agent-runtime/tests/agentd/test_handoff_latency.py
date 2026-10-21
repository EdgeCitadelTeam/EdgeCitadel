"""Hand-offs wake the next stage instead of waiting for a poll.

An idle Managed Agent slot waits in agentd (``task.claim`` with ``wait_ms``)
and is woken when a task for it is created; the outbound transport publishes
an enqueued message when it is woken. The fallback re-check intervals are set
far above the budget here, so only the wake-ups can meet it.

agentd's state lives on tmpfs when there is one: a hand-off is several SQLite
commits (rollback journal, ``synchronous=EXTRA``), and on a slow disk their
syncs alone exceed the budget (about 24 ms per commit on the development VM),
which says nothing about waiting. Without tmpfs the budget is the one a poll
could never meet (half the 1 s claim wait).
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.store import AgentdStore
from managed_runtime_support import AGENT, eventually, managed_agent
from service_test_support import serve

HANDOFFS = 5
TMPFS = Path("/dev/shm")


@pytest.fixture
def state_root(tmp_path):
    """Where agentd keeps its state, and the hand-off budget there."""
    if TMPFS.is_dir() and os.access(TMPFS, os.W_OK):
        root = Path(tempfile.mkdtemp(prefix="agentd-handoff-", dir=TMPFS))
        try:
            yield root, 0.1
        finally:
            shutil.rmtree(root, ignore_errors=True)
    else:
        yield tmp_path, 0.5


def operator(admin: AgentdClient, socket_path: Path, outbound: str) -> AgentdClient:
    """A native connector that may create tasks for ``outbound``."""
    registration = cast(
        Mapping[str, object],
        admin.call(
            "connector.register",
            connector_id="operator-local",
            host_type="pi",
            agent_id="operator",
            capabilities=["edgecitadel_delegate", "edgecitadel_task_status"],
        ),
    )
    admin.call(
        "connector.grant", connector_id="operator-local", outbound_agents=[outbound]
    )
    client = AgentdClient(
        socket_path, connector_id="operator-local", token=str(registration["token"])
    )
    client.call("session.open")
    return client


@pytest.mark.asyncio
async def test_an_idle_slot_starts_a_created_task_well_within_100_ms(
    state_root, monkeypatch
):
    root, budget = state_root
    monkeypatch.setenv("EDGECITADEL_CLAIM_POLL_MS", "5000")
    started: dict[str, float] = {}

    async def handler(envelope: dict[str, Any], context: Any):
        started[envelope["payload"]["body"]] = time.monotonic()
        return {"body": "done"}, "completed"

    async with managed_agent(root, monkeypatch, handler) as agent:
        client = operator(
            agent.admin, socket_path_for(agent.service_dir), outbound=AGENT
        )
        latencies = []
        for i in range(HANDOFFS):
            await asyncio.sleep(0.3)  # the slot is idle, waiting in agentd
            sent = time.monotonic()
            await asyncio.to_thread(
                client.call, "task.create", recipient_id=AGENT, payload={"body": str(i)}
            )
            await eventually(lambda i=i: str(i) in started, timeout=5)
            latencies.append(started[str(i)] - sent)
    assert statistics.median(latencies) < budget, latencies


@pytest.mark.asyncio
async def test_a_waiting_claim_still_finds_a_task_written_without_a_signal(
    tmp_path, monkeypatch
):
    """A task another connection wrote (no wake-up in this process) is found
    by the fallback re-check."""
    monkeypatch.setenv("EDGECITADEL_CLAIM_POLL_MS", "50")
    started: list[float] = []

    async def handler(envelope: dict[str, Any], context: Any):
        started.append(time.monotonic())
        return {"body": "done"}, "completed"

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        await asyncio.sleep(0.3)
        sent = time.monotonic()
        agent.submit()  # through a second store connection
        await eventually(lambda: started, timeout=5)
    assert started[0] - sent < 0.5


def test_a_claim_waits_only_as_long_as_it_may(tmp_path):
    from edgecitadel_agentd.service import MAX_CLAIM_WAIT_MS, _claim
    from edgecitadel_agentd.store import StoreError

    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        for wait_ms in (-1, MAX_CLAIM_WAIT_MS + 1, 1.5, True):
            with pytest.raises(StoreError, match="wait_ms"):
                _claim(
                    store,
                    connector_id="c",
                    token="t",
                    agent_id="a",
                    session_id="s",
                    wait_ms=wait_ms,
                )
    finally:
        store.close()


def _unused_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1",
    reason="set RUN_AGENTD_NATS_INTEGRATION=1 to run owned NATS integration",
)
async def test_an_enqueued_message_reaches_the_broker_well_within_100_ms(
    state_root, monkeypatch
):
    from nats.aio.client import Client as NATS

    from edgecitadel_plugin_runtime.jetstream import ensure_consumer, ensure_stream

    executable = shutil.which("nats-server")
    if executable is None:
        pytest.skip("nats-server is not installed")
    monkeypatch.setenv("EDGECITADEL_TRANSPORT_POLL_MS", "5000")
    tmp_path, budget = state_root
    port, token = _unused_port(), secrets.token_urlsafe(32)
    server_dir = tmp_path / "nats"
    server_dir.mkdir()
    config = server_dir / "nats.conf"
    config.write_text(
        f'host: "127.0.0.1"\nport: {port}\n'
        f"authorization {{ token: {json.dumps(token)} }}\n"
        f"jetstream {{ store_dir: {server_dir / 'js'} }}\n"
    )
    process = subprocess.Popen(
        [executable, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    url = f"nats://127.0.0.1:{port}"
    (state_dir / "node.json").write_text(
        json.dumps(
            {
                "agent_id": "owned-edge",
                "version": 2,
                "mode": "edge",
                "messaging_mode": "single-client",
                "nats_url": url,
                "nats_token": token,
                "plugin_nats_url": url,
                "plugin_nats_token": token,
            }
        )
    )
    stop = threading.Event()
    service = threading.Thread(target=serve, args=(state_dir / "agentd", stop))
    service.start()
    socket_path = socket_path_for(state_dir / "agentd")
    nc = NATS()
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                await nc.connect(servers=[url], token=token, connect_timeout=0.2)
                break
            except Exception:  # noqa: BLE001 - the broker is starting
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.05)
        while not socket_path.exists():
            await asyncio.sleep(0.01)
        admin = AgentdClient(
            socket_path,
            admin_token=(state_dir / "agentd/admin.token").read_text().strip(),
        )
        client = operator(admin, socket_path, outbound="remote-agent")
        anonymous = AgentdClient(socket_path)
        while not anonymous.call("health")["transport"].get("ready_inbox_count"):
            await asyncio.sleep(0.05)
        js = nc.jetstream()
        await ensure_stream(js, "remote-agent")
        await ensure_consumer(js, "remote-agent")
        inbox = await js.pull_subscribe(
            "agents.remote-agent.inbox", durable="remote-agent_inbox"
        )
        latencies = []
        for i in range(HANDOFFS):
            await asyncio.sleep(0.3)  # the outbound loop is idle
            sent = time.monotonic()
            await asyncio.to_thread(
                client.call,
                "task.create",
                recipient_id="remote-agent",
                payload={"body": str(i)},
            )
            [message] = await inbox.fetch(batch=1, timeout=5)
            latencies.append(time.monotonic() - sent)
            await message.ack()
        assert statistics.median(latencies) < budget, latencies
    finally:
        stop.set()
        service.join(timeout=10)
        if not nc.is_closed:
            await nc.close()
        process.terminate()
        process.wait(timeout=5)
