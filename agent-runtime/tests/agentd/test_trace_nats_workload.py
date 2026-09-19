"""Scoped overlapping roots across four owned agentd nodes and a real broker."""

import asyncio
import json
import os
import secrets
import shutil
import socket
import subprocess
import threading

import pytest
from nats.aio.client import Client as NATS
from test_trace_socket_workload import exercise

from service_test_support import serve
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.trace_correlation import TaskTraceContext

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1",
    reason="owned NATS integration opt-in required",
)


@pytest.fixture
def owned_nodes(tmp_path):
    binary = shutil.which("nats-server")
    assert binary is not None, "explicit qualification requires nats-server"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    token = secrets.token_urlsafe(32)
    config = tmp_path / "owned-nats.conf"
    config.write_text(
        f'host: "127.0.0.1"\nport: {port}\nauthorization {{ token: {json.dumps(token)} }}\njetstream {{ store_dir: {json.dumps(str(tmp_path / "js"))} }}\n'
    )
    config.chmod(0o600)
    broker = subprocess.Popen(
        [binary, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"nats://127.0.0.1:{port}"
    nodes = {}
    threads = []
    try:
        for letter in "abcd":
            state = tmp_path / f"node-{letter}"
            state.mkdir()
            (state / "node.json").write_text(
                json.dumps(
                    {
                        "agent_id": f"owned-node-{letter}",
                        "version": 2,
                        "mode": "edge",
                        "messaging_mode": "single-client",
                        "plugin_nats_url": url,
                        "plugin_nats_token": token,
                    }
                )
            )
            (state / "node.json").chmod(0o600)
            directory = state / "agentd"
            stop = threading.Event()
            thread = threading.Thread(target=serve, args=(directory, stop), daemon=True)
            thread.start()
            threads.append((thread, stop))
            endpoint = socket_path_for(directory)
            for _ in range(300):
                assert broker.poll() is None and thread.is_alive()
                if endpoint.exists():
                    break
                stop.wait(0.01)
            else:
                pytest.fail("owned agentd did not become ready")
            nodes[f"fixture-{letter}"] = (endpoint, directory)
        yield nodes, url, token
    finally:
        for _thread, stop in threads:
            stop.set()
        for thread, _stop in threads:
            thread.join(timeout=10)
        broker.terminate()
        broker.wait(timeout=10)
        assert all(not thread.is_alive() for thread, _ in threads)
        assert all(not endpoint.exists() for endpoint, _ in nodes.values())


async def exercise_nats(nodes, url, token):
    nc = NATS()
    await nc.connect(servers=[url], token=token, allow_reconnect=False)
    wire = []

    async def observe(message):
        if len(wire) < 128:
            wire.append((message.subject, json.loads(message.data)))

    try:
        await nc.subscribe("agents.*.inbox", cb=observe)
        await nc.flush()
        result = await exercise(
            *nodes["fixture-a"],
            recipient_services={
                key: value for key, value in nodes.items() if key != "fixture-a"
            },
        )
        await nc.flush()
        await asyncio.sleep(0)  # Drain the local callback task after broker flush.
        commands = [
            (subject, envelope)
            for subject, envelope in wire
            if envelope["type"] in {"command", "delegation"}
        ]
        results = [
            (subject, envelope)
            for subject, envelope in wire
            if envelope["type"] == "result"
            and envelope.get("task_state") == "completed"
        ]
        expected = {child["task_id"]: child for child in result["children"]}
        assert len(commands) == len(results) == 6
        assert {envelope["task_id"] for _, envelope in commands} == set(expected)
        assert {envelope["task_id"] for _, envelope in results} == set(expected)
        for subject, envelope in commands + results:
            context = TaskTraceContext.from_envelope(envelope)
            child = expected[envelope["task_id"]]
            assert context.trace_id == context.parent_run_id == child["trace_id"]
            assert subject == f"agents.{envelope['recipient_id']}.inbox"
        assert len(result["source_nodes"]) == 4
        result["wire_commands"] = len(commands)
        result["wire_completed_results"] = len(results)
        result["wire_message_ids"] = [
            envelope["id"] for _, envelope in commands + results
        ]
        return result
    finally:
        await nc.close()


@pytest.mark.asyncio
async def test_scoped_overlapping_roots_cross_real_nats(owned_nodes):
    result = await exercise_nats(*owned_nodes)
    assert result["effects"] == 6
