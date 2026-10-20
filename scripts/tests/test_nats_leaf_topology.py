"""Opt-in proof of the destination-owned JetStream topology over real Leaves."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import nats
import pytest

from tests.nats_server import NATS_IMAGE


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("RUN_NATS_LEAF_INTEGRATION") != "1",
        reason="set RUN_NATS_LEAF_INTEGRATION=1 to run owned Leaf integration",
    ),
]
ROOT = Path(__file__).parents[2]
IMAGE = NATS_IMAGE
OWNER_LABEL = "ai.edgecitadel.owner=test-nats-leaf"


def _run(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(arguments), check=False, capture_output=True, text=True
    )
    if check and result.returncode:
        raise RuntimeError(
            f"command failed: {arguments!r}\n{result.stdout}\n{result.stderr}"
        )
    return result


@dataclass
class _Server:
    name: str
    config: Path
    network: str
    port: int | None = None

    def start(self) -> None:
        _run(
            "docker",
            "run",
            "--detach",
            "--name",
            self.name,
            "--network",
            self.network,
            "--label",
            OWNER_LABEL,
            "--publish",
            "127.0.0.1::4222",
            "--mount",
            f"type=bind,source={self.config},target=/etc/nats/nats.conf,readonly",
            IMAGE,
            "-c",
            "/etc/nats/nats.conf",
        )
        self._refresh_port()

    def _refresh_port(self) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            output = _run(
                "docker", "port", self.name, "4222/tcp", check=False
            ).stdout.strip()
            if output:
                self.port = int(output.rsplit(":", 1)[1])
                return
            time.sleep(0.1)
        raise RuntimeError(f"NATS container did not publish a port: {self.name}")

    def stop(self) -> None:
        _run("docker", "stop", self.name)

    def restart(self) -> None:
        _run("docker", "start", self.name)
        self._refresh_port()

    def remove(self) -> None:
        _run("docker", "rm", "--force", self.name, check=False)

    @property
    def url(self) -> str:
        assert self.port is not None
        return f"nats://127.0.0.1:{self.port}"


def _core_config(password: str = "leaf-password") -> str:
    return f"""
server_name: core
listen: 0.0.0.0:4222
jetstream {{store_dir: "/data" }}
authorization {{token: "core-token" }}
leafnodes {{listen: 0.0.0.0:7422
  authorization {{username: "leaf-user"
    password: {json.dumps(password)}
  }}
}}
"""


def _edge_config(
    name: str, domain: str, core_name: str, password: str = "leaf-password"
) -> str:
    return f"""
server_name: {json.dumps(name)}
listen: 0.0.0.0:4222
jetstream {{
  domain: {json.dumps(domain)}
  store_dir: "/data"
}}
authorization {{ token: {json.dumps(name + "-token")} }}
leafnodes {{
  reconnect: "250ms"
  remotes: [
    {{ url: {json.dumps(f"nats-leaf://leaf-user:{password}@{core_name}:7422")} }}
  ]
}}
"""


async def _connect(url: str, token: str):
    deadline = time.monotonic() + 10
    while True:
        try:
            return await nats.connect(
                url,
                token=token,
                connect_timeout=1,
                allow_reconnect=False,
                max_reconnect_attempts=0,
            )
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.1)


async def _count(js) -> int:
    return (await js.stream_info("AGENT_INBOX")).state.messages


async def _publish_until_ack(js, subject: str, payload: bytes, message_id: str):
    deadline = time.monotonic() + 10
    while True:
        try:
            return await js.publish(
                subject,
                payload,
                timeout=1,
                headers={"Nats-Msg-Id": message_id},
            )
        except Exception:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.2)


async def test_destination_owned_streams_survive_disconnect_without_duplicates():
    suffix = uuid.uuid4().hex[:10]
    network = f"edgecitadel-leaf-test-{suffix}"
    core_name = f"edgecitadel-leaf-core-{suffix}"
    edge_a_name = f"edgecitadel-leaf-a-{suffix}"
    edge_b_name = f"edgecitadel-leaf-b-{suffix}"
    temporary = tempfile.TemporaryDirectory(prefix="edgecitadel-leaf-test-")
    directory = Path(temporary.name)
    configs = {
        "core": _core_config(),
        "a": _edge_config("edge-a", "EDGE_A", core_name),
        "b": _edge_config("edge-b", "EDGE_B", core_name),
    }
    paths: dict[str, Path] = {}
    for key, content in configs.items():
        path = directory / f"{key}.conf"
        path.write_text(content)
        path.chmod(0o600)
        paths[key] = path
    core = _Server(core_name, paths["core"], network)
    edge_a = _Server(edge_a_name, paths["a"], network)
    edge_b = _Server(edge_b_name, paths["b"], network)
    clients = []
    try:
        _run("docker", "network", "create", "--label", OWNER_LABEL, network)
        for server in (core, edge_a, edge_b):
            server.start()
        core_nc = await _connect(core.url, "core-token")
        edge_a_nc = await _connect(edge_a.url, "edge-a-token")
        edge_b_nc = await _connect(edge_b.url, "edge-b-token")
        clients.extend((core_nc, edge_a_nc, edge_b_nc))
        core_js = core_nc.jetstream()
        edge_a_js = edge_a_nc.jetstream(domain="EDGE_A")
        edge_b_js = edge_b_nc.jetstream(domain="EDGE_B")
        await core_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.aggregator.inbox"]
        )
        await edge_a_js.add_stream(
            name="AGENT_INBOX",
            subjects=["agents.edge-a-one.inbox", "agents.edge-a-two.inbox"],
        )
        await edge_b_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.edge-b-one.inbox"]
        )
        await asyncio.sleep(1)

        await _publish_until_ack(
            edge_a_js, "agents.edge-a-two.inbox", b"same-edge", "same-edge"
        )
        assert [
            await _count(core_js),
            await _count(edge_a_js),
            await _count(edge_b_js),
        ] == [0, 1, 0]

        await _publish_until_ack(
            core_js, "agents.edge-a-one.inbox", b"core-to-edge", "core-to-edge"
        )
        assert [
            await _count(core_js),
            await _count(edge_a_js),
            await _count(edge_b_js),
        ] == [0, 2, 0]

        await _publish_until_ack(
            edge_a_js, "agents.aggregator.inbox", b"edge-to-core", "edge-to-core"
        )
        assert [
            await _count(core_js),
            await _count(edge_a_js),
            await _count(edge_b_js),
        ] == [1, 2, 0]

        await _publish_until_ack(
            edge_a_js, "agents.edge-b-one.inbox", b"edge-to-edge", "edge-to-edge"
        )
        assert [
            await _count(core_js),
            await _count(edge_a_js),
            await _count(edge_b_js),
        ] == [1, 2, 1]

        core.stop()
        await asyncio.sleep(1)
        await edge_a_js.publish(
            "agents.edge-a-one.inbox",
            b"local-offline",
            timeout=1,
            headers={"Nats-Msg-Id": "local-offline"},
        )
        with pytest.raises(Exception):
            await edge_a_js.publish(
                "agents.edge-b-one.inbox",
                b"remote-offline",
                timeout=1,
                headers={"Nats-Msg-Id": "remote-offline"},
            )
        assert [await _count(edge_a_js), await _count(edge_b_js)] == [3, 1]

        core.restart()
        replacement_core = await _connect(core.url, "core-token")
        clients.append(replacement_core)
        replacement_core_js = replacement_core.jetstream()
        await _publish_until_ack(
            edge_a_js,
            "agents.edge-b-one.inbox",
            b"remote-after-reconnect",
            "remote-after-reconnect",
        )
        await edge_a_js.publish(
            "agents.edge-b-one.inbox",
            b"remote-after-reconnect",
            timeout=1,
            headers={"Nats-Msg-Id": "remote-after-reconnect"},
        )
        assert await _count(edge_b_js) == 2
        assert await _count(replacement_core_js) == 1

        edge_a.stop()
        edge_a.restart()
        replacement_edge_a = await _connect(edge_a.url, "edge-a-token")
        clients.append(replacement_edge_a)
        replacement_edge_a_js = replacement_edge_a.jetstream(domain="EDGE_A")
        assert await _count(replacement_edge_a_js) == 3
        await replacement_edge_a_js.publish(
            "agents.edge-a-one.inbox",
            b"local-after-restart",
            timeout=1,
            headers={"Nats-Msg-Id": "local-after-restart"},
        )
        assert await _count(replacement_edge_a_js) == 4
    finally:
        for client in clients:
            try:
                await client.close()
            except Exception:
                pass
        for server in (edge_b, edge_a, core):
            server.remove()
        _run("docker", "network", "rm", network, check=False)
        temporary.cleanup()


async def test_wrong_leaf_credential_cannot_reach_core_destination():
    suffix = uuid.uuid4().hex[:10]
    network = f"edgecitadel-leaf-deny-{suffix}"
    core_name = f"edgecitadel-leaf-deny-core-{suffix}"
    edge_name = f"edgecitadel-leaf-deny-edge-{suffix}"
    temporary = tempfile.TemporaryDirectory(prefix="edgecitadel-leaf-deny-")
    directory = Path(temporary.name)
    core_path = directory / "core.conf"
    edge_path = directory / "edge.conf"
    core_path.write_text(_core_config())
    edge_path.write_text(_edge_config("edge-denied", "EDGE_DENIED", core_name, "wrong"))
    core = _Server(core_name, core_path, network)
    edge = _Server(edge_name, edge_path, network)
    clients = []
    try:
        _run("docker", "network", "create", "--label", OWNER_LABEL, network)
        core.start()
        edge.start()
        core_nc = await _connect(core.url, "core-token")
        edge_nc = await _connect(edge.url, "edge-denied-token")
        clients.extend((core_nc, edge_nc))
        core_js = core_nc.jetstream()
        edge_js = edge_nc.jetstream(domain="EDGE_DENIED")
        await core_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.aggregator.inbox"]
        )
        await edge_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.edge-denied.inbox"]
        )
        await edge_js.publish("agents.edge-denied.inbox", b"local", timeout=1)
        with pytest.raises(Exception):
            await edge_js.publish("agents.aggregator.inbox", b"denied", timeout=1)
        assert await _count(core_js) == 0
        assert await _count(edge_js) == 1
    finally:
        for client in clients:
            try:
                await client.close()
            except Exception:
                pass
        edge.remove()
        core.remove()
        _run("docker", "network", "rm", network, check=False)
        temporary.cleanup()


async def test_revoked_leaf_credential_stops_remote_delivery_but_keeps_local():
    suffix = uuid.uuid4().hex[:10]
    network = f"edgecitadel-leaf-revoke-{suffix}"
    core_name = f"edgecitadel-leaf-revoke-core-{suffix}"
    edge_name = f"edgecitadel-leaf-revoke-edge-{suffix}"
    temporary = tempfile.TemporaryDirectory(prefix="edgecitadel-leaf-revoke-")
    directory = Path(temporary.name)
    core_path = directory / "core.conf"
    edge_path = directory / "edge.conf"
    core_path.write_text(_core_config())
    edge_path.write_text(_edge_config("edge-revoked", "EDGE_REVOKED", core_name))
    core = _Server(core_name, core_path, network)
    edge = _Server(edge_name, edge_path, network)
    clients = []
    try:
        _run("docker", "network", "create", "--label", OWNER_LABEL, network)
        core.start()
        edge.start()
        core_nc = await _connect(core.url, "core-token")
        edge_nc = await _connect(edge.url, "edge-revoked-token")
        clients.extend((core_nc, edge_nc))
        core_js = core_nc.jetstream()
        edge_js = edge_nc.jetstream(domain="EDGE_REVOKED")
        await core_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.aggregator.inbox"]
        )
        await edge_js.add_stream(
            name="AGENT_INBOX", subjects=["agents.edge-revoked.inbox"]
        )
        await _publish_until_ack(
            edge_js,
            "agents.aggregator.inbox",
            b"before-revocation",
            "before-revocation",
        )
        assert await _count(core_js) == 1

        core.stop()
        core_path.write_text(_core_config(password="rotated-leaf-password"))
        core.restart()
        replacement_core = await _connect(core.url, "core-token")
        clients.append(replacement_core)
        replacement_core_js = replacement_core.jetstream()
        await asyncio.sleep(1)

        with pytest.raises(Exception):
            await edge_js.publish(
                "agents.aggregator.inbox",
                b"after-revocation",
                timeout=1,
                headers={"Nats-Msg-Id": "after-revocation"},
            )
        await edge_js.publish(
            "agents.edge-revoked.inbox",
            b"local-after-revocation",
            timeout=1,
            headers={"Nats-Msg-Id": "local-after-revocation"},
        )
        assert await _count(replacement_core_js) == 1
        assert await _count(edge_js) == 1
    finally:
        for client in clients:
            try:
                await client.close()
            except Exception:
                pass
        edge.remove()
        core.remove()
        _run("docker", "network", "rm", network, check=False)
        temporary.cleanup()
