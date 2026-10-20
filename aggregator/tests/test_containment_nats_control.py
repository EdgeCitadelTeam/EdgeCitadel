"""The monitor's signed NATS control client against a real broker and agentd.

agentd's own wire handler (``containment_control.handle_wire``, which its NATS
transport calls) answers on the node's control subject, so authentication,
replay and fencing are agentd's; only the broker hop is owned by the test.
Opt-in like agentd's binary broker tests: RUN_AGENTD_NATS_INTEGRATION=1 with
nats-server on PATH.
"""

import asyncio
import json
import os
import secrets
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from uuid import uuid4

import pytest
from nats.aio.client import Client as NATS

from aggregator.containment_monitor.control import (
    AGENTD_CAPABILITIES,
    ControlError,
    ControlUnconfirmed,
    NatsControlClient,
)
from edgecitadel_agentd.containment_control import handle_wire, subject_for
from edgecitadel_agentd.store import AgentdStore

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1"
    or shutil.which("nats-server") is None,
    reason="set RUN_AGENTD_NATS_INTEGRATION=1 with nats-server on PATH",
)
OWNER = "core-monitor"


def _port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class Broker:
    """An owned nats-server and a responder loop standing in for agentd's transport."""

    def __init__(self, root: Path):
        self.port, self.token = _port(), secrets.token_urlsafe(24)
        config = root / "nats.conf"
        config.write_text(
            f"port: {self.port}\nauthorization {{ token: {json.dumps(self.token)} }}\n"
        )
        self.process = subprocess.Popen(
            [shutil.which("nats-server"), "-c", str(config)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.nc = NATS()
        deadline = time.monotonic() + 10
        while True:
            try:
                self.run(
                    self.nc.connect(
                        servers=[self.url], token=self.token, allow_reconnect=False
                    )
                )
                break
            except Exception:  # noqa: BLE001 - broker still starting
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

    @property
    def url(self) -> str:
        return f"nats://127.0.0.1:{self.port}"

    def run(self, coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(10)

    def serve(self, node_id: str, store: AgentdStore, *, drop_first: int = 0):
        """Answer like agentd's transport; drop the first replies if asked."""
        seen = []

        async def handler(message):
            reply = await asyncio.to_thread(
                handle_wire, store, message.data, node_id=node_id, gate=None
            )
            seen.append(json.loads(message.data)["command_id"])
            if len(seen) > drop_first:
                await message.respond(json.dumps(reply).encode())

        subscription = self.run(self.nc.subscribe(subject_for(node_id), cb=handler))
        self.run(self.nc.flush())
        return seen, subscription

    def close(self):
        self.run(self.nc.close())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.process.terminate()
        self.process.wait(5)


@pytest.fixture
def broker(tmp_path):
    value = Broker(tmp_path)
    try:
        yield value
    finally:
        value.close()


@pytest.fixture
def node(tmp_path):
    state = tmp_path / "edge"
    state.mkdir()
    (state / "node.json").write_text(json.dumps({"agent_id": "endurance"}))
    store = AgentdStore(state / "agentd" / "agentd.sqlite3")
    keys = state / "agentd" / "control-keys"
    keys.mkdir(mode=0o700)
    key = secrets.token_hex(32).encode()
    (keys / f"{OWNER}.key").write_bytes(key + b"\n")
    try:
        yield store, key
    finally:
        store.close()


def client(broker, key, **kwargs):
    kwargs.setdefault("confirmation_timeout_s", 2.0)
    kwargs.setdefault("attempt_timeout_s", 0.3)
    return NatsControlClient(broker.url, broker.token, OWNER, key, **kwargs)


def hold(task_id):
    return {"kind": "task", "object_id": task_id}


def test_signed_commands_are_confirmed_and_retried_once(broker, node):
    store, key = node
    seen, _ = broker.serve("endurance", store, drop_first=1)
    control = client(broker, key)
    try:
        control.refresh(["endurance"])
        assert control.capabilities("endurance") == AGENTD_CAPABILITIES
        task_id = str(uuid4())
        # The describe reply was dropped and its retry answered; a hold too.
        confirmation = control.send(
            "endurance", "hold", hold(task_id), policy_version=2
        )
        assert confirmation.applied_at_ms is not None
        assert confirmation.enforcement["gates"][0] == "claim"
        # Every retry repeated the same command; agentd applied each once.
        assert len(seen) == 3 and len(set(seen)) == 2
        assert [h["object_id"] for h in store.list_containment()["holds"]] == [task_id]
        with pytest.raises(ControlError) as stale:
            control.send("endurance", "release", hold(task_id), policy_version=1)
        assert stale.value.code == "stale_policy_version"
        assert not isinstance(stale.value, ControlUnconfirmed)
    finally:
        control.close()


def test_rejections_and_missing_confirmations_are_told_apart(broker, node, tmp_path):
    store, key = node
    broker.serve("endurance", store)
    wrong = client(broker, secrets.token_hex(32).encode())
    absent = client(broker, key, confirmation_timeout_s=0.6)
    try:
        with pytest.raises(ControlError) as unauthenticated:
            wrong.send("endurance", "hold", hold(str(uuid4())), policy_version=1)
        assert unauthenticated.value.code == "not_authenticated"
        # Nobody subscribes for this node: nothing was delivered.
        with pytest.raises(ControlError) as unreachable:
            absent.send("cluster", "hold", hold(str(uuid4())), policy_version=1)
        assert unreachable.value.code == "node_unreachable"
        assert not isinstance(unreachable.value, ControlUnconfirmed)
        # A node that receives but never answers: the outcome is unknown.
        broker.serve("jim-eq", store, drop_first=1000)
        with pytest.raises(ControlUnconfirmed):
            absent.send("jim-eq", "hold", hold(str(uuid4())), policy_version=1)
    finally:
        wrong.close()
        absent.close()
