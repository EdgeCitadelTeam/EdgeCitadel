"""Integration tests for the Watchdog Plugin against an isolated test broker.

Each test starts a fresh NATS+JetStream subprocess via the same fixture
the _common conformance suite uses, registers/heartbeats simulated peer
agents, and asserts the watchdog's synthesised-result publishes land
on the original sender's inbox.
"""

from __future__ import annotations
import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from nats.aio.client import Client as NATS

import edgecitadel_watchdog_plugin
from edgecitadel_plugin_runtime.agent_card import build_card
from edgecitadel_plugin_runtime.jetstream import ensure_stream
from edgecitadel_watchdog_plugin import adapter as wd_adapter
from edgecitadel_watchdog_plugin.synth import WATCHDOG_AGENT_ID

pytestmark = pytest.mark.asyncio


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _envelope(env_type: str, sender: str, **kw) -> dict:
    base = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": env_type,
        "sender_id": sender,
        "timestamp": _now_iso(),
        "payload": kw.pop("payload", {}),
    }
    base.update(kw)
    return base


async def _publish(nc: NATS, subject: str, env: dict) -> None:
    await nc.publish(subject, json.dumps(env).encode())
    await nc.flush()


async def _publish_js(js, subject: str, env: dict) -> None:
    await js.publish(
        subject, json.dumps(env).encode(), headers={"Nats-Msg-Id": env["id"]}
    )


async def _start_watchdog(nats_url: str) -> tuple[asyncio.Task, asyncio.Event]:
    os.environ["NATS_URL"] = nats_url
    os.environ.pop("NATS_TOKEN", None)
    os.environ["WATCHDOG_THRESHOLD_FLOOR_SEC"] = "1"
    os.environ["WATCHDOG_TOLERANCE_SEC"] = "0"
    os.environ["WATCHDOG_DEFAULT_INTERVAL_SEC"] = "1"
    config_path = (
        Path(edgecitadel_watchdog_plugin.__file__).resolve().parent / "config.yaml"
    )
    stop = asyncio.Event()
    task = asyncio.create_task(
        wd_adapter.main(
            config_path=config_path, _stop_event=stop, _check_cadence_sec=0.2
        )
    )
    await asyncio.sleep(0.5)
    return task, stop


async def _stop_watchdog(task: asyncio.Task, stop: asyncio.Event) -> None:
    stop.set()
    try:
        await asyncio.wait_for(task, timeout=2)
    except asyncio.TimeoutError:
        task.cancel()


@pytest_asyncio.fixture
async def watchdog_task(nats_url):
    """Start the watchdog and guarantee teardown of its owned task."""
    task, stop = await _start_watchdog(nats_url)
    yield task, stop
    await _stop_watchdog(task, stop)


async def test_watchdog_registers_card(nats_url):
    """The watchdog publishes its own card on agents.watchdog-1.register."""
    nc = NATS()
    await nc.connect(nats_url)

    received: list[dict] = []

    async def cb(msg):
        received.append(json.loads(msg.data))

    sub = await nc.subscribe(f"agents.{WATCHDOG_AGENT_ID}.register", cb=cb)
    task, stop = await _start_watchdog(nats_url)

    # Wait briefly; the Plugin publishes register on startup.
    await asyncio.sleep(1.0)

    await sub.unsubscribe()
    await nc.close()
    await _stop_watchdog(task, stop)
    assert any(
        e["type"] == "register" and e["sender_id"] == WATCHDOG_AGENT_ID
        for e in received
    ), f"no register seen, got: {received}"


async def test_heartbeat_staleness_synthesises_failure(watchdog_task, nats_url):
    """tester-1 registers + heartbeats, then goes silent. The watchdog
    eventually publishes a synthesised failure to the sender's inbox."""
    nc = NATS()
    await nc.connect(nats_url)
    js = nc.jetstream()
    await ensure_stream(js, "test-sender")
    await ensure_stream(js, "tester-1")

    # Subscribe to aggregator's inbox (we'll be the sender)
    inbox_msgs: list[dict] = []

    async def inbox_cb(msg):
        inbox_msgs.append(json.loads(msg.data))

    inbox_sub = await js.subscribe(
        "agents.test-sender.inbox",
        durable="test_sender_inbox",
        cb=inbox_cb,
        manual_ack=False,
    )

    # tester-1 registers with 10s heartbeat
    card = build_card(
        Path(edgecitadel_watchdog_plugin.__file__).resolve().parent / "config.yaml"
    )
    card["name"] = "tester-1"
    card["metadata"]["runtime.roles"] = ["worker"]
    card["metadata"]["runtime.heartbeat_interval_sec"] = 1
    card_env = _envelope("register", "tester-1", payload=card)
    await _publish(nc, "agents.tester-1.register", card_env)
    await _publish(nc, "agents.tester-1.heartbeat", _envelope("heartbeat", "tester-1"))

    # Sender publishes a command to tester-1, mirroring on its outbox
    task_id = str(uuid.uuid4())
    cmd = _envelope(
        "command",
        "test-sender",
        recipient_id="tester-1",
        task_id=task_id,
        payload={"body": "noop"},
    )
    await _publish_js(js, "agents.tester-1.inbox", cmd)
    await _publish(nc, "agents.test-sender.outbox", cmd)

    # tester-1 stays silent. With 0.2s check cadence, threshold for 10s
    # interval = max(20, 20)+5 = 25s. We can't actually wait 25s in tests;
    # instead, fast-forward by manipulating the watchdog's state directly
    # via injected time would require more wiring. For this test we set
    # the THRESHOLD_FLOOR_SEC and TOLERANCE_SEC via env, OR (better) we
    # accept this is a slow-skipped test and gate it.
    # SHORT-CIRCUIT: instead of timing, send a heartbeat that's already
    # too old, then trigger a check.
    #
    # In practice the easiest path is to allow override via env:
    #   WATCHDOG_THRESHOLD_FLOOR_SEC=2 WATCHDOG_TOLERANCE_SEC=1
    # Then a 4s wait reliably fires.
    # For this plan we assume that override exists.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if any(
            m.get("payload", {}).get("error") == "recipient_offline" for m in inbox_msgs
        ):
            break
        await asyncio.sleep(0.5)

    await inbox_sub.unsubscribe()
    await nc.close()

    synth = [
        m
        for m in inbox_msgs
        if m.get("payload", {}).get("error") == "recipient_offline"
    ]
    assert synth, f"no synthesised failure observed, inbox: {inbox_msgs}"
    assert synth[0]["sender_id"] == WATCHDOG_AGENT_ID
    assert synth[0]["task_id"] == task_id
    assert synth[0]["payload"]["trigger"] == "heartbeat_staleness"


@pytest.mark.skip(
    reason="sticky-offline integration test deferred — covered by E2E phase3-watchdog-fast-path.spec.js"
)
async def test_sticky_offline_synthesises_immediately(watchdog_task, nats_url):
    """After tester-1 is flagged offline, a NEW command to tester-1 should
    be synthesised within milliseconds (no heartbeat-wait)."""
    # Setup: same bootstrap as previous test, but here we send TWO
    # commands — first triggers staleness path; second triggers sticky.
    # Implementation deferred to plan execution; the assertion is:
    # synth[1]["payload"]["trigger"] == "sticky_offline".


async def test_inbox_unknown_command_rejected(watchdog_task, nats_url):
    """A command sent to the watchdog's inbox should produce a
    task_state='rejected' result with reason='unknown_command'."""
    nc = NATS()
    await nc.connect(nats_url)
    js = nc.jetstream()
    await ensure_stream(js, "unknown-caller")

    # Subscribe to the rejection's destination — the sender's inbox
    received: list[dict] = []

    async def cb(msg):
        received.append(json.loads(msg.data))

    sub = await js.subscribe(
        "agents.unknown-caller.inbox",
        durable="unknown_caller_inbox",
        cb=cb,
        manual_ack=False,
    )

    cmd = _envelope(
        "command",
        "unknown-caller",
        recipient_id=WATCHDOG_AGENT_ID,
        task_id=str(uuid.uuid4()),
        payload={"body": "hello"},
    )
    await _publish_js(js, f"agents.{WATCHDOG_AGENT_ID}.inbox", cmd)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if any(m.get("task_state") == "rejected" for m in received):
            break
        await asyncio.sleep(0.2)

    await sub.unsubscribe()
    await nc.close()

    rej = [m for m in received if m.get("task_state") == "rejected"]
    assert rej, f"no rejection received, got: {received}"
    assert rej[0]["payload"]["reason"] == "unknown_command"
    assert rej[0]["sender_id"] == WATCHDOG_AGENT_ID
