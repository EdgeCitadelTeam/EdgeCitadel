"""End-to-end pull-consumer behavior. Requires live NATS with JetStream.
Skip if NATS_URL_TEST unset."""

import asyncio
import json
import os
import pytest
import uuid
from nats.aio.client import Client as NATS
from edgecitadel_plugin_runtime.pull_consumer import PullConsumer, Context
from edgecitadel_plugin_runtime.jetstream import ensure_consumer, ensure_stream

NATS_URL = os.environ.get("NATS_URL_TEST", "nats://localhost:4222")
TOKEN = os.environ.get("NATS_TOKEN_TEST", os.environ.get("NATS_TOKEN", ""))
pytestmark = pytest.mark.asyncio


async def _connect():
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL], token=TOKEN, connect_timeout=1)
    except Exception:
        pytest.skip("NATS not reachable")
    return nc


async def test_fifo_one_at_a_time():
    nc = await _connect()
    js = nc.jetstream()
    agent_id = f"test-{uuid.uuid4().hex[:6]}"
    await ensure_stream(js, agent_id)
    await ensure_consumer(js, agent_id, ack_wait_sec=30)

    processing: list[str] = []
    order: list[str] = []
    gate = asyncio.Event()

    async def handle(env: dict, ctx: Context):
        processing.append(env["id"])
        assert len(processing) == 1, "violated max_ack_pending=1"
        await gate.wait()
        order.append(env["id"])
        processing.remove(env["id"])
        return ({"body": "ok"}, "completed")

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle, ack_wait_sec=30)
    task = asyncio.create_task(pc.run())

    # publish 3 commands
    ids = []
    for i in range(3):
        env = _cmd_env(recipient=agent_id, sender="test-sender", body=f"x{i}")
        ids.append(env["id"])
        await js.publish(
            f"agents.{agent_id}.inbox",
            json.dumps(env).encode(),
            headers={"Nats-Msg-Id": env["id"]},
        )

    await asyncio.sleep(0.5)
    gate.set()
    await asyncio.sleep(3)
    await pc.stop()
    task.cancel()
    await nc.drain()

    assert order == ids[: len(order)]  # in order


async def test_dedup_via_nats_msg_id():
    nc = await _connect()
    js = nc.jetstream()
    agent_id = f"dedup-{uuid.uuid4().hex[:6]}"
    await ensure_stream(js, agent_id)
    await ensure_consumer(js, agent_id, ack_wait_sec=10)

    calls = 0

    async def handle(env, ctx):
        nonlocal calls
        calls += 1
        return ({"body": "done"}, "completed")

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle, ack_wait_sec=10)
    task = asyncio.create_task(pc.run())

    env = _cmd_env(recipient=agent_id, sender="test", body="once")
    for _ in range(3):
        await js.publish(
            f"agents.{agent_id}.inbox",
            json.dumps(env).encode(),
            headers={"Nats-Msg-Id": env["id"]},
        )
    await asyncio.sleep(2)
    await pc.stop()
    task.cancel()
    await nc.drain()
    assert calls == 1, f"expected dedup to reduce to 1 call, got {calls}"


def _cmd_env(*, recipient, sender, body):
    from datetime import datetime, timezone

    ts = (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    return {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "command",
        "sender_id": sender,
        "recipient_id": recipient,
        "task_id": str(uuid.uuid4()),
        "timestamp": ts,
        "payload": {"body": body},
    }
