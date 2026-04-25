"""Skeleton adapter. Copy to adapters/<type>/adapter.py and fill in handle()."""
from __future__ import annotations
import asyncio, logging, os, signal
from pathlib import Path

from nats.aio.client import Client as NATS
from .agent_card import build_card
from .pull_consumer import PullConsumer, Context

log = logging.getLogger(__name__)


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    """Replace with real work. Return (payload, task_state)."""
    return ({"body": f"stub reply to {env['sender_id']}"}, "completed")


async def main(config_path: str | Path) -> None:
    card = build_card(config_path)
    agent_id = card["name"]
    ack_wait = int(os.environ.get("ACK_WAIT_SEC", "300"))

    nc = NATS()
    await nc.connect(servers=[os.environ["NATS_URL"]],
                     token=os.environ.get("NATS_TOKEN"))

    # Publish register
    import json, uuid
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")
    env = {"v": 1, "id": str(uuid.uuid4()), "type": "register",
           "sender_id": agent_id, "timestamp": ts, "payload": card}
    await nc.publish(f"agents.{agent_id}.register", json.dumps(env).encode())

    # Heartbeat loop
    async def heartbeat():
        interval = card["metadata"]["runtime.heartbeat_interval_sec"]
        while True:
            ts2 = datetime.now(timezone.utc).isoformat(
                timespec="milliseconds").replace("+00:00", "Z")
            hb = {"v": 1, "id": str(uuid.uuid4()), "type": "heartbeat",
                  "sender_id": agent_id, "timestamp": ts2, "payload": {}}
            await nc.publish(f"agents.{agent_id}.heartbeat",
                             json.dumps(hb).encode())
            await asyncio.sleep(interval)

    hb_task = asyncio.create_task(heartbeat())

    pc = PullConsumer(agent_id=agent_id, nc=nc, handler=handle,
                      ack_wait_sec=ack_wait)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(s, stop.set)

    consumer_task = asyncio.create_task(pc.run())
    await stop.wait()

    # graceful shutdown
    off = {"v": 1, "id": str(uuid.uuid4()), "type": "status",
           "sender_id": agent_id, "agent_state": "offline",
           "timestamp": datetime.now(timezone.utc).isoformat(
               timespec="milliseconds").replace("+00:00", "Z"),
           "payload": {"reason": "shutdown"}}
    await nc.publish(f"agents.{agent_id}.status", json.dumps(off).encode())
    await pc.stop()
    hb_task.cancel(); consumer_task.cancel()
    await nc.drain()
