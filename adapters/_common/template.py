"""Skeleton adapter. Copy to adapters/<type>/adapter.py and fill in handle()."""
from __future__ import annotations
import asyncio, json, logging, os, signal, uuid
from datetime import datetime, timezone
from pathlib import Path

from nats.aio.client import Client as NATS
from .agent_card import build_card
from .pull_consumer import PullConsumer, Context

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


async def publish_log(nc: NATS, agent_id: str, *,
                      level: str = "INFO",
                      message: str,
                      source: str | None = None,
                      extra: dict | None = None) -> None:
    """Publish a `log` envelope on agents.{id}.log.

    Adapters call this for lifecycle events (register, shutdown) and
    handler errors. Frontend `LogViewer` reads payload.level/source/message.
    Best-effort; swallows publish failures so logging never breaks the
    adapter loop."""
    payload = {"level": level, "message": message}
    if source:
        payload["source"] = source
    if extra:
        payload.update(extra)
    env = {"v": 1, "id": str(uuid.uuid4()), "type": "log",
           "sender_id": agent_id, "timestamp": _now_iso(),
           "payload": payload}
    try:
        await nc.publish(f"agents.{agent_id}.log",
                         json.dumps(env).encode())
    except Exception as e:  # noqa: BLE001
        log.warning("log envelope publish failed (%s): %s",
                    type(e).__name__, e)


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
    env = {"v": 1, "id": str(uuid.uuid4()), "type": "register",
           "sender_id": agent_id, "timestamp": _now_iso(), "payload": card}
    await nc.publish(f"agents.{agent_id}.register", json.dumps(env).encode())

    # Lifecycle log: started
    await publish_log(nc, agent_id, level="INFO", source="lifecycle",
                      message=f"adapter registered as {agent_id} "
                              f"(role={card['metadata']['runtime.roles'][0]}, "
                              f"kind={card['metadata']['runtime.kind']})")

    # Heartbeat loop
    async def heartbeat():
        interval = card["metadata"]["runtime.heartbeat_interval_sec"]
        while True:
            hb = {"v": 1, "id": str(uuid.uuid4()), "type": "heartbeat",
                  "sender_id": agent_id, "timestamp": _now_iso(), "payload": {}}
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
    await publish_log(nc, agent_id, level="INFO", source="lifecycle",
                      message="adapter received shutdown signal; draining")
    off = {"v": 1, "id": str(uuid.uuid4()), "type": "status",
           "sender_id": agent_id, "agent_state": "offline",
           "timestamp": _now_iso(),
           "payload": {"reason": "shutdown"}}
    await nc.publish(f"agents.{agent_id}.status", json.dumps(off).encode())
    await pc.stop()
    hb_task.cancel(); consumer_task.cancel()
    await nc.drain()
