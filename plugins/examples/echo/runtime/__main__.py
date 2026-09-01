from __future__ import annotations

import asyncio
import json
import os
import signal
import uuid
from datetime import datetime, timezone

from nats.aio.client import Client as NATS


AGENT_ID = "echo-agent"


def now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def envelope(kind: str, payload: dict, **fields: object) -> bytes:
    value = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": kind,
        "sender_id": AGENT_ID,
        "timestamp": now(),
        "payload": payload,
        **fields,
    }
    return json.dumps(value, separators=(",", ":")).encode()


def card() -> dict:
    return {
        "name": AGENT_ID,
        "description": "Minimal working EdgeCitadel echo agent",
        "version": "0.1.0",
        "url": f"nats://edgecitadel/agents.{AGENT_ID}.inbox",
        "provider": {"organization": "EdgeCitadel"},
        "capabilities": {"streaming": False},
        "securitySchemes": {},
        "skills": [
            {
                "id": "edgecitadel.echo",
                "name": "echo",
                "description": "Return the received body",
            }
        ],
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 10,
            "runtime.deployment": os.environ.get("EDGECITADEL_NODE_ID", "unknown"),
        },
    }


async def run() -> None:
    nc = NATS()
    await nc.connect(servers=[os.environ["NATS_URL"]], token=os.environ["NATS_TOKEN"])
    domain = os.environ.get("NATS_DOMAIN")
    js = nc.jetstream(domain=domain) if domain else nc.jetstream()
    await nc.publish(f"agents.{AGENT_ID}.register", envelope("register", card()))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def heartbeat() -> None:
        while not stop.is_set():
            await nc.publish(f"agents.{AGENT_ID}.heartbeat", envelope("heartbeat", {}))
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
            except TimeoutError:
                pass

    subscription = await js.pull_subscribe(
        f"agents.{AGENT_ID}.inbox", durable=f"{AGENT_ID}_inbox"
    )

    async def consume() -> None:
        while not stop.is_set():
            try:
                messages = await subscription.fetch(1, timeout=1)
            except TimeoutError:
                continue
            for message in messages:
                try:
                    incoming = json.loads(message.data)
                    if incoming.get("type") in {"command", "delegation"}:
                        result = envelope(
                            "result",
                            {"body": incoming.get("payload", {}).get("body", "")},
                            recipient_id=incoming["sender_id"],
                            task_id=incoming["task_id"],
                            task_state="completed",
                        )
                        await js.publish(
                            f"agents.{incoming['sender_id']}.inbox", result
                        )
                    await message.ack()
                except Exception:
                    await message.nak()

    tasks = [asyncio.create_task(heartbeat()), asyncio.create_task(consume())]
    print(f"{AGENT_ID} registered and listening", flush=True)
    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await nc.publish(
        f"agents.{AGENT_ID}.status",
        envelope("status", {"reason": "supervisor-stop"}, agent_state="offline"),
    )
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(run())
