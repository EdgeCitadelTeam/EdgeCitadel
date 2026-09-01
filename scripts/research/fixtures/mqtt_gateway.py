from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from nats.aio.client import Client as NATS

from scripts.research.benchmark_core import command_envelope, now_iso


GATEWAY_ID = "bench-mqtt-gateway"


def _topic_from_subject(subject: str) -> str:
    if "/" in subject:
        return subject
    if subject.startswith("devices."):
        return subject.replace(".", "/")
    return subject


def normalize_mqtt_payload(
    *, topic: str, payload: bytes, sender_id: str = GATEWAY_ID
) -> dict[str, Any]:
    try:
        body = json.loads(payload.decode())
        malformed = False
    except json.JSONDecodeError:
        body = {"raw": payload.decode(errors="replace")}
        malformed = True

    parts = topic.split("/")
    if len(parts) >= 4 and parts[0] == "devices" and parts[2] == "command":
        target = parts[3]
        return command_envelope(
            sender_id=sender_id,
            recipient_id=target,
            body=str(body.get("body", "")),
            payload_extra={
                "mqtt_topic": topic,
                "mqtt_device": parts[1],
                "malformed": malformed,
            },
        )

    payload_obj = {
        "mqtt_topic": topic,
        "mqtt_device": parts[1] if len(parts) > 1 else None,
        "malformed": malformed,
        **body,
    }
    return {
        "v": 1,
        "id": command_envelope(sender_id=sender_id, recipient_id=sender_id, body="log")[
            "id"
        ],
        "type": "log",
        "sender_id": sender_id,
        "timestamp": now_iso(),
        "payload": payload_obj,
    }


async def _connect() -> NATS:
    nc = NATS()
    kwargs = {
        "servers": [os.environ.get("NATS_URL", "nats://127.0.0.1:4222")],
        "connect_timeout": 3,
    }
    token = os.environ.get("NATS_TOKEN")
    if token:
        kwargs["token"] = token
    await nc.connect(**kwargs)
    return nc


async def run() -> None:
    nc = await _connect()
    try:
        js = nc.jetstream()

        async def cb(msg: Any) -> None:
            topic = _topic_from_subject(msg.subject)
            if not topic.startswith("devices/"):
                return
            env = normalize_mqtt_payload(
                topic=topic, payload=msg.data, sender_id=GATEWAY_ID
            )
            if env["type"] == "command":
                await js.publish(
                    f"agents.{env['recipient_id']}.inbox",
                    json.dumps(env).encode(),
                    headers={"Nats-Msg-Id": env["id"]},
                )
                await nc.publish(
                    f"agents.{GATEWAY_ID}.outbox", json.dumps(env).encode()
                )
                log_env = {
                    "v": 1,
                    "id": command_envelope(
                        sender_id=GATEWAY_ID, recipient_id=GATEWAY_ID, body="log"
                    )["id"],
                    "type": "log",
                    "sender_id": GATEWAY_ID,
                    "timestamp": now_iso(),
                    "payload": {
                        "mqtt_topic": topic,
                        "task_id": env["task_id"],
                        "recipient_id": env["recipient_id"],
                        "normalized_type": "command",
                    },
                }
                await nc.publish(
                    f"agents.{GATEWAY_ID}.log", json.dumps(log_env).encode()
                )
            else:
                await nc.publish(f"agents.{GATEWAY_ID}.log", json.dumps(env).encode())
            await nc.flush()

        await nc.subscribe(">", cb=cb)
        while True:
            await asyncio.sleep(1)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
