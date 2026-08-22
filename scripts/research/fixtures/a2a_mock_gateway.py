from __future__ import annotations

import json
import os
from typing import Any

from fastapi import FastAPI
from nats.aio.client import Client as NATS
from pydantic import BaseModel

from scripts.research.benchmark_core import command_envelope, now_iso


GATEWAY_ID = "bench-a2a-gateway"
app = FastAPI(title="Benchmark A2A Mock Gateway")
state: dict[str, Any] = {}


class SendTask(BaseModel):
    target_agent: str
    body: str


@app.on_event("startup")
async def startup() -> None:
    nc = NATS()
    kwargs = {
        "servers": [os.environ.get("NATS_URL", "nats://127.0.0.1:4222")],
        "connect_timeout": 3,
    }
    token = os.environ.get("NATS_TOKEN")
    if token:
        kwargs["token"] = token
    await nc.connect(**kwargs)
    state["nc"] = nc
    state["js"] = nc.jetstream()


@app.on_event("shutdown")
async def shutdown() -> None:
    nc: NATS | None = state.get("nc")
    if nc:
        await nc.close()


@app.post("/tasks/send")
async def send_task(req: SendTask) -> dict[str, str]:
    nc: NATS = state["nc"]
    js = state["js"]
    env = command_envelope(
        sender_id=GATEWAY_ID,
        recipient_id=req.target_agent,
        body=req.body,
        payload_extra={"gateway": "a2a_mock"},
    )
    data = json.dumps(env).encode()
    await js.publish(
        f"agents.{req.target_agent}.inbox",
        data,
        headers={"Nats-Msg-Id": env["id"]},
    )
    await nc.publish(f"agents.{GATEWAY_ID}.outbox", data)
    await nc.flush()
    return {"task_id": env["task_id"], "accepted_at": now_iso()}
