from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from nats.aio.client import Client as NATS

from scripts.research.benchmark_core import (
    progress_envelope,
    register_envelope,
    result_envelope,
)


AGENT_ID = "bench-cancel"


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


async def _publish_result(
    nc: NATS, js: Any, inbound: dict[str, Any], state: str, payload: dict[str, Any]
) -> None:
    env = result_envelope(
        sender_id=AGENT_ID,
        recipient_id=inbound["sender_id"],
        task_id=inbound["task_id"],
        task_state=state,
        payload=payload,
        context_id=inbound.get("context_id"),
    )
    data = json.dumps(env).encode()
    await js.publish(
        f"agents.{inbound['sender_id']}.inbox",
        data,
        headers={"Nats-Msg-Id": env["id"]},
    )
    await nc.publish(f"agents.{AGENT_ID}.outbox", data)
    await nc.flush()


async def _run_task(
    nc: NATS, js: Any, inbound: dict[str, Any], active: dict[str, asyncio.Task[None]]
) -> None:
    task_id = inbound["task_id"]
    try:
        for index in range(1, 41):
            env = progress_envelope(
                sender_id=AGENT_ID,
                recipient_id=inbound["sender_id"],
                task_id=task_id,
                payload={"message": f"progress {index}", "progress": index},
                context_id=inbound.get("context_id"),
            )
            await nc.publish(
                f"agents.{AGENT_ID}.task_progress.{task_id}", json.dumps(env).encode()
            )
            await nc.flush()
            await asyncio.sleep(0.25)
        await _publish_result(
            nc, js, inbound, "completed", {"body": "completed without cancel"}
        )
    except asyncio.CancelledError:
        await _publish_result(nc, js, inbound, "canceled", {"body": "canceled"})
        raise
    finally:
        active.pop(task_id, None)


async def run() -> None:
    nc = await _connect()
    active: dict[str, asyncio.Task[None]] = {}
    try:
        js = nc.jetstream()
        register = register_envelope(
            AGENT_ID,
            metadata={
                "runtime.kind": "native",
                "runtime.roles": ["worker"],
                "runtime.heartbeat_interval_sec": 30,
                "runtime.deployment": "test",
                "runtime.tags": ["benchmark", "cancel-fixture"],
            },
        )
        await nc.publish(f"agents.{AGENT_ID}.register", json.dumps(register).encode())
        await nc.flush()

        async def cb(msg: Any) -> None:
            env = json.loads(msg.data)
            if env.get("type") == "command":
                active[env["task_id"]] = asyncio.create_task(
                    _run_task(nc, js, env, active)
                )
            elif env.get("type") == "cancel":
                task = active.get(env["task_id"])
                if task:
                    task.cancel()

        await nc.subscribe(f"agents.{AGENT_ID}.inbox", cb=cb)
        while True:
            await asyncio.sleep(1)
    finally:
        for task in active.values():
            task.cancel()
        await nc.close()


if __name__ == "__main__":
    asyncio.run(run())
