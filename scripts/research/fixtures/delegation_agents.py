from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Any
import uuid

from nats.aio.client import Client as NATS

from aggregator.jetstream_bootstrap import ensure_stream
from scripts.research.benchmark_core import (
    delegation_envelope,
    register_envelope,
    result_envelope,
)


RUNNING = True


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


async def _register(nc: NATS, agent_id: str) -> None:
    env = register_envelope(
        agent_id,
        metadata={
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.heartbeat_interval_sec": 30,
            "runtime.deployment": "test",
            "runtime.tags": ["benchmark", "delegation-fixture"],
        },
    )
    await nc.publish(f"agents.{agent_id}.register", json.dumps(env).encode())
    await nc.flush()


async def _publish_result(
    nc: NATS,
    js: Any,
    agent_id: str,
    inbound: dict[str, Any],
    payload: dict[str, Any],
    state: str = "completed",
) -> None:
    out = result_envelope(
        sender_id=agent_id,
        recipient_id=inbound["sender_id"],
        task_id=inbound["task_id"],
        task_state=state,
        payload=payload,
        context_id=inbound.get("context_id"),
    )
    data = json.dumps(out).encode()
    await js.publish(
        f"agents.{inbound['sender_id']}.inbox",
        data,
        headers={"Nats-Msg-Id": out["id"]},
    )
    await nc.publish(f"agents.{agent_id}.outbox", data)
    await nc.flush()


async def _publish_delegation(nc: NATS, js: Any, env: dict[str, Any]) -> None:
    data = json.dumps(env).encode()
    await js.publish(
        f"agents.{env['recipient_id']}.inbox",
        data,
        headers={"Nats-Msg-Id": env["id"]},
    )
    await nc.publish(f"agents.{env['sender_id']}.outbox", data)
    await nc.flush()


class ResultWaiter:
    def __init__(self) -> None:
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    async def watch(self, msg: Any) -> None:
        try:
            env = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        if env.get("type") != "result":
            return
        future = self._pending.get(env.get("task_id"))
        if future and not future.done():
            future.set_result(env)

    async def wait(self, task_id: str, timeout: float = 30) -> dict[str, Any]:
        future: asyncio.Future[dict[str, Any]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending[task_id] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(task_id, None)


async def _handle_e2(
    agent_id: str, env: dict[str, Any], nc: NATS, js: Any, waiter: ResultWaiter
) -> None:
    if agent_id == "bench-worker":
        await _publish_result(nc, js, agent_id, env, {"body": "worker complete"})
        return

    context_id = env.get("context_id") or str(uuid.uuid4())
    child = delegation_envelope(
        sender_id=agent_id,
        recipient_id="bench-worker",
        body="child work",
        context_id=context_id,
        hop_count=1,
        payload_extra={"parent_task_id": env["task_id"]},
    )
    wait_task = asyncio.create_task(waiter.wait(child["task_id"]))
    await _publish_delegation(nc, js, child)
    child_result = await wait_task
    await _publish_result(
        nc,
        js,
        agent_id,
        {**env, "context_id": context_id},
        {
            "body": "delegator complete",
            "child_task_id": child["task_id"],
            "child_state": child_result.get("task_state"),
        },
    )


NEXT_HOP = {
    "bench-hop-1": "bench-hop-2",
    "bench-hop-2": "bench-hop-3",
}


async def _handle_e3(
    agent_id: str, env: dict[str, Any], nc: NATS, js: Any, waiter: ResultWaiter
) -> None:
    if env.get("hop_count", 0) >= 8:
        await _publish_result(
            nc, js, agent_id, env, {"error": "hop_count_exceeded"}, state="rejected"
        )
        return
    if agent_id == "bench-hop-3":
        await _publish_result(nc, js, agent_id, env, {"body": "hop-3 complete"})
        return

    next_agent = NEXT_HOP.get(agent_id)
    if not next_agent:
        await _publish_result(
            nc, js, agent_id, env, {"error": "unknown_hop"}, state="failed"
        )
        return

    context_id = env.get("context_id") or str(uuid.uuid4())
    child = delegation_envelope(
        sender_id=agent_id,
        recipient_id=next_agent,
        body=f"hop from {agent_id}",
        context_id=context_id,
        hop_count=int(env.get("hop_count", 0)) + 1,
        payload_extra={"parent_task_id": env["task_id"]},
    )
    wait_task = asyncio.create_task(waiter.wait(child["task_id"]))
    await _publish_delegation(nc, js, child)
    child_result = await wait_task
    await _publish_result(
        nc,
        js,
        agent_id,
        {**env, "context_id": context_id},
        {"body": f"{agent_id} complete", "child_state": child_result.get("task_state")},
    )


async def _subscribe_agent(
    nc: NATS, js: Any, agent_id: str, scenario: str, waiter: ResultWaiter
) -> None:
    async def cb(msg: Any) -> None:
        try:
            env = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        if env.get("type") not in {"command", "delegation"}:
            return
        if scenario == "e2":
            await _handle_e2(agent_id, env, nc, js, waiter)
        else:
            await _handle_e3(agent_id, env, nc, js, waiter)

    await nc.subscribe(f"agents.{agent_id}.inbox", cb=cb)


async def run(scenario: str) -> None:
    nc = await _connect()
    try:
        js = nc.jetstream()
        waiter = ResultWaiter()
        await nc.subscribe("agents.*.outbox", cb=waiter.watch)
        agents = (
            ["bench-delegator", "bench-worker"]
            if scenario == "e2"
            else ["bench-hop-1", "bench-hop-2", "bench-hop-3", "bench-hop-limit"]
        )
        for agent_id in agents:
            await ensure_stream(js, agent_id)
            await _register(nc, agent_id)
            await _subscribe_agent(nc, js, agent_id, scenario, waiter)
        while RUNNING:
            await asyncio.sleep(1)
    finally:
        await nc.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["e2", "e3"], required=True)
    args = parser.parse_args()
    asyncio.run(run(args.scenario))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
