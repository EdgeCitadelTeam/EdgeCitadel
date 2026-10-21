"""Deterministic Managed Agents for the C2 containment scenario.

Each agent is one process running agentd's production ``managed_runtime.run``
loop with a scripted handler: it claims tasks, binds them, dispatches children
through ``trace.dispatch``, reads child results with ``task.get`` (which records
lineage use) and authorizes effects through the effect gateway, exactly like a
model-driven harness, but with fixed content. Steps that the scenario sequences
wait for a gate file to exist. Every identifier a handler creates is derived
from its task, so a handler resumed after a containment pause repeats the same
dispatch, effect and commit requests and agentd answers them from its receipts.

The simulated actuator is ``b1``'s scheduled ``device.command``, executed by the
effect gateway: b1 asks ``effect.execute`` with ``scheduled_at_ms`` and returns;
endurance's gateway performs it when due, through its ``http`` adapter, by
posting to the harness's actuator ledger. The ledger is the scenario's
independent evidence of what happened, separate from agentd's own records.

    EDGECITADEL_STATE_DIR=<node state> EDGECITADEL_CONNECTOR_ID=managed-<agent> \\
      python -m e2e.containment.agents --agent tars --run-dir <run>
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
import uuid
from pathlib import Path
from typing import Any

from edgecitadel_agentd.client import AgentdClientError
from edgecitadel_agentd.managed_runtime import run

TERMINAL = {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
WAIT_LIMIT_SECONDS = 600
EFFECT_DELAY_MS = 20_000
ARTIFACT = "calibration-table v7: gain=1.85 offset=-0.02 (shared by team A)"


def stable_id(*parts: str) -> str:
    """A UUIDv4-shaped identifier that a resumed handler derives again."""
    digest = hashlib.sha256("|".join(parts).encode()).digest()[:16]
    return str(uuid.UUID(bytes=digest, version=4))


class Scenario:
    def __init__(self, agent: str, run_dir: Path):
        self.agent = agent
        self.gates = run_dir / "gates"
        self.marks = run_dir / "marks"
        self.due: dict[str, int] = {}  # task -> scheduled effect time, across resumes

    async def gate(self, name: str) -> None:
        deadline = time.monotonic() + WAIT_LIMIT_SECONDS
        while not (self.gates / name).exists():
            if time.monotonic() > deadline:
                raise TimeoutError(f"gate {name} never opened")
            await asyncio.sleep(0.05)

    def mark(self, name: str, **detail: object) -> None:
        self.marks.mkdir(parents=True, exist_ok=True)
        (self.marks / name).write_text(json.dumps(detail))

    async def call(self, context, operation: str, **params: Any) -> Any:
        return await asyncio.to_thread(context.client.call, operation, **params)

    async def dispatch(
        self, context, envelope: dict, recipient: str, step: str, request: dict
    ) -> str:
        task_id = envelope["task_id"]
        child = stable_id(task_id, step, "child")
        reply = await self.call(
            context,
            "trace.dispatch",
            schema_version=1,
            request_id=stable_id(task_id, step, "dispatch"),
            binding_id=context.trace.binding_id,
            child_task_id=child,
            recipient_id=recipient,
            request=json.dumps(request, sort_keys=True),
            skill_id=None,
            deadline_at_ms=None,
        )
        if reply.get("status") != "ok":
            raise RuntimeError(f"dispatch to {recipient} refused: {reply.get('code')}")
        return child

    async def result(self, context, child: str) -> dict:
        """Wait for a child's result; reading it records its use (lineage)."""
        deadline = time.monotonic() + WAIT_LIMIT_SECONDS
        while time.monotonic() < deadline:
            task = await self.call(context, "task.get", task_id=child)
            if task["state"] in TERMINAL:
                if task["state"] != "completed":
                    raise RuntimeError(f"child {child} ended {task['state']}")
                return json.loads(task["result"]["body"])
            await asyncio.sleep(0.2)
        raise TimeoutError(f"child {child} did not finish")

    # -- roles -------------------------------------------------------------------
    async def kipp(self, envelope: dict, context) -> dict:
        team_a = await self.dispatch(
            context, envelope, "tars", "team-a", {"step": "analyse"}
        )
        await self.dispatch(context, envelope, "case", "team-b", {"step": "healthy"})
        analysis = await self.result(context, team_a)
        acted = await self.dispatch(
            context,
            envelope,
            "case",
            "fan-out",
            {"step": "act", "analysis": analysis["analysis"]},
        )
        return {"summary": (await self.result(context, acted))["outcome"]}

    async def tars(self, envelope: dict, context) -> dict:
        await self.dispatch(context, envelope, "a3", "healthy", {"step": "survey"})
        produced = await self.dispatch(
            context, envelope, "a1", "produce", {"step": "produce"}
        )
        artifact = (await self.result(context, produced))["artifact"]
        consumed = await self.dispatch(
            context,
            envelope,
            "a2",
            "consume",
            {"step": "analyse", "artifact": artifact},
        )
        return {"analysis": (await self.result(context, consumed))["analysis"]}

    async def a1(self, envelope: dict, context) -> dict:
        return {"artifact": ARTIFACT}

    async def a2(self, envelope: dict, context) -> dict:
        request = json.loads(envelope["payload"]["body"])
        self.mark("a2-started", task_id=envelope["task_id"])
        await self.gate("a2-finish")
        return {"analysis": f"setpoint derived from [{request['artifact']}]"}

    async def a3(self, envelope: dict, context) -> dict:
        await self.gate("healthy-finish")
        return {"survey": "independent survey complete"}

    async def case(self, envelope: dict, context) -> dict:
        request = json.loads(envelope["payload"]["body"])
        if request["step"] == "healthy":
            await self.dispatch(context, envelope, "b2", "healthy", {"step": "monitor"})
            return {"outcome": "independent work dispatched"}
        acted = await self.dispatch(
            context,
            envelope,
            "b1",
            "actuate",
            {"step": "actuate", "analysis": request["analysis"]},
        )
        return {"outcome": (await self.result(context, acted))["outcome"]}

    async def b1(self, envelope: dict, context) -> dict:
        task_id = envelope["task_id"]
        request = json.loads(envelope["payload"]["body"])
        effect_id = stable_id(task_id, "actuate", "effect")
        due = self.due.setdefault(
            task_id, time.time_ns() // 1_000_000 + EFFECT_DELAY_MS
        )
        reply = await self.call(
            context,
            "effect.execute",
            schema_version=1,
            request_id=effect_id,
            binding_id=context.trace.binding_id,
            adapter="http",
            effect_class="device.command",
            target="actuator/1",
            data={"command": request["analysis"][:256]},
            tool_call_id=None,
            scheduled_at_ms=due,
        )
        if reply.get("status") != "ok":
            return {"outcome": f"effect refused at request: {reply.get('code')}"}
        self.mark("b1-scheduled", effect_id=effect_id, due=due)
        # The gateway owns the command now and performs it when it is due.
        return {"outcome": "actuator command scheduled"}

    async def b2(self, envelope: dict, context) -> dict:
        await self.gate("healthy-finish")
        return {"monitoring": "independent monitoring complete"}

    async def handle(self, envelope: dict, context) -> tuple[dict, str]:
        try:
            result = await getattr(self, self.agent)(envelope, context)
        except (RuntimeError, TimeoutError, AgentdClientError) as error:
            print(
                f"{self.agent}: task {envelope['task_id']} failed: {error}", flush=True
            )
            return {"body": json.dumps({"error": str(error)})}, "failed"
        return {"body": json.dumps(result, sort_keys=True)}, "completed"


ROLES = ("kipp", "tars", "case", "a1", "a2", "a3", "b1", "b2")


def card(agent: str, path: Path) -> Path:
    path.write_text(
        f"""agent_id: {agent}
name: {agent}
description: Scripted C2 scenario agent {agent}
runtime:
  kind: native
  roles: [worker]
  conformance: L1
  heartbeat_interval_sec: 30
skills:
  - id: work
    name: work
    description: scripted scenario step
"""
    )
    return path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent", choices=ROLES, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    scenario = Scenario(args.agent, args.run_dir)
    config = card(args.agent, args.run_dir / f"{args.agent}.card.yaml")
    asyncio.run(run(config, scenario.handle))


if __name__ == "__main__":
    main()
