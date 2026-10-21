"""The same workload in LangGraph's functional API, for timing only (system
``langgraph_functional``: reference and uncontained runs, no controls).

The StateGraph baseline (``harness.lg_graph``) is what the comparison runs,
because only it resumes and forks deterministically: a functional-API task's
identity depends on the order of calls, which an online supervisor's depends
on timing. This variant measures what that choice costs: the entrypoint
starts each invocation as a ``@task`` the moment its request is ready, with
no super-step barrier, and handles completions in the order they happen.
The agents' memory stays live in this process (deliveries and rogue
messages land in it as on agentd), so the det-worker runs exactly as there.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from typing import Any

from langgraph.func import entrypoint, task

from harness import lg_graph
from harness.lg_graph import INLINE_KINDS, _invoke, run


@task
async def invocation(request: dict[str, Any]) -> dict[str, Any]:
    port, _ = await _invoke(request, str(uuid.uuid4()))
    return {"requests": port.dispatches, "effects": port.scheduled}


@task
async def actuation(effect: dict[str, Any]) -> dict[str, Any]:
    return await lg_graph.actuate({"effect": effect})


def _ready(body: dict[str, Any]) -> bool:
    task_id = body["task"]
    agent = run().plan["tasks"][task_id]["agent"]
    return lg_graph._ready(task_id, run().workers[agent].state.delivered)


@entrypoint()
async def workload(inputs: dict[str, Any]) -> dict[str, Any]:
    slots = run().slots
    pending: list[dict[str, Any]] = list(inputs["requests"])
    running: dict[Any, str | None] = {}  # future -> agent holding a slot (or None)
    per_agent: Counter[str] = Counter()
    completed = 0
    while pending or running:
        progress = True
        while progress:
            progress, rest = False, []
            for request in pending:
                body, agent = request["body"], request["recipient"]
                kind = body.get("kind")
                if kind in {"task", "rogue"}:
                    if per_agent[agent] < slots[agent]:
                        future = asyncio.ensure_future(invocation(request))
                        running[future] = agent
                        per_agent[agent] += 1
                        progress = True
                    else:
                        rest.append(request)
                elif kind == "create" and not _ready(body):
                    rest.append(request)
                elif kind in INLINE_KINDS:
                    port, _ = await _invoke(request, str(uuid.uuid4()))
                    rest += port.dispatches
                    progress = True
            pending = rest
        if not running:
            break  # what is left waits for inputs that will never come
        done, _ = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            agent = running.pop(future)
            if agent is not None:
                per_agent[agent] -= 1
                completed += 1
            result = future.result()
            pending += result.get("requests") or []
            for effect in result.get("effects") or []:
                running[asyncio.ensure_future(actuation(effect))] = None
    return {"invocations": completed, "unstarted": len(pending)}


graph = workload
