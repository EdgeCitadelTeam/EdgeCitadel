"""The workload as a LangGraph StateGraph supervisor (the LangGraph baseline).

``langgraph dev`` serves this graph, one server per run, with the run's
compiled plan. It runs the unchanged det-worker: every agent is one
``DetWorker`` (the agent's memory) in this process, and the graph routes the
det-worker.v1 requests the workers dispatch.

Shape, as a LangGraph supervisor is usually written:

- ``supervisor`` routes. Requests that only move data (``deliver``, a
  pending ``create`` once its task's pushed inputs are in the state,
  ``rogue_msg``) run inline as det-worker invocations, since a LangGraph app
  keeps those as state updates, not nodes; a planned task (``task``) or an
  undeclared spawn (``rogue``) becomes a ``Send`` to ``worker``, at most the
  agent's det-worker slot count per step; a scheduled effect becomes a
  ``Send`` to ``actuate``.
- ``worker`` runs one invocation. Its dispatches and scheduled effects are
  returned as state at the node's end: the super-step barrier, not this
  module, decides when the next ones start.
- ``actuate`` performs one scheduled effect on the run's sink when it is due
  (the device call); a cancel before then stops it.

Every workload byte that later invocations read (pushed deliveries) is in the
graph state, so a fork from an earlier checkpoint replays with that
checkpoint's inputs; the operator's ``blocked_items`` and ``skip_tasks`` are
state too (``harness.lg_control``). Each node execution is a new invocation
(a fresh id), so a re-executed node repeats its work and effects, as LangGraph
does without application-level idempotency; the sink counts repeats.

Environment: ``LG_WORK_DIR`` (``plan.json``, ``slots.json``, ``sink.url``),
``DET_WORKER_SERVICES`` and ``DET_WORKER_SERVICES_TOKEN``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
import uuid
from collections import Counter
from functools import cache
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any, TypedDict

from det_worker.services import ServicesClient
from det_worker.worker import Delivered, DetWorker
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from harness import _worker  # noqa: F401  (puts det_worker on sys.path)
from harness.native import now_ms

log = logging.getLogger(__name__)
SINK_TIMEOUT_S = 10.0
INLINE_KINDS = ("deliver", "create", "rogue_msg")


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
def _union(a: list | None, b: list | None) -> list:
    return list(dict.fromkeys([*(a or []), *(b or [])]))


def _append(a: list | None, b: list | None) -> list:
    return [*(a or []), *(b or [])]


def _merge_delivered(a: dict | None, b: dict | None) -> dict:
    """target task -> item -> copies ({id, content}), each copy once."""
    out = {t: {i: list(c) for i, c in items.items()} for t, items in (a or {}).items()}
    for target, items in (b or {}).items():
        for item, copies in items.items():
            have = out.setdefault(target, {}).setdefault(item, [])
            seen = {c["id"] for c in have}
            have += [c for c in copies if c["id"] not in seen]
    return out


class State(TypedDict, total=False):
    requests: Annotated[list, _append]  # {id, recipient, body}
    taken: Annotated[list, _union]  # request ids routed or rejected
    delivered: Annotated[dict, _merge_delivered]
    effects: Annotated[list, _append]  # scheduled effects {id, due_ms, record}
    effects_taken: Annotated[list, _union]
    completed: Annotated[list, _union]  # spec tasks whose invocation finished
    blocked_items: Annotated[list, _union]  # operator: reject these artifacts
    skip_tasks: Annotated[list, _union]  # operator: never run these tasks
    outbox: list  # the supervisor's routing decision for this step


# ---------------------------------------------------------------------------
# The run: plan, agents, sink
# ---------------------------------------------------------------------------
class Run:
    def __init__(self, work: Path) -> None:
        self.plan = json.loads((work / "plan.json").read_text())
        self.slots = json.loads((work / "slots.json").read_text())
        self.sink_url = (work / "sink.url").read_text().strip()
        self.services = ServicesClient(
            os.environ["DET_WORKER_SERVICES"], os.environ["DET_WORKER_SERVICES_TOKEN"]
        )
        self.workers = {
            agent: DetWorker(self.plan, agent, self.services, node=spec["node"])
            for agent, spec in self.plan["agents"].items()
        }
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def post_sink(self, record: dict[str, Any]) -> tuple[bool, str | None]:
        """Perform one effect on the run's sink (the actuator). The ledger keeps
        one commit per effect; a repeated performance (a re-executed node) is
        recorded as a ``sink_repeat`` event, since it reached the device again."""
        request = urllib.request.Request(
            self.sink_url,
            data=json.dumps(record).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with self._opener.open(request, timeout=SINK_TIMEOUT_S) as response:
                reply = json.loads(response.read() or b"{}")
            if reply.get("repeated"):
                data = record["data"]
                self.services.event(
                    {
                        "run": self.plan["run_id"],
                        "agent": record["agent_id"],
                        "kind": "sink_repeat",
                        "task": data.get("ec_issuer"),
                        "effect_id": data.get("ec_effect"),
                        "rt": record["task_id"],
                        "inv_kind": "effect",
                        "t_local_ms": now_ms(),
                    }
                )
            return True, None
        except urllib.error.HTTPError as error:
            return False, f"http:{error.code}"
        except (urllib.error.URLError, OSError) as error:
            return False, type(error).__name__


@cache
def run() -> Run:
    return Run(Path(os.environ["LG_WORK_DIR"]))


class Port:
    """``context`` for one det-worker invocation: dispatches and scheduled
    effects are collected and returned as state when the node ends."""

    def __init__(self, agent: str, rt: str) -> None:
        self.agent = agent
        self.rt = rt
        self.trace = SimpleNamespace(binding_id=rt)
        self.client = self
        self.interruption: str | None = None
        self.resumed = False
        self.dispatches: list[dict[str, Any]] = []
        self.scheduled: list[dict[str, Any]] = []

    def call(self, operation: str, **p: Any) -> dict[str, Any]:
        if operation == "trace.use":
            return {"status": "ok", "result": {}}
        if operation == "trace.dispatch":
            self.dispatches.append(
                {
                    "id": str(p["request_id"]),
                    "recipient": str(p["recipient_id"]),
                    "body": json.loads(p["request"]),
                }
            )
            return {"status": "ok", "result": {"task_id": p["child_task_id"]}}
        if operation == "effect.execute":
            record = {
                "data": p.get("data") or {},
                "effect_id": p.get("request_id"),
                "agent_id": self.agent,
                "task_id": self.rt,
                "effect_class": p.get("effect_class"),
                "target": p.get("target"),
                "scheduled_at_ms": p.get("scheduled_at_ms"),
            }
            if p.get("scheduled_at_ms") is not None:
                self.scheduled.append(
                    {
                        "id": str(p["request_id"]),
                        "due_ms": int(p["scheduled_at_ms"]),
                        "record": record,
                    }
                )
                return {"status": "ok", "result": {"state": "scheduled"}}
            performed, code = run().post_sink({**record, "committed_at_ms": now_ms()})
            outcome = {"performed": performed, "code": code}
            return {
                "status": "ok",
                "result": {"state": "committed", "outcome": outcome},
            }
        return {"status": "error", "code": f"unsupported:{operation}"}


def _carried_item(body: dict[str, Any]) -> str | None:
    if body.get("kind") == "deliver":
        return body.get("item")
    return (body.get("input") or {}).get("item")


def _target_task(body: dict[str, Any]) -> str | None:
    kind = body.get("kind")
    if kind == "deliver":
        return body.get("target")
    if kind in {"create", "task"}:
        return body.get("task")
    return None


def rejected(body: dict[str, Any], blocked: set[str], skip: set[str]) -> bool:
    """The operator's controls: a blocked artifact, or a task removed by a fork."""
    return _carried_item(body) in blocked or _target_task(body) in skip


def _ready(task: str, delivered: dict[str, Any]) -> bool:
    plan = run().plan
    fault = plan["fault"]
    for d in plan["tasks"][task]["deliveries_in"]:
        need = (
            2
            if fault["class"] == "message_duplicate" and fault["item"] == d["item"]
            else 1
        )
        if len(delivered.get(task, {}).get(d["item"], [])) < need:
            return False
    return True


async def _invoke(request: dict[str, Any], rt: str) -> Port:
    agent = request["recipient"]
    port = Port(agent, rt)
    envelope = {"payload": {"body": json.dumps(request["body"])}, "task_id": rt}
    await run().workers[agent].handle(envelope, port)
    return port


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------
async def supervisor(state: State) -> dict[str, Any]:
    taken = set(state.get("taken") or [])
    blocked = set(state.get("blocked_items") or [])
    skip = set(state.get("skip_tasks") or [])
    delivered = _merge_delivered(state.get("delivered"), {})
    queue = [r for r in state.get("requests") or [] if r["id"] not in taken]
    new_requests: list[dict[str, Any]] = []
    new_taken: list[str] = []
    new_delivered: dict[str, Any] = {}
    sends: list[dict[str, Any]] = []
    per_agent: Counter[str] = Counter()
    slots = run().slots
    progress = True
    while progress:
        progress, rest = False, []
        for request in queue:
            body, agent = request["body"], request["recipient"]
            kind = body.get("kind")
            if rejected(body, blocked, skip):
                new_taken.append(request["id"])
                progress = True
            elif kind in {"task", "rogue"}:
                if per_agent[agent] < slots[agent]:
                    task = body.get("task")
                    sends.append(
                        {
                            "node": "worker",
                            "request": request,
                            "delivered": delivered.get(task, {}) if task else {},
                        }
                    )
                    per_agent[agent] += 1
                    new_taken.append(request["id"])
                    progress = True
                else:
                    rest.append(request)
            elif kind == "create" and not _ready(body["task"], delivered):
                rest.append(request)
            elif kind in INLINE_KINDS:
                port = await _invoke(request, str(uuid.uuid4()))
                new_taken.append(request["id"])
                if kind == "deliver":
                    copy = {"id": request["id"], "content": body["content"]}
                    update = {body["target"]: {body["item"]: [copy]}}
                    delivered = _merge_delivered(delivered, update)
                    new_delivered = _merge_delivered(new_delivered, update)
                new_requests += port.dispatches
                rest += port.dispatches
                progress = True
            else:
                new_taken.append(request["id"])  # not a workload request
                progress = True
        queue = rest
    done_effects = set(state.get("effects_taken") or [])
    effects = [e for e in state.get("effects") or [] if e["id"] not in done_effects]
    sends += [{"node": "actuate", "effect": e} for e in effects]
    return {
        "requests": new_requests,
        "taken": new_taken,
        "delivered": new_delivered,
        "effects_taken": [e["id"] for e in effects],
        "outbox": sends,
    }


def route(state: State) -> list[Send] | str:
    blocked = set(state.get("blocked_items") or [])
    skip = set(state.get("skip_tasks") or [])
    out = []
    for send in state.get("outbox") or []:
        if send["node"] == "worker" and rejected(
            send["request"]["body"], blocked, skip
        ):
            continue
        out.append(Send(send["node"], send))
    return out or END


async def worker(arg: dict[str, Any]) -> dict[str, Any]:
    request = arg["request"]
    body = request["body"]
    task = body.get("task")
    agent = run().workers[request["recipient"]]
    if task and body.get("kind") == "task":
        # The agent reads its pushed inputs from memory: as the state has them.
        agent.state.delivered[task] = {
            item: [Delivered(dict(c["content"]), c["id"]) for c in copies]
            for item, copies in (arg.get("delivered") or {}).items()
        }
    port = await _invoke(request, str(uuid.uuid4()))
    return {
        "requests": port.dispatches,
        "effects": port.scheduled,
        "completed": [task] if task and body.get("kind") == "task" else [],
    }


async def actuate(arg: dict[str, Any]) -> dict[str, Any]:
    effect = arg["effect"]
    delay = (effect["due_ms"] - now_ms()) / 1000
    if delay > 0:
        await asyncio.sleep(delay)
    record = {**effect["record"], "committed_at_ms": now_ms()}
    await asyncio.to_thread(run().post_sink, record)
    return {}


def build() -> Any:
    builder = StateGraph(State)
    builder.add_node("supervisor", supervisor)
    builder.add_node("worker", worker)
    builder.add_node("actuate", actuate)
    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges("supervisor", route, ["worker", "actuate", END])
    builder.add_edge("worker", "supervisor")
    builder.add_edge("actuate", "supervisor")
    return builder.compile()


graph = build()
