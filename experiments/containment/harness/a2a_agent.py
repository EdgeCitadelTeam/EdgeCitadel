"""One workload agent as a Google A2A server (a2a-sdk), running the unchanged det-worker.

The det-worker reaches its runtime only through ``context.client.call`` (three
operations) and ``context.trace.binding_id``. This module supplies that
context on A2A, so ``det_worker.worker.DetWorker`` runs exactly as it does on
agentd:

- ``trace.dispatch`` sends the request to the recipient's A2A server
  (``SendMessage`` with ``return_immediately``); the recipient's A2A task id is
  the new invocation's id. A transport failure is retried with the same message
  id, and the receiving server drops a repeated message id, so a retry never
  becomes a second delivery.
- ``trace.use`` succeeds and records nothing: A2A has no lineage.
- ``effect.execute`` performs the effect itself, as an A2A agent calls a device
  API: it waits inside its task until the effect is due, then posts it to the
  run's sink. A cancel before that stops it; no gateway re-checks anything.

A2A has no pause: ``CancelTask`` ends the invocation (the worker records it as
cancelled). The protocol defines no cascade, so tasks a cancelled task already
created keep running. The operator's input block (``POST /operator/block``) is
the one control outside the protocol: from then on the server rejects
(``TASK_STATE_REJECTED``) every message that carries the named artifact.

Each agent runs at most ``--slots`` invocations at once, as its det-worker does
on agentd (``harness.deploy.slots``). Tasks live in a2a-sdk's in-memory store by
default; ``--task-store sqlite`` keeps them in a SQLite file instead (a2a-sdk's
``DatabaseTaskStore``), for overhead runs that match EdgeCitadel's durable
journal. Secrets arrive in files or the
environment, never argv: ``DET_WORKER_SERVICES_TOKEN``, ``<work-dir>/sink.url``
(the sink's URL carries its token) and ``<work-dir>/operator.token``.
"""

from __future__ import annotations

import argparse
import asyncio
import hmac
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import uvicorn
from a2a.helpers.proto_helpers import new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_jsonrpc_routes
from a2a.server.routes.agent_card_routes import create_agent_card_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import a2a_pb2 as pb
from a2a.utils.errors import A2AError
from det_worker.services import ServicesClient
from det_worker.worker import DetWorker
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from . import _worker  # noqa: F401  (puts det_worker on sys.path)
from .a2a_common import Peers, agent_card, is_transport_error, now_ms, request_body

log = logging.getLogger("a2a_agent")
SEND_ATTEMPTS = 4
SEND_RETRY_S = 0.5
DISPATCH_TIMEOUT_S = 60.0
SINK_TIMEOUT_S = 10.0
# Transport failures that outlast the retries; the runner counts this code as
# a testbed failure, like agentd's own transport codes.
UNAVAILABLE = "rpc:a2a unavailable"
CANCELLED = "a2a:task_cancelled"


@dataclass
class Invocation:
    """The runtime context ``DetWorker.handle`` gets for one A2A task."""

    task_id: str
    client: PortClient
    interruption: str | None = None
    cancelled: threading.Event = field(default_factory=threading.Event)

    @property
    def trace(self) -> Any:
        return _Trace(self.task_id)


@dataclass(frozen=True)
class _Trace:
    binding_id: str


class PortClient:
    """``context.client``: the det-worker calls it synchronously, in a thread."""

    def __init__(self, agent: Agent, invocation_id: str) -> None:
        self.agent = agent
        self.invocation_id = invocation_id

    def call(self, operation: str, **params: Any) -> dict[str, Any]:
        invocation = self.agent.invocations.get(self.invocation_id)
        if invocation is None or invocation.cancelled.is_set():
            return {"status": "error", "code": CANCELLED}
        if operation == "trace.use":
            return {"status": "ok", "result": {}}
        if operation == "trace.dispatch":
            return self.agent.dispatch(invocation, params)
        if operation == "effect.execute":
            return self.agent.execute_effect(invocation, params)
        return {"status": "error", "code": f"unsupported:{operation}"}


class Agent:
    """One workload agent: its det-worker (the agent's memory) and its A2A side."""

    def __init__(
        self,
        plan: dict[str, Any],
        agent: str,
        worker: DetWorker,
        peers: Peers,
        loop: asyncio.AbstractEventLoop,
        *,
        sink_url: str,
        slots: int,
    ) -> None:
        self.plan = plan
        self.agent = agent
        self.node = plan["agents"][agent]["node"]
        self.worker = worker
        self.peers = peers
        self.loop = loop
        self.sink_url = sink_url
        self.slots = asyncio.Semaphore(slots)
        self.invocations: dict[str, Invocation] = {}
        self.blocked: set[str] = set()
        self.seen_messages: set[str] = set()
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    # -- trace.dispatch ---------------------------------------------------------
    def dispatch(
        self, invocation: Invocation, params: dict[str, Any]
    ) -> dict[str, Any]:
        recipient = str(params["recipient_id"])
        if recipient not in self.peers.urls:
            return {"status": "error", "code": "a2a:unknown_recipient"}
        future = asyncio.run_coroutine_threadsafe(
            self._send(
                recipient,
                json.loads(params["request"]),
                message_id=str(params["request_id"]),
                context_id=str(params["child_task_id"]),
            ),
            self.loop,
        )
        return future.result(DISPATCH_TIMEOUT_S)

    async def _send(
        self, recipient: str, body: dict[str, Any], *, message_id: str, context_id: str
    ) -> dict[str, Any]:
        for attempt in range(SEND_ATTEMPTS):
            try:
                task = await self.peers.send(
                    recipient, body, message_id=message_id, context_id=context_id
                )
                return {"status": "ok", "result": {"task_id": task.id}}
            except A2AError as error:
                if not is_transport_error(error):
                    return {"status": "error", "code": f"a2a:{type(error).__name__}"}
                if attempt + 1 == SEND_ATTEMPTS:
                    return {"status": "error", "code": UNAVAILABLE}
                await self.worker.note(
                    "rpc_retry", operation="trace.dispatch", code=str(error)[:200]
                )
                await asyncio.sleep(SEND_RETRY_S * (attempt + 1))
        raise AssertionError("unreachable")

    # -- effect.execute -----------------------------------------------------------
    def execute_effect(
        self, invocation: Invocation, params: dict[str, Any]
    ) -> dict[str, Any]:
        due = params.get("scheduled_at_ms")
        if due is not None:
            wait_s = max(0.0, (int(due) - now_ms()) / 1000)
            if invocation.cancelled.wait(wait_s):
                return {"status": "error", "code": CANCELLED}
        if invocation.cancelled.is_set():
            return {"status": "error", "code": CANCELLED}
        record = {
            "data": params.get("data") or {},
            "effect_id": params.get("request_id"),
            "node_id": self.node,
            "agent_id": self.agent,
            "task_id": invocation.task_id,
            "effect_class": params.get("effect_class"),
            "target": params.get("target"),
            "scheduled_at_ms": due,
            "committed_at_ms": now_ms(),
        }
        request = urllib.request.Request(
            self.sink_url,
            data=json.dumps(record).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with self._opener.open(request, timeout=SINK_TIMEOUT_S) as response:
                status = int(response.status)
        except urllib.error.HTTPError as error:
            status = int(error.code)
        except (urllib.error.URLError, OSError) as error:
            log.warning("sink unreachable: %s", type(error).__name__)
            status = 502
        performed = 200 <= status < 300
        outcome = {
            "performed": performed,
            "code": None if performed else f"http:{status}",
        }
        return {"status": "ok", "result": {"state": "committed", "outcome": outcome}}

    # -- input block ----------------------------------------------------------------
    def carries_blocked(self, body: dict[str, Any]) -> str | None:
        kind = body.get("kind")
        item = None
        if kind == "deliver":
            item = body.get("item")
        elif kind in {"create", "task"}:
            item = (body.get("input") or {}).get("item")
        return str(item) if item is not None and str(item) in self.blocked else None


class Executor(AgentExecutor):
    def __init__(self, agent: Agent) -> None:
        self.agent = agent

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        agent, task_id = self.agent, str(context.task_id)
        updater = TaskUpdater(event_queue, task_id, str(context.context_id))
        message = context.message
        if context.current_task is None:
            await event_queue.enqueue_event(
                new_task(
                    task_id,
                    str(context.context_id),
                    pb.TaskState.TASK_STATE_SUBMITTED,
                    history=[message] if message is not None else [],
                )
            )
        body = request_body(message)
        message_id = message.message_id if message is not None else ""
        if body is None:
            await updater.reject()
            return
        if message_id in agent.seen_messages:
            await agent.worker.note("duplicate_message", message=message_id)
            await updater.reject()
            return
        agent.seen_messages.add(message_id)
        blocked = agent.carries_blocked(body)
        if blocked is not None:
            await agent.worker.note("input_blocked", item=blocked, task_id=task_id)
            await updater.reject()
            return
        invocation = Invocation(task_id, PortClient(agent, task_id))
        agent.invocations[task_id] = invocation
        try:
            async with agent.slots:
                await updater.start_work()
                envelope = {"payload": {"body": json.dumps(body)}, "task_id": task_id}
                try:
                    _, outcome = await agent.worker.handle(envelope, invocation)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("task %s failed", task_id)
                    await updater.failed()
                    return
            if outcome == "completed":
                await updater.complete()
            else:
                await updater.reject()
        finally:
            agent.invocations.pop(task_id, None)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        invocation = self.agent.invocations.get(str(context.task_id))
        if invocation is not None:
            invocation.interruption = "cancel"
            invocation.cancelled.set()
        updater = TaskUpdater(
            event_queue, str(context.task_id), str(context.context_id)
        )
        await updater.cancel()


def operator_routes(agent: Agent, token: str) -> list[Route]:
    async def block(request: Request) -> JSONResponse:
        given = request.headers.get("x-operator-token", "")
        if not hmac.compare_digest(given.encode(), token.encode()):
            return JSONResponse({"error": "invalid token"}, status_code=401)
        body = await request.json()
        item = body.get("item") if isinstance(body, dict) else None
        if not isinstance(item, str) or not item:
            return JSONResponse({"error": "item required"}, status_code=400)
        agent.blocked.add(item)
        return JSONResponse({"blocked": sorted(agent.blocked), "t_ms": now_ms()})

    async def health(_request: Request) -> JSONResponse:
        return JSONResponse({"ok": True, "agent": agent.agent})

    return [
        Route("/operator/block", block, methods=["POST"]),
        Route("/health", health, methods=["GET"]),
    ]


async def task_store(kind: str, work: Path) -> Any:
    if kind == "memory":
        return InMemoryTaskStore()
    from a2a.server.tasks import DatabaseTaskStore
    from sqlalchemy.ext.asyncio import create_async_engine

    store = DatabaseTaskStore(
        create_async_engine(f"sqlite+aiosqlite:///{work}/tasks.db")
    )
    await store.initialize()
    return store


def parse(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="harness.a2a_agent")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--agent", required=True)
    parser.add_argument("--services", required=True)
    parser.add_argument("--peers", required=True, help="JSON file: agent -> URL")
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--slots", type=int, default=4)
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--task-store", choices=("memory", "sqlite"), default="memory")
    args = parser.parse_args(argv)
    if not os.environ.get("DET_WORKER_SERVICES_TOKEN"):
        parser.error("DET_WORKER_SERVICES_TOKEN is required")
    if args.slots < 1:
        parser.error("--slots must be positive")
    return args


async def serve(args: argparse.Namespace) -> None:
    work = Path(args.work_dir)
    plan = json.loads(Path(args.plan).read_text())
    if args.agent not in plan["agents"]:
        raise SystemExit(f"agent {args.agent} is not in the plan")
    if plan["effect_sink"].get("adapter") != "http":
        raise SystemExit("the A2A baseline performs effects on the run's ledger only")
    peers_urls = json.loads(Path(args.peers).read_text())
    loop = asyncio.get_running_loop()
    # Each pending dispatch and each waiting effect holds one thread.
    loop.set_default_executor(ThreadPoolExecutor(max_workers=32 + 16 * args.slots))
    worker = DetWorker(
        plan,
        args.agent,
        ServicesClient(args.services, os.environ["DET_WORKER_SERVICES_TOKEN"]),
        node=plan["agents"][args.agent]["node"],
        event_log=work / f"{args.agent}.events.jsonl",
    )
    peers = Peers(peers_urls)
    agent = Agent(
        plan,
        args.agent,
        worker,
        peers,
        loop,
        sink_url=(work / "sink.url").read_text().strip(),
        slots=args.slots,
    )
    card = agent_card(args.agent, peers_urls[args.agent])
    handler = DefaultRequestHandler(
        agent_executor=Executor(agent),
        task_store=await task_store(args.task_store, work),
        agent_card=card,
    )
    app = Starlette(
        routes=[
            *operator_routes(agent, (work / "operator.token").read_text().strip()),
            *create_agent_card_routes(card),
            *create_jsonrpc_routes(handler, "/"),
        ]
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app, host=args.bind, port=args.port, log_level="warning", access_log=False
        )
    )
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            await serving  # surfaces the bind error
            raise SystemExit("the A2A server stopped before it started")
        await asyncio.sleep(0.05)
    (work / "ready").write_text(str(time.time()))
    try:
        await serving
    finally:
        await peers.close()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    asyncio.run(serve(parse(argv)))


if __name__ == "__main__":
    main()
