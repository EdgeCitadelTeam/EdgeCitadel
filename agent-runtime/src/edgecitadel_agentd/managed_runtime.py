"""Runtime bridge for an EdgeCitadel-owned Managed Agent process.

Containment of claimed work
---------------------------
agentd owns task state; this runtime only reacts to it. While a handler runs,
a watcher reads the task every ``WATCH_INTERVAL_SECONDS``:

- A hold (containment or restore) **pauses** the invocation. The handler is
  cancelled (``context.interruption == "pause"``; a harness aborts its current
  turn) and the runtime waits. The task keeps its state and claim, so agentd
  still refuses its transitions, dispatches and effects. When the hold is
  released, the runtime **resumes** the same invocation: it calls the handler
  again for the same task and binding with ``context.resumed`` set. Whether a
  resumed handler continues or refuses to repeat work is the package's rule.
- A terminal state set by someone else (a sender or controller cancel, or a
  daemon outcome) **cancels** the invocation: the handler is cancelled
  (``context.interruption == "cancel"``) and the run finishes with that state.

A handler that returns while its task is held keeps its result in memory until
the hold is released (then the result is committed) or the task ends
otherwise. If this process exits meanwhile, agentd's session recovery decides
the task once the hold is released.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, cast

from edgecitadel_plugin_runtime.agent_card import build_card

from .client import AgentdClient, AgentdClientError
from .service import socket_path_for
from .trace_correlation import TaskTraceContext
from .trace_producer import RuntimeTrace

Handler = Callable[[dict[str, Any], Any], Awaitable[tuple[dict[str, Any], str]]]
Signal = Literal["run", "held", "terminal", "stopped"]
WATCH_INTERVAL_SECONDS = 0.5
TERMINAL_STATES = frozenset(
    {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
)


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ManagedNatsProxy:
    """Narrow compatibility surface for existing memory helpers."""

    def __init__(self, client: AgentdClient, agent_id: str) -> None:
        self.client = client
        self.agent_id = agent_id

    async def request(self, subject: str, payload: bytes, *, timeout: float) -> object:
        del timeout
        if subject not in {"memory.turns.get", "memory.turns.put"}:
            raise ValueError("Agents from packages may only request the memory service")
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("memory payload must be an object")
        value["agent_id"] = self.agent_id
        operation = "memory.get" if subject.endswith(".get") else "memory.put"
        result = await asyncio.to_thread(
            self.client.call, operation, payload=cast(Mapping[str, object], value)
        )
        return SimpleNamespace(data=json.dumps(result).encode())


class ManagedContext:
    def __init__(self, client: AgentdClient, agent_id: str) -> None:
        self.client = client
        self.agent_id = agent_id
        self.nc = ManagedNatsProxy(client, agent_id)
        self.js = None
        self.msg = SimpleNamespace()
        self.trace = RuntimeTrace(client)
        # Set by the runtime: why the handler is being cancelled, and whether
        # this call resumes an invocation that a containment hold paused.
        self.interruption: Literal["pause", "cancel"] | None = None
        self.resumed = False

    async def in_progress(self) -> None:
        return None

    async def publish_progress(
        self,
        task_id: str,
        *,
        body: str = "",
        progress: int | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> None:
        payload: dict[str, object] = {"message": body}
        if progress is not None:
            payload["progress"] = progress
        if extra:
            payload.update(extra)
        try:
            await asyncio.to_thread(
                self.client.call,
                "task.progress",
                task_id=task_id,
                payload=payload,
            )
        except AgentdClientError:
            return


class _TaskSupervisor:
    """Drive one claimed task through pause, resume, cancel and completion."""

    def __init__(
        self,
        client: AgentdClient,
        session_id: str,
        task_id: str,
        stop: asyncio.Event,
    ) -> None:
        self.client = client
        self.session_id = session_id
        self.task_id = task_id
        self.stop = stop

    async def signal(self) -> tuple[Signal, str]:
        """The task's containment signal; an unreadable task keeps running."""
        if self.stop.is_set():
            return "stopped", ""
        try:
            task = cast(
                Mapping[str, object],
                await asyncio.to_thread(
                    self.client.call, "task.get", task_id=self.task_id
                ),
            )
        except AgentdClientError:
            return "run", ""
        state = str(task["state"])
        if state in TERMINAL_STATES:
            return "terminal", state
        if "containment_hold" in task or "restore_status" in task:
            return "held", state
        return "run", state

    async def wait_while_held(self) -> tuple[Signal, str]:
        while True:
            try:
                await asyncio.wait_for(self.stop.wait(), WATCH_INTERVAL_SECONDS)
            except TimeoutError:
                pass
            signal, state = await self.signal()
            if signal != "held":
                return signal, state

    async def transition(
        self, state: str, result: dict[str, Any] | None = None
    ) -> str | None:
        """Commit a transition, waiting out a hold; return the task's final say.

        Returns ``state`` once committed, another terminal state if the task
        ended meanwhile (a cancel wins over a held result), or None if the
        runtime is stopping.
        """
        while True:
            try:
                await asyncio.to_thread(
                    self.client.call,
                    "task.transition",
                    task_id=self.task_id,
                    state=state,
                    session_id=self.session_id,
                    **({"result": result} if result is not None else {}),
                )
                return state
            except AgentdClientError:
                signal, current = await self.signal()
                if signal == "terminal":
                    return current
                if signal != "held":
                    raise
            signal, current = await self.wait_while_held()
            if signal == "terminal":
                return current
            if signal == "stopped":
                return None

    async def execute(
        self, handler: Handler, envelope: dict[str, Any], context: ManagedContext
    ) -> str | None:
        """Run the handler until it completes, is cancelled, or the runtime stops."""
        while True:
            context.interruption = None
            work = asyncio.create_task(handler(envelope, context))
            signal: Signal = "run"
            state = ""
            try:
                while not work.done():
                    await asyncio.wait({work}, timeout=WATCH_INTERVAL_SECONDS)
                    if work.done():
                        break
                    signal, state = await self.signal()
                    # A handler that finished meanwhile keeps its result.
                    if signal in {"held", "terminal"} and not work.done():
                        break
            finally:
                if not work.done() and signal not in {"held", "terminal"}:
                    # The runtime itself is being cancelled; do not orphan work.
                    work.cancel()
            if work.done():
                try:
                    result, returned = work.result()
                except (Exception, asyncio.CancelledError) as error:  # noqa: BLE001
                    result, returned = {"error": type(error).__name__}, "failed"
                terminal = "cancelled" if returned == "canceled" else returned
                if terminal not in {"completed", "failed", "rejected", "cancelled"}:
                    result, terminal = {"error": "invalid_handler_state"}, "failed"
                return await self.transition(terminal, result)
            context.interruption = "pause" if signal == "held" else "cancel"
            work.cancel()
            await asyncio.gather(work, return_exceptions=True)
            if signal == "terminal":
                return state
            signal, state = await self.wait_while_held()
            if signal != "run":
                return state if signal == "terminal" else None
            context.resumed = True


async def run(
    config_path: str | Path,
    handler: Handler,
    *,
    on_start: Callable[[AgentdClient], Awaitable[None]] | None = None,
) -> None:
    state_dir = Path(os.environ["EDGECITADEL_STATE_DIR"])
    card = build_card(config_path)
    raw_skills = card.get("skills", [])
    if not isinstance(raw_skills, list):
        raise ValueError("Managed Agent Card skills must be a list")
    skill_items = [
        cast(dict[str, object], skill)
        for skill in raw_skills
        if isinstance(skill, dict)
    ]
    card["skills"] = [
        {
            key: skill[key]
            for key in ("id", "name", "description", "tags")
            if key in skill
        }
        for skill in skill_items
    ]
    capabilities = [str(skill.get("id")) for skill in skill_items if skill.get("id")]
    agent_id = str(card["name"])
    connector_id = os.environ.get("EDGECITADEL_CONNECTOR_ID", f"managed-{agent_id}")
    token_path = state_dir / "connectors" / f"{connector_id}.token"
    if not token_path.is_file():
        raise ValueError(
            "Managed Agent credential is missing; start it with edgecitadel agent start"
        )
    token = token_path.read_text().strip()
    client = AgentdClient(
        socket_path_for(state_dir / "agentd"), connector_id=connector_id, token=token
    )
    await asyncio.to_thread(
        client.call,
        "connector.update",
        host_type="managed-agent",
        agent_id=agent_id,
        capabilities=capabilities,
        card=card,
    )
    session = cast(
        Mapping[str, object], await asyncio.to_thread(client.call, "session.open")
    )
    session_id = str(session["session_id"])
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)

    async def renew() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=20)
            except TimeoutError:
                await asyncio.to_thread(
                    client.call, "session.renew", session_id=session_id
                )

    renewer = asyncio.create_task(renew())
    try:
        if on_start is not None:
            await on_start(client)
        while not stop.is_set():
            task = await asyncio.to_thread(
                client.call, "task.claim", session_id=session_id
            )
            if task is None:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.5)
                except TimeoutError:
                    pass
                continue
            record = cast(Mapping[str, object], task)
            task_id = str(record["task_id"])
            context = ManagedContext(client, agent_id)
            supervisor = _TaskSupervisor(client, session_id, task_id, stop)
            await context.trace.bind(session_id=session_id, task_id=task_id)
            # A hold placed between the claim and this transition pauses the
            # task before its handler starts.
            started = await supervisor.transition("running")
            if started != "running":
                if started is not None:
                    await context.trace.finish(started)
                continue
            envelope = {
                "v": 1,
                "id": str(uuid.uuid4()),
                "type": "command",
                "sender_id": record["sender_id"],
                "recipient_id": agent_id,
                "task_id": task_id,
                "context_id": record.get("context_id") or task_id,
                "hop_count": 0,
                "timestamp": _timestamp(),
                "payload": {
                    **cast(dict[str, object], record["payload"]),
                    **(
                        {"skill_id": record["skill_id"]}
                        if record.get("skill_id")
                        else {}
                    ),
                },
            }
            if "trace_context" in record:
                envelope = TaskTraceContext(
                    **cast(dict[str, Any], record["trace_context"])
                ).apply(envelope)
            final = await supervisor.execute(handler, envelope, context)
            if final is not None:
                await context.trace.finish(final)
    finally:
        stop.set()
        renewer.cancel()
        await asyncio.gather(renewer, return_exceptions=True)
        try:
            await asyncio.to_thread(client.call, "session.close", session_id=session_id)
        except AgentdClientError:
            pass
