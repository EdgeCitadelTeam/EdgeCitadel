"""Runtime bridge for an EdgeCitadel-owned Managed Agent process."""

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
from typing import Any, cast

from edgecitadel_plugin_runtime.agent_card import build_card

from .client import AgentdClient, AgentdClientError
from .service import socket_path_for

Handler = Callable[[dict[str, Any], Any], Awaitable[tuple[dict[str, Any], str]]]


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
            raise ValueError("Managed Agents may only request the memory service")
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


async def run(config_path: str | Path, handler: Handler) -> None:
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
    context = ManagedContext(client, agent_id)
    try:
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
            await asyncio.to_thread(
                client.call,
                "task.transition",
                task_id=task_id,
                state="running",
                session_id=session_id,
            )
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
            try:
                result, state = await handler(envelope, context)
            except Exception as error:  # noqa: BLE001
                result, state = {"error": type(error).__name__}, "failed"
            terminal = "cancelled" if state == "canceled" else state
            if terminal not in {"completed", "failed", "rejected", "cancelled"}:
                result, terminal = {"error": "invalid_handler_state"}, "failed"
            await asyncio.to_thread(
                client.call,
                "task.transition",
                task_id=task_id,
                state=terminal,
                session_id=session_id,
                result=result,
            )
    finally:
        stop.set()
        renewer.cancel()
        await asyncio.gather(renewer, return_exceptions=True)
        try:
            await asyncio.to_thread(client.call, "session.close", session_id=session_id)
        except AgentdClientError:
            pass
