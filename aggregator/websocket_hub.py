"""WebSocket fan-out hub.

The MessageRouter calls `broadcast()` (for envelopes) and
`broadcast_event()` (for status / register / log shorthands) after it
persists an inbound NATS event. The hub tracks two kinds of
subscriptions:

- Global subscribers (`/ws/stream`) receive everything.
- Per-agent subscribers (`/ws/agent/{id}`) receive only envelopes whose
  `sender_id` or `recipient_id` matches `id`.

The wire format matches what `frontend/src/hooks/useWebSocket.js`
already parses today:

    {"event": "message",            "data": <envelope>}
    {"event": "agent_status_change","data": {agent_id, agent_state}}
    {"event": "agent_registered",   "data": {agent_id}}
    {"event": "log",                "data": {level, message, source?}}

Send failures (closed sockets, slow consumers) drop the offending
client without crashing the broadcast — the next handler tick is
unaffected.
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Set

from fastapi import WebSocket

log = logging.getLogger(__name__)


class WebSocketHub:
    def __init__(self) -> None:
        self._global: Set[WebSocket] = set()
        self._per_agent: dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def add_global(self, ws: WebSocket) -> None:
        async with self._lock:
            self._global.add(ws)
        log.info("ws subscriber added (global) — total=%d", len(self._global))

    async def add_agent(self, agent_id: str, ws: WebSocket) -> None:
        async with self._lock:
            self._per_agent.setdefault(agent_id, set()).add(ws)
        log.info("ws subscriber added (agent=%s)", agent_id)

    async def remove(self, ws: WebSocket) -> None:
        async with self._lock:
            self._global.discard(ws)
            for subs in self._per_agent.values():
                subs.discard(ws)

    async def broadcast(self, env: dict) -> None:
        """Push a `{event: "message", data: env}` frame to all subscribers
        of the global firehose and to per-agent subscribers whose id
        matches the envelope's sender or recipient."""
        msg = json.dumps({"event": "message", "data": env})
        targets = set(self._global)
        for who in (env.get("sender_id"), env.get("recipient_id")):
            if who:
                targets |= self._per_agent.get(who, set())
        await self._send_all(targets, msg)

    async def broadcast_event(self, event: str, data: dict, *,
                              agent_id: str | None = None) -> None:
        """Generic typed event (agent_status_change, agent_registered,
        log). `agent_id` selects which per-agent stream gets fan-out;
        global stream always gets it."""
        msg = json.dumps({"event": event, "data": data})
        targets = set(self._global)
        if agent_id:
            targets |= self._per_agent.get(agent_id, set())
        await self._send_all(targets, msg)

    async def _send_all(self, targets: Set[WebSocket], msg: str) -> None:
        if not targets:
            return
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_text(msg)
            except Exception:  # noqa: BLE001 — broad: closed sockets, RuntimeError, etc.
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._global.discard(ws)
                    for subs in self._per_agent.values():
                        subs.discard(ws)
            log.info("ws pruned %d dead subscriber(s)", len(dead))

    @property
    def stats(self) -> dict:
        return {
            "global_subscribers": len(self._global),
            "per_agent_subscribers": {k: len(v) for k, v in self._per_agent.items()},
        }
