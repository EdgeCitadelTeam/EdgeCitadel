"""Memory service: NATS request-reply handlers + idle cleanup loop."""

from __future__ import annotations
import asyncio
import json
import logging

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from . import database as db

log = logging.getLogger(__name__)

DEFAULT_TOKEN_BUDGET = 2048
RETENTION_SEC = 30 * 24 * 3600  # 30-day hard delete
CLEANUP_INTERVAL_SEC = 300  # check every 5 min


def estimate_tokens(content: str) -> int:
    """Byte-length / 4 heuristic. Replaced by real tokenizer in v0.3+."""
    return max(1, len(content.encode("utf-8")) // 4)


class MemoryService:
    """Owns conversation_turns table reads/writes via NATS request-reply.

    Agent Plugins never touch the DB; they call:
      memory.turns.get    {context_id, agent_id, token_budget?}
      memory.turns.put    {context_id, agent_id, role, content, skill_id?}
      memory.turns.delete {context_id}
    """

    def __init__(self, nc: NATS) -> None:
        self.nc = nc
        self._cleanup_task: asyncio.Task | None = None

    async def start(self) -> None:
        await self.nc.subscribe("memory.turns.get", cb=self.on_get)
        await self.nc.subscribe("memory.turns.put", cb=self.on_put)
        await self.nc.subscribe("memory.turns.delete", cb=self.on_delete)
        self._cleanup_task = asyncio.create_task(self._idle_cleanup_loop())
        log.info("memory service started: subscribed to memory.turns.{get,put,delete}")

    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def on_get(self, msg: Msg) -> None:
        req = self._parse(msg.data) or {}
        ctx_id = req.get("context_id")
        agent = req.get("agent_id")
        raw_budget = req.get("token_budget")
        if raw_budget is None:
            budget = DEFAULT_TOKEN_BUDGET
        else:
            try:
                budget = int(raw_budget)
            except (TypeError, ValueError):
                await msg.respond(b'{"error":"invalid_token_budget"}')
                return
        if not (ctx_id and agent):
            await msg.respond(b'{"error":"missing_context_or_agent"}')
            return
        rows = db.fetch_recent_turns(agent_id=agent, context_id=ctx_id)
        kept: list[dict] = []
        total = 0
        for r in rows:
            if total + r["token_count"] > budget:
                break
            kept.append(
                {
                    "role": r["role"],
                    "content": r["content"],
                    "created_at": r["created_at"],
                    "skill_id": r["skill_id"],
                    "token_count": r["token_count"],
                }
            )
            total += r["token_count"]
        kept.reverse()  # chronological for the LLM
        await msg.respond(json.dumps({"turns": kept, "total_tokens": total}).encode())

    async def on_put(self, msg: Msg) -> None:
        req = self._parse(msg.data) or {}
        ctx_id = req.get("context_id")
        agent = req.get("agent_id")
        role = req.get("role")
        content = req.get("content")
        if not all((ctx_id, agent, role, content)):
            await msg.respond(b'{"error":"missing_fields"}')
            return
        if role not in ("user", "assistant", "system"):
            await msg.respond(b'{"error":"invalid_role"}')
            return
        tok = estimate_tokens(content)
        row_id = db.insert_turn(
            context_id=ctx_id,
            agent_id=agent,
            role=role,
            content=content,
            token_count=tok,
            skill_id=req.get("skill_id"),
        )
        await msg.respond(json.dumps({"id": row_id, "token_count": tok}).encode())

    async def on_delete(self, msg: Msg) -> None:
        req = self._parse(msg.data) or {}
        ctx_id = req.get("context_id")
        if not ctx_id:
            await msg.respond(b'{"error":"missing_context_id"}')
            return
        n = db.delete_turns_by_context(context_id=ctx_id)
        await msg.respond(json.dumps({"deleted_count": n}).encode())

    async def _idle_cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_SEC)
            try:
                n = db.purge_idle_contexts(idle_seconds=RETENTION_SEC)
                if n > 0:
                    log.info("memory: purged %d idle turns", n)
            except Exception as e:  # noqa: BLE001
                log.warning("memory: idle cleanup failed (%s): %s", type(e).__name__, e)

    def _parse(self, data: bytes) -> dict | None:
        try:
            result = json.loads(data)
        except json.JSONDecodeError:
            return None
        return result if isinstance(result, dict) else None
