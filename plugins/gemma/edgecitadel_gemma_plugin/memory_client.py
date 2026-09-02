"""Managed Agent wrappers around agentd's memory service proxy.

Best-effort by design: a memory service timeout or failure logs WARN and
returns/yields empty data — the caller proceeds statelessly, never raising.
"""

from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional

from nats.aio.client import Client as NATS

log = logging.getLogger(__name__)

DEFAULT_TOKEN_BUDGET = 2048
DEFAULT_TIMEOUT_SEC = 2.0


async def fetch_memory(
    nc: NATS,
    *,
    context_id: Optional[str],
    agent_id: str,
    token_budget: int = DEFAULT_TOKEN_BUDGET,
    timeout: float = DEFAULT_TIMEOUT_SEC,
) -> list[dict]:
    """Return prior turns in chronological order, trimmed to fit budget.

    Returns [] if context_id is None/empty (first turn of a new conversation)
    or if the memory service is unreachable. Never raises."""
    if not context_id:
        return []
    payload = json.dumps(
        {"context_id": context_id, "agent_id": agent_id, "token_budget": token_budget}
    ).encode()
    try:
        msg = await nc.request("memory.turns.get", payload, timeout=timeout)
    except (asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
        log.warning(
            "memory.turns.get failed (%s); proceeding stateless", type(e).__name__
        )
        return []
    try:
        body = json.loads(msg.data) or {}
    except json.JSONDecodeError:
        return []
    return body.get("turns", []) or []


async def persist_memory(
    nc: NATS,
    *,
    context_id: Optional[str],
    agent_id: str,
    role: str,
    content: str,
    skill_id: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT_SEC,
) -> None:
    """Persist a single turn. Logs WARN on failure; never raises."""
    if not context_id:
        return
    payload = json.dumps(
        {
            "context_id": context_id,
            "agent_id": agent_id,
            "role": role,
            "content": content,
            "skill_id": skill_id,
        }
    ).encode()
    try:
        await nc.request("memory.turns.put", payload, timeout=timeout)
    except (asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
        log.warning(
            "memory.turns.put failed (%s); turn not persisted", type(e).__name__
        )
