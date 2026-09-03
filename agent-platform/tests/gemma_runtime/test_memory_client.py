"""Unit tests for memory_client wrapper around NATS request-reply."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from edgecitadel_gemma_plugin.memory_client import fetch_memory, persist_memory


def _reply(payload: dict) -> MagicMock:
    m = MagicMock()
    m.data = json.dumps(payload).encode()
    return m


@pytest.mark.asyncio
async def test_fetch_memory_returns_turns():
    nc = AsyncMock()
    nc.request.return_value = _reply(
        {"turns": [{"role": "user", "content": "hi"}], "total_tokens": 1}
    )
    turns = await fetch_memory(nc, context_id="ctx-1", agent_id="gemma-1")
    assert turns == [{"role": "user", "content": "hi"}]


@pytest.mark.asyncio
async def test_fetch_memory_empty_context_id_short_circuits():
    nc = AsyncMock()
    turns = await fetch_memory(nc, context_id=None, agent_id="gemma-1")
    assert turns == []
    nc.request.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_memory_timeout_returns_empty():
    nc = AsyncMock()
    nc.request.side_effect = asyncio.TimeoutError()
    turns = await fetch_memory(nc, context_id="ctx-1", agent_id="gemma-1")
    assert turns == []


@pytest.mark.asyncio
async def test_persist_memory_failure_does_not_raise():
    nc = AsyncMock()
    nc.request.side_effect = ConnectionError("nats down")
    # Must not raise
    await persist_memory(
        nc, context_id="ctx-1", agent_id="gemma-1", role="user", content="x"
    )


@pytest.mark.asyncio
async def test_persist_memory_passes_skill_id():
    nc = AsyncMock()
    nc.request.return_value = _reply({"id": 1, "token_count": 2})
    await persist_memory(
        nc,
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="x",
        skill_id="text.summarize",
    )
    call_payload = json.loads(nc.request.call_args.args[1])
    assert call_payload["skill_id"] == "text.summarize"
