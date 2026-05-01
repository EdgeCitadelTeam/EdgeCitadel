"""Chaos test: memory service down → adapter still completes inference."""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from adapters.gemma.memory_client import fetch_memory, persist_memory


@pytest.mark.asyncio
async def test_fetch_memory_timeout_logs_warn_returns_empty(caplog):
    nc = AsyncMock()
    nc.request.side_effect = asyncio.TimeoutError()
    caplog.set_level("WARNING")
    turns = await fetch_memory(nc, context_id="ctx-1", agent_id="gemma-1")
    assert turns == []
    assert any("memory.turns.get" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_persist_memory_failure_logs_warn_does_not_raise(caplog):
    nc = AsyncMock()
    nc.request.side_effect = ConnectionError("nats down")
    caplog.set_level("WARNING")
    # Must not raise
    await persist_memory(nc, context_id="ctx-1", agent_id="gemma-1",
                         role="user", content="x")
    assert any("memory.turns.put" in rec.message for rec in caplog.records)
