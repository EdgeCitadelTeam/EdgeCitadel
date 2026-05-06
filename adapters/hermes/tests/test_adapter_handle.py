"""Unit tests for the Hermes adapter command handler."""
from __future__ import annotations
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def _hermes_env(monkeypatch):
    monkeypatch.setenv("HERMES_BASE_URL", "http://localhost:8642")
    monkeypatch.setenv("HERMES_TOKEN", "test-token")
    monkeypatch.setenv("HERMES_MODEL", "hermes-test")
    monkeypatch.setenv("HERMES_TIMEOUT_SEC", "10")


@pytest.mark.asyncio
async def test_handle_command_returns_completed(fake_ctx, cmd):
    from adapters.hermes.adapter import handle

    env = cmd(body="hello hermes")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("delta-")
        await publish_progress("rest")
        return "delta-rest"

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        payload, state = await handle(env, fake_ctx)

    assert state == "completed"
    assert payload["body"] == "delta-rest"
    assert payload["upstream"] == "hermes-agent"
