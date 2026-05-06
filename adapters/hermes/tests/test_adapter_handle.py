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


@pytest.mark.asyncio
async def test_handle_empty_body_rejected(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    env = cmd(body="   ")  # whitespace only

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_non_command_rejected(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    env = cmd(body="hi")
    env["type"] = "heartbeat"

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "unsupported_type"


@pytest.mark.asyncio
async def test_handle_missing_payload_rejected(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    env = cmd(body="hi")
    env.pop("payload")

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_hermes_failure_returns_failed(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    from adapters.hermes.hermes_client import HermesError
    env = cmd(body="hi")

    async def fake_call(**_):
        raise HermesError("http_500")

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        payload, state = await handle(env, fake_ctx)

    assert state == "failed"
    assert payload["error"] == "hermes_request_failed"
    assert "http_500" in payload["detail"]


@pytest.mark.asyncio
async def test_handle_hermes_connect_error_returns_failed(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    from adapters.hermes.hermes_client import HermesError
    env = cmd(body="hi")

    async def fake_call(**_):
        raise HermesError("ConnectError(refused)")

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        payload, state = await handle(env, fake_ctx)

    assert state == "failed"
    assert payload["error"] == "hermes_request_failed"


@pytest.mark.asyncio
async def test_handle_does_not_publish_to_memory_turns(fake_ctx, cmd):
    """Regression guard for ADR-0009: bridge adapters MUST NOT call
    memory.turns.{get,put,delete}."""
    from adapters.hermes.adapter import handle
    env = cmd(body="remember teal", context_id="ctx-test")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("ok")
        return "ok"

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        await handle(env, fake_ctx)

    # nc.publish must never be called with a memory.turns.* subject
    publish_calls = fake_ctx.nc.publish.await_args_list
    request_calls = getattr(fake_ctx.nc, "request", AsyncMock()).await_args_list
    for call in list(publish_calls) + list(request_calls):
        subject = call.args[0] if call.args else call.kwargs.get("subject", "")
        assert not subject.startswith("memory.turns."), (
            f"Bridge adapter must not touch memory.turns.* (got {subject})")


@pytest.mark.asyncio
async def test_handle_publishes_progress_with_upstream_tag(fake_ctx, cmd):
    from adapters.hermes.adapter import handle
    env = cmd(body="long prompt", task_id="t-1")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("first-")
        await publish_progress("second")
        return "first-second"

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        await handle(env, fake_ctx)

    assert len(fake_ctx.progress_calls) == 2
    for call in fake_ctx.progress_calls:
        assert call["task_id"] == "t-1"
        assert call["extra"]["upstream"] == "hermes-agent"
    assert fake_ctx.progress_calls[0]["body"] == "first-"
    assert fake_ctx.progress_calls[1]["body"] == "second"


@pytest.mark.asyncio
async def test_handle_skips_progress_publish_when_no_task_id(fake_ctx, cmd):
    """If the inbound command has no task_id, suppress task.progress
    publishes (the dashboard would have no key to bind them to)."""
    from adapters.hermes.adapter import handle
    env = cmd(body="hi")
    env["task_id"] = ""

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("delta")
        return "delta"

    with patch("adapters.hermes.adapter.call_hermes_streaming",
               side_effect=fake_call):
        await handle(env, fake_ctx)

    assert fake_ctx.progress_calls == []
