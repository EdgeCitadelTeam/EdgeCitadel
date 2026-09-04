"""Unit tests for the Hermes Plugin command handler."""

from __future__ import annotations
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def _hermes_env(monkeypatch, tmp_path):
    token_file = tmp_path / "hermes-token"
    token_file.write_text("test-token\n")
    monkeypatch.setenv("HERMES_BASE_URL", "http://localhost:8642")
    monkeypatch.setenv("HERMES_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("HERMES_MODEL", "hermes-test")
    monkeypatch.setenv("HERMES_TIMEOUT_SEC", "10")


@pytest.mark.asyncio
async def test_handle_command_returns_completed(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="hello hermes")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("delta-")
        await publish_progress("rest")
        return "delta-rest"

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        payload, state = await handle(env, fake_ctx)

    assert state == "completed"
    assert payload["body"] == "delta-rest"
    assert payload["upstream"] == "hermes-agent"


@pytest.mark.asyncio
async def test_handle_empty_body_rejected(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="   ")  # whitespace only

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_non_command_rejected(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="hi")
    env["type"] = "heartbeat"

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "unsupported_type"


@pytest.mark.asyncio
async def test_handle_missing_payload_rejected(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="hi")
    env.pop("payload")

    payload, state = await handle(env, fake_ctx)

    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_hermes_failure_returns_failed(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle
    from edgecitadel_hermes_plugin.hermes_client import HermesError

    env = cmd(body="hi")

    async def fake_call(**_):
        raise HermesError("http_500")

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        payload, state = await handle(env, fake_ctx)

    assert state == "failed"
    assert payload["error"] == "hermes_request_failed"
    assert "http_500" in payload["detail"]


@pytest.mark.asyncio
async def test_handle_hermes_connect_error_returns_failed(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle
    from edgecitadel_hermes_plugin.hermes_client import HermesError

    env = cmd(body="hi")

    async def fake_call(**_):
        raise HermesError("ConnectError(refused)")

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        payload, state = await handle(env, fake_ctx)

    assert state == "failed"
    assert payload["error"] == "hermes_request_failed"


@pytest.mark.asyncio
async def test_handle_does_not_publish_to_memory_turns(fake_ctx, cmd):
    """Regression guard for ADR-0009: bridge Plugins MUST NOT call
    memory.turns.{get,put,delete}."""
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="remember teal", context_id="ctx-test")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("ok")
        return "ok"

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        await handle(env, fake_ctx)

    for call in list(fake_ctx.nc.publish.await_args_list) + list(
        fake_ctx.nc.request.await_args_list
    ):
        subject = call.args[0] if call.args else call.kwargs.get("subject", "")
        assert not subject.startswith("memory.turns."), (
            f"Bridge Plugin must not touch memory.turns.* (got {subject})"
        )


@pytest.mark.asyncio
async def test_handle_publishes_progress_with_upstream_tag(fake_ctx, cmd):
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="long prompt", task_id="t-1")

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("first-")
        await publish_progress("second")
        return "first-second"

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        await handle(env, fake_ctx)

    assert len(fake_ctx.progress_calls) == 2
    for call in fake_ctx.progress_calls:
        assert call["task_id"] == "t-1"
        assert call["extra"]["upstream"] == "hermes-agent"
    assert fake_ctx.progress_calls[0]["body"] == "first-"
    assert fake_ctx.progress_calls[1]["body"] == "second"


@pytest.mark.asyncio
async def test_main_runs_through_managed_agent_runtime():
    from edgecitadel_hermes_plugin import adapter

    with (
        patch(
            "edgecitadel_hermes_plugin.adapter.preflight",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "edgecitadel_hermes_plugin.adapter.run_managed_agent",
            new=AsyncMock(return_value=None),
        ) as managed,
    ):
        await adapter.main()
    managed.assert_awaited_once_with(adapter.CONFIG_PATH, adapter.handle)


@pytest.mark.asyncio
async def test_handle_skips_progress_publish_when_no_task_id(fake_ctx, cmd):
    """If the inbound command has no task_id, suppress task.progress
    publishes (the dashboard would have no key to bind them to)."""
    from edgecitadel_hermes_plugin.adapter import handle

    env = cmd(body="hi")
    env["task_id"] = ""

    async def fake_call(*, prompt, session_id, publish_progress):
        await publish_progress("delta")
        return "delta"

    with patch(
        "edgecitadel_hermes_plugin.adapter.call_hermes_streaming", side_effect=fake_call
    ):
        await handle(env, fake_ctx)

    assert fake_ctx.progress_calls == []
