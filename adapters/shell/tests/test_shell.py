import asyncio, pytest
from unittest.mock import MagicMock
from adapters.shell.adapter import handle
from adapters._common.pull_consumer import Context


@pytest.mark.asyncio
async def test_handle_echo():
    env = {"v": 1, "id": "i", "type": "command",
           "sender_id": "tester", "recipient_id": "shell-1",
           "task_id": "t", "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "echo hello"}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)   # stub
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "completed"
    assert "hello" in payload["body"]


@pytest.mark.asyncio
async def test_handle_timeout():
    env = {"v": 1, "id": "i", "type": "command",
           "sender_id": "tester", "recipient_id": "shell-1",
           "task_id": "t", "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "sleep 99", "args": {"timeout_sec": 1}}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "failed"
    assert payload["error"] == "timeout"


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"v": 1, "id": "i", "type": "delegation",
           "sender_id": "planner-1", "recipient_id": "shell-1",
           "task_id": "t", "context_id": "c", "hop_count": 0,
           "timestamp": "2026-04-23T10:00:00.000Z",
           "payload": {"body": "ignored"}}
    ctx = Context(agent_id="shell-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    payload, state = await handle(env, ctx)
    assert state == "rejected"
