"""Tests for MemoryService NATS handlers (mocks NATS client)."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from aggregator.memory import MemoryService, estimate_tokens
from aggregator import database as db


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    p = str(tmp_path / "t.db")
    monkeypatch.setenv("DB_PATH", p)
    db.init_db(p, wipe=True)
    yield p


def _msg(payload: dict) -> MagicMock:
    """Fake NATS Msg with .data and async .respond()."""
    m = MagicMock()
    m.data = json.dumps(payload).encode()
    m.respond = AsyncMock()
    return m


def test_estimate_tokens_byte_4_heuristic():
    assert estimate_tokens("") == 1  # floor
    assert estimate_tokens("hello world") == 11 // 4  # 2
    assert estimate_tokens("a" * 100) == 25


@pytest.mark.asyncio
async def test_put_then_get_roundtrip():
    svc = MemoryService(nc=AsyncMock())
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "gemma-1",
                "role": "user",
                "content": "hello",
            }
        )
    )
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "gemma-1",
                "role": "assistant",
                "content": "hi back",
            }
        )
    )

    get_msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1", "token_budget": 1000})
    await svc.on_get(get_msg)
    reply = json.loads(get_msg.respond.call_args.args[0])
    assert len(reply["turns"]) == 2
    # Returned chronologically (oldest first)
    assert reply["turns"][0]["content"] == "hello"
    assert reply["turns"][1]["content"] == "hi back"


@pytest.mark.asyncio
async def test_get_respects_token_budget():
    svc = MemoryService(nc=AsyncMock())
    # Insert 10 turns of ~100 tokens each (content ~400 bytes)
    big = "x" * 400
    for i in range(10):
        await svc.on_put(
            _msg(
                {
                    "context_id": "ctx-1",
                    "agent_id": "gemma-1",
                    "role": "user",
                    "content": big,
                }
            )
        )
    get_msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1", "token_budget": 250})
    await svc.on_get(get_msg)
    reply = json.loads(get_msg.respond.call_args.args[0])
    assert len(reply["turns"]) == 2  # 100 + 100 fits; third would exceed
    assert reply["total_tokens"] == 200


@pytest.mark.asyncio
async def test_get_filters_by_agent_id():
    svc = MemoryService(nc=AsyncMock())
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "gemma-1",
                "role": "user",
                "content": "from gemma",
            }
        )
    )
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "ag2-1",
                "role": "user",
                "content": "from ag2",
            }
        )
    )
    get_msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1", "token_budget": 1000})
    await svc.on_get(get_msg)
    reply = json.loads(get_msg.respond.call_args.args[0])
    assert len(reply["turns"]) == 1
    assert reply["turns"][0]["content"] == "from gemma"


@pytest.mark.asyncio
async def test_put_invalid_role_rejected():
    svc = MemoryService(nc=AsyncMock())
    msg = _msg(
        {"context_id": "ctx-1", "agent_id": "gemma-1", "role": "robot", "content": "x"}
    )
    await svc.on_put(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert reply == {"error": "invalid_role"}


@pytest.mark.asyncio
async def test_put_missing_field_rejected():
    svc = MemoryService(nc=AsyncMock())
    msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1"})  # no role/content
    await svc.on_put(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert reply == {"error": "missing_fields"}


@pytest.mark.asyncio
async def test_get_missing_fields_rejected():
    svc = MemoryService(nc=AsyncMock())
    msg = _msg({})
    await svc.on_get(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert "error" in reply


@pytest.mark.asyncio
async def test_delete_purges_only_target():
    svc = MemoryService(nc=AsyncMock())
    for ctx in ("a", "b"):
        await svc.on_put(
            _msg(
                {
                    "context_id": ctx,
                    "agent_id": "gemma-1",
                    "role": "user",
                    "content": "x",
                }
            )
        )
    msg = _msg({"context_id": "a"})
    await svc.on_delete(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert reply == {"deleted_count": 1}


@pytest.mark.asyncio
async def test_skill_id_persisted_in_put():
    svc = MemoryService(nc=AsyncMock())
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "gemma-1",
                "role": "user",
                "content": "x",
                "skill_id": "text.summarize",
            }
        )
    )
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-1")
    assert rows[0]["skill_id"] == "text.summarize"


@pytest.mark.asyncio
async def test_concurrent_writes_serialized():
    svc = MemoryService(nc=AsyncMock())
    msgs = [
        _msg(
            {
                "context_id": "c",
                "agent_id": "gemma-1",
                "role": "user",
                "content": f"turn-{i}",
            }
        )
        for i in range(10)
    ]
    await asyncio.gather(*(svc.on_put(m) for m in msgs))
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="c", limit=20)
    assert len(rows) == 10
    ids = sorted(r["id"] for r in rows)
    assert ids == list(range(ids[0], ids[0] + 10))  # contiguous monotonic


@pytest.mark.asyncio
async def test_get_token_budget_null_uses_default():
    """JSON null on token_budget falls back to DEFAULT_TOKEN_BUDGET, no crash."""
    svc = MemoryService(nc=AsyncMock())
    await svc.on_put(
        _msg(
            {
                "context_id": "ctx-1",
                "agent_id": "gemma-1",
                "role": "user",
                "content": "hi",
            }
        )
    )
    msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1", "token_budget": None})
    await svc.on_get(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert "turns" in reply  # responded with data, did not crash


@pytest.mark.asyncio
async def test_get_token_budget_non_numeric_rejected():
    """Non-numeric token_budget yields invalid_token_budget error envelope."""
    svc = MemoryService(nc=AsyncMock())
    msg = _msg({"context_id": "ctx-1", "agent_id": "gemma-1", "token_budget": "many"})
    await svc.on_get(msg)
    reply = json.loads(msg.respond.call_args.args[0])
    assert reply == {"error": "invalid_token_budget"}


@pytest.mark.asyncio
async def test_parse_non_dict_payload_treated_as_empty():
    """JSON list/scalar payload should be treated as missing, not crash."""
    svc = MemoryService(nc=AsyncMock())
    m = MagicMock()
    m.data = b"[1,2,3]"
    m.respond = AsyncMock()
    await svc.on_get(m)
    reply = json.loads(m.respond.call_args.args[0])
    assert "error" in reply  # missing_context_or_agent
