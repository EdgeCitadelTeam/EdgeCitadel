"""Tests for GET /api/conversations endpoint."""

import pytest
from fastapi.testclient import TestClient

from aggregator.main import make_app
from aggregator import database as db


@pytest.fixture
def client(tmp_path, envelope_schema_path, card_schema_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("ENVELOPE_SCHEMA_PATH", str(envelope_schema_path))
    monkeypatch.setenv("CARD_SCHEMA_PATH", str(card_schema_path))
    app = make_app(for_testing=True)
    with TestClient(app) as c:
        yield c


def test_conversations_empty(client):
    r = client.get("/api/conversations")
    assert r.status_code == 200
    assert r.json() == []


def test_conversations_grouped_by_context(client):
    db.insert_turn(
        context_id="ctx-A",
        agent_id="gemma-1",
        role="user",
        content="hi",
        token_count=1,
        skill_id="reasoning.chat",
    )
    db.insert_turn(
        context_id="ctx-A",
        agent_id="gemma-1",
        role="assistant",
        content="hello",
        token_count=2,
        skill_id="reasoning.chat",
    )
    db.insert_turn(
        context_id="ctx-B",
        agent_id="gemma-1",
        role="user",
        content="summary",
        token_count=2,
        skill_id="text.summarize",
    )
    body = client.get("/api/conversations").json()
    by_ctx = {r["context_id"]: r for r in body}
    assert set(by_ctx) == {"ctx-A", "ctx-B"}
    assert by_ctx["ctx-A"]["turns"] == 2
    assert by_ctx["ctx-A"]["tokens"] == 3
    assert "reasoning.chat" in by_ctx["ctx-A"]["skills"]


def test_conversations_filter_by_agent(client):
    db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="x",
        token_count=1,
        skill_id=None,
    )
    db.insert_turn(
        context_id="ctx-2",
        agent_id="ag2-1",
        role="user",
        content="y",
        token_count=1,
        skill_id=None,
    )
    body = client.get("/api/conversations?agent_id=gemma-1").json()
    assert {r["context_id"] for r in body} == {"ctx-1"}
