"""Unit tests for conversation_turns DB helpers (no NATS, no FastAPI)."""

import time
import pytest

from aggregator import database as db


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    p = str(tmp_path / "t.db")
    monkeypatch.setenv("DB_PATH", p)
    db.init_db(p, wipe=True)
    yield p


def test_insert_and_fetch_returns_newest_first(fresh_db):
    db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="hello",
        token_count=2,
        skill_id="reasoning.chat",
    )
    time.sleep(0.01)
    db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="assistant",
        content="hi back",
        token_count=2,
        skill_id="reasoning.chat",
    )
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-1")
    assert len(rows) == 2
    # Newest first
    assert rows[0]["content"] == "hi back"
    assert rows[1]["content"] == "hello"


def test_fetch_filters_by_agent_id(fresh_db):
    db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="for gemma",
        token_count=2,
        skill_id=None,
    )
    db.insert_turn(
        context_id="ctx-1",
        agent_id="ag2-1",
        role="user",
        content="for ag2",
        token_count=2,
        skill_id=None,
    )
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-1")
    assert len(rows) == 1
    assert rows[0]["content"] == "for gemma"


def test_delete_turns_by_context(fresh_db):
    for ctx in ("ctx-keep", "ctx-drop"):
        db.insert_turn(
            context_id=ctx,
            agent_id="gemma-1",
            role="user",
            content="x",
            token_count=1,
            skill_id=None,
        )
    n = db.delete_turns_by_context(context_id="ctx-drop")
    assert n == 1
    remaining = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-keep")
    assert len(remaining) == 1


def test_purge_idle_contexts_drops_old_rows(fresh_db):
    # Insert with backdated created_at by raw SQL
    import sqlite3

    conn = sqlite3.connect(fresh_db)
    conn.execute(
        "INSERT INTO conversation_turns "
        "(context_id, agent_id, role, content, token_count, created_at) "
        "VALUES ('ctx-old', 'gemma-1', 'user', 'old', 1, ?)",
        (time.time() - 31 * 24 * 3600,),
    )  # 31 days ago
    conn.execute(
        "INSERT INTO conversation_turns "
        "(context_id, agent_id, role, content, token_count, created_at) "
        "VALUES ('ctx-new', 'gemma-1', 'user', 'new', 1, ?)",
        (time.time(),),
    )
    conn.commit()
    conn.close()

    deleted = db.purge_idle_contexts(idle_seconds=30 * 24 * 3600)
    assert deleted == 1
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-old")
    assert rows == []
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-new")
    assert len(rows) == 1


def test_skill_id_persisted_when_provided(fresh_db):
    rid = db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="x",
        token_count=1,
        skill_id="text.summarize",
    )
    assert rid > 0
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-1")
    assert rows[0]["skill_id"] == "text.summarize"


def test_skill_id_null_when_omitted(fresh_db):
    db.insert_turn(
        context_id="ctx-1",
        agent_id="gemma-1",
        role="user",
        content="x",
        token_count=1,
        skill_id=None,
    )
    rows = db.fetch_recent_turns(agent_id="gemma-1", context_id="ctx-1")
    assert rows[0]["skill_id"] is None


def test_list_conversations_grouped(fresh_db):
    for role in ("user", "assistant"):
        db.insert_turn(
            context_id="ctx-A",
            agent_id="gemma-1",
            role=role,
            content="x",
            token_count=2,
            skill_id="reasoning.chat",
        )
    db.insert_turn(
        context_id="ctx-B",
        agent_id="gemma-1",
        role="user",
        content="y",
        token_count=3,
        skill_id="text.summarize",
    )
    rows = db.list_conversations()
    by_ctx = {r["context_id"]: r for r in rows}
    assert by_ctx["ctx-A"]["turns"] == 2
    assert by_ctx["ctx-A"]["tokens"] == 4
    assert "reasoning.chat" in by_ctx["ctx-A"]["skills"]
    assert by_ctx["ctx-B"]["turns"] == 1


def test_list_conversations_filter_by_agent(fresh_db):
    db.insert_turn(
        context_id="ctx-A",
        agent_id="gemma-1",
        role="user",
        content="x",
        token_count=1,
        skill_id=None,
    )
    db.insert_turn(
        context_id="ctx-B",
        agent_id="ag2-1",
        role="user",
        content="y",
        token_count=1,
        skill_id=None,
    )
    rows = db.list_conversations(agent_id="gemma-1")
    assert {r["context_id"] for r in rows} == {"ctx-A"}
