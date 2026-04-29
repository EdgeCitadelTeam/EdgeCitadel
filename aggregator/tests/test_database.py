import os
import tempfile
import pytest
from aggregator import database as db


@pytest.fixture
def fresh_db():
    f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    f.close()
    os.environ["DB_PATH"] = f.name
    db.init_db(f.name)
    yield f.name
    os.unlink(f.name)


def test_schema_has_canonical_columns(fresh_db):
    cols = db.table_columns("messages")
    assert "recipient_id" in cols
    assert "type" in cols
    assert "task_id" in cols
    assert "context_id" in cols
    assert "task_state" in cols
    assert "agent_state" in cols
    assert "receiver_id" not in cols
    assert "message_type" not in cols


def test_wipe_on_first_boot_flag(tmp_path):
    """init_db(path, wipe=True) drops and recreates schema."""
    p = str(tmp_path / "openclaw.db")
    db.init_db(p)
    db.insert_message(dict(
        id="11111111-2222-4333-8444-555555555555",
        type="heartbeat", sender_id="shell-1",
        timestamp="2026-04-23T10:00:00.000Z", payload={}
    ))
    assert db.count_messages() == 1
    db.init_db(p, wipe=True)
    assert db.count_messages() == 0


def test_insert_and_retrieve_result(fresh_db):
    env = dict(
        id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        type="result", sender_id="gemma-1", recipient_id="shell-1",
        task_id="bbbbbbbb-cccc-4ddd-8eee-ffffffffffff",
        task_state="completed",
        timestamp="2026-04-23T10:00:05.000Z",
        payload={"body": "done"}
    )
    db.insert_message(env)
    rows = db.query_messages(agent_id="gemma-1")
    assert len(rows) == 1
    assert rows[0]["task_state"] == "completed"
    assert rows[0]["recipient_id"] == "shell-1"


def test_query_messages_deployment_filters(fresh_db):
    """deployment / exclude_deployment filters let the dashboard hide
    test traffic. See docs/roadmap.md test-data-separation convention."""
    base = dict(type="status", sender_id="shell-1",
                timestamp="2026-04-23T10:00:00.000Z",
                payload={"reason": "boot"}, agent_state="online")
    db.insert_message({**base, "id": "11111111-1111-4111-8111-111111111111"},
                      deployment="default")
    db.insert_message({**base, "id": "22222222-2222-4222-8222-222222222222"},
                      deployment="test")
    db.insert_message({**base, "id": "33333333-3333-4333-8333-333333333333"},
                      deployment="staging")

    # No filter → all three rows
    assert len(db.query_messages()) == 3

    # deployment="test" → only the test row
    rows = db.query_messages(deployment="test")
    assert len(rows) == 1 and rows[0]["deployment"] == "test"

    # exclude_deployment="test" → default + staging (2 rows)
    rows = db.query_messages(exclude_deployment="test")
    assert len(rows) == 2
    assert all(r["deployment"] != "test" for r in rows)
