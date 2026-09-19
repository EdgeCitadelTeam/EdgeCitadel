import json
import os
import sqlite3

import pytest

from edgecitadel_agentd import storage_workspace, trace_reservations
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_completed import materialize


@pytest.fixture
def installed(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_workspace, "WORKSPACE_BYTES", 256 * 1024)
    if not hasattr(os, "posix_fallocate"):
        monkeypatch.setattr(
            os,
            "posix_fallocate",
            lambda fd, offset, size: os.pwrite(fd, b"\0" * size, offset),
            raising=False,
        )
    (tmp_path / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    token = store.register_connector(
        connector_id="native", host_type="codex", agent_id="worker", capabilities=[]
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    store._connection.install_workspace(
        storage_workspace.CompletionWorkspace(store.path.parent / "completion.reserve")
    )
    try:
        yield store, token, session
    finally:
        store.close()


def running(installed):
    store, token, session = installed
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    store.transition_task(
        task_id=task["task_id"], state="running", actor_id="worker", session_id=session
    )
    return task


def complete(installed, task, **kwargs):
    store, _, session = installed
    return store.transition_task(
        task_id=task["task_id"],
        state="completed",
        actor_id="worker",
        session_id=session,
        result={"answer": "owned"},
        **kwargs,
    )


def snapshot(store):
    return {
        name: [
            tuple(row)
            for row in store._connection.execute(f"SELECT * FROM {name} ORDER BY 1")
        ]
        for name in (
            "tasks",
            "task_attempts",
            "transport_outbox",
            "events_all",
            "trace_journal_all",
            "trace_sources",
            "trace_export_generations",
            "trace_completion_slots",
        )
    }


def test_task_admission_refusal_leaves_no_task_or_transport(installed, monkeypatch):
    store, _, _ = installed
    monkeypatch.setattr(trace_reservations, "MAX_SLOTS", 1)
    store.create_task(sender_id="origin", recipient_id="remote", payload={})
    before = snapshot(store)
    with pytest.raises(StoreError, match="quota_exceeded"):
        store.create_task(sender_id="origin", recipient_id="remote", payload={})
    assert snapshot(store) == before


def test_terminal_state_event_and_delivery_survive_materialization_and_retry(installed):
    store, _, _ = installed
    task = running(installed)
    db = store._connection
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    # JSON escaping expands this valid reason sixfold; the whole envelope fits.
    reason = "\x01" * 1024
    result = complete(installed, task, reason=reason)
    assert db.execute("PRAGMA page_count").fetchone()[0] == pages
    event = store.get_trace(task["trace_id"])["events"][-1]
    assert event["event_type"] == "task.completed"
    assert event["attributes"] == {"reason": reason}
    assert (
        db.execute(
            "SELECT count(*) FROM events WHERE event_type='task.completed'"
        ).fetchone()[0]
        == 0
    )
    canonical = json.loads(
        db.execute(
            "SELECT event_json FROM trace_journal_all ORDER BY source_seq DESC LIMIT 1"
        ).fetchone()[0]
    )
    assert (
        canonical["phase"] == "completed"
        and canonical["attributes"]["reason"] == "unknown"
    )
    assert len(store.pending_transport()) == 1
    before = snapshot(store)
    assert complete(installed, task) == result
    assert snapshot(store) == before
    reopened = AgentdStore(store.path)
    try:
        assert reopened.get_trace(task["trace_id"]) == store.get_trace(task["trace_id"])
        assert reopened.get_task(task["task_id"]) == result
    finally:
        reopened.close()
    db.execute("PRAGMA max_page_count=1073741823")
    trace = store.get_trace(task["trace_id"])
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert materialize(db, 1)
    assert store.get_trace(task["trace_id"]) == trace
    assert complete(installed, task) == result


def test_legacy_event_failure_rolls_back_task_attempt_delivery_and_counters(installed):
    store, _, _ = installed
    task = running(installed)
    db = store._connection
    db.execute("""CREATE TRIGGER owned_legacy_failure BEFORE UPDATE ON trace_completion_slots
    WHEN json_extract(CAST(NEW.record AS TEXT),'$.legacy_event') IS NOT NULL
    BEGIN SELECT RAISE(ABORT,'owned legacy failure'); END""")
    before = snapshot(store)
    with pytest.raises(sqlite3.IntegrityError, match="owned legacy failure"):
        complete(installed, task)
    assert snapshot(store) == before
    db.execute("DROP TRIGGER owned_legacy_failure")
    assert complete(installed, task)["state"] == "completed"


def test_oversized_reason_is_rejected_before_state_change(installed):
    store, _, _ = installed
    task = running(installed)
    before = snapshot(store)
    with pytest.raises(StoreError, match="1024 UTF-8 bytes"):
        complete(installed, task, reason="é" * 513)
    assert snapshot(store) == before
