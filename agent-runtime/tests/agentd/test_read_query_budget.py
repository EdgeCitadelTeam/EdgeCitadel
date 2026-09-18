import sqlite3

import pytest

from edgecitadel_agentd import store as store_module
from edgecitadel_agentd.store import AgentdStore, StoreError


@pytest.mark.parametrize("method", ["list_tasks", "list_traces", "get_trace"])
@pytest.mark.parametrize("budget", ["steps", "time"])
def test_read_interruption_is_explicit_and_connection_recovers(
    tmp_path, monkeypatch, method, budget
):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        store.record_span(
            trace_id="owned-trace", operation="owned", status="ok", agent_id="actor"
        )
        before = list(store._connection.iterdump())
        timeout = store._connection.execute("PRAGMA busy_timeout").fetchone()[0]
        with monkeypatch.context() as patch:
            patch.setattr(store_module, "READ_PROGRESS_STEPS", 1, raising=False)
            patch.setattr(
                store_module,
                "READ_MAX_STEPS" if budget == "steps" else "READ_MAX_SECONDS",
                0,
                raising=False,
            )
            with pytest.raises(StoreError, match="^read_query_budget_exceeded$"):
                getattr(store, method)(
                    *(["owned-trace"] if method == "get_trace" else [])
                )
        assert list(store._connection.iterdump()) == before
        assert store._connection.execute("PRAGMA busy_timeout").fetchone()[0] == timeout
        store.create_task(
            sender_id="origin", recipient_id="remote", payload={"body": "owned"}
        )
        assert len(store.list_tasks()) == 1
        assert len(store.list_traces()) == 1
        assert len(store.get_trace("owned-trace")["spans"]) == 1
    finally:
        store.close()


def test_busy_reader_reports_unavailable_and_restores_policy(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    other = sqlite3.connect(store.path, timeout=0)
    try:
        store._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        assert (
            store._connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            == "delete"
        )
        original = store._connection.execute("PRAGMA busy_timeout").fetchone()[0]
        other.execute("BEGIN EXCLUSIVE")
        with pytest.raises(StoreError, match="^read_query_unavailable$"):
            store.list_tasks()
        other.rollback()
        assert (
            store._connection.execute("PRAGMA busy_timeout").fetchone()[0] == original
        )
        assert store.list_tasks() == []
    finally:
        other.close()
        store.close()
