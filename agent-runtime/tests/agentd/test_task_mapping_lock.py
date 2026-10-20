"""Task response mapping must serialize every shared SQLite connection read."""

import pytest

from edgecitadel_agentd.store import AgentdStore


@pytest.mark.parametrize("method", ["get_task", "list_tasks"])
@pytest.mark.parametrize("held", [False, True])
def test_task_response_queries_own_connection_lock(tmp_path, monkeypatch, method, held):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        task = store.create_task(
            sender_id="sender", recipient_id="recipient", payload={"body": "echo"}
        )
        task_id = task["task_id"]
        if held:
            with store._lock, store._connection:
                store._connection.execute(
                    "INSERT INTO restore_holds VALUES ('task',?,'old-epoch',1)",
                    (task_id,),
                )
        connection = store._connection
        queries = []

        class CheckedConnection:
            def set_progress_handler(self, callback, steps):
                assert store._lock._is_owned(), (
                    "shared SQLite handler outside store lock"
                )
                return connection.set_progress_handler(callback, steps)

            def execute(self, sql, *args):
                assert store._lock._is_owned(), "shared SQLite read outside store lock"
                queries.append(sql)
                return connection.execute(sql, *args)

        with monkeypatch.context() as patch:
            patch.setattr(store, "_connection", CheckedConnection())
            result = (
                store.get_task(task_id)
                if method == "get_task"
                else store.list_tasks()[0]
            )
        assert result["payload"] == {"body": "echo"}
        assert result.get("restore_status") == (
            "reconciliation_required" if held else None
        )
        assert any("restore_holds" in query for query in queries)
    finally:
        store.close()
