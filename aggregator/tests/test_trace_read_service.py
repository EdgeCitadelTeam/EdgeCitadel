import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest
from test_trace_projection_store import core as core_fixture

from aggregator import trace_read_service as module
from aggregator.trace_event_pages import TraceReadError

core = core_fixture


def service(core, tmp_path):
    return module.TraceReadService(
        Path(core.execute("PRAGMA database_list").fetchone()[2]),
        tmp_path / "key",
    )


def test_canceling_waiter_keeps_actual_worker_slot_until_connection_closes(
    core, tmp_path, monkeypatch
):
    release, started = threading.Event(), threading.Event()
    guard = threading.Lock()
    calls = 0
    closed_in = []
    original_connect = sqlite3.connect

    class ObservedConnection(sqlite3.Connection):
        def close(self):
            closed_in.append(threading.get_ident())
            super().close()

    monkeypatch.setattr(
        module.sqlite3,
        "connect",
        lambda *a, **kw: original_connect(*a, **kw, factory=ObservedConnection),
    )

    def blocked(connection, **kwargs):
        nonlocal calls
        with guard:
            calls += 1
            if calls == module.MAX_READERS:
                started.set()
        assert release.wait(5)
        return {}

    monkeypatch.setattr(module.trace_list_pages, "read_list", blocked)
    reader = service(core, tmp_path)

    async def run():
        tasks = [
            asyncio.create_task(reader.query("list")) for _ in range(module.MAX_READERS)
        ]
        try:
            assert await asyncio.to_thread(started.wait, 5)
            with pytest.raises(TraceReadError, match="unavailable"):
                await reader.query("list")
            tasks[0].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[0]
            with pytest.raises(TraceReadError, match="unavailable"):
                await reader.query("list")
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.to_thread(reader.close)

    asyncio.run(run())
    assert len(closed_in) == module.MAX_READERS
    assert all(identity != threading.get_ident() for identity in closed_in)
    with pytest.raises(TraceReadError, match="unavailable"):
        asyncio.run(reader.query("list"))


def test_query_deadline_interrupts_sql_and_does_not_poison_reader(
    core, tmp_path, monkeypatch
):
    monkeypatch.setattr(module, "QUERY_TIMEOUT_SECONDS", 0.01)
    original = module.trace_list_pages.read_list

    def expensive(connection, **kwargs):
        connection.execute(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000) SELECT sum(x) FROM c"
        ).fetchone()
        pytest.fail("query deadline did not interrupt SQL")

    monkeypatch.setattr(module.trace_list_pages, "read_list", expensive)
    reader = service(core, tmp_path)
    try:
        with pytest.raises(TraceReadError, match="^unavailable$"):
            asyncio.run(reader.query("list"))
        monkeypatch.setattr(module.trace_list_pages, "read_list", original)
        assert asyncio.run(reader.query("list"))["items"] == []
    finally:
        reader.close()


def test_absent_core_is_unavailable_without_creating_a_database(tmp_path):
    path = tmp_path / "missing.db"
    reader = module.TraceReadService(path, tmp_path / "key")
    try:
        with pytest.raises(TraceReadError, match="^unavailable$"):
            asyncio.run(reader.query("list"))
        assert not path.exists()
    finally:
        reader.close()
