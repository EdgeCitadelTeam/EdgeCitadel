import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from storage_test_support import flatten_connection

import pytest

import edgecitadel_agentd.trace_sync_manager as scheduling
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_exporter import ExportScope
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.fixture
def store(tmp_path):
    result = AgentdStore(tmp_path / "state.db")
    yield result
    result.close()


def add_source(store, node, *, selected=True, retired=False):
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize(node)
        if selected:
            journal.record(node, event, selected=True)
        if retired:
            store._connection.execute(
                "UPDATE trace_sources SET active=0 WHERE node_id=?", (node,)
            )
            store._connection.execute(
                "UPDATE trace_export_generations SET active=0 WHERE node_id=?", (node,)
            )
    return ExportScope(node, epoch, generation)


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.001)


def fast_slices(monkeypatch):
    monkeypatch.setattr(scheduling, "SLICE_SECONDS", 0.03)
    monkeypatch.setattr(scheduling, "DISCOVERY_SECONDS", 0.005)


def test_keyset_pages_include_retired_empty_and_paused_without_losing_cursor(store):
    scopes = [
        add_source(store, f"node-{i:02}", selected=i != 1, retired=i == 2)
        for i in range(19)
    ]
    with store._connection:
        store._connection.execute(
            "UPDATE trace_export_generations SET sync_fault='unsupported_version' WHERE node_id='node-00'"
        )
    after = ceiling = None
    seen = []
    while True:
        page, ceiling = scheduling.scope_page(store, after, ceiling)
        assert len(page) <= scheduling.MAX_ACTIVE_SCOPES
        if not page:
            break
        seen.extend(page)
        after = page[-1][0].values()
    assert [scope for scope, _ in seen] == scopes
    assert [eligible for _, eligible in seen[:3]] == [False, False, True]
    # An insertion behind the cursor appears after wraparound, not as a lost scope.
    added = add_source(store, "node-00a")
    first, _ = scheduling.scope_page(store, None, None)
    assert added in [scope for scope, _ in first]


@pytest.mark.asyncio
async def test_rotating_windows_bound_workers_and_eventually_serve_all_scopes(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    scopes = {add_source(store, f"node-{i:02}", retired=i < 12) for i in range(25)}
    active, seen = set(), set()
    maximum = 0

    class Worker:
        def __init__(self, store, js, nc, scope):
            self.scope, self.state = scope, "running"

        async def run(self, stop):
            nonlocal maximum
            assert self.scope not in active
            active.add(self.scope)
            seen.add(self.scope)
            maximum = max(maximum, len(active))
            try:
                await asyncio.Event().wait()
            finally:
                active.remove(self.scope)

    monkeypatch.setattr(scheduling, "TraceSyncWorker", Worker)
    manager = scheduling.TraceSyncManager(store, None, None)
    stop = asyncio.Event()
    runner = asyncio.create_task(manager.run(stop))
    try:
        await until(lambda: seen == scopes)
        assert maximum == 8
        with pytest.raises(RuntimeError, match="already_running"):
            await manager.run(stop)
    finally:
        stop.set()
        await asyncio.wait_for(runner, 1)
    assert not active and not manager.active and manager.state == "stopped"


@pytest.mark.asyncio
async def test_paused_fault_survives_rotation_and_reopen_until_explicit_retry(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    scope = add_source(store, "node-a")
    starts = []

    class Worker:
        def __init__(self, store, js, nc, scope):
            self.state, self.fault = "paused", "unsupported_version"

        async def run(self, stop):
            starts.append(1)

    monkeypatch.setattr(scheduling, "TraceSyncWorker", Worker)
    manager = scheduling.TraceSyncManager(store, None, None)
    stop = asyncio.Event()
    runner = asyncio.create_task(manager.run(stop))
    await until(lambda: len(starts) == 1)
    await asyncio.sleep(0.1)
    assert len(starts) == 1
    stop.set()
    await runner
    reopened = AgentdStore(store.path)
    try:
        assert scheduling.scope_page(reopened, None, None)[0] == [(scope, False)]
        scheduling.clear_scope_fault(reopened, scope)
        assert scheduling.scope_page(reopened, None, None)[0] == [(scope, True)]
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_second_owner_is_refused_and_cancellation_releases_lock(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    first = scheduling.TraceSyncManager(store, None, None)
    runner = asyncio.create_task(first.run(asyncio.Event()))
    await until(lambda: first.state == "running")
    second = scheduling.TraceSyncManager(store, None, None)
    await second.run(asyncio.Event())
    assert second.state == "paused" and second.fault == "sync_owner_active"
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
    stop = asyncio.Event()
    stop.set()
    await second.run(stop)
    assert second.state == "stopped"


@pytest.mark.asyncio
async def test_new_current_generation_is_discovered_after_retired_generation(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    old = add_source(store, "node-a", retired=True)
    db = sqlite3.connect(":memory:")
    initialize(db)

    async def publish(subject, data, **kwargs):
        ingest_wire(db, subject, data, received_at_ms=1)
        return SimpleNamespace(stream=STREAM_NAME)

    async def request(subject, data, **kwargs):
        return SimpleNamespace(
            data=json.dumps(settlement_page_reply(db, json.loads(data))).encode()
        )

    manager = scheduling.TraceSyncManager(
        store, SimpleNamespace(publish=publish), SimpleNamespace(request=request)
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(manager.run(stop))
    try:
        await until(
            lambda: (
                db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
            )
        )
        current = add_source(store, "node-a")
        assert current.source_epoch != old.source_epoch
        await until(
            lambda: (
                db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 2
            )
        )
        await until(
            lambda: (
                store._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                ).fetchone()[0]
                == 2
            )
        )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        stop.set()
        await runner
        db.close()


def test_schema_20_migration_rolls_back_atomically(store):
    add_source(store, "node-a")
    with store._connection:
        store._connection.execute(
            "ALTER TABLE trace_export_generations DROP COLUMN sync_fault"
        )
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=20")
    before = list(store._connection.iterdump())
    connections = []

    class FailingStore(AgentdStore):
        def _migrate_locked(self):
            super()._migrate_locked()
            connections.append(self._connection)
            raise RuntimeError("migration fault")

    try:
        with pytest.raises(RuntimeError, match="migration fault"):
            FailingStore(store.path)
    finally:
        for connection in connections:
            connection.close()
    assert list(store._connection.iterdump()) == before
    assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 20
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        assert (
            reopened._connection.execute(
                "SELECT sync_fault FROM trace_export_generations"
            ).fetchone()[0]
            is None
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_recovery_marker_activates_previously_empty_current_writer(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    from edgecitadel_agentd.trace_exporter import selected_batch
    from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request

    old = add_source(store, "node-a", retired=True)
    initial = sqlite3.connect(":memory:")
    initialize(initial)
    record = selected_batch(store, old)[0]
    ingest_wire(initial, record.subject, record.payload, received_at_ms=1)
    req = page_request(store, old.values())
    apply_page(store, req, settlement_page_reply(initial, req))
    initial.close()
    current = add_source(store, "node-a", selected=False)
    with store._connection:
        store._connection.execute("UPDATE trace_spool SET journal_event_id=NULL")
        store._connection.execute("DELETE FROM trace_journal")
    replacement = sqlite3.connect(":memory:")
    initialize(replacement)

    async def publish(subject, data, **kwargs):
        ingest_wire(replacement, subject, data, received_at_ms=2)
        return SimpleNamespace(stream=STREAM_NAME)

    async def request(subject, data, **kwargs):
        return SimpleNamespace(
            data=json.dumps(
                settlement_page_reply(replacement, json.loads(data))
            ).encode()
        )

    manager = scheduling.TraceSyncManager(
        store, SimpleNamespace(publish=publish), SimpleNamespace(request=request)
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(manager.run(stop))
    try:
        await until(
            lambda: (
                replacement.execute("SELECT count(*) FROM trace_raw_events").fetchone()[
                    0
                ]
                == 1
            )
        )
        await until(
            lambda: (
                store._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                ).fetchone()[0]
                == 2
            )
        )
        marker = json.loads(
            replacement.execute("SELECT event_json FROM trace_raw_events").fetchone()[0]
        )
        assert marker["source_epoch"] == current.source_epoch
        assert marker["attributes"]["affected_source_epoch"] == old.source_epoch
        assert marker["attributes"]["lost_ranges"] == [{"first": 1, "last": 1}]
        assert page_request(store, old.values())["after_export_seq"] == 1
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        stop.set()
        await runner
        replacement.close()


@pytest.mark.asyncio
async def test_fault_persistence_failure_pauses_manager_and_releases_all_workers(
    store, monkeypatch
):
    fast_slices(monkeypatch)
    add_source(store, "node-a")
    with store._connection:
        store._connection.execute(
            "CREATE TRIGGER fail_sync_fault BEFORE UPDATE OF sync_fault ON trace_export_generations BEGIN SELECT RAISE(ABORT, 'fault'); END"
        )

    class Worker:
        def __init__(self, *args):
            self.state, self.fault = "paused", "local_sync_fault"

        async def run(self, stop):
            return

    monkeypatch.setattr(scheduling, "TraceSyncWorker", Worker)
    manager = scheduling.TraceSyncManager(store, None, None)
    await asyncio.wait_for(manager.run(asyncio.Event()), 1)
    assert manager.state == "paused" and manager.fault == "local_scheduler_fault"
    assert not manager.active
