import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_collector_recovery import begin_recovery
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from edgecitadel_agentd.trace_settlement_poll import SettlementPoller
from edgecitadel_agentd.trace_sync import TraceSyncWorker
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.fixture
def source(tmp_path):
    store = AgentdStore(tmp_path / "source.db")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize("edge-a")
        journal.record("edge-a", event, selected=True)
    try:
        yield store, ExportScope("edge-a", epoch, generation)
    finally:
        store.close()


@pytest.fixture
def core(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    db = sqlite3.connect(tmp_path / "core.db")
    initialize(db)
    yield db, ingest_wire, settlement_page_reply
    db.close()


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(0.005)


def control(db, reply, *, observed=None):
    async def request(subject, data, **kwargs):
        result = reply(db, json.loads(data))
        if observed is not None:
            observed.append(result)
        return SimpleNamespace(data=json.dumps(result).encode())

    return SimpleNamespace(request=AsyncMock(side_effect=request))


def publisher(db, ingest):
    async def publish(subject, data, **kwargs):
        ingest(db, subject, data, received_at_ms=1)
        return SimpleNamespace(stream=STREAM_NAME)

    return SimpleNamespace(publish=AsyncMock(side_effect=publish))


def fast_poll(monkeypatch):
    schedule = SettlementPoller._schedule
    monkeypatch.setattr(
        SettlementPoller, "_schedule", lambda self, delay: schedule(self, 0.01)
    )


@pytest.mark.asyncio
async def test_unknown_source_replays_broker_acked_identity_and_settles(
    source, core, monkeypatch
):
    store, scope = source
    db, ingest, reply = core
    fast_poll(monkeypatch)
    with store._connection:
        store._connection.execute("UPDATE trace_spool SET state='broker_acked'")
    observed = []
    worker = TraceSyncWorker(
        store, publisher(db, ingest), control(db, reply, observed=observed), scope
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    try:
        await until(
            lambda: page_request(store, scope.values())["after_export_seq"] == 1
        )
        assert any(r.get("code") == "unknown_source" for r in observed)
        assert db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 1
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, 1)
    assert worker.state == "stopped"


@pytest.mark.parametrize("resume", [False, True])
@pytest.mark.asyncio
async def test_collector_change_recovers_and_replays_without_manual_trigger(
    source, core, monkeypatch, resume
):
    store, scope = source
    db, ingest, reply = core
    fast_poll(monkeypatch)
    record = selected_batch(store, scope)[0]
    ingest(db, record.subject, record.payload, received_at_ms=1)
    request = page_request(store, scope.values())
    apply_page(store, request, reply(db, request))
    old_epoch = page_request(store, scope.values())["collector_epoch"]
    # A distinct initialized Core represents an empty replacement collector.
    from aggregator.trace_store import initialize

    replacement = sqlite3.connect(":memory:")
    initialize(replacement)
    if resume:
        begin_recovery(store, scope.values(), expected_epoch=old_epoch)
        store.close()
        store = AgentdStore(store.path)
    observed = []
    worker = TraceSyncWorker(
        store,
        publisher(replacement, ingest),
        control(replacement, reply, observed=observed),
        scope,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    try:
        await until(
            lambda: (
                (
                    store._connection.execute(
                        "SELECT phase FROM trace_collector_recovery"
                    ).fetchone()
                    or [None]
                )[0]
                == "live"
            )
        )
        assert page_request(store, scope.values())["collector_epoch"] != old_epoch
        assert (
            replacement.execute("SELECT event_id FROM trace_raw_events").fetchone()[0]
            == json.loads(record.payload)["event"]["event_id"]
        )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
        if not resume:
            assert any(r.get("code") == "collector_changed" for r in observed)
    finally:
        stop.set()
        await asyncio.wait_for(task, 1)
        replacement.close()
        if resume:
            store.close()


@pytest.mark.asyncio
async def test_control_wait_does_not_block_publishing_and_stop_cancels_both(source):
    store, scope = source
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def request(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    js = SimpleNamespace(
        publish=AsyncMock(return_value=SimpleNamespace(stream=STREAM_NAME))
    )
    worker = TraceSyncWorker(store, js, SimpleNamespace(request=request), scope)
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    await asyncio.wait_for(entered.wait(), 1)
    await until(lambda: js.publish.call_count == 1)
    with pytest.raises(RuntimeError, match="already_running"):
        await worker.run(stop)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert cancelled.is_set()
    assert page_request(store, scope.values())["after_export_seq"] == 0
    assert worker.poller._inflight.done()


@pytest.mark.asyncio
async def test_permanent_control_fault_stops_publisher_without_retiring(source):
    store, scope = source
    cancelled = asyncio.Event()

    async def publish(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def request(subject, data, **kwargs):
        await asyncio.sleep(0)
        return SimpleNamespace(
            data=json.dumps(
                {
                    "schema_version": 2,
                    "request_id": json.loads(data)["request_id"],
                    "status": "error",
                    "code": "unsupported_version",
                    "retry_after_ms": 500,
                }
            ).encode()
        )

    worker = TraceSyncWorker(
        store, SimpleNamespace(publish=publish), SimpleNamespace(request=request), scope
    )
    await asyncio.wait_for(worker.run(asyncio.Event()), 1)
    assert worker.state == "paused" and worker.fault == "unsupported_version"
    assert cancelled.is_set()
    assert page_request(store, scope.values())["after_export_seq"] == 0


@pytest.mark.asyncio
async def test_recovery_cancels_publish_before_resetting_spool(
    source, core, monkeypatch
):
    store, scope = source
    db, ingest, reply = core
    fast_poll(monkeypatch)
    original = selected_batch(store, scope)[0]
    ingest(db, original.subject, original.payload, received_at_ms=1)
    request = page_request(store, scope.values())
    apply_page(store, request, reply(db, request))
    event = json.loads(original.payload)["event"]
    for key in ("node_id", "source_epoch", "source_seq"):
        event.pop(key, None)
    event["event_id"] = str(uuid4())
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        TraceJournal(store._connection).record(scope.node_id, event, selected=True)
    started, cancelled = asyncio.Event(), asyncio.Event()
    calls = 0

    async def publish(subject, data, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        return SimpleNamespace(stream=STREAM_NAME)

    async def request(subject, data, **kwargs):
        await started.wait()
        req = json.loads(data)
        if req["collector_epoch"] is not None:
            return SimpleNamespace(
                data=json.dumps(
                    {
                        "schema_version": 2,
                        "request_id": req["request_id"],
                        "status": "error",
                        "code": "collector_changed",
                        "retry_after_ms": 500,
                    }
                ).encode()
            )
        await asyncio.Event().wait()

    import edgecitadel_agentd.trace_sync as sync

    begin = sync.begin_recovery

    def checked_begin(*args, **kwargs):
        assert cancelled.is_set()
        begin(*args, **kwargs)

    monkeypatch.setattr(sync, "begin_recovery", checked_begin)
    worker = TraceSyncWorker(
        store, SimpleNamespace(publish=publish), SimpleNamespace(request=request), scope
    )
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    try:
        await until(lambda: calls >= 3)
        assert cancelled.is_set()
        assert page_request(store, scope.values())["after_export_seq"] == 0
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, 1)
