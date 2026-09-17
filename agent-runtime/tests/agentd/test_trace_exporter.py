import asyncio
import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import APIError

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_exporter import (
    ExportScope,
    TraceExporter,
    checkpoint_broker_ack,
    selected_batch,
)
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.fixture
def source(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        epoch, generation = TraceJournal(store._connection).initialize("edge-a")
    try:
        yield store, ExportScope("edge-a", epoch, generation), event
    finally:
        store.close()


def write(source, *, selected=True):
    store, _, event = source
    value = deepcopy(event)
    value["event_id"] = str(uuid4())
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return TraceJournal(store._connection).record(
            "edge-a", value, selected=selected
        )


def state(store):
    return store._connection.execute(
        "SELECT state FROM trace_spool ORDER BY export_seq"
    ).fetchone()[0]


def test_committed_selected_keyset_batch_and_restart_identity(source):
    store, scope, _ = source
    write(source, selected=False)
    for _ in range(35):
        write(source)
    batch = selected_batch(store, scope)
    assert len(batch) == 32
    assert json.loads(batch[0].payload)["event"]["source_seq"] == 2
    assert [r.export_seq for r in selected_batch(store, scope, after=32)] == [
        33,
        34,
        35,
    ]
    reopened = AgentdStore(store.path)
    try:
        assert selected_batch(reopened, scope)[0] == batch[0]
        assert selected_batch(reopened, scope)[0].message_id == batch[0].message_id
    finally:
        reopened.close()
    store._connection.execute("BEGIN IMMEDIATE")
    with pytest.raises(TraceContractError, match="committed_spool"):
        selected_batch(store, scope)
    with pytest.raises(TraceContractError, match="committed_spool"):
        checkpoint_broker_ack(store, batch[0])
    assert store._connection.in_transaction
    store._connection.rollback()


@pytest.mark.asyncio
async def test_ack_keeps_payload_and_replay_uses_identical_id(source):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(return_value=SimpleNamespace(stream=STREAM_NAME))
    )
    exporter = TraceExporter(store, js, scope)
    assert await exporter.publish_batch() == 1
    assert state(store) == "broker_acked"
    assert selected_batch(store, scope) == []
    assert len(selected_batch(store, scope, replay=True)) == 1
    assert await exporter.publish_batch(replay=True) == 1
    assert js.publish.call_args_list[0] == js.publish.call_args_list[1]
    assert (
        store._connection.execute("SELECT collector_epoch FROM trace_spool").fetchone()[
            0
        ]
        is None
    )
    assert (
        store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        == 1
    )


@pytest.mark.asyncio
async def test_lost_publish_ack_preserves_pending_and_stable_retry(source):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(
            side_effect=[NatsTimeoutError(), SimpleNamespace(stream=STREAM_NAME)]
        )
    )
    exporter = TraceExporter(store, js, scope)
    with pytest.raises(NatsTimeoutError):
        await exporter.publish_batch()
    assert state(store) == "pending"
    await exporter.publish_batch()
    assert js.publish.call_args_list[0] == js.publish.call_args_list[1]
    assert state(store) == "broker_acked"


@pytest.mark.parametrize("late_state", ["lost_with_marker", "core_settled"])
@pytest.mark.asyncio
async def test_late_ack_does_not_overwrite_retention_or_settlement(source, late_state):
    store, scope, _ = source
    write(source)

    async def publish(*args, **kwargs):
        with store._connection:
            store._connection.execute("UPDATE trace_spool SET state=?", (late_state,))
        return SimpleNamespace(stream=STREAM_NAME)

    await TraceExporter(store, SimpleNamespace(publish=publish), scope).publish_batch()
    assert state(store) == late_state
    assert selected_batch(store, scope, replay=True) == []


@pytest.mark.asyncio
async def test_wrong_stream_ack_never_checkpoints(source):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(return_value=SimpleNamespace(stream="AGENT_INBOX"))
    )
    with pytest.raises(TraceContractError, match="ack_stream_mismatch"):
        await TraceExporter(store, js, scope).publish_batch()
    assert state(store) == "pending"


@pytest.mark.asyncio
async def test_retry_backoff_is_bounded_and_stop_interrupts_worker(source, monkeypatch):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(publish=AsyncMock(side_effect=NatsTimeoutError))
    exporter = TraceExporter(store, js, scope)
    stop = asyncio.Event()
    waits = []

    async def wait(_stop, seconds):
        waits.append(seconds)
        if len(waits) == 8:
            stop.set()

    monkeypatch.setattr(exporter, "_wait", wait)
    await exporter.run(stop)
    assert all(
        base <= actual <= base + 0.25
        for base, actual in zip([1, 2, 4, 8, 16, 30, 30, 30], waits, strict=True)
    )
    assert exporter.state == "stopped"
    assert state(store) == "pending"


@pytest.mark.asyncio
async def test_permanent_configuration_failure_pauses_with_fixed_fault(source):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(
            side_effect=APIError(code=400, description="secret must not escape")
        )
    )
    exporter = TraceExporter(store, js, scope)
    await exporter.run(asyncio.Event())
    assert (exporter.state, exporter.fault) == (
        "paused",
        "telemetry_configuration_error",
    )
    assert js.publish.call_count == 1
    assert state(store) == "pending"


@pytest.mark.asyncio
async def test_failed_local_ack_checkpoint_replays_same_identity(source):
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(return_value=SimpleNamespace(stream=STREAM_NAME))
    )
    exporter = TraceExporter(store, js, scope)
    store._connection.execute(
        "CREATE TEMP TRIGGER fail_ack BEFORE UPDATE ON trace_spool BEGIN SELECT RAISE(ABORT, 'full'); END"
    )
    with pytest.raises(sqlite3.IntegrityError):
        await exporter.publish_batch()
    assert state(store) == "pending"
    store._connection.execute("DROP TRIGGER fail_ack")
    await exporter.publish_batch()
    assert js.publish.call_args_list[0] == js.publish.call_args_list[1]
    assert state(store) == "broker_acked"


@pytest.mark.asyncio
async def test_stop_between_publishes_keeps_remaining_work_pending(source):
    store, scope, _ = source
    write(source)
    write(source)
    stop = asyncio.Event()

    async def publish(*args, **kwargs):
        stop.set()
        return SimpleNamespace(stream=STREAM_NAME)

    exporter = TraceExporter(store, SimpleNamespace(publish=publish), scope)
    await exporter.run(stop)
    assert exporter.state == "stopped"
    assert [
        row[0]
        for row in store._connection.execute(
            "SELECT state FROM trace_spool ORDER BY export_seq"
        )
    ] == ["broker_acked", "pending"]


def test_retired_scope_and_sparse_rows_remain_replayable(source):
    store, scope, _ = source
    for _ in range(3):
        write(source)
    with store._connection:
        store._connection.execute("UPDATE trace_sources SET active=0")
        store._connection.execute("UPDATE trace_export_generations SET active=0")
        store._connection.execute(
            "UPDATE trace_spool SET state='broker_acked' WHERE export_seq=1"
        )
        store._connection.execute(
            "UPDATE trace_spool SET state='lost_with_marker',journal_event_id=NULL WHERE export_seq=2"
        )
    assert [row.export_seq for row in selected_batch(store, scope)] == [3]
    assert [row.export_seq for row in selected_batch(store, scope, replay=True)] == [
        1,
        3,
    ]
    assert [
        row.export_seq for row in selected_batch(store, scope, replay=True, after=1)
    ] == [3]


def test_recovery_delivery_id_changes_once_per_epoch_and_survives_restart(source):
    store, scope, _ = source
    write(source)
    original = selected_batch(store, scope)[0]
    old_epoch, next_epoch = str(uuid4()), str(uuid4())
    with store._connection:
        store._connection.execute(
            "INSERT INTO trace_collector_recovery VALUES(?,?,?,?,?,?,?)",
            (*scope.values(), "ready", 1, 1, json.dumps([old_epoch])),
        )
    replay = selected_batch(store, scope)[0]
    assert replay.payload == original.payload
    assert replay.message_id != original.message_id
    with store._connection:
        store._connection.execute("UPDATE trace_collector_recovery SET phase='live'")
    assert selected_batch(store, scope)[0].message_id == replay.message_id
    reopened = AgentdStore(store.path)
    try:
        assert selected_batch(reopened, scope)[0].message_id == replay.message_id
    finally:
        reopened.close()
    with store._connection:
        store._connection.execute(
            "UPDATE trace_collector_recovery SET blocked_epochs_json=?",
            (json.dumps([old_epoch, next_epoch]),),
        )
    second = selected_batch(store, scope)[0]
    assert second.payload == original.payload
    assert second.message_id not in (original.message_id, replay.message_id)
