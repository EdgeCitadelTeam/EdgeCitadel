import asyncio
import os
import secrets
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nats.aio.client import Client as NATS
from test_trace_ingest import connection, encode, record  # noqa: F401
from tests.nats_server import NatsServer

from aggregator.trace_collector import TraceCollectorService
from aggregator.trace_ingest import ingest_delivery
from edgecitadel_plugin_runtime.telemetry_stream import (
    CONSUMER_NAME,
    EVENT_SUBJECT,
    STREAM_NAME,
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)


@pytest.mark.asyncio
async def test_backlog_error_clears_old_counts_and_can_recover(tmp_path):
    collector = TraceCollectorService(tmp_path / "unused.db", "unused", "unused")
    consumer = SimpleNamespace(
        consumer_info=AsyncMock(
            side_effect=[
                SimpleNamespace(num_pending=4, num_ack_pending=2),
                OSError("must-not-appear"),
                SimpleNamespace(num_pending=0, num_ack_pending=0),
            ]
        )
    )
    await collector._sample_backlog(consumer)
    status = collector.status()
    assert status["broker_backlog"]["pending_delivery"] == 4
    status["broker_backlog"]["pending_delivery"] = 99
    assert collector.status()["broker_backlog"]["pending_delivery"] == 4
    await collector._sample_backlog(consumer)
    assert collector.status()["broker_backlog"] == {"state": "unavailable"}
    await collector._sample_backlog(consumer)
    assert collector.status()["broker_backlog"]["pending_delivery"] == 0
    assert collector.status()["metrics"]["ack_successes"] == 0
    assert not (tmp_path / "unused.db").exists()


@pytest.mark.parametrize("value", [None, True, -1, 2**53])
@pytest.mark.asyncio
async def test_invalid_counts_are_unavailable(tmp_path, value):
    collector = TraceCollectorService(tmp_path / "unused.db", "unused", "unused")
    consumer = SimpleNamespace(
        consumer_info=AsyncMock(
            return_value=SimpleNamespace(num_pending=value, num_ack_pending=0)
        )
    )
    await collector._sample_backlog(consumer)
    assert collector.status()["broker_backlog"] == {"state": "unavailable"}


@pytest.mark.asyncio
async def test_backlog_watch_cancellation_clears_sample(tmp_path):
    collector = TraceCollectorService(tmp_path / "unused.db", "unused", "unused")
    consumer = SimpleNamespace(
        consumer_info=AsyncMock(
            return_value=SimpleNamespace(num_pending=3, num_ack_pending=1)
        )
    )
    task = asyncio.create_task(collector._watch_backlog(consumer))
    try:
        async with asyncio.timeout(1):
            while collector.status()["broker_backlog"]["state"] != "available":
                await asyncio.sleep(0)
        assert consumer.consumer_info.await_count == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert collector.status()["broker_backlog"] == {"state": "unavailable"}


@pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1",
    reason="owned NATS opt-in required",
)
@pytest.mark.asyncio
async def test_real_backlog_distinguishes_delivery_ack_and_core_commit(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    nc = NATS()
    try:
        await nc.connect(servers=[server.url], token=server.token)
        js = nc.jetstream()
        await ensure_telemetry_stream(js)
        await ensure_telemetry_consumer(js)
        consumer = await js.pull_subscribe(
            EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
        )
        subject = "edgecitadel.telemetry.v1." + record["node_id"]
        await js.publish(subject, encode(record))
        second = deepcopy(record)
        second["export_seq"] += 1
        await js.publish(subject, encode(second))
        collector = TraceCollectorService(tmp_path / "core.db", server.url, "")
        await collector._sample_backlog(consumer)
        assert collector.status()["broker_backlog"]["pending_delivery"] == 2
        first = (await consumer.fetch(batch=1, timeout=1))[0]
        await collector._sample_backlog(consumer)
        assert collector.status()["broker_backlog"]["pending_delivery"] == 1
        assert collector.status()["broker_backlog"]["awaiting_ack"] == 1
        assert (
            connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
            == 0
        )
        await ingest_delivery(connection, first)
        await collector._sample_backlog(consumer)
        assert collector.status()["broker_backlog"]["awaiting_ack"] == 0
        assert (
            connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
            == 1
        )
        await ingest_delivery(connection, (await consumer.fetch(batch=1, timeout=1))[0])
        await collector._sample_backlog(consumer)
        status = collector.status()["broker_backlog"]
        assert status["pending_delivery"] == status["awaiting_ack"] == 0
        assert (
            connection.execute(
                "SELECT count(*) FROM trace_ingest_positions"
            ).fetchone()[0]
            == 2
        )
        await consumer.unsubscribe()
    finally:
        await nc.close()
        await asyncio.to_thread(server.close)


@pytest.mark.asyncio
async def test_backlog_request_times_out_and_cancels_without_counts(tmp_path):
    collector = TraceCollectorService(tmp_path / "unused.db", "unused", "unused")
    cancelled = asyncio.Event()

    async def stalled_info():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with asyncio.timeout(3):
        await collector._sample_backlog(SimpleNamespace(consumer_info=stalled_info))
    assert cancelled.is_set()
    assert collector.status()["broker_backlog"] == {"state": "unavailable"}
