import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nats.errors import TimeoutError as NatsTimeoutError
from test_trace_exporter import source, state, write  # noqa: F401
from test_trace_settlement_poll import success

from edgecitadel_agentd import trace_exporter
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_metrics import COUNTERS, SourceMetrics
from edgecitadel_agentd.trace_settlement_poll import SettlementPoller
from edgecitadel_agentd.trace_sync import TraceSyncWorker
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.mark.parametrize("error", [NatsTimeoutError, OSError])
@pytest.mark.asyncio
async def test_publish_failure_and_broker_ack_are_not_settlement(source, error):  # noqa: F811
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(publish=AsyncMock(side_effect=error()))
    exporter = trace_exporter.TraceExporter(store, js, scope)
    with pytest.raises(error):
        await exporter.publish_batch()
    assert state(store) == "pending"
    js.publish.side_effect = None
    js.publish.return_value = SimpleNamespace(stream=STREAM_NAME)
    await exporter.publish_batch()
    counts = exporter.metrics.snapshot()["counts"]
    assert counts["publish_attempts"] == 2 and counts["publish_failures"] == 1
    assert counts["broker_acknowledgments"] == 1
    assert counts["settlement_page_observations"] == 0
    assert state(store) == "broker_acked"
    assert (
        store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        == 1
    )


@pytest.mark.asyncio
async def test_broker_ack_and_failed_local_checkpoint_are_distinct(source, monkeypatch):  # noqa: F811
    store, scope, _ = source
    write(source)
    js = SimpleNamespace(
        publish=AsyncMock(return_value=SimpleNamespace(stream=STREAM_NAME))
    )
    exporter = trace_exporter.TraceExporter(store, js, scope)

    def fail(*args):
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(trace_exporter, "checkpoint_broker_ack", fail)
    with pytest.raises(sqlite3.OperationalError):
        await exporter.publish_batch()
    counts = exporter.metrics.snapshot()["counts"]
    assert (
        counts["broker_acknowledgments"]
        == counts["broker_ack_checkpoint_failures"]
        == 1
    )
    assert counts["publish_failures"] == 0 and state(store) == "pending"


@pytest.mark.asyncio
async def test_wrong_stream_ack_is_not_a_valid_broker_ack(source):  # noqa: F811
    store, scope, _ = source
    write(source)
    exporter = trace_exporter.TraceExporter(
        store,
        SimpleNamespace(
            publish=AsyncMock(return_value=SimpleNamespace(stream="OTHER"))
        ),
        scope,
    )
    with pytest.raises(TraceContractError):
        await exporter.publish_batch()
    counts = exporter.metrics.snapshot()["counts"]
    assert (
        counts["invalid_broker_acknowledgments"] == 1
        and counts["broker_acknowledgments"] == 0
    )
    assert state(store) == "pending"


@pytest.mark.asyncio
async def test_settlement_requests_are_separate_from_committed_page_observation(source):  # noqa: F811
    store, scope, _ = source
    write(source)
    client = SimpleNamespace(request=AsyncMock(side_effect=NatsTimeoutError()))
    poller = SettlementPoller(store, client, scope.values())
    await poller.poll()
    counts = poller.metrics.snapshot()["counts"]
    assert counts["settlement_requests"] == counts["settlement_request_failures"] == 1
    assert counts["settlement_page_observations"] == 0
    client.request.side_effect = lambda subject, data, **kwargs: success(data)
    poller._next_at = 0
    await poller.poll()
    counts = poller.metrics.snapshot()["counts"]
    assert (
        counts["settlement_requests"] == 2
        and counts["settlement_page_observations"] == 1
    )
    assert state(store) == "core_settled"
    await poller.close()


@pytest.mark.asyncio
async def test_worker_replacements_share_metrics_and_snapshot_is_bounded(source):  # noqa: F811
    store, scope, _ = source
    metrics = SourceMetrics()
    worker = TraceSyncWorker(store, None, None, scope)
    worker.metrics = metrics
    import asyncio

    stop = asyncio.Event()
    await worker._recover(stop, changed=False)
    assert worker.poller.metrics is metrics
    worker._start_publisher(stop)
    assert worker.exporter.metrics is metrics
    await worker._cancel_publisher()
    await worker.poller.close()
    metrics.note("publish_attempts")
    snapshot = metrics.snapshot()
    snapshot["counts"]["publish_attempts"] = 99
    metrics.note("arbitrary-untrusted-label")
    assert metrics.snapshot()["counts"]["publish_attempts"] == 1
    assert set(metrics.snapshot()["counts"]) == set(COUNTERS)
    metrics._counts["publish_attempts"] = 2**53 - 1
    metrics.note("publish_attempts")
    assert metrics.snapshot()["counts"]["publish_attempts"] == 2**53 - 1
