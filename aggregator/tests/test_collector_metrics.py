from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from nats import errors as nats_errors
from test_trace_ingest import connection, encode, record  # noqa: F401

from aggregator.trace_collector import TraceCollectorService
from aggregator.trace_ingest import ingest_delivery


class Message:
    subject = "edgecitadel.telemetry.v1.edge-a"

    def __init__(self, data, *, fail_ack=False):
        self.data, self.fail_ack = data, fail_ack
        self.acks, self.naks = 0, 0
        self.metadata = SimpleNamespace(
            timestamp=datetime.now(UTC) - timedelta(seconds=2)
        )

    async def ack_sync(self, timeout):
        self.acks += 1
        if self.fail_ack:
            raise self.fail_ack()

    async def nak(self, delay):
        self.naks += 1


async def consume(collector, database, messages):
    class Consumer:
        async def fetch(self, **kwargs):
            collector._stop.set()
            return messages

    await collector._ingest(database, SimpleNamespace(is_connected=True), Consumer())


@pytest.mark.parametrize("ack_error", [nats_errors.TimeoutError, OSError])
@pytest.mark.asyncio
async def test_commit_observed_before_failed_ack_and_redelivery(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    ack_error,  # noqa: F811
):
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    message = Message(encode(record), fail_ack=ack_error)
    with pytest.raises(ack_error):
        await consume(collector, connection, [message])
    metrics = collector.status()["metrics"]
    assert metrics["commit_observations"]["accepted"] == 1
    assert metrics["ack_failures"] == 1 and metrics["ack_successes"] == 0
    assert metrics["last_delivery_age_ms"] >= 2000
    assert (
        connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
    )
    collector._stop.clear()
    retry = Message(message.data)
    await consume(collector, connection, [retry])
    metrics = collector.status()["metrics"]
    # Observations count delivery attempts, including a previously committed disposition.
    assert metrics["commit_observations"]["accepted"] == 2
    assert metrics["ack_successes"] == 1
    assert (
        connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
    )


@pytest.mark.asyncio
async def test_persistence_failure_does_not_count_commit_or_ack_and_next_record_continues(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    connection.execute(
        "CREATE TRIGGER fail_poison BEFORE INSERT ON trace_poison_counts BEGIN SELECT RAISE(ABORT,'injected'); END"
    )
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    poison, valid = Message(b"not-json"), Message(encode(record))
    await consume(collector, connection, [poison, valid])
    metrics = collector.status()["metrics"]
    assert metrics["persistence_failures"] == 1
    assert metrics["ack_successes"] == 1 and metrics["ack_failures"] == 0
    assert metrics["commit_observations"]["quarantined"] == 0
    assert metrics["commit_observations"]["accepted"] == 1
    assert (poison.acks, poison.naks, valid.acks) == (0, 1, 1)


@pytest.mark.asyncio
async def test_observation_failure_cannot_prevent_ack(connection, record):  # noqa: F811
    def broken(result):
        raise RuntimeError("private-observer-error")

    message = Message(encode(record))
    result = await ingest_delivery(connection, message, on_commit=broken)
    assert result.outcome == "accepted" and message.acks == 1


@pytest.mark.asyncio
async def test_future_broker_clock_is_unknown_age_and_metrics_are_copy_safe(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,  # noqa: F811
):
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    message = Message(encode(record))
    message.metadata.timestamp = datetime.now(UTC) + timedelta(days=1)
    await consume(collector, connection, [message])
    metrics = collector.status()["metrics"]
    assert (
        metrics["last_delivery_age_ms"] is None
        and metrics["delivery_clock_skew"] is True
    )
    metrics["commit_observations"]["accepted"] = 900
    assert collector.status()["metrics"]["commit_observations"]["accepted"] == 1
    collector._status["metrics"]["ack_successes"] = 2**53 - 1
    collector._increment("ack_successes")
    assert collector.status()["metrics"]["ack_successes"] == 2**53 - 1
    before = collector.status()["metrics"]
    collector.stop()
    assert collector.status()["metrics"] == before


@pytest.mark.parametrize(
    "offset_ms,expected,state",
    [(-10000, 10000, "observed"), (0, 0, "observed"), (1000, None, "clock_skew")],
)
@pytest.mark.asyncio
async def test_event_collection_age_is_distinct_from_broker_age(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    monkeypatch,
    offset_ms,
    expected,
    state,  # noqa: F811
):
    import aggregator.trace_collector as module

    now = datetime(2026, 9, 16, 12, 0, 10, tzinfo=UTC)
    monkeypatch.setattr(
        module.time, "time_ns", lambda: int(now.timestamp()) * 1_000_000_000
    )
    record["event"]["occurred_at"] = (
        (now + timedelta(milliseconds=offset_ms))
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    message = Message(encode(record))
    message.metadata.timestamp = now - timedelta(seconds=2)
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    await consume(collector, connection, [message])
    metrics = collector.status()["metrics"]
    assert metrics["last_event_collection_age_ms"] == expected
    assert metrics["event_collection_age_state"] == state
    assert metrics["last_delivery_age_ms"] == 2000
    assert metrics["ack_successes"] == 1
    assert (
        connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
    )


@pytest.mark.asyncio
async def test_redelivery_uses_event_time_and_poison_clears_age(
    connection,  # noqa: F811
    record,  # noqa: F811
    tmp_path,
    monkeypatch,  # noqa: F811
):
    import aggregator.trace_collector as module

    now = datetime(2026, 9, 16, 12, 0, 10, tzinfo=UTC)
    milliseconds = [int(now.timestamp()) * 1000]
    monkeypatch.setattr(module.time, "time_ns", lambda: milliseconds[0] * 1_000_000)
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    message = Message(encode(record))
    await consume(collector, connection, [message])
    assert collector.status()["metrics"]["last_event_collection_age_ms"] == 10000
    milliseconds[0] += 5000
    collector._stop.clear()
    await consume(collector, connection, [message])
    assert collector.status()["metrics"]["last_event_collection_age_ms"] == 15000
    assert (
        connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
    )
    collector._stop.clear()
    await consume(collector, connection, [Message(b"private-invalid-wire")])
    metrics = collector.status()["metrics"]
    assert metrics["last_event_collection_age_ms"] is None
    assert metrics["event_collection_age_state"] == "unavailable"
    assert metrics["commit_observations"]["quarantined"] == 1
    assert metrics["ack_successes"] == 3


@pytest.mark.parametrize("outcome", ["rejected", "conflict", "quarantined"])
def test_nonaccepted_disposition_does_not_read_event_timestamp(tmp_path, outcome):
    class UntrustedMessage:
        metadata = SimpleNamespace(timestamp=datetime.now(UTC))

        @property
        def data(self):
            raise AssertionError("untrusted event payload must not supply an age")

    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")
    collector._status["metrics"]["last_event_collection_age_ms"] = 99
    collector._status["metrics"]["event_collection_age_state"] = "observed"
    collector._observe_commit(UntrustedMessage(), SimpleNamespace(outcome=outcome))
    metrics = collector.status()["metrics"]
    assert metrics["last_event_collection_age_ms"] is None
    assert metrics["event_collection_age_state"] == "unavailable"
