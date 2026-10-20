"""Owned broker proof for commit-before-ACK and poison progress, opt-in only."""

import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes
from edgecitadel_plugin_runtime.telemetry_stream import (
    CONSUMER_NAME,
    EVENT_SUBJECT,
    STREAM_NAME,
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)
from nats.aio.client import Client as NATS

from aggregator.trace_ingest import ingest_delivery
from aggregator.trace_store import initialize
from tests.nats_server import NatsServer

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1",
    reason="set RUN_JETSTREAM_INTEGRATION=1 for owned telemetry broker tests",
)


@pytest.mark.asyncio
async def test_commit_failure_redelivery_and_poison_do_not_block_next_valid(tmp_path):
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    nc = NATS()
    connection = sqlite3.connect(tmp_path / "core.db")
    try:
        initialize(connection)
        await nc.connect(
            servers=[server.url], token=server.token, allow_reconnect=False
        )
        js = nc.jetstream()
        await ensure_telemetry_stream(js)
        await ensure_telemetry_consumer(js)
        consumer = await js.pull_subscribe(
            EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
        )
        subject = "edgecitadel.telemetry.v1.edge-a"
        event = json.loads(
            (
                Path(__file__).parents[2]
                / "agent-runtime/tests/fixtures/traces/events.v1.json"
            ).read_text()
        )["fixtures"][0]["event"]
        wrapper = {
            "schema_version": 1,
            "node_id": event["node_id"],
            "source_epoch": event["source_epoch"],
            "export_generation": str(uuid4()),
            "export_seq": 1,
            "event_sha256": hashlib.sha256(canonical_bytes(event)).hexdigest(),
            "event": event,
        }
        payload = canonical_bytes(wrapper)
        await js.publish(subject, payload)
        (first,) = await consumer.fetch(1, timeout=2)
        connection.execute(
            "CREATE TEMP TRIGGER fail BEFORE UPDATE ON trace_collector BEGIN SELECT RAISE(ABORT, 'injected'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            await ingest_delivery(connection, first)
        assert (await js.consumer_info(STREAM_NAME, CONSUMER_NAME)).num_ack_pending == 1
        assert (
            connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
            == 0
        )
        connection.execute("DROP TRIGGER fail")
        # NAK accelerates the normal redelivery delay without changing the durable's policy.
        await first.nak()
        (second,) = await consumer.fetch(1, timeout=2)
        assert second.metadata.sequence.stream == first.metadata.sequence.stream
        assert second.metadata.num_delivered == 2
        result = await ingest_delivery(connection, second)
        assert result.outcome == "accepted"
        assert (await js.consumer_info(STREAM_NAME, CONSUMER_NAME)).num_ack_pending == 0
        assert (
            connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
            == 1
        )
        # Repeat the wrapper at a fresh broker position: Core deduplication does not
        # depend on the broker duplicate window or broker sequence.
        await js.publish(subject, payload)
        (duplicate,) = await consumer.fetch(1, timeout=2)
        assert await ingest_delivery(connection, duplicate) == result
        await js.publish(subject, b"private-secret-invalid-json")
        (poison,) = await consumer.fetch(1, timeout=2)
        assert (await ingest_delivery(connection, poison)).outcome == "quarantined"
        assert (await js.consumer_info(STREAM_NAME, CONSUMER_NAME)).num_ack_pending == 0
        wrapper["export_seq"] = 2
        event["event_id"], event["source_seq"] = str(uuid4()), 2
        wrapper["event_sha256"] = hashlib.sha256(canonical_bytes(event)).hexdigest()
        await js.publish(subject, canonical_bytes(wrapper))
        (valid,) = await consumer.fetch(1, timeout=2)
        assert (await ingest_delivery(connection, valid)).outcome == "accepted"
        assert (
            connection.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
            == 2
        )
        assert "private-secret" not in "\n".join(connection.iterdump())
    finally:
        connection.close()
        try:
            await nc.close()
        finally:
            await asyncio.to_thread(server.close)
