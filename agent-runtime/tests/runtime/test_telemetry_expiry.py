"""Actual age eviction after broker ACK, before collector commit, with retained replay."""

import asyncio
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from nats.js.errors import NotFoundError
from test_telemetry_stream import broker, pytestmark  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime import telemetry_stream as telemetry


@pytest.mark.parametrize("known_source", [False, True])
async def test_age_expiry_replays_retained_payload_without_false_settlement(
    broker,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    known_source,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    # Shorten only owned broker retention/deduplication, not source retry/poll timers.
    normal = telemetry.stream_config
    monkeypatch.setattr(
        telemetry,
        "stream_config",
        lambda: replace(normal(), max_age=2, duplicate_window=1),
    )
    js = broker.jetstream()
    await telemetry.ensure_telemetry_stream(js)
    node = tmp_path / "source"
    node.mkdir()
    url, token = broker.connected_url.geturl(), broker.options["token"]
    (node / "node.json").write_text(
        json.dumps(
            {
                "version": 1,
                "mode": "core",
                "agent_id": "edge-a",
                "nats_url": url,
                "nats_token": token,
            }
        )
    )
    (node / "node.json").chmod(0o600)
    store = AgentdStore(node / "agentd/agentd.sqlite3")
    db = sqlite3.connect(tmp_path / "core.db")
    initialize(db)
    sync = TraceSyncService(node, store.path, enabled=True)
    collector = TraceCollectorService(tmp_path / "core.db", url, token)
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]

    def append():
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            journal.record("edge-a", {**event, "event_id": str(uuid4())}, selected=True)
        return ExportScope("edge-a", epoch, generation)

    async def until(predicate, timeout=45):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.02)

    try:
        scope = append()
        if known_source:
            record = selected_batch(store, scope)[0]
            # Keep Core receipt fresh: this fixture expires broker retention only.
            ingest_wire(
                db,
                record.subject,
                record.payload,
                received_at_ms=int(time.time() * 1000),
            )
            request = page_request(store, scope.values())
            apply_page(store, request, settlement_page_reply(db, request))
            append()
        base = int(known_source)
        sync.start()
        await until(
            lambda: (
                store._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='broker_acked'"
                ).fetchone()[0]
                == 1
            )
        )
        before = await js.stream_info(telemetry.STREAM_NAME)
        assert before.state.messages == 1
        expired_sequence = before.state.last_seq
        async with asyncio.timeout(8):
            while (await js.stream_info(telemetry.STREAM_NAME)).state.messages:
                await asyncio.sleep(0.05)
        with pytest.raises(NotFoundError):
            await js.get_msg(telemetry.STREAM_NAME, seq=expired_sequence)
        assert page_request(store, scope.values())["after_export_seq"] == base
        assert db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == base
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == base + 1
        )
        assert (
            store._connection.execute(
                "SELECT count(*) FROM trace_spool WHERE state='broker_acked'"
            ).fetchone()[0]
            == 1
        )
        collector.start()
        await until(
            lambda: page_request(store, scope.values())["after_export_seq"] == base + 1
        )
        assert (
            await js.stream_info(telemetry.STREAM_NAME)
        ).state.last_seq > expired_sequence
        source = {
            tuple(row)
            for row in store._connection.execute(
                "SELECT event_id,event_sha256,event_json FROM trace_journal"
            )
        }
        assert (
            set(
                db.execute(
                    "SELECT r.event_id,r.event_sha256,p.event_json "
                    "FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
                )
            )
            == source
        )
        assert (
            db.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[0]
            == base + 1
        )
        assert db.execute("SELECT count(*) FROM trace_loss_ranges").fetchone()[0] == 0
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        db.close()
        store.close()
