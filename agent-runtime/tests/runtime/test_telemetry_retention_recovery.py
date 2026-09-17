"""Source maintenance and real broker expiry cannot manufacture complete history."""

import asyncio
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from test_telemetry_stream import broker, pytestmark  # noqa: F401

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import page_request
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime import telemetry_stream as telemetry


@pytest.mark.parametrize("removed", [2, 4])
async def test_outage_retention_reports_exact_loss_and_recovers_retained_data(
    broker,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    removed,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService

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
    sync = TraceSyncService(node, store.path, enabled=True)
    collector = TraceCollectorService(tmp_path / "core.db", url, token)
    template = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    original_ids = []

    def append():
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            value = journal.record(
                "edge-a", {**template, "event_id": str(uuid4())}, selected=True
            )
        return ("edge-a", epoch, generation), value["event_id"]

    async def until(predicate):
        async with asyncio.timeout(95):
            while not predicate():
                await asyncio.sleep(0.02)

    try:
        for _ in range(4):
            scope, event_id = append()
            original_ids.append(event_id)
        sync.start()
        await until(
            lambda: (
                store._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='broker_acked'"
                ).fetchone()[0]
                == 4
            )
        )
        await asyncio.to_thread(sync.stop)
        async with asyncio.timeout(8):
            while (await js.stream_info(telemetry.STREAM_NAME)).state.messages:
                await asyncio.sleep(0.05)
        assert page_request(store, scope)["after_export_seq"] == 0
        # Age only selected receipt timestamps; deletion uses production reconcile.
        with store._connection:
            store._connection.execute(
                "UPDATE trace_journal SET received_at_ms=0 WHERE source_seq<=?",
                (removed,),
            )
        before = list(store._connection.iterdump())
        used = store._connection.execute(
            "SELECT event_bytes FROM trace_storage_usage"
        ).fetchone()[0]
        with monkeypatch.context() as capacity:
            capacity.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", used)
            capacity.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
            store.reconcile(now_ms=int(time.time() * 1000))
        assert list(store._connection.iterdump()) == before
        # With the reserve restored, actual maintenance records loss before removal.
        store.reconcile(now_ms=int(time.time() * 1000))
        journal = [
            json.loads(row[0])
            for row in store._connection.execute("SELECT event_json FROM trace_journal")
        ]
        markers = [event for event in journal if event["kind"] == "coverage"]
        assert len(markers) == 1
        assert markers[0]["attributes"]["lost_ranges"] == [
            {"first": 1, "last": removed}
        ]
        assert markers[0]["attributes"]["reason"] == "retention_expired"
        assert {
            event["event_id"] for event in journal if event["kind"] != "coverage"
        } == set(original_ids[removed:])
        assert page_request(store, scope)["after_export_seq"] == 0
        # Persisted loss and retained payloads must survive source restart.
        store.close()
        store = AgentdStore(node / "agentd/agentd.sqlite3")
        _, new_id = append()
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        sync.start()
        await until(lambda: page_request(store, scope)["after_export_seq"] == 6)
        with sqlite3.connect(tmp_path / "core.db") as core:
            rows = core.execute(
                "SELECT r.event_id,r.event_sha256,p.event_json "
                "FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
            ).fetchall()
            assert set(rows) == {
                tuple(row)
                for row in store._connection.execute(
                    "SELECT event_id,event_sha256,event_json FROM trace_journal"
                )
            }
            assert {row[0] for row in rows} == set(original_ids[removed:]) | {
                markers[0]["event_id"],
                new_id,
            }
            assert core.execute(
                "SELECT first_seq,last_seq FROM trace_loss_ranges"
            ).fetchall() == [(1, removed)]
        page = json.loads(
            store._connection.execute(
                "SELECT last_page_json FROM trace_source_settlements"
            ).fetchone()[0]
        )
        # Pages cover only their own interval; check durable loss evidence at Core above.
        assert page["settled_export_seq"] == 6
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        store.close()
