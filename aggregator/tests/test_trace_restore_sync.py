import asyncio
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from edgecitadel_agentd.trace_settlement_poll import SettlementPoller
from edgecitadel_agentd.trace_sync_service import TraceSyncService

from aggregator.trace_collector import TraceCollectorService
from aggregator.trace_ingest import ingest_wire
from aggregator.trace_payloads import read_payload
from aggregator.trace_restore import prepare_restore
from aggregator.trace_settlement import settlement_page_reply
from aggregator.trace_store import initialize
from tests.nats_server import NatsServer


@pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1", reason="owned NATS required"
)
@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.asyncio
async def test_older_snapshot_replays_retained_evidence_and_declares_exact_loss(
    tmp_path, monkeypatch, missing
):
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    schedule = SettlementPoller._schedule
    monkeypatch.setattr(
        SettlementPoller,
        "_schedule",
        lambda self, delay: schedule(self, min(delay, 0.05)),
    )

    def run():
        node = tmp_path / "source"
        node.mkdir()
        (node / "node.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "mode": "core",
                    "agent_id": "edge-a",
                    "nats_url": server.url,
                    "nats_token": server.token,
                }
            )
        )
        store = AgentdStore(node / "agentd" / "agentd.sqlite3")
        event = json.loads(
            (
                Path(__file__).parents[2]
                / "agent-runtime/tests/fixtures/traces/events.v1.json"
            ).read_text()
        )["fixtures"][0]["event"]
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            for _ in range(3):
                journal.record(
                    "edge-a", {**event, "event_id": str(uuid4())}, selected=True
                )
        scope = ExportScope("edge-a", epoch, generation)
        records = selected_batch(store, scope)
        ids = [json.loads(r.payload)["event"]["event_id"] for r in records]
        current, backup, restored = (
            tmp_path / n for n in ("current.db", "backup.db", "restored.db")
        )
        with sqlite3.connect(current) as db:
            initialize(db)
            ingest_wire(
                db,
                records[0].subject,
                records[0].payload,
                received_at_ms=time.time_ns() // 1_000_000,
            )
            with sqlite3.connect(backup) as target:
                db.backup(target)
        collector = TraceCollectorService(current, server.url, server.token)
        sync = TraceSyncService(node, store.path, enabled=True)

        def until(predicate):
            deadline = time.monotonic() + 15
            while not predicate():
                assert time.monotonic() < deadline, (
                    collector.status(),
                    sync.status(),
                    page_request(store, scope.values()),
                )
                time.sleep(0.02)

        try:
            collector.start()
            until(lambda: collector.status()["state"] == "running")
            sync.start()
            until(lambda: page_request(store, scope.values())["after_export_seq"] == 3)
            old_epoch = page_request(store, scope.values())["collector_epoch"]
            stale_request = {
                **page_request(store, scope.values()),
                "after_export_seq": 0,
            }
            with sqlite3.connect(current) as db:
                stale_reply = settlement_page_reply(db, stale_request)
            sync.stop()
            collector.stop()
            # Inject unavailable local payload after settlement; exact assigned position remains.
            if missing:
                with store._connection:
                    store._connection.execute(
                        "UPDATE trace_spool SET journal_event_id=NULL WHERE export_seq=2"
                    )
                    store._connection.execute(
                        "DELETE FROM trace_journal WHERE event_id=?", (ids[1],)
                    )
            prepared = prepare_restore(backup, restored)
            assert prepared["previous_collector_epoch"] == old_epoch
            collector = TraceCollectorService(restored, server.url, server.token)
            collector.start()
            until(lambda: collector.status()["state"] == "running")
            # Keep the same broker, ACKed durable and active deduplication window.
            sync.start()
            until(
                lambda: (
                    page_request(store, scope.values())["collector_epoch"]
                    == prepared["collector_epoch"]
                    and page_request(store, scope.values())["after_export_seq"]
                    >= (4 if missing else 3)
                )
            )
            with sqlite3.connect(restored) as db:
                rows = [
                    (event_id, json.dumps(read_payload(db, sequence)["event"]))
                    for event_id, sequence in db.execute(
                        "SELECT event_id,ingest_seq FROM trace_raw_events"
                    ).fetchall()
                ]
                markers = [
                    json.loads(raw)
                    for _, raw in rows
                    if json.loads(raw)["kind"] == "coverage"
                ]
                assert {
                    i for i, raw in rows if json.loads(raw)["kind"] != "coverage"
                } == ({ids[0], ids[2]} if missing else set(ids))
                assert len(markers) == int(missing)
                if missing:
                    assert markers[0]["attributes"]["lost_ranges"] == [
                        {"first": 2, "last": 2}
                    ]
                    assert markers[0]["attributes"]["affected_source_epoch"] == epoch
                assert db.execute(
                    "SELECT first_seq,last_seq FROM trace_loss_ranges"
                ).fetchall() == ([(2, 2)] if missing else [])
            sync.stop()
            before = list(store._connection.iterdump())
            with pytest.raises(TraceContractError, match="retired_collector_epoch"):
                apply_page(store, stale_request, stale_reply)
            assert list(store._connection.iterdump()) == before
            recovery = store._connection.execute(
                "SELECT phase,blocked_epochs_json FROM trace_collector_recovery"
            ).fetchone()
            assert recovery[0] == "live" and json.loads(recovery[1]) == [old_epoch]
            assert (
                store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
                == 0
            )
        finally:
            sync.stop()
            collector.stop()
            store.close()

    try:
        await asyncio.to_thread(run)
    finally:
        await asyncio.to_thread(server.close)
