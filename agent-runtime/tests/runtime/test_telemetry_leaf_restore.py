"""Older Core restore reconciles two real Leaves without resetting broker history."""

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest
from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.mark.parametrize("missing", [False, True])
async def test_older_core_restore_replays_two_leaves_with_normal_timers(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    missing,
    record_property,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_restore import prepare_restore
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    stores, syncs, scopes, records, task_ids = [], [], [], [], []
    current, backup, restored = (
        tmp_path / name for name in ("current.db", "backup.db", "restored.db")
    )
    url, token = topology.endpoints["core"]
    collector = TraceCollectorService(current, url, token)

    async def until(predicate, timeout=100):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.05)

    try:
        for leaf in ("a", "b"):
            directory = tmp_path / f"source-{leaf}"
            directory.mkdir()
            leaf_url, leaf_token = topology.endpoints[leaf]
            identity = directory / "node.json"
            identity.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "mode": "edge",
                        "agent_id": f"edge-{leaf}",
                        "messaging_mode": "nats_leaf",
                        "jetstream_domain": f"EDGE_{leaf.upper()}",
                        "plugin_nats_url": leaf_url,
                        "plugin_nats_token": leaf_token,
                    }
                )
            )
            identity.chmod(0o600)
            store = AgentdStore(directory / "agentd/agentd.sqlite3")
            stores.append(store)
            secret = store.register_connector(
                connector_id="worker",
                host_type="codex",
                agent_id=f"worker-{leaf}",
                capabilities=["inbox"],
            )
            session = store.open_session(connector_id="worker", token=secret)[
                "session_id"
            ]
            task = store.create_task(
                sender_id="origin",
                recipient_id=f"worker-{leaf}",
                payload={"fixture": leaf},
            )
            assert (
                store.claim_next_task(
                    connector_id="worker", token=secret, session_id=session
                )["task_id"]
                == task["task_id"]
            )
            for state in ("running", "completed"):
                store.transition_task(
                    task_id=task["task_id"],
                    state=state,
                    actor_id=f"worker-{leaf}",
                    session_id=session,
                    queue_transport=False,
                )
            task_ids.append(task["task_id"])
            scope = ExportScope(
                *store._connection.execute(
                    "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
                ).fetchone()
            )
            scopes.append(scope)
            records.append(selected_batch(store, scope))
            assert len(records[-1]) == 5
            syncs.append(TraceSyncService(directory, store.path, enabled=True))
        with sqlite3.connect(current) as db:
            initialize(db)
            for batch in records:
                first = batch[0]
                ingest_wire(
                    db,
                    first.subject,
                    first.payload,
                    received_at_ms=time.time_ns() // 1_000_000,
                )
            with sqlite3.connect(backup) as target:
                db.backup(target)
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        initial_publish_start = time.monotonic()
        for sync in syncs:
            sync.start()
        await until(
            lambda: all(
                page_request(s, scope.values())["after_export_seq"] == 5
                for s, scope in zip(stores, scopes, strict=True)
            )
        )
        stale = []
        with sqlite3.connect(current) as db:
            for store, scope in zip(stores, scopes, strict=True):
                request = {**page_request(store, scope.values()), "after_export_seq": 0}
                stale.append((request, settlement_page_reply(db, request)))
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        if missing:
            event_id = json.loads(records[0][1].payload)["event"]["event_id"]
            with stores[0]._connection as db:
                db.execute(
                    "UPDATE trace_spool SET journal_event_id=NULL WHERE export_seq=2"
                )
                db.execute("DELETE FROM trace_journal WHERE event_id=?", (event_id,))
        prepared = prepare_restore(backup, restored)
        old_epoch = stale[0][0]["collector_epoch"]
        assert prepared["previous_collector_epoch"] == old_epoch
        assert prepared["collector_epoch"] != old_epoch
        collector = TraceCollectorService(restored, url, token)
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        started = time.monotonic()
        for sync in syncs:
            sync.start()

        def recovered():
            for index, (store, scope) in enumerate(zip(stores, scopes, strict=True)):
                page = page_request(store, scope.values())
                if page["collector_epoch"] != prepared["collector_epoch"] or page[
                    "after_export_seq"
                ] < (6 if missing and index == 0 else 5):
                    return False
            return True

        await until(recovered)
        duration = time.monotonic() - started
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        expected = {
            (
                record.scope.node_id,
                record.scope.source_epoch,
                json.loads(record.payload)["event"]["event_id"],
                record.event_sha256,
            )
            for index, batch in enumerate(records)
            for record in batch
            if not (missing and index == 0 and record.export_seq == 2)
        }
        with sqlite3.connect(restored) as db:
            raw = list(
                db.execute(
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.event_sha256,p.event_json FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
                )
            )
            assert {
                row[:4] for row in raw if json.loads(row[4])["kind"] != "coverage"
            } == expected
            source_payloads = {
                tuple(row)
                for store in stores
                for row in store._connection.execute(
                    "SELECT node_id,source_epoch,event_id,event_sha256,event_json FROM trace_journal"
                )
            }
            assert set(raw) == source_payloads
            markers = [
                json.loads(row[4])
                for row in raw
                if json.loads(row[4])["kind"] == "coverage"
            ]
            assert len(markers) == int(missing)
            if missing:
                assert markers[0]["attributes"]["lost_ranges"] == [
                    {"first": 2, "last": 2}
                ]
            assert db.execute(
                "SELECT node_id,source_epoch,export_generation,first_seq,last_seq FROM trace_loss_ranges"
            ).fetchall() == ([(*scopes[0].values(), 2, 2)] if missing else [])
            for table in ("trace_ingest_conflicts", "trace_rejected_positions"):
                assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        for store, task_id, (request, reply) in zip(
            stores, task_ids, stale, strict=True
        ):
            before = list(store._connection.iterdump())
            with pytest.raises(TraceContractError, match="retired_collector_epoch"):
                apply_page(store, request, reply)
            assert list(store._connection.iterdump()) == before
            assert store.get_task(task_id)["state"] == "completed"
            assert (
                store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
                == 1
            )
            phase, blocked = store._connection.execute(
                "SELECT phase,blocked_epochs_json FROM trace_collector_recovery"
            ).fetchone()
            assert phase == "live" and json.loads(blocked) == [old_epoch]
        replay_window = time.monotonic() - initial_publish_start
        broker = await topology["core"].jetstream().stream_info(STREAM_NAME)
        assert replay_window < broker.config.duplicate_window
        assert (
            broker.state.messages == 20
        )  # Ten original deliveries plus ten recovery deliveries.
        evidence = {
            "missing_payload": missing,
            "normal_timers": True,
            "original_publish_through_recovery_seconds": replay_window,
            "broker_duplicate_window_seconds": broker.config.duplicate_window,
            "broker_messages_after_recovery": broker.state.messages,
            "recovery_seconds": duration,
            "exact_retained_events": len(expected),
            "exact_loss_ranges": int(missing),
            "stale_page_refusals_without_mutation": 2,
            "completed_task_rows_unchanged": 2,
            "scope": "Same Core broker/durable; two real Leaves; completed local tasks; missing payload injected after settlement; no crash-cutover or external effects",
        }
        record_property("restore_evidence", json.dumps(evidence))
        print(json.dumps(evidence), flush=True)
    finally:
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        for store in stores:
            store.close()
