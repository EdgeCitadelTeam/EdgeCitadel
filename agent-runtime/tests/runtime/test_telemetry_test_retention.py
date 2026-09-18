"""Trusted development provenance survives Leaf ingestion, settlement and cleanup."""

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_agentd.store import TELEMETRY_RETENTION_MS, AgentdStore
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_retention import maintain_capacity
from edgecitadel_agentd.trace_settlement_apply import page_request
from edgecitadel_agentd.trace_sync_service import TraceSyncService


async def test_leaf_test_priority_retention_preserves_identity_and_active_work(
    topology,  # noqa: F811 - imported fixture
    tmp_path,
    monkeypatch,
    record_property,  # noqa: F811
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator import trace_payloads, trace_retention
    from aggregator.trace_collector import TraceCollectorService
    from aggregator.trace_ingest import ingest_wire
    from aggregator.trace_store import initialize

    stores, syncs, scopes, batches, tasks = [], [], [], [], []
    path = tmp_path / "core.db"
    core_url, core_token = topology.endpoints["core"]
    collector = TraceCollectorService(path, core_url, core_token)
    run_id = str(uuid4())

    async def until(predicate):
        async with asyncio.timeout(100):
            while not predicate():
                await asyncio.sleep(0.05)

    try:
        # Ordinary data arrives first, so test priority cannot pass as FIFO.
        for leaf in ("b", "a"):
            directory = tmp_path / f"source-{leaf}"
            directory.mkdir()
            url, token = topology.endpoints[leaf]
            (directory / "node.json").write_text(
                json.dumps(
                    {
                        "version": 2,
                        "mode": "edge",
                        "agent_id": f"edge-{leaf}",
                        "messaging_mode": "nats_leaf",
                        "jetstream_domain": f"EDGE_{leaf.upper()}",
                        "plugin_nats_url": url,
                        "plugin_nats_token": token,
                    }
                )
            )
            (directory / "node.json").chmod(0o600)
            store = AgentdStore(directory / "agentd/agentd.sqlite3")
            stores.append(store)
            if leaf == "a":
                store.configure_test_source(node_id="edge-a", test_run_id=run_id)
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
                sender_id="origin", recipient_id=f"worker-{leaf}", payload={}
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
            tasks.append(task["task_id"])
            scope = ExportScope(
                *store._connection.execute(
                    "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
                ).fetchone()
            )
            scopes.append(scope)
            batches.append(selected_batch(store, scope))
            assert len(batches[-1]) == 5
            syncs.append(TraceSyncService(directory, store.path, enabled=True))
        collector.start()
        await until(lambda: collector.status()["state"] == "running")
        for store, sync, scope in zip(stores, syncs, scopes, strict=True):
            sync.start()
            await until(
                lambda: page_request(store, scope.values())["after_export_seq"] == 5
            )
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)

        with sqlite3.connect(path) as core:
            initialize(core)
            trace_payloads.prepare(core)
            actual = core.execute(
                "SELECT r.node_id,r.event_id,r.event_sha256,p.event_json FROM trace_raw_events r JOIN trace_payloads p USING(ingest_seq)"
            ).fetchall()
            expected = []
            for batch in batches:
                for record in batch:
                    wrapper = json.loads(record.payload)
                    e = wrapper["event"]
                    expected.append(
                        (e["node_id"], e["event_id"], wrapper["event_sha256"], e)
                    )
            assert {
                (n, i, h, json.dumps(json.loads(e), sort_keys=True))
                for n, i, h, e in actual
            } == {(n, i, h, json.dumps(e, sort_keys=True)) for n, i, h, e in expected}
            assert all(
                json.loads(e).get("test_run_id") == (run_id if n == "edge-a" else None)
                for n, _, _, e in actual
            )
            before = core.execute(
                "SELECT count(*) FROM trace_ingest_positions"
            ).fetchone()[0]
            now = time.time_ns() // 1_000_000
            monkeypatch.setattr(trace_payloads, "BATCH_ROWS", 1)
            result = trace_retention.expire_payloads(
                core, now_ms=now + trace_retention.RETENTION_MS + 1
            )
            assert result["expired_payloads"] == 1
            expired = core.execute(
                "SELECT node_id,event_id FROM trace_raw_events WHERE payload_expired_at_ms IS NOT NULL"
            ).fetchall()
            assert len(expired) == 1 and expired[0][0] == "edge-a"
            for batch in batches:
                for record in batch:
                    ingest_wire(
                        core, record.subject, record.payload, received_at_ms=now
                    )
            assert (
                core.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
                == 10
            )
            assert (
                core.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[
                    0
                ]
                == before
            )
            assert (
                core.execute("SELECT count(*) FROM trace_payloads").fetchone()[0] == 9
            )
            assert core.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

        test_store = stores[1]
        active = test_store.create_task(
            sender_id="origin", recipient_id="worker-a", payload={}
        )
        with test_store._task_transaction():
            # Make the active record age-eligible: preservation must come from
            # its task/settlement obligations, not its recent arrival time.
            test_store._connection.execute(
                "UPDATE trace_journal SET received_at_ms=1 WHERE task_id=?",
                (active["task_id"],),
            )
            assert (
                maintain_capacity(
                    test_store._connection,
                    now_ms=now + TELEMETRY_RETENTION_MS + 2,
                    expire_before_ms=now + 1,
                )
                == 5
            )
        assert test_store.get_task(active["task_id"])["state"] == "queued"
        assert (
            test_store._connection.execute(
                "SELECT count(*) FROM trace_journal WHERE task_id=?",
                (active["task_id"],),
            ).fetchone()[0]
            == 1
        )
        assert (
            test_store._connection.execute(
                "SELECT state FROM trace_spool WHERE journal_event_id IN (SELECT event_id FROM trace_journal WHERE task_id=?)",
                (active["task_id"],),
            ).fetchone()[0]
            == "pending"
        )
        for store, task_id in zip(stores, tasks, strict=True):
            assert store.get_task(task_id)["state"] == "completed"
            assert (
                store._connection.execute("PRAGMA integrity_check").fetchone()[0]
                == "ok"
            )
        reopened = AgentdStore(test_store.path)
        try:
            assert (
                reopened._connection.execute(
                    "SELECT test_run_id FROM trace_sources WHERE active=1"
                ).fetchone()[0]
                == run_id
            )
            assert reopened.get_task(active["task_id"])["state"] == "queued"
        finally:
            reopened.close()
        record_property("exact_events", 10)
        record_property("core_test_payloads_expired_first", 1)
        record_property("source_settled_payloads_reclaimed", 5)
        record_property("active_pending_task_preserved", True)
    finally:
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        for store in stores:
            store.close()
