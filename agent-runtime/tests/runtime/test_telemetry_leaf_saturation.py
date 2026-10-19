"""Configured telemetry saturation preserves cross-Leaf task delivery and replay."""

from functools import partial
import asyncio
import json
import sqlite3
from pathlib import Path

from nats.js.errors import ServiceUnavailableError
from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_agentd.transport import AgentdNatsTransport
from edgecitadel_plugin_runtime.jetstream import ensure_stream
from edgecitadel_plugin_runtime.telemetry_stream import (
    STREAM_BYTES,
    STREAM_NAME,
    ensure_telemetry_stream,
)


async def test_configured_stream_saturation_keeps_cross_leaf_tasks_running(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    record_property,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService

    stores, transports, syncs, credentials = [], [], [], []
    collector = None
    samples = []
    effects = []

    def sample(phase):
        usage = {}
        for broker in ("core", "a", "b"):
            files = [
                p.stat() for p in (tmp_path / broker / "js").rglob("*") if p.is_file()
            ]
            usage[broker] = {
                "named_file_bytes": sum(s.st_size for s in files),
                "allocated_bytes": sum(s.st_blocks * 512 for s in files),
            }
        samples.append({"phase": phase, "brokers": usage})

    async def until(predicate, timeout=30):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.05)

    def tuples(table, columns):
        return {
            tuple(row)
            for store in stores
            for row in store._connection.execute(f"SELECT {columns} FROM {table}")
        }

    try:
        core_js = topology["core"].jetstream()
        await ensure_stream(core_js, "aggregator")
        await ensure_telemetry_stream(core_js)
        for leaf, agent in (("a", "caller"), ("b", "worker")):
            directory = tmp_path / f"source-{leaf}"
            directory.mkdir()
            url, token = topology.endpoints[leaf]
            identity = directory / "node.json"
            identity.write_text(
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
            identity.chmod(0o600)
            store = AgentdStore(directory / "agentd/agentd.sqlite3")
            stores.append(store)
            secret = store.register_connector(
                connector_id=agent,
                host_type="codex",
                agent_id=agent,
                capabilities=["inbox"],
            )
            session = store.open_session(
                connector_id=agent, token=secret, lease_seconds=300
            )["session_id"]
            credentials.append((agent, secret, session))
            transport = AgentdNatsTransport(directory, store)
            transports.append(transport)
            transport.start()
            syncs.append(
                TraceSyncService(
                    directory, partial(AgentdStore, store.path), enabled=True
                )
            )
        await until(
            lambda: all(t.status().get("ready_inbox_count") == 1 for t in transports)
        )
        sample("before_fill")
        leaf_js = topology["a"].jetstream()
        filled = 0
        # Only fixture filler is published before saturation. There is no collector,
        # authoritative source event or implied retention claim for these bytes.
        for size in (18 * 1024, 512):
            while True:
                try:
                    await leaf_js.publish(
                        "edgecitadel.telemetry.v1.fixture", b"x" * size
                    )
                    filled += 1
                    if filled % 512 == 0:
                        sample("filling")
                except ServiceUnavailableError as error:
                    assert error.err_code == 10077
                    break
        full = await core_js.stream_info(STREAM_NAME)
        assert full.config.max_bytes == STREAM_BYTES == 128 * 1024 * 1024
        assert 0 <= STREAM_BYTES - full.state.bytes < 1024
        assert full.state.messages == filled
        sample("full")
        for sync in syncs:
            sync.start()
        caller, worker = stores
        agent, secret, session = credentials[1]
        for index in range(10):
            task = caller.create_task(
                sender_id="caller", recipient_id="worker", payload={"operand": index}
            )
            await until(
                lambda: worker._connection.execute(
                    "SELECT count(*) FROM tasks WHERE task_id=?", (task["task_id"],)
                ).fetchone()[0]
                == 1
            )
            claimed = worker.claim_next_task(
                connector_id=agent, token=secret, session_id=session
            )
            assert claimed["task_id"] == task["task_id"]
            worker.transition_task(
                task_id=task["task_id"],
                state="running",
                actor_id=agent,
                session_id=session,
            )
            effects.append(task["task_id"])
            worker.transition_task(
                task_id=task["task_id"],
                state="completed",
                actor_id=agent,
                session_id=session,
                result={"value": index + 1},
            )
            await until(
                lambda: caller.get_task(task["task_id"])["state"] == "completed"
            )
            assert caller.get_task(task["task_id"])["result"] == {
                "value": index + 1,
                "trace_id": task["trace_id"],
                "execution_context": {
                    "schema_version": 1,
                    "context_origin": "legacy_default",
                    "parent_run_id": None,
                },
            }
            assert all(t.status()["connected"] for t in transports)
            sample("task_completed_at_saturation")
        assert len(effects) == len(set(effects)) == 10
        assert all(
            store._connection.execute(
                "SELECT count(*) FROM tasks WHERE state='completed'"
            ).fetchone()[0]
            == 10
            for store in stores
        )
        assert all(
            store._connection.execute(
                "SELECT count(*) FROM trace_spool WHERE state!='pending'"
            ).fetchone()[0]
            == 0
            for store in stores
        )
        retained = tuples(
            "trace_journal", "node_id,source_epoch,event_id,event_sha256,event_json"
        )
        assert len(retained) > 0
        assert (await core_js.stream_info(STREAM_NAME)).state.messages == filled
        # Prove actual exporter refusals, not merely an unscheduled worker.
        await until(
            lambda: all(
                s.status()["metrics"]["counts"].get("publish_failures", 0) > 0
                for s in syncs
            )
        )
        failures = [s.status()["metrics"]["counts"]["publish_failures"] for s in syncs]
        # This removes only identified synthetic filler. All source payloads are
        # still pending locally and the collector has never consumed this stream.
        await core_js.purge_stream(STREAM_NAME)
        url, token = topology.endpoints["core"]
        collector = TraceCollectorService(tmp_path / "core.db", url, token)
        collector.start()
        await until(
            lambda: all(
                store._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state!='core_settled'"
                ).fetchone()[0]
                == 0
                for store in stores
            ),
            120,
        )
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        with sqlite3.connect(tmp_path / "core.db") as db:
            assert (
                set(
                    db.execute(
                        "SELECT r.node_id,r.source_epoch,r.event_id,r.event_sha256,p.event_json FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
                    )
                )
                == retained
            )
            columns = "node_id,source_epoch,export_generation,export_seq,event_id,event_sha256"
            assert set(
                db.execute(f"SELECT {columns} FROM trace_ingest_positions")
            ) == tuples("trace_spool", columns)
            for table in (
                "trace_ingest_conflicts",
                "trace_rejected_positions",
                "trace_loss_ranges",
            ):
                assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        assert all(
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 10
            for store in stores
        )
        assert len(effects) == 10
        sample("recovered")
        evidence = {
            "stream_limit_bytes": STREAM_BYTES,
            "full_stream_logical_bytes": full.state.bytes,
            "filler_messages": filled,
            "cross_leaf_tasks_completed_once": len(effects),
            "retained_and_reconciled_events": len(retained),
            "source_publish_failures": failures,
            "sampled_peak_combined_broker_allocated_bytes": max(
                sum(b["allocated_bytes"] for b in item["brokers"].values())
                for item in samples
            ),
            "samples": samples,
            "scope": "Production stream quota and task transports; fixture effects; synthetic fill/purge; sampled broker files, not whole-filesystem hard bounds",
        }
        record_property("saturation_evidence", json.dumps(evidence))
        print(
            json.dumps({k: v for k, v in evidence.items() if k != "samples"}),
            flush=True,
        )
    finally:
        for sync in syncs:
            await asyncio.to_thread(sync.stop)
        if collector is not None:
            await asyncio.to_thread(collector.stop)
        for transport in transports:
            await asyncio.to_thread(transport.stop)
        for store in stores:
            store.close()
