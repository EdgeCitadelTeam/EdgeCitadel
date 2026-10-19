"""Wall-clock collector outage with continuing local tasks over two real Leaves."""

from functools import partial
import asyncio
import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

import pytest
from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.mark.parametrize(
    "outage_seconds",
    [
        6,
        pytest.param(
            600,
            marks=pytest.mark.skipif(
                os.environ.get("RUN_TRACE_OUTAGE_QUALIFICATION") != "1",
                reason="ten-minute wall-clock qualification opt-in required",
            ),
        ),
    ],
)
async def test_collector_outage_with_continuing_leaf_tasks(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
    outage_seconds,
    record_property,  # noqa: F811
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService

    stores, services, agents = [], [], []
    core_url, core_token = topology.endpoints["core"]
    collector = TraceCollectorService(tmp_path / "core.db", core_url, core_token)
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    base = next(f["event"] for f in fixtures if f["name"] == "tool")
    effects = []
    samples = []
    producer = None
    stop = asyncio.Event()
    generated = 0
    max_delay = 0.0

    def settled():
        return {
            tuple(row)
            for store in stores
            for row in store._connection.execute(
                "SELECT node_id,source_epoch,event_id FROM trace_spool "
                "WHERE state='core_settled' AND core_outcome='accepted'"
            )
        }

    def expected():
        return {
            tuple(row)
            for store in stores
            for row in store._connection.execute(
                "SELECT node_id,source_epoch,event_id FROM trace_journal"
            )
        }

    async def until(predicate, timeout):
        async with asyncio.timeout(timeout):
            while not predicate():
                if producer is not None and producer.done():
                    producer.result()
                    raise AssertionError("workload stopped unexpectedly")
                await asyncio.sleep(0.1)

    def execute(index):
        store, node, connector, token, session = agents[index % len(agents)]
        store.renew_session(
            connector_id=connector, token=token, session_id=session, lease_seconds=300
        )
        task = store.create_task(
            sender_id="origin", recipient_id=connector, payload={"operand": index}
        )
        claimed = store.claim_next_task(
            connector_id=connector, token=token, session_id=session
        )
        assert claimed["task_id"] == task["task_id"]
        store.transition_task(
            task_id=task["task_id"],
            state="running",
            actor_id=connector,
            session_id=session,
        )
        # Deterministic local effect; synthetic metadata supplies the 50-event load.
        effects.append(task["task_id"])
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            for _ in range(45):
                TraceJournal(store._connection).record(
                    node,
                    {
                        **base,
                        "event_id": str(uuid4()),
                        "span_id": str(uuid4()),
                        "agent_id": connector,
                        "task_id": task["task_id"],
                        "trace_id": task["trace_id"],
                        "context_id": None,
                        "execution_attempt_id": None,
                        "occurred_at": store._iso_timestamp(
                            time.time_ns() // 1_000_000
                        ),
                        "evidence_kind": "integration_reported",
                    },
                    selected=True,
                )
        store.transition_task(
            task_id=task["task_id"],
            state="completed",
            actor_id=connector,
            session_id=session,
            result={"value": index + 1},
        )
        assert store.get_task(task["task_id"])["state"] == "completed"

    async def sample(phase, start):
        info = await topology["core"].jetstream().stream_info(STREAM_NAME)
        oldest = min(
            (
                row[0]
                for store in stores
                for row in store._connection.execute(
                    "SELECT min(j.received_at_ms) FROM trace_spool s JOIN trace_journal j "
                    "ON j.node_id=s.node_id AND j.source_epoch=s.source_epoch "
                    "AND j.event_id=s.journal_event_id WHERE s.state!='core_settled'"
                )
                if row[0] is not None
            ),
            default=None,
        )
        entry = {
            "phase": phase,
            "elapsed_seconds": time.monotonic() - start,
            "completed_tasks": len(effects),
            "pending_events": len(expected() - settled()),
            "oldest_pending_seconds": None
            if oldest is None
            else time.time() - oldest / 1000,
            "broker_messages": info.state.messages,
            "broker_bytes": info.state.bytes,
        }
        samples.append(entry)
        print(json.dumps(entry), flush=True)

    try:
        for leaf in ("a", "b"):
            directory = tmp_path / f"source-{leaf}"
            directory.mkdir()
            url, token = topology.endpoints[leaf]
            node = f"edge-{leaf}"
            identity = directory / "node.json"
            identity.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "mode": "edge",
                        "agent_id": node,
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
            services.append(
                TraceSyncService(
                    directory, partial(AgentdStore, store.path), enabled=True
                )
            )
            for number in range(5):
                connector = f"worker-{leaf}-{number}"
                secret = store.register_connector(
                    connector_id=connector,
                    host_type="codex",
                    agent_id=connector,
                    capabilities=["inbox"],
                )
                session = store.open_session(
                    connector_id=connector, token=secret, lease_seconds=300
                )["session_id"]
                agents.append((store, node, connector, secret, session))
        collector.start()
        await until(lambda: collector.status()["state"] == "running", 15)
        for index in range(10):
            execute(index)
        for service in services:
            service.start()
        await until(lambda: expected() == settled(), 90)
        initial = expected()
        assert len(initial) == 500
        await asyncio.to_thread(collector.stop)
        assert collector.status()["state"] == "stopped"
        start = time.monotonic()

        async def produce():
            nonlocal generated, max_delay
            while not stop.is_set():
                due = start + generated * 6  # 600 tasks/hour × 50 events/task.
                try:
                    await asyncio.wait_for(
                        stop.wait(), max(0.001, due - time.monotonic())
                    )
                    break
                except TimeoutError:
                    pass
                max_delay = max(max_delay, time.monotonic() - due)
                execute(10 + generated)
                generated += 1

        producer = asyncio.create_task(produce())
        while time.monotonic() - start < outage_seconds:
            await asyncio.sleep(
                min(30, max(0, outage_seconds - (time.monotonic() - start)))
            )
            assert not producer.done()
            assert collector.status()["state"] == "stopped"
            assert all(process.poll() is None for process in topology.processes)
            await sample("outage", start)
        duration = time.monotonic() - start
        backlog = expected() - initial
        assert len(backlog) >= (outage_seconds // 6) * 50
        assert settled() == initial
        with sqlite3.connect(tmp_path / "core.db") as db:
            assert (
                db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 500
            )
        tasks_before = len(effects)
        recovery_start = time.monotonic()
        collector.start()
        await until(lambda: backlog <= settled(), 300)
        catchup = time.monotonic() - recovery_start
        tasks_during_catchup = len(effects) - tasks_before
        # Ensure there is actual new workload during the recovery observation window.
        await until(lambda: len(effects) > tasks_before, 10)
        await sample("recovered", start)
        stop.set()
        await producer
        producer = None
        await until(lambda: expected() == settled(), 90)
        for service in services:
            await asyncio.to_thread(service.stop)
        await asyncio.to_thread(collector.stop)
        source = {
            tuple(row)
            for store in stores
            for row in store._connection.execute(
                "SELECT node_id,source_epoch,event_id,event_sha256,event_json FROM trace_journal"
            )
        }
        with sqlite3.connect(tmp_path / "core.db") as db:
            actual = set(
                db.execute(
                    "SELECT r.node_id,r.source_epoch,r.event_id,r.event_sha256,p.event_json "
                    "FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
                )
            )
            assert actual == source
            position_columns = "node_id,source_epoch,export_generation,export_seq,event_id,event_sha256"
            source_positions = {
                tuple(row)
                for store in stores
                for row in store._connection.execute(
                    f"SELECT {position_columns} FROM trace_spool"
                )
            }
            assert (
                set(
                    db.execute(f"SELECT {position_columns} FROM trace_ingest_positions")
                )
                == source_positions
            )
            assert len(source_positions) == len(source)
            for table in (
                "trace_ingest_conflicts",
                "trace_rejected_positions",
                "trace_loss_ranges",
            ):
                assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
        tasks = {
            row[0]
            for store in stores
            for row in store._connection.execute(
                "SELECT task_id FROM tasks WHERE state='completed'"
            )
        }
        assert len(effects) == len(set(effects))
        assert tasks == set(effects)
        assert len(source) == len(effects) * 50
        result = {
            "outage_seconds": duration,
            "catchup_seconds": catchup,
            "backlog_events": len(backlog),
            "completed_tasks": len(tasks),
            "exact_events": len(source),
            "max_schedule_delay_seconds": max_delay,
            "new_tasks_before_backlog_settled": tasks_during_catchup,
            "new_tasks_during_recovery_window": len(effects) - tasks_before,
            "identity_payload_digest": hashlib.sha256(
                json.dumps(sorted(source)).encode()
            ).hexdigest(),
            "samples": samples,
            "scope": "Two real Leaves; ten local task agents; synthetic tool metadata; no real adapters or cross-Edge task routing",
        }
        record_property("outage_evidence", json.dumps(result))
        print(json.dumps(result), flush=True)
    finally:
        stop.set()
        if producer is not None:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)
        for service in services:
            await asyncio.to_thread(service.stop)
        await asyncio.to_thread(collector.stop)
        for store in stores:
            store.close()
