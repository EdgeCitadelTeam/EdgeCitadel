"""Production telemetry lifecycles survive Core broker loss through a real Leaf."""

import asyncio
import json
import shutil
import sqlite3
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from nats.js.errors import NotFoundError
from test_telemetry_leaf import pytestmark, topology  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import page_request
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime.jetstream import ensure_stream
from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME


@pytest.mark.parametrize("topology", ["broker_restart", "link"], indirect=True)
async def test_core_or_link_recovery_preserves_leaf_spool_and_reconciles(
    topology,  # noqa: F811 - imported pytest fixture
    tmp_path,
    monkeypatch,
):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_collector import TraceCollectorService

    core_url, core_token = topology.endpoints["core"]
    leaf_url, leaf_token = topology.endpoints["a"]
    directory = tmp_path / "source"
    directory.mkdir()
    (directory / "node.json").write_text(
        json.dumps(
            {
                "version": 2,
                "mode": "edge",
                "agent_id": "edge-a",
                "messaging_mode": "nats_leaf",
                "jetstream_domain": "EDGE_A",
                "plugin_nats_url": leaf_url,
                "plugin_nats_token": leaf_token,
            }
        )
    )
    (directory / "node.json").chmod(0o600)
    store = AgentdStore(directory / "agentd/agentd.sqlite3")
    sync = TraceSyncService(directory, store.path, enabled=True)
    collector = TraceCollectorService(tmp_path / "core.db", core_url, core_token)
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    expected = set()

    def put():
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            stamped = journal.record(
                "edge-a", {**event, "event_id": str(uuid4())}, selected=True
            )
            expected.add(stamped["event_id"])
        return ("edge-a", epoch, generation)

    async def until(predicate, timeout=65):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.05)

    try:
        collector.start()
        await until(lambda: collector.status()["state"] == "running", 15)
        scope = put()
        sync.start()
        await until(lambda: page_request(store, scope)["after_export_seq"] == 1)
        collector_epoch = page_request(store, scope)["collector_epoch"]
        local = topology["a"].jetstream(domain="EDGE_A")
        await ensure_stream(local, "edge-a")
        core_process = topology.processes[0]
        if topology.link is None:
            core_process.terminate()
            await asyncio.to_thread(core_process.wait, timeout=5)
        else:
            assert topology.link.connections
            await asyncio.wait_for(topology.link.cut(), timeout=5)
            assert all(process.poll() is None for process in topology.processes)
            await topology["core"].flush()
            assert (
                await topology["core"].jetstream().stream_info(STREAM_NAME)
            ).state.messages == 1
        for _ in range(5):
            put()
        await asyncio.sleep(6)  # Actual outage exceeds the publish request timeout.
        assert page_request(store, scope)["after_export_seq"] == 1
        assert (
            store._connection.execute(
                "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
            ).fetchone()[0]
            == 1
        )
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 6
        )
        # Leaf-local command capture works while its Core link is unavailable.
        ack = await local.publish("agents.edge-a.inbox", b"owned-local-command")
        assert ack.stream == "AGENT_INBOX"
        assert (await local.stream_info("AGENT_INBOX")).state.messages == 1
        with pytest.raises(NotFoundError):
            await local.stream_info(STREAM_NAME)
        if topology.link is None:
            replacement = await asyncio.to_thread(
                subprocess.Popen,
                [shutil.which("nats-server"), "-c", str(tmp_path / "core/nats.conf")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            topology.processes.append(replacement)
        else:
            assert all(process.poll() is None for process in topology.processes)
            assert not topology.link.connections
            with sqlite3.connect(tmp_path / "core.db") as core:
                assert (
                    core.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0]
                    == 1
                )
            await topology.link.start()
            replacement = core_process
        await until(lambda: page_request(store, scope)["after_export_seq"] == 6)
        assert replacement.poll() is None
        if topology.link is not None:
            assert topology.link.connections
            assert all(process.poll() is None for process in topology.processes)
        assert page_request(store, scope)["collector_epoch"] == collector_epoch
        with sqlite3.connect(tmp_path / "core.db") as core:
            actual = {
                row[0] for row in core.execute("SELECT event_id FROM trace_raw_events")
            }
            assert actual == expected
            assert (
                core.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[
                    0
                ]
                == 6
            )
            source_payloads = {
                tuple(row)
                for row in store._connection.execute(
                    "SELECT event_id,event_sha256,event_json FROM trace_journal"
                )
            }
            assert (
                set(
                    core.execute(
                        "SELECT r.event_id,r.event_sha256,p.event_json "
                        "FROM trace_raw_events r LEFT JOIN trace_payloads p USING(ingest_seq)"
                    )
                )
                == source_payloads
            )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        await asyncio.to_thread(sync.stop)
        await asyncio.to_thread(collector.stop)
        store.close()
