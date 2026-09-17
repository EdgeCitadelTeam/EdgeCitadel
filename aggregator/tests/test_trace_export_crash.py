"""Actual SIGKILL at exporter, collector ACK and source settlement boundaries."""

import asyncio
import json
import os
import secrets
import select
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from aggregator.trace_ingest import ingest_delivery
from aggregator.trace_settlement import settlement_page_reply
from aggregator.trace_store import initialize
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from tests.nats_server import NatsServer

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_exporter import ExportScope, TraceExporter
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from edgecitadel_plugin_runtime.telemetry_stream import (
    CONSUMER_NAME,
    EVENT_SUBJECT,
    STREAM_NAME,
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)

ROOT = Path(__file__).resolve().parents[2]
POINTS = (
    "before_publish",
    "broker_ack_before_checkpoint",
    "after_checkpoint",
    "core_commit_before_ack",
    "settlement_before_retirement",
)


def barrier():
    print("ready", flush=True)
    while True:
        signal.pause()


async def child(config):
    store = AgentdStore(Path(config["source"]))
    event = json.loads(
        (ROOT / "agent-runtime/tests/fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    event["event_id"] = config["event_id"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize("edge-a")
        journal.record("edge-a", event, selected=True)
    scope = ExportScope("edge-a", epoch, generation)
    if config["point"] == "before_publish":
        barrier()
    nc = NATS()
    await nc.connect(servers=[config["url"]], token=config["token"])
    if config["point"] == "broker_ack_before_checkpoint":
        import edgecitadel_agentd.trace_exporter as exporter

        exporter.checkpoint_broker_ack = lambda *args: barrier()
    await TraceExporter(store, nc.jetstream(), scope).publish_batch()
    if config["point"] == "after_checkpoint":
        barrier()
    db = sqlite3.connect(config["core"])
    initialize(db)
    consumer = await nc.jetstream().pull_subscribe(
        EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
    )
    (message,) = await consumer.fetch(1, timeout=5)
    if config["point"] == "core_commit_before_ack":

        async def no_ack(*args, **kwargs):
            barrier()

        Msg.ack_sync = no_ack
    await ingest_delivery(db, message)
    request = page_request(store, scope.values())
    apply_page(store, request, settlement_page_reply(db, request))
    barrier()


def kill_child(config, config_path):
    config_path.write_text(json.dumps(config))
    config_path.chmod(0o600)
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), str(config_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "agent-runtime/src"))),
        },
    )
    try:
        assert select.select([process.stdout], [], [], 15)[0], (
            "child did not reach barrier"
        )
        assert process.stdout.readline() == "ready\n", "child exited before barrier"
        process.kill()
        assert process.wait(timeout=10) == -signal.SIGKILL
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        process.stdout.close()
        process.stderr.close()
        config_path.unlink()


@pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1", reason="owned NATS required"
)
@pytest.mark.parametrize("point", POINTS)
@pytest.mark.asyncio
async def test_sigkill_recovers_exact_evidence_without_creating_tasks(tmp_path, point):
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    nc = NATS()
    source_path, core_path = tmp_path / "source.db", tmp_path / "core.db"
    store = None
    db = sqlite3.connect(core_path)
    event_id = str(uuid4())
    try:
        initialize(db)
        await nc.connect(servers=[server.url], token=server.token)
        js = nc.jetstream()
        await ensure_telemetry_stream(js)
        await ensure_telemetry_consumer(js)
        await asyncio.to_thread(
            kill_child,
            {
                "source": str(source_path),
                "core": str(core_path),
                "url": server.url,
                "token": server.token,
                "point": point,
                "event_id": event_id,
            },
            tmp_path / "child.json",
        )
        store = AgentdStore(source_path)
        scope = ExportScope(
            *store._connection.execute(
                "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
            ).fetchone()
        )
        state = store._connection.execute("SELECT state FROM trace_spool").fetchone()[0]
        expected_state = (
            "pending"
            if point in POINTS[:2]
            else "core_settled"
            if point == POINTS[4]
            else "broker_acked"
        )
        assert state == expected_state
        assert (
            store._connection.execute("SELECT event_id FROM trace_journal").fetchall()[
                0
            ][0]
            == event_id
        )
        assert (await js.stream_info(STREAM_NAME)).state.messages == (
            0 if point == POINTS[0] else 1
        )
        assert db.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == int(
            point in POINTS[3:]
        )
        if point != POINTS[4]:
            assert page_request(store, scope.values())["after_export_seq"] == 0
            await TraceExporter(store, js, scope).publish_batch(replay=True)
            # Duplicate publication after a lost local checkpoint reuses the broker ID.
            assert (await js.stream_info(STREAM_NAME)).state.messages == 1
            consumer = await js.pull_subscribe(
                EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
            )
            (message,) = await consumer.fetch(1, timeout=40)
            if point == POINTS[3]:
                assert message.metadata.num_delivered == 2
            await ingest_delivery(db, message)
            request = page_request(store, scope.values())
            apply_page(store, request, settlement_page_reply(db, request))
        else:
            assert await TraceExporter(store, js, scope).publish_batch() == 0
        assert db.execute("SELECT event_id FROM trace_raw_events").fetchall() == [
            (event_id,)
        ]
        assert (
            db.execute("SELECT count(*) FROM trace_ingest_positions").fetchone()[0] == 1
        )
        assert db.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0] == 1
        assert (await js.consumer_info(STREAM_NAME, CONSUMER_NAME)).num_ack_pending == 0
        source_record = tuple(
            store._connection.execute(
                "SELECT event_id,event_sha256,event_json FROM trace_journal"
            ).fetchone()
        )
        assert (
            db.execute(
                "SELECT event_id,event_sha256,event_json FROM trace_raw_events"
            ).fetchone()
            == source_record
        )
        store.close()
        store = AgentdStore(source_path)
        assert page_request(store, scope.values())["after_export_seq"] == 1
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 1
        )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
    finally:
        if store is not None:
            store.close()
        db.close()
        await nc.close()
        await asyncio.to_thread(server.close)


if __name__ == "__main__":
    asyncio.run(child(json.loads(Path(sys.argv[1]).read_text())))
