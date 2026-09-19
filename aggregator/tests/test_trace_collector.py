import asyncio
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from uuid import uuid4

import pytest
from aggregator.main import make_app
from fastapi.testclient import TestClient
from nats.aio.client import Client as NATS
from tests.nats_server import NatsServer

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_sync_service import TraceSyncService
from edgecitadel_plugin_runtime.telemetry_stream import CONSUMER_NAME, STREAM_NAME


@pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1",
    reason="owned NATS opt-in required",
)
@pytest.mark.parametrize("startup_iteration", range(3))
@pytest.mark.asyncio
async def test_production_core_startup_and_source_sync_settle_and_stop(
    tmp_path, monkeypatch, startup_iteration
):
    server = await asyncio.to_thread(
        NatsServer(token=secrets.token_hex(32), jetstream=True).start
    )
    db_path = tmp_path / "core.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("NATS_URL", server.url)
    monkeypatch.setenv("NATS_TOKEN", server.token)
    monkeypatch.setenv("EDGECITADEL_TRACE_COLLECTOR", "1")
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", "test-collector-admin")

    def run():
        node_dir = tmp_path / "source"
        node_dir.mkdir()
        (node_dir / "node.json").write_text(
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
        source = AgentdStore(node_dir / "agentd" / "agentd.sqlite3")
        event = json.loads(
            (
                Path(__file__).parents[2]
                / "agent-runtime/tests/fixtures/traces/events.v1.json"
            ).read_text()
        )["fixtures"][0]["event"]
        with source._connection:
            source._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(source._connection)
            journal.initialize("edge-a")
            journal.record("edge-a", event, selected=True)
        sync = TraceSyncService(node_dir, source.path, enabled=True)
        try:
            with TestClient(make_app()) as client:
                deadline = time.monotonic() + 5
                while (
                    client.get("/api/system/status").json()["telemetry"]["state"]
                    != "running"
                ):
                    assert time.monotonic() < deadline, client.get(
                        "/api/system/status"
                    ).json()
                    time.sleep(0.02)
                with sqlite3.connect(db_path) as core:
                    core.execute(
                        "CREATE TRIGGER fail_poison BEFORE INSERT ON trace_poison_counts BEGIN SELECT RAISE(ABORT, 'injected'); END"
                    )

                async def broker_probe(publish=False):
                    nc = NATS()
                    await nc.connect(servers=[server.url], token=server.token)
                    try:
                        js = nc.jetstream()
                        if publish:
                            await js.publish(
                                "edgecitadel.telemetry.v1.edge-a", b"invalid-wire"
                            )
                        return (
                            await js.consumer_info(STREAM_NAME, CONSUMER_NAME)
                        ).num_ack_pending
                    finally:
                        await nc.close()

                asyncio.run(broker_probe(publish=True))
                sync.start()
                deadline = time.monotonic() + 12
                while (
                    source._connection.execute(
                        "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                    ).fetchone()[0]
                    != 1
                ):
                    assert time.monotonic() < deadline, (
                        sync.status(),
                        client.get("/api/system/status").json(),
                    )
                    time.sleep(0.02)
                status = client.get("/api/system/status").json()
                assert (
                    status["nats_connected"]
                    and status["telemetry"]["state"] == "running"
                )
                with sqlite3.connect(db_path) as core:
                    assert (
                        core.execute(
                            "SELECT event_id FROM trace_raw_events"
                        ).fetchone()[0]
                        == event["event_id"]
                    )
                    epoch = core.execute(
                        "SELECT collector_epoch FROM trace_collector"
                    ).fetchone()[0]
                assert (
                    source._connection.execute("SELECT count(*) FROM tasks").fetchone()[
                        0
                    ]
                    == 0
                )
                assert asyncio.run(broker_probe()) >= 1
                with sqlite3.connect(db_path) as core:
                    core.execute("DROP TRIGGER fail_poison")
                deadline = time.monotonic() + 5
                while asyncio.run(broker_probe()):
                    assert time.monotonic() < deadline, client.get(
                        "/api/system/status"
                    ).json()
                    time.sleep(0.02)
                with sqlite3.connect(db_path) as core:
                    assert (
                        core.execute(
                            "SELECT observations FROM trace_poison_counts WHERE reason='wire'"
                        ).fetchone()[0]
                        == 1
                    )
                headers = {"X-EdgeCitadel-Admin-Token": "test-collector-admin"}
                endpoint = "/api/system/telemetry/control"
                assert client.post(endpoint, json={"action": "stop"}).status_code == 401
                assert (
                    client.post(
                        endpoint, headers=headers, json={"action": "stop"}
                    ).json()["state"]
                    == "stopped"
                )
                with source._connection:
                    source._connection.execute("BEGIN IMMEDIATE")
                    queued = TraceJournal(source._connection).record(
                        "edge-a", {**event, "event_id": str(uuid4())}, selected=True
                    )
                deadline = time.monotonic() + 5
                while (
                    source._connection.execute(
                        "SELECT state FROM trace_spool WHERE export_seq=2"
                    ).fetchone()[0]
                    != "broker_acked"
                ):
                    assert time.monotonic() < deadline
                    time.sleep(0.02)
                with sqlite3.connect(db_path) as core:
                    assert (
                        core.execute(
                            "SELECT count(*) FROM trace_raw_events"
                        ).fetchone()[0]
                        == 1
                    )
                assert (
                    client.post(
                        endpoint, headers=headers, json={"action": "retry"}
                    ).status_code
                    == 200
                )
                deadline = time.monotonic() + 5
                with sqlite3.connect(db_path) as core:
                    while (
                        core.execute(
                            "SELECT count(*) FROM trace_raw_events WHERE event_id=?",
                            (queued["event_id"],),
                        ).fetchone()[0]
                        != 1
                    ):
                        assert time.monotonic() < deadline
                        time.sleep(0.02)
                    assert (
                        core.execute(
                            "SELECT collector_epoch FROM trace_collector"
                        ).fetchone()[0]
                        == epoch
                    )
                deadline = time.monotonic() + 5
                while (
                    client.get("/api/system/status").json()["telemetry"]["metrics"][
                        "ack_successes"
                    ]
                    < 3
                ):
                    assert time.monotonic() < deadline
                    time.sleep(0.02)
                metrics = client.get("/api/system/status").json()["telemetry"][
                    "metrics"
                ]
                assert metrics["commit_observations"]["accepted"] >= 2
                assert metrics["commit_observations"]["quarantined"] == 1
                assert metrics["persistence_failures"] >= 1
                assert metrics["last_commit_observed_at_ms"] is not None
                sync.stop()
                assert client.get("/api/system/status").json()["nats_connected"]
            # A fresh Core lifecycle keeps the committed collector identity.
            with TestClient(make_app()) as client:
                deadline = time.monotonic() + 5
                while (
                    client.get("/api/system/status")
                    .json()["telemetry"]
                    .get("collector_epoch")
                    != epoch
                ):
                    assert time.monotonic() < deadline, client.get(
                        "/api/system/status"
                    ).json()
                    time.sleep(0.02)
        finally:
            sync.stop()
            source.close()

    try:
        await asyncio.to_thread(run)
    finally:
        await asyncio.to_thread(server.close)


@pytest.mark.asyncio
async def test_aggregator_stops_inbox_even_if_fetch_converts_cancellation_to_timeout():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from aggregator.aggregator import AggregatorApp
    from nats.errors import TimeoutError as NatsTimeoutError

    entered = asyncio.Event()

    async def fetch(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise NatsTimeoutError() from None

    app = AggregatorApp.__new__(AggregatorApp)
    app._stopping = False
    nc = SimpleNamespace(is_closed=False, drain=AsyncMock())
    app.router = SimpleNamespace(nc=nc)
    subscription = SimpleNamespace(fetch=fetch, unsubscribe=AsyncMock())
    app._inbox_subscription = subscription
    app._inbox_task = asyncio.create_task(app._drain_own_inbox(subscription))
    await entered.wait()
    await asyncio.wait_for(app.stop(), timeout=2)
    subscription.unsubscribe.assert_awaited_once()
    nc.drain.assert_awaited_once()
    assert app._inbox_task is None
    assert app._inbox_subscription is None


@pytest.mark.asyncio
async def test_transient_initial_database_lock_retries_without_operator_restart(
    tmp_path, monkeypatch
):
    from aggregator.trace_collector import TraceCollectorService

    collector = TraceCollectorService(tmp_path / "db", "nats://unused", "unused")
    attempts = 0

    async def run():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("database is locked")
        collector._stop.set()

    monkeypatch.setattr(collector, "_run", run)
    await collector._entry()
    assert attempts == 2
    assert collector._loop is None and collector._task is None
