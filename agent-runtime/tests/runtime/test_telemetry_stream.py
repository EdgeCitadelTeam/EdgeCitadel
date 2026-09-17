"""Owned broker evidence for isolation, quota, explicit ACK and drift refusal."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import shutil
import socket
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy
from nats.js.errors import APIError, NotFoundError, ServiceUnavailableError

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_exporter import ExportScope, TraceExporter, selected_batch
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_plugin_runtime import telemetry_stream as telemetry
from edgecitadel_plugin_runtime.jetstream import ensure_stream

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1",
        reason="owned NATS integration opt-in required",
    ),
]


@pytest_asyncio.fixture
async def broker(tmp_path, request):
    binary = shutil.which("nats-server")
    assert binary, "explicit qualification requires nats-server"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    token = secrets.token_urlsafe(32)
    storage_limit = getattr(request, "param", "2GB")
    config = tmp_path / "nats.conf"
    config.write_text(
        f'host: "127.0.0.1"\nport: {port}\nauthorization {{ token: {json.dumps(token)} }}\njetstream {{ store_dir: {json.dumps(str(tmp_path / "js"))}, max_file_store: {storage_limit} }}\n'
    )
    config.chmod(0o600)
    process = await asyncio.to_thread(
        subprocess.Popen,
        [binary, "-c", str(config)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = NATS()
    try:
        for _ in range(100):
            assert process.poll() is None, "owned broker exited during startup"
            try:
                _reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                await asyncio.sleep(0.02)
        await client.connect(
            servers=[f"nats://127.0.0.1:{port}"],
            token=token,
            allow_reconnect=False,
            connect_timeout=1,
        )
        yield client
    finally:
        await client.close()
        process.terminate()
        await asyncio.to_thread(process.wait, timeout=5)


async def test_isolated_stream_and_explicit_ack_preserve_durable_cursor(broker):
    js = broker.jetstream()
    await ensure_stream(js, "worker")
    command_before = (await js.stream_info("AGENT_INBOX")).config.as_dict()
    await telemetry.ensure_telemetry_stream(js)
    await telemetry.ensure_telemetry_consumer(js)
    ack = await js.publish("edgecitadel.telemetry.v1.node-a", b"owned-event")
    assert ack.stream == telemetry.STREAM_NAME
    consumer = await js.pull_subscribe(
        telemetry.EVENT_SUBJECT,
        durable=telemetry.CONSUMER_NAME,
        stream=telemetry.STREAM_NAME,
    )
    (message,) = await consumer.fetch(1, timeout=1)
    assert message.data == b"owned-event"
    assert (
        await js.consumer_info(telemetry.STREAM_NAME, telemetry.CONSUMER_NAME)
    ).num_ack_pending == 1
    await message.ack_sync()
    before = await js.consumer_info(telemetry.STREAM_NAME, telemetry.CONSUMER_NAME)
    await telemetry.ensure_telemetry_stream(js)
    after = await telemetry.ensure_telemetry_consumer(js)
    assert before.ack_floor == after.ack_floor
    assert after.num_ack_pending == 0 and after.config.ack_policy == AckPolicy.EXPLICIT
    assert (await js.stream_info("AGENT_INBOX")).config.as_dict() == command_before
    # Broker consumption is independent of the future Core settlement protocol.
    assert (await js.stream_info(telemetry.STREAM_NAME)).state.messages == 1


async def test_exporter_lost_ack_retries_same_broker_identity_and_retains_payload(
    broker, tmp_path
):
    js = broker.jetstream()
    await telemetry.ensure_telemetry_stream(js)
    store = AgentdStore(tmp_path / "agentd" / "state.sqlite3")
    try:
        event = json.loads(
            (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
        )["fixtures"][0]["event"]
        event["event_id"] = str(uuid4())
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            journal.record("edge-a", event, selected=True)
        scope = ExportScope("edge-a", epoch, generation)
        original = selected_batch(store, scope)[0]
        acknowledgments = []

        class LoseFirstAck:
            async def publish(self, *args, **kwargs):
                ack = await js.publish(*args, **kwargs)
                acknowledgments.append(ack)
                if len(acknowledgments) == 1:
                    raise NatsTimeoutError()
                return ack

        exporter = TraceExporter(store, LoseFirstAck(), scope)
        with pytest.raises(NatsTimeoutError):
            await exporter.publish_batch()
        assert selected_batch(store, scope) == [original]
        assert await exporter.publish_batch() == 1
        assert acknowledgments[1].duplicate is True
        assert acknowledgments[0].seq == acknowledgments[1].seq
        assert (await js.stream_info(telemetry.STREAM_NAME)).state.messages == 1
        assert selected_batch(store, scope) == []
        assert selected_batch(store, scope, replay=True) == [original]
        persisted = await js.get_msg(telemetry.STREAM_NAME, seq=acknowledgments[0].seq)
        assert persisted.data == original.payload
        assert persisted.headers["Nats-Msg-Id"] == original.message_id
        # Stream eviction cannot settle or delete the retained local event.
        await js.delete_msg(telemetry.STREAM_NAME, acknowledgments[0].seq)
        assert selected_batch(store, scope, replay=True) == [original]
        row = store._connection.execute(
            "SELECT state,collector_epoch FROM trace_spool"
        ).fetchone()
        assert tuple(row) == ("broker_acked", None)
    finally:
        store.close()


async def test_full_telemetry_stream_rejects_new_data_but_command_stream_still_accepts(
    broker, monkeypatch
):
    monkeypatch.setattr(telemetry, "STREAM_BYTES", 2048)
    js = broker.jetstream()
    await ensure_stream(js, "worker")
    await telemetry.ensure_telemetry_stream(js)
    await js.publish("edgecitadel.telemetry.v1.node-a", b"a" * 1000)
    with pytest.raises(ServiceUnavailableError) as error:
        await js.publish("edgecitadel.telemetry.v1.node-a", b"b" * 1500)
    assert error.value.err_code == 10077
    info = await js.stream_info(telemetry.STREAM_NAME)
    assert info.state.messages == 1 and info.state.bytes <= 2048
    assert (
        await js.publish("agents.worker.inbox", b"owned-command")
    ).stream == "AGENT_INBOX"


async def test_stream_and_consumer_drift_is_refused_without_rewriting(broker):
    js = broker.jetstream()
    stream = await telemetry.ensure_telemetry_stream(js)
    stream.config.max_age = 1800
    await js.update_stream(stream.config)
    with pytest.raises(
        telemetry.TelemetryConfigurationError, match="stream_configuration_mismatch"
    ):
        await telemetry.ensure_telemetry_stream(js)
    assert (await js.stream_info(telemetry.STREAM_NAME)).config.max_age == 1800
    await js.update_stream(telemetry.stream_config())
    consumer = await telemetry.ensure_telemetry_consumer(js)
    consumer.config.max_ack_pending = 1
    await js.add_consumer(telemetry.STREAM_NAME, consumer.config)
    with pytest.raises(
        telemetry.TelemetryConfigurationError, match="consumer_configuration_mismatch"
    ):
        await telemetry.ensure_telemetry_consumer(js)
    assert (
        await js.consumer_info(telemetry.STREAM_NAME, telemetry.CONSUMER_NAME)
    ).config.max_ack_pending == 1


@pytest.mark.parametrize("broker", ["1GB"], indirect=True)
async def test_existing_broker_budget_cannot_reserve_both_streams(broker):
    js = broker.jetstream()
    await ensure_stream(js, "worker")
    before = (await js.stream_info("AGENT_INBOX")).config.as_dict()
    with pytest.raises(APIError) as error:
        await telemetry.ensure_telemetry_stream(js)
    assert error.value.err_code == 10047
    with pytest.raises(NotFoundError):
        await js.stream_info(telemetry.STREAM_NAME)
    assert (await js.stream_info("AGENT_INBOX")).config.as_dict() == before
    assert (
        await js.publish("agents.worker.inbox", b"owned-command")
    ).stream == "AGENT_INBOX"


async def test_opt_in_agentd_service_exports_and_settles_over_owned_broker(
    broker, tmp_path, monkeypatch
):
    import sqlite3
    import threading

    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_ingest import ingest_delivery
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    from edgecitadel_agentd.client import AgentdClient
    from edgecitadel_agentd.service import serve, socket_path_for
    from edgecitadel_agentd.trace_settlement_pages import SETTLEMENT_PAGE_SUBJECT

    node = tmp_path / "node.json"
    node.write_text(
        json.dumps(
            {
                "version": 1,
                "mode": "core",
                "agent_id": "edge-a",
                "nats_url": broker.connected_url.geturl(),
                "nats_token": broker.options["token"],
            }
        )
    )
    node.chmod(0o600)
    state = tmp_path / "agentd"
    source = AgentdStore(state / "agentd.sqlite3")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with source._connection:
        source._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(source._connection)
        journal.initialize("edge-a")
        journal.record("edge-a", event, selected=True)
    source.close()
    core = sqlite3.connect(tmp_path / "core.sqlite3")
    initialize(core)
    js = broker.jetstream()
    await telemetry.ensure_telemetry_stream(js)
    await telemetry.ensure_telemetry_consumer(js)
    subscription = await js.pull_subscribe(
        telemetry.EVENT_SUBJECT,
        durable=telemetry.CONSUMER_NAME,
        stream=telemetry.STREAM_NAME,
    )

    async def respond(message):
        await message.respond(
            json.dumps(settlement_page_reply(core, json.loads(message.data))).encode()
        )

    control = await broker.subscribe(SETTLEMENT_PAGE_SUBJECT, cb=respond)
    await broker.flush()
    monkeypatch.setenv("EDGECITADEL_TRACE_SYNC", "1")
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(state, stop), daemon=True)
    thread.start()
    observer = None
    try:
        async with asyncio.timeout(8):
            while not socket_path_for(state).exists():
                await asyncio.sleep(0.01)
            client = AgentdClient(socket_path_for(state))
            (message,) = await subscription.fetch(1, timeout=5)
            await ingest_delivery(core, message)
            observer = AgentdStore(state / "agentd.sqlite3")
            while (
                observer._connection.execute(
                    "SELECT count(*) FROM trace_spool WHERE state='core_settled'"
                ).fetchone()[0]
                != 1
            ):
                await asyncio.sleep(0.02)
            health = await asyncio.to_thread(client.call, "health")
            assert health["telemetry"]["enabled"] is True
            assert (
                health["telemetry"]["metrics"]["counts"]["broker_acknowledgments"] >= 1
            )
            assert (
                health["telemetry"]["metrics"]["counts"]["settlement_page_observations"]
                >= 1
            )
            assert health["transport"]["configured"] is True
            assert (
                core.execute("SELECT event_id FROM trace_raw_events").fetchone()[0]
                == event["event_id"]
            )
            assert (
                observer._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
                == 0
            )
            assert (
                observer._connection.execute(
                    "SELECT count(*) FROM trace_journal"
                ).fetchone()[0]
                == 1
            )
            admin = AgentdClient(
                socket_path_for(state),
                admin_token=(state / "admin.token").read_text().strip(),
                timeout=15,
            )
            stopped = await asyncio.to_thread(
                admin.call, "trace.sync.control", action="stop"
            )
            assert stopped["state"] == "stopped" and stopped["connected"] is False
            assert (await asyncio.to_thread(client.call, "health"))["transport"][
                "configured"
            ] is True
            with observer._connection:
                observer._connection.execute("BEGIN IMMEDIATE")
                extra = TraceJournal(observer._connection).record(
                    "edge-a", {**event, "event_id": str(uuid4())}, selected=True
                )
                scope = list(
                    observer._connection.execute(
                        "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
                    ).fetchone()
                )
                observer._connection.execute(
                    "UPDATE trace_export_generations SET sync_fault='invalid_export_record'"
                )
            await asyncio.sleep(0.1)
            assert (
                observer._connection.execute(
                    "SELECT state FROM trace_spool WHERE export_seq=2"
                ).fetchone()[0]
                == "pending"
            )
            await asyncio.to_thread(
                admin.call, "trace.sync.control", action="retry", scope=scope
            )
            (message,) = await subscription.fetch(1, timeout=5)
            await ingest_delivery(core, message)
            assert (
                core.execute(
                    "SELECT count(*) FROM trace_raw_events WHERE event_id=?",
                    (extra["event_id"],),
                ).fetchone()[0]
                == 1
            )
            assert (
                observer._connection.execute(
                    "SELECT sync_fault FROM trace_export_generations"
                ).fetchone()[0]
                is None
            )
            while (await asyncio.to_thread(client.call, "health"))["telemetry"][
                "metrics"
            ]["counts"]["broker_acknowledgments"] < 2:
                await asyncio.sleep(0.02)
            assert (
                observer._connection.execute("SELECT count(*) FROM tasks").fetchone()[0]
                == 0
            )
    finally:
        stop.set()
        await asyncio.to_thread(thread.join, 10)
        assert not thread.is_alive()
        if observer is not None:
            observer.close()
        await control.unsubscribe()
        await subscription.unsubscribe()
        core.close()
    assert broker.is_connected
    assert not socket_path_for(state).exists()


async def test_source_verification_does_not_provision_missing_stream(broker):
    js = broker.jetstream()
    with pytest.raises(NotFoundError):
        await telemetry.ensure_telemetry_stream(js, create=False)
    with pytest.raises(NotFoundError):
        await js.stream_info(telemetry.STREAM_NAME)
    await telemetry.ensure_telemetry_stream(js)
    assert (
        await telemetry.ensure_telemetry_stream(js, create=False)
    ).config.name == telemetry.STREAM_NAME
