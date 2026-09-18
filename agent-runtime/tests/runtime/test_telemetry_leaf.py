"""Core-owned telemetry and control routing over two authenticated real Leaves."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from leaf_link import CuttableLink
from nats.aio.client import Client as NATS
from nats.errors import NoRespondersError, TimeoutError
from nats.js.errors import NoStreamResponseError, NotFoundError

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import (
    canonical_bytes,
    validate_export,
    validate_settlement_reply,
    validate_settlement_request,
)
from edgecitadel_agentd.trace_exporter import ExportScope, TraceExporter
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import page_request
from edgecitadel_agentd.trace_settlement_pages import SETTLEMENT_PAGE_SUBJECT
from edgecitadel_agentd.trace_settlement_poll import SettlementPoller
from edgecitadel_plugin_runtime.jetstream import ensure_stream
from edgecitadel_plugin_runtime.telemetry_stream import (
    CONSUMER_NAME,
    EVENT_SUBJECT,
    SETTLEMENT_SUBJECT,
    STREAM_NAME,
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.environ.get("RUN_AGENTD_NATS_INTEGRATION") != "1",
        reason="owned NATS integration opt-in required",
    ),
]


class OwnedTopology(dict):
    """Client mapping with explicit handles for owned broker fault injection."""

    def __init__(self):
        super().__init__()
        self.processes = []
        self.endpoints = {}
        self.link = None


def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@pytest_asyncio.fixture
async def topology(tmp_path, request):
    binary = shutil.which("nats-server")
    assert binary, "explicit qualification requires nats-server"
    ports = {name: available_port() for name in ("core", "a", "b", "leaf")}
    assert len(set(ports.values())) == 4
    tokens = {name: secrets.token_urlsafe(32) for name in ("core", "a", "b")}
    password = secrets.token_urlsafe(32)
    clients = OwnedTopology()
    processes = clients.processes
    try:
        if getattr(request, "param", None) == "link":
            clients.link = CuttableLink(ports["leaf"])
            await clients.link.start()
        for name in ("core", "a", "b"):
            directory = tmp_path / name
            directory.mkdir()
            domain = "" if name == "core" else f'domain: "EDGE_{name.upper()}"'
            remote_port = (
                clients.link.port if name == "a" and clients.link else ports["leaf"]
            )
            advertise = (
                f'advertise: "127.0.0.1:{clients.link.port}", ' if clients.link else ""
            )
            leaf = (
                f'leafnodes {{ host: "127.0.0.1", port: {ports["leaf"]}, {advertise}authorization {{ user: "leaf", password: {json.dumps(password)} }} }}'
                if name == "core"
                else f'leafnodes {{ reconnect: "100ms", remotes: [{{ url: "nats-leaf://leaf:{password}@127.0.0.1:{remote_port}" }}] }}'
            )
            config = directory / "nats.conf"
            config.write_text(
                f'host: "127.0.0.1"\nport: {ports[name]}\nauthorization {{ token: {json.dumps(tokens[name])} }}\njetstream {{ store_dir: {json.dumps(str(directory / "js"))}, max_file_store: 2GB, {domain} }}\n{leaf}\n'
            )
            config.chmod(0o600)
            process = await asyncio.to_thread(
                subprocess.Popen,
                [binary, "-c", str(config)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            processes.append(process)
            for _ in range(100):
                assert process.poll() is None, (
                    f"owned {name} broker exited during startup"
                )
                try:
                    _reader, writer = await asyncio.open_connection(
                        "127.0.0.1", ports[name]
                    )
                    writer.close()
                    await writer.wait_closed()
                    break
                except OSError:
                    await asyncio.sleep(0.02)
            client = NATS()
            clients[name] = client
            clients.endpoints[name] = (f"nats://127.0.0.1:{ports[name]}", tokens[name])
            await client.connect(
                servers=[f"nats://127.0.0.1:{ports[name]}"],
                token=tokens[name],
                allow_reconnect=False,
                connect_timeout=1,
            )
        yield clients
    finally:
        try:
            if clients.link is not None:
                await asyncio.wait_for(clients.link.cut(), timeout=5)
        finally:
            try:
                for client in clients.values():
                    await client.close()
            finally:
                for process in reversed(processes):
                    process.terminate()
                    await asyncio.to_thread(process.wait, timeout=5)


async def publish_when_routed(js, subject, payload, identity):
    for attempt in range(30):
        try:
            return await js.publish(
                subject, payload, timeout=0.5, headers={"Nats-Msg-Id": identity}
            )
        except (NoRespondersError, TimeoutError):
            if attempt == 29:
                raise
            await asyncio.sleep(0.1)
    raise AssertionError("unreachable")


async def test_leaf_export_core_ingestion_and_v2_poll_apply_real_durable_page(
    topology, tmp_path, monkeypatch
):
    # This cross-layer contract test deliberately uses the repository's actual
    # Core helpers, without importing or starting its HTTP application.
    monkeypatch.syspath_prepend(str(Path(__file__).parents[3]))
    from aggregator.trace_ingest import ingest_delivery
    from aggregator.trace_settlement import settlement_page_reply
    from aggregator.trace_store import initialize

    core, leaf = topology["core"], topology["a"]
    js = core.jetstream()
    await ensure_telemetry_stream(js)
    await ensure_telemetry_consumer(js)
    consumer = await js.pull_subscribe(
        EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
    )
    store = AgentdStore(tmp_path / "source/state.db")
    database = sqlite3.connect(tmp_path / "core.db")
    try:
        initialize(database)
        event = json.loads(
            (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
        )["fixtures"][0]["event"]
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(store._connection)
            epoch, generation = journal.initialize("edge-a")
            journal.record("edge-a", event, selected=True)
        scope = ("edge-a", epoch, generation)
        exporter = TraceExporter(store, leaf.jetstream(), ExportScope(*scope))
        for attempt in range(5):
            try:
                await exporter.publish_batch()
                break
            except (NoRespondersError, NoStreamResponseError, TimeoutError):
                if attempt == 4:
                    raise
                await asyncio.sleep(0.1)
        assert (
            store._connection.execute("SELECT state FROM trace_spool").fetchone()[0]
            == "broker_acked"
        )
        assert page_request(store, scope)["after_export_seq"] == 0
        (message,) = await consumer.fetch(1, timeout=2)
        assert (await ingest_delivery(database, message)).outcome == "accepted"
        requests = []

        async def control(message):
            request = json.loads(message.data)
            requests.append(request)
            response = settlement_page_reply(database, request)
            await message.respond(canonical_bytes(response, limit=18 * 1024))

        await core.subscribe(SETTLEMENT_PAGE_SUBJECT, cb=control)
        await core.flush()
        poller = SettlementPoller(store, leaf, scope)
        for _ in range(3):
            if await poller.poll() == "applied":
                break
        assert page_request(store, scope)["after_export_seq"] == 1
        assert tuple(
            store._connection.execute(
                "SELECT state,core_outcome FROM trace_spool"
            ).fetchone()
        ) == ("core_settled", "accepted")
        assert (
            database.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == 1
        )
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 1
        )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
        assert (
            requests[-1]["schema_version"] == 2
            and requests[-1]["source_epoch"] == epoch
        )
        assert (await js.stream_info(STREAM_NAME)).state.messages == 1
        assert poller.delay == 30 and not poller.replay_required
    finally:
        database.close()
        store.close()


async def test_two_leaves_route_telemetry_to_core_and_return_correlated_control(
    topology,
):
    core = topology["core"]
    core_js = core.jetstream()
    leaves = {
        name: topology[name].jetstream(domain=f"EDGE_{name.upper()}")
        for name in ("a", "b")
    }
    await ensure_stream(core_js, "aggregator")
    await ensure_telemetry_stream(core_js)
    await ensure_telemetry_consumer(core_js)
    for name, js in leaves.items():
        await ensure_stream(js, f"edge-{name}")
    requests = []

    async def control(message):
        request = json.loads(message.data)
        validate_settlement_request(request)
        requests.append(request["request_id"])
        # No Core ledger exists yet: route proof must not fabricate settlement.
        reply = {
            "schema_version": 1,
            "request_id": request["request_id"],
            "status": "error",
            "code": "unknown_source",
            "retry_after_ms": 500,
        }
        validate_settlement_reply(reply, request=request)
        await message.respond(canonical_bytes(reply))

    await core.subscribe(SETTLEMENT_SUBJECT, cb=control)
    await core.flush()
    fixture = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    payloads = []
    for name, js in leaves.items():
        event = deepcopy(fixture)
        event.update(
            node_id=f"edge-{name}",
            event_id=str(uuid4()),
            source_epoch=str(uuid4()),
            source_seq=1,
        )
        wrapper = {
            "schema_version": 1,
            "node_id": event["node_id"],
            "source_epoch": event["source_epoch"],
            "export_generation": str(uuid4()),
            "export_seq": 1,
            "event_sha256": hashlib.sha256(canonical_bytes(event)).hexdigest(),
            "event": event,
        }
        payload = validate_export(wrapper)
        payloads.append(payload)
        identity = str(uuid4())
        ack = await publish_when_routed(
            js, f"edgecitadel.telemetry.v1.edge-{name}", payload, identity
        )
        assert ack.stream == STREAM_NAME and not ack.duplicate
        retry = await js.publish(
            f"edgecitadel.telemetry.v1.edge-{name}",
            payload,
            headers={"Nats-Msg-Id": identity},
        )
        assert retry.duplicate and retry.seq == ack.seq
        request = {
            key: wrapper[key]
            for key in (
                "schema_version",
                "node_id",
                "source_epoch",
                "export_generation",
            )
        }
        request["request_id"] = str(uuid4())
        answer = await topology[name].request(
            SETTLEMENT_SUBJECT, validate_settlement_request(request), timeout=2
        )
        reply = json.loads(answer.data)
        validate_settlement_reply(reply, request=request)
        assert reply["code"] == "unknown_source" and "checkpoint" not in reply
        with pytest.raises(NotFoundError):
            await js.stream_info(STREAM_NAME)
    assert len(set(requests)) == 2
    assert (await core_js.stream_info(STREAM_NAME)).state.messages == 2
    assert [
        (await core_js.get_msg(STREAM_NAME, seq=sequence)).data for sequence in (1, 2)
    ] == payloads
    # Destination-owned command capture still crosses the same Leaf link.
    assert (
        await publish_when_routed(
            leaves["a"], "agents.edge-b.inbox", b"owned-command", str(uuid4())
        )
    ).stream == "AGENT_INBOX"
    assert (await leaves["b"].stream_info("AGENT_INBOX")).state.messages == 1
    assert (await leaves["a"].stream_info("AGENT_INBOX")).state.messages == 0
    assert (await core_js.stream_info("AGENT_INBOX")).state.messages == 0
    assert (await core_js.stream_info(STREAM_NAME)).state.messages == 2
