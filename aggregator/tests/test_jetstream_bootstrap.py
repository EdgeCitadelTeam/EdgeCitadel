"""Explicitly opt-in JetStream integration tests with an owned NATS server."""

import os
import secrets

import pytest
from nats.aio.client import Client as NATS

from aggregator.jetstream_bootstrap import ensure_consumer, ensure_stream
from tests.research.nats_server import NatsServer


pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_JETSTREAM_INTEGRATION") != "1",
    reason="set RUN_JETSTREAM_INTEGRATION=1 to run owned JetStream integration",
)


def test_jetstream_integration_is_explicitly_opted_in():
    assert os.environ["RUN_JETSTREAM_INTEGRATION"] == "1"


@pytest.fixture
async def js_client():
    server = NatsServer(token=secrets.token_hex(32), jetstream=True).start()
    nc = NATS()
    try:
        await nc.connect(
            servers=[server.url],
            token=server.token,
            connect_timeout=1,
            allow_reconnect=False,
            max_reconnect_attempts=0,
        )
        yield nc.jetstream()
    finally:
        try:
            await nc.drain()
        finally:
            server.close()


async def test_ensure_stream_idempotent(js_client):
    info1 = await ensure_stream(js_client)
    info2 = await ensure_stream(js_client)
    assert info1.config.name == "AGENT_INBOX"
    assert info2.config.name == "AGENT_INBOX"


async def test_ensure_consumer_serialization(js_client):
    await ensure_stream(js_client, "shell-test")
    ci = await ensure_consumer(js_client, "shell-test", ack_wait_sec=30)
    assert ci.config.max_ack_pending == 1
    assert ci.config.ack_wait == 30
    assert ci.config.filter_subject == "agents.shell-test.inbox"


async def test_stream_config_matches_spec(js_client):
    info = await ensure_stream(js_client)
    cfg = info.config
    assert cfg.name == "AGENT_INBOX"
    assert cfg.subjects == ["agents.aggregator.inbox"]
    assert str(cfg.retention).lower() in ("workqueue", "workqueuepolicy")
    assert str(cfg.discard).lower() in ("new", "discardnew")
    assert cfg.max_msg_size == 1024 * 1024
    assert cfg.duplicate_window == 5 * 60
