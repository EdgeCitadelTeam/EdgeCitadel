"""Requires a live NATS with JetStream on $NATS_URL. Skipped if unreachable."""
import os, pytest, asyncio
from nats.aio.client import Client as NATS
from aggregator.jetstream_bootstrap import ensure_stream, ensure_consumer


NATS_URL = os.environ.get("NATS_URL_TEST", "nats://localhost:4222")
NATS_TOKEN = os.environ.get("NATS_TOKEN_TEST", os.environ.get("NATS_TOKEN", ""))


@pytest.fixture
async def js_client():
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL], token=NATS_TOKEN,
                         connect_timeout=1)
    except Exception:
        pytest.skip("NATS not reachable; set NATS_URL_TEST to run")
    js = nc.jetstream()
    yield js
    # cleanup
    try:
        await js.delete_consumer("AGENT_INBOX", "shell-test_inbox")
    except Exception: pass
    try:
        await js.delete_stream("AGENT_INBOX")
    except Exception: pass
    await nc.drain()


async def test_ensure_stream_idempotent(js_client):
    info1 = await ensure_stream(js_client)
    info2 = await ensure_stream(js_client)
    assert info1.config.name == "AGENT_INBOX"
    assert info2.config.name == "AGENT_INBOX"


async def test_ensure_consumer_serialization(js_client):
    await ensure_stream(js_client)
    ci = await ensure_consumer(js_client, "shell-test", ack_wait_sec=30)
    assert ci.config.max_ack_pending == 1
    assert ci.config.ack_wait == 30 * 1_000_000_000  # ns in nats-py
    assert ci.config.filter_subject == "agents.shell-test.inbox"


async def test_stream_config_matches_spec(js_client):
    info = await ensure_stream(js_client)
    cfg = info.config
    assert cfg.name == "AGENT_INBOX"
    assert cfg.subjects == ["agents.*.inbox"]
    assert cfg.retention.name in ("workqueue", "WorkQueuePolicy",
                                  "WorkQueue")
    assert cfg.discard.name in ("new", "DiscardNew")
    assert cfg.max_msg_size == 1024 * 1024
    assert cfg.duplicate_window == 5 * 60 * 1_000_000_000
