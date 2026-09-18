"""Unit tests for the NATS subscriber glue. Does not talk to a live NATS."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from aggregator.aggregator import AggregatorApp, MessageRouter


def _bytes(env: dict) -> bytes:
    return json.dumps(env).encode()


@pytest.fixture
def router(tmp_path, envelope_schema_path, card_schema_path):
    from aggregator import database as db

    p = str(tmp_path / "t.db")
    db.init_db(p, wipe=True)
    return MessageRouter(
        db_path=p, envelope_schema=envelope_schema_path, card_schema=card_schema_path
    )


@pytest.mark.asyncio
async def test_register_caches_card(router):
    card = {
        "name": "shell-1",
        "description": "x",
        "version": "0.1",
        "url": "nats://x",
        "provider": {"organization": "EC"},
        "capabilities": {},
        "securitySchemes": {},
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 30,
        },
    }
    env = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "register",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": card,
    }
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))
    assert router.cache.get("shell-1")["name"] == "shell-1"


@pytest.mark.asyncio
async def test_aggregator_self_registration_is_protocol_conformant(
    tmp_path, envelope_schema_path, card_schema_path
):
    app = AggregatorApp(
        nats_url="nats://unused",
        nats_token="unused",
        db_path=str(tmp_path / "t.db"),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    app.router.nc = MagicMock()
    app.router.nc.publish = AsyncMock()

    await app._publish_self_register()

    _, encoded = app.router.nc.publish.await_args.args
    envelope = json.loads(encoded)
    app.router.validator.validate_register(envelope)
    assert envelope["payload"]["metadata"]["runtime.conformance"] == "L1"


@pytest.mark.asyncio
async def test_register_rejects_sender_id_mismatch(router):
    card = {
        "name": "impostor",
        "description": "x",
        "version": "0.1",
        "url": "nats://x",
        "provider": {"organization": "EC"},
        "capabilities": {},
        "securitySchemes": {},
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 30,
        },
    }
    env = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "register",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": card,
    }
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))
    assert "shell-1" not in router.cache  # rejected


@pytest.mark.asyncio
async def test_malformed_envelope_dropped(router):
    bad = {"v": 1, "type": "not-a-type"}  # missing required
    await router.on_outbox(_fake_msg("agents.x.outbox", _bytes(bad)))
    # No exception, no DB row
    from aggregator import database as db

    assert db.count_messages() == 0


@pytest.mark.asyncio
async def test_heartbeat_updates_last_seen(router):
    await _register_shell(router)
    env = {
        "v": 1,
        "id": "22222222-3333-4444-8555-666666666666",
        "type": "heartbeat",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:30.000Z",
        "payload": {"cpu_percent": 5},
    }
    await router.on_heartbeat(_fake_msg("agents.shell-1.heartbeat", _bytes(env)))
    from aggregator import database as db

    a = db.get_agent("shell-1")
    assert a["last_heartbeat"] == "2026-04-23T10:00:30.000Z"


def _fake_msg(subject, data):
    m = MagicMock()
    m.subject = subject
    m.data = data
    return m


async def _register_shell(router):
    card = {
        "name": "shell-1",
        "description": "x",
        "version": "0.1",
        "url": "nats://x",
        "provider": {"organization": "EC"},
        "capabilities": {},
        "securitySchemes": {},
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 30,
        },
    }
    env = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "register",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": card,
    }
    await router.on_register(_fake_msg("agents.shell-1.register", _bytes(env)))


@pytest.mark.asyncio
async def test_stop_releases_canceled_pull_inbox_before_connection_drain(
    tmp_path, envelope_schema_path, card_schema_path
):
    import asyncio
    from nats.aio.subscription import Subscription
    from nats.js.client import JetStreamContext

    app = AggregatorApp(
        nats_url="nats://unused",
        nats_token="unused",
        db_path=str(tmp_path / "t.db"),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    # Real nats-py queue/drain behavior, without a live broker. An unread pull
    # response remains after fetch cancellation; callback subscriptions still
    # need connection drain, but this inbox has no remaining reader.
    nc = MagicMock(is_closed=False, is_draining=False, is_reconnecting=False)
    nc.flush = AsyncMock()
    nc._send_unsubscribe = AsyncMock()
    subscription = Subscription(nc, id=7, subject="_INBOX.owned-test")
    subscription._pending_queue.put_nowait(object())
    active = {7: subscription}
    nc._remove_sub.side_effect = active.pop

    async def drain():
        for pending in tuple(active.values()):
            await pending._drain()
        nc.is_closed = True

    nc.drain = AsyncMock(side_effect=drain)
    app.router.nc = nc
    js = MagicMock()
    app._inbox_subscription = JetStreamContext.PullSubscription(
        js, subscription, "AGENT_INBOX", "aggregator_inbox", b"_INBOX.owned-test"
    )
    canceled = asyncio.Event()
    started = asyncio.Event()

    async def reader():
        started.set()
        try:
            await asyncio.Future()
        finally:
            canceled.set()

    app._inbox_task = asyncio.create_task(reader())
    await started.wait()
    await asyncio.wait_for(app.stop(), timeout=1)
    assert canceled.is_set()
    assert not active
    nc.drain.assert_awaited_once()
    js.delete_consumer.assert_not_called()
    await app.stop()
    nc.drain.assert_awaited_once()
