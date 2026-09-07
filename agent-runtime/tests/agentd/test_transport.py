from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from nats.aio.msg import Msg

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.transport import AgentdNatsTransport


@pytest.mark.parametrize("explicit", [False, True])
def test_core_node_supplies_transport_endpoint(tmp_path: Path, explicit: bool) -> None:
    node = {
        "version": 1,
        "mode": "core",
        "nats_url": "nats://core.example:4222",
        "nats_token": "legacy-token",
    }
    if explicit:
        node.update(
            plugin_nats_url="nats://127.0.0.1:4222", plugin_nats_token="local-token"
        )
    (tmp_path / "node.json").write_text(json.dumps(node))
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    try:
        actual = AgentdNatsTransport(tmp_path, store)._node()
        assert actual is not None
        assert actual["plugin_nats_url"] == (
            "nats://127.0.0.1:4222" if explicit else node["nats_url"]
        )
        assert actual["plugin_nats_token"] == (
            "local-token" if explicit else "legacy-token"
        )
    finally:
        store.close()


@pytest.mark.parametrize(
    "updates",
    [
        {"mode": "edge"},
        {"version": 99},
        {"nats_token": ""},
        {"nats_url": "http://core.example"},
        {"plugin_nats_url": ""},
    ],
)
def test_core_fallback_does_not_normalize_invalid_records(
    tmp_path: Path, updates: dict
) -> None:
    node = {
        "version": 1,
        "mode": "core",
        "nats_url": "nats://core.example:4222",
        "nats_token": "token",
    }
    node.update(updates)
    (tmp_path / "node.json").write_text(json.dumps(node))
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    try:
        actual = AgentdNatsTransport(tmp_path, store)._node()
        assert actual is None or not actual.get("plugin_nats_token")
    finally:
        store.close()


@pytest.mark.parametrize(
    "updates",
    [
        {"version": 99},
        {"version": True},
        {"mode": "unknown"},
        {"plugin_nats_token": ""},
        {"plugin_nats_url": "http://wrong"},
        {"messaging_mode": "unknown"},
        {"messaging_mode": "nats_leaf"},
    ],
)
def test_malformed_explicit_edge_is_not_configured(
    tmp_path: Path, updates: dict
) -> None:
    node = {
        "version": 2,
        "mode": "edge",
        "plugin_nats_url": "nats://127.0.0.1:4222",
        "plugin_nats_token": "test-only",
    }
    node.update(updates)
    (tmp_path / "node.json").write_text(json.dumps(node))
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    try:
        assert AgentdNatsTransport(tmp_path, store)._node() is None
    finally:
        store.close()


@pytest.mark.asyncio
async def test_generated_native_card_declares_l1_transport_binding(
    tmp_path: Path,
) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    store.register_connector(
        connector_id="codex-session",
        host_type="codex",
        agent_id="edge-codex",
        capabilities=["edgecitadel_delegate"],
    )
    connector = store.list_connectors()[0]
    published: list[tuple[str, bytes]] = []

    class Connection:
        async def publish(self, subject: str, payload: bytes) -> None:
            published.append((subject, payload))

    transport = AgentdNatsTransport(tmp_path, store)
    await transport._publish_register(cast(object, Connection()), connector)

    assert published[0][0] == "agents.edge-codex.register"
    envelope = json.loads(published[0][1])
    card = envelope["payload"]
    assert card["metadata"]["runtime.conformance"] == "L1"
    assert card["capabilities"]["extensions"] == [
        {
            "uri": "https://edgecitadel.local/ext/nats-binding/v1",
            "description": "Agent messaging is carried by the host-local EdgeCitadel transport.",
            "required": True,
        }
    ]
    store.close()


@pytest.mark.asyncio
async def test_max_delivery_diagnostic_uses_real_advisory_subject_shape(
    tmp_path: Path,
) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    transport = AgentdNatsTransport(tmp_path, store)
    message = cast(
        Msg,
        SimpleNamespace(
            subject=(
                "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
            ),
            data=json.dumps(
                {
                    "stream": "AGENT_INBOX",
                    "consumer": "worker-1_inbox",
                    "stream_seq": 42,
                    "deliveries": 3,
                }
            ).encode(),
        ),
    )

    await transport._observe_advisory(message)

    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT agent_id, attributes_json FROM events "
            "WHERE event_type = 'transport.max_deliveries'"
        ).fetchone()
    store.close()
    assert row is not None
    assert row[0] == "worker-1"
    assert json.loads(row[1]) == {
        "consumer": "worker-1_inbox",
        "stream": "AGENT_INBOX",
        "stream_seq": 42,
    }


@pytest.mark.asyncio
async def test_malformed_max_delivery_advisory_is_ignored(tmp_path: Path) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    transport = AgentdNatsTransport(tmp_path, store)
    message = cast(
        Msg,
        SimpleNamespace(
            subject=(
                "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
            ),
            data=b"{}",
        ),
    )

    await transport._observe_advisory(message)

    with sqlite3.connect(store.path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type = 'transport.max_deliveries'"
        ).fetchone()[0]
    store.close()
    assert count == 0
