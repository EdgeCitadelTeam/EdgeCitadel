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
