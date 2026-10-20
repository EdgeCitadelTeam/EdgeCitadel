"""System-owned task reconciliation that replaces the Watchdog Agent."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from aggregator import database as db
from aggregator.aggregator import MessageRouter


def _source_envelope(
    task_id: str, *, sender: str = "aggregator", recipient: str = "worker-1"
) -> bytes:
    return json.dumps(
        {
            "v": 1,
            "id": "20000000-0000-4000-8000-000000000001",
            "type": "command",
            "sender_id": sender,
            "recipient_id": recipient,
            "task_id": task_id,
            "timestamp": "2026-01-01T00:00:00.000Z",
            "payload": {"body": "work"},
        }
    ).encode()


def _advisory() -> bytes:
    return json.dumps(
        {
            "type": "io.nats.jetstream.advisory.v1.max_deliver",
            "stream": "AGENT_INBOX",
            "consumer": "worker-1_inbox",
            "stream_seq": 42,
            "deliveries": 5,
        }
    ).encode()


def _consumer_info(*, delivered_count: int = 5) -> SimpleNamespace:
    return SimpleNamespace(
        config=SimpleNamespace(max_deliver=5, max_ack_pending=1),
        delivered=SimpleNamespace(consumer_seq=delivered_count, stream_seq=42),
        ack_floor=SimpleNamespace(consumer_seq=0, stream_seq=0),
    )


@pytest.mark.asyncio
async def test_max_delivery_advisory_emits_system_failure(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    task_id = "10000000-0000-4000-8000-000000000001"
    router.js = SimpleNamespace(
        consumer_info=AsyncMock(return_value=_consumer_info()),
        get_msg=AsyncMock(return_value=SimpleNamespace(data=_source_envelope(task_id))),
        publish=AsyncMock(),
    )
    router.nc = SimpleNamespace(publish=AsyncMock())
    message = SimpleNamespace(
        subject=(
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
        ),
        data=_advisory(),
    )

    await router.on_advisory(message)

    publish = router.js.publish.await_args
    assert publish.args[0] == "agents.aggregator.inbox"
    result = json.loads(publish.args[1])
    assert result["sender_id"] == "edgecitadel-system"
    assert result["recipient_id"] == "aggregator"
    assert result["task_id"] == task_id
    assert result["task_state"] == "failed"
    assert result["payload"] == {
        "error": "recipient_unavailable",
        "recipient_id": "worker-1",
        "trigger": "max_deliveries",
    }
    assert publish.kwargs["headers"] == {
        "Nats-Msg-Id": f"edgecitadel-system-undeliverable-{task_id}"
    }
    router.js.get_msg.assert_awaited_once_with("AGENT_INBOX", seq=42)
    router.nc.publish.assert_awaited_once()
    assert db.count_poison_by_agent() == {"worker-1": 1}


@pytest.mark.asyncio
async def test_duplicate_advisory_does_not_emit_duplicate_outbox_result(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    publish = AsyncMock(
        side_effect=[
            SimpleNamespace(duplicate=False),
            SimpleNamespace(duplicate=True),
        ]
    )
    task_id = "10000000-0000-4000-8000-000000000001"
    router.js = SimpleNamespace(
        consumer_info=AsyncMock(return_value=_consumer_info()),
        get_msg=AsyncMock(return_value=SimpleNamespace(data=_source_envelope(task_id))),
        publish=publish,
    )
    router.nc = SimpleNamespace(publish=AsyncMock())
    message = SimpleNamespace(
        subject=(
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
        ),
        data=_advisory(),
    )

    await router.on_advisory(message)
    await router.on_advisory(message)

    first = json.loads(publish.await_args_list[0].args[1])
    second = json.loads(publish.await_args_list[1].args[1])
    assert first["id"] == second["id"]
    router.nc.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_advisory_records_diagnostic_without_fabricating_result(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    router.js = SimpleNamespace(get_msg=AsyncMock(), publish=AsyncMock())
    router.nc = SimpleNamespace(publish=AsyncMock())

    await router.on_advisory(
        SimpleNamespace(
            subject="$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.consumer",
            data=b"{}",
        )
    )

    router.js.publish.assert_not_awaited()
    router.nc.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_utf8_advisory_is_ignored(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    router.js = SimpleNamespace(get_msg=AsyncMock(), publish=AsyncMock())
    router.nc = SimpleNamespace(publish=AsyncMock())

    await router.on_advisory(
        SimpleNamespace(
            subject="$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.consumer",
            data=b"\xff",
        )
    )

    router.js.get_msg.assert_not_awaited()
    router.js.publish.assert_not_awaited()
    router.nc.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_forged_early_advisory_cannot_fail_a_task(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    router.js = SimpleNamespace(
        consumer_info=AsyncMock(return_value=_consumer_info(delivered_count=1)),
        get_msg=AsyncMock(),
        publish=AsyncMock(),
    )
    router.nc = SimpleNamespace(publish=AsyncMock())

    await router.on_advisory(
        SimpleNamespace(
            subject=(
                "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
            ),
            data=_advisory(),
        )
    )

    router.js.get_msg.assert_not_awaited()
    router.js.publish.assert_not_awaited()
    router.nc.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_advisory_headers_cannot_inject_a_subject(
    tmp_path, envelope_schema_path, card_schema_path, monkeypatch
) -> None:
    database_path = tmp_path / "aggregator.sqlite3"
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    db.init_db(str(database_path))
    router = MessageRouter(
        db_path=str(database_path),
        envelope_schema=envelope_schema_path,
        card_schema=card_schema_path,
    )
    task_id = "10000000-0000-4000-8000-000000000001"
    router.js = SimpleNamespace(
        consumer_info=AsyncMock(return_value=_consumer_info()),
        get_msg=AsyncMock(
            return_value=SimpleNamespace(
                data=_source_envelope(task_id, sender="aggregator.>")
            )
        ),
        publish=AsyncMock(),
    )
    router.nc = SimpleNamespace(publish=AsyncMock())

    await router.on_advisory(
        SimpleNamespace(
            subject=(
                "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.worker-1_inbox"
            ),
            data=_advisory(),
        )
    )

    router.js.publish.assert_not_awaited()
    router.nc.publish.assert_not_awaited()
