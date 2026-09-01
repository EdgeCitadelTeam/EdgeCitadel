"""FastAPI endpoint contracts for v0.1."""

import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from nats.js.errors import ServiceUnavailableError

from aggregator import validator as validator_module
from aggregator.main import make_app
from aggregator.models import CommandRequest
from aggregator.validator import EnvelopeValidator


@pytest.fixture
def client(tmp_path, envelope_schema_path, card_schema_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("ENVELOPE_SCHEMA_PATH", str(envelope_schema_path))
    monkeypatch.setenv("CARD_SCHEMA_PATH", str(card_schema_path))
    app = make_app(for_testing=True)  # skips NATS wiring
    with TestClient(app) as c:
        yield c


def test_get_agents_empty(client):
    r = client.get("/api/agents")
    assert r.status_code == 200
    assert r.json() == []


def test_system_status_shape(client):
    r = client.get("/api/system/status")
    assert r.status_code == 200
    body = r.json()
    assert "nats_connected" in body
    assert "mqtt_connected" not in body  # legacy field gone
    assert "jetstream_stream_ok" in body


def test_post_command_returns_task_id(client, monkeypatch):
    # With testing flag, command dispatch stubs out JetStream publish but
    # synthesizes a task_id.
    r = client.post("/api/command/shell-1", json={"body": "echo hi"})
    assert r.status_code == 202
    body = r.json()
    assert "task_id" in body
    assert len(body["task_id"]) == 36
    assert body["recipient_id"] == "shell-1"


def test_post_command_rejects_stale_registered_agent(client):
    from aggregator import database as db

    db.upsert_agent_card(
        {
            "name": "eu-amd-hermes",
            "description": "x",
            "version": "0",
            "url": "u",
            "provider": {"organization": "x"},
            "capabilities": {},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["worker"],
                "runtime.heartbeat_interval_sec": 30,
            },
        },
        timestamp="2026-04-23T10:00:00.000Z",
    )
    db.update_heartbeat("eu-amd-hermes", "2000-01-01T00:00:00.000Z")

    r = client.post("/api/command/eu-amd-hermes", json={"body": "weather in London"})

    assert r.status_code == 409
    assert "offline" in r.text


def test_post_command_rejects_invalid_body(client):
    r = client.post("/api/command/shell-1", json={"unknown": 1})
    assert r.status_code == 422


class _CapturePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str] | None]] = []

    async def publish(
        self,
        subject: str,
        data: bytes,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.published.append((subject, data, headers))

    async def drain(self) -> None:
        pass


def test_post_command_correlation_preserves_actual_producer_shape(
    client: TestClient,
    envelope_schema_path: Path,
    card_schema_path: Path,
) -> None:
    js = _CapturePublisher()
    nc = _CapturePublisher()
    app = cast(FastAPI, client.app)
    route = next(
        route
        for route in app.routes
        if isinstance(route, APIRoute) and route.path == "/api/command/{agent_id}"
    )
    state = cast(
        dict[str, Any],
        inspect.getclosurevars(route.endpoint).nonlocals["state"],
    )
    state["app"] = SimpleNamespace(
        router=SimpleNamespace(js=js, nc=nc, cache={}),
    )

    response = client.post(
        "/api/command/worker-1",
        json={"body": "printf spine:nonce"},
    )

    assert response.status_code == 202
    subject, data, headers = js.published[0]
    env = json.loads(data)
    assert subject == "agents.worker-1.inbox"
    assert headers == {"Nats-Msg-Id": env["id"]}
    assert set(env) == {
        "v",
        "id",
        "type",
        "sender_id",
        "recipient_id",
        "task_id",
        "context_id",
        "hop_count",
        "timestamp",
        "payload",
    }
    assert env["context_id"] == env["task_id"]
    assert env["hop_count"] == 0

    validator = EnvelopeValidator(envelope_schema_path, card_schema_path)
    validator.validate_envelope(env)
    correlated = validator_module.normalize_task_correlation(env)
    assert correlated["context_id"] == env["task_id"]
    assert correlated["hop_count"] == 0


def test_post_command_reports_unreachable_durable_destination_as_not_accepted(
    client: TestClient,
) -> None:
    app = cast(FastAPI, client.app)
    route = next(
        route
        for route in app.routes
        if isinstance(route, APIRoute) and route.path == "/api/command/{agent_id}"
    )
    state = cast(
        dict[str, Any], inspect.getclosurevars(route.endpoint).nonlocals["state"]
    )
    state["app"] = SimpleNamespace(
        router=SimpleNamespace(
            js=SimpleNamespace(
                publish=AsyncMock(side_effect=ServiceUnavailableError())
            ),
            nc=SimpleNamespace(publish=AsyncMock(), drain=AsyncMock()),
            cache={},
        )
    )

    response = client.post("/api/command/remote-edge-agent", json={"body": "test"})

    assert response.status_code == 503
    assert response.json()["detail"].endswith("command was not accepted")


def test_delete_agent_removes_card(client):
    # seed by direct DB insert
    from aggregator import database as db

    db.upsert_agent_card(
        {
            "name": "gemma-1",
            "description": "x",
            "version": "0",
            "url": "u",
            "provider": {"organization": "x"},
            "capabilities": {},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["worker"],
                "runtime.heartbeat_interval_sec": 30,
            },
        },
        timestamp="2026-04-23T10:00:00.000Z",
    )
    assert client.get("/api/agents/gemma-1/card").status_code == 200
    assert client.delete("/api/agents/gemma-1").status_code == 204
    assert client.get("/api/agents/gemma-1/card").status_code == 404


def test_get_queue_requires_jetstream(client):
    r = client.get("/api/agents/shell-1/queue")
    # In test mode, returns 503 when JetStream not wired
    assert r.status_code in (200, 503)


@pytest.mark.asyncio
async def test_queue_uses_the_agent_inbox_subject_not_a_legacy_durable_name():
    from aggregator.main import _agent_inbox_consumer

    matching = SimpleNamespace(
        config=SimpleNamespace(filter_subject="agents.shell-1.inbox"),
        num_pending=0,
        num_ack_pending=0,
    )
    ignored = SimpleNamespace(
        config=SimpleNamespace(filter_subject="agents.other.inbox"),
    )
    js = SimpleNamespace(consumers_info=AsyncMock(return_value=[ignored, matching]))

    consumer = await _agent_inbox_consumer(js, "shell-1")

    assert consumer is matching
    js.consumers_info.assert_awaited_once_with("AGENT_INBOX")


def test_openclaw_login_returns_token(client):
    r = client.post("/api/openclaw/login", json={"session_id": "sess-abc123"})
    assert r.status_code == 200
    body = r.json()
    assert "token" in body
    assert "expires_at" in body
    assert body["agent_id"] == "openclaw-sess-abc123"


def test_openclaw_login_rejects_bad_session(client):
    r = client.post("/api/openclaw/login", json={"session_id": "bad/slash"})
    assert r.status_code == 422


def test_messages_exposes_replay_and_observation_metadata(client):
    from aggregator import database as db

    env = {
        "v": 1,
        "id": "audit-wire-1",
        "type": "result",
        "sender_id": "shell-1",
        "recipient_id": "aggregator",
        "task_id": "audit-task-1",
        "task_state": "completed",
        "timestamp": "2026-07-25T12:00:01.000Z",
        "payload": {"body": "edgecitadel:audit"},
    }
    db.insert_message(env)
    db.insert_message(env)

    response = client.get("/api/messages?task_id=audit-task-1")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 1
    assert rows[0]["duplicate_count"] == 1
    assert isinstance(rows[0]["observation_index"], int)
    assert rows[0]["observation_index"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("requested_context", "expected_context"),
    [
        (None, None),
        (
            "6e088543-c9de-4459-a0fe-2191d20dfba1",
            "6e088543-c9de-4459-a0fe-2191d20dfba1",
        ),
    ],
)
async def test_direct_command_publish_has_complete_correlation(
    requested_context,
    expected_context,
):
    from aggregator.main import (
        _build_direct_command_envelope,
        _publish_direct_command,
    )

    router = SimpleNamespace(
        js=SimpleNamespace(publish=AsyncMock()),
        nc=SimpleNamespace(publish=AsyncMock()),
    )
    request = CommandRequest(body="operator-nonce", context_id=requested_context)
    envelope = _build_direct_command_envelope(
        agent_id="shell-1",
        sender_id="aggregator",
        request=request,
    )
    await _publish_direct_command(router, envelope)

    assert UUID(envelope["id"]).version == 4
    assert UUID(envelope["task_id"]).version == 4
    assert UUID(envelope["context_id"]).version == 4
    if expected_context is None:
        assert envelope["context_id"] == envelope["task_id"]
    else:
        assert envelope["context_id"] == expected_context
    assert envelope["hop_count"] == 0
    assert envelope["payload"] == {"body": "operator-nonce"}

    inbox = router.js.publish.await_args
    assert inbox.args[0] == "agents.shell-1.inbox"
    assert inbox.kwargs["headers"] == {"Nats-Msg-Id": envelope["id"]}
    outbox = router.nc.publish.await_args
    assert outbox.args[0] == "agents.aggregator.outbox"
    assert json.loads(inbox.args[1]) == envelope
    assert json.loads(outbox.args[1]) == envelope


@pytest.mark.parametrize(
    "bad_context",
    [
        "not-a-uuid",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "6E088543-C9DE-4459-A0FE-2191D20DFBA1",
    ],
)
def test_direct_command_rejects_non_uuid4_context(client, bad_context):
    response = client.post(
        "/api/command/shell-1",
        json={"body": "operator-nonce", "context_id": bad_context},
    )

    assert response.status_code == 422
