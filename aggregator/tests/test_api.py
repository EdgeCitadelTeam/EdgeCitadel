"""FastAPI endpoint contracts for v0.1."""

import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from aggregator import validator as validator_module
from aggregator.main import make_app
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
        "timestamp",
        "payload",
    }
    assert "context_id" not in env
    assert "hop_count" not in env

    validator = EnvelopeValidator(envelope_schema_path, card_schema_path)
    validator.validate_envelope(env)
    correlated = validator_module.normalize_task_correlation(env)
    assert correlated["context_id"] == env["task_id"]
    assert correlated["hop_count"] == 0


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
