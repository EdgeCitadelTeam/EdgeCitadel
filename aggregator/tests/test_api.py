"""FastAPI endpoint contracts for v0.1."""
import pytest
from fastapi.testclient import TestClient
from aggregator.main import make_app


@pytest.fixture
def client(tmp_path, envelope_schema_path, card_schema_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("ENVELOPE_SCHEMA_PATH", str(envelope_schema_path))
    monkeypatch.setenv("CARD_SCHEMA_PATH", str(card_schema_path))
    app = make_app(for_testing=True)   # skips NATS wiring
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
    assert "mqtt_connected" not in body          # legacy field gone
    assert "jetstream_stream_ok" in body


def test_post_command_returns_task_id(client, monkeypatch):
    # With testing flag, command dispatch stubs out JetStream publish but
    # synthesizes a task_id.
    r = client.post("/api/command/shell-1",
                    json={"body": "echo hi"})
    assert r.status_code == 202
    body = r.json()
    assert "task_id" in body
    assert len(body["task_id"]) == 36
    assert body["recipient_id"] == "shell-1"


def test_post_command_rejects_stale_registered_agent(client):
    from aggregator import database as db

    db.upsert_agent_card({
        "name": "eu-amd-hermes", "description": "x", "version": "0",
        "url": "u", "provider": {"organization": "x"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}},
        timestamp="2026-04-23T10:00:00.000Z")
    db.update_heartbeat("eu-amd-hermes", "2000-01-01T00:00:00.000Z")

    r = client.post("/api/command/eu-amd-hermes",
                    json={"body": "weather in London"})

    assert r.status_code == 409
    assert "offline" in r.text


def test_post_command_rejects_invalid_body(client):
    r = client.post("/api/command/shell-1", json={"unknown": 1})
    assert r.status_code == 422


def test_delete_agent_removes_card(client):
    # seed by direct DB insert
    from aggregator import database as db
    db.upsert_agent_card({
        "name": "gemma-1", "description": "x", "version": "0",
        "url": "u", "provider": {"organization": "x"},
        "capabilities": {}, "securitySchemes": {},
        "metadata": {"runtime.kind": "native", "runtime.roles": ["worker"],
                     "runtime.heartbeat_interval_sec": 30}},
        timestamp="2026-04-23T10:00:00.000Z")
    assert client.get("/api/agents/gemma-1/card").status_code == 200
    assert client.delete("/api/agents/gemma-1").status_code == 204
    assert client.get("/api/agents/gemma-1/card").status_code == 404


def test_get_queue_requires_jetstream(client):
    r = client.get("/api/agents/shell-1/queue")
    # In test mode, returns 503 when JetStream not wired
    assert r.status_code in (200, 503)


def test_openclaw_login_returns_token(client):
    r = client.post("/api/openclaw/login",
                    json={"session_id": "sess-abc123"})
    assert r.status_code == 200
    body = r.json()
    assert "token" in body
    assert "expires_at" in body
    assert body["agent_id"] == "openclaw-sess-abc123"


def test_openclaw_login_rejects_bad_session(client):
    r = client.post("/api/openclaw/login", json={"session_id": "bad/slash"})
    assert r.status_code == 422
