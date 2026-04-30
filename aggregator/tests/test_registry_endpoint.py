"""Tests for GET /api/registry — fleet snapshot endpoint."""
import json

import pytest
from fastapi.testclient import TestClient

from aggregator.main import make_app


@pytest.fixture
def client(tmp_path, envelope_schema_path, card_schema_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("ENVELOPE_SCHEMA_PATH", str(envelope_schema_path))
    monkeypatch.setenv("CARD_SCHEMA_PATH", str(card_schema_path))
    app = make_app(for_testing=True)
    with TestClient(app) as c:
        yield c


def _seed_card(name: str, roles: list[str], deployment: str | None = None):
    from aggregator import database as db
    md = {"runtime.kind": "native", "runtime.roles": roles,
          "runtime.heartbeat_interval_sec": 30}
    if deployment:
        md["runtime.deployment"] = deployment
    db.upsert_agent_card({
        "name": name, "description": "x", "version": "0",
        "url": "u", "provider": {"organization": "x"},
        "capabilities": {}, "securitySchemes": {}, "metadata": md},
        timestamp="2026-04-29T10:00:00.000Z")


def test_registry_empty(client):
    r = client.get("/api/registry")
    assert r.status_code == 200
    assert r.json() == []


def test_registry_returns_fields(client):
    _seed_card("gemma-1", ["reasoner"])
    r = client.get("/api/registry")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    e = body[0]
    assert e["agent_id"] == "gemma-1"
    assert e["agent_state"] == "online"
    assert "card" in e
    assert e["card"]["metadata"]["runtime.roles"] == ["reasoner"]
    assert e["heartbeat_interval_sec"] == 30
    # JetStream queue gracefully degrades to zeros without a live broker
    assert e["queue"] == {"pending": 0, "ack_pending": 0}
    assert e["poison_count"] == 0


def test_registry_includes_poison_count(client):
    _seed_card("shell-1", ["worker"])
    from aggregator import database as db
    db.insert_poison_event(agent_id="shell-1", consumer="shell-1_inbox",
                           task_id="t1", original_sender="aggregator",
                           detected_at="2026-04-29T10:01:00.000Z",
                           advisory={"foo": "bar"})
    db.insert_poison_event(agent_id="shell-1", consumer="shell-1_inbox",
                           task_id="t2", original_sender="aggregator",
                           detected_at="2026-04-29T10:01:01.000Z",
                           advisory={"foo": "bar"})
    body = client.get("/api/registry").json()
    entry = next(e for e in body if e["agent_id"] == "shell-1")
    assert entry["poison_count"] == 2


def test_registry_deployment_filter(client):
    _seed_card("prod-1", ["worker"], deployment="default")
    _seed_card("test-runner", ["worker"], deployment="test")
    body = client.get("/api/registry").json()
    assert {e["agent_id"] for e in body} == {"prod-1", "test-runner"}

    only_test = client.get("/api/registry?deployment=test").json()
    assert {e["agent_id"] for e in only_test} == {"test-runner"}

    only_default = client.get("/api/registry?deployment=default").json()
    assert {e["agent_id"] for e in only_default} == {"prod-1"}


def test_registry_includes_aggregator_and_watchdog_when_seeded(client):
    _seed_card("aggregator", ["aggregator"])
    _seed_card("watchdog-1", ["watchdog"])
    _seed_card("gemma-1", ["reasoner"])
    body = client.get("/api/registry").json()
    ids = {e["agent_id"] for e in body}
    # Registry shows ALL fleet members; client filters by role for sidebar.
    assert ids == {"aggregator", "watchdog-1", "gemma-1"}
