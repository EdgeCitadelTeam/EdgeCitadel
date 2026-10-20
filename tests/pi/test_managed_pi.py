"""Failure boundaries of the Pi integration; live acceptance covers real providers."""

import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def pi(tmp_path, monkeypatch):
    monkeypatch.setenv("EDGECITADEL_PLUGIN_STATE_DIR", str(tmp_path))
    monkeypatch.setenv("PI_ROLE", "root")
    source = Path(__file__).parents[2] / "agent-packages/pi-root/pi_runtime/__main__.py"
    spec = importlib.util.spec_from_file_location("managed_pi", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_remote_requires_explicit_grant_online_presence_and_card(pi):
    presence = [
        {"agent_id": a, "state": "online", "capabilities": []}
        for a in ["endurance-pi", "unknown-agent"]
    ]

    def card(agent, skill):
        return {
            "agent_id": agent,
            "agent_state": "online",
            "card": {"name": agent, "skills": [{"id": skill}]},
        }

    cards = [card("endurance-pi", "pi.execute"), card("unknown-agent", "pi.execute")]
    assert pi.eligible_agents(presence, cards) == ["endurance-pi"]
    cards[0]["card"]["skills"] = [{"id": "unrelated.skill"}]
    assert pi.eligible_agents(presence, cards) == []
    cards[0] = card("endurance-pi", "pi.execute")
    presence[0]["state"] = "unavailable"
    assert pi.eligible_agents(presence, cards) == []


def test_dispatch_id_is_durable_before_uncertain_transport_failure(pi):
    harness = pi.Harness()
    record = {
        "id": "r1",
        "body": "Write a file",
        "session_key": "a" * 64,
        "decision": {"choice": "endurance-pi"},
    }

    def call(operation, **params):
        assert operation == "trace.dispatch"
        import json
        import sqlite3

        saved = json.loads(
            sqlite3.connect(pi.STATE / "requests.sqlite3")
            .execute("SELECT document FROM requests WHERE id=?", ("r1",))
            .fetchone()[0]
        )
        assert saved["child_task_id"] == params["child_task_id"]
        assert saved["dispatch_attempted"] is True
        raise ConnectionError("transport outcome unknown")

    context = SimpleNamespace(
        client=SimpleNamespace(call=call), trace=SimpleNamespace(binding_id="binding")
    )
    with pytest.raises(ConnectionError):
        asyncio.run(harness.delegate({"recipient": "endurance-pi"}, context, record))
    with pytest.raises(ValueError, match="repeated execution refused"):
        asyncio.run(harness.delegate({"recipient": "endurance-pi"}, context, record))


def test_unknown_destination_cannot_be_granted_by_model(pi):
    harness = pi.Harness()
    with pytest.raises(ValueError, match="authorized roster"):
        asyncio.run(
            harness.delegate(
                {"recipient": "ungranted-agent"},
                None,
                {"decision": {"choice": "ungranted-agent"}},
            )
        )


def test_replayed_interrupted_request_never_starts_pi(pi):
    import hashlib

    harness = pi.Harness()
    session = hashlib.sha256(b"aggregator\0conversation").hexdigest()
    identity = hashlib.sha256(b"aggregator\0stable-request").hexdigest()
    harness.save(
        {
            "id": identity,
            "body": "write a file",
            "session_key": session,
            "child_task_id": "reserved-child",
        }
    )
    envelope = {
        "sender_id": "aggregator",
        "task_id": "different-parent",
        "payload": {
            "body": "write a file",
            "args": {"session_id": "conversation", "request_id": "stable-request"},
        },
    }
    result, state = asyncio.run(harness.handle(envelope, None))
    assert state == "failed"
    assert result["child_task_id"] == "reserved-child"
    assert harness.process is None
    envelope["payload"]["body"] = "different action"
    assert asyncio.run(harness.handle(envelope, None))[1] == "rejected"
