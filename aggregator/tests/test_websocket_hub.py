"""Unit tests for WebSocketHub. No FastAPI / live broker required —
the WebSocket protocol surface we depend on is a single async send_text;
the test substitutes a stub object that records what would have been
sent."""

import json
import pytest
from aggregator.websocket_hub import WebSocketHub


class FakeWS:
    """Minimal async WebSocket double — just records sent frames and
    supports a one-shot raise to simulate a closed/dead socket."""

    def __init__(self, *, raise_on_send: bool = False):
        self.sent: list[str] = []
        self.raise_on_send = raise_on_send

    async def send_text(self, msg: str) -> None:
        if self.raise_on_send:
            raise ConnectionError("fake closed socket")
        self.sent.append(msg)


@pytest.mark.asyncio
async def test_global_subscriber_receives_envelope():
    hub = WebSocketHub()
    ws = FakeWS()
    await hub.add_global(ws)

    env = {
        "v": 1,
        "type": "command",
        "sender_id": "agg",
        "recipient_id": "shell-1",
        "task_id": "t1",
    }
    await hub.broadcast(env)

    assert len(ws.sent) == 1
    frame = json.loads(ws.sent[0])
    assert frame["event"] == "message"
    assert frame["data"] == env


@pytest.mark.asyncio
async def test_per_agent_subscriber_filtered():
    """A subscriber on /ws/agent/shell-1 only sees envelopes whose
    sender_id or recipient_id matches shell-1."""
    hub = WebSocketHub()
    shell_ws = FakeWS()
    gemma_ws = FakeWS()
    await hub.add_agent("shell-1", shell_ws)
    await hub.add_agent("gemma-1", gemma_ws)

    # Envelope only mentions shell-1
    env = {"v": 1, "type": "command", "sender_id": "agg", "recipient_id": "shell-1"}
    await hub.broadcast(env)

    assert len(shell_ws.sent) == 1
    assert len(gemma_ws.sent) == 0

    # Envelope mentions both via sender + recipient
    env2 = {
        "v": 1,
        "type": "delegation",
        "sender_id": "shell-1",
        "recipient_id": "gemma-1",
    }
    await hub.broadcast(env2)

    assert len(shell_ws.sent) == 2
    assert len(gemma_ws.sent) == 1


@pytest.mark.asyncio
async def test_dead_subscriber_pruned_on_send_failure():
    hub = WebSocketHub()
    healthy = FakeWS()
    dead = FakeWS(raise_on_send=True)
    await hub.add_global(healthy)
    await hub.add_global(dead)

    env = {"v": 1, "type": "result", "sender_id": "x"}
    await hub.broadcast(env)

    # healthy got the message; dead was pruned and won't appear in stats
    assert len(healthy.sent) == 1
    assert hub.stats["global_subscribers"] == 1


@pytest.mark.asyncio
async def test_broadcast_event_typed_frame():
    hub = WebSocketHub()
    ws = FakeWS()
    await hub.add_agent("shell-1", ws)

    await hub.broadcast_event(
        "agent_status_change",
        {"agent_id": "shell-1", "agent_state": "offline"},
        agent_id="shell-1",
    )

    frame = json.loads(ws.sent[0])
    assert frame["event"] == "agent_status_change"
    assert frame["data"]["agent_id"] == "shell-1"
    assert frame["data"]["agent_state"] == "offline"


@pytest.mark.asyncio
async def test_remove_clears_both_global_and_per_agent():
    hub = WebSocketHub()
    ws = FakeWS()
    await hub.add_global(ws)
    await hub.add_agent("shell-1", ws)

    await hub.remove(ws)

    env = {"v": 1, "type": "command", "sender_id": "x", "recipient_id": "shell-1"}
    await hub.broadcast(env)

    assert ws.sent == []  # received nothing post-remove
