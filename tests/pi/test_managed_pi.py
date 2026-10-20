"""Failure boundaries of the Pi integration; live acceptance covers real providers."""

import asyncio
import contextlib
import importlib.util
import json
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


class FakePi:
    """The session.mjs side of the private IPC, scripted per received message."""

    def __init__(self, script):
        self.script = script
        self.sent = []
        self.returncode = None
        self.lines = asyncio.Queue()
        harness_process = self

        class Stdin:
            def write(self, data):
                message = json.loads(data)
                harness_process.sent.append(message)
                for line in harness_process.script(message):
                    harness_process.lines.put_nowait(json.dumps(line).encode() + b"\n")

            async def drain(self):
                return None

        class Stdout:
            async def readline(self):
                return await harness_process.lines.get()

        self.stdin, self.stdout = Stdin(), Stdout()

    def of_type(self, kind):
        return [message for message in self.sent if message["type"] == kind]


def context_for(calls, task_states):
    async def progress(*_args, **_kwargs):
        return None

    @contextlib.asynccontextmanager
    async def operation(*_args, **_kwargs):
        yield

    def call(operation, **params):
        calls.append((operation, params))
        if operation == "trace.dispatch":
            return {"status": "ok"}
        if operation == "task.get":
            return task_states[0]
        if operation == "agent.list":
            return []
        raise AssertionError(operation)

    return SimpleNamespace(
        client=SimpleNamespace(call=call),
        trace=SimpleNamespace(
            binding_id="binding", task_id="parent", operation=operation
        ),
        publish_progress=progress,
        resumed=False,
        interruption=None,
    )


async def wait_for(predicate):
    for _ in range(500):
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was not reached")


def envelope(body="inspect the workspace"):
    return {
        "sender_id": "aggregator",
        "task_id": "parent",
        "payload": {"body": body, "args": {"request_id": "r-1"}},
    }


def test_pause_aborts_the_pi_turn_and_resume_continues_the_session(pi, monkeypatch):
    monkeypatch.setattr(pi, "ROLE", "worker")
    harness = pi.Harness()

    def script(message):
        if message["type"] == "abort":
            return [{"type": "error", "body": "aborted"}]
        if message["type"] == "prompt" and message["body"].startswith(pi.RESUME_NOTE):
            return [{"type": "result", "body": "finished after resume"}]
        return []

    async def scenario():
        harness.process = FakePi(script)
        context = context_for([], [])
        work = asyncio.create_task(harness.handle(envelope(), context))
        await wait_for(lambda: harness.process.of_type("prompt"))
        context.interruption = "pause"
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await work
        assert harness.process.of_type("abort")
        context.resumed, context.interruption = True, None
        return await harness.handle(envelope(), context)

    result, state = asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state == "completed" and result["body"] == "finished after resume"
    [first, resumed] = harness.process.of_type("prompt")
    assert first["body"] == "inspect the workspace"
    assert resumed["body"] == pi.RESUME_NOTE + "inspect the workspace"
    assert resumed["session_key"] == first["session_key"]


def test_a_resumed_root_waits_for_its_child_and_never_redispatches(pi, monkeypatch):
    monkeypatch.setattr(pi, "core_roster", lambda: [])
    monkeypatch.setattr(pi, "eligible_agents", lambda *_: ["endurance-pi"])
    monkeypatch.setattr(pi, "select", lambda *_: {"choice": "endurance-pi"})
    harness = pi.Harness()
    calls, task_states = [], [{"state": "running"}]

    def script(message):
        if message["type"] == "prompt":
            return [
                {"type": "call", "id": "route-1", "operation": "route", "body": "x"}
            ]
        if message["type"] == "reply" and message["id"] == "route-1":
            return [
                {
                    "type": "call",
                    "id": "delegate-1",
                    "operation": "delegate",
                    "recipient": "endurance-pi",
                }
            ]
        if message["type"] == "reply" and "result" in message:
            return [{"type": "result", "body": message["result"]["body"]}]
        if message["type"] == "abort":
            return [{"type": "error", "body": "aborted"}]
        return []

    async def scenario():
        harness.process = FakePi(script)
        context = context_for(calls, task_states)
        work = asyncio.create_task(harness.handle(envelope(), context))
        await wait_for(lambda: any(c[0] == "task.get" for c in calls))
        context.interruption = "pause"
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await work
        # The Pi tool call waiting on the adapter is rejected, ending the turn.
        assert {"type": "reply", "id": "delegate-1", "error": "interrupted"} in (
            harness.process.sent
        )
        task_states[0] = {"state": "completed", "result": {"body": "child result"}}
        context.resumed, context.interruption = True, None
        return await harness.handle(envelope(), context)

    result, state = asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state == "completed" and result["body"] == "child result"
    assert [c[0] for c in calls].count("trace.dispatch") == 1


def test_a_cancelled_turn_is_never_resumed_or_repeated(pi, monkeypatch):
    monkeypatch.setattr(pi, "ROLE", "worker")
    harness = pi.Harness()

    def script(message):
        return (
            [{"type": "error", "body": "aborted"}] if message["type"] == "abort" else []
        )

    async def scenario():
        harness.process = FakePi(script)
        context = context_for([], [])
        work = asyncio.create_task(harness.handle(envelope(), context))
        await wait_for(lambda: harness.process.of_type("prompt"))
        context.interruption = "cancel"
        work.cancel()
        with pytest.raises(asyncio.CancelledError):
            await work
        context.resumed, context.interruption = True, None
        return await harness.handle(envelope(), context)

    result, state = asyncio.run(asyncio.wait_for(scenario(), 10))
    assert state == "failed" and "not repeated" in result["body"]
    assert len(harness.process.of_type("prompt")) == 1
