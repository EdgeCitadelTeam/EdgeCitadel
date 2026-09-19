"""Two overlapping scoped roots through the real owned agentd Unix socket."""

import asyncio
import json
import runpy
import threading
from pathlib import Path
from uuid import uuid4

from storage_test_support import paired_connect

import pytest

from edgecitadel_agentd.client import AgentdClient
from service_test_support import serve
from edgecitadel_agentd.service import socket_path_for
from edgecitadel_agentd.trace_producer import RuntimeTrace


@pytest.fixture
def owned_service(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "node.json").write_text('{"agent_id":"owned-edge"}')
    directory = state / "agentd"
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(directory, stop), daemon=True)
    thread.start()
    socket = socket_path_for(directory)
    try:
        for _ in range(300):
            if socket.exists():
                break
            assert thread.is_alive()
            assert not stop.wait(0.01)
        else:
            pytest.fail("owned socket did not become ready")
        yield socket, directory
    finally:
        stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()


async def exercise(socket, directory, recipient_services=None):
    admin = AgentdClient(
        socket, admin_token=(directory / "admin.token").read_text().strip()
    )

    def register(name, host, capabilities, endpoint=None):
        target_socket, target_directory = endpoint or (socket, directory)
        target_admin = (
            admin
            if endpoint is None
            else AgentdClient(
                target_socket,
                admin_token=(target_directory / "admin.token").read_text().strip(),
            )
        )
        registration = target_admin.call(
            "connector.register",
            connector_id=name,
            host_type=host,
            agent_id=name,
            capabilities=capabilities,
        )
        client = AgentdClient(
            target_socket, connector_id=name, token=registration["token"]
        )
        session = client.call("session.open", lease_seconds=300)
        return client, session["session_id"]

    parent, parent_session = register(
        "fixture-a",
        "codex",
        ["edgecitadel_trace", "edgecitadel_delegate", "edgecitadel_task_status"],
    )
    endpoints = recipient_services or {
        name: (socket, directory) for name in ("fixture-b", "fixture-c", "fixture-d")
    }
    recipients = {
        name: register(name, "managed-agent", [], endpoint)
        for name, endpoint in endpoints.items()
    }
    if recipient_services:
        for target_socket, _ in [(socket, directory), *endpoints.values()]:
            deadline = asyncio.get_running_loop().time() + 10
            while True:
                health = AgentdClient(target_socket).call("health")["transport"]
                if health["connected"] and health.get("ready_inbox_count") == 1:
                    break
                assert asyncio.get_running_loop().time() < deadline, (
                    "owned NATS inbox not ready"
                )
                await asyncio.sleep(0.01)
    roots = []
    for _ in range(2):
        reply = parent.call(
            "trace.bind",
            schema_version=1,
            request_id=str(uuid4()),
            session_id=parent_session,
            task_id=None,
            context_id=None,
        )
        assert reply["status"] == "ok"
        roots.append(reply["result"])
    assert roots[0]["trace_id"] != roots[1]["trace_id"]
    children = {}
    for index, root in enumerate(roots):
        for recipient in recipients:
            request = {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": root["binding_id"],
                "recipient_id": recipient,
                "request": f"root-{index}:{recipient}",
                "skill_id": None,
                "deadline_at_ms": None,
            }
            reply = parent.call("trace.dispatch", **request)
            assert reply["status"] == "ok"
            assert parent.call("trace.dispatch", **request) == reply
            child = reply["result"]
            assert child["parent_run_id"] == root["trace_id"]
            children[child["task_id"]] = {
                "trace_id": root["trace_id"],
                "recipient": recipient,
                "body": request["request"],
            }
    handler = runpy.run_path(
        str(
            Path(__file__).parents[3]
            / "agent-packages/examples/echo/runtime/__main__.py"
        )
    )["handle"]
    effects = []
    overlap = asyncio.Event()
    arrived = 0

    async def worker(recipient, client, session):
        nonlocal arrived
        for index in range(2):
            deadline = asyncio.get_running_loop().time() + 10
            while (task := client.call("task.claim", session_id=session)) is None:
                assert asyncio.get_running_loop().time() < deadline, (
                    "owned command not delivered"
                )
                await asyncio.sleep(0.01)
            assert (
                task is not None and children[task["task_id"]]["recipient"] == recipient
            )
            client.call(
                "task.transition",
                task_id=task["task_id"],
                state="running",
                session_id=session,
            )
            trace = RuntimeTrace(client)
            await trace.bind(session_id=session, task_id=task["task_id"])
            assert trace.binding_id is not None
            if index == 0:
                arrived += 1
                if arrived == 3:
                    overlap.set()
                await asyncio.wait_for(overlap.wait(), timeout=5)
            async with trace.operation("tool", "owned-echo"):
                result, phase = await handler(
                    {"type": "command", "payload": task["payload"]}, None
                )
                effects.append(task["task_id"])
            assert result == {"body": children[task["task_id"]]["body"]}
            client.call(
                "task.transition",
                task_id=task["task_id"],
                state=phase,
                result=result,
                session_id=session,
            )
            await trace.finish(phase)
            assert trace.dropped_observations == 0
        assert client.call("task.claim", session_id=session) is None

    await asyncio.gather(
        *(worker(name, *credentials) for name, credentials in recipients.items())
    )
    assert len(effects) == len(set(effects)) == 6
    for task_id, expected in children.items():
        deadline = asyncio.get_running_loop().time() + 10
        while (task := parent.call("task.get", task_id=task_id))[
            "state"
        ] != "completed":
            assert task["state"] not in {"failed", "rejected", "undeliverable"}
            assert asyncio.get_running_loop().time() < deadline, (
                "owned result not delivered",
                {
                    str(endpoint): AgentdClient(endpoint).call("health")["transport"]
                    for endpoint, _ in [(socket, directory), *endpoints.values()]
                },
            )
            await asyncio.sleep(0.01)
        expected_result = {"body": expected["body"]}
        if recipient_services:
            expected_result.update(
                trace_id=expected["trace_id"],
                execution_context={
                    "schema_version": 1,
                    "context_origin": "legacy_default",
                    "parent_run_id": expected["trace_id"],
                },
            )
        assert task["state"] == "completed" and task["result"] == expected_result
    for root in roots:
        reply = parent.call(
            "trace.finish",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=root["binding_id"],
            outcome="completed",
            reason="unknown",
        )
        assert reply["status"] == "ok"
    events = []
    persisted_task_ids = set()
    for database_dir in {directory, *(endpoint[1] for endpoint in endpoints.values())}:
        db = paired_connect(
            (database_dir / "agentd.sqlite3").as_uri() + "?mode=ro", uri=True
        )
        try:
            events.extend(
                json.loads(row[0])
                for row in db.execute("SELECT event_json FROM trace_journal")
            )
            persisted_task_ids.update(
                row[0] for row in db.execute("SELECT task_id FROM tasks")
            )
            assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        finally:
            db.close()
    tools = [event for event in events if event["kind"] == "tool"]
    assert len(tools) == 12
    for event in tools:
        expected = children[event["task_id"]]
        assert event["trace_id"] == expected["trace_id"]
        assert event["parent_run_id"] == expected["trace_id"]
        assert event["agent_id"] == expected["recipient"]
        receiver_dir = endpoints[expected["recipient"]][1]
        assert (
            event["node_id"]
            == json.loads((receiver_dir.parent / "node.json").read_text())["agent_id"]
        )
    for task_id in children:
        boundaries = [event for event in tools if event["task_id"] == task_id]
        assert {event["phase"] for event in boundaries} == {"started", "finished"}
        assert len({event["span_id"] for event in boundaries}) == 1
    assert persisted_task_ids == set(children)  # Native roots are observational.
    return {
        "roots": 2,
        "completed_children": 6,
        "effects": 6,
        "tool_boundaries": len(tools),
        "journal_events": len(events),
        "source_nodes": sorted({event["node_id"] for event in events}),
        "parentage_checked": True,
        "dispatch_retries_stable": True,
        "root_trace_ids": [root["trace_id"] for root in roots],
        "children": [
            {
                "task_id": task_id,
                "trace_id": expected["trace_id"],
                "recipient": expected["recipient"],
                "tool_event_ids": [
                    event["event_id"] for event in tools if event["task_id"] == task_id
                ],
                "tool_span_id": next(
                    event["span_id"] for event in tools if event["task_id"] == task_id
                ),
            }
            for task_id, expected in children.items()
        ],
    }


@pytest.mark.asyncio
async def test_two_roots_three_recipients_keep_socket_parentage(owned_service):
    result = await exercise(*owned_service)
    assert result["completed_children"] == 6
