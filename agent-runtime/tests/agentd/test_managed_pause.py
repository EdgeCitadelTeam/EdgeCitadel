"""Containment of running managed work: pause, resume, cancel and held results.

The daemon and the production runtime loop run for real; only the handler is
scripted. Before this behaviour existed, a hold on a running task made its
terminal transition fail and the runtime loop exited.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from containment_test_support import Node
from managed_runtime_support import AGENT, eventually, managed_agent


@pytest.mark.asyncio
async def test_hold_pauses_a_running_handler_and_release_resumes_it(
    tmp_path, monkeypatch
):
    calls: list[tuple[str, Any]] = []

    async def handler(envelope: dict[str, Any], context: Any):
        calls.append(("start", context.resumed))
        if not context.resumed:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                calls.append(("interrupted", context.interruption))
                raise
        return {"body": "resumed work"}, "completed"

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        task_id = agent.submit()
        await eventually(lambda: agent.task(task_id)["state"] == "running")
        agent.hold(task_id)
        await eventually(lambda: ("interrupted", "pause") in calls)
        # Paused: still running and claimed, held, and not advanced.
        await asyncio.sleep(0.2)
        paused = agent.task(task_id)
        assert paused["state"] == "running"
        assert paused["containment_hold"]["owner"] == "core-monitor"
        assert calls == [("start", False), ("interrupted", "pause")]

        agent.release(task_id)
        await eventually(lambda: agent.task(task_id)["state"] == "completed")
        assert agent.task(task_id)["result"] == {"body": "resumed work"}
        assert calls[-1] == ("start", True)
        # One invocation: a single binding started and completed.
        await eventually(lambda: agent.journal("run")[-1]["phase"] == "completed")
        runs = agent.journal("run")
        assert [e["phase"] for e in runs] == ["started", "completed"]
        assert len({e["execution_attempt_id"] for e in runs}) == 1


@pytest.mark.asyncio
async def test_cancel_by_the_sender_aborts_the_handler_and_the_loop_continues(
    tmp_path, monkeypatch
):
    calls: list[tuple[str, Any]] = []

    async def handler(envelope: dict[str, Any], context: Any):
        if envelope["payload"]["body"] == "long":
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                calls.append(("interrupted", context.interruption))
                raise
        return {"body": "next"}, "completed"

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        task_id = agent.submit("long")
        await eventually(lambda: agent.task(task_id)["state"] == "running")
        with agent.store() as store:
            store.transition_task(
                task_id=task_id, state="cancelled", actor_id="operator"
            )
        await eventually(lambda: ("interrupted", "cancel") in calls)
        assert agent.task(task_id)["state"] == "cancelled"
        await eventually(lambda: agent.journal("run")[-1]["phase"] == "canceled")

        following = agent.submit("short")
        await eventually(lambda: agent.task(following)["state"] == "completed")


@pytest.mark.asyncio
async def test_a_result_returned_while_held_waits_for_release(tmp_path, monkeypatch):
    agent_ref: list[Any] = []

    async def handler(envelope: dict[str, Any], context: Any):
        # The hold lands after the work finished but before its result commits.
        # The call blocks the loop, so the watcher cannot interrupt in between.
        agent_ref[0].hold(envelope["task_id"])
        return {"body": "produced while held"}, "completed"

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        agent_ref.append(agent)
        task_id = agent.submit()
        await eventually(lambda: "containment_hold" in agent.task(task_id))
        await asyncio.sleep(0.3)
        waiting = agent.task(task_id)
        assert waiting["state"] == "running" and waiting["result"] is None
        agent.release(task_id)
        await eventually(lambda: agent.task(task_id)["state"] == "completed")
        assert agent.task(task_id)["result"] == {"body": "produced while held"}
        await eventually(lambda: agent.journal("run")[-1]["phase"] == "completed")
        assert [e["phase"] for e in agent.journal("run")] == ["started", "completed"]


@pytest.mark.asyncio
async def test_an_egress_hold_lets_the_handler_finish_but_holds_its_result(
    tmp_path, monkeypatch
):
    agent_ref: list[Any] = []
    calls: list[str] = []
    proceed = asyncio.Event()

    async def handler(envelope: dict[str, Any], context: Any):
        agent_ref[0].hold(envelope["task_id"], kind="egress")
        try:
            await proceed.wait()  # The watcher sees the egress hold meanwhile.
        except asyncio.CancelledError:
            calls.append("interrupted")
            raise
        calls.append("finished")
        return {"body": "kept until egress opens"}, "completed"

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        agent_ref.append(agent)
        task_id = agent.submit()
        await eventually(lambda: "containment_egress_hold" in agent.task(task_id))
        await asyncio.sleep(0.3)
        proceed.set()
        await eventually(lambda: calls == ["finished"])
        await asyncio.sleep(0.2)
        assert agent.task(task_id)["state"] == "running"
        agent.release(task_id, kind="egress")
        await eventually(lambda: agent.task(task_id)["state"] == "completed")
        assert calls == ["finished"]


@pytest.mark.asyncio
async def test_a_labelled_result_paused_at_commit_is_admitted_after_the_pause(
    tmp_path, monkeypatch
):
    """A frontier pause of a labelled task that finished: once the pause ends
    and the label stays, its result is held as an event and admitted alone."""
    agent_ref: list[Any] = []

    async def handler(envelope: dict[str, Any], context: Any):
        # Labelled, then paused, after the work finished and before the commit.
        agent_ref[0].hold(envelope["task_id"], kind="egress")
        agent_ref[0].hold(envelope["task_id"])
        return {"body": "admitted alone"}, "completed"

    def held_result(task_id: str) -> dict[str, Any] | None:
        with agent.store() as store:
            row = store._connection.execute(
                "SELECT * FROM held_events WHERE kind='result' AND subject_id=?",
                (task_id,),
            ).fetchone()
        return dict(row) if row else None

    async with managed_agent(tmp_path, monkeypatch, handler) as agent:
        agent_ref.append(agent)
        task_id = agent.submit()
        await eventually(lambda: "containment_hold" in agent.task(task_id))
        await asyncio.sleep(0.3)
        assert agent.task(task_id)["state"] == "running"
        assert held_result(task_id) is None  # refused by the pause itself

        agent.release(task_id)
        await eventually(lambda: held_result(task_id) is not None)
        assert agent.task(task_id)["state"] == "running"
        event = held_result(task_id)
        assert event is not None and event["state"] == "held"
        agent.control("release", {"kind": "event", "object_id": event["event_id"]}, 1)
        await eventually(lambda: agent.task(task_id)["state"] == "completed")
        assert agent.task(task_id)["result"] == {"body": "admitted alone"}
        assert held_result(task_id)["state"] == "admitted"


def test_release_recovers_a_task_whose_runtime_exited_while_paused(tmp_path):
    node = Node(tmp_path / "node", "node-a")
    try:
        node.managed(AGENT)
        running_id = node.submit(AGENT)
        node.start(AGENT)
        session = node.agents[AGENT].session_id
        accepted_id = node.submit(AGENT)
        node.rpc(AGENT, "task.claim", session_id=session)
        for task_id in (running_id, accepted_id):
            node.store.place_hold(
                kind="task", object_id=task_id, owner="core-monitor", policy_version=1
            )
        agent = node.agents[AGENT]
        node.store.close_session(
            connector_id=agent.connector_id, token=agent.token, session_id=session
        )
        # Session recovery skips held work.
        assert node.store.get_task(running_id)["state"] == "running"
        assert node.store.get_task(accepted_id)["state"] == "accepted"
        for task_id in (running_id, accepted_id):
            node.store.release_hold(
                kind="task", object_id=task_id, owner="core-monitor", policy_version=1
            )
        failed = node.store.get_task(running_id)
        assert failed["state"] == "failed"
        assert failed["terminal_reason"] == "executor_session_lost"
        assert node.store.get_task(accepted_id)["state"] == "queued"
    finally:
        node.close()
