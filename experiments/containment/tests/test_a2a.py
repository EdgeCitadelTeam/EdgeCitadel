"""The A2A baseline: its native controller's log scores with the monitor's code,
its port refuses work of a cancelled invocation, and its input block matches
only the named artifact. End-to-end runs are the dry-run grid
(experiments/dryrun-a2a-c2.json)."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("a2a")

from a2a.types import a2a_pb2 as pb
from a2a.utils.errors import TaskNotCancelableError

from harness import a2a_agent, a2a_common, a2a_control
from harness.metrics import decision_metrics


class FakePeers:
    """Two agents; agent a has two live tasks, one of which finishes before
    the operator's cancel reaches it."""

    def __init__(self) -> None:
        self.urls = {"a": "http://a/", "b": "http://b/"}
        self.live = {"a": ["t1", "t2"], "b": ["t3"]}
        self.blocked: list[tuple[str, str]] = []

    async def live_tasks(self, agent: str) -> list[pb.Task]:
        tasks = [pb.Task(id=t) for t in self.live[agent]]
        self.live[agent] = []
        return tasks

    async def cancel(self, agent: str, task_id: str) -> pb.Task:
        if task_id == "t2":
            raise TaskNotCancelableError()
        return pb.Task(
            id=task_id, status=pb.TaskStatus(state=pb.TaskState.TASK_STATE_CANCELED)
        )

    async def post(self, url, body, headers):
        self.blocked.append((url, body["item"]))
        return SimpleNamespace(status_code=200)


def run_controller(tmp_path, strategy: str) -> tuple[list[dict], FakePeers]:
    loop = a2a_common.LoopThread("test")
    peers = FakePeers()
    path = tmp_path / "core" / "containment-decisions.jsonl"
    controller = a2a_control.NativeController(
        loop,
        peers,  # type: ignore[arg-type]
        strategy=strategy,
        agent_nodes={"a": "core", "b": "edge-1"},
        faulty_agent="a",
        bad_item="A@v1",
        decisions_path=path,
        operator_token="op",
        incident_id="native:test",
    )
    reply = controller.alert({"kind": "operator", "attempt_id": "rt-origin"})
    assert reply["accepted"]
    loop.run(asyncio.sleep(0.35))
    controller.stop()
    loop.close()
    return [json.loads(line) for line in path.read_text().splitlines()], peers


def test_cancel_agent_sweeps_only_the_faulty_agent_and_scores_as_established(
    tmp_path,
):
    records, _ = run_controller(tmp_path, "a2a_cancel_agent")
    assert records[0]["type"] == "decision"
    actions = records[0]["phase1"]
    assert {a["action"]["object_id"] for a in actions} == {"t1", "t2"}
    states = {a["action"]["object_id"]: a["state"] for a in actions}
    # The task that finished first left nothing to stop: not a control failure.
    assert states == {"t1": "confirmed", "t2": "noop"}
    row = decision_metrics(records, t_alert_ms=records[0]["t_received"])
    assert row["established"] == 1
    assert row["control_failed"] == 0 and row["unconfirmed"] == 0
    assert row["n_logical_actions"] == 2
    assert row["time_to_established_s"] >= 0


def test_cancel_all_reaches_every_server(tmp_path):
    records, _ = run_controller(tmp_path, "a2a_cancel_all")
    objects = {a["action"]["object_id"] for r in records for a in r["phase1"]}
    assert objects == {"t1", "t2", "t3"}


def test_block_input_is_one_confirmed_action_per_server(tmp_path):
    records, peers = run_controller(tmp_path, "a2a_block_input")
    assert len(records) == 1
    assert sorted(peers.blocked) == [
        ("http://a/operator/block", "A@v1"),
        ("http://b/operator/block", "A@v1"),
    ]
    row = decision_metrics(records, t_alert_ms=records[0]["t_received"])
    assert row["established"] == 1 and row["n_logical_actions"] == 2


def test_an_unmanaged_run_takes_no_action(tmp_path):
    loop = a2a_common.LoopThread("test")
    controller = a2a_control.NativeController(
        loop,
        FakePeers(),  # type: ignore[arg-type]
        strategy=None,
        agent_nodes={"a": "core"},
        faulty_agent="a",
        bad_item="A@v1",
        decisions_path=tmp_path / "d.jsonl",
        operator_token="op",
        incident_id="native:test",
    )
    assert controller.alert({"kind": "operator"})["accepted"] is False
    controller.stop()
    loop.close()
    assert (tmp_path / "d.jsonl").read_text() == ""


def agent_with(blocked: set[str]) -> a2a_agent.Agent:
    agent = object.__new__(a2a_agent.Agent)
    agent.blocked = blocked
    agent.invocations = {}
    return agent


def test_the_input_block_matches_only_the_named_artifact():
    agent = agent_with({"A@v1"})
    assert agent.carries_blocked({"kind": "deliver", "item": "A@v1"}) == "A@v1"
    create = {"kind": "create", "input": {"item": "A@v1"}}
    assert agent.carries_blocked(create) == "A@v1"
    # Derived artifacts are not recognised: A2A carries no lineage.
    assert agent.carries_blocked({"kind": "deliver", "item": "B@v1"}) is None
    assert agent.carries_blocked({"kind": "task", "input": None}) is None


def test_a_cancelled_invocation_can_neither_dispatch_nor_act():
    agent = agent_with(set())
    invocation = a2a_agent.Invocation("rt1", a2a_agent.PortClient(agent, "rt1"))
    agent.invocations["rt1"] = invocation
    assert invocation.client.call("trace.use", task_id="d1")["status"] == "ok"
    invocation.cancelled.set()
    for operation in ("trace.dispatch", "effect.execute", "trace.use"):
        reply = invocation.client.call(operation)
        assert reply == {"status": "error", "code": a2a_agent.CANCELLED}


def test_the_a2a_stack_submits_external_requests_at_their_times():
    """The runner's per-node request schedule (EdgeCitadel's nodectl schedule
    needs agentd): each request is submitted to its agent's server at its time
    and marked submitted (the A2A task id is the invocation id) or refused."""
    import threading
    import time

    from harness.a2a_stack import A2ARunStack

    stack = object.__new__(A2ARunStack)
    stack._stop_schedules, stack._schedules = threading.Event(), []
    marks, sent = [], []

    def submit(node, recipient, body):
        sent.append((node, recipient, body["task"], time.time()))
        if body["task"] == "Q2":
            raise RuntimeError("server gone")
        return f"rt-{body['task']}"

    stack.submit = submit
    stack.services = lambda method, path, data: marks.append((method, path, data))
    now_ms = int(time.time() * 1000)
    stack.schedule(
        "core",
        [
            {
                "at_ms": now_ms + 200,
                "recipient": "assistant",
                "task": "Q2",
                "body": {"task": "Q2"},
            },
            {
                "at_ms": now_ms + 100,
                "recipient": "assistant",
                "task": "Q1",
                "body": {"task": "Q1"},
            },
        ],
    )
    stack._schedules[0].join(timeout=5)
    assert [s[2] for s in sent] == ["Q1", "Q2"]
    assert sent[0][3] >= (now_ms + 100) / 1000
    assert [(m[1], m[2]["name"], m[2]["task"]) for m in marks] == [
        ("/mark", "submitted", "Q1"),
        ("/mark", "refused", "Q2"),
    ]
    assert marks[0][2]["rt"] == "rt-Q1" and "server gone" in marks[1][2]["code"]


def test_only_the_langgraph_stack_can_be_busy():
    """The runner's end-of-run rules ask ``busy()``: always False for
    EdgeCitadel (full or bare) and A2A, so their runs end as before."""
    from harness.a2a_stack import A2ARunStack
    from harness.stack import RunStack

    assert RunStack.busy(None) is False
    assert A2ARunStack.busy is RunStack.busy
    assert A2ARunStack.alert_endpoint(None) is None
