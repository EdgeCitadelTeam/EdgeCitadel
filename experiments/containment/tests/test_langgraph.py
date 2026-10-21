"""The LangGraph baseline: the operator's state filters, the delivered-state
merge a fork relies on, and the controller's fork point. End-to-end runs are
the dry-run grid (experiments/dryrun-langgraph-c2.json)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

pytest.importorskip("langgraph")
pytest.importorskip("langgraph_sdk")

from harness import lg_control, lg_graph
from harness.native import LoopThread


def test_the_operator_filters_match_the_artifact_or_the_task():
    deliver = {"kind": "deliver", "item": "A@v1", "target": "L2.a"}
    create = {"kind": "create", "task": "L2.a", "input": {"item": "d_L2a"}}
    start = {"kind": "task", "task": "L2.a", "input": {"item": "A@v1"}}
    assert lg_graph.rejected(deliver, {"A@v1"}, set())
    assert lg_graph.rejected(start, {"A@v1"}, set())
    assert not lg_graph.rejected(create, {"A@v1"}, set())  # carries another item
    assert lg_graph.rejected(create, set(), {"L2.a"})
    assert lg_graph.rejected(deliver, set(), {"L2.a"})
    assert not lg_graph.rejected({"kind": "rogue_msg", "msg": "m"}, {"A@v1"}, {"x"})


def test_delivered_copies_merge_once_per_delivery():
    a = {"T": {"X": [{"id": "d1", "content": {"value": "1", "labels": []}}]}}
    b = {"T": {"X": [{"id": "d1", "content": {"value": "1", "labels": []}}]}}
    c = {"T": {"X": [{"id": "d2", "content": {"value": "1", "labels": []}}]}}
    merged = lg_graph._merge_delivered(lg_graph._merge_delivered(a, b), c)
    assert [copy["id"] for copy in merged["T"]["X"]] == ["d1", "d2"]
    assert a["T"]["X"] == [{"id": "d1", "content": {"value": "1", "labels": []}}]


class FakeThreads:
    def __init__(self, history):
        self.history = history

    async def get_history(self, thread_id, limit, before=None):
        start = 0
        if before is not None:
            start = next(
                i + 1 for i, e in enumerate(self.history) if e["checkpoint"] == before
            )
        return self.history[start : start + limit]


def entry(step, source, nxt, completed):
    return {
        "metadata": {"step": step, "source": source},
        "next": nxt,
        "values": {"completed": completed},
        "checkpoint": {"checkpoint_id": f"c{step}"},
    }


def test_time_travel_forks_at_the_last_supervisor_checkpoint_before_the_fault():
    history = [  # newest first
        entry(6, "loop", ["supervisor"], ["K.plan", "L1.a", "L2.a"]),
        entry(5, "loop", ["worker"], ["K.plan", "L1.a"]),
        entry(4, "loop", ["supervisor"], ["K.plan", "L1.a"]),
        entry(3, "loop", ["worker", "actuate"], ["K.plan"]),
        entry(2, "loop", ["supervisor"], ["K.plan"]),
        entry(1, "loop", ["worker"], []),
        entry(-1, "input", ["__start__"], []),
    ]
    loop = LoopThread("test")
    controller = lg_control.LangGraphController(
        loop,
        "http://unused",
        strategy="lg_time_travel",
        node="local",
        faulty_task="L1.a",
        faulty_agent="tars-l1",
        bad_item="A@v1",
        decisions_path=Path("/dev/null"),
        incident_id="native:test",
        run_config={},
    )
    client = type("C", (), {"threads": FakeThreads(history)})()
    fork = loop.run(controller._fork_point(client))
    lg_control.HISTORY_PAGE, page = 2, lg_control.HISTORY_PAGE
    try:
        paged = loop.run(controller._fork_point(client))  # found on the second page
    finally:
        lg_control.HISTORY_PAGE = page
    loop.close()
    assert fork["checkpoint"] == {"checkpoint_id": "c3"}
    assert paged["checkpoint"] == {"checkpoint_id": "c3"}


def test_an_unmanaged_run_takes_no_action(tmp_path):
    loop = LoopThread("test")
    controller = lg_control.LangGraphController(
        loop,
        "http://unused",
        strategy=None,
        node="local",
        faulty_task="L1.a",
        faulty_agent="tars-l1",
        bad_item="A@v1",
        decisions_path=tmp_path / "d.jsonl",
        incident_id="native:test",
        run_config={},
    )
    assert controller.alert({"kind": "operator"})["accepted"] is False
    controller.stop()
    loop.run(asyncio.sleep(0))
    loop.close()
    assert (tmp_path / "d.jsonl").read_text() == ""


class FakeEdit:
    def __init__(self):
        self.as_nodes = []
        self.threads = self
        self.runs = self

    async def update_state(self, thread_id, values, as_node, checkpoint):
        self.as_nodes.append(as_node)
        return {"checkpoint": {"checkpoint_id": "edited"}}

    async def create(self, thread_id, assistant, **kwargs):
        return {"run_id": "resumed"}


def test_the_edit_is_written_by_the_node_that_keeps_the_step_unduplicated(tmp_path):
    loop = LoopThread("test")
    controller = lg_control.LangGraphController(
        loop,
        "http://unused",
        strategy="lg_block_resume",
        node="local",
        faulty_task="L1.a",
        faulty_agent="tars-l1",
        bad_item="A@v1",
        decisions_path=tmp_path / "d.jsonl",
        incident_id="native:test",
        run_config={},
    )
    client = FakeEdit()

    def edit(nxt):
        target = {"next": nxt, "checkpoint": {"checkpoint_id": "c"}}
        return loop.run(
            controller._edit_and_resume(
                client, {"blocked_items": ["A@v1"]}, target, "block", "A@v1"
            )
        )

    cut_step = edit(["worker", "worker"])  # cancelled during a worker step: re-plan it
    between = edit(["supervisor"])  # the step's writes are in: the supervisor runs next
    ended = edit([])
    loop.close()
    assert client.as_nodes == ["supervisor", "worker"]
    assert [a["state"] for a in cut_step] == ["confirmed", "confirmed"]
    assert between[0]["as_node"] == "worker"
    assert [a["state"] for a in ended] == ["noop"]
