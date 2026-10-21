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

    async def get_history(self, thread_id, limit):
        return self.history


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
    loop.close()
    assert fork == {"checkpoint_id": "c3"}


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
