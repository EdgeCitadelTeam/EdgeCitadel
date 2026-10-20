"""Run completeness derived from real agentd evidence, end to end into Core.

Two in-process agentd nodes produce their journals through the real dispatch,
ingest and lineage boundaries; Core ingests those exact events and projects them.
"""

import json
import sys
from pathlib import Path

import pytest

from aggregator import trace_projection_store as projection
from test_trace_projection_store import core as core_fixture, ingest

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "agent-runtime/tests/agentd")
)
from containment_test_support import Node, deliver  # noqa: E402

core = core_fixture


def journal(node):
    rows = node.store._connection.execute(
        "SELECT event_json FROM trace_journal_all ORDER BY received_at_ms,source_seq"
    ).fetchall()
    return [json.loads(row[0]) for row in rows]


def export(core, events, skip=lambda event: False):
    for event in events:
        if not skip(event):
            ingest(core, event)
    while (
        projection.project_batch(core).ingest_cursor
        < core.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0]
    ):
        pass


@pytest.fixture
def delegation(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def run(root, worker):
    root.submit("parent")
    _, binding = root.start("parent")
    reply = root.dispatch("parent", binding, "child")
    deliver(root, worker)
    child_id = reply["result"]["task_id"]
    worker.start("child")
    worker.complete("child", child_id, {"body": "done"})
    deliver(worker, root)
    root.rpc("parent", "task.get", task_id=child_id)
    return reply["result"]["trace_id"], child_id


def test_closed_lineage_and_observed_children_make_a_run_complete(core, delegation):
    root, worker = delegation
    trace_id, _ = run(root, worker)
    export(core, journal(root))
    before = projection.read_graph(core, trace_id=trace_id)
    # The child was dispatched but its executor has not reported yet.
    assert before["coverage"]["unknown_sources"] and before["coverage"]["partial"]
    assert before["coverage_reasons"]["participant_set_unknown"]
    export(core, journal(worker))
    after = projection.read_graph(core, trace_id=trace_id)
    assert after["coverage"] == {
        **after["coverage"],
        "partial": False,
        "unknown_sources": False,
        "catching_up": False,
        "gap": False,
    }
    assert not any(after["coverage_reasons"].values())


def test_consumed_result_without_recorded_producer_stays_partial(core, delegation):
    root, worker = delegation
    trace_id, child_id = run(root, worker)

    def produced_result(event):
        return (
            event["kind"] == "lineage"
            and event["phase"] == "generated"
            and event["attributes"]["entity"] == "result"
        )

    export(core, journal(root) + journal(worker), skip=produced_result)
    value = projection.read_graph(core, trace_id=trace_id)
    assert value["coverage"]["partial"] and not value["coverage"]["unknown_sources"]
    assert value["coverage_reasons"]["unproduced_input"]
    assert not value["coverage_reasons"]["input_unreported"]
