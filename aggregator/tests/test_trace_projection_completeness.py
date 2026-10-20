"""Run completeness derived from real agentd evidence, end to end into Core.

Two in-process agentd nodes produce their journals through the real dispatch,
ingest and lineage boundaries; Core ingests those exact events and projects them.
"""

import json
import sys
import time
from pathlib import Path
from uuid import uuid4

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


def test_core_accepts_effect_and_containment_evidence(core, tmp_path):
    root = Node(tmp_path / "fx-root", "root-node")
    worker = Node(tmp_path / "fx-worker", "worker-node")
    write = {"effect_class": "fs.write", "target": "workspace/*"}
    try:
        root.managed("parent", outbound=["child"], effects=[write])
        worker.managed("child", effects=[write])
        root.submit("parent")
        _, binding = root.start("parent")
        reply = root.dispatch("parent", binding, "child", lease={"max_effects": 1})
        deliver(root, worker)
        _, child_binding = worker.start("child")

        def effect(target, **extra):
            return worker.rpc(
                "child",
                "effect.authorize",
                schema_version=1,
                request_id=str(uuid4()),
                binding_id=child_binding["binding_id"],
                effect_class="fs.write",
                target=target,
                tool_call_id=None,
                **extra,
            )

        assert effect("workspace/a")["status"] == "ok"
        assert effect("/etc/passwd")["code"] == "outside_effect_scope"
        assert effect("workspace/b")["code"] == "lease_exceeded"
        later = time.time_ns() // 1_000_000 + 60_000
        assert effect("workspace/c", scheduled_at_ms=later)["code"] == (
            "lease_exceeded"
        )
        events = journal(worker)
        kinds = {(e["kind"], e["phase"]) for e in events}
        assert {
            ("effect", "requested"),
            ("effect", "committed"),
            ("effect", "denied"),
            ("containment", "alert"),
        } <= kinds
        outcomes = {ingest(core, event).outcome for event in events}
        assert outcomes == {"accepted"}
        export(core, [])
        graph = projection.read_graph(core, trace_id=reply["result"]["trace_id"])
        assert graph["nodes"]
    finally:
        root.close()
        worker.close()
