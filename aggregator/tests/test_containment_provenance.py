"""Follower, provenance joins, exactly-once cursor and deterministic detectors.

Real agentd nodes produce the events through their dispatch, ingest and lineage
boundaries; Core ingests those exact events; the monitor follows Core.
"""

import json
import sys
from pathlib import Path

import pytest

from aggregator.containment_monitor.config import MonitorConfig
from aggregator.containment_monitor.model import inv, task, ver
from containment_support import Harness, TraceBuilder, ingest

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "agent-runtime/tests/agentd")
)
from containment_test_support import Node, deliver  # noqa: E402


def journal(node):
    rows = node.store._connection.execute(
        "SELECT event_json FROM trace_journal_all ORDER BY received_at_ms,source_seq"
    ).fetchall()
    return [json.loads(row[0]) for row in rows]


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child", "other"])
    worker.managed("child", outbound=["parent", "other"])
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


@pytest.fixture
def harness(tmp_path):
    value = Harness(tmp_path / "monitor")
    try:
        yield value
    finally:
        value.close()


def delegate_and_complete(root, worker, *, read=True):
    root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    deliver(root, worker)
    worker.start("child")
    worker.complete("child", child_id, {"body": "result"})
    deliver(worker, root)
    if read:
        root.rpc("parent", "task.get", task_id=child_id)
    return child_id


def edges(db):
    return set(db.execute("SELECT src,dst,etype FROM containment_edges"))


def test_real_delegation_joins_across_nodes_by_task_and_version(nodes, harness):
    root, worker = nodes
    child_id = delegate_and_complete(root, worker)
    for event in journal(root) + journal(worker):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    graph = harness.monitor.graph
    (parent_attempt,) = graph.running  # the parent is still running; the child ended
    child_attempt = graph.invocations_of_task(child_id)[0]
    request, result = (
        harness.db.execute(
            "SELECT version_id FROM containment_versions WHERE entity_task_id=? AND entity=?",
            (child_id, entity),
        ).fetchone()[0]
        for entity in ("request", "result")
    )
    assert {
        (inv(parent_attempt), ver(request), "data"),
        (ver(request), inv(child_attempt), "data"),
        (inv(child_attempt), ver(result), "data"),
        (ver(result), inv(parent_attempt), "data"),
        (inv(parent_attempt), task(child_id), "authority"),
        (task(child_id), inv(child_attempt), "authority"),
    } <= edges(harness.db)
    assert not graph.pending and not graph.results
    assert graph.agent_nodes == {"parent": "root-node", "child": "worker-node"}
    root_request = harness.db.execute(
        "SELECT external FROM containment_versions v JOIN containment_tasks t "
        "ON v.version_id=t.request_version WHERE t.recipient_id='parent'"
    ).fetchone()
    assert root_request == (1,)


def test_live_state_tracks_pending_tasks_and_unread_results(nodes, harness):
    root, worker = nodes
    root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    for event in journal(root):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    graph = harness.monitor.graph
    assert set(graph.pending) == {child_id}
    assert graph.pending[child_id].recipient_node is None  # not observed there yet
    deliver(root, worker)
    worker.start("child")
    worker.complete("child", child_id, {"body": "unread"})
    for event in journal(worker):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    assert not graph.pending and len(graph.running) == 1  # the parent; the child ended
    (pending_result,) = graph.results.values()
    assert pending_result.task_id == child_id


def test_cross_node_arrival_order_does_not_change_the_graph(nodes, tmp_path):
    root, worker = nodes
    delegate_and_complete(root, worker)
    states = []
    for order in ("root-first", "worker-first"):
        harness = Harness(tmp_path / order)
        events = journal(root) + journal(worker)
        if order == "worker-first":
            events = journal(worker) + journal(root)
        for event in events:
            ingest(harness.core, event)
        harness.monitor.catch_up()
        graph = harness.monitor.graph
        states.append(
            (
                edges(harness.db),
                set(
                    harness.db.execute(
                        "SELECT task_id,recipient_id,recipient_node,parent_attempt_id,state FROM containment_tasks"
                    )
                ),
                set(graph.running),
                set(graph.results),
                dict(graph.counts),
            )
        )
        harness.close()
    assert states[0] == states[1]


def scripted_events():
    trace = TraceBuilder()
    root_task = trace.root("kipp", "n0", 0)
    kipp = trace.start(root_task, 10)
    child = trace.dispatch(kipp, "tars", "n1", 20)
    trace.arrive(child, 30)
    tars = trace.start(child, 40)
    trace.complete(tars, 60)
    trace.read_result(kipp, child, 80)
    return trace.events


def test_exactly_once_cursor_survives_failed_batches_and_restart(tmp_path, monkeypatch):
    events = scripted_events()
    reference = Harness(tmp_path / "reference")
    for event in events:
        ingest(reference.core, event)
    reference.monitor.catch_up()
    expected = (edges(reference.db), dict(reference.monitor.graph.counts))
    reference.close()

    harness = Harness(tmp_path / "faulty")
    harness.monitor.batch = 5
    for event in events:
        ingest(harness.core, event)
    graph = harness.monitor.graph
    original = graph.apply
    calls = {"n": 0}

    def flaky(event, seq):
        calls["n"] += 1
        if calls["n"] == 8:
            raise RuntimeError("injected")
        original(event, seq)

    monkeypatch.setattr(graph, "apply", flaky)
    assert harness.monitor.follow() == 5
    after_first = (edges(harness.db), dict(graph.counts), set(graph.running))
    with pytest.raises(RuntimeError):
        harness.monitor.follow()  # second batch fails part-way and rolls back
    assert harness.monitor.cursor() == 5
    assert (edges(harness.db), dict(graph.counts), set(graph.running)) == after_first
    monkeypatch.setattr(graph, "apply", original)
    harness.monitor.catch_up()
    assert (edges(harness.db), dict(graph.counts)) == expected
    # Restart: nothing is applied twice and memory is rebuilt from the tables.
    cursor = harness.monitor.cursor()
    running = set(graph.running)
    reopened = harness.reopen()
    assert reopened.follow() == 0 and reopened.cursor() == cursor
    assert set(reopened.graph.running) == running
    assert (edges(harness.db), dict(reopened.graph.counts)) == expected
    harness.close()


def test_failed_batch_leaves_memory_equal_to_the_tables(tmp_path, monkeypatch):
    harness = Harness(tmp_path / "rollback")
    for event in scripted_events():
        ingest(harness.core, event)
    graph = harness.monitor.graph
    original = graph.apply

    def fail_late(event, seq):
        original(event, seq)
        if seq == 9:
            raise RuntimeError("injected")

    monkeypatch.setattr(graph, "apply", fail_late)
    with pytest.raises(RuntimeError):
        harness.monitor.follow()
    assert harness.monitor.cursor() == 0
    assert (graph.running, graph.pending, graph.counts) == (
        {},
        {},
        {"nodes": 0, "edges": 0},
    )
    harness.close()


def alerts(harness):
    return [
        (kind, subject, json.loads(detail)["detail"])
        for kind, subject, detail in harness.db.execute(
            "SELECT kind,subject,alert_json FROM containment_alerts ORDER BY observed_ms,kind"
        )
    ]


def test_detectors_report_scope_and_grant_denials_but_not_holds(nodes, harness):
    root, worker = nodes
    root.submit("parent")
    _, binding = root.start("parent")
    root.dispatch("parent", binding, "child")
    root.dispatch("parent", binding, "outsider")  # not in the grant: not_authorized
    deliver(root, worker)
    _, child_binding = worker.start("child")
    # Inherited scope {child, other} ∩ grant {parent, other}: "parent" is outside.
    worker.dispatch("child", child_binding, "parent")
    worker.dispatch("child", child_binding, "other")
    held_task = root.submit("parent")
    del held_task
    for event in journal(root) + journal(worker):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    found = alerts(harness)
    assert sorted(
        (kind, detail["reason"], detail["recipient_id"]) for kind, _, detail in found
    ) == [
        ("admission_denied", "outside_authority_scope", "parent"),
        ("admission_denied", "permission_denied", "outsider"),
    ]
    # A hold makes the next dispatch fail as execution_held: evidence, not an alert.
    worker.store.place_hold(
        kind="task",
        object_id=child_binding["task_id"],
        owner="core-monitor",
        policy_version=1,
    )
    worker.dispatch("child", child_binding, "other")
    for event in journal(worker)[-2:]:
        ingest(harness.core, event, export_seq=2)
    harness.monitor.catch_up()
    assert len(alerts(harness)) == 2
    assert harness.monitor.graph.blocked_dispatches == 1


def test_revoked_version_consumption_and_lease_loss_are_detected(nodes, harness):
    root, worker = nodes
    child_id = delegate_and_complete(root, worker, read=False)
    for event in journal(root) + journal(worker):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    (result,) = harness.monitor.graph.results
    from aggregator.containment_monitor.model import parse_external_alert

    harness.monitor.submit(
        parse_external_alert(
            {"kind": "version_revoked", "version_id": result}, now_ms=1
        )
    )
    before = len(journal(root))
    root.rpc("parent", "task.get", task_id=child_id)  # the parent now consumes it
    for event in journal(root)[before:]:
        ingest(harness.core, event, export_seq=3)
    harness.monitor.catch_up()
    consumed = [a for a in alerts(harness) if a[0] == "revoked_version_consumed"]
    assert len(consumed) == 1 and consumed[0][2] == {"version_id": result}

    trace = TraceBuilder()
    attempt = trace.start(trace.root("solo", "n9", 0), 1)
    trace.end(attempt, 5, "interrupted", "session_unavailable")
    trace.end(
        trace.start(trace.root("solo", "n9", 10), 11),
        12,
        "interrupted",
        "session_closed",
    )
    harness.feed(trace.events)
    lease = [a for a in alerts(harness) if a[0] == "lease_violation"]
    assert [(subject, detail) for _, subject, detail in lease] == [
        (inv(attempt), {"reason": "session_unavailable"})
    ]


def test_session_edges_join_consecutive_invocations_of_one_conversation(harness):
    trace = TraceBuilder()
    first_task = trace.root(
        "hermes", "n1", 0, context="7b0a7ad8-58e4-4c4c-8d4f-6a3fb6ad1b11"
    )
    first = trace.start(first_task, 1)
    trace.complete(first, 2)
    second = trace.start(
        trace.root("hermes", "n1", 3, context="7b0a7ad8-58e4-4c4c-8d4f-6a3fb6ad1b11"), 4
    )
    isolated = Harness(
        harness.root.parent / "isolated",
        MonitorConfig(session_isolated_agents=frozenset({"hermes"})),
    )
    for target in (harness, isolated):
        target.feed(trace.events)
    assert (inv(first), inv(second), "session") in edges(harness.db)
    assert not any(etype == "session" for *_, etype in edges(isolated.db))
    isolated.close()
