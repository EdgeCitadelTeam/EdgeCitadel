"""Dispatch-to-child edges, causes and used/generated lineage at agentd boundaries."""

from uuid import uuid4

import pytest

from containment_test_support import Node, deliver, ref
from edgecitadel_agentd.trace_contract import TraceContractError, validate_event


@pytest.fixture
def same_node(tmp_path):
    node = Node(tmp_path / "edge", "edge-a")
    node.managed("parent", outbound=["child"])
    node.managed("child")
    try:
        yield node
    finally:
        node.close()


@pytest.fixture
def two_nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def lineage(node, phase, entity):
    return [
        e for e in node.events("lineage", phase) if e["attributes"]["entity"] == entity
    ]


def test_local_delegation_links_dispatch_child_and_every_boundary(same_node):
    node = same_node
    root_id = node.submit("parent")
    parent, parent_binding = node.start("parent")
    assert parent["task_id"] == root_id
    reply = node.dispatch("parent", parent_binding, "child")
    child_id = reply["result"]["task_id"]

    # The root input is declared external; its cause is the task's arrival.
    root_queued = node.events("task", "queued")[0]
    [root_input] = lineage(node, "used", "request")
    assert root_input["attributes"]["external"] is True
    assert root_input["execution_attempt_id"] == parent_binding["execution_attempt_id"]
    assert root_input["causes"] == [ref(root_queued)]

    # dispatch -> child: named in attributes and as the child's first cause.
    [allowed] = node.events("dispatch", "allowed")
    assert allowed["attributes"]["child_task_id"] == child_id
    child_queued = [
        e for e in node.events("task", "queued") if e["task_id"] == child_id
    ]
    assert [e["causes"] for e in child_queued] == [[ref(allowed)]]
    [request] = lineage(node, "generated", "request")
    assert request["attributes"]["entity_task_id"] == child_id
    assert request["execution_attempt_id"] == parent_binding["execution_attempt_id"]

    # The child invocation starts from its arrival and uses exactly that version.
    _, child_binding = node.start("child")
    [child_started] = [
        e for e in node.events("run", "started") if e["task_id"] == child_id
    ]
    assert child_started["causes"] == [ref(child_queued[0])]
    child_input = next(
        e
        for e in lineage(node, "used", "request")
        if e["attributes"]["entity_task_id"] == child_id
    )
    assert child_input["attributes"]["external"] is False
    assert (
        child_input["attributes"]["version_id"] == request["attributes"]["version_id"]
    )

    node.complete("child", child_id, {"body": "verified"})
    [produced] = lineage(node, "generated", "result")
    assert produced["execution_attempt_id"] == child_binding["execution_attempt_id"]
    [child_completed] = [
        e for e in node.events("task", "completed") if e["task_id"] == child_id
    ]
    assert produced["causes"] == [ref(child_completed)]

    # result -> parent's consumption: recorded once, on the parent's read.
    for _ in range(2):
        assert node.rpc("parent", "task.get", task_id=child_id)["state"] == "completed"
    [consumed] = lineage(node, "used", "result")
    assert consumed["execution_attempt_id"] == parent_binding["execution_attempt_id"]
    assert consumed["attributes"]["version_id"] == produced["attributes"]["version_id"]
    assert consumed["causes"] == [ref(child_completed)]


def test_cross_node_versions_match_without_sharing_identifiers(two_nodes):
    root, worker = two_nodes
    root.submit("parent")
    _, parent_binding = root.start("parent")
    child_id = root.dispatch("parent", parent_binding, "child")["result"]["task_id"]
    assert deliver(root, worker) == 1

    _, child_binding = worker.start("child")
    worker_arrival = worker.events("task", "queued")[0]
    assert worker_arrival["task_id"] == child_id
    [worker_input] = lineage(worker, "used", "request")
    [root_request] = lineage(root, "generated", "request")
    assert worker_input["trace_id"] == root_request["trace_id"]
    assert worker_input["causes"] == [ref(worker_arrival)]
    assert worker_input["attributes"]["external"] is False
    assert (
        worker_input["attributes"]["version_id"]
        == root_request["attributes"]["version_id"]
    )

    worker.complete("child", child_id, {"body": "done", "transcript": "t.jsonl"})
    assert deliver(worker, root) == 1
    root.rpc("parent", "task.get", task_id=child_id)
    [produced] = lineage(worker, "generated", "result")
    [consumed] = lineage(root, "used", "result")
    assert produced["attributes"]["version_id"] == consumed["attributes"]["version_id"]
    root_arrival = [
        e for e in root.events("task", "completed") if e["task_id"] == child_id
    ]
    assert consumed["causes"] == [ref(root_arrival[0])]
    assert root_arrival[0]["evidence_kind"] == "integration_reported"


def test_reads_by_non_parents_and_unfinished_children_record_no_consumption(
    same_node,
):
    node = same_node
    node.submit("parent")
    _, binding = node.start("parent")
    child_id = node.dispatch("parent", binding, "child")["result"]["task_id"]
    node.rpc("parent", "task.get", task_id=child_id)  # still queued
    node.start("child")
    node.rpc("child", "task.get", task_id=child_id)  # recipient, not consumer
    node.complete("child", child_id, {"body": "x"})
    node.rpc("child", "task.get", task_id=child_id)
    assert lineage(node, "used", "result") == []


def test_contract_requires_child_on_allowed_dispatch_and_internal_producers(
    same_node,
):
    node = same_node
    node.submit("parent")
    _, binding = node.start("parent")
    node.dispatch("parent", binding, "child")
    denied = node.dispatch("parent", binding, "nobody")
    assert denied["status"] == "error"
    [allowed] = node.events("dispatch", "allowed")
    [rejected] = node.events("dispatch", "denied")
    assert "child_task_id" not in rejected["attributes"]
    for broken in (
        {**allowed, "attributes": {**allowed["attributes"], "child_task_id": None}},
        {
            **rejected,
            "attributes": {**rejected["attributes"], "child_task_id": str(uuid4())},
        },
    ):
        with pytest.raises(TraceContractError):
            validate_event(broken)
    [produced] = lineage(node, "generated", "request")
    with pytest.raises(TraceContractError, match="invalid_lineage_origin"):
        validate_event(
            {**produced, "attributes": {**produced["attributes"], "external": True}}
        )
    without = {k: v for k, v in allowed["attributes"].items() if k != "child_task_id"}
    with pytest.raises(TraceContractError, match="invalid_dispatch_child"):
        validate_event({**allowed, "attributes": without})


def test_trace_use_records_a_delivered_request_used_from_memory(tmp_path):
    """An invocation declares it used a request delivered to its agent earlier:
    lineage links it to that version (hence to its producer); only deliveries to
    the same agent qualify, and a repeat returns the recorded event."""
    import json
    from uuid import uuid4

    from containment_test_support import Node

    node = Node(tmp_path / "node", "node-a")
    node.managed("producer", outbound=["consumer", "other"])
    node.managed("consumer")
    node.managed("other")
    try:
        node.submit("producer")
        _, producer = node.start("producer")
        pushed = node.dispatch("producer", producer, "consumer", request="item v1")
        foreign = node.dispatch("producer", producer, "other", request="not yours")
        delivery = pushed["result"]["task_id"]
        stored, _ = node.start("consumer")
        node.complete("consumer", stored["task_id"], {"body": "kept"})
        node.submit("consumer", "later work")
        task, consumer = node.start("consumer")

        def use(task_id, request_id=None):
            return node.rpc(
                "consumer",
                "trace.use",
                schema_version=1,
                request_id=request_id or str(uuid4()),
                binding_id=consumer["binding_id"],
                task_id=task_id,
            )

        request_id = str(uuid4())
        reply = use(delivery, request_id)
        generated = [
            e
            for e in node.events("lineage", "generated")
            if e["attributes"]["entity_task_id"] == delivery
        ]
        assert reply["result"]["version_id"] == generated[0]["attributes"]["version_id"]
        (used,) = [
            e
            for e in node.events("lineage", "used")
            if e["execution_attempt_id"] == consumer["execution_attempt_id"]
            and e["attributes"]["entity_task_id"] == delivery
        ]
        assert used["event_id"] == reply["result"]["event_id"]
        assert use(delivery, request_id) == reply
        assert use(delivery)["result"]["event_id"] == reply["result"]["event_id"]
        assert use(foreign["result"]["task_id"])["code"] == "not_authorized"
        assert use(task["task_id"])["code"] == "not_authorized"  # bind records it
        assert json.dumps(reply)  # a plain RPC reply
    finally:
        node.close()
