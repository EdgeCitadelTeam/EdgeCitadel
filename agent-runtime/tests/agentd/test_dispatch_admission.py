"""trace.dispatch admission: containment holds, delegation grants and scope attenuation."""

import json
from uuid import uuid4

import pytest

from containment_test_support import Node, deliver
from edgecitadel_agentd.service import dispatch as rpc_dispatch
from edgecitadel_agentd.store import StoreError


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def decisions(node):
    return [
        (e["phase"], e["attributes"]["policy"], e["attributes"]["reason"])
        for e in node.events("permission")
    ]


def test_held_invocation_cannot_dispatch_and_the_denial_is_receipted(nodes):
    root, _ = nodes
    root.managed("parent", outbound=["child"])
    task_id = root.submit("parent")
    _, binding = root.start("parent")
    root.store.place_hold(
        kind="task", object_id=task_id, owner="core-monitor", policy_version=3
    )
    request_id = str(uuid4())
    reply = root.dispatch("parent", binding, "child", request_id=request_id)
    assert reply == {
        "schema_version": 1,
        "operation": "dispatch",
        "request_id": request_id,
        "status": "error",
        "code": "execution_held",
        "retryable": False,
    }
    [permission] = root.events("permission", "denied")
    assert permission["attributes"]["policy"] == "containment_hold"
    assert permission["attributes"]["policy_version"] == "3"
    assert root.events("dispatch", "denied")[0]["attributes"]["reason"] == (
        "execution_held"
    )
    assert root.store.list_tasks(recipient_id="child") == []
    # A retried request returns its receipt, even after release.
    root.store.release_hold(
        kind="task", object_id=task_id, owner="core-monitor", policy_version=3
    )
    assert root.dispatch("parent", binding, "child", request_id=request_id) == reply
    assert root.dispatch("parent", binding, "child")["status"] == "ok"


def test_native_connector_delegates_only_to_granted_recipients(nodes):
    root, _ = nodes
    token = root.store.register_connector(
        connector_id="pi-local",
        host_type="pi",
        agent_id="edge-pi",
        capabilities=["edgecitadel_trace", "edgecitadel_delegate"],
    )
    session = root.store.open_session(connector_id="pi-local", token=token)
    binding = root.store.bind_trace(
        node_id="root-node",
        connector_id="pi-local",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session["session_id"],
            "task_id": None,
            "context_id": None,
        },
    )["result"]

    def dispatch(recipient):
        return root.store.dispatch_trace(
            node_id="root-node",
            connector_id="pi-local",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": binding["binding_id"],
                "recipient_id": recipient,
                "request": "inspect",
                "skill_id": None,
                "deadline_at_ms": None,
            },
        )

    assert dispatch("endurance-pi")["code"] == "not_authorized"
    with pytest.raises(StoreError, match="not authorized by its grant"):
        rpc_dispatch(
            root.store,
            {
                "version": 1,
                "operation": "task.create",
                "connector_id": "pi-local",
                "token": token,
                "params": {"recipient_id": "endurance-pi", "payload": {}},
            },
        )
    root.store.grant_connector(
        connector_id="pi-local", outbound_agents=["endurance-pi"]
    )
    allowed = dispatch("endurance-pi")
    assert allowed["status"] == "ok"
    assert dispatch("jim-eq-hermes")["code"] == "not_authorized"
    [envelope] = [m["envelope"] for m in root.store.pending_transport()]
    assert envelope["payload"]["execution_context"]["authority_scope"] == [
        "endurance-pi"
    ]
    assert [d[1] for d in decisions(root)] == ["native_connector_grant"] * 3


def test_child_scope_is_the_parent_scope_narrowed_by_the_local_grant(nodes):
    root, worker = nodes
    root.managed("parent", outbound=["child", "shared"])
    worker.managed("child", outbound=["shared", "beyond"])
    root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    [envelope] = [m["envelope"] for m in root.store.pending_transport()]
    assert envelope["payload"]["execution_context"]["authority_scope"] == [
        "child",
        "shared",
    ]
    deliver(root, worker)
    task, child_binding = worker.start("child")
    assert task["task_id"] == child_id

    # Granted locally but not by the delegating parent: rejected, with a type.
    beyond = worker.dispatch("child", child_binding, "beyond")
    assert beyond["status"] == "error" and beyond["code"] == "outside_authority_scope"
    assert decisions(worker)[-1] == (
        "denied",
        "managed_package_grant",
        "outside_authority_scope",
    )
    shared = worker.dispatch("child", child_binding, "shared")
    assert shared["status"] == "ok"
    grandchild = worker.store._connection.execute(
        "SELECT context_json FROM trace_task_contexts WHERE task_id=?",
        (shared["result"]["task_id"],),
    ).fetchone()[0]
    assert json.loads(grandchild)["authority_scope"] == ["shared"]


def test_delegation_without_an_inherited_scope_is_rejected_at_ingest(nodes):
    root, worker = nodes
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    root.submit("parent")
    _, binding = root.start("parent")
    root.dispatch("parent", binding, "child")
    [message] = root.store.pending_transport()
    envelope = message["envelope"]
    stripped = {
        **envelope,
        "payload": {
            **envelope["payload"],
            "execution_context": {
                **envelope["payload"]["execution_context"],
                "authority_scope": None,
            },
        },
    }
    with pytest.raises(StoreError, match="missing_authority_scope"):
        worker.store.ingest_transport_envelope(stripped)
    assert worker.store.list_tasks() == []
