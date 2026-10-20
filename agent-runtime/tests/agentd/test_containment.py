"""Containment holds at the real ingest, outbox, claim and admin RPC boundaries."""

from uuid import uuid4

import pytest

from containment_test_support import Node, deliver
from edgecitadel_agentd.containment import ContainmentError
from edgecitadel_agentd.service import dispatch as rpc_dispatch
from edgecitadel_agentd.store import StoreError

MONITOR = "core-monitor"


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def hold(node, object_id, version=1, kind="task", owner=MONITOR):
    return node.store.place_hold(
        kind=kind, object_id=object_id, owner=owner, policy_version=version
    )


def release(node, object_id, version=1, kind="task", owner=MONITOR):
    return node.store.release_hold(
        kind=kind, object_id=object_id, owner=owner, policy_version=version
    )


def delegate(root, child_id=None):
    root.submit("parent")
    _, binding = root.start("parent")
    extra = {"child_task_id": child_id} if child_id else {}
    return root.dispatch("parent", binding, "child", **extra)["result"]["task_id"]


def parked(node):
    return node.store._connection.execute(
        "SELECT COUNT(*) FROM held_deliveries"
    ).fetchone()[0]


def test_inbound_delegation_is_acknowledged_parked_and_applied_on_release(nodes):
    root, worker = nodes
    child_id = str(uuid4())
    # A hold may precede the task: the monitor saw the dispatch decision first.
    hold(worker, child_id)
    delegate(root, child_id)
    assert deliver(root, worker) == 1  # Ingest returns normally, so NATS acks.
    assert parked(worker) == 1
    with pytest.raises(StoreError, match="not found"):
        worker.store.get_task(child_id)
    assert (
        worker.rpc("child", "task.claim", session_id=worker.agents["child"].session_id)
        is None
    )

    assert release(worker, child_id) == {"released": True, "applied_deliveries": 1}
    assert parked(worker) == 0
    assert worker.store.get_task(child_id)["state"] == "offered"
    task, _ = worker.start("child")
    assert task["task_id"] == child_id


def test_inbound_result_is_parked_until_the_parent_may_consume_it(nodes):
    root, worker = nodes
    child_id = delegate(root)
    deliver(root, worker)
    worker.start("child")
    worker.complete("child", child_id, {"body": "suspect output"})
    hold(root, child_id)
    assert deliver(worker, root) == 1
    assert parked(root) == 1
    seen = root.rpc("parent", "task.get", task_id=child_id)
    assert seen["state"] == "queued" and seen["result"] is None
    assert seen["containment_hold"] == {"owner": MONITOR, "policy_version": 1}
    release(root, child_id)
    applied = root.rpc("parent", "task.get", task_id=child_id)
    assert applied["state"] == "completed"
    assert applied["result"]["body"] == "suspect output"


def test_outbound_messages_of_a_held_task_stay_in_the_outbox(nodes):
    root, worker = nodes
    child_id = delegate(root)
    # Queued before the hold: publication, not queueing, is the gate.
    hold(root, child_id)
    assert deliver(root, worker) == 0
    release(root, child_id)
    [message] = root.store.pending_transport()
    hold(root, str(message["message_id"]), kind="transport", version=2)
    assert deliver(root, worker) == 0
    release(root, str(message["message_id"]), kind="transport", version=2)
    assert deliver(root, worker) == 1
    assert worker.store.get_task(child_id)["state"] == "offered"


def test_held_task_cannot_be_claimed_or_advanced(nodes):
    root, _ = nodes
    task_id = root.submit("parent")
    hold(root, task_id)
    session = root.agents["parent"].session_id
    assert root.rpc("parent", "task.claim", session_id=session) is None
    with pytest.raises(StoreError, match="held by containment"):
        root.store.transition_task(
            task_id=task_id, state="cancelled", actor_id="parent"
        )
    release(root, task_id)
    assert root.rpc("parent", "task.claim", session_id=session)["task_id"] == task_id


def test_fencing_rejects_stale_and_foreign_decisions(nodes):
    root, _ = nodes
    first, second = str(uuid4()), str(uuid4())
    hold(root, first, version=3)
    with pytest.raises(ContainmentError, match="stale_policy_version"):
        release(root, first, version=2)
    with pytest.raises(ContainmentError, match="stale_policy_version"):
        hold(root, second, version=2)
    with pytest.raises(ContainmentError, match="hold_owned_by_another"):
        release(root, first, version=9, owner="other-monitor")
    # Each owner fences only its own decisions.
    hold(root, second, version=1, owner="other-monitor")
    release(root, first, version=3)
    with pytest.raises(ContainmentError, match="hold_not_found"):
        release(root, first, version=4)
    assert [h["object_id"] for h in root.store.list_holds()] == [second]
    for bad in (
        {"kind": "agent"},
        {"object_id": "not-a-uuid"},
        {"owner": "Bad Owner"},
        {"policy_version": 0},
        {"policy_version": True},
    ):
        params = {
            "kind": "task",
            "object_id": str(uuid4()),
            "owner": MONITOR,
            "policy_version": 5,
            **bad,
        }
        with pytest.raises(ContainmentError):
            root.store.place_hold(**params)


def test_containment_rpc_requires_the_management_credential(nodes):
    root, _ = nodes
    task_id = str(uuid4())
    request = {
        "version": 1,
        "operation": "containment.hold",
        "params": {
            "kind": "task",
            "object_id": task_id,
            "owner": MONITOR,
            "policy_version": 1,
        },
    }
    with pytest.raises(StoreError, match="management authentication"):
        rpc_dispatch(root.store, request, admin_token="a" * 48)
    held = rpc_dispatch(
        root.store, {**request, "admin_token": "a" * 48}, admin_token="a" * 48
    )
    assert held["object_id"] == task_id and held["reason"] == "containment"
    listed = rpc_dispatch(
        root.store,
        {"version": 1, "operation": "containment.list", "admin_token": "a" * 48},
        admin_token="a" * 48,
    )
    assert [item["object_id"] for item in listed] == [task_id]
    released = rpc_dispatch(
        root.store,
        {**request, "operation": "containment.release", "admin_token": "a" * 48},
        admin_token="a" * 48,
    )
    assert released == {"released": True, "applied_deliveries": 0}


def test_holds_and_parked_deliveries_survive_restart(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    child_id = str(uuid4())
    hold(worker, child_id)
    delegate(root, child_id)
    deliver(root, worker)
    root.close()
    worker.close()
    reopened = Node(tmp_path / "worker", "worker-node")
    try:
        assert parked(reopened) == 1
        # A crash after release but before application is finished by maintenance.
        with reopened.store._connection:
            reopened.store._connection.execute("DELETE FROM holds")
        reopened.store.reconcile()
        assert parked(reopened) == 0
        assert reopened.store.get_task(child_id)["state"] == "offered"
    finally:
        reopened.close()
