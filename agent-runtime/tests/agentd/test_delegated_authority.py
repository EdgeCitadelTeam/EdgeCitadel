"""Delegated authority at the real dispatch, outbox and ingest boundaries.

Two in-process nodes exchange only committed outbox envelopes. Each decision is
made by the node that holds the state: revocations and filters reach
descendants through the authority chain carried in the execution context.
"""

import json
import pytest

from containment_test_support import Node, deliver
from edgecitadel_agentd import containment
from edgecitadel_agentd.containment import ContainmentError
from edgecitadel_agentd.store import StoreError

MONITOR = "core-monitor"
WRITE = {"effect_class": "fs.write", "target": "workspace/*"}
EXEC = {"effect_class": "process.exec", "target": "bash"}


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    # A child can reach only what its parent could: the root grants grandchild too.
    root.managed(
        "parent", outbound=["child", "other", "grandchild"], effects=[WRITE, EXEC]
    )
    worker.managed("child", outbound=["grandchild"], effects=[WRITE])
    worker.managed("other")
    worker.managed("grandchild", effects=[WRITE])
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def saved_context(node, task_id):
    row = node.store._connection.execute(
        "SELECT context_json FROM trace_task_contexts WHERE task_id=?", (task_id,)
    ).fetchone()
    return json.loads(row[0])


def delegate(root, **extra):
    """parent (root task) -> child on the worker; returns (parent, binding, child)."""
    parent_id = root.submit("parent")
    _, binding = root.start("parent")
    reply = root.dispatch("parent", binding, "child", **extra)
    assert reply["status"] == "ok", reply
    return parent_id, binding, reply["result"]["task_id"]


def test_child_inherits_attenuated_effects_lease_and_its_authority_chain(nodes):
    root, worker = nodes
    parent_id, _, child_id = delegate(
        root, lease={"wall_time_ms": 60_000, "max_effects": 3}
    )
    [message] = root.store.pending_transport()
    capsule = message["envelope"]["payload"]["execution_context"]
    assert capsule["authority_chain"] == [parent_id]
    assert capsule["effect_scope"] == [
        {"effect_class": "fs.write", "target": "workspace/*"},
        {"effect_class": "process.exec", "target": "bash"},
    ]
    assert capsule["lease"]["max_effects"] == 3
    deliver(root, worker)
    _, child_binding = worker.start("child")
    assert saved_context(worker, child_id)["authority_chain"] == [parent_id]
    reply = worker.dispatch("child", child_binding, "grandchild")
    context = saved_context(worker, reply["result"]["task_id"])
    assert context["authority_chain"] == [parent_id, child_id]
    # The worker's own grant narrows what the child inherited.
    assert context["effect_scope"] == [WRITE]


def test_revoking_a_parent_denies_its_descendants_on_another_node(nodes):
    root, worker = nodes
    parent_id, binding, child_id = delegate(root)
    deliver(root, worker)
    _, child_binding = worker.start("child")
    assert worker.dispatch("child", child_binding, "grandchild")["status"] == "ok"

    # The worker never saw the parent task; the chain carries its authority.
    confirmation = containment.revoke_authority(
        worker.store, task_id=parent_id, owner=MONITOR, policy_version=1
    )
    assert confirmation["local_tasks"] == 2  # the child and the queued grandchild
    denied = worker.dispatch("child", child_binding, "grandchild")
    assert denied["code"] == "authority_revoked"
    [permission] = worker.events("permission", "denied")
    assert permission["attributes"]["policy"] == "authority_revocation"
    assert permission["attributes"]["policy_version"] == "1"

    containment.revoke_authority(
        root.store, task_id=parent_id, owner=MONITOR, policy_version=1
    )
    assert root.dispatch("parent", binding, "other")["code"] == "authority_revoked"


def test_child_lease_cannot_exceed_the_parent_remainder(nodes):
    root, worker = nodes
    delegate(root, lease={"max_effects": 3, "wall_time_ms": 60_000})
    deliver(root, worker)
    task, binding = worker.start("child")
    child_id = task["task_id"]

    too_large = worker.dispatch(
        "child", binding, "grandchild", lease={"max_effects": 4}
    )
    assert too_large["code"] == "lease_exceeded"
    [alert] = worker.events("containment", "alert")
    assert alert["attributes"] == {
        "containment_version": 1,
        "alert": "lease_exceeded",
        "request_id": alert["attributes"]["request_id"],
        "lease_dimension": "effects",
    }
    assert alert["execution_attempt_id"] == binding["execution_attempt_id"]

    first = worker.dispatch("child", binding, "grandchild", lease={"max_effects": 2})
    # Two effects are reserved from the child for the grandchild; one remains,
    # and an omitted limit inherits exactly that remainder.
    second = worker.dispatch("child", binding, "grandchild")
    leases = [
        saved_context(worker, reply["result"]["task_id"])["lease"]
        for reply in (first, second)
    ]
    assert [lease["max_effects"] for lease in leases] == [2, 1]
    parent_expiry = saved_context(worker, child_id)["lease"]["expires_at_ms"]
    assert all(lease["expires_at_ms"] == parent_expiry for lease in leases)
    assert (
        worker.dispatch("child", binding, "grandchild", lease={"max_effects": 1})[
            "code"
        ]
        == "lease_exceeded"
    )


def test_an_expired_parent_lease_admits_no_delegation(nodes):
    root, worker = nodes
    delegate(root, lease={"wall_time_ms": 1})
    deliver(root, worker)
    _, binding = worker.start("child")
    denied = worker.dispatch("child", binding, "grandchild")
    assert denied["code"] == "lease_exceeded"
    assert (
        worker.events("containment", "alert")[0]["attributes"]["lease_dimension"]
        == "wall_time"
    )


def test_egress_hold_blocks_what_leaves_an_invocation_but_not_its_execution(nodes):
    root, worker = nodes
    parent_id, binding, child_id = delegate(root)
    # The request was queued before the block; publication is the gate.
    containment.place_hold(
        root.store, kind="egress", object_id=parent_id, owner=MONITOR, policy_version=1
    )
    assert deliver(root, worker) == 0
    assert root.dispatch("parent", binding, "other")["code"] == "egress_held"
    seen = root.store.get_task(parent_id)
    assert seen["containment_egress_hold"] == {"owner": MONITOR, "policy_version": 1}
    assert "containment_hold" not in seen
    with pytest.raises(StoreError, match="egress is held"):
        root.complete("parent", parent_id, {"body": "derived output"})
    # Not a pause: the invocation still runs and its task is not frozen.
    assert root.store.get_task(parent_id)["state"] == "running"

    containment.release_hold(
        root.store, kind="egress", object_id=parent_id, owner=MONITOR, policy_version=1
    )
    assert deliver(root, worker) == 1
    assert worker.store.get_task(child_id)["state"] == "offered"
    root.complete("parent", parent_id, {"body": "derived output"})


def test_ingress_filter_withholds_one_authority_into_one_target(nodes):
    root, worker = nodes
    parent_id, binding, child_id = delegate(root)
    target = {"agent_id": "child"}
    containment.place_filter(
        worker.store,
        authority={"task_id": parent_id},
        target=target,
        owner=MONITOR,
        policy_version=1,
    )
    # The receiving node parks the filtered delegation; the broker still acks.
    assert deliver(root, worker) == 1
    with pytest.raises(StoreError, match="not found"):
        worker.store.get_task(child_id)
    # Other traffic into the same target is unaffected.
    unrelated = root.submit("parent", "unrelated request")
    task, other_binding = root.start("parent")
    assert task["task_id"] == unrelated
    other_child = root.dispatch("parent", other_binding, "child")["result"]["task_id"]
    assert deliver(root, worker) == 1
    assert worker.store.get_task(other_child)["state"] == "offered"

    # At the sending node the same filter refuses the dispatch outright.
    containment.place_filter(
        root.store,
        authority={"task_id": unrelated},
        target={"agent_id": "other"},
        owner=MONITOR,
        policy_version=2,
    )
    assert root.dispatch("parent", other_binding, "other")["code"] == "ingress_filtered"
    assert root.dispatch("parent", other_binding, "child")["status"] == "ok"

    removed = containment.remove_filter(
        worker.store,
        authority={"task_id": parent_id},
        target=target,
        owner=MONITOR,
        policy_version=3,
    )
    assert removed == {"removed": True, "applied_deliveries": 1}
    assert worker.store.get_task(child_id)["state"] == "offered"


def test_controller_cancel_ends_a_held_task_and_tells_the_remote_executor(nodes):
    root, worker = nodes
    _, _, child_id = delegate(root)
    deliver(root, worker)
    worker.start("child")
    containment.place_hold(
        root.store,
        kind="task",
        object_id=child_id,
        owner="other-monitor",
        policy_version=1,
    )
    with pytest.raises(ContainmentError, match="hold_owned_by_another"):
        containment.cancel_task(
            root.store, task_id=child_id, owner=MONITOR, policy_version=1
        )
    containment.release_hold(
        root.store,
        kind="task",
        object_id=child_id,
        owner="other-monitor",
        policy_version=1,
    )
    containment.place_hold(
        root.store, kind="task", object_id=child_id, owner=MONITOR, policy_version=1
    )
    cancelled = containment.cancel_task(
        root.store, task_id=child_id, owner=MONITOR, policy_version=2
    )
    assert cancelled == {"state": "cancelled", "released_holds": ["task"]}
    assert root.store.get_task(child_id)["state"] == "cancelled"
    # The sender's node cancels toward the remote executor.
    assert deliver(root, worker) == 1
    assert worker.store.get_task(child_id)["state"] == "cancelled"
    assert containment.cancel_task(
        root.store, task_id=child_id, owner=MONITOR, policy_version=2
    ) == {"state": "cancelled", "released_holds": []}


def test_redelivered_delegation_is_idempotent(nodes):
    root, worker = nodes
    delegate(root)
    [message] = root.store.pending_transport()
    first = worker.store.ingest_transport_envelope(message["envelope"])
    # A crash between commit and broker ack redelivers the same envelope.
    again = worker.store.ingest_transport_envelope(message["envelope"])
    assert again["task_id"] == first["task_id"]
    assert (
        worker.store._connection.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='task.duplicate_conflict'"
        ).fetchone()[0]
        == 0
    )


def test_schema_31_upgrade_keeps_holds_and_extends_saved_contexts(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    parent_id, _, child_id = delegate(root)
    deliver(root, worker)
    worker.store.place_hold(
        kind="task", object_id=child_id, owner=MONITOR, policy_version=4
    )
    root.close()
    db = worker.store._connection
    # Return the pair to the schema 31 layout: holds without egress, no
    # authority tables, saved contexts without the schema 32 fields.
    with db:
        db.execute("ALTER TABLE task_state.holds RENAME TO holds_current")
        db.execute(
            "CREATE TABLE task_state.holds (kind TEXT NOT NULL CHECK(kind IN "
            "('task', 'transport')), object_id TEXT NOT NULL, reason TEXT NOT NULL "
            "CHECK(reason IN ('restore', 'containment')), owner TEXT NOT NULL, "
            "policy_version INTEGER NOT NULL CHECK(policy_version >= 0), held_at_ms "
            "INTEGER NOT NULL, PRIMARY KEY(kind, object_id, reason))"
        )
        db.execute(
            "INSERT INTO task_state.holds SELECT * FROM task_state.holds_current"
        )
        db.execute("DROP TABLE task_state.holds_current")
        for table in (
            "revocations",
            "ingress_filters",
            "authority_ledger",
            "control_commands",
        ):
            db.execute(f"DROP TABLE task_state.{table}")
        context = saved_context(worker, child_id)
        for key in ("effect_scope", "lease", "authority_chain"):
            del context[key]
        db.execute(
            "UPDATE trace_task_contexts SET context_json=? WHERE task_id=?",
            (json.dumps(context), child_id),
        )
        db.execute("PRAGMA main.user_version=31")
        db.execute("PRAGMA task_state.user_version=31")
    worker.close()
    upgraded = Node(tmp_path / "worker", "worker-node")
    try:
        assert upgraded.store.health()["schema_version"] == 32
        [hold] = upgraded.store.list_containment()["holds"]
        assert (hold["object_id"], hold["policy_version"]) == (child_id, 4)
        migrated = saved_context(upgraded, child_id)
        assert migrated["authority_chain"] == [parent_id]
        assert migrated["effect_scope"] is None and migrated["lease"] is None
        containment.place_hold(
            upgraded.store,
            kind="egress",
            object_id=child_id,
            owner=MONITOR,
            policy_version=4,
        )
    finally:
        upgraded.close()


def test_credential_filter_stops_one_sender_into_one_target(nodes):
    root, worker = nodes
    _, binding, child_id = delegate(root)
    containment.place_filter(
        worker.store,
        authority={"agent_id": "parent"},
        target={"agent_id": "child"},
        owner=MONITOR,
        policy_version=1,
    )
    other_id = root.dispatch("parent", binding, "other")["result"]["task_id"]
    assert deliver(root, worker) == 2
    # Only the parent's traffic into the child is withheld.
    assert worker.store.get_task(other_id)["state"] == "offered"
    with pytest.raises(StoreError, match="not found"):
        worker.store.get_task(child_id)
    containment.place_filter(
        root.store,
        authority={"agent_id": "parent"},
        target={"agent_id": "other"},
        owner=MONITOR,
        policy_version=1,
    )
    assert root.dispatch("parent", binding, "other")["code"] == "ingress_filtered"
    assert root.dispatch("parent", binding, "child")["status"] == "ok"
    listed = worker.store.list_containment()["ingress_filters"]
    assert listed == [
        {
            "authority": {"agent_id": "parent"},
            "target": {"agent_id": "child"},
            "owner": MONITOR,
            "policy_version": 1,
        }
    ]
