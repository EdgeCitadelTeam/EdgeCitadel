"""effect.authorize at the private RPC boundary, with real delegated authority.

Each effect is asked for by a bound invocation through ``service.dispatch``,
exactly as a harness does, and is decided from the node's stored state.
"""

import time
from uuid import uuid4

import pytest

from containment_test_support import Node, deliver, ref
from edgecitadel_agentd import containment

MONITOR = "core-monitor"
WRITE = {"effect_class": "fs.write", "target": "workspace/*"}
EXEC = {"effect_class": "process.exec", "target": "bash"}


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def authorize(node, agent, binding, effect_class, target, request_id=None):
    return node.rpc(
        agent,
        "effect.authorize",
        schema_version=1,
        request_id=request_id or str(uuid4()),
        binding_id=binding["binding_id"],
        effect_class=effect_class,
        target=target,
        tool_call_id="call-1",
    )


def delegated_worker(nodes, *, parent_effects, worker_effects, lease=None):
    """parent on the root delegates to a running worker; returns its binding."""
    root, worker = nodes
    root.managed("parent", outbound=["worker"], effects=parent_effects)
    worker.managed("worker", effects=worker_effects)
    parent_id = root.submit("parent")
    _, binding = root.start("parent")
    extra = {"lease": lease} if lease else {}
    assert root.dispatch("parent", binding, "worker", **extra)["status"] == "ok"
    deliver(root, worker)
    task, worker_binding = worker.start("worker")
    return parent_id, task["task_id"], worker_binding


def test_committed_effect_is_recorded_as_provenance_and_retried_idempotently(nodes):
    _, worker = nodes
    _, task_id, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE], lease={"max_effects": 3}
    )
    request_id = str(uuid4())
    reply = authorize(
        worker, "worker", binding, "fs.write", "workspace/a.txt", request_id
    )
    assert reply["status"] == "ok"
    assert reply["result"]["effects_remaining"] == 2
    requested, committed = worker.events("effect")
    assert (requested["phase"], committed["phase"]) == ("requested", "committed")
    assert committed["causes"] == [ref(requested)]
    assert committed["event_id"] == reply["result"]["event_id"]
    assert reply["result"]["state"] == "committed"
    assert committed["task_id"] == task_id
    assert committed["execution_attempt_id"] == binding["execution_attempt_id"]
    assert committed["attributes"] | {"policy_version": None} == {
        "effect_version": 1,
        "effect_id": request_id,
        "effect_class": "fs.write",
        "target": "workspace/a.txt",
        "tool_call_id": "call-1",
        "scheduled_at_ms": None,
        "policy": "managed_package_grant",
        "policy_version": None,
        "reason": None,
        "effects_remaining": 2,
    }
    # A retried request is answered from its receipt and charged once.
    again = authorize(
        worker, "worker", binding, "fs.write", "workspace/a.txt", request_id
    )
    assert again == reply
    assert len(worker.events("effect")) == 2
    assert (
        authorize(worker, "worker", binding, "fs.write", "workspace/b.txt")["result"][
            "effects_remaining"
        ]
        == 1
    )


def test_effect_scope_is_the_local_grant_narrowed_by_the_delegator(nodes):
    _, worker = nodes
    # The worker's package grants bash, but its delegator could not pass it on.
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE, EXEC]
    )
    denied = authorize(worker, "worker", binding, "process.exec", "bash")
    assert denied["code"] == "outside_effect_scope"
    assert worker.events("effect", "denied")[0]["attributes"]["reason"] == (
        "outside_effect_scope"
    )
    assert authorize(worker, "worker", binding, "fs.write", "other/x")["code"] == (
        "outside_effect_scope"
    )
    assert (
        authorize(worker, "worker", binding, "fs.write", "workspace/x")["status"]
        == "ok"
    )


def test_holds_revocation_and_filters_deny_effects_with_their_policy(nodes):
    _, worker = nodes
    parent_id, task_id, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE]
    )

    def decide(target="workspace/a.txt"):
        reply = authorize(worker, "worker", binding, "fs.write", target)
        return reply.get("code", "ok")

    containment.place_hold(
        worker.store, kind="egress", object_id=task_id, owner=MONITOR, policy_version=1
    )
    assert decide() == "egress_held"
    containment.release_hold(
        worker.store, kind="egress", object_id=task_id, owner=MONITOR, policy_version=1
    )
    containment.place_filter(
        worker.store,
        authority={"task_id": parent_id},
        target={"effect_class": "fs.write", "target": "workspace/secret*"},
        owner=MONITOR,
        policy_version=2,
    )
    # Only effects under that authority into that target are filtered.
    assert decide("workspace/secret.txt") == "ingress_filtered"
    assert decide("workspace/notes.txt") == "ok"
    containment.revoke_authority(
        worker.store, task_id=parent_id, owner=MONITOR, policy_version=3
    )
    assert decide() == "authority_revoked"
    denied = worker.events("effect", "denied")
    assert [
        (e["attributes"]["policy"], e["attributes"]["policy_version"]) for e in denied
    ] == [
        ("containment_egress_hold", "1"),
        ("ingress_filter", "2"),
        ("authority_revocation", "3"),
    ]
    containment.place_hold(
        worker.store, kind="task", object_id=task_id, owner=MONITOR, policy_version=4
    )
    assert decide() == "execution_held"


def test_lease_budget_and_expiry_deny_with_an_alert(nodes):
    _, worker = nodes
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE], lease={"max_effects": 1}
    )
    assert (
        authorize(worker, "worker", binding, "fs.write", "workspace/a")["status"]
        == "ok"
    )
    exceeded = authorize(worker, "worker", binding, "fs.write", "workspace/b")
    assert exceeded["code"] == "lease_exceeded"
    [alert] = worker.events("containment", "alert")
    assert alert["attributes"]["alert"] == "lease_exceeded"
    assert alert["attributes"]["lease_dimension"] == "effects"
    assert (
        alert["attributes"]["request_id"]
        == worker.events("effect", "denied")[0]["attributes"]["effect_id"]
    )


def test_expired_wall_time_denies_effects(nodes):
    _, worker = nodes
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE], lease={"wall_time_ms": 1}
    )
    denied = authorize(worker, "worker", binding, "fs.write", "workspace/a")
    assert denied["code"] == "lease_exceeded"
    assert worker.events("containment", "alert")[0]["attributes"][
        "lease_dimension"
    ] == ("wall_time")


def test_controller_lease_fails_closed_only_when_configured(nodes):
    _, worker = nodes
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE]
    )
    assert (
        authorize(worker, "worker", binding, "fs.write", "workspace/a")["status"]
        == "ok"
    )
    worker.store.controller_lease_ms = 60_000
    denied = authorize(worker, "worker", binding, "fs.write", "workspace/a")
    assert denied["code"] == "controller_lease_expired"
    assert worker.events("containment", "alert")[0]["attributes"]["alert"] == (
        "controller_lease_expired"
    )
    now = time.time_ns() // 1_000_000
    # A renewal never outlives the renewing command's own expiry.
    assert worker.store.renew_controller_lease(now, now + 5_000) == now + 5_000
    assert (
        authorize(worker, "worker", binding, "fs.write", "workspace/a")["status"]
        == "ok"
    )


def test_native_root_effects_follow_its_connector_grant(nodes):
    root, _ = nodes
    token = root.store.register_connector(
        connector_id="pi-local",
        host_type="pi",
        agent_id="edge-pi",
        capabilities=["edgecitadel_trace"],
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

    def decide():
        return root.store.authorize_effect(
            node_id="root-node",
            connector_id="pi-local",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": binding["binding_id"],
                "effect_class": "process.exec",
                "target": "bash",
                "tool_call_id": None,
            },
        )

    assert decide()["code"] == "outside_effect_scope"
    root.store.grant_connector(
        connector_id="pi-local", outbound_agents=[], effects=[EXEC]
    )
    assert decide()["status"] == "ok"


def test_a_scheduled_effect_can_be_held_until_it_commits(nodes):
    _, worker = nodes
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE], lease={"max_effects": 2}
    )
    effect_id = str(uuid4())
    scheduled = worker.rpc(
        "worker",
        "effect.authorize",
        schema_version=1,
        request_id=effect_id,
        binding_id=binding["binding_id"],
        effect_class="fs.write",
        target="workspace/later",
        tool_call_id=None,
        scheduled_at_ms=time.time_ns() // 1_000_000 + 60_000,
    )
    assert scheduled["result"]["state"] == "scheduled"
    # Checked and requested, but not committed or charged yet.
    assert [e["phase"] for e in worker.events("effect")] == ["requested"]
    assert scheduled["result"]["effects_remaining"] == 2

    def commit(request_id):
        return worker.rpc(
            "worker",
            "effect.commit",
            schema_version=1,
            request_id=request_id,
            binding_id=binding["binding_id"],
            effect_id=effect_id,
        )

    request_id = str(uuid4())

    containment.place_hold(
        worker.store,
        kind="effect",
        object_id=effect_id,
        owner=MONITOR,
        policy_version=1,
    )
    held = commit(request_id)
    assert held["code"] == "effect_held"
    assert [e["phase"] for e in worker.events("effect")] == ["requested"]
    containment.release_hold(
        worker.store,
        kind="effect",
        object_id=effect_id,
        owner=MONITOR,
        policy_version=1,
    )
    # The held attempt left no receipt, so the same request now commits.
    committed = commit(request_id)
    assert committed["result"] == {
        "event_id": committed["result"]["event_id"],
        "state": "committed",
        "effects_remaining": 1,
        "lease_expires_at_ms": None,
    }
    requested, decided = worker.events("effect")
    assert decided["phase"] == "committed"
    assert decided["causes"] == [ref(requested)]
    assert decided["attributes"]["effect_id"] == effect_id
    assert decided["attributes"]["scheduled_at_ms"] is not None
    # A retry gets the same answer; a committed effect cannot commit again.
    assert commit(request_id) == committed
    assert commit(str(uuid4()))["code"] == "invalid_metadata"


def test_a_scheduled_effect_is_checked_again_when_it_commits(nodes):
    _, worker = nodes
    parent_id, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE]
    )
    effect_id = str(uuid4())
    worker.rpc(
        "worker",
        "effect.authorize",
        schema_version=1,
        request_id=effect_id,
        binding_id=binding["binding_id"],
        effect_class="fs.write",
        target="workspace/later",
        tool_call_id=None,
        scheduled_at_ms=time.time_ns() // 1_000_000 + 60_000,
    )
    containment.revoke_authority(
        worker.store, task_id=parent_id, owner=MONITOR, policy_version=1
    )
    denied = worker.rpc(
        "worker",
        "effect.commit",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        effect_id=effect_id,
    )
    assert denied["code"] == "authority_revoked"
    assert [e["phase"] for e in worker.events("effect")] == ["requested", "denied"]
