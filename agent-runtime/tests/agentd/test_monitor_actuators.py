"""The monitor's actuator vocabulary through the signed control channel.

``place_hold``, ``release_hold`` and ``revoke`` take ``{kind, object_id}`` as the
monitor's ControlClient does; ``describe`` advertises the supported pairs.
"""

from __future__ import annotations

import json
import time
from uuid import uuid4

import pytest

from containment_test_support import Node, deliver
from edgecitadel_agentd.containment_control import command_mac, handle_wire
from edgecitadel_agentd.store import StoreError

MONITOR = "core-monitor"
KEY = b"m" * 48
WRITE = {"effect_class": "fs.write", "target": "workspace/*"}


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child", "other"], effects=[WRITE])
    worker.managed("child", outbound=["other"], effects=[WRITE])
    worker.managed("other", effects=[WRITE])
    for node in (root, worker):
        keys = node.store.state_directory / "control-keys"
        keys.mkdir(mode=0o700)
        (keys / f"{MONITOR}.key").write_bytes(KEY)
        (keys / "operator.key").write_bytes(KEY)
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


class Monitor:
    """A minimal NATS-side ControlClient: one signed command per call."""

    def __init__(self) -> None:
        self.version = 0

    def send(self, node, action, target, owner=MONITOR):
        self.version += 1
        now = time.time_ns() // 1_000_000
        command = {
            "schema_version": 1,
            "command_id": str(uuid4()),
            "node_id": node.node_id,
            "owner": owner,
            "policy_version": self.version,
            "issued_at_ms": now,
            "expires_at_ms": now + 30_000,
            "action": action,
            "target": target,
        }
        command["mac"] = command_mac(KEY, command)
        return handle_wire(
            node.store, json.dumps(command).encode(), node_id=node.node_id, gate=None
        )

    def call(self, node, action, target):
        reply = self.send(node, action, target)
        assert reply["status"] == "applied", reply
        return reply

    def revoke(self, node, kind, object_id):
        return self.call(node, "revoke", {"kind": kind, "object_id": object_id})


def effect(node, agent, binding, target="workspace/a"):
    return node.rpc(
        agent,
        "effect.authorize",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        effect_class="fs.write",
        target=target,
        tool_call_id=None,
    ).get("code", "ok")


def delegate(root):
    parent_id = root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    return parent_id, binding, child_id


def test_describe_advertises_the_supported_pairs(nodes):
    root, _ = nodes
    capabilities = Monitor().call(root, "describe", {})["enforcement"]["capabilities"]
    assert {
        "hold:task",
        "hold:transport",
        "hold:effect",
        "revoke:egress",
        "revoke:grant",
        "revoke:credential",
        "revoke:admission",
    } <= set(capabilities)


def test_egress_revocation_stops_one_invocation_and_lets_it_complete(nodes):
    root, worker = nodes
    monitor = Monitor()
    parent_id, binding, _ = delegate(root)
    deliver(root, worker)
    _, child_binding = worker.start("child")
    confirmation = monitor.revoke(root, "egress", parent_id)
    assert confirmation["enforcement"] == {
        "gates": ["dispatch", "effect"],
        "local_tasks": 1,
    }
    assert root.dispatch("parent", binding, "other")["code"] == "authority_revoked"
    assert effect(root, "parent", binding) == "authority_revoked"
    root.complete("parent", parent_id, {"body": "finished anyway"})
    assert root.store.get_task(parent_id)["state"] == "completed"
    # Only that invocation: its already delegated child keeps its authority.
    monitor.revoke(worker, "egress", parent_id)
    assert worker.dispatch("child", child_binding, "other")["status"] == "ok"


def test_task_less_native_root_is_held_and_revoked_by_its_binding(nodes):
    root, _ = nodes
    monitor = Monitor()
    token = root.store.register_connector(
        connector_id="pi-local",
        host_type="pi",
        agent_id="edge-pi",
        capabilities=["edgecitadel_trace", "edgecitadel_delegate"],
    )
    root.store.grant_connector(connector_id="pi-local", outbound_agents=["child"])
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

    def dispatch():
        return root.store.dispatch_trace(
            node_id="root-node",
            connector_id="pi-local",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": binding["binding_id"],
                "recipient_id": "child",
                "request": "inspect",
                "skill_id": None,
                "deadline_at_ms": None,
            },
        )

    assert dispatch()["status"] == "ok"
    held = monitor.call(
        root, "hold", {"kind": "egress", "object_id": binding["binding_id"]}
    )
    # Its queued request is withheld and it cannot dispatch again.
    assert held["enforcement"]["outbound"]["held"] == 1
    assert held["enforcement"]["task"] is None
    assert root.store.pending_transport() == []
    assert dispatch()["code"] == "egress_held"
    monitor.call(
        root, "release", {"kind": "egress", "object_id": binding["binding_id"]}
    )
    assert len(root.store.pending_transport()) == 1
    monitor.revoke(root, "egress", binding["binding_id"])
    assert dispatch()["code"] == "authority_revoked"


def test_credential_revocation_silences_an_agent(nodes):
    root, worker = nodes
    monitor = Monitor()
    _, _, child_id = delegate(root)
    deliver(root, worker)
    _, child_binding = worker.start("child")
    monitor.revoke(worker, "credential", "child")
    assert worker.dispatch("child", child_binding, "other")["code"] == (
        "authority_revoked"
    )
    assert effect(worker, "child", child_binding) == "authority_revoked"
    # It may finish, but its result to the parent is withheld, as is
    # everything else it emits.
    worker.complete("child", child_id, {"body": "result"})
    assert deliver(worker, root) == 0
    confirmation = monitor.revoke(worker, "credential", "child")
    assert confirmation["enforcement"]["outbound"] == {
        "held": 1,
        "pending": 0,
        "in_flight": [],
        "published": 0,
    }
    token = worker.store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="worker-codex",
        capabilities=["edgecitadel_delegate"],
    )
    worker.store.grant_connector(connector_id="codex-local", outbound_agents=["other"])
    monitor.revoke(worker, "credential", "worker-codex")
    from edgecitadel_agentd.service import dispatch as rpc_dispatch

    with pytest.raises(StoreError, match="authority_revoked"):
        rpc_dispatch(
            worker.store,
            {
                "version": 1,
                "operation": "task.create",
                "connector_id": "codex-local",
                "token": token,
                "params": {"recipient_id": "other", "payload": {}},
            },
        )


def test_grant_and_admission_revocations_narrow_one_target(nodes):
    root, worker = nodes
    monitor = Monitor()
    _, binding, child_id = delegate(root)
    monitor.revoke(root, "grant", "parent>other")
    assert root.dispatch("parent", binding, "other")["code"] == "ingress_filtered"
    assert root.dispatch("parent", binding, "child")["status"] == "ok"
    monitor.revoke(root, "grant", "parent>fx:fs.write")
    assert effect(root, "parent", binding) == "ingress_filtered"

    # Admission: no new task for the child at its node, under any authority.
    confirmation = monitor.revoke(worker, "admission", "child")
    assert confirmation["enforcement"]["gates"] == [
        "dispatch",
        "task_create",
        "publication",
        "inbound",
    ]
    assert deliver(root, worker) == 2  # Acknowledged to the broker, then parked.
    with pytest.raises(StoreError, match="not found"):
        worker.store.get_task(child_id)
    listed = worker.store.list_containment()["ingress_filters"]
    assert listed[0]["authority"] == {"any": True}


def test_a_scheduled_effect_is_held_by_its_effect_id(nodes):
    """The gateway keeps a scheduled effect; held when due, it is cancelled."""
    from containment_test_support import Ledger
    from edgecitadel_agentd.effect_adapters import LedgerAdapter

    root, _ = nodes
    monitor = Monitor()
    ledger = Ledger()
    root.store.effects.adapters = {"http": LedgerAdapter(ledger.url)}
    try:
        root.submit("parent")
        _, binding = root.start("parent")
        effect_id = str(uuid4())
        due = time.time_ns() // 1_000_000 + 60_000
        scheduled = root.rpc(
            "parent",
            "effect.execute",
            schema_version=1,
            request_id=effect_id,
            binding_id=binding["binding_id"],
            adapter="http",
            effect_class="fs.write",
            target="workspace/report",
            data=None,
            tool_call_id=None,
            scheduled_at_ms=due,
        )
        assert scheduled["result"]["state"] == "scheduled"
        target = {"kind": "effect", "object_id": effect_id}
        confirmation = monitor.call(root, "hold", target)
        assert confirmation["enforcement"] == {
            "gates": ["effect"],
            "effect_state": "scheduled",
        }
        root.store.effects.run_due(root.node_id, now_ms=due)
        released = monitor.call(root, "release", target)
        assert released["enforcement"]["effect_state"] == "denied"
        assert ledger.requests == []
        phases = [e["phase"] for e in root.events("effect")]
        assert phases == ["requested", "denied"]
    finally:
        ledger.close()


def test_rejections_use_the_monitor_error_codes(nodes):
    root, _ = nodes
    monitor = Monitor()
    parent_id = root.submit("parent")
    target = {"kind": "task", "object_id": parent_id}
    assert monitor.send(root, "release", target)["code"] == "hold_not_found"
    monitor.call(root, "hold", target)
    other = monitor.send(root, "release", target, owner="operator")
    assert (other["status"], other["code"]) == ("rejected", "hold_owned_by_another")
    monitor.call(root, "release", target)
    monitor.version = 1  # Older than the versions this node already accepted.
    assert monitor.send(root, "hold", target)["code"] == "stale_policy_version"
