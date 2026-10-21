"""The containment control protocol at the NATS transport and admin RPC boundaries.

Commands are authenticated with an owner key file, executed once, and answered
with a confirmation of what the node enforced, including whether an outbound
message of the object was already in flight to the broker.
"""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

from containment_test_support import Node, deliver
from edgecitadel_agentd.containment_control import command_mac, handle_wire
from edgecitadel_agentd.service import dispatch as rpc_dispatch
from edgecitadel_agentd.store import StoreError
from edgecitadel_agentd.transport import AgentdNatsTransport

MONITOR = "core-monitor"
KEY = b"k" * 48


@pytest.fixture
def nodes(tmp_path):
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"])
    worker.managed("child")
    for node in (root, worker):
        keys = node.store.state_directory / "control-keys"
        keys.mkdir(mode=0o700)
        (keys / f"{MONITOR}.key").write_bytes(KEY + b"\n")
    try:
        yield root, worker
    finally:
        root.close()
        worker.close()


def command(node, action, target, *, version=1, key=KEY, **changes):
    now = time.time_ns() // 1_000_000
    document = {
        "schema_version": 1,
        "command_id": str(uuid4()),
        "node_id": node.node_id,
        "owner": MONITOR,
        "policy_version": version,
        "issued_at_ms": now,
        "expires_at_ms": now + 30_000,
        "action": action,
        "target": target,
        **changes,
    }
    return {**document, "mac": command_mac(key, document)}


def send(node, document, gate=None):
    return handle_wire(
        node.store, json.dumps(document).encode(), node_id=node.node_id, gate=gate
    )


def delegate(root):
    parent_id = root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    return parent_id, binding, child_id


def test_signed_hold_is_applied_once_and_confirms_its_gates(nodes):
    root, _ = nodes
    parent_id, _, child_id = delegate(root)
    hold = command(root, "hold", {"kind": "egress", "object_id": parent_id})
    confirmation = send(root, hold)
    assert confirmation["status"] == "applied" and confirmation["code"] is None
    assert confirmation["applied_at_ms"] >= confirmation["received_at_ms"]
    assert confirmation["enforcement"] == {
        "gates": ["result", "dispatch", "effect", "publication"],
        "task": {"state": "running", "claimed": True},
        # The parent's request to its child was queued, not yet published.
        "outbound": {"held": 1, "pending": 0, "in_flight": [], "published": 0},
    }
    # A retried or replayed command returns the original confirmation.
    assert send(root, hold) == confirmation
    altered = {**hold, "target": {"kind": "task", "object_id": parent_id}}
    altered["mac"] = command_mac(KEY, {k: v for k, v in altered.items() if k != "mac"})
    assert send(root, altered)["code"] == "idempotency_conflict"

    release = send(
        root, command(root, "release", {"kind": "egress", "object_id": parent_id})
    )
    assert release["enforcement"]["outbound"] == {
        "held": 0,
        "pending": 1,
        "in_flight": [],
        "published": 0,
    }
    assert root.store.get_task(child_id)["state"] == "queued"


def test_unauthenticated_stale_misaddressed_and_expired_commands_are_rejected(nodes):
    root, _ = nodes
    task_id = str(uuid4())
    target = {"kind": "task", "object_id": task_id}
    forged = command(root, "hold", target, key=b"x" * 48)
    assert send(root, forged)["code"] == "not_authenticated"
    unknown = command(root, "hold", target)
    unknown["owner"] = "other-monitor"
    assert send(root, unknown)["code"] == "not_authenticated"
    assert (
        send(root, command(root, "hold", target, node_id="worker-node"))["code"]
        == "wrong_node"
    )
    expired = command(root, "hold", target, issued_at_ms=1, expires_at_ms=2)
    assert send(root, expired)["code"] == "command_expired"
    assert send(root, {"schema_version": 1})["code"] == "invalid_command"
    floating = {**command(root, "hold", target), "policy_version": 1.0}
    assert send(root, floating)["code"] == "invalid_command"
    assert send(root, command(root, "hold", target, version=5))["status"] == "applied"
    stale = send(root, command(root, "release", target, version=4))
    assert stale["status"] == "rejected" and stale["code"] == "stale_policy_version"
    assert [h["object_id"] for h in root.store.list_containment()["holds"]] == [task_id]


def test_revoke_filter_cancel_and_renew_through_the_control_path(nodes):
    root, worker = nodes
    parent_id, _, child_id = delegate(root)
    filtered = send(
        worker,
        command(
            worker,
            "filter",
            {"authority": {"task_id": parent_id}, "into": {"agent_id": "child"}},
        ),
    )
    assert filtered["enforcement"]["gates"] == [
        "dispatch",
        "task_create",
        "publication",
        "inbound",
    ]
    deliver(root, worker)
    unfiltered = send(
        worker,
        command(
            worker,
            "unfilter",
            {"authority": {"task_id": parent_id}, "into": {"agent_id": "child"}},
            version=2,
        ),
    )
    assert unfiltered["enforcement"]["applied_deliveries"] == 1
    assert unfiltered["enforcement"]["parked_deliveries"] == 0
    revoked = send(
        worker,
        command(
            worker, "revoke", {"kind": "authority", "object_id": parent_id}, version=3
        ),
    )
    assert revoked["enforcement"] == {
        "gates": ["dispatch", "effect"],
        "local_tasks": 1,
    }
    cancelled = send(root, command(root, "cancel", {"task_id": child_id}, version=2))
    assert cancelled["enforcement"]["task"] == {"state": "cancelled", "claimed": False}
    assert cancelled["enforcement"]["released_holds"] == []
    root.store.controller_lease_ms = 10_000
    renewed = send(root, command(root, "renew", {}, version=2))
    assert renewed["enforcement"]["controller_lease_enforced"] is True
    assert root.store.controller_lease_valid(time.time_ns() // 1_000_000)


def test_admin_rpc_issues_the_same_command_locally(nodes):
    root, _ = nodes
    task_id = str(uuid4())
    request = {
        "version": 1,
        "operation": "containment.control",
        "params": {
            "action": "hold",
            "target": {"kind": "task", "object_id": task_id},
            "owner": MONITOR,
            "policy_version": 1,
        },
    }
    with pytest.raises(StoreError, match="management authentication"):
        rpc_dispatch(root.store, request, admin_token="a" * 48)
    confirmation = rpc_dispatch(
        root.store, {**request, "admin_token": "a" * 48}, admin_token="a" * 48
    )
    assert confirmation["status"] == "applied"
    assert confirmation["node_id"] == "root-node"
    assert confirmation["enforcement"]["task"] is None  # held before it arrives
    listed = rpc_dispatch(
        root.store,
        {"version": 1, "operation": "containment.list", "admin_token": "a" * 48},
        admin_token="a" * 48,
    )
    assert [hold["object_id"] for hold in listed["holds"]] == [task_id]


@pytest.mark.asyncio
async def test_confirmation_reports_a_message_already_in_flight(nodes):
    root, _ = nodes
    parent_id, binding, first_child = delegate(root)
    root.dispatch("parent", binding, "child")
    first_message, second_message = (
        str(m["message_id"]) for m in root.store.pending_transport()
    )
    transport = AgentdNatsTransport(root.store.state_directory.parent, root.store)
    entered, proceed = asyncio.Event(), asyncio.Event()
    published = []

    class JetStream:
        async def publish(self, subject, payload, *, headers):
            published.append(json.loads(payload)["task_id"])
            entered.set()
            await proceed.wait()  # The broker has the message; no ack yet.

    class Connection:
        async def publish(self, subject, payload):
            transport._stop.set()

    loop = asyncio.create_task(transport._connected_loop(Connection(), JetStream()))
    await entered.wait()
    # The controller blocks the parent's egress while its first request is
    # with the broker: that one cannot be recalled, the second is held.
    confirmation = await asyncio.to_thread(
        send,
        root,
        command(root, "hold", {"kind": "egress", "object_id": parent_id}),
        transport,
    )
    assert confirmation["enforcement"]["outbound"] == {
        "held": 1,
        "pending": 0,
        "in_flight": [first_message],
        "published": 0,
    }
    proceed.set()
    await loop
    assert published == [first_child]
    assert [str(m["message_id"]) for m in root.store.pending_transport()] == []
    unpublished = root.store._connection.execute(
        "SELECT message_id FROM transport_outbox WHERE published_at_ms IS NULL"
    ).fetchall()
    assert [row[0] for row in unpublished] == [second_message]


@pytest.mark.asyncio
async def test_transport_answers_control_requests_on_its_subject(nodes):
    root, _ = nodes
    transport = AgentdNatsTransport(root.store.state_directory.parent, root.store)
    replies = []

    async def respond(data):
        replies.append(json.loads(data))

    task_id = str(uuid4())
    message = SimpleNamespace(
        subject="edgecitadel.control.v1.root-node",
        data=json.dumps(
            command(root, "hold", {"kind": "task", "object_id": task_id})
        ).encode(),
        reply="_INBOX.reply",
        respond=respond,
    )
    await transport._handle_control(message, "root-node")
    [reply] = replies
    assert reply["status"] == "applied" and reply["node_id"] == "root-node"


def test_receipts_expire_with_their_commands(nodes):
    root, _ = nodes
    now = time.time_ns() // 1_000_000
    hold = command(
        root,
        "hold",
        {"kind": "task", "object_id": str(uuid4())},
        issued_at_ms=now,
        expires_at_ms=now + 300,
    )
    assert send(root, hold)["status"] == "applied"
    time.sleep(0.35)
    root.store.reconcile()
    count = root.store._connection.execute(
        "SELECT COUNT(*) FROM control_commands"
    ).fetchone()[0]
    assert count == 0
    # Without its receipt, a replay is refused by its own expiry.
    assert send(root, hold)["code"] == "command_expired"


def test_credential_filter_through_the_control_path(nodes):
    root, worker = nodes
    _, _, child_id = delegate(root)
    target = {"authority": {"agent_id": "parent"}, "into": {"agent_id": "child"}}
    filtered = send(worker, command(worker, "filter", target))
    assert filtered["enforcement"]["parked_deliveries"] == 0
    deliver(root, worker)
    listed = send(worker, command(worker, "filter", target, version=2))
    assert listed["enforcement"]["parked_deliveries"] == 1
    removed = send(worker, command(worker, "unfilter", target, version=3))
    assert removed["enforcement"]["applied_deliveries"] == 1
    assert worker.store.get_task(child_id)["state"] == "offered"


def test_a_confirmation_carries_the_journal_position_it_fences(nodes):
    """Everything journaled before the decision took effect is at or below the
    reported position; what the decision then stops is journaled after it."""
    root, _ = nodes
    parent_id, binding, _ = delegate(root)

    def latest():
        return root.store._connection.execute(
            "SELECT source_epoch,MAX(source_seq) FROM trace_journal_all "
            "GROUP BY source_epoch"
        ).fetchone()

    epoch, before = latest()
    confirmation = send(
        root, command(root, "hold", {"kind": "egress", "object_id": parent_id})
    )
    assert confirmation["journal"] == {"source_epoch": epoch, "source_seq": before}
    denied = root.dispatch("parent", binding, "child")
    assert denied["code"] == "egress_held"
    assert latest()[1] > before  # the denial is journaled after the position
    rejected = send(
        root, command(root, "release", {"kind": "task", "object_id": str(uuid4())})
    )
    assert rejected["status"] == "rejected" and rejected["journal"] is None
