"""The effect gateway at the private RPC boundary, with real delegated authority.

Each effect is asked for by a bound invocation through ``service.dispatch``,
exactly as a harness does, and is decided from the node's stored state.
``effect.authorize`` decides an effect the harness performs; ``effect.execute``
has the gateway perform a registered effect through an adapter, fenced by the
control lock at its commit.
"""

import json
import threading
import time
from uuid import uuid4

import pytest

from containment_test_support import Ledger, Node, deliver, ref
from edgecitadel_agentd import containment
from edgecitadel_agentd.effect_adapters import (
    HomeAssistantAdapter,
    LedgerAdapter,
    load_adapters,
)

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
    assert committed["attributes"]["committed_at_ms"] is not None
    assert committed["attributes"] | {
        "policy_version": None,
        "committed_at_ms": None,
    } == {
        "effect_version": 2,
        "effect_id": request_id,
        "effect_class": "fs.write",
        "target": "workspace/a.txt",
        "tool_call_id": "call-1",
        "scheduled_at_ms": None,
        "adapter": None,  # performed by the harness, not the gateway
        "committed_at_ms": None,
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


# ---------------------------------------------------------------------------
# effect.execute: the gateway performs registered effects
# ---------------------------------------------------------------------------
ACTUATOR = {"effect_class": "device.command", "target": "light.*"}


@pytest.fixture
def ledger():
    endpoint = Ledger()
    try:
        yield endpoint
    finally:
        endpoint.close()


def execute(
    node,
    agent,
    binding,
    *,
    adapter="http",
    target="light.turn_on/light.hall",
    effect_class="device.command",
    data=None,
    request_id=None,
    scheduled=None,
):
    return node.rpc(
        agent,
        "effect.execute",
        schema_version=1,
        request_id=request_id or str(uuid4()),
        binding_id=binding["binding_id"],
        adapter=adapter,
        effect_class=effect_class,
        target=target,
        data=data,
        tool_call_id=None,
        scheduled_at_ms=scheduled,
    )


def actuating_worker(nodes, ledger, **adapters):
    _, worker = nodes
    _, task_id, binding = delegated_worker(
        nodes,
        parent_effects=[ACTUATOR],
        worker_effects=[ACTUATOR],
        lease={"max_effects": 5},
    )
    worker.store.effects.adapters = {"http": LedgerAdapter(f"{ledger.url}/ledger")}
    worker.store.effects.adapters.update(adapters)
    return worker, task_id, binding


def test_execute_commits_then_performs_through_the_adapter(nodes, ledger):
    worker, task_id, binding = actuating_worker(nodes, ledger)
    request_id = str(uuid4())
    reply = execute(worker, "worker", binding, data={"level": 3}, request_id=request_id)
    assert reply["result"]["state"] == "committed"
    assert reply["result"]["adapter"] == "http"
    assert reply["result"]["outcome"] == {
        "performed": True,
        "code": None,
        "http_status": 200,
    }
    requested, committed = worker.events("effect")
    assert committed["attributes"]["adapter"] == "http"
    (posted,) = ledger.requests
    assert posted["path"] == "/ledger"
    assert posted["body"] == {
        "schema_version": 1,
        "effect_id": request_id,
        "node_id": "worker-node",
        "agent_id": "worker",
        "task_id": task_id,
        "execution_attempt_id": binding["execution_attempt_id"],
        "effect_class": "device.command",
        "target": "light.turn_on/light.hall",
        "scheduled_at_ms": None,
        "committed_at_ms": committed["attributes"]["committed_at_ms"],
        "data": {"level": 3},
    }
    # A retry is answered from its receipt: charged and performed once.
    again = execute(worker, "worker", binding, data={"level": 3}, request_id=request_id)
    assert again["result"]["event_id"] == reply["result"]["event_id"]
    assert len(ledger.requests) == 1 and len(worker.events("effect")) == 2


def test_denied_and_unconfigured_effects_are_never_performed(nodes, ledger):
    worker, task_id, binding = actuating_worker(nodes, ledger)
    assert (
        execute(worker, "worker", binding, target="door.unlock/door.front")["code"]
        == "outside_effect_scope"
    )
    containment.place_hold(
        worker.store, kind="egress", object_id=task_id, owner=MONITOR, policy_version=1
    )
    assert execute(worker, "worker", binding)["code"] == "egress_held"
    worker.store.effects.adapters = {}
    assert execute(worker, "worker", binding)["code"] == "effect_adapter_unavailable"
    assert ledger.requests == []
    phases = [e["phase"] for e in worker.events("effect")]
    assert phases == ["requested", "denied", "requested", "denied"]


def test_home_assistant_token_stays_in_the_gateway(nodes, ledger, tmp_path):
    token = "ha-" + "s" * 40
    token_file = tmp_path / "ha.token"
    token_file.write_text(token + "\n")
    adapter = HomeAssistantAdapter.from_settings(
        {"base_url": ledger.url, "token_file": str(token_file)}
    )
    assert token not in repr(adapter)
    worker, _, binding = actuating_worker(nodes, ledger, homeassistant=adapter)
    reply = execute(
        worker, "worker", binding, adapter="homeassistant", data={"brightness": 80}
    )
    assert reply["result"]["outcome"]["performed"]
    (call,) = ledger.requests
    assert call["path"] == "/api/services/light/turn_on"
    assert call["authorization"] == f"Bearer {token}"
    assert call["body"] == {"entity_id": "light.hall", "brightness": 80}
    # The authorized target is what is called: no other entity can be smuggled.
    smuggled = execute(
        worker,
        "worker",
        binding,
        adapter="homeassistant",
        data={"entity_id": "lock.front"},
    )
    assert smuggled["code"] == "invalid_metadata"
    assert (
        execute(worker, "worker", binding, adapter="homeassistant", target="x")["code"]
        == "invalid_metadata"
    )
    journal = worker.store._connection.execute(
        "SELECT group_concat(event_json) FROM trace_journal_all"
    ).fetchone()[0]
    assert token not in journal and token not in str(reply)


def test_adapter_configuration_rejects_unsafe_settings(tmp_path):
    token = tmp_path / "token"
    token.write_text("t" * 20)
    link = tmp_path / "link"
    link.symlink_to(token)
    config = tmp_path / "adapters.json"
    assert load_adapters(None) == {}
    for document in (
        {"homeassistant": {"token_file": str(link)}},
        {"homeassistant": {"token_file": str(token), "base_url": "ftp://x"}},
        {"http": {"ledger_url": "http://user:secret@127.0.0.1/x"}},
        {"http": {"ledger_url": "http://127.0.0.1/x", "timeout_ms": 0}},
        {"smtp": {}},
    ):
        config.write_text(json.dumps(document))
        with pytest.raises(ValueError):
            load_adapters(str(config))
    config.write_text(
        json.dumps(
            {
                "homeassistant": {"token_file": str(token)},
                "http": {"ledger_url": "http://127.0.0.1:9/ledger"},
            }
        )
    )
    adapters = load_adapters(str(config))
    assert adapters["homeassistant"].base_url == "http://127.0.0.1:8123"
    assert set(adapters) == {"homeassistant", "http"}


def test_a_command_confirmed_before_the_commit_wins(nodes, ledger):
    """The commit step takes the control lock: a hold applied while it waits is
    seen by its re-check, and the effect is denied, never performed."""
    worker, task_id, binding = actuating_worker(nodes, ledger)
    replies = []
    with worker.store.control_lock:  # a control command is being applied
        caller = threading.Thread(
            target=lambda: replies.append(execute(worker, "worker", binding))
        )
        caller.start()
        time.sleep(0.2)
        assert not replies  # the commit waits for the command
        containment.place_hold(
            worker.store,
            kind="task",
            object_id=task_id,
            owner=MONITOR,
            policy_version=1,
        )
    caller.join(5)
    assert replies[0]["code"] == "execution_held"
    assert ledger.requests == []


def test_a_confirmation_lists_effects_still_being_performed(nodes, ledger):
    from edgecitadel_agentd.containment_control import execute as control, local_command

    worker, task_id, binding = actuating_worker(nodes, ledger)
    ledger.hold.clear()  # the adapter call stays in progress
    request_id = str(uuid4())
    caller = threading.Thread(
        target=lambda: execute(worker, "worker", binding, request_id=request_id)
    )
    caller.start()
    assert ledger.entered.wait(5)
    confirmation = control(
        worker.store,
        local_command(
            {
                "action": "hold",
                "target": {"kind": "task", "object_id": task_id},
                "owner": MONITOR,
                "policy_version": 1,
            },
            node_id="worker-node",
        ),
        node_id="worker-node",
        gate=None,
    )
    assert confirmation["enforcement"]["effects_in_flight"] == [request_id]
    ledger.hold.set()
    caller.join(5)
    assert worker.events("effect", "committed")[0]["attributes"]["effect_id"] == (
        request_id
    )


def test_a_scheduled_effect_runs_at_its_time_unless_held(nodes, ledger):
    worker, _, binding = actuating_worker(nodes, ledger)
    due = time.time_ns() // 1_000_000 + 60_000
    on_time, held, released = (
        execute(worker, "worker", binding, scheduled=due, data={"n": i})
        for i in range(3)
    )
    assert {r["result"]["state"] for r in (on_time, held, released)} == {"scheduled"}
    assert [e["phase"] for e in worker.events("effect")] == ["requested"] * 3
    assert on_time["result"]["outcome"] is None and ledger.requests == []
    for reply in (held, released):
        containment.place_hold(
            worker.store,
            kind="effect",
            object_id=reply["request_id"],
            owner=MONITOR,
            policy_version=1,
        )
    containment.release_hold(
        worker.store,
        kind="effect",
        object_id=released["request_id"],
        owner=MONITOR,
        policy_version=1,
    )
    assert worker.store.effects.run_due("worker-node", now_ms=due - 1) == 0
    assert worker.store.effects.run_due("worker-node", now_ms=due) == 3
    assert sorted(r["body"]["data"]["n"] for r in ledger.requests) == [0, 2]
    decided = {
        e["attributes"]["effect_id"]: e
        for e in worker.events("effect")
        if e["phase"] != "requested"
    }
    assert decided[held["request_id"]]["phase"] == "denied"
    assert decided[held["request_id"]]["attributes"]["reason"] == "effect_held"
    assert decided[on_time["request_id"]]["attributes"]["scheduled_at_ms"] == due
    # Cancelled: releasing the hold later performs nothing.
    containment.release_hold(
        worker.store,
        kind="effect",
        object_id=held["request_id"],
        owner=MONITOR,
        policy_version=1,
    )
    assert worker.store.effects.run_due("worker-node", now_ms=due + 10_000) == 0
    assert len(ledger.requests) == 2
    states = dict(
        worker.store._connection.execute(
            "SELECT effect_id,state FROM scheduled_effects"
        ).fetchall()
    )
    assert states[held["request_id"]] == "denied"


def test_a_scheduled_effect_is_checked_again_when_due(nodes, ledger):
    worker, task_id, binding = actuating_worker(nodes, ledger)
    due = time.time_ns() // 1_000_000 + 60_000
    execute(worker, "worker", binding, scheduled=due)
    containment.revoke(
        worker.store, kind="egress", object_id=task_id, owner=MONITOR, policy_version=1
    )
    worker.store.effects.run_due("worker-node", now_ms=due)
    assert [e["phase"] for e in worker.events("effect")] == ["requested", "denied"]
    assert worker.events("effect", "denied")[0]["attributes"]["reason"] == (
        "authority_revoked"
    )
    assert ledger.requests == []


def test_the_scheduler_thread_performs_due_effects(nodes, ledger):
    worker, _, binding = actuating_worker(nodes, ledger)
    worker.store.effects.start("worker-node")
    try:
        execute(
            worker,
            "worker",
            binding,
            scheduled=time.time_ns() // 1_000_000 + 300,
        )
        deadline = time.monotonic() + 5
        while not ledger.requests:
            assert time.monotonic() < deadline
            time.sleep(0.05)
    finally:
        worker.store.effects.stop()
    outcome = worker.store._connection.execute(
        "SELECT outcome_json FROM scheduled_effects"
    ).fetchone()[0]
    assert json.loads(outcome)["performed"] is True


def test_authorize_no_longer_schedules(nodes):
    _, worker = nodes
    _, _, binding = delegated_worker(
        nodes, parent_effects=[WRITE], worker_effects=[WRITE]
    )
    reply = worker.rpc(
        "worker",
        "effect.authorize",
        schema_version=1,
        request_id=str(uuid4()),
        binding_id=binding["binding_id"],
        effect_class="fs.write",
        target="workspace/later",
        tool_call_id=None,
        scheduled_at_ms=time.time_ns() // 1_000_000 + 60_000,
    )
    assert reply["code"] == "invalid_metadata"


def test_schema_33_retires_effects_a_harness_scheduled(tmp_path):
    node = Node(tmp_path / "node", "node-a")
    db = node.store._connection
    with db:
        for column in (
            "connector_id",
            "adapter",
            "data_text",
            "due_at_ms",
            "outcome_json",
        ):
            db.execute(f"ALTER TABLE task_state.scheduled_effects DROP COLUMN {column}")
        db.execute(
            "INSERT INTO task_state.scheduled_effects VALUES "
            "(?,?,'fs.write','workspace/x','{}','{}','scheduled')",
            (str(uuid4()), str(uuid4())),
        )
        db.execute("PRAGMA main.user_version=32")
        db.execute("PRAGMA task_state.user_version=32")
    node.close()
    upgraded = Node(tmp_path / "node", "node-a")
    try:
        assert upgraded.store.health()["schema_version"] == 33
        row = upgraded.store._connection.execute(
            "SELECT state,adapter,due_at_ms FROM scheduled_effects"
        ).fetchone()
        # Its harness can no longer commit it (effect.commit is gone).
        assert tuple(row) == ("denied", None, None)
    finally:
        upgraded.close()
