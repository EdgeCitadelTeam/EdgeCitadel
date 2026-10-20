"""Two-phase enforcement, fencing and the control clients."""

import random
import sys
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from aggregator.containment_monitor import solver
from aggregator.containment_monitor.control import (
    AGENTD_CAPABILITIES,
    ControlError,
    FakeControlClient,
    LocalAgentdControlClient,
    execute,
    release,
)
from aggregator.containment_monitor.model import Action, Alert, inv
from containment_support import Harness
from edgecitadel_agentd.service import dispatch as rpc
from test_containment_projection import event_ms, workload

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "agent-runtime/tests/agentd")
)
from containment_test_support import Node, deliver  # noqa: E402

ADMIN = "a" * 48


def operator_alert(subject, at):
    return Alert(str(uuid4()), "operator", subject, at, "external")


def respond(tmp_path, seed, control):
    rng = random.Random(seed)
    trace, config = workload(rng)
    # The cut property concerns avoidable sinks; the best-effort guard adds
    # actions on sinks the decision judged unavoidable, which would make those
    # sinks count once the guard actions are in force.
    config = replace(config, best_effort_guard=False)
    t_alert = rng.randint(30, 160)
    started = [
        e["execution_attempt_id"]
        for e in trace.events
        if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
    ]
    root = inv(rng.choice(started))
    harness = Harness(tmp_path / f"seed-{seed}", config, control)
    harness.feed([e for e in trace.events if event_ms(e) <= t_alert])
    harness.at(t_alert)
    decision = harness.monitor.respond(
        operator_alert(root, harness.now), strategy="firebreak", roots=[root]
    )
    return harness, decision


def jittered(seed):
    rng = random.Random(seed)
    delays = {}

    def delay(node, op):
        return delays.setdefault((node, op), rng.uniform(0, 0.004))

    return delay


def test_releases_only_after_every_phase_two_confirmation(tmp_path):
    """Property: from phase-1 convergence on, the actions in force at every instant
    separate the sources from every avoidable sink of G_C (theory 7.3)."""
    releases = checked = 0
    for seed in range(30):
        control = FakeControlClient(delay=jittered(seed))
        harness, decision = respond(tmp_path, seed, control)
        placed = {
            r.action for r in decision.phase1 + decision.phase2 if r.state != "failed"
        }
        cg = harness.monitor._project(2, frozenset(placed))
        instance = cg.instance()
        sinks = instance.sinks - solver.unavoidable_sinks(instance)
        assert decision.status == "contained", (seed, decision.status)
        # Ordering: no release was sent before the last phase-2 confirmation.
        last_confirmation = max((r.t_confirmed for r in decision.phase2), default=0)
        assert all(r.t_release_sent >= last_confirmation for r in decision.released)
        releases += len(decision.released)
        # Replay what the nodes applied, in order.
        phase1 = {r.action for r in decision.phase1 if r.state == "in_force"}
        in_force: set[Action] = set()
        converged = not phase1
        for _, op, (node, kind, object_id, _) in sorted(control.timeline):
            action = Action(
                "hold" if op in {"hold", "release"} else "revoke", node, kind, object_id
            )
            if op == "release":
                in_force.discard(action)
            else:
                in_force.add(action)
            converged = converged or phase1 <= in_force
            if converged:
                assert solver.separated(instance, in_force, sinks), seed
                checked += 1
        assert solver.separated(
            instance,
            set(
                decision.solve
                and [Action.from_json(a) for a in decision.solve["actions"]]
            ),
            sinks,
        )
        harness.close()
    assert releases > 0 and checked > 0  # the property was exercised


def three_invocations(tmp_path, control):
    """Three tainted invocations of agent a share one credential hub and cannot
    emit within a second of starting. Phase 1 holds each of them; the optimum
    revokes a's credential once (a gated group), in time for their emissions."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"a": frozenset({"w"})},
        agent_nodes={"a": "n0", "w": "n1"},
        session_isolated_agents=frozenset({"a"}),
        min_emission_ms={"a": 1000},
    )
    harness = Harness(tmp_path, config, control)
    trace = TraceBuilder()
    tasks = [trace.root("a", "n0", i) for i in range(3)]
    roots = [inv(trace.start(task_id, 5)) for task_id in tasks]
    harness.feed(trace.events)
    harness.at(10)
    decision = harness.monitor.respond(
        operator_alert(roots[0], harness.now), roots=roots
    )
    return harness, decision, tasks


def test_phase_two_replaces_frontier_holds_with_one_revocation(tmp_path):
    harness, decision, tasks = three_invocations(tmp_path, FakeControlClient())
    assert sorted(r.action.label() for r in decision.phase1) == sorted(
        f"hold:task:{t}@n0" for t in tasks
    )
    assert [r.action.label() for r in decision.phase2] == ["revoke:credential:a@n0"]
    assert decision.solve["method"] == "gadget"
    assert sorted(r.state for r in decision.released) == ["released"] * 3
    assert decision.status == "contained" and decision.cut_valid
    assert max(r.t_confirmed for r in decision.phase2) <= min(
        r.t_release_sent for r in decision.released
    )
    harness.close()


def test_unconfirmed_phase_two_keeps_every_phase_one_hold(tmp_path):
    failing = FakeControlClient(
        fail=lambda node, op, object_id: "node_unreachable" if op == "revoke" else None
    )
    harness, decision, tasks = three_invocations(tmp_path, failing)
    assert decision.status == "phase2_unconfirmed"
    assert decision.released == []
    assert failing.in_force() == {("n0", "task", t) for t in tasks}
    assert [(r.state, r.error) for r in decision.phase2] == [
        ("failed", "node_unreachable")
    ]
    harness.close()


def test_a_lost_confirmation_is_neither_in_force_nor_released(tmp_path):
    """The revocation is applied at the node but its confirmation never arrives:
    the monitor must not count it, and must not release the frontier holds."""
    lossy = FakeControlClient(lose=lambda node, op, object_id: op == "revoke")
    harness, decision, tasks = three_invocations(tmp_path, lossy)
    (record,) = decision.phase2
    assert (record.state, record.error) == ("unconfirmed", "confirmation_timeout")
    assert record.t_confirmed is None and record.sites["n0"]["state"] == "unconfirmed"
    assert decision.status == "phase2_unconfirmed" and decision.released == []
    assert lossy.in_force() == {("n0", "task", t) for t in tasks} | {
        ("n0", "credential", "a")
    }
    status = harness.monitor.status()
    assert [a["action"]["kind"] for a in status["in_force"]] == ["task"] * 3
    assert [a["action"]["kind"] for a in status["unconfirmed"]] == ["credential"]
    harness.close()


def test_projection_offers_only_what_each_node_describes(tmp_path):
    """n0 cannot revoke credentials: the solver keeps the per-invocation holds."""
    limited = FakeControlClient(
        {"n0": AGENTD_CAPABILITIES - {"revoke:credential"}, "n1": AGENTD_CAPABILITIES}
    )
    harness, decision, tasks = three_invocations(tmp_path, limited)
    chosen = {r.action.capability for r in decision.phase1 + decision.phase2}
    assert "revoke:credential" not in chosen
    assert decision.status == "contained" and decision.cut_valid
    harness.close()


def test_policy_versions_are_monotone_and_survive_restart(tmp_path):
    control = FakeControlClient()
    harness, decision, _ = three_invocations(tmp_path, control)
    assert {r.policy_version for r in decision.phase1} == {1}
    assert [r.policy_version for r in decision.phase2] == [2]
    latest = harness.monitor.status()["policy_version"]
    assert latest == 3  # the releases were stamped with a fresh version
    harness.reopen()
    assert harness.monitor._next_version() == latest + 1
    # A decision stamped with an older version is refused by the node.
    with pytest.raises(ControlError, match="stale_policy_version"):
        control.send(
            "n0", "hold", {"kind": "task", "object_id": str(uuid4())}, policy_version=2
        )
    harness.close()


# ---------------------------------------------------------------------------
# agentd admin RPC
# ---------------------------------------------------------------------------
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


def transport(node):
    def call(operation, params):
        return rpc(
            node.store,
            {
                "version": 1,
                "operation": operation,
                "params": params,
                "admin_token": ADMIN,
            },
            admin_token=ADMIN,
        )

    return call


def test_local_agentd_client_speaks_the_control_protocol(nodes):
    """Capabilities from describe, confirmations with enforcement detail, and
    typed rejections, through agentd's real containment.control admin RPC."""
    root, _ = nodes
    client = LocalAgentdControlClient("core-monitor", {"root-node": transport(root)})
    assert client.capabilities("root-node") == frozenset()  # not described yet
    client.refresh(["root-node", "elsewhere"])
    assert client.capabilities("root-node") == AGENTD_CAPABILITIES
    assert client.capabilities("elsewhere") == frozenset()
    task_id = str(uuid4())
    hold = Action("hold", "root-node", "task", task_id)
    confirmation = execute(client, hold, node_id="root-node", policy_version=2)
    assert confirmation.applied_at_ms is not None
    assert "dispatch" in confirmation.enforcement["gates"]
    assert confirmation.enforcement["task"] is None  # held before it arrives
    holds = root.store.list_containment()["holds"]
    assert [h["object_id"] for h in holds] == [task_id]
    with pytest.raises(ControlError) as stale:
        release(client, hold, policy_version=1)
    assert stale.value.code == "stale_policy_version"
    release(client, hold, policy_version=3)
    assert root.store.list_containment()["holds"] == []
    with pytest.raises(ControlError) as missing:
        release(client, hold, policy_version=4)
    assert missing.value.code == "hold_not_found"
    grant = Action("revoke", "root-node", "grant", "parent>child")
    execute(client, grant, node_id="root-node", policy_version=5)
    (stored,) = root.store.list_containment()["ingress_filters"]
    assert (stored["authority"], stored["target"]) == (
        {"agent_id": "parent"},
        {"agent_id": "child"},
    )
    effect = Action("hold", "root-node", "effect", str(uuid4()))
    unknown = execute(client, effect, node_id="root-node", policy_version=6)
    assert unknown.enforcement["effect_state"] is None
    with pytest.raises(ControlError) as unsupported:
        execute(client, hold, node_id="elsewhere", policy_version=7)
    assert unsupported.value.code == "unsupported_action"


def test_engine_hold_stops_the_parent_at_the_agentd_boundary(nodes, tmp_path):
    """A child result fails validation before its parent reads it: the monitor
    pauses the parent through the real admin RPC, and agentd then refuses the
    parent's next dispatch as execution_held."""
    root, worker = nodes
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import parse_external_alert
    from test_containment_provenance import journal
    from containment_support import ingest

    root.submit("parent")
    _, binding = root.start("parent")
    child_id = root.dispatch("parent", binding, "child")["result"]["task_id"]
    deliver(root, worker)
    worker.start("child")
    worker.complete("child", child_id, {"body": "suspect"})
    deliver(worker, root)
    control = LocalAgentdControlClient(
        "core-monitor",
        {"root-node": transport(root), "worker-node": transport(worker)},
    )
    config = MonitorConfig(
        grants={"parent": frozenset({"child"}), "child": frozenset()}
    )
    harness = Harness(tmp_path / "monitor", config, control)
    for event in journal(root) + journal(worker):
        ingest(harness.core, event)
    harness.monitor.catch_up()
    (version,) = harness.monitor.graph.results
    harness.monitor.submit(
        parse_external_alert(
            {"kind": "validator_failure", "version_id": version}, now_ms=harness.now
        )
    )
    (decision,) = harness.monitor.handle_alerts()
    assert decision.status == "contained" and decision.cut_valid
    held = {h["object_id"] for h in root.store.list_containment()["holds"]}
    assert binding["task_id"] in held  # the parent is paused at its node
    reply = root.dispatch("parent", binding, "child")
    assert (reply["status"], reply["code"]) == ("error", "execution_held")
    for record in decision.phase1 + decision.phase2:
        assert (
            record.t_alert <= record.t_decision <= record.t_sent <= record.t_confirmed
        )
        assert record.t_applied is not None
    harness.close()


# ---------------------------------------------------------------------------
# Receding horizon and the strategy family
# ---------------------------------------------------------------------------
def test_frontier_advance_triggers_a_receding_re_solve(tmp_path):
    """A tainted invocation dispatches before its pause lands (its early window).
    The new task appears in the next batch; the monitor re-solves and holds it at
    the recipient's node before it can start."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        # The child inherits grant(w) ∩ scope(r) = {z}: it could delegate on.
        grants={"r": frozenset({"w", "z"}), "w": frozenset({"z"})},
        agent_nodes={"r": "n0", "w": "n1", "z": "n2"},
        default_delivery_ms=200,
        session_isolated_agents=frozenset({"r", "w"}),
    )
    control = FakeControlClient()
    harness = Harness(tmp_path, config, control)
    trace = TraceBuilder()
    attempt = trace.start(trace.root("r", "n0", 0), 1)
    harness.feed(trace.events)
    harness.at(10)
    first = harness.monitor.respond(
        operator_alert(inv(attempt), harness.now), roots=[inv(attempt)]
    )
    assert first.status == "contained"
    before = len(trace.events)
    child = trace.dispatch(attempt, "w", "n1", 12)  # emitted inside the early window
    harness.feed(trace.events[before:])
    assert harness.monitor.graph.frontier_advanced
    harness.at(20)
    again = harness.monitor.resolve_frontier()
    assert again is not None and again.status == "contained"
    assert ("n1", "task", child) in control.in_force()
    assert harness.monitor.resolve_frontier() is None  # nothing new
    harness.close()


@pytest.mark.parametrize("seed", range(6))
def test_every_strategy_emits_the_same_vocabulary(tmp_path, seed):
    from aggregator.containment_monitor.model import HOLD_KINDS, REVOKE_KINDS
    from aggregator.containment_monitor.strategies import CUT_STRATEGIES, STRATEGIES

    costs = {}
    for name in STRATEGIES:
        rng = random.Random(seed)
        trace, config = workload(rng)
        t_alert = rng.randint(30, 160)
        started = [
            e["execution_attempt_id"]
            for e in trace.events
            if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
        ]
        root = inv(rng.choice(started))
        control = FakeControlClient()
        harness = Harness(tmp_path / f"{name}", config, control)
        harness.feed([e for e in trace.events if event_ms(e) <= t_alert])
        harness.at(t_alert)
        decision = harness.monitor.respond(
            operator_alert(root, harness.now), strategy=name, roots=[root]
        )
        for record in decision.phase1 + decision.phase2:
            action = record.action
            assert action.kind in (HOLD_KINDS if action.op == "hold" else REVOKE_KINDS)
            assert (
                record.t_alert
                <= record.t_decision
                <= record.t_sent
                <= record.t_confirmed
            )
        if name in CUT_STRATEGIES:
            assert decision.status == "contained" and decision.phase1 is not None
        else:
            assert decision.status == "applied" and decision.phase1 == []
        costs[name] = decision.solve["predicted_cost"]
        harness.close()
    # Same G_C and cost model: the exact cut is never costlier than the greedy one.
    assert costs["firebreak"] <= costs["greedy_cut"] + 1e-9


# ---------------------------------------------------------------------------
# Effects: real effect-gateway evidence, and a held scheduled effect
# ---------------------------------------------------------------------------
def test_a_tainted_scheduled_effect_is_stopped_before_its_commit(tmp_path):
    """agentd's effect evidence reaches G_P/G_C through Core: an immediate effect
    is committed history, a scheduled one is a pending flow into a protected
    sink. Firebreak cuts it, and agentd then refuses the real commit."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import fx
    from containment_support import ingest
    from test_containment_provenance import journal

    actuator = {"effect_class": "device.command", "target": "actuator/*"}
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child"], effects=[actuator])
    worker.managed("child", effects=[actuator])
    try:
        root.submit("parent")
        _, binding = root.start("parent")
        root.dispatch("parent", binding, "child")
        deliver(root, worker)
        _, child = worker.start("child")

        def authorize(**extra):
            reply = worker.rpc(
                "child",
                "effect.authorize",
                schema_version=1,
                request_id=str(uuid4()),
                binding_id=child["binding_id"],
                effect_class="device.command",
                target="actuator/1",
                tool_call_id=None,
                **extra,
            )
            assert reply["status"] == "ok", reply
            return reply

        immediate = authorize()
        due = time.time_ns() // 1_000_000 + 60_000
        scheduled = authorize(scheduled_at_ms=due)
        assert (immediate["result"]["state"], scheduled["result"]["state"]) == (
            "committed",
            "scheduled",
        )
        control = LocalAgentdControlClient(
            "core-monitor",
            {"root-node": transport(root), "worker-node": transport(worker)},
        )
        config = MonitorConfig(protected_effect_classes=frozenset({"device.command"}))
        harness = Harness(tmp_path / "monitor", config, control)
        for event in journal(root) + journal(worker):
            ingest(harness.core, event)
        harness.monitor.catch_up()
        graph = harness.monitor.graph
        effect_id = scheduled["request_id"]
        assert set(graph.effects) == {effect_id}
        assert graph.effects[effect_id].scheduled_ms == due
        states = dict(
            graph.db.execute("SELECT effect_id,state FROM containment_effects")
        )
        assert states == {immediate["request_id"]: "committed", effect_id: "pending"}
        (attempt,) = [
            a for a, live in graph.running.items() if live.agent_id == "child"
        ]
        assert {fx(effect_id), fx(immediate["request_id"])} <= set(
            graph.successors(inv(attempt), ["data"])
        )
        decision = harness.monitor.respond(
            operator_alert(inv(attempt), harness.now), roots=[inv(attempt)]
        )
        assert decision.status == "contained" and decision.cut_valid
        in_force = {r.action.capability for r in harness.monitor.in_force.values()}
        assert in_force & {"hold:effect", "hold:task"}
        commit = worker.rpc(
            "child",
            "effect.commit",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=child["binding_id"],
            effect_id=effect_id,
        )
        assert (commit["status"], commit["code"]) in {
            ("error", "effect_held"),
            ("error", "execution_held"),
        }
        harness.close()
    finally:
        root.close()
        worker.close()


def test_the_monitor_keeps_the_fail_closed_controller_lease_alive(tmp_path):
    """With EDGECITADEL_CONTROLLER_LEASE_MS a node denies effects until a
    controller renewal arrives; the monitor's renewals reopen them."""
    from aggregator.containment_monitor.config import MonitorConfig

    actuator = {"effect_class": "device.command", "target": "actuator/*"}
    worker = Node(tmp_path / "worker", "worker-node")
    worker.managed("child", effects=[actuator])
    worker.store.controller_lease_ms = 30_000
    try:
        worker.submit("child")
        _, binding = worker.start("child")

        def authorize():
            return worker.rpc(
                "child",
                "effect.authorize",
                schema_version=1,
                request_id=str(uuid4()),
                binding_id=binding["binding_id"],
                effect_class="device.command",
                target="actuator/1",
                tool_call_id=None,
            )

        assert authorize()["code"] == "controller_lease_expired"
        control = LocalAgentdControlClient(
            "core-monitor", {"worker-node": transport(worker)}
        )
        config = MonitorConfig(agent_nodes={"child": "worker-node"})
        harness = Harness(tmp_path / "monitor", config, control)
        harness.monitor.step()  # describes the node and sends the renewal
        deadline = time.monotonic() + 5
        while "worker-node" not in harness.monitor.leases:
            assert time.monotonic() < deadline
            time.sleep(0.02)
            harness.monitor.renew_leases()
        lease = harness.monitor.leases["worker-node"]
        assert lease["enforced"] and lease["error"] is None
        assert lease["until_ms"] > time.time_ns() // 1_000_000 + 20_000
        assert authorize()["status"] == "ok"
        status = harness.monitor.status()["control"]["controller_leases"]
        assert status["worker-node"]["until_ms"] == lease["until_ms"]
        harness.close()
    finally:
        worker.close()
