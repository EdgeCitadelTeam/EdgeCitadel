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
def test_every_strategy_shares_the_vocabulary_and_mechanisms(
    tmp_path, seed, monkeypatch
):
    """Strategies differ only in their phase-2 rule: the same vocabulary, the
    same phase-1 freeze, the same best-effort guard, and the same protocol."""
    from aggregator.containment_monitor import strategies
    from aggregator.containment_monitor.model import HOLD_KINDS, REVOKE_KINDS

    guarded = []
    original = strategies.best_effort

    def guard(cg, unavoidable):
        actions = original(cg, unavoidable)
        guarded.append(frozenset(actions))
        return actions

    monkeypatch.setattr(strategies, "best_effort", guard)
    rng = random.Random(seed)
    trace, config = workload(rng)
    t_alert = rng.randint(30, 160)
    started = [
        e["execution_attempt_id"]
        for e in trace.events
        if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
    ]
    root = inv(rng.choice(started))
    costs, phase1, guards = {}, {}, {}
    for name in strategies.STRATEGIES:
        guarded.clear()
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
        assert decision.status in {"contained", "phase2_check_failed"}, name
        assert decision.solve["solver"] and "fast_path" in decision.solve
        phase1[name] = sorted(r.action for r in decision.phase1)
        # The guard of the first phase-2 round, and it was issued.
        guards[name] = guarded[0]
        issued = {r.action for r in decision.phase1 + decision.phase2}
        assert guards[name] <= issued, name
        costs[name] = decision.solves[0]["predicted_cost"]
        harness.close()
    # One phase-1 freeze and one guard for all: neither depends on the strategy.
    assert len({tuple(actions) for actions in phase1.values()}) == 1
    assert len(set(guards.values())) == 1
    # Same G_C and cost model: the exact cut is never costlier than the greedy one.
    assert costs["firebreak"] <= costs["greedy_cut"] + 1e-9


# ---------------------------------------------------------------------------
# Effects: real effect-gateway evidence, and a held scheduled effect
# ---------------------------------------------------------------------------
def test_a_tainted_scheduled_effect_is_stopped_before_its_commit(tmp_path):
    """agentd's effect evidence reaches G_P/G_C through Core: an immediate effect
    is committed history, a scheduled one is a pending flow into a protected
    sink. Firebreak cuts it, and the gateway then refuses to perform it."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import fx
    from containment_support import ingest
    from containment_test_support import Ledger
    from edgecitadel_agentd.effect_adapters import LedgerAdapter
    from test_containment_provenance import journal

    actuator = {"effect_class": "device.command", "target": "actuator/*"}
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    ledger = Ledger()
    root.managed("parent", outbound=["child"], effects=[actuator])
    worker.managed("child", effects=[actuator])
    worker.store.effects.adapters = {"http": LedgerAdapter(ledger.url)}
    try:
        root.submit("parent")
        _, binding = root.start("parent")
        root.dispatch("parent", binding, "child")
        deliver(root, worker)
        _, child = worker.start("child")

        def execute(**extra):
            reply = worker.rpc(
                "child",
                "effect.execute",
                schema_version=1,
                request_id=str(uuid4()),
                binding_id=child["binding_id"],
                adapter="http",
                effect_class="device.command",
                target="actuator/1",
                data=None,
                tool_call_id=None,
                **extra,
            )
            assert reply["status"] == "ok", reply
            return reply

        immediate = execute(scheduled_at_ms=None)
        due = time.time_ns() // 1_000_000 + 60_000
        scheduled = execute(scheduled_at_ms=due)
        assert (immediate["result"]["state"], scheduled["result"]["state"]) == (
            "committed",
            "scheduled",
        )
        assert len(ledger.requests) == 1  # the immediate one, performed at once
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
        worker.store.effects.run_due("worker-node", now_ms=due)
        (denied,) = [
            e
            for e in journal(worker)
            if e["kind"] == "effect" and e["phase"] == "denied"
        ]
        assert denied["attributes"]["effect_id"] == effect_id
        assert denied["attributes"]["reason"] in {"effect_held", "execution_held"}
        assert len(ledger.requests) == 1
        harness.close()
    finally:
        ledger.close()
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


# ---------------------------------------------------------------------------
# Ingest-then-release: confirmations carry journal positions
# ---------------------------------------------------------------------------
def journaled_after_alert(tmp_path, timeout_ms):
    """Three tainted invocations on n0; n0 confirms every decision with a journal
    position one event beyond what Core has ingested."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"a": frozenset({"w"})},
        agent_nodes={"a": "n0", "w": "n1"},
        session_isolated_agents=frozenset({"a"}),
        min_emission_ms={"a": 1000},
        ingest_timeout_ms=timeout_ms,
    )
    trace = TraceBuilder()
    tasks = [trace.root("a", "n0", i) for i in range(3)]
    roots = [inv(trace.start(task_id, 5)) for task_id in tasks]
    late = trace._event(
        "n0",
        "task",
        "queued",
        6,
        agent="operator",
        task_id=str(uuid4()),
        context=str(uuid4()),
        attributes={"source_role": "sender"},
    )
    position = {"source_epoch": late["source_epoch"], "source_seq": late["source_seq"]}
    control = FakeControlClient(journal=lambda node: position if node == "n0" else None)
    harness = Harness(tmp_path, config, control)
    harness.feed(trace.events[:-1])
    return harness, control, roots, late


def test_a_release_waits_until_the_confirmed_journal_is_ingested(tmp_path):
    from containment_support import ingest

    harness, control, roots, late = journaled_after_alert(tmp_path, 5000)
    polls, ingested_at = [], []

    def wait(seconds):
        polls.append(seconds)
        if len(polls) == 3:  # Core receives the event a little later
            ingest(harness.core, late)
            ingested_at.append(time.monotonic())

    harness.monitor.wait = wait
    decision = harness.monitor.respond(
        operator_alert(roots[0], harness.now), roots=roots
    )
    assert decision.status == "contained" and len(polls) >= 3
    releases = [t for t, op, _ in control.timeline if op == "release"]
    assert releases and min(releases) >= ingested_at[0]
    for record in decision.phase1 + decision.phase2:
        assert record.t_fenced is not None
        assert record.sites["n0"]["journal"]["source_seq"] == late["source_seq"]
    status = harness.monitor.status()
    assert (
        status["ingest"]["n0"][late["source_epoch"]]["through_seq"]
        == (late["source_seq"])
    )
    harness.close()


def test_a_journal_never_ingested_keeps_every_phase_one_hold(tmp_path):
    harness, control, roots, _ = journaled_after_alert(tmp_path, 100)
    decision = harness.monitor.respond(
        operator_alert(roots[0], harness.now), roots=roots
    )
    assert decision.status == "ingest_timeout" and decision.released == []
    assert not [op for _, op, _ in control.timeline if op == "release"]
    assert {r.action for r in decision.phase1} <= set(harness.monitor.in_force)
    assert all(r.t_fenced is None for r in decision.phase2)
    harness.close()


# ---------------------------------------------------------------------------
# Admission invariant between phase 1 and phase 2
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("worker_holds_tasks", [True, False])
def test_a_tainted_invocation_is_admitted_nothing_between_the_phases(
    tmp_path, worker_holds_tasks
):
    """Once phase 1 is confirmed, every new dispatch and effect a source-side
    invocation requests is denied at its node until phase 2 confirms a cut
    covering it: it neither propagates nor acts. Phase 1 pauses it (its hold
    lands before any emission after the landing, thanks to the early-window
    split); where its node cannot hold tasks, the admission freeze holds its
    egress instead."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import ingest
    from containment_test_support import Ledger
    from edgecitadel_agentd.effect_adapters import LedgerAdapter
    from test_containment_provenance import journal

    actuator = {"effect_class": "device.command", "target": "actuator/*"}
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    ledger = Ledger()
    root.managed("parent", outbound=["child", "other"], effects=[actuator])
    worker.managed("child", outbound=["other"], effects=[actuator])
    worker.managed("other")
    worker.store.effects.adapters = {"http": LedgerAdapter(ledger.url)}
    try:
        root.submit("parent")
        _, binding = root.start("parent")
        root.dispatch("parent", binding, "child")
        deliver(root, worker)
        _, child = worker.start("child")
        control = LocalAgentdControlClient(
            "core-monitor",
            {"root-node": transport(root), "worker-node": transport(worker)},
        )
        if not worker_holds_tasks:
            described = control.capabilities
            control.capabilities = lambda node: (
                described(node) - {"hold:task"}
                if node == "worker-node"
                else described(node)
            )
        config = MonitorConfig(protected_effect_classes=frozenset({"device.command"}))
        harness = Harness(tmp_path / "monitor", config, control)
        ingested = set()
        for event in journal(root) + journal(worker):
            ingest(harness.core, event)
            ingested.add(event["event_id"])
        harness.monitor.catch_up()
        (attempt,) = [
            a
            for a, live in harness.monitor.graph.running.items()
            if live.agent_id == "child"
        ]
        attempts: dict[str, dict] = {}
        fence = harness.monitor.fence

        def between_phases(records):
            if not attempts:  # right after phase 1 is confirmed
                attempts["dispatch"] = worker.dispatch("child", child, "other")
                attempts["effect"] = worker.rpc(
                    "child",
                    "effect.execute",
                    schema_version=1,
                    request_id=str(uuid4()),
                    binding_id=child["binding_id"],
                    adapter="http",
                    effect_class="device.command",
                    target="actuator/1",
                    data=None,
                    tool_call_id=None,
                    scheduled_at_ms=None,
                )
                attempts["phase1"] = sorted(r.action.label() for r in records)
                # Trace sync carries the denials to Core; phase 2 waits for them.
                for event in journal(worker):
                    if event["event_id"] not in ingested:
                        ingest(harness.core, event)
            return fence(records)

        harness.monitor.fence = between_phases
        decision = harness.monitor.respond(
            operator_alert(inv(attempt), harness.now), roots=[inv(attempt)]
        )
        child_task = child["task_id"]
        kind, code = (
            ("task", "execution_held")
            if worker_holds_tasks
            else ("egress", "egress_held")
        )
        assert f"hold:{kind}:{child_task}@worker-node" in attempts["phase1"]
        assert attempts["dispatch"]["code"] == code
        assert attempts["effect"]["code"] == code
        assert ledger.requests == []  # the effect never acted
        # The dispatch created no child and nothing was queued for publication.
        assert not [
            m
            for m in worker.store.pending_transport()
            if m["envelope"].get("recipient_id") == "other"
        ]
        assert decision.status == "contained" and decision.cut_valid
        assert all(r.t_fenced is not None for r in decision.phase2)
        harness.close()
    finally:
        ledger.close()
        root.close()
        worker.close()


# ---------------------------------------------------------------------------
# The two baselines added in M4
# ---------------------------------------------------------------------------
def two_teams(tmp_path):
    """Root r delegates to a (tainted) and b (healthy); both schedule a
    protected actuator command; a's sibling result is already back at r."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"r": frozenset({"a", "b", "c"}), "a": frozenset(), "b": frozenset()},
        effect_scopes={
            "r": frozenset(),
            "a": frozenset({"device.command"}),
            "b": frozenset({"device.command"}),
            "c": frozenset(),
        },
        protected_effect_classes=frozenset({"device.command"}),
        agent_nodes={"r": "n0", "a": "n1", "b": "n2", "c": "n2"},
        session_isolated_agents=frozenset({"r", "a", "b", "c"}),
        best_effort_guard=False,
    )
    trace = TraceBuilder()
    root = trace.start(trace.root("r", "n0", 0), 1)
    task_a = trace.dispatch(root, "a", "n1", 2)
    task_b = trace.dispatch(root, "b", "n2", 2)
    task_c = trace.dispatch(root, "c", "n2", 2)
    for task_id in (task_a, task_b, task_c):
        trace.arrive(task_id, 3)
    a, b, c = (trace.start(t, 4) for t in (task_a, task_b, task_c))
    effect_a = trace.effect(a, "device.command", "requested", 5, scheduled=10_000)
    effect_b = trace.effect(b, "device.command", "requested", 5, scheduled=10_000)
    trace.complete(c, 6)  # a result r has not read yet
    harness = Harness(tmp_path, config, FakeControlClient())
    harness.feed(trace.events)
    harness.at(20)
    ids = {
        "a": task_a,
        "b": task_b,
        "c": task_c,
        "effect_a": effect_a["attributes"]["effect_id"],
        "effect_b": effect_b["attributes"]["effect_id"],
    }
    return harness, inv(a), ids


def phase_two(decision):
    return {Action.from_json(a).label() for a in decision.solves[0]["actions"]}


def test_trace_and_quarantine_pauses_and_revokes_the_tainted_region(tmp_path):
    harness, root, ids = two_teams(tmp_path)
    decision = harness.monitor.respond(
        operator_alert(root, harness.now), strategy="trace_and_quarantine", roots=[root]
    )
    chosen = phase_two(decision)
    assert {f"hold:task:{ids['a']}@n1", f"revoke:egress:{ids['a']}@n1"} <= chosen
    # Only the tainted region: the healthy invocations stay untouched.
    for healthy in ("b", "c"):
        assert not any(ids[healthy] in label and "@n2" in label for label in chosen)
    assert decision.solve["solver"] == "rule" and decision.solve["fast_path"] is False
    harness.close()


def test_sink_guard_guards_every_protected_sink_without_tracing(tmp_path):
    harness, root, ids = two_teams(tmp_path)
    decision = harness.monitor.respond(
        operator_alert(root, harness.now), strategy="sink_guard", roots=[root]
    )
    chosen = phase_two(decision)
    # Every pending scheduled effect, tainted or not, at its gateway.
    assert {
        f"hold:effect:{ids['effect_a']}@n1",
        f"hold:effect:{ids['effect_b']}@n2",
    } <= chosen
    # Every agent's protected effect class, revoked at its gateway.
    assert {
        "revoke:grant:a>fx:device.command@n1",
        "revoke:grant:b>fx:device.command@n2",
    } <= chosen
    # Every result delivered into the root (r's output), live or already sent.
    assert {f"hold:task:{ids[t]}@n0" for t in ("a", "b", "c")} <= chosen
    # No tracing: the tainted invocation itself is neither paused nor revoked.
    assert f"hold:task:{ids['a']}@n1" not in chosen
    assert f"revoke:egress:{ids['a']}@n1" not in chosen
    harness.close()
