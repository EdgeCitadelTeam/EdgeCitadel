"""Two-phase enforcement, fencing and the control clients."""

import math
import random
from collections import Counter
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
from containment_support import Harness, ingest
from edgecitadel_agentd.service import dispatch as rpc
from edgecitadel_agentd.store import StoreError
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
    config = replace(config, mechanisms={"*": {"best_effort_guard": False}})
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
        # Replay what the nodes applied, in order. Phase 1 converges once every
        # phase-1 action confirmed is in force, including those released later.
        phase1 = {
            r.action for r in decision.phase1 if r.state in {"in_force", "released"}
        }
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
    emit within a second of starting. The frontier-freeze phase 1 holds each of
    them; the optimum revokes a's credential once (a gated group), in time for
    their emissions. Taint-aware admission is off: it would add the labels of
    the three, and these tests are about the protocol around the one
    revocation."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"a": frozenset({"w"})},
        agent_nodes={"a": "n0", "w": "n1"},
        session_isolated_agents=frozenset({"a"}),
        min_emission_ms={"a": 1000},
        mechanisms={"firebreak": {"taint_aware_admission": False}},
        phase1_mode="freeze",
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


def test_a_released_decision_also_releases_its_unconfirmed_holds():
    """A hold sent without a confirmation may be in force at its node: the
    guarded release releases it with the others, and one its node never
    applied counts as released (nothing was in force)."""
    from aggregator.containment_monitor.enforcement import (
        ActionRecord,
        EnforcementDriver,
    )
    from aggregator.containment_monitor.model import hold_task

    applied, never = str(uuid4()), str(uuid4())
    control = FakeControlClient(lose=lambda node, op, object_id: op == "hold")
    versions = iter(range(1, 100))
    driver = EnforcementDriver(control, lambda: next(versions))
    try:
        (lost,) = driver.apply(
            [hold_task("n0", applied)],
            incident_id="i",
            phase=1,
            t_alert=0,
            t_decision=0,
        )
        assert lost.state == "unconfirmed"
        assert ("n0", "task", applied) in control.in_force()  # it did land
        unsent = ActionRecord(hold_task("n0", never), "i", 1, 1, 0, 0)
        unsent.state = "unconfirmed"
        released = driver.release([lost, unsent])
        assert [r.state for r in released] == ["released", "released"]
        assert control.in_force() == set()
    finally:
        driver.close()


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
    # Paused, or labelled (adaptive phase 1): nothing it emits leaves unheld.
    assert {("n1", "task", child), ("n1", "egress", child)} & control.in_force()
    assert harness.monitor.resolve_frontier() is None  # nothing new
    harness.close()


def every_strategy(tmp_path, seed, monkeypatch, profile):
    """Run every strategy on one workload under a mechanism profile; spy on the
    guard and taint-aware admission of each strategy's first phase-2 round."""
    from aggregator.containment_monitor import strategies
    from aggregator.containment_monitor.model import HOLD_KINDS, REVOKE_KINDS

    calls: dict[str, list] = {"guard": [], "admission": []}
    originals = {
        "guard": strategies.best_effort,
        "admission": strategies.taint_admission,
    }

    def spy(kind):
        def call(*args):
            actions = originals[kind](*args)
            calls[kind].append(frozenset(actions))
            return actions

        return call

    monkeypatch.setattr(strategies, "best_effort", spy("guard"))
    monkeypatch.setattr(strategies, "taint_admission", spy("admission"))
    rng = random.Random(seed)
    trace, config = workload(rng)
    config = replace(config, profile=profile)
    t_alert = rng.randint(30, 160)
    started = [
        e["execution_attempt_id"]
        for e in trace.events
        if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
    ]
    root = inv(rng.choice(started))
    runs = {}
    for name in strategies.STRATEGIES:
        for kind in calls:
            calls[kind].clear()
        control = FakeControlClient()
        harness = Harness(tmp_path / f"{profile}-{name}", config, control)
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
        assert decision.solve["solver"] and "fast_path" in decision.solve
        assert decision.profile == profile
        assert decision.mechanisms == config.mechanisms_for(name).to_json()
        issued = {r.action for r in decision.phase1 + decision.phase2}
        runs[name] = {
            "decision": decision,
            "phase1": sorted(r.action for r in decision.phase1),
            "guard": calls["guard"][0] if calls["guard"] else None,
            "admission": bool(calls["admission"]),
            "issued": issued,
        }
        harness.close()
    return runs


@pytest.mark.parametrize("seed", range(6))
def test_the_shared_profile_isolates_the_phase_two_rule(tmp_path, seed, monkeypatch):
    """Ablation: strategies differ only in their phase-2 rule: the same
    vocabulary, the same phase-1 freeze, the same best-effort guard and
    taint-aware admission, and the same protocol."""
    runs = every_strategy(tmp_path, seed, monkeypatch, "shared")
    for name, run in runs.items():
        assert run["decision"].status in {"contained", "phase2_check_failed"}, name
        assert run["guard"] is not None and run["guard"] <= run["issued"], name
        assert run["admission"], name
    # One phase-1 freeze and one guard for all: neither depends on the strategy.
    assert len({tuple(run["phase1"]) for run in runs.values()}) == 1
    assert len({run["guard"] for run in runs.values()}) == 1
    # Same G_C and cost model: the exact cut is never costlier than the greedy one.
    costs = {n: r["decision"].solves[0]["predicted_cost"] for n, r in runs.items()}
    assert costs["firebreak"] <= costs["greedy_cut"] + 1e-9


@pytest.mark.parametrize("seed", range(6))
def test_the_deployed_profile_adds_mechanisms_only_to_the_cut_strategies(
    tmp_path, seed, monkeypatch
):
    """Main runs: the rule-based baselines apply their own rule directly, with
    nothing lineage-based added (and no phase 1); the cut strategies run the
    two-phase protocol with the adaptive phase 1 (their own cut at the alert
    and its labels), the guard and taint-aware admission (Theorem 5). Every
    strategy's actions are confirmed either way."""
    from aggregator.containment_monitor.config import CUT_STRATEGIES

    runs = every_strategy(tmp_path, seed, monkeypatch, "deployed")
    for name, run in runs.items():
        decision = run["decision"]
        assert all(r.t_confirmed is not None for r in decision.phase2), name
        if name in CUT_STRATEGIES:
            assert set(decision.mechanisms.values()) == {True}, name
            assert decision.status in {"contained", "phase2_check_failed"}, name
            assert run["guard"] is not None and run["guard"] <= run["issued"], name
            assert run["admission"], name
            assert decision.phase1_mode == "adaptive", name
            at_alert = decision.solves[0]
            assert at_alert["phase"] == 1, name
            assert {Action.from_json(a) for a in at_alert["actions"]} <= set(
                run["phase1"]
            ), name
        else:
            assert set(decision.mechanisms.values()) == {False}, name
            assert decision.status == "applied" and not decision.phase1, name
            assert decision.phase1_mode is None, name
            assert run["guard"] is None and not run["admission"], name
            assert decision.released == [], name


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
        phase1_mode="freeze",  # frontier holds, released after phase 2
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
    """Once the frontier-freeze phase 1 is confirmed, every new dispatch and
    effect a source-side invocation requests is stopped at its node: it neither
    propagates nor acts. Phase 1 pauses it (its hold lands before any emission
    after the landing, thanks to the early-window split): both are denied.
    Where its node cannot hold tasks, the admission freeze labels its egress
    instead: both are held as events, to be admitted one by one."""
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
        config = MonitorConfig(
            protected_effect_classes=frozenset({"device.command"}),
            phase1_mode="freeze",
        )
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
        kind = "task" if worker_holds_tasks else "egress"
        assert f"hold:{kind}:{child_task}@worker-node" in attempts["phase1"]
        if worker_holds_tasks:
            assert attempts["dispatch"]["code"] == "execution_held"
            assert attempts["effect"]["code"] == "execution_held"
        else:
            assert attempts["dispatch"]["status"] == "ok"
            assert attempts["effect"]["result"]["state"] == "held"
            held = dict(
                worker.store._connection.execute(
                    "SELECT kind,state FROM held_events"
                ).fetchall()
            )
            assert held == {"dispatch": "held", "effect": "held"}
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
# Per-event admission of labelled units (admission.py)
# ---------------------------------------------------------------------------
class LabelledAssistant:
    """An assistant on one node with three tainted live invocations, contained
    by Firebreak (deployed: adaptive phase 1, taint-aware labels). Its replies
    to users are unprotected (network.http), its device commands protected;
    stopping its service (credential, admission) forfeits its future requests
    and pausing the three costs more than revoking its grants, so the cut
    revokes its device class (and, with ``boundary="sink"``, its grant to the
    helper, whose new invocations are horizon sinks) and labels all three."""

    ACT = {"effect_class": "device.command", "target": "actuator/*"}
    REPLY = {"effect_class": "network.http", "target": "reply/*"}

    def __init__(self, tmp_path, boundary="sink"):
        from aggregator.containment_monitor.config import MonitorConfig
        from containment_test_support import Ledger
        from edgecitadel_agentd.effect_adapters import LedgerAdapter

        self.node = Node(tmp_path / "k", "site-k")
        self.ledger = Ledger()
        self.node.managed(
            "assistant", outbound=["helper"], effects=[self.ACT, self.REPLY]
        )
        self.node.managed("helper", effects=[self.ACT])
        self.node.store.effects.adapters = {"http": LedgerAdapter(self.ledger.url)}
        self.tasks, self.bindings = [], []
        for _ in range(3):
            self.tasks.append(self.node.submit("assistant"))
            self.bindings.append(self.node.start("assistant")[1])
        control = LocalAgentdControlClient(
            "core-monitor", {"site-k": transport(self.node)}
        )
        config = MonitorConfig(
            protected_effect_classes=frozenset({"device.command"}),
            future_requests={"assistant": 10.0},
            agent_nodes={"assistant": "site-k", "helper": "site-k"},
            # No emission within a minute of starting: every action is timely.
            min_emission_ms={"assistant": 60_000, "helper": 60_000},
            boundary=boundary,
        )
        self.harness = Harness(tmp_path / "monitor", config, control)
        self.harness.now = time.time_ns() // 1_000_000
        self.ingested: set[str] = set()
        self.sync()
        roots = [inv(b["execution_attempt_id"]) for b in self.bindings]
        self.decision = self.harness.monitor.respond(
            operator_alert(roots[0], self.harness.now),
            strategy="firebreak",
            roots=roots,
        )

    def sync(self):
        """Trace sync: Core ingests what the node journaled since last time."""
        from containment_support import ingest
        from test_containment_provenance import journal

        for event in journal(self.node):
            if event["event_id"] not in self.ingested:
                ingest(self.harness.core, event)
                self.ingested.add(event["event_id"])
        self.harness.monitor.catch_up()

    def execute(self, binding, rule, target):
        reply = self.node.rpc(
            "assistant",
            "effect.execute",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=binding["binding_id"],
            adapter="http",
            effect_class=rule["effect_class"],
            target=target,
            data=None,
            tool_call_id=None,
            scheduled_at_ms=None,
        )
        assert reply["status"] == "ok", reply
        return reply

    def held(self):
        return {
            row["subject_id"]: row["state"]
            for row in self.node.store._connection.execute(
                "SELECT subject_id,state FROM held_events"
            )
        }

    def close(self):
        self.harness.close()
        self.ledger.close()
        self.node.close()


def test_a_labelled_units_reply_is_admitted_and_its_device_command_stays_held(
    tmp_path,
):
    """The reply (unprotected) is held by agentd and admitted after one round,
    with its latency logged; the device command, whose class the cut blocks,
    stays held, also when the decision is taken again."""
    unit = LabelledAssistant(tmp_path)
    try:
        assert unit.decision.status == "contained"
        in_force = {r.action.label() for r in unit.harness.monitor.in_force.values()}
        assert "revoke:grant:assistant>fx:device.command@site-k" in in_force
        for task_id in unit.tasks:
            assert f"hold:egress:{task_id}@site-k" in in_force
        reply = unit.execute(unit.bindings[0], unit.REPLY, "reply/1")
        command = unit.execute(unit.bindings[0], unit.ACT, "actuator/1")
        assert {reply["result"]["state"], command["result"]["state"]} == {"held"}
        unit.sync()
        (admitted, kept) = sorted(
            unit.harness.monitor.admit_held(), key=lambda r: r["decision"]
        )
        assert (admitted["decision"], admitted["reason"]) == (
            "admit",
            "unprotected_effect",
        )
        assert admitted["subject_id"] == reply["request_id"]
        assert admitted["release_state"] == "released"
        assert admitted["latency_ms"] >= 0
        assert (kept["decision"], kept["reason"]) == ("keep", "protected_sink")
        assert kept["subject_id"] == command["request_id"]
        assert unit.node.store.effects.run_due("site-k") == 1
        assert [r["body"]["target"] for r in unit.ledger.requests] == ["reply/1"]
        # Decided again (nothing new is logged): the command stays held.
        unit.sync()
        assert unit.harness.monitor.admit_held() == []
        assert unit.held() == {
            reply["request_id"]: "admitted",
            command["request_id"]: "held",
        }
        assert unit.node.store.effects.run_due("site-k") == 0
        status = unit.harness.monitor.status()["held_events"]
        assert status == {"admitted": 1, "kept": 1}
    finally:
        unit.close()


def test_a_labelled_dispatch_is_admitted_once_its_new_task_is_labelled(tmp_path):
    """(R) for a new edge: the dispatch's head (the new task) is labelled and
    the label confirmed before the dispatch is admitted; the new task then runs
    under its own label. The result, which no live invocation consumes, is
    admitted too, and the invocation completes."""
    unit = LabelledAssistant(tmp_path, boundary="defer")
    try:
        node = unit.node
        reply = node.dispatch("assistant", unit.bindings[0], "helper")
        assert reply["status"] == "ok", reply
        helper = reply["result"]["task_id"]
        session = node.agents["helper"].session_id
        assert node.rpc("helper", "task.claim", session_id=session) is None
        unit.sync()
        (record,) = unit.harness.monitor.admit_held()
        assert (record["event"], record["decision"]) == ("dispatch", "admit")
        assert record["reason"] == "head_separated"
        assert [label["action"]["kind"] for label in record["labels"]] == ["egress"]
        assert record["labels"][0]["action"]["object_id"] == helper
        assert record["labels"][0]["t_fenced"] is not None
        holds = {
            (h["kind"], h["object_id"]) for h in node.store.list_containment()["holds"]
        }
        assert ("egress", helper) in holds
        assert node.rpc("helper", "task.claim", session_id=session)["task_id"] == helper
        # The labelled invocation's result is held, then admitted.
        with pytest.raises(StoreError, match="egress is held"):
            node.complete("assistant", unit.tasks[0], {"body": "answer"})
        unit.sync()
        (record,) = unit.harness.monitor.admit_held()
        assert (record["event"], record["reason"]) == ("result", "no_live_consumer")
        node.complete("assistant", unit.tasks[0], {"body": "answer"})
        assert node.store.get_task(unit.tasks[0])["state"] == "completed"
    finally:
        unit.close()


class Arriving:
    """A control client through which a new request reaches the assistant
    while the alert's first actions are in flight (before their confirmation)."""

    def __init__(self, inner, arrive):
        self.inner, self.arrive = inner, arrive

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def send(self, node_id, action, target, *, policy_version):
        if self.arrive is not None and action not in ("describe", "renew"):
            arrive, self.arrive = self.arrive, None
            arrive()
        return self.inner.send(node_id, action, target, policy_version=policy_version)


@pytest.mark.parametrize("boundary", ["defer", "sink"])
def test_an_idle_tainted_context_is_covered_before_phase_1_converges(
    tmp_path, boundary
):
    """Whole-context taint (a shared-memory assistant) with no live invocation
    at the alert: the context has no unit to label, so (C2) covers it with its
    protected classes' revocation, pushed at the alert and confirmed before
    phase 1 converges. A request that arrives between the alert and that
    confirmation, and one that arrives after convergence, run with tainted
    memory: their device commands are refused, their replies go out."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_test_support import Ledger
    from edgecitadel_agentd.effect_adapters import LedgerAdapter

    act = {"effect_class": "device.command", "target": "actuator/*"}
    reply = {"effect_class": "network.http", "target": "reply/*"}
    node = Node(tmp_path / "k", "site-k")
    ledger = Ledger()
    node.managed("assistant", effects=[act, reply])
    node.store.effects.adapters = {"http": LedgerAdapter(ledger.url)}
    node.submit("assistant", "reads the corrupted schedule")
    first, binding = node.start("assistant")
    node.complete("assistant", first["task_id"], {"body": "noted"})
    arrived = []

    def arrive():  # in the driver's thread; Core ingests it on the next sync
        node.submit("assistant", "a request during the first round")
        arrived.append(node.start("assistant")[1])

    control = Arriving(
        LocalAgentdControlClient("core-monitor", {"site-k": transport(node)}), arrive
    )
    config = MonitorConfig(
        protected_effect_classes=frozenset({"device.command"}),
        future_requests={"assistant": 10.0},
        agent_nodes={"assistant": "site-k"},
        shared_memory_agents=frozenset({"assistant"}),
        boundary=boundary,
    )
    harness = Harness(tmp_path / "monitor", config, control)
    harness.now = time.time_ns() // 1_000_000
    ingested: set[str] = set()
    catch_up = harness.monitor.catch_up

    def sync():
        """Trace sync, whenever the monitor reads Core (as while it fences)."""
        from containment_support import ingest
        from test_containment_provenance import journal

        for event in journal(node):
            if event["event_id"] not in ingested:
                ingest(harness.core, event)
                ingested.add(event["event_id"])
        return catch_up()

    harness.monitor.catch_up = sync

    def execute(bound, rule, target):
        result = node.rpc(
            "assistant",
            "effect.execute",
            schema_version=1,
            request_id=str(uuid4()),
            binding_id=bound["binding_id"],
            adapter="http",
            effect_class=rule["effect_class"],
            target=target,
            data=None,
            tool_call_id=None,
            scheduled_at_ms=None,
        )
        return result["result"]["state"] if result["status"] == "ok" else result["code"]

    try:
        sync()
        root = inv(binding["execution_attempt_id"])
        decision = harness.monitor.respond(
            operator_alert(root, harness.now), strategy="firebreak", roots=[root]
        )
        assert arrived, "no request arrived during the first round"
        assert decision.status == "contained"
        cover = "revoke:grant:assistant>fx:device.command@site-k"
        phase1 = {r.action.label(): r for r in decision.phase1}
        assert cover in phase1 and phase1[cover].state == "in_force"
        assert phase1[cover].t_fenced is not None
        assert decision.convergence[0]["phase"] == 1
        assert decision.convergence[0]["converged"]
        node.submit("assistant", "a request after convergence")
        later = node.start("assistant")[1]
        # The request seen before convergence was labelled too: its command is
        # held (and kept); the later one is refused at the gateway.
        assert execute(arrived[0], act, "actuator/1") == "held"
        assert execute(later, act, "actuator/1") == "ingress_filtered"
        assert execute(later, reply, "reply/1") == "committed"
    finally:
        harness.close()
        ledger.close()
        node.close()


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
        mechanisms={"*": {"best_effort_guard": False}},
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


def test_gsafeguard_flags_the_root_agent_and_drops_only_its_outputs(tmp_path):
    """G-Safeguard: the flagged agent (a) keeps running and receiving; its
    egress is revoked and its result parked at its parent's node; nothing of
    the consumers (r, b, c), no other agent's grant, no pause and nothing
    already sent (c's result back at r, a's scheduled effect) is touched."""
    harness, root, ids = two_teams(tmp_path)
    decision = harness.monitor.respond(
        operator_alert(root, harness.now), strategy="gsafeguard", roots=[root]
    )
    assert decision.status == "applied" and decision.phase1 == []
    assert phase_two(decision) == {
        f"revoke:egress:{ids['a']}@n1",
        f"hold:task:{ids['a']}@n0",  # a's result, parked at r's node
    }
    assert harness.monitor.graph.flagged_agents == {"a"}
    harness.close()


def test_gsafeguard_covers_a_later_unit_of_the_flagged_agent(tmp_path):
    """The flag is sticky: a task r dispatches to a after the alert is a
    frontier advance, though nothing tainted reaches it, and the re-solve
    applies the same rule to it; a later task of another agent is not one.
    The flag survives a monitor restart."""
    from containment_support import TraceBuilder

    harness, root, ids = two_teams(tmp_path)
    harness.monitor.respond(
        operator_alert(root, harness.now), strategy="gsafeguard", roots=[root]
    )
    trace = TraceBuilder()
    other = trace.root("b", "n2", 30)  # a clean root task of b
    harness.feed(trace.events)
    assert not harness.monitor.graph.frontier_advanced
    harness.reopen()
    assert harness.monitor.graph.flagged_agents == {"a"}
    trace = TraceBuilder()
    later = trace.root("a", "n1", 40)  # a new, clean request to the flagged a
    trace.start(later, 41)
    harness.feed(trace.events)
    assert harness.monitor.graph.frontier_advanced
    harness.at(100)
    resolved = harness.monitor.resolve_frontier()
    assert resolved is not None and resolved.strategy == "gsafeguard"
    added = {r.action.label() for r in resolved.phase2}
    assert f"revoke:egress:{later}@n1" in added
    assert not any(other in label for label in added)
    assert not any(label.startswith(f"hold:task:{later}") for label in added)
    harness.close()


@pytest.mark.parametrize(
    "mode, labels",
    [
        ("freeze", "frontier"),
        ("adaptive", "frontier"),
        ("adaptive", "forward_closure"),
    ],
)
def test_phase_one_labels_the_source_side_not_only_the_taint(tmp_path, mode, labels):
    """Theory A.6 (b), counter-scenario of section 8.3 step 2 (and 8.6 (2a)
    for the adaptive phase 1): the parent has not read its child's tainted
    result yet (not tainted) but is reachable from the taint because the
    result reached its node before a park could land. Phase 1 must label it,
    in the adaptive mode as part of S(B_a): labelling only tainted invocations
    (or only U) would let it dispatch between the phases, unheld."""
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import ingest
    from test_containment_provenance import journal

    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child", "other"])
    root.managed("other")
    worker.managed("child")
    try:
        root.submit("parent")
        _, parent = root.start("parent")
        child_id = root.dispatch("parent", parent, "child")["result"]["task_id"]
        deliver(root, worker)
        _, child = worker.start("child")
        worker.complete("child", child_id, {"body": "suspect"})
        deliver(worker, root)  # the tainted result is at the parent's node
        control = LocalAgentdControlClient(
            "core-monitor",
            {"root-node": transport(root), "worker-node": transport(worker)},
        )
        config = MonitorConfig(phase1_mode=mode, phase1_labels=labels)
        harness = Harness(tmp_path / "monitor", config, control)
        for event in journal(root) + journal(worker):
            ingest(harness.core, event)
        harness.monitor.catch_up()
        graph = harness.monitor.graph
        observed: dict[str, object] = {}
        fence = harness.monitor.fence

        def between_phases(records):
            if not observed:
                observed["parent_tainted"] = inv(parent["execution_attempt_id"]) in (
                    graph.taint
                )
                observed["phase1"] = {r.action.label() for r in records}
                observed["dispatch"] = root.dispatch("parent", parent, "other")
                for event in journal(root):
                    ingest(harness.core, event)
            return fence(records)

        harness.monitor.fence = between_phases
        root_cause = inv(child["execution_attempt_id"])
        decision = harness.monitor.respond(
            operator_alert(root_cause, harness.now), roots=[root_cause]
        )
        assert observed["parent_tainted"] is False  # reachable, not tainted
        held = {
            f"hold:{kind}:{parent['task_id']}@root-node" for kind in ("task", "egress")
        }
        assert held & observed["phase1"], observed["phase1"]
        assert observed["dispatch"]["code"] in {"execution_held", "egress_held"}
        assert decision.status == "contained" and decision.cut_valid
        assert decision.phase1_mode == mode and decision.phase1_labels == labels
        harness.close()
    finally:
        root.close()
        worker.close()


# ---------------------------------------------------------------------------
# phase1_mode: freeze, freeze_plus_cut and adaptive phase 1
# ---------------------------------------------------------------------------
def late_effect(tmp_path, mode, seed=0, due=60_000, latency_ms=50.0, **config):
    """C2's late alert in miniature: a's tainted result reached the root r,
    which fanned out to b (tainted) and c (healthy); b scheduled a protected
    actuator command (due at ``due``) and ended before the alert at 20."""
    harness, control, trace, ids = late_effect_setup(
        tmp_path, mode, seed, due, latency_ms, **config
    )
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    return harness, control, decision, ids["effect"]


def late_effect_setup(tmp_path, mode, seed, due, latency_ms, control=None, **settings):
    from aggregator.containment_monitor.config import MonitorConfig
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"r": frozenset({"a", "b", "c"}), "a": frozenset(), "b": frozenset()},
        effect_scopes={
            "r": frozenset(),
            "a": frozenset(),
            "b": frozenset({"device.command"}),
            "c": frozenset({"device.command"}),
        },
        protected_effect_classes=frozenset({"device.command"}),
        agent_nodes={"r": "n0", "a": "n1", "b": "n2", "c": "n2"},
        session_isolated_agents=frozenset({"a", "b", "c"}),
        default_control_latency_ms=latency_ms,
        phase1_mode=mode,
        **settings,
    )
    trace = TraceBuilder()
    root = trace.start(trace.root("r", "n0", 0), 1)
    task_a = trace.dispatch(root, "a", "n1", 2)
    trace.arrive(task_a, 3)
    a = trace.start(task_a, 4)
    trace.complete(a, 6)
    trace.read_result(root, task_a, 8)
    task_b = trace.dispatch(root, "b", "n2", 9)
    task_c = trace.dispatch(root, "c", "n2", 9)
    for task_id in (task_b, task_c):
        trace.arrive(task_id, 10)
    b, _ = (trace.start(t, 11) for t in (task_b, task_c))
    effect = trace.effect(b, "device.command", "requested", 12, scheduled=due)
    trace.complete(b, 13)
    control = control or FakeControlClient(delay=jittered(seed))
    harness = Harness(tmp_path, config, control)
    harness.feed(trace.events)
    harness.at(20)
    ids = {"a": inv(a), "root": root, "effect": effect["attributes"]["effect_id"]}
    return harness, control, trace, ids


def test_a_pending_effect_whose_class_is_revoked_is_refused_at_its_commit(tmp_path):
    """The gateway's commit re-check applies decisions in force: with b's
    device class revoked, b's tainted scheduled command no longer reaches its
    sink in G_C (and no deadline makes phase 1 freeze for it)."""
    from aggregator.containment_monitor.model import revoke_grant

    harness, _, _, ids = late_effect_setup(tmp_path, "adaptive", 0, 60_000, 50.0)
    harness.monitor.graph.mark_roots([ids["a"]])
    sink = f"fx:{ids['effect']}"
    open_cg = harness.monitor._project(2, frozenset())
    assert sink in open_cg.sinks and sink in open_cg.closure()
    grant = revoke_grant("n2", "b", "fx:device.command")
    cut_cg = harness.monitor._project(2, frozenset({grant}))
    assert sink not in cut_cg.closure(removed={grant})
    assert cut_cg.eat.get(sink, math.inf) == math.inf
    harness.close()


def frontier_shape(decision) -> Counter:
    return Counter(
        (r.action.op, r.action.node_id, r.action.kind) for r in decision.phase1
    )


@pytest.mark.parametrize("mode", ["freeze", "freeze_plus_cut", "adaptive"])
@pytest.mark.parametrize("seed", range(3))
def test_every_phase_one_mode_blocks_the_late_effect_and_releases_after_phase_two(
    tmp_path, mode, seed
):
    """In every mode the scheduled effect is blocked from the first round on,
    at every instant the nodes applied, and every release (holds only) still
    waits for the confirmed phase-2 cut."""
    from aggregator.containment_monitor.model import fx, hold_effect

    harness, control, decision, effect_id = late_effect(tmp_path, mode, seed)
    assert decision.phase1_mode == mode and decision.status == "contained"
    phases = [s["phase"] for s in decision.solves]
    assert phases[0] == (2 if mode == "freeze" else 1) and phases[-1] == 2
    assert set(phases[1:]) <= {2}
    first_round = {r.action for r in decision.phase1}
    if mode != "freeze":
        at_alert = {Action.from_json(a) for a in decision.solves[0]["actions"]}
        assert at_alert <= first_round
    assert hold_effect("n2", effect_id) in first_round
    # Replay what the nodes applied: from the first round's convergence on,
    # the effect's sink is separated from the sources at every instant.
    placed = {r.action for r in decision.phase1 + decision.phase2}
    instance = harness.monitor._project(2, frozenset(placed)).instance()
    sink = {fx(effect_id)}
    assert sink <= instance.sinks
    in_force: set[Action] = set()
    converged = False
    for _, op, (node, kind, object_id, _) in sorted(control.timeline):
        action = Action(
            "hold" if op in {"hold", "release"} else "revoke", node, kind, object_id
        )
        if op == "release":
            in_force.discard(action)
        else:
            in_force.add(action)
        converged = converged or first_round <= in_force
        if converged:
            assert solver.separated(instance, in_force, sink), (seed, op, action)
    assert converged
    last_confirmation = max(r.t_confirmed for r in decision.phase2)
    assert all(r.action.op == "hold" for r in decision.released)
    assert all(r.t_release_sent >= last_confirmation for r in decision.released)
    harness.close()


def test_the_cut_modes_add_the_at_alert_cut_to_the_frontier_freeze(tmp_path):
    harness, _, freeze, _ = late_effect(tmp_path / "freeze", "freeze")
    harness.close()
    harness, _, both, _ = late_effect(tmp_path / "both", "freeze_plus_cut")
    harness.close()
    # The two runs differ in identifiers only: compare what the actions are.
    assert not frontier_shape(freeze) - frontier_shape(both)


def test_adaptive_phase_one_freezes_only_when_the_cut_is_not_safely_timely(
    tmp_path,
):
    """Adaptive: a timely at-alert cut goes out alone with the labels of its
    source side; an effect due before any control can land (untimely) adds
    the frontier freeze."""
    harness, _, timely, _ = late_effect(tmp_path / "timely", "adaptive")
    trigger = timely.phase1_trigger
    assert trigger["freeze"] is False and trigger["untimely_effects"] == []
    assert trigger["earliest_effect_deadline_ms"] > (
        trigger["landing_ms"] + trigger["margin_ms"]
    )
    at_alert = {Action.from_json(a) for a in timely.solves[0]["actions"]}
    issued = {r.action for r in timely.phase1}
    # B_a and the labels of its source side (admission holds), no freeze.
    assert at_alert <= issued
    assert all(
        a.op == "hold" and a.kind in {"egress", "task"} for a in issued - at_alert
    )
    assert timely.phase1_trigger["checks"] == [
        {"C1": True, "C2": True, "C4": True, "blocking_sinks": []}
    ]
    harness.close()
    harness, _, untimely, effect_id = late_effect(
        tmp_path / "untimely", "adaptive", due=25, latency_ms=200.0
    )
    trigger = untimely.phase1_trigger
    assert trigger["freeze"] is True
    assert trigger["untimely_effects"] == [f"fx:{effect_id}"]
    harness.close()
    # The freeze it added is the frontier freeze of the plain mode.
    harness, _, plain, _ = late_effect(
        tmp_path / "plain", "freeze", due=25, latency_ms=200.0
    )
    assert not frontier_shape(plain) - frontier_shape(untimely)
    harness.close()
    # Near the margin: an effect due within two control rounds of the landing.
    harness, _, near, _ = late_effect(
        tmp_path / "near", "adaptive", due=20 + 50 + 60, latency_ms=50.0
    )
    assert near.phase1_trigger["freeze"] is True
    assert near.phase1_trigger["untimely_effects"] == []
    harness.close()


def test_receding_additions_during_the_waits_are_fenced_before_any_release(
    tmp_path,
):
    """receding_during_waits: a taint advance seen while the protocol waits
    for ingest (the tainted root dispatched once more before its hold landed)
    adds the strategy's new actions at once, additions only; they are fenced
    before the phase-2 release, which still follows every confirmation."""
    harness, _, decision, _ = late_effect(tmp_path / "off", "freeze")
    assert decision.receding == [] and not decision.receding_during_waits
    harness.close()

    harness, _, trace, ids = late_effect_setup(
        tmp_path / "on", "freeze", 0, 60_000, 50.0, receding_during_waits=True
    )
    original = harness.monitor.fence
    late: dict[str, str] = {}

    def fence(records, on_poll=None):
        if not late:  # the first wait: r's dispatch raced its hold
            seen = len(trace.events)
            late["task"] = trace.dispatch(ids["root"], "c", "n2", 21)
            trace.arrive(late["task"], 22)
            for event in trace.events[seen:]:
                ingest(harness.core, event)
        return original(records, on_poll=on_poll)

    harness.monitor.fence = fence
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    assert decision.receding_during_waits and decision.receding
    assert any(late["task"] in label for label in decision.receding)
    added = [
        r
        for r in decision.phase1 + decision.phase2
        if r.action.label() in decision.receding
    ]
    assert added and all(r.state == "in_force" and r.t_fenced for r in added)
    assert decision.status == "contained"
    latest = max(r.t_confirmed for r in [*decision.phase2, *added])
    assert all(r.t_release_sent >= latest for r in decision.released)
    harness.close()


# ---------------------------------------------------------------------------
# Adaptive phase 1: the convergence check (C1)-(C4) and (J') (theory 8.6)
# ---------------------------------------------------------------------------
def test_a_failed_convergence_check_adds_the_frontier_freeze(tmp_path):
    from aggregator.containment_monitor.model import hold_effect

    """(C4): before B_a landed, the tainted root dispatched once more and the
    new task scheduled a protected effect and ended; once that is ingested,
    the in-force set is no longer a cut (no label can cover a pending effect
    whose issuer ended): the check guards the effect's last hop and passes.
    Where no action can reach that sink (the node offers no effect hold), the
    check fails, the freeze is added and the check repeated. (C1): a fence that
    times out fails the check too."""
    harness, _, trace, ids = late_effect_setup(
        tmp_path / "c4", "adaptive", 0, 60_000, 50.0
    )
    original = harness.monitor.fence
    late: dict[str, str] = {}

    def fence(records, **options):
        if not late:  # r's new dispatch raced B_a; its journal arrives now
            seen = len(trace.events)
            late["task"] = trace.dispatch(ids["root"], "c", "n2", 21)
            trace.arrive(late["task"], 22)
            attempt = trace.start(late["task"], 23)
            late["effect"] = trace.effect(
                attempt, "device.command", "requested", 24, scheduled=60_000
            )["attributes"]["effect_id"]
            trace.complete(attempt, 25)
            for event in trace.events[seen:]:
                ingest(harness.core, event)
        return original(records, **options)

    harness.monitor.fence = fence
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    first = decision.phase1_trigger["checks"][0]
    assert first["blocked_by"] == [f"fx:{late['effect']}"]
    assert first["guard"] == [hold_effect("n2", late["effect"]).label()]
    assert first["C4"] and first["blocking_sinks"] == []
    assert hold_effect("n2", late["effect"]) in {r.action for r in decision.phase1}
    assert decision.status == "contained"
    harness.close()

    no_effect_hold = FakeControlClient(
        {
            "n0": AGENTD_CAPABILITIES,
            "n1": AGENTD_CAPABILITIES,
            "n2": AGENTD_CAPABILITIES - {"hold:effect"},
        }
    )
    harness, _, decision, effect_id = late_effect(
        tmp_path / "unreachable", "adaptive", control=no_effect_hold
    )
    checks = decision.phase1_trigger["checks"]
    assert checks[0]["C4"] is False
    assert checks[0]["blocking_sinks"] == [f"fx:{effect_id}"]
    # Untimely at the alert already: the freeze went out with B_a; the check
    # still fails, round after round, and nothing is released.
    assert decision.phase1_trigger["freeze"] and len(checks) > 1
    assert not any(c["C4"] for c in checks)
    assert decision.status == "phase2_check_failed" and decision.released == []
    harness.close()

    harness, _, _, ids = late_effect_setup(tmp_path / "c1", "adaptive", 0, 60_000, 50.0)
    original = harness.monitor.fence
    calls = {"n": 0}

    def slow(records, **options):
        calls["n"] += 1
        return original(records, **options) and calls["n"] > 1

    harness.monitor.fence = slow
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    first = decision.phase1_trigger["checks"][0]
    assert first["C1"] is False and decision.phase1_trigger["freeze_after_check"]
    harness.close()


def test_conservative_reconciliation_keeps_a_stale_journal_from_releasing(tmp_path):
    """Theory 8.6 (2d) / T11 with injected ingest delays: x runs on a slow node
    and has dispatched y (fast node), which dispatched z (fast node), which
    scheduled a protected effect. The fast nodes' journals are ingested, the
    slow node's (x's dispatch of y) is not. Under (J') the known use of y's
    request, whose producer is x, taints y and so z: B_a holds z's effect at
    the alert and no phase-2 release lifts it, before or after the slow journal
    arrives."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import hold_effect, hold_task
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"x": frozenset({"y"}), "y": frozenset({"z"}), "z": frozenset()},
        effect_scopes={"z": frozenset({"device.command"})},
        protected_effect_classes=frozenset({"device.command"}),
        agent_nodes={"x": "slow", "y": "fast-1", "z": "fast-2"},
        control_latency_ms={"slow": 400, "fast-1": 5, "fast-2": 5},
        session_isolated_agents=frozenset({"x", "y", "z"}),
        phase1_mode="adaptive",
    )
    trace = TraceBuilder()
    x = trace.start(trace.root("x", "slow", 0), 1)
    task_y = trace.dispatch(x, "y", "fast-1", 5)
    trace.arrive(task_y, 6)
    y = trace.start(task_y, 7)
    task_z = trace.dispatch(y, "z", "fast-2", 8)
    trace.arrive(task_z, 9)
    z = trace.start(task_z, 10)
    effect = trace.effect(z, "device.command", "requested", 11, scheduled=60_000)
    effect_id = effect["attributes"]["effect_id"]
    x_dispatch = [
        e for e in trace.events if e["node_id"] == "slow" and event_ms(e) >= 5
    ]
    harness = Harness(tmp_path, config, FakeControlClient(delay=jittered(1)))
    harness.feed([e for e in trace.events if e not in x_dispatch])
    graph = harness.monitor.graph
    graph.mark_roots([inv(x)])
    # The stale view: y's request has no known producer yet, nothing below x
    # is tainted.
    assert inv(y) not in graph.taint and inv(z) not in graph.taint
    harness.at(20)
    decision = harness.monitor.respond(
        operator_alert(inv(x), harness.now), strategy="firebreak", roots=[inv(x)]
    )
    assert {inv(y), inv(z)} <= set(graph.taint)  # (J')
    # B_a cuts z's effect at the alert (its own hold, or the hold of z, which
    # the gateway honours at commit), and nothing that cuts it is released.
    cuts = {hold_effect("fast-2", effect_id), hold_task("fast-2", task_z)}
    at_alert = {Action.from_json(a) for a in decision.solves[0]["actions"]}
    assert cuts & at_alert and cuts & {r.action for r in decision.phase1}
    assert decision.status == "contained"
    assert cuts & set(harness.monitor.in_force)
    assert not cuts & {r.action for r in decision.released}
    # The slow journal arrives: the exact taint agrees; a re-solve keeps it.
    harness.feed(x_dispatch)
    assert {inv(y), inv(z)} <= set(graph.taint)
    harness.monitor.resolve_frontier()
    assert cuts & set(harness.monitor.in_force)
    harness.close()


def test_phase_two_keeps_literal_masks_beside_the_held_edges(tmp_path):
    """Theory 8.6 (2b) / Prop 5.1: only edges held by confirmed in-force
    actions get d = infinity; an unheld source-side edge keeps its deadline,
    and an action untimely for it stays untimely (the mask binds)."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import hold_task
    from aggregator.containment_monitor.projection import project
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"a": frozenset({"w"}), "b": frozenset({"w"})},
        agent_nodes={"a": "n0", "b": "n1", "w": "n2"},
        session_isolated_agents=frozenset({"a", "b"}),
        default_control_latency_ms=1000,
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    task_a, task_b = trace.root("a", "n0", 0), trace.root("b", "n1", 0)
    a, b = trace.start(task_a, 1), trace.start(task_b, 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(a), inv(b)])
    harness.at(100)
    held = hold_task("n0", task_a)
    cg = project(
        harness.monitor.graph,
        config,
        now_ms=harness.now,
        capabilities=lambda node: AGENTD_CAPABILITIES,
        in_force=[held],
    )
    behind_a = [
        e for e in cg.edges if e.action is not None and e.tail.startswith(f"out:{a}")
    ]
    assert behind_a and all(e.deadline == float("inf") for e in behind_a)
    assert not any(e.tail.startswith(f"early:{a}") for e in cg.edges)
    # b emits before any control lands: its immediate emissions keep finite
    # deadlines, and the actions on them stay untimely.
    early_b = [
        e for e in cg.edges if e.action is not None and e.tail.startswith(f"early:{b}")
    ]
    assert early_b and all(e.deadline < float("inf") for e in early_b)
    assert not any(e.feasible for e in early_b)
    harness.close()


def test_forward_closure_labels_cover_every_unit_the_taint_can_reach(tmp_path):
    """phase1_labels="forward_closure": admission holds on the forward closure
    of the sources (a superset of the frontier labels), released like the rest
    of phase 1 only after the confirmed phase-2 cut."""
    harness, _, frontier, _ = late_effect(tmp_path / "frontier", "freeze")
    harness.close()
    harness, _, closure, _ = late_effect(
        tmp_path / "closure", "freeze", phase1_labels="forward_closure"
    )
    assert closure.phase1_labels == "forward_closure"
    assert frontier.phase1_labels == "frontier"
    assert not frontier_shape(frontier) - frontier_shape(closure)
    last = max(r.t_confirmed for r in closure.phase2)
    assert all(r.t_release_sent >= last for r in closure.released)
    assert closure.status == "contained"
    harness.close()


# ---------------------------------------------------------------------------
# Convergence and release checks cover every protected sink, doomed or not
# ---------------------------------------------------------------------------
def test_a_doomed_sink_reachable_through_an_unheld_edge_blocks_convergence(
    tmp_path,
):
    """The late effect is due before any control can land: the deadline-aware
    graph judges it unavoidable, and no phase-1 block holds the edge into it.
    Ignoring unavoidable sinks would declare phase 1 converged while the
    effect is still reachable; the check instead reports it as the blocking
    sink, guards its last hop, and converges only once that is in force.
    Releases still follow the confirmed phase-2 cut."""
    from aggregator.containment_monitor.model import fx, hold_effect

    harness, _, _, ids = late_effect_setup(
        tmp_path, "freeze", 0, due=25, latency_ms=200.0
    )
    effect = fx(ids["effect"])
    harness.monitor.graph.mark_roots([ids["a"]])
    at_alert = harness.monitor._project(2, frozenset()).instance()
    assert effect in solver.unavoidable_sinks(at_alert)  # doomed at the alert
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    first, guarded, *_ = decision.convergence
    assert (first["phase"], first["converged"]) == (1, False)
    assert first["blocking_sinks"] == [effect]
    assert guarded["guard"] == [hold_effect("n2", ids["effect"]).label()]
    assert guarded["converged"] and guarded["blocking_sinks"] == []
    assert hold_effect("n2", ids["effect"]) in harness.monitor.in_force
    assert decision.status == "contained"
    last = max(r.t_confirmed for r in decision.phase2)
    assert all(r.t_release_sent >= last for r in decision.released)
    assert decision.to_json()["convergence"] == decision.convergence
    harness.close()


def test_a_sink_no_action_can_reach_keeps_every_hold(tmp_path):
    """No node offers an action on the edge into the effect: phase 1 never
    converges, the release re-check fails on the same sink, and every hold
    stays in force."""
    from aggregator.containment_monitor.model import fx

    control = FakeControlClient(
        {
            "n0": AGENTD_CAPABILITIES,
            "n1": AGENTD_CAPABILITIES,
            "n2": AGENTD_CAPABILITIES - {"hold:effect"},
        }
    )
    harness, _, decision, effect_id = late_effect(tmp_path, "freeze", control=control)
    assert decision.status == "phase2_check_failed"
    assert decision.released == []
    assert all(not c["converged"] for c in decision.convergence)
    assert decision.convergence[-1]["phase"] == 2
    assert decision.convergence[-1]["blocking_sinks"] == [fx(effect_id)]
    held = {r.action for r in decision.phase1 if r.state == "in_force"}
    assert held and held <= set(harness.monitor.in_force)
    harness.close()


def test_phase_two_sees_an_admission_label_in_force_as_held(tmp_path):
    """A phase-1 admission label (hold:egress) in force stops the egress in
    the phase-2 graph too: the edge carries the hold, flows behind it have
    d = infinity, and the label separates them."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import hold_egress, revoke_egress
    from aggregator.containment_monitor.projection import project
    from containment_support import TraceBuilder

    config = MonitorConfig(
        grants={"a": frozenset({"w"})},
        agent_nodes={"a": "n0", "w": "n1"},
        session_isolated_agents=frozenset({"a"}),
        min_emission_ms={"a": 1000},
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    task_a = trace.root("a", "n0", 0)
    a = trace.start(task_a, 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(a)])
    harness.at(10)
    label = hold_egress("n0", task_a)
    graphs = {
        name: project(
            harness.monitor.graph,
            config,
            now_ms=harness.now,
            capabilities=lambda node: AGENTD_CAPABILITIES,
            in_force=in_force,
        )
        for name, in_force in (("free", []), ("labelled", [label]))
    }
    egress = {
        name: [e for e in cg.edges if e.kind == "egress"] for name, cg in graphs.items()
    }
    assert {e.action for e in egress["free"]} == {revoke_egress("n0", task_a)}
    assert {e.action for e in egress["labelled"]} == {label}
    cg = graphs["labelled"]
    behind = [
        e
        for e in cg.edges
        if e.action is not None and e.kind not in {"egress", "split"}
    ]
    assert behind and all(e.deadline == float("inf") for e in behind)
    assert solver.separated(cg.instance(), [label])
    harness.close()


def test_receding_additions_must_be_confirmed_before_any_release(tmp_path):
    """An action the receding trigger added during a wait that is not
    confirmed keeps every hold: what it was to stop may still flow."""
    late: dict[str, str] = {}
    control = FakeControlClient(
        fail=lambda node, op, object_id: (
            "node_unreachable" if object_id == late.get("task") else None
        )
    )
    harness, _, trace, ids = late_effect_setup(
        tmp_path,
        "freeze",
        0,
        60_000,
        50.0,
        control=control,
        receding_during_waits=True,
    )
    original = harness.monitor.fence

    def fence(records, on_poll=None):
        if not late:
            seen = len(trace.events)
            late["task"] = trace.dispatch(ids["root"], "c", "n2", 21)
            trace.arrive(late["task"], 22)
            for event in trace.events[seen:]:
                ingest(harness.core, event)
        return original(records, on_poll=on_poll)

    harness.monitor.fence = fence
    decision = harness.monitor.respond(
        operator_alert(ids["a"], harness.now), strategy="firebreak", roots=[ids["a"]]
    )
    assert any(late["task"] in label for label in decision.receding)
    added = [r for r in decision.phase1 if r.action.label() in decision.receding]
    assert any(r.state == "failed" for r in added)
    # Never released: the added action is not in force (phase 2, which wants
    # it too, finds it refused again, or the check on additions stops it).
    assert decision.status in {"receding_unconfirmed", "phase2_unconfirmed"}
    assert decision.released == []
    harness.close()
