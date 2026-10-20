"""Two-phase enforcement, fencing and the control clients."""

import random
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from aggregator.containment_monitor import solver
from aggregator.containment_monitor.control import (
    ControlError,
    FakeControlClient,
    LocalAgentdControlClient,
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
    assert [r.error for r in decision.phase2] == ["node_unreachable"]
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
        control.place_hold(
            "n0", "task", str(uuid4()), owner="core-monitor", policy_version=2
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


def test_local_agentd_client_places_fenced_holds(nodes):
    root, _ = nodes
    client = LocalAgentdControlClient({"root-node": transport(root)})
    task_id = str(uuid4())
    confirmation = client.place_hold(
        "root-node", "task", task_id, owner="core-monitor", policy_version=2
    )
    assert confirmation.applied_at_ms is not None
    assert [h["object_id"] for h in root.store.list_holds()] == [task_id]
    with pytest.raises(ControlError) as stale:
        client.release_hold(
            "root-node", "task", task_id, owner="core-monitor", policy_version=1
        )
    assert stale.value.code == "stale_policy_version"
    client.release_hold(
        "root-node", "task", task_id, owner="core-monitor", policy_version=3
    )
    assert root.store.list_holds() == []
    for call in (
        lambda: client.place_hold(
            "elsewhere", "task", task_id, owner="core-monitor", policy_version=4
        ),
        lambda: client.revoke(
            "root-node", "grant", "parent>child", owner="core-monitor", policy_version=4
        ),
        lambda: client.place_hold(
            "root-node", "effect", task_id, owner="core-monitor", policy_version=4
        ),
    ):
        with pytest.raises(ControlError):
            call()


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
        {"root-node": transport(root), "worker-node": transport(worker)}
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
    held = {h["object_id"] for h in root.store.list_holds()}
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
