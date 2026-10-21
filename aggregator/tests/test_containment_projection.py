"""Control graph projection: authority mirror, star hubs, soundness and splits."""

import heapq
import random
import sys
from collections import Counter
from pathlib import Path

import pytest

from aggregator.containment_monitor import solver
from aggregator.containment_monitor.config import Mechanisms, MonitorConfig
from aggregator.containment_monitor.control import FakeControlClient, execute
from aggregator.containment_monitor.model import (
    Action,
    hold_egress,
    hold_task,
    inv,
    revoke_admission,
    revoke_egress,
)
from aggregator.containment_monitor.projection import live_units, project, ScopeMirror
from aggregator.containment_monitor.strategies import (
    Context,
    firebreak,
    with_mechanisms,
)
from containment_support import BASE_MS, Harness, TraceBuilder, ingest

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "agent-runtime/tests/agentd")
)
from containment_test_support import Node, deliver  # noqa: E402
from test_containment_provenance import journal  # noqa: E402

ALL = FakeControlClient().capabilities


def supported_everywhere(action):
    return True


def gc(harness, now, **kwargs):
    harness.at(now)
    return project(
        harness.monitor.graph,
        harness.config,
        now_ms=harness.now,
        capabilities=kwargs.pop("capabilities", ALL),
        **kwargs,
    )


def reachable(cg, predicate):
    """Earliest arrival over reachable nodes matching predicate (inf if none)."""
    times = [
        t - cg.t_now
        for name, t in cg.eat.items()
        if predicate(name, cg.nodes.get(name, {}))
    ]
    return min(times, default=float("inf"))


# ---------------------------------------------------------------------------
def test_scope_mirror_reads_the_authority_agentd_exports(tmp_path):
    """run/started carries grant ∩ inherited; it wins over a stale configured
    mirror, and an unstarted task is bounded by its dispatcher's export."""
    actuator = {"effect_class": "device.command", "target": "actuator/*"}
    root = Node(tmp_path / "root", "root-node")
    worker = Node(tmp_path / "worker", "worker-node")
    root.managed("parent", outbound=["child", "other"], effects=[actuator])
    worker.managed("child", outbound=["parent", "other"], effects=[actuator])
    stale = MonitorConfig(
        grants={"parent": frozenset({"child"}), "child": frozenset({"parent"})},
        effect_scopes={"child": frozenset()},
        protected_effect_classes=frozenset({"device.command"}),
    )
    harness = Harness(tmp_path / "monitor", stale)
    try:
        root.submit("parent")
        _, binding = root.start("parent")
        root.dispatch("parent", binding, "child")
        deliver(root, worker)
        _, child_binding = worker.start("child")
        denied = worker.dispatch("child", child_binding, "parent")
        allowed = worker.dispatch("child", child_binding, "other")
        assert (denied["status"], allowed["status"]) == ("error", "ok")
        for event in journal(root) + journal(worker):
            ingest(harness.core, event)
        harness.monitor.catch_up()
        graph = harness.monitor.graph
        units = live_units(graph, stale, harness.now, ScopeMirror(graph, stale))
        scopes = {
            unit.agent: (unit.recipients, unit.effects) for unit in units.values()
        }
        # agentd: child = grant(child) ∩ scope(parent) = {parent, other} ∩ {child, other}
        assert scopes == {
            "parent": ({"child", "other"}, {"device.command"}),
            "child": ({"other"}, {"device.command"}),
            # Not started: no configured grant, bounded by the child's export.
            "other": ({"other"}, {"device.command"}),
        }
    finally:
        harness.close()
        root.close()
        worker.close()


def test_star_hubs_keep_potential_edges_linear(tmp_path):
    config = MonitorConfig(grants={"r": frozenset(f"w{i}" for i in range(30))})
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    attempts = [trace.start(trace.root("r", "n0", i), i + 1) for i in range(20)]
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(a) for a in attempts])
    cg = gc(harness, 100, static=True)
    # 20 invocations x 30 targets would be 600 junctions; one hub serves them all:
    # 2 spoke edges per invocation, 4 gate-path edges per target, and 2 edges per
    # tainted conversation (each root task here is its own context).
    assert cg.stats["hubs"] == 1
    assert cg.stats["potential_edges"] == 2 * 20 + 4 * 30 + 2 * 20
    harness.close()


# ---------------------------------------------------------------------------
# Soundness: everything that later happens is reachable no later than it happens.
# ---------------------------------------------------------------------------
AGENT_NODES = {"r": "n0", "a": "n1", "b": "n1", "c": "n2", "d": "n2"}
DELIVERY, MIN_START, MIN_EMIT = 20, 10, 5


def workload(rng: random.Random):
    grants = {
        "r": frozenset(rng.sample(["a", "b", "c", "d"], 3)),
        "a": frozenset(rng.sample(["b", "c", "d", "r"], 2)),
        "b": frozenset(rng.sample(["c", "d", "a"], 2)),
        "c": frozenset({"d"}),
        "d": frozenset(),
    }
    effects = {
        "r": frozenset({"device.command", "network.http"}),
        "a": frozenset({"device.command", "network.http"}),
        "c": frozenset({"device.command"}),
        "d": frozenset({"device.command"}),
        "b": frozenset({"network.http"}),
    }
    config = MonitorConfig(
        grants=grants,
        effect_scopes=effects,
        protected_effect_classes=frozenset({"device.command", "network.http"}),
        agent_nodes=AGENT_NODES,
        default_delivery_ms=DELIVERY,
        min_start_ms=MIN_START,
        default_min_emission_ms=MIN_EMIT,
        default_control_latency_ms=rng.choice([0, 10, 40]),
    )
    trace = TraceBuilder()
    plan: list = []
    order = [0]
    scope, fx_scope = {}, {}
    live = set()

    def at(time, action, *args):
        order[0] += 1
        heapq.heappush(plan, (time, order[0], action, args))

    def started(attempt, agent, inherited, inherited_fx, time):
        # agentd attenuates both: a child never exceeds what its parent could do.
        scope[attempt] = grants[agent] & inherited
        fx_scope[attempt] = effects.get(agent, frozenset()) & inherited_fx
        live.add(attempt)
        for _ in range(rng.randint(0, 2)):
            if scope[attempt]:
                at(
                    time + MIN_EMIT + rng.randint(0, 60),
                    "dispatch",
                    attempt,
                    rng.choice(sorted(scope[attempt])),
                )
        if fx_scope[attempt] and rng.random() < 0.7:
            at(
                time + MIN_EMIT + rng.randint(0, 50),
                "effect",
                attempt,
                min(fx_scope[attempt]),
            )
        # Delegating invocations tend to outlive their children and read results.
        at(
            time + MIN_EMIT + rng.randint(40, 260 if scope[attempt] else 140),
            "complete",
            attempt,
        )

    root_task = trace.root("r", "n0", 0)
    at(
        1,
        "start",
        root_task,
        frozenset({"a", "b", "c", "d", "r"}),
        config.protected_effect_classes,
    )
    while plan:
        time, _, action, args = heapq.heappop(plan)
        if action == "start":
            task_id, inherited, inherited_fx = args
            started(
                trace.start(task_id, time),
                trace.tasks[task_id]["agent"],
                inherited,
                inherited_fx,
                time,
            )
        elif action == "dispatch":
            attempt, target = args
            if attempt not in live:
                continue
            child = trace.dispatch(attempt, target, AGENT_NODES[target], time)
            sender_node = trace.attempts[attempt]["node"]
            arrival = (
                time
                + (DELIVERY if AGENT_NODES[target] != sender_node else 0)
                + rng.randint(0, 20)
            )
            at(arrival, "arrive", child)
            at(
                arrival + MIN_START + rng.randint(0, 15),
                "start",
                child,
                scope[attempt],
                fx_scope[attempt],
            )
        elif action == "arrive":
            trace.arrive(args[0], time)
        elif action == "effect":
            if args[0] in live:
                trace.effect(args[0], args[1], "requested", time, scheduled=time + 30)
        elif action == "complete":
            attempt = args[0]
            if attempt not in live:
                continue
            live.discard(attempt)
            trace.complete(attempt, time)
            parent = trace.tasks[trace.attempts[attempt]["task"]]["sender"]
            if parent is not None:
                read = time + (
                    DELIVERY
                    if trace.attempts[parent]["node"] != trace.attempts[attempt]["node"]
                    else 0
                )
                at(
                    read + rng.randint(0, 20),
                    "read",
                    parent,
                    trace.attempts[attempt]["task"],
                )
        elif action == "read":
            if args[0] in live:
                trace.read_result(args[0], args[1], time)
    return trace, config


def event_ms(event):
    from aggregator.containment_monitor.provenance import occurred_ms

    return occurred_ms(event["occurred_at"]) - BASE_MS


def soundness_checks(root_dir, seed: int) -> Counter:
    """Every flow that taints something after the alert must be reachable in
    G_C(t_alert) no later than it happens. Flows that originate in an invocation
    created after the alert lie beyond the h = 1 boundary of its requester."""
    rng = random.Random(seed)
    trace, config = workload(rng)
    t_alert = rng.randint(30, 160)
    started = [
        e["execution_attempt_id"]
        for e in trace.events
        if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
    ]
    root = inv(rng.choice(started))
    now = Harness(root_dir / f"now-{seed}", config)
    now.feed([e for e in trace.events if event_ms(e) <= t_alert])
    graph = now.monitor.graph
    graph.mark_roots([root])
    running, pending = set(graph.running), set(graph.pending)
    sessions = set(graph.tainted_sessions)
    before = dict(graph.taint)
    cg = gc(now, t_alert)
    full = Harness(root_dir / f"full-{seed}", config)
    full.feed(trace.events)
    # Taint times relative to the trace start (roots are tainted from the beginning).
    truth = {
        n: max(0, t - BASE_MS) for n, t in full.monitor.graph.closure([root]).items()
    }
    known = {
        a for a in trace.attempts if trace.attempts[a]["task"] in pending
    } | running
    counts: Counter = Counter()

    def assert_reached(predicate, time, kind):
        assert reachable(cg, predicate) <= time - t_alert, (seed, kind)
        counts[kind] += 1

    for event in trace.events:
        time = event_ms(event)
        attempt = event["execution_attempt_id"]
        if time <= t_alert or attempt is None:
            continue
        node, agent = inv(attempt), event["agent_id"]
        tainted_now = node in truth and truth[node] <= time
        if (
            event["kind"] == "run"
            and event["phase"] == "started"
            and node in truth
            and node not in before
        ):
            task_id = event["task_id"]
            if task_id in pending:
                if truth[node] == time:
                    assert (
                        cg.eat.get(f"in:task:{task_id}", float("inf")) <= BASE_MS + time
                    ), seed
                    counts["start of pending task"] += 1
            elif (
                trace.tasks[task_id]["sender"] in running
                or (agent, event["context_id"]) in sessions
            ):
                assert_reached(
                    lambda n, a: a.get("kind") == "arrival" and a.get("agent") == agent,
                    time,
                    "new invocation",
                )
            else:
                counts["beyond horizon"] += 1
        elif (
            attempt in known
            and tainted_now
            and event["kind"] == "dispatch"
            and event["phase"] == "allowed"
        ):
            target = event["attributes"]["recipient_id"]
            assert_reached(
                lambda n, a: n.startswith(f"A:{agent}:{target}"), time, "dispatch"
            )
        elif attempt in known and tainted_now and event["kind"] == "effect":
            effect_class = event["attributes"]["effect_class"]
            assert_reached(
                lambda n, a: n.startswith(f"K:{agent}:{effect_class}"), time, "effect"
            )
        elif (
            event["kind"] == "lineage"
            and event["phase"] == "used"
            and attempt in running
            and node not in before
            and node in truth
            and truth[node] == time
        ):
            assert cg.eat.get(f"in:{attempt}", float("inf")) <= BASE_MS + time, seed
            counts["result read"] += 1
    now.close()
    full.close()
    return counts


def test_projection_is_a_sound_over_approximation(tmp_path):
    totals: Counter = Counter()
    for seed in range(60):
        totals += soundness_checks(tmp_path, seed)
    # Not vacuous: every kind of flow was checked many times.
    for kind in (
        "new invocation",
        "dispatch",
        "effect",
        "result read",
        "start of pending task",
    ):
        assert totals[kind] >= 3, totals


# ---------------------------------------------------------------------------
# Deadline splits
# ---------------------------------------------------------------------------
def test_early_window_copies_keep_later_emissions_stoppable(tmp_path):
    """Alert at 100: the tainted invocation may emit at once, while its own controls
    land at 110 (n0). Admission at the target lands at 108 (n1) and a dispatch
    takes 5 ms, so emissions before 103 arrive too early and later ones can still
    be refused: the early window splits at 104 and only its first part is doomed."""
    config = MonitorConfig(
        grants={"r": frozenset({"w"})},
        agent_nodes={"r": "n0", "w": "n1"},
        control_latency_ms={"n0": 10, "n1": 8},
        tau_ms=0,
        default_delivery_ms=5,
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    attempt = trace.start(trace.root("r", "n0", 0), 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(attempt)])
    cg = gc(harness, 100)
    windows = sorted(n for n in cg.nodes if n.startswith(f"early:{attempt}#"))
    assert len(windows) == 2
    admissions = {
        e.tail: (e.deadline - cg.t_now, e.feasible)
        for e in cg.edges
        if e.kind == "admission" and "ctx:" not in e.tail
    }
    assert admissions == {
        f"Fin:r>w@early:{attempt}#0": (5, False),
        f"Fin:r>w@early:{attempt}#1": (9, True),
        "Fin:r>w": (16, True),
    }
    result = solver.solve(cg.instance())
    assert {s for s in result.unavoidable if "ctx:" not in s} == {
        f"Fout:r>w@early:{attempt}#0"
    }
    assert revoke_admission("n1", "w") in result.actions
    assert solver.separated(
        cg.instance(), result.actions, cg.instance().sinks - result.unavoidable
    )
    harness.close()


def test_doomed_hub_copy_isolates_unstoppable_arrivals(tmp_path):
    """h = 2: new invocations of w requested by r1 and r2 share w's expansion hub.
    Both requesters emit late (at 301), r1's request reaches w at once and r2's
    after 50 ms; w's gates land at 320. Without the doomed copy, the hub's earliest
    arrival (301) would make w's grant revocation look infeasible for r2's too."""
    config = MonitorConfig(
        grants={"r1": frozenset({"w"}), "r2": frozenset({"w"}), "w": frozenset({"z"})},
        agent_nodes={"r1": "n1", "r2": "n2", "w": "n3", "z": "n4"},
        control_latency_ms={"n1": 10, "n2": 10, "n3": 220, "n4": 10},
        delivery_ms={"n1>n3": 0, "n2>n3": 50},
        min_emission_ms={"r1": 300, "r2": 300},
        tau_ms=0,
        horizon=2,
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    first = trace.start(trace.root("r1", "n1", 0), 1)
    second = trace.start(trace.root("r2", "n2", 0), 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(first), inv(second)])
    cg = gc(harness, 100)
    assert cg.stats["doomed_moves"] == 1
    gates = {
        e.tail: (e.deadline - BASE_MS, e.feasible)
        for e in cg.edges
        if e.kind == "gate" and e.tail.startswith("A:w:z")
    }
    assert gates == {"A:w:z@new": (351, True), "A:w:z@new@doomed": (301, False)}
    harness.close()


def test_horizon_and_boundary_modes(tmp_path):
    base = dict(
        grants={"r": frozenset({"w"}), "w": frozenset({"z"})},
        agent_nodes={"r": "n0", "w": "n1", "z": "n2"},
        session_isolated_agents=frozenset({"r"}),
    )
    sizes = {}
    for name, extra in {
        "h1": {},
        "h2": {"horizon": 2},
        "defer": {"boundary": "defer"},
    }.items():
        harness = Harness(tmp_path / name, MonitorConfig(**base, **extra))
        trace = TraceBuilder()
        attempt = trace.start(trace.root("r", "n0", 0), 1)
        harness.feed(trace.events)
        harness.monitor.graph.mark_roots([inv(attempt)])
        cg = gc(harness, 100, static=True)
        sizes[name] = (
            cg.stats["boundary"],
            sorted(s for s in cg.sinks if s.startswith("Fout")),
        )
        harness.close()
    assert sizes["h1"] == (1, ["Fout:r>w"])
    assert sizes["h2"] == (1, ["Fout:w>z@new"])  # w's new invocation expanded one level
    assert sizes["defer"] == (0, [])


def test_best_effort_guard_and_taint_aware_admission(tmp_path):
    """Controls land 1000 ms after the alert: the cut pauses r after its early
    window, whose boundary is unavoidable. The guard additionally issues the
    c0-cost admission block on that boundary's last hop; with a deferred boundary
    there is nothing to cut, and taint-aware admission blocks r's egress instead."""
    results = {}
    for name, (guard, admission, boundary) in {
        "plain": (False, False, "sink"),
        "guard": (True, False, "sink"),
        "admission": (False, True, "defer"),
    }.items():
        mechanisms = Mechanisms(True, guard, admission)
        config = MonitorConfig(
            grants={"r": frozenset({"w"})},
            agent_nodes={"r": "n0", "w": "n1"},
            default_control_latency_ms=1000,
            session_isolated_agents=frozenset({"r"}),
            boundary=boundary,
        )
        harness = Harness(tmp_path / name, config)
        trace = TraceBuilder()
        task_id = trace.root("r", "n0", 0)
        attempt = trace.start(task_id, 1)
        harness.feed(trace.events)
        harness.monitor.graph.mark_roots([inv(attempt)])
        cg = gc(harness, 100)
        context = Context(
            harness.monitor.graph,
            config,
            cg,
            lambda: cg,
            [inv(attempt)],
            supported_everywhere,
        )
        result = with_mechanisms(context, firebreak(context), mechanisms)
        results[name] = (set(result.actions), result.unavoidable, task_id, attempt)
        harness.close()
    # Pausing the tainted r and blocking its egress both cost c0 after the window.
    actions, unavoidable, task_id, attempt = results["plain"]
    assert unavoidable == [f"Fout:r>w@early:{attempt}#0"]
    assert actions in ({hold_task("n0", task_id)}, {revoke_egress("n0", task_id)})
    plain = actions
    actions, _, task_id, _ = results["guard"]
    assert {a.kind for a in actions} == {a.kind for a in plain} | {"admission"}
    actions, unavoidable, task_id, _ = results["admission"]
    # With nothing else to cut, taint-aware admission labels the source side
    # (an egress hold: its new events are admitted per event).
    assert (actions, unavoidable) == ({hold_egress("n0", task_id)}, [])


# ---------------------------------------------------------------------------
# Cut property after enforcement
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("seed", range(25))
def test_no_source_reaches_an_avoidable_sink_after_enforcement(tmp_path, seed):
    rng = random.Random(1000 + seed)
    trace, config = workload(rng)
    t_alert = rng.randint(30, 160)
    started = [
        e["execution_attempt_id"]
        for e in trace.events
        if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
    ]
    root = inv(rng.choice(started))
    harness = Harness(tmp_path, config)
    harness.feed([e for e in trace.events if event_ms(e) <= t_alert])
    harness.monitor.graph.mark_roots([root])
    cg = gc(harness, t_alert)
    context = Context(
        harness.monitor.graph, config, cg, lambda: cg, [root], supported_everywhere
    )
    chosen = firebreak(context)
    control = FakeControlClient()
    for version, action in enumerate(chosen.actions, start=1):
        execute(control, action, node_id=action.node_id, policy_version=version)
    in_force = {
        Action(op, node_id, kind, object_id)
        for node_id, node in control.nodes.items()
        for op, applied in (("hold", node.holds), ("revoke", node.revoked))
        for kind, object_id in applied
    }
    assert in_force == set(chosen.actions)
    after = gc(harness, t_alert, in_force=in_force)
    instance = after.instance()
    unavoidable = solver.unavoidable_sinks(instance)
    assert solver.separated(instance, in_force, instance.sinks - unavoidable)
    assert unavoidable <= set(chosen.unavoidable) | {
        s for s in instance.sinks if "@early" in s or "ctx:" in s
    }
    harness.close()


def test_node_local_revocations_reach_every_node_that_enforces_them(tmp_path):
    config = MonitorConfig(
        grants={
            "r": frozenset({"a", "b"}),
            "a": frozenset({"c"}),
            "b": frozenset(),
            "c": frozenset(),
        },
        agent_nodes={"r": "n0", "a": "n1", "b": "n2", "c": "n3"},
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    task_a = trace.dispatch(trace.start(trace.root("r", "n0", 0), 1), "a", "n1", 2)
    harness.feed(trace.events)
    sites = ScopeMirror(harness.monitor.graph, config).sites
    try:
        assert sites(hold_task("n1", task_a)) == ("n1",)
        assert sites(revoke_egress("n1", task_a)) == ("n1",)
        # Sender's node refuses the dispatch, recipient's node parks a published one.
        assert sites(Action("revoke", "n0", "grant", "r>a")) == ("n0", "n1")
        assert sites(Action("revoke", "n0", "grant", "r>fx:device.command")) == ("n0",)
        # Target's node first, then the nodes of every agent that may send to it.
        assert sites(revoke_admission("n3", "c")) == ("n3", "n1")
        # Everywhere the delegation's descendants may run: a, and c below it.
        assert sites(Action("revoke", "n1", "authority", task_a)) == ("n1", "n3")
    finally:
        harness.close()


@pytest.mark.parametrize(("level", "team"), [(0, {"x"}), (1, {"l", "x", "y"})])
def test_stop_team_holds_the_team_the_configured_levels_up(tmp_path, level, team):
    from aggregator.containment_monitor.strategies import stop_team

    config = MonitorConfig(
        agent_nodes={"r": "n0", "l": "n1", "x": "n1", "y": "n1"},
        org_parent={"l": "r", "x": "l", "y": "l"},
        stop_team_level=level,
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    root = trace.start(trace.root("r", "n0", 0), 1)
    lead = trace.start(trace.dispatch(root, "l", "n1", 2), 3)
    tasks = {
        agent: trace.dispatch(lead, agent, "n1", 4 + index)
        for index, agent in enumerate(("x", "y"))
    }
    culprit = trace.start(tasks["x"], 7)
    trace.start(tasks["y"], 8)
    harness.feed(trace.events)
    cg = gc(harness, 10)
    context = Context(
        harness.monitor.graph,
        config,
        cg,
        lambda: cg,
        [inv(culprit)],
        supported_everywhere,
    )
    held = {
        trace.tasks[action.object_id]["agent"] for action in stop_team(context).actions
    }
    assert held == team
    harness.close()


# ---------------------------------------------------------------------------
# Vertex-local vocabulary and per-action timely masks
# ---------------------------------------------------------------------------
def test_vocabulary_is_vertex_local_and_deadlines_are_per_action_masks(tmp_path):
    """No offered action spans a delegation subtree; every action's timely mask
    is judged from its own site, an action in force keeps all of R(a), and the
    cut instance lists exactly the timely masks."""
    from aggregator.containment_monitor.projection import NOT_OFFERED

    partial = 0
    for seed in range(12):
        rng = random.Random(seed)
        trace, config = workload(rng)
        t_alert = rng.randint(30, 160)
        started = [
            e["execution_attempt_id"]
            for e in trace.events
            if e["kind"] == "run" and e["phase"] == "started" and event_ms(e) <= t_alert
        ]
        harness = Harness(tmp_path / f"seed-{seed}", config)
        harness.feed([e for e in trace.events if event_ms(e) <= t_alert])
        harness.monitor.graph.mark_roots([inv(rng.choice(started))])
        cg = gc(harness, t_alert)
        assert not {a.capability for a in cg.actions()} & NOT_OFFERED
        masks = cg.masks()
        instance = cg.instance()
        for action, (removal, timely) in masks.items():
            landing = (
                harness.now + config.tau_ms + config.control_latency(action.node_id)
            )
            assert timely == [i for i in removal if landing < cg.edges[i].deadline]
            covered = [i for i, edge in enumerate(instance.edges) if action in edge[2]]
            assert covered == timely
            assert (action in instance.costs) == bool(timely)
        chosen = [a for a, (r, t) in masks.items() if 0 < len(t) < len(r)]
        assert cg.stats["partial_actions"] == len(chosen)
        partial += len(chosen)
        for action in chosen[:1]:
            # Confirmed in force: nothing has to land, the whole R(a) is removed.
            held = gc(harness, t_alert, in_force=[action])
            removal, timely = held.masks()[action]
            assert timely == removal and removal
        harness.close()
    assert partial > 0  # partially timely actions were exercised


def test_an_authority_revocation_is_never_offered(tmp_path):
    from aggregator.containment_monitor.projection import _Builder

    harness = Harness(tmp_path, MonitorConfig())
    graph = harness.monitor.graph
    builder = _Builder(
        graph,
        harness.config,
        {},
        ScopeMirror(graph, harness.config),
        None,
        ALL,
        harness.now,
        0.0,
        False,
        frozenset(),
    )
    task_id = "00000000-0000-4000-8000-000000000001"
    with pytest.raises(ValueError, match="action_not_offered"):
        builder.allowed(Action("revoke", "n0", "authority", task_id))
    assert builder.allowed(revoke_egress("n0", task_id)) is not None
    harness.close()


def test_a_replacing_action_behind_a_confirmed_hold_is_timely(tmp_path):
    """Theory A.6 (a): an edge removed by a confirmed action in force cannot be
    crossed before its release, which follows the replacing cut's confirmation,
    so the flows behind it have d = infinity. Without that, the immediate
    emissions of a held invocation make every replacing action untimely."""
    config = MonitorConfig(
        grants={"r": frozenset({"w"})},
        agent_nodes={"r": "n0", "w": "n1"},
        session_isolated_agents=frozenset({"r"}),
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    task_id = trace.root("r", "n0", 0)
    attempt = trace.start(task_id, 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(attempt)])
    free = gc(harness, 100)
    held = gc(harness, 100, in_force=[hold_task("n0", task_id)])
    # Not held: the unit can emit at once, so its immediate copy is untimely.
    assert any(e.action is not None and not e.feasible for e in free.edges)
    split = next(e for e in held.edges if e.kind == "split")
    assert split.action == hold_task("n0", task_id)
    behind = [e for e in held.edges if e.action is not None and e is not split]
    assert {e.action for e in behind} >= {revoke_egress("n0", task_id)}
    assert all(e.deadline == float("inf") and e.feasible for e in behind)
    assert not any(e.tail.startswith("early") for e in held.edges)
    harness.close()


def test_pausing_queued_or_held_work_costs_it_whatever_its_planned_end(tmp_path):
    """The cost model has no planned ends: pausing a queued task or keeping a
    held invocation paused stops that work (its value and, for a service
    agent, its request) however soon it would have finished, so a pause is
    never priced as free (the simulator's v5 mispricing of held and queued
    invocations has no counterpart here)."""
    from aggregator.containment_monitor.config import MonitorConfig

    config = MonitorConfig(
        agent_nodes={"s": "n0"},
        future_requests={"s": 12.0},
        future_request_value=2.0,
        session_isolated_agents=frozenset({"s"}),
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    queued = trace.root("s", "n0", 0)  # accepted, never started
    held = trace.root("s", "n0", 0)
    trace.start(held, 1)
    harness.feed(trace.events)
    c0 = config.c0
    for now in (2, 600_000):  # just after, and long after any short turn
        cost = gc(harness, now, in_force={hold_task("n0", held)}).cost
        assert cost(hold_task("n0", queued)) == c0 + 1.0 + 2.0
        assert cost(hold_task("n0", held)) == c0 + 1.0 + 2.0
    harness.close()


def test_stopping_a_service_costs_its_requests_and_a_label_does_not(tmp_path):
    """A service agent (future_requests): revoking its credential or its
    admission forfeits its expected future requests; pausing one invocation or
    revoking its egress forfeits that invocation's request; a label (egress
    hold, admitted per event) and a grant revocation of a protected class
    forfeit none."""
    from aggregator.containment_monitor.config import MonitorConfig
    from aggregator.containment_monitor.model import (
        revoke_credential,
        revoke_grant,
    )

    config = MonitorConfig(
        agent_nodes={"s": "n0"},
        future_requests={"s": 12.0},
        future_request_value=2.0,
        session_isolated_agents=frozenset({"s"}),
    )
    harness = Harness(tmp_path, config)
    trace = TraceBuilder()
    task_id = trace.root("s", "n0", 0)
    attempt = trace.start(task_id, 1)
    harness.feed(trace.events)
    harness.monitor.graph.mark_roots([inv(attempt)])
    cost = gc(harness, 10).cost
    c0 = config.c0
    # A tainted invocation's own work is worth nothing; its request is not.
    assert cost(hold_task("n0", task_id)) == c0 + 2.0
    assert cost(revoke_egress("n0", task_id)) == c0 + 2.0
    assert cost(hold_egress("n0", task_id)) == c0
    assert cost(revoke_grant("n0", "s", "fx:device.command")) == c0
    assert cost(revoke_credential("n0", "s")) == c0 + 24.0
    assert cost(revoke_admission("n0", "s")) == c0 + 24.0
    harness.close()
