"""Containment strategies for experiments, one action vocabulary.

| name                  | display                     | phase-2 rule                                            |
|-----------------------|-----------------------------|---------------------------------------------------------|
| reject_source         | Reject source               | park every output of the root invocation(s)/version     |
| stop_team             | Stop team                   | hold every live unit of the root agent's team           |
| stop_downstream       | Stop everything downstream  | every action inside the full G_C closure of the sources |
| trace_and_stop        | Trace & stop                | every action inside the declared-flow closure           |
| trace_and_quarantine  | Trace & quarantine          | Trace & stop, plus pause and egress-revoke every unit   |
|                       |                             | of the tainted region (persistent quarantine)           |
| sink_guard            | Sink guard                  | hold every pending scheduled effect, revoke every       |
|                       |                             | agent's protected effect classes at its gateway, park   |
|                       |                             | every result delivered into a delegation root           |
| gsafeguard            | G-Safeguard                 | flag the root's agent(s): revoke the egress of each of  |
|                       |                             | its units and park their results, live and later        |
| greedy_cut            | Greedy cut                  | greedy cover on the deadline-aware G_C                  |
| cut_without_deadlines | Cut without deadlines       | exact cut on G_C with every element feasible            |
| firebreak             | Firebreak                   | exact deadline-aware cut (gadget/enumeration/ILP)       |

A strategy chooses its own actions; every strategy shares the same G_P, X_t,
roots and G_C, the same action vocabulary and per-site landing, and the
confirmation, fencing and agentd gates of its actions. The mechanisms around
the choice are set per strategy (``config.Mechanisms``, by profile; see
``config``): the two-phase protocol (``phase1_freeze``) and two additions that
``with_mechanisms`` makes to the strategy's result:

- ``best_effort_guard``: also issue every c0-cost action on the last hop into a
  sink that is unavoidable on the deadline-aware G_C (deadlines are lower
  bounds, so some of those may still land in time).
- ``taint_aware_admission``: label every invocation still on the source side
  of the chosen actions (an egress hold, under which agentd holds each new
  event of the invocation and the monitor admits it per event, ``admission``):
  the label set Q of theory A.6 (AI), and needed with ``boundary="defer"``,
  where nothing else constrains new invocations until they materialize.

Rules ignore feasibility (their actions are issued anyway).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from . import solver
from .config import Mechanisms, MonitorConfig
from .graph import ControlGraph
from .model import (
    Action,
    TaintedOutputs,
    hold_effect,
    hold_egress,
    hold_task,
    revoke_egress,
    revoke_grant,
    split,
)
from .projection import ScopeMirror
from .provenance import ProvenanceGraph
from .rootcause import subtree

DISPLAY = {
    "reject_source": "Reject source",
    "stop_team": "Stop team",
    "stop_downstream": "Stop everything downstream",
    "trace_and_stop": "Trace & stop",
    "trace_and_quarantine": "Trace & quarantine",
    "sink_guard": "Sink guard",
    "gsafeguard": "G-Safeguard",
    "greedy_cut": "Greedy cut",
    "cut_without_deadlines": "Cut without deadlines",
    "firebreak": "Firebreak",
}


# The per-solve report of a rule: no cut solver ran, so no fast path either.
RULE_REPORT = {
    "solver": "rule",
    "status": "applied",
    "fast_path": False,
    "fast_path_reason": "no_cut_solver",
    "exceptions": None,
    "exception_actions": [],
    "relevant_edges": None,
}


@dataclass
class StrategyResult:
    name: str
    actions: list[Action]
    solve_ms: float
    predicted_cost: float
    status: str
    method: str
    unavoidable: list[str] = field(default_factory=list)
    # solver.CutSolution.report() for a cut strategy, else RULE_REPORT.
    report: dict = field(default_factory=lambda: dict(RULE_REPORT))


@dataclass
class Context:
    graph: ProvenanceGraph
    config: MonitorConfig
    cg: ControlGraph
    static_cg: Callable[[], ControlGraph]
    roots: list[str]
    # Whether every node an action must reach enforces it (describe).
    supported: Callable[[Action], bool]


def _finish(
    ctx: Context,
    name: str,
    actions,
    started: float,
    status="applied",
    method="rule",
    unavoidable=(),
) -> StrategyResult:
    chosen = sorted({a for a in actions if a is not None and ctx.supported(a)})
    cost = sum(ctx.cg.cost(a) for a in chosen) if ctx.cg.cost else float(len(chosen))
    return StrategyResult(
        name,
        chosen,
        (time.perf_counter() - started) * 1000,
        cost,
        status,
        method,
        sorted(unavoidable),
    )


def _root_attempts(
    graph: ProvenanceGraph, roots: list[str], *, producers: bool
) -> list[str]:
    """Invocations named by the roots; a version root names its producer if asked."""
    attempts = []
    for root in roots:
        kind, identity = split(root)
        if kind == "inv":
            attempts.append(identity)
        elif kind == "ver" and producers:
            row = graph.version_row(identity)
            if row and row["producer_attempt"]:
                attempts.append(row["producer_attempt"])
        elif kind == "task":
            attempts.extend(graph.invocations_of_task(identity))
    return attempts


def reject_source(ctx: Context) -> StrategyResult:
    started = time.perf_counter()
    graph, actions = ctx.graph, []
    # A version root rejects that version only; an invocation root all its outputs.
    versions = {split(r)[1] for r in ctx.roots if r.startswith("ver:")}
    attempts = set(_root_attempts(graph, ctx.roots, producers=False))
    for task_id, pending in graph.pending.items():
        if pending.sender_attempt in attempts or pending.request_version in versions:
            node = pending.recipient_node or graph.node_of(pending.recipient_id)
            if node:
                actions.append(hold_task(node, task_id))
    for version, result in graph.results.items():
        if result.producer_attempt in attempts or version in versions:
            parent = graph.running.get(result.parent_attempt)
            if parent:
                actions.append(hold_task(parent.node_id, result.task_id))
    for attempt in attempts:
        live = graph.running.get(attempt)
        parent = (
            graph.running.get(graph.parent_attempt(live.task_id))
            if live and live.task_id
            else None
        )
        if parent:
            actions.append(hold_task(parent.node_id, live.task_id))  # its future result
    for effect in graph.effects.values():
        if effect.attempt_id in attempts:
            actions.append(hold_effect(effect.node_id, effect.effect_id))
    return _finish(ctx, "reject_source", actions, started)


def stop_team(ctx: Context) -> StrategyResult:
    """The org subtree ``stop_team_level`` levels above the root's agent (0: its
    own subtree; 1: the team a leaf belongs to)."""
    started = time.perf_counter()
    attempts = _root_attempts(ctx.graph, ctx.roots, producers=True)
    actions = []
    if attempts:
        row = ctx.graph.invocation(attempts[0])
        agent = row["agent_id"]
        for _ in range(ctx.config.stop_team_level):
            agent = ctx.config.org_parent.get(agent, agent)
        team = subtree(ctx.graph, ctx.config, agent)
        for unit in ctx.cg.units.values():
            if unit.agent in team and unit.node and unit.task_id:
                actions.append(hold_task(unit.node, unit.task_id))
    return _finish(ctx, "stop_team", actions, started)


def _closure_actions(ctx: Context, name: str, declared_only: bool) -> StrategyResult:
    started = time.perf_counter()
    reached = ctx.cg.closure(declared_only=declared_only)
    actions = [
        edge.action
        for edge in ctx.cg.edges
        if edge.action is not None
        and edge.tail in reached
        and edge.head in reached
        and (edge.declared or not declared_only)
    ]
    return _finish(ctx, name, actions, started)


def stop_downstream(ctx: Context) -> StrategyResult:
    return _closure_actions(ctx, "stop_downstream", declared_only=False)


def trace_and_stop(ctx: Context) -> StrategyResult:
    return _closure_actions(ctx, "trace_and_stop", declared_only=True)


def trace_and_quarantine(ctx: Context) -> StrategyResult:
    """Trace & stop, plus a persistent quarantine of the tainted region: every
    live unit in the declared closure of the taint is paused and its egress
    revoked (agentd denies all its new dispatches and effects), no optimization."""
    started = time.perf_counter()
    traced = trace_and_stop(ctx).actions
    quarantine = [
        action
        for key in sorted(ctx.cg.taint_est)
        if (unit := ctx.cg.units.get(key)) is not None and unit.node and unit.task_id
        for action in (
            hold_task(unit.node, unit.task_id),
            revoke_egress(unit.node, unit.task_id),
        )
    ]
    return _finish(ctx, "trace_and_quarantine", [*traced, *quarantine], started)


def sink_guard(ctx: Context) -> StrategyResult:
    """Guard every protected sink, without tracing or optimization.

    The protected sinks are the pending scheduled effects (each held at its
    gateway), the protected effect classes (each agent's grant to them revoked
    at its gateway: an ingress filter on the agent's effects of that class) and
    the protected outputs: the results the delegation roots return to their
    requesters, guarded by parking every result delivered into a root."""
    started = time.perf_counter()
    graph, cg = ctx.graph, ctx.cg
    actions = [
        hold_effect(effect.node_id, effect.effect_id)
        for effect in graph.effects.values()
    ]
    scopes = ScopeMirror(graph, ctx.config)
    classes: dict[str, set[str]] = {
        agent: set(scopes.effects(agent)) for agent in scopes.known
    }
    for unit in cg.units.values():
        classes.setdefault(unit.agent, set()).update(unit.effects)
    for agent, effect_classes in sorted(classes.items()):
        node = graph.node_of(agent)
        if node:
            actions += [
                revoke_grant(node, agent, f"fx:{effect_class}")
                for effect_class in sorted(effect_classes)
            ]
    roots = {
        key
        for key, unit in cg.units.items()
        if not unit.pending and graph.parent_attempt(unit.task_id) is None
    }
    for unit in cg.units.values():
        parent = cg.units.get(unit.parent) if unit.parent in roots else None
        if parent is not None and parent.node and unit.task_id:
            actions.append(hold_task(parent.node, unit.task_id))
    for result in graph.results.values():
        parent = cg.units.get(result.parent_attempt)
        if result.parent_attempt in roots and parent is not None and parent.node:
            actions.append(hold_task(parent.node, result.task_id))
    return _finish(ctx, "sink_guard", actions, started)


def gsafeguard(ctx: Context) -> StrategyResult:
    """G-Safeguard's remediation (Wang et al., ACL 2025, section 3.3, Eq. 11),
    under the shared alert: the agent of each root invocation is flagged, and
    from then on every message it sends, every result it delivers and every
    effect it requests is dropped. Their code (TA/agents.py re_generate and
    are_generate, TA/main_defense_for_different_topology.py
    defense_communication) drops a flagged agent's replies from every
    receiver's input from the next round on; the flag is sticky; the flagged
    agent keeps running and receiving; nothing already received is withdrawn.

    Here, for each unit of a flagged agent (live or queued): its egress is
    revoked (agentd denies its new dispatches and effect requests) and its
    result is parked at its live parent's node when that parent runs on
    another node. Nothing else: its tasks are neither held nor cancelled
    (where the parent shares the unit's node, parking the result would pause
    the unit, so that result flows), no consumer is touched, outputs already
    published or delivered stay where they are, and no other agent's lease or
    grant changes. Effect requests count as outgoing edges because the
    actuator is a node of our graph, which can only help this baseline (the
    original has no effects to drop).

    Sticky: the flag stays with the incident (``model.STICKY_FLAG_STRATEGIES``),
    so a later unit of the agent is a frontier advance and the receding
    re-solve (the persistent quarantine's mechanism) applies the same rule to
    it once the monitor sees it."""
    started = time.perf_counter()
    agents = ctx.graph.agents_of(ctx.roots)
    actions = []
    for unit in ctx.cg.units.values():
        if unit.agent not in agents or not (unit.node and unit.task_id):
            continue
        actions.append(revoke_egress(unit.node, unit.task_id))
        parent = ctx.cg.units.get(unit.parent) if unit.parent else None
        if parent is not None and parent.node and parent.node != unit.node:
            actions.append(hold_task(parent.node, unit.task_id))
    return _finish(ctx, "gsafeguard", actions, started)


def best_effort(cg: ControlGraph, unavoidable) -> list[Action]:
    """c0-cost actions on the last hop into sinks judged unavoidable."""
    threshold = cg.cost.config.c0 + 1e-9 if cg.cost else 0.0
    return [
        edge.action
        for edge in cg.edges
        if edge.head in unavoidable
        and edge.action is not None
        and cg.cost(edge.action) <= threshold
    ]


def taint_admission(cg: ControlGraph, actions: list[Action]) -> list[Action]:
    """Labels (egress holds) on every unit the cut leaves on the source side;
    one whose egress is already revoked needs none."""
    reached = cg.closure(removed=set(actions))
    out = []
    for name in reached:
        unit = cg.units.get(cg.nodes.get(name, {}).get("unit"))
        if unit is None or not (unit.node and unit.task_id):
            continue
        if revoke_egress(unit.node, unit.task_id) not in actions:
            out.append(hold_egress(unit.node, unit.task_id))
    return out


def with_mechanisms(
    ctx: Context, result: StrategyResult, mechanisms: Mechanisms
) -> StrategyResult:
    """Add the guard and taint-aware admission where the strategy has them.

    Both are computed the same way for every strategy, on the deadline-aware
    G_C from the strategy's own actions: the guard on the sinks unavoidable
    there, the admission on the units those actions leave on the source side.
    """
    started = time.perf_counter()
    actions = list(result.actions)
    unavoidable = solver.unavoidable_sinks(ctx.cg.instance())
    if mechanisms.best_effort_guard:
        actions += best_effort(ctx.cg, unavoidable)
    if mechanisms.taint_aware_admission:
        actions += taint_admission(ctx.cg, actions)
    shared = _finish(
        ctx, result.name, actions, started, result.status, result.method, unavoidable
    )
    shared.predicted_cost += sum(
        result.report.get("tainted_outputs_accepted", {}).values()
    )
    shared.solve_ms = result.solve_ms
    shared.report = result.report
    return shared


def _cut(ctx: Context, name: str, cg: ControlGraph, method: Callable) -> StrategyResult:
    started = time.perf_counter()
    result = method(cg.instance(penalties=True))
    # A priced outcome the cut accepts (tainted outputs) is no action: its loss
    # counts in the predicted cost and the report, nothing is sent.
    accepted = {
        a.agent: cg.penalty_costs[a]
        for a in result.actions
        if isinstance(a, TaintedOutputs)
    }
    finished = _finish(
        ctx,
        name,
        [a for a in result.actions if not isinstance(a, TaintedOutputs)],
        started,
        result.status,
        result.method,
        result.unavoidable,
    )
    finished.predicted_cost += sum(accepted.values())
    finished.solve_ms = result.solve_ms
    finished.report = result.report()
    if finished.report["fast_path"] is None:  # the greedy checks nothing
        finished.report.update(fast_path=False, fast_path_reason="heuristic")
    if cg.penalty_costs:
        finished.report["tainted_outputs_accepted"] = accepted
    return finished


def greedy_cut(ctx: Context) -> StrategyResult:
    return _cut(ctx, "greedy_cut", ctx.cg, solver.greedy)


def cut_without_deadlines(ctx: Context) -> StrategyResult:
    return _cut(
        ctx,
        "cut_without_deadlines",
        ctx.static_cg(),
        lambda instance: solver.solve(instance, enum_limit=ctx.config.enum_limit),
    )


def firebreak(ctx: Context) -> StrategyResult:
    return _cut(
        ctx,
        "firebreak",
        ctx.cg,
        lambda instance: solver.solve(instance, enum_limit=ctx.config.enum_limit),
    )


STRATEGIES: dict[str, Callable[[Context], StrategyResult]] = {
    "reject_source": reject_source,
    "stop_team": stop_team,
    "stop_downstream": stop_downstream,
    "trace_and_stop": trace_and_stop,
    "trace_and_quarantine": trace_and_quarantine,
    "sink_guard": sink_guard,
    "gsafeguard": gsafeguard,
    "greedy_cut": greedy_cut,
    "cut_without_deadlines": cut_without_deadlines,
    "firebreak": firebreak,
}
