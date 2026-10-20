"""Containment strategies for experiments, one action vocabulary.

| name                  | display                     | rule                                                    |
|-----------------------|-----------------------------|---------------------------------------------------------|
| reject_source         | Reject source               | park every output of the root invocation(s)/version     |
| stop_team             | Stop team                   | hold every live unit of the root agent's subtree        |
| stop_downstream       | Stop everything downstream  | every action inside the full G_C closure of the sources |
| trace_and_stop        | Trace & stop                | every action inside the declared-flow closure           |
| greedy_cut            | Greedy cut                  | greedy cover on the deadline-aware G_C                  |
| cut_without_deadlines | Cut without deadlines       | exact cut on G_C with every element feasible            |
| firebreak             | Firebreak                   | exact deadline-aware cut (gadget/enumeration/ILP)       |

All strategies see the same G_P, X_t and roots; their actions land with the same
per-site latency. Baselines ignore feasibility (they are issued anyway).

Two optional additions apply to the cut strategies (configuration flags):
- ``best_effort_guard``: also issue every c0-cost action on the last hop into a
  sink the solver dropped as unavoidable (deadlines are lower bounds, so some of
  those may still land in time). Off by default.
- ``taint_aware_admission``: block new dispatches (egress) of every invocation
  still on the source side of the cut. Needed with ``boundary="defer"``, where
  nothing else constrains new invocations until they materialize.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from . import solver
from .config import MonitorConfig
from .graph import ControlGraph
from .model import Action, hold_effect, hold_task, revoke_egress, split
from .provenance import ProvenanceGraph
from .rootcause import subtree

DISPLAY = {
    "reject_source": "Reject source",
    "stop_team": "Stop team",
    "stop_downstream": "Stop everything downstream",
    "trace_and_stop": "Trace & stop",
    "greedy_cut": "Greedy cut",
    "cut_without_deadlines": "Cut without deadlines",
    "firebreak": "Firebreak",
}
CUT_STRATEGIES = frozenset({"greedy_cut", "cut_without_deadlines", "firebreak"})


@dataclass
class StrategyResult:
    name: str
    actions: list[Action]
    solve_ms: float
    predicted_cost: float
    status: str
    method: str
    unavoidable: list[str] = field(default_factory=list)


@dataclass
class Context:
    graph: ProvenanceGraph
    config: MonitorConfig
    cg: ControlGraph
    static_cg: Callable[[], ControlGraph]
    roots: list[str]
    capabilities: frozenset[str]


def _finish(
    ctx: Context,
    name: str,
    actions,
    started: float,
    status="applied",
    method="rule",
    unavoidable=(),
) -> StrategyResult:
    chosen = sorted(
        {a for a in actions if a is not None and a.capability in ctx.capabilities}
    )
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


def stop_team(ctx: Context, level: int = 0) -> StrategyResult:
    started = time.perf_counter()
    attempts = _root_attempts(ctx.graph, ctx.roots, producers=True)
    actions = []
    if attempts:
        row = ctx.graph.invocation(attempts[0])
        agent = row["agent_id"]
        for _ in range(level):
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
    """Egress blocks for every unit the cut leaves on the source side."""
    reached = cg.closure(removed=set(actions))
    out = []
    for name in reached:
        unit = cg.units.get(cg.nodes.get(name, {}).get("unit"))
        if unit is not None and unit.node and unit.task_id:
            out.append(revoke_egress(unit.node, unit.task_id))
    return out


def _cut(ctx: Context, name: str, cg: ControlGraph, method: Callable) -> StrategyResult:
    started = time.perf_counter()
    result = method(cg.instance())
    actions = list(result.actions)
    if ctx.config.best_effort_guard:
        actions += best_effort(cg, result.unavoidable)
    if ctx.config.taint_aware_admission:
        actions += taint_admission(cg, actions)
    finished = _finish(
        ctx, name, actions, started, result.status, result.method, result.unavoidable
    )
    finished.solve_ms = result.solve_ms
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
    "greedy_cut": greedy_cut,
    "cut_without_deadlines": cut_without_deadlines,
    "firebreak": firebreak,
}
