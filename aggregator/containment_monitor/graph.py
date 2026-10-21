"""Control graph G_C(t): nodes, edges with deadlines, and the cut instance.

Every edge is a possible future flow. ``action`` is the control decision that
removes it (None: uncuttable); ``deadline`` is the earliest time tainted influence
can traverse it; ``feasible`` says whether the action lands before that deadline
(or is already in force). Infeasible edges are uncuttable for the solver.

Deadlines enter the cut problem only through per-action timely removal masks:
R(a) is the set of edges whose action is a, and

    R_tau(a) = {e in R(a) : now + tau + L(site(a)) < d(e)}

for an action not in force; an action in force (confirmed at every site it
must reach) keeps its whole R(a), since nothing has to land any more. No action
ever removes another action's edges: a revocation never implies its
descendants' (no tree semantics), and each mask is judged from its own site.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass, field

from .model import Action
from .solver import CutInstance

# Edge kinds that represent declared or recorded flows (the rest are possible,
# authority-bounded flows). "Trace & stop" follows only declared kinds.
DECLARED_KINDS = frozenset({"request", "split", "early", "result", "session", "effect"})


@dataclass
class Edge:
    tail: str
    head: str
    kind: str
    action: Action | None
    fn: tuple
    declared: bool
    deadline: float = float("inf")
    t_ap: float | None = None
    feasible: bool = False
    # A priced outcome, not a flow (``tainted_output_value``): the
    # model.TaintedOutputs token accepting it and the hold that, landing in
    # time, stops it instead (None: none can). Uncuttable (``action`` None)
    # and into no sink, except in the strategies' solve.
    penalty: tuple[Hashable, Action | None] | None = None


@dataclass
class ControlGraph:
    t_now: float
    static: bool
    nodes: dict[str, dict] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    out: dict[str, list[int]] = field(default_factory=dict)
    sources: set[str] = field(default_factory=set)
    sinks: set[str] = field(default_factory=set)
    eat: dict[str, float] = field(default_factory=dict)
    taint_est: set[str] = field(default_factory=set)
    units: dict = field(default_factory=dict)  # unit key -> projection.Unit
    cost: Callable[[Action], float] | None = None
    stats: dict = field(default_factory=dict)
    # The loss of each priced outcome (``Edge.penalty``). Only the strategies'
    # solve sees those edges as removable into sinks (``instance(penalties=
    # True)``); a check never does.
    penalty_costs: dict[Hashable, float] = field(default_factory=dict)

    def node(self, name: str, **attrs) -> str:
        if name not in self.nodes:
            self.nodes[name] = attrs
        return name

    def edge(
        self,
        tail: str,
        head: str,
        kind: str,
        *,
        fn: tuple = ("id",),
        action: Action | None = None,
        declared: bool | None = None,
    ) -> Edge:
        self.node(tail)
        self.node(head)
        value = Edge(
            tail,
            head,
            kind,
            action,
            fn,
            kind in DECLARED_KINDS if declared is None else declared,
        )
        self.out.setdefault(tail, []).append(len(self.edges))
        self.edges.append(value)
        return value

    def successors(self, node: str) -> Iterable[Edge]:
        return (self.edges[i] for i in self.out.get(node, ()))

    def closure(
        self, *, declared_only: bool = False, removed: set[Action] | None = None
    ) -> set[str]:
        removed = removed or set()
        seen = set(self.sources)
        queue = deque(seen)
        while queue:
            node = queue.popleft()
            for edge in self.successors(node):
                if declared_only and not edge.declared:
                    continue
                if edge.action is not None and edge.feasible and edge.action in removed:
                    continue
                if edge.head not in seen:
                    seen.add(edge.head)
                    queue.append(edge.head)
        return seen

    def actions(self) -> set[Action]:
        return {edge.action for edge in self.edges if edge.action is not None}

    def masks(self) -> dict[Action, tuple[list[int], list[int]]]:
        """Per action, its removal set R(a) and timely mask R_tau(a) (edge indices)."""
        found: dict[Action, tuple[list[int], list[int]]] = {}
        for index, edge in enumerate(self.edges):
            if edge.action is None:
                continue
            removal, timely = found.setdefault(edge.action, ([], []))
            removal.append(index)
            if edge.feasible:
                timely.append(index)
        return found

    def instance(self, *, penalties: bool = False) -> CutInstance:
        """The cut problem: each action removes its timely mask, at its full
        cost; with ``penalties``, also the priced outcomes (always removable,
        at their loss)."""
        covering: dict[int, tuple[Hashable, ...]] = {}
        costs: dict[Hashable, float] = {}
        for action, (_, timely) in self.masks().items():
            if not timely:
                continue  # cannot land before any of its edges: useless now
            costs[action] = self.cost(action) if self.cost else 1.0
            for index in timely:
                covering[index] = (*covering.get(index, ()), action)
        sinks = set(self.sinks)
        if penalties:
            for index, edge in enumerate(self.edges):
                if edge.penalty is None:
                    continue
                token, hold = edge.penalty
                costs[token] = self.penalty_costs[token]
                covering[index] = (token,)
                if hold is not None:
                    covering[index] += (hold,)
                    if hold not in costs:
                        costs[hold] = self.cost(hold) if self.cost else 1.0
                sinks.add(edge.head)
        return CutInstance(
            tuple(
                (edge.tail, edge.head, covering.get(index, ()))
                for index, edge in enumerate(self.edges)
            ),
            costs,
            frozenset(self.sources),
            frozenset(sinks),
        )
