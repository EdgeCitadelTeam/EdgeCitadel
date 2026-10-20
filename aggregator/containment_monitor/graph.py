"""Control graph G_C(t): nodes, edges with deadlines, and the cut instance.

Every edge is a possible future flow. ``action`` is the control decision that
removes it (None: uncuttable); ``deadline`` is the earliest time tainted influence
can traverse it; ``feasible`` says whether the action lands before that deadline
(or is already in force). Infeasible edges are uncuttable for the solver.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
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

    def instance(self) -> CutInstance:
        """The cut problem: feasible actions only, each costed once."""
        edges = []
        costs: dict[Action, float] = {}
        for edge in self.edges:
            covering: tuple[Action, ...] = ()
            if edge.action is not None and edge.feasible:
                covering = (edge.action,)
                if edge.action not in costs:
                    costs[edge.action] = self.cost(edge.action) if self.cost else 1.0
            edges.append((edge.tail, edge.head, covering))
        return CutInstance(
            tuple(edges), costs, frozenset(self.sources), frozenset(self.sinks)
        )
