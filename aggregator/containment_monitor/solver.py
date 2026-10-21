"""Exact minimum cut with grouped actions (grouped s-t cut / minimum label cut).

An instance is a directed graph whose edges are removable by zero or more actions
(ANY semantics: one covering action suffices), action costs, sources and sinks.
An action's edges are its timely removal mask R_tau(a): deadlines are applied
before this module, so an action that cannot land in time for an edge is simply
not listed on it. A partially timely action removes only its timely edges while
its full cost is paid (theory note section 5). The solver never infers removal
from any other action (no tree semantics).

The single max-flow (theory Theorem 4.2) is exact only under two conditions,
which ``solve`` checks on every instance instead of assuming them:

- every grouped action is *gated*: its removal set is equivalent, for source-sink
  connectivity, to a set of edges at one port (the out- or in-edges of one
  vertex). Checked sufficient conditions: all members share a tail or a head
  (G1), or one member lies on every path that meets the group (G3);
- at every port the gate sets are laminar (pairwise nested or disjoint).

Algorithm (theory note sections 4.2-4.8):
1. Sinks reachable through uncuttable edges only are unavoidable; they are
   reported and dropped (no cut exists for them).
2. Only edges on some source-to-sink path matter, and the remaining sinks are
   contracted into the super-sink (reaching any sink is reaching it).
3. Classify every action: private (one edge), gated at a port, or an exception
   (ungated, or gated but crossing another gate set at its port).
4. No exception: ONE max-flow on the laminar chain gadget (theory 4.3), where an
   edge passes through its tail's out-chain, its private actions in series and
   its head's in-chain.
5. k exceptions, k <= ``enum_limit``: 2^k max-flows on the gated residual
   (Proposition 4.7). Above it, the compact ILP over the timely masks
   (Proposition 4.8); without HiGHS the greedy cut is returned with status
   ``approximate``. The fast path is never used when a condition fails.

Every solution reports whether the fast path was valid, why not, the exception
set and the solver used. Max-flow is Dinic's algorithm in pure Python.
"""

from __future__ import annotations

import importlib.util
import itertools
import math
import time
from collections import deque
from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field

INF = math.inf
EPS = 1e-12
SRC, SNK = "\x00source", "\x00sink"


@dataclass(frozen=True)
class CutInstance:
    edges: tuple[tuple[str, str, tuple[Hashable, ...]], ...]
    costs: dict[Hashable, float]
    sources: frozenset[str]
    sinks: frozenset[str]


@dataclass
class CutSolution:
    actions: list
    cost: float
    status: str  # optimal | trivial | infeasible | approximate | heuristic
    method: str  # trivial | gadget | enumeration | ilp | greedy | brute_force
    unavoidable: frozenset[str]
    solve_ms: float
    relevant_nodes: int = 0
    relevant_edges: int = 0
    # The checked fast-path conditions: None when they were not checked (the
    # greedy, the ILP called directly, brute force).
    fast_path: bool | None = None
    fast_path_reason: str | None = None  # ungated | non_laminar | both, joined by +
    exceptions: list = field(default_factory=list)
    lower_bound: float | None = None

    def report(self) -> dict:
        """The per-solve record kept in status and the decision ledger."""
        return {
            "solver": self.method,
            "status": self.status,
            "fast_path": self.fast_path,
            "fast_path_reason": self.fast_path_reason,
            "exceptions": len(self.exceptions),
            "exception_actions": [_label(a) for a in self.exceptions],
            "relevant_edges": self.relevant_edges,
        }


def _order(action: Hashable) -> str:
    return str(action)


def _label(action: Hashable) -> str:
    label = getattr(action, "label", None)
    return label() if callable(label) else str(action)


# ---------------------------------------------------------------------------
# Reachability helpers
# ---------------------------------------------------------------------------
def _adjacency(edges) -> dict[str, list[int]]:
    out: dict[str, list[int]] = {}
    for index, (tail, _, _) in enumerate(edges):
        out.setdefault(tail, []).append(index)
    return out


def unavoidable_sinks(instance: CutInstance) -> frozenset[str]:
    """Sinks reachable from a source over edges no action can remove."""
    out = _adjacency(instance.edges)
    seen = set(instance.sources)
    queue = deque(seen)
    while queue:
        node = queue.popleft()
        for index in out.get(node, ()):
            _, head, actions = instance.edges[index]
            if not actions and head not in seen:
                seen.add(head)
                queue.append(head)
    return frozenset(instance.sinks & seen)


def relevant_edges(instance: CutInstance, sinks: frozenset[str]) -> list[int]:
    """Indices of edges lying on some source-to-sink path."""
    out = _adjacency(instance.edges)
    inc: dict[str, list[int]] = {}
    for index, (_, head, _) in enumerate(instance.edges):
        inc.setdefault(head, []).append(index)
    forward = set(instance.sources)
    queue = deque(forward)
    while queue:
        node = queue.popleft()
        for index in out.get(node, ()):
            head = instance.edges[index][1]
            if head not in forward:
                forward.add(head)
                queue.append(head)
    backward = {sink for sink in sinks if sink in forward}
    queue = deque(backward)
    while queue:
        node = queue.popleft()
        for index in inc.get(node, ()):
            tail = instance.edges[index][0]
            if tail in forward and tail not in backward:
                backward.add(tail)
                queue.append(tail)
    return [
        index
        for index, (tail, head, _) in enumerate(instance.edges)
        if tail in backward and head in backward
    ]


def removed_by(instance: CutInstance, actions: Iterable[Hashable]) -> set[int]:
    chosen = set(actions)
    return {
        index
        for index, (_, _, covering) in enumerate(instance.edges)
        if chosen.intersection(covering)
    }


def reached_sinks(
    instance: CutInstance,
    actions: Iterable[Hashable],
    sinks: frozenset[str] | None = None,
) -> frozenset[str]:
    """The (given) sinks a source still reaches after removing the actions' edges."""
    targets = instance.sinks if sinks is None else sinks
    removed = removed_by(instance, actions)
    out = _adjacency(instance.edges)
    seen = set(instance.sources)
    queue = deque(seen)
    while queue:
        node = queue.popleft()
        for index in out.get(node, ()):
            head = instance.edges[index][1]
            if index not in removed and head not in seen:
                seen.add(head)
                queue.append(head)
    return frozenset(seen & targets)


def separated(
    instance: CutInstance,
    actions: Iterable[Hashable],
    sinks: frozenset[str] | None = None,
) -> bool:
    """True when no source reaches a (given) sink after removing the actions' edges."""
    targets = instance.sinks if sinks is None else sinks
    removed = removed_by(instance, actions)
    out = _adjacency(instance.edges)
    seen = set(instance.sources)
    queue = deque(seen)
    while queue:
        node = queue.popleft()
        if node in targets:
            return False
        for index in out.get(node, ()):
            head = instance.edges[index][1]
            if index not in removed and head not in seen:
                seen.add(head)
                queue.append(head)
    return not (seen & targets)


# ---------------------------------------------------------------------------
# Dinic max-flow
# ---------------------------------------------------------------------------
class FlowNetwork:
    def __init__(self) -> None:
        self.index: dict[str, int] = {}
        self.adj: list[list[int]] = []
        self.to: list[int] = []
        self.cap: list[float] = []
        self.label: list[Hashable | None] = []

    def node(self, key: str) -> int:
        found = self.index.get(key)
        if found is None:
            found = self.index[key] = len(self.adj)
            self.adj.append([])
        return found

    def add(
        self, tail: str, head: str, capacity: float, label: Hashable | None = None
    ) -> None:
        u, v = self.node(tail), self.node(head)
        self.adj[u].append(len(self.to))
        self.to.append(v)
        self.cap.append(capacity)
        self.label.append(label)
        self.adj[v].append(len(self.to))
        self.to.append(u)
        self.cap.append(0.0)
        self.label.append(None)

    def has_infinite_path(self, source: str, sink: str) -> bool:
        if source not in self.index or sink not in self.index:
            return False
        s, t = self.index[source], self.index[sink]
        seen = {s}
        queue = deque([s])
        while queue:
            u = queue.popleft()
            if u == t:
                return True
            for e in self.adj[u]:
                if e % 2 == 0 and self.cap[e] == INF and self.to[e] not in seen:
                    seen.add(self.to[e])
                    queue.append(self.to[e])
        return False

    def max_flow(self, source: str, sink: str) -> float:
        if source not in self.index or sink not in self.index:
            return 0.0
        s, t = self.index[source], self.index[sink]
        total = 0.0
        n = len(self.adj)
        while True:
            level = [-1] * n
            level[s] = 0
            queue = deque([s])
            while queue:
                u = queue.popleft()
                for e in self.adj[u]:
                    v = self.to[e]
                    if self.cap[e] > EPS and level[v] < 0:
                        level[v] = level[u] + 1
                        queue.append(v)
            if level[t] < 0:
                return total
            pointer = [0] * n
            while True:
                pushed = self._augment(s, t, level, pointer)
                if pushed <= EPS:
                    break
                total += pushed

    def _augment(self, s: int, t: int, level: list[int], pointer: list[int]) -> float:
        path: list[int] = []
        u = s
        while True:
            if u == t:
                amount = min(self.cap[e] for e in path)
                if amount == INF:
                    raise ValueError("infinite_flow")
                for e in path:
                    self.cap[e] -= amount
                    self.cap[e ^ 1] += amount
                return amount
            edges = self.adj[u]
            while pointer[u] < len(edges):
                e = edges[pointer[u]]
                if self.cap[e] > EPS and level[self.to[e]] == level[u] + 1:
                    break
                pointer[u] += 1
            else:
                if not path:
                    return 0.0
                level[u] = -1  # dead end in this phase
                e = path.pop()
                u = self.to[e ^ 1]
                pointer[u] += 1
                continue
            e = edges[pointer[u]]
            path.append(e)
            u = self.to[e]

    def min_cut_labels(self, source: str) -> list[Hashable]:
        """Labels of saturated edges leaving the residual source side."""
        s = self.index[source]
        seen = {s}
        queue = deque([s])
        while queue:
            u = queue.popleft()
            for e in self.adj[u]:
                v = self.to[e]
                if self.cap[e] > EPS and v not in seen:
                    seen.add(v)
                    queue.append(v)
        labels = []
        for u in seen:
            for e in self.adj[u]:
                if e % 2 == 0 and self.to[e] not in seen and self.label[e] is not None:
                    labels.append(self.label[e])
        return labels


# ---------------------------------------------------------------------------
# Exact grouped solver
# ---------------------------------------------------------------------------
Port = tuple[str, str]  # ("out" | "in", vertex)


@dataclass
class _Prepared:
    """The relevant instance, classified for the chain gadget.

    ``edges`` keep every covering action; ``bundles`` are the timely masks
    R_tau(a) by edge position. ``private`` actions remove a single edge.
    ``ports`` hold, per port, the laminar family of gate sets with the actions
    that have each set. ``exceptions`` are the actions the gadget cannot carry:
    ``ungated`` ones and ``non_laminar`` ones (gated, but crossing a gate set
    kept at their port).
    """

    edges: list[tuple[str, str, tuple[Hashable, ...]]]
    bundles: dict[Hashable, list[int]]
    private: set[Hashable]
    ports: dict[Port, dict[frozenset[int], list[Hashable]]]
    ungated: list[Hashable]
    non_laminar: list[Hashable]
    sources: frozenset[str]
    costs: dict[Hashable, float]

    @property
    def exceptions(self) -> list[Hashable]:
        return sorted([*self.ungated, *self.non_laminar], key=_order)

    @property
    def fast_path_reason(self) -> str | None:
        reasons = [
            name
            for name, found in (
                ("ungated", self.ungated),
                ("non_laminar", self.non_laminar),
            )
            if found
        ]
        return "+".join(reasons) or None


def _laminar(left: frozenset[int], right: frozenset[int]) -> bool:
    return left <= right or right <= left or not (left & right)


def _prepare(
    instance: CutInstance, sinks: frozenset[str], indices: list[int]
) -> _Prepared:
    edges: list[tuple[str, str, tuple[Hashable, ...]]] = []
    for index in indices:
        tail, head, actions = instance.edges[index]
        # Reaching any sink is reaching the super-sink: contract the sinks. Paths
        # stop there, and every group of last hops into sinks becomes gated at SNK
        # (e.g. an admission block over several per-flow boundaries).
        if tail in sinks:
            continue
        edges.append(
            (
                tail,
                SNK if head in sinks else head,
                tuple(sorted(set(actions), key=_order)),
            )
        )
    bundles: dict[Hashable, list[int]] = {}
    for position, (_, _, actions) in enumerate(edges):
        for action in actions:
            bundles.setdefault(action, []).append(position)
    in_degree: dict[str, int] = {}
    for _, head, _ in edges:
        in_degree[head] = in_degree.get(head, 0) + 1
    private: set[Hashable] = set()
    gated: dict[Port, dict[frozenset[int], list[Hashable]]] = {}
    ungated: list[Hashable] = []
    for action in sorted(bundles, key=_order):
        members = bundles[action]
        if len(members) == 1:
            private.add(action)
            continue
        tails = {edges[i][0] for i in members}
        heads = {edges[i][1] for i in members}
        port: Port | None = None
        if len(tails) == 1:
            port = ("out", next(iter(tails)))
        elif len(heads) == 1:
            port = ("in", next(iter(heads)))
        if port is not None:  # G1: the gate set is the whole bundle
            gated.setdefault(port, {}).setdefault(frozenset(members), []).append(action)
            continue
        # G3: a member g whose head is no source, is entered only by g and is the
        # tail of every other member lies on every path that meets the group.
        # By substitutability (Lemma 4.1) the action may remove just g.
        dominant = [
            g
            for g in members
            if edges[g][1] not in instance.sources
            and in_degree.get(edges[g][1], 0) == 1
            and all(edges[e][0] == edges[g][1] for e in members if e != g)
        ]
        if dominant:
            for e in members:
                if e != dominant[0]:
                    tail, head, actions = edges[e]
                    edges[e] = (tail, head, tuple(a for a in actions if a != action))
            bundles[action] = [dominant[0]]
            private.add(action)
        else:
            ungated.append(action)
    # Laminarity per port: keep gate sets largest first while they nest with or
    # avoid every kept set; the actions of a crossing set become exceptions.
    ports: dict[Port, dict[frozenset[int], list[Hashable]]] = {}
    non_laminar: list[Hashable] = []
    for port, family in gated.items():
        kept: dict[frozenset[int], list[Hashable]] = {}
        for gate_set in sorted(family, key=lambda s: (-len(s), sorted(s))):
            if all(_laminar(gate_set, other) for other in kept):
                kept[gate_set] = family[gate_set]
            else:
                non_laminar.extend(family[gate_set])
        ports[port] = kept
    return _Prepared(
        edges,
        bundles,
        private,
        ports,
        sorted(ungated, key=_order),
        sorted(non_laminar, key=_order),
        instance.sources,
        dict(instance.costs),
    )


def _gated_flow(
    prepared: _Prepared, chosen: Iterable[Hashable]
) -> tuple[float, list[Hashable]]:
    """One max-flow on the laminar chain gadget (theory 4.3).

    The chosen exceptions are applied (their edges deleted); the others are
    unavailable. Each port's gate sets form a forest by inclusion with one node
    per set, entered from its parent set (or the port's vertex) through an edge
    costing the cheapest action with that set. An edge leaves its tail through
    the smallest out-set containing it, crosses its private actions in series and
    enters its head through the smallest in-set containing it; an edge with no
    action left is uncuttable.
    """
    removed = {position for action in chosen for position in prepared.bundles[action]}
    net = FlowNetwork()
    smallest: dict[tuple[str, int], str] = {}  # (side, edge position) -> chain node
    for (side, vertex), family in prepared.ports.items():
        ordered = sorted(family, key=lambda s: (-len(s), sorted(s)))
        names = {
            gate_set: f"\x00{side}:{vertex}:{i}" for i, gate_set in enumerate(ordered)
        }
        for i, gate_set in enumerate(ordered):
            parents = [s for s in ordered[:i] if gate_set < s]
            upper = names[min(parents, key=len)] if parents else vertex
            action = min(family[gate_set], key=lambda a: (prepared.costs[a], _order(a)))
            if side == "out":
                net.add(upper, names[gate_set], prepared.costs[action], action)
            else:
                net.add(names[gate_set], upper, prepared.costs[action], action)
            # Larger sets come first, so the last assignment is the smallest set.
            for position in gate_set:
                smallest[(side, position)] = names[gate_set]
    for position, (tail, head, actions) in enumerate(prepared.edges):
        if position in removed:
            continue
        start = smallest.get(("out", position), tail)
        end = smallest.get(("in", position), head)
        segments = [a for a in actions if a in prepared.private]
        if not segments:
            net.add(start, end, INF)
            continue
        hops = [
            start,
            *(f"\x00m:{position}:{i}" for i in range(len(segments) - 1)),
            end,
        ]
        for i, action in enumerate(segments):
            net.add(hops[i], hops[i + 1], prepared.costs[action], action)
    for source in prepared.sources:
        net.add(SRC, source, INF)
    if net.has_infinite_path(SRC, SNK):
        return INF, []
    value = net.max_flow(SRC, SNK)
    return value, (net.min_cut_labels(SRC) if value > EPS else [])


def solve(
    instance: CutInstance, *, enum_limit: int = 12, use_ilp: bool = True
) -> CutSolution:
    started = time.perf_counter()
    unavoidable = unavoidable_sinks(instance)
    sinks = instance.sinks - unavoidable
    indices = relevant_edges(instance, sinks)
    nodes = {instance.edges[i][0] for i in indices} | {
        instance.edges[i][1] for i in indices
    }

    def result(actions, cost, status, method, **extra) -> CutSolution:
        return CutSolution(
            sorted(set(actions), key=_order),
            cost,
            status,
            method,
            unavoidable,
            (time.perf_counter() - started) * 1000,
            len(nodes),
            len(indices),
            **extra,
        )

    if not indices:
        return result([], 0.0, "trivial", "trivial", fast_path=True)
    prepared = _prepare(instance, sinks, indices)
    exceptions = prepared.exceptions
    checked = {
        "fast_path": not exceptions,
        "fast_path_reason": prepared.fast_path_reason,
        "exceptions": exceptions,
    }
    k = len(exceptions)
    if k == 0:
        value, actions = _gated_flow(prepared, ())
        if value == INF:
            return result([], INF, "infeasible", "gadget", **checked)
        return result(actions, value, "optimal", "gadget", **checked)
    if k > enum_limit:
        if use_ilp and ilp_available():
            solution = ilp(instance)
            if solution is not None:
                solution.solve_ms = (time.perf_counter() - started) * 1000
                for name, value in checked.items():
                    setattr(solution, name, value)
                return solution
        approximate = greedy(instance)
        approximate.status = "approximate"
        for name, value in checked.items():
            setattr(approximate, name, value)
        return approximate
    best, best_actions = INF, []
    costs = [prepared.costs[action] for action in exceptions]
    for mask in range(1 << k):
        chosen = [exceptions[i] for i in range(k) if mask >> i & 1]
        group_cost = sum(costs[i] for i in range(k) if mask >> i & 1)
        if group_cost >= best - EPS:
            continue
        value, actions = _gated_flow(prepared, chosen)
        if group_cost + value < best - EPS:
            best, best_actions = group_cost + value, [*chosen, *actions]
    if best == INF:
        return result([], INF, "infeasible", "enumeration", **checked)
    return result(best_actions, best, "optimal", "enumeration", **checked)


# ---------------------------------------------------------------------------
# Optional ILP / LP bound (HiGHS)
# ---------------------------------------------------------------------------
def ilp_available() -> bool:
    return importlib.util.find_spec("highspy") is not None


def ilp(instance: CutInstance, *, relaxed: bool = False) -> CutSolution | None:
    """Compact ILP (theory 4.8): binaries x_a, potentials p_v in [0, 1].

    p = 0 on sources, 1 on sinks; for every edge p_head - p_tail <= sum of x over
    its covering actions. ``relaxed`` returns the LP bound with fractional x.
    """
    if not ilp_available():
        return None
    import highspy

    started = time.perf_counter()
    unavoidable = unavoidable_sinks(instance)
    sinks = instance.sinks - unavoidable
    indices = relevant_edges(instance, sinks)
    if not indices:
        return CutSolution([], 0.0, "trivial", "ilp", unavoidable, 0.0)
    actions = sorted({a for i in indices for a in instance.edges[i][2]}, key=_order)
    nodes = sorted(
        {instance.edges[i][0] for i in indices}
        | {instance.edges[i][1] for i in indices}
    )
    h = highspy.Highs()
    h.setOptionValue("output_flag", False)
    column: dict[Hashable, int] = {}
    for action in actions:
        column[action] = h.getNumCol()
        h.addVar(0.0, 1.0)
        h.changeColCost(column[action], instance.costs[action])
        if not relaxed:
            h.changeColIntegrality(column[action], highspy.HighsVarType.kInteger)
    potential: dict[str, int] = {}
    for node in nodes:
        potential[node] = h.getNumCol()
        low = 1.0 if node in sinks else 0.0
        high = 0.0 if node in instance.sources else 1.0
        h.addVar(low, high)
    for index in indices:
        tail, head, covering = instance.edges[index]
        columns = [potential[head], potential[tail], *[column[a] for a in covering]]
        values = [1.0, -1.0, *[-1.0] * len(covering)]
        h.addRow(-highspy.kHighsInf, 0.0, len(columns), columns, values)
    h.run()
    if h.getModelStatus() != highspy.HighsModelStatus.kOptimal:
        return CutSolution([], INF, "infeasible", "ilp", unavoidable, 0.0)
    values = h.getSolution().col_value
    if relaxed:
        bound = sum(instance.costs[a] * values[column[a]] for a in actions)
        return CutSolution(
            [],
            bound,
            "optimal",
            "lp",
            unavoidable,
            (time.perf_counter() - started) * 1000,
            len(nodes),
            len(indices),
            lower_bound=bound,
        )
    chosen = [a for a in actions if values[column[a]] > 0.5]
    return CutSolution(
        chosen,
        sum(instance.costs[a] for a in chosen),
        "optimal",
        "ilp",
        unavoidable,
        (time.perf_counter() - started) * 1000,
        len(nodes),
        len(indices),
    )


# ---------------------------------------------------------------------------
# Greedy cut and brute force
# ---------------------------------------------------------------------------
def greedy(instance: CutInstance) -> CutSolution:
    """Repeatedly take the cheapest action covering an edge on a remaining
    source-to-sink path (ties by action key). Edges on such paths only disappear as
    actions are taken, so one ascending scan with a relevance re-check is exact."""
    started = time.perf_counter()
    unavoidable = unavoidable_sinks(instance)
    sinks = instance.sinks - unavoidable
    indices = relevant_edges(instance, sinks)
    cover: dict[Hashable, list[int]] = {}
    for index in indices:
        for action in instance.edges[index][2]:
            cover.setdefault(action, []).append(index)
    alive = set(indices)
    out: dict[str, list[int]] = {}
    inc: dict[str, list[int]] = {}
    for index in indices:
        tail, head, _ = instance.edges[index]
        out.setdefault(tail, []).append(index)
        inc.setdefault(head, []).append(index)

    def on_path() -> set[int]:
        forward = set(instance.sources)
        queue = deque(forward)
        while queue:
            node = queue.popleft()
            for index in out.get(node, ()):
                head = instance.edges[index][1]
                if index in alive and head not in forward:
                    forward.add(head)
                    queue.append(head)
        backward = {sink for sink in sinks if sink in forward}
        queue = deque(backward)
        while queue:
            node = queue.popleft()
            for index in inc.get(node, ()):
                tail = instance.edges[index][0]
                if index in alive and tail in forward and tail not in backward:
                    backward.add(tail)
                    queue.append(tail)
        return {
            i
            for i in alive
            if instance.edges[i][0] in backward and instance.edges[i][1] in backward
        }

    chosen: list[Hashable] = []
    relevant = on_path()
    for action in sorted(cover, key=lambda a: (instance.costs[a], _order(a))):
        if not relevant:
            break
        if not relevant.intersection(cover[action]):
            continue
        chosen.append(action)
        alive.difference_update(cover[action])
        relevant = on_path()
    return CutSolution(
        sorted(chosen, key=_order),
        sum(instance.costs[a] for a in chosen),
        "heuristic",
        "greedy",
        unavoidable,
        (time.perf_counter() - started) * 1000,
        len(
            {instance.edges[i][0] for i in indices}
            | {instance.edges[i][1] for i in indices}
        ),
        len(indices),
    )


def brute_force(instance: CutInstance, max_actions: int = 18) -> CutSolution | None:
    """Exhaustive optimum over the relevant actions (verification oracle)."""
    started = time.perf_counter()
    unavoidable = unavoidable_sinks(instance)
    sinks = instance.sinks - unavoidable
    indices = relevant_edges(instance, sinks)
    actions = sorted({a for i in indices for a in instance.edges[i][2]}, key=_order)
    if len(actions) > max_actions:
        return None
    best, best_set = INF, None
    for size in range(len(actions) + 1):
        for combination in itertools.combinations(actions, size):
            cost = sum(instance.costs[a] for a in combination)
            if cost >= best - EPS:
                continue
            if separated(instance, combination, sinks):
                best, best_set = cost, combination
    status = "optimal" if best_set is not None else "infeasible"
    return CutSolution(
        sorted(best_set or (), key=_order),
        best,
        status,
        "brute_force",
        unavoidable,
        (time.perf_counter() - started) * 1000,
    )
