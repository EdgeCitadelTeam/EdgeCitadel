"""Exact minimum cut with grouped actions (grouped s-t cut / minimum label cut).

An instance is a directed graph whose edges are removable by zero or more actions
(ANY semantics: one covering action suffices), action costs, sources and sinks.
Deadlines are applied before this module: an action that cannot land in time is
simply not listed on that edge, so a partially feasible group removes only its
feasible members while its full cost is paid (theory note section 5).

Algorithm (theory note sections 4.2-4.8):
1. Sinks reachable through uncuttable edges only are unavoidable; they are
   reported and dropped (no cut exists for them).
2. Only edges on some source-to-sink path matter, and the remaining sinks are
   contracted into the super-sink (reaching any sink is reaching it).
3. An edge covered by several actions is replaced by a private series path with
   one action per segment, so every edge has at most one action.
4. A group whose edges share a tail (or a head) is gated at that vertex; a group
   with a member every other member's path must cross is gated by that edge.
   Gated groups become one gadget edge each: the whole instance is ONE max-flow.
5. Ungated groups (k of them) are enumerated: 2^k max-flows on the gated residual.
   Above ``enum_limit``, the compact ILP runs when HiGHS is importable; otherwise
   the greedy cut is returned with status ``approximate``.

Max-flow is Dinic's algorithm in pure Python; the aggregator has no graph library.
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
    status: str  # optimal | trivial | infeasible | approximate
    method: str  # trivial | gadget | enumeration | ilp | greedy | brute_force
    unavoidable: frozenset[str]
    solve_ms: float
    relevant_nodes: int = 0
    relevant_edges: int = 0
    ungated: int = 0
    lower_bound: float | None = None
    info: dict = field(default_factory=dict)


def _order(action: Hashable) -> str:
    return str(action)


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
@dataclass
class _Prepared:
    edges: list[tuple[str, str, Hashable | None]]  # at most one action per edge
    bundles: dict[Hashable, list[int]]
    gates: dict[Hashable, tuple[str, str]]  # action -> ("out"|"in", vertex)
    ungated: list[Hashable]
    sources: frozenset[str]
    sinks: frozenset[str]
    costs: dict[Hashable, float]


def _prepare(
    instance: CutInstance, sinks: frozenset[str], indices: list[int]
) -> _Prepared:
    edges: list[tuple[str, str, Hashable | None]] = []
    for index in indices:
        tail, head, actions = instance.edges[index]
        # Reaching any sink is reaching the super-sink: contract the sinks. Paths
        # stop there, and every group of last hops into sinks becomes gated at SNK
        # (e.g. an admission block over several per-flow boundaries).
        if tail in sinks:
            continue
        if head in sinks:
            head = SNK
        if len(actions) <= 1:
            edges.append((tail, head, actions[0] if actions else None))
            continue
        # Series split (theory 2.1(c)): one private segment per covering action.
        ordered = sorted(actions, key=_order)
        hops = [
            tail,
            *[f"\x00split:{index}:{i}" for i in range(len(ordered) - 1)],
            head,
        ]
        for i, action in enumerate(ordered):
            edges.append((hops[i], hops[i + 1], action))
    bundles: dict[Hashable, list[int]] = {}
    for position, (_, _, action) in enumerate(edges):
        if action is not None:
            bundles.setdefault(action, []).append(position)
    in_degree: dict[str, int] = {}
    for _, head, _ in edges:
        in_degree[head] = in_degree.get(head, 0) + 1
    gates: dict[Hashable, tuple[str, str]] = {}
    ungated: list[Hashable] = []
    for action, members in list(bundles.items()):
        if len(members) == 1:
            continue
        tails = {edges[i][0] for i in members}
        heads = {edges[i][1] for i in members}
        if len(tails) == 1:
            gates[action] = ("out", next(iter(tails)))
            continue
        if len(heads) == 1:
            gates[action] = ("in", next(iter(heads)))
            continue
        # Single-edge gate (theory G3): a member g whose head is entered only by g
        # and is the tail of every other member lies on every path meeting the
        # group. By substitutability (Lemma 4.1) the group may remove just g.
        dominant = [
            g
            for g in members
            if in_degree.get(edges[g][1], 0) == 1
            and all(edges[e][0] == edges[g][1] for e in members if e != g)
        ]
        if dominant:
            for e in members:
                if e != dominant[0]:
                    edges[e] = (edges[e][0], edges[e][1], None)
            bundles[action] = [dominant[0]]
        else:
            ungated.append(action)
    ungated.sort(key=_order)
    return _Prepared(
        edges, bundles, gates, ungated, instance.sources, sinks, dict(instance.costs)
    )


def _gated_flow(prepared: _Prepared, removed: set[int]) -> tuple[float, list[Hashable]]:
    """One max-flow on the chain gadget; ungated actions not chosen are uncuttable."""
    net = FlowNetwork()
    ungated = set(prepared.ungated)
    placed: set[Hashable] = set()
    for position, (tail, head, action) in enumerate(prepared.edges):
        if position in removed:
            continue
        if action is None or action in ungated:
            net.add(tail, head, INF)
        elif action in prepared.gates:
            side, vertex = prepared.gates[action]
            gadget = f"\x00gate:{_order(action)}"
            if action not in placed:
                placed.add(action)
                if side == "out":
                    net.add(vertex, gadget, prepared.costs[action], action)
                else:
                    net.add(gadget, vertex, prepared.costs[action], action)
            if side == "out":
                net.add(gadget, head, INF)
            else:
                net.add(tail, gadget, INF)
        else:
            net.add(tail, head, prepared.costs[action], action)
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
        return result([], 0.0, "trivial", "trivial")
    prepared = _prepare(instance, sinks, indices)
    k = len(prepared.ungated)
    if k == 0:
        value, actions = _gated_flow(prepared, set())
        return result(actions, value, "optimal", "gadget")
    if k > enum_limit:
        if use_ilp and ilp_available():
            solution = ilp(instance)
            if solution is not None:
                solution.solve_ms = (time.perf_counter() - started) * 1000
                solution.ungated = k
                return solution
        approximate = greedy(instance)
        approximate.status = "approximate"
        approximate.ungated = k
        return approximate
    best, best_actions = INF, []
    costs = [prepared.costs[action] for action in prepared.ungated]
    for mask in range(1 << k):
        chosen = [prepared.ungated[i] for i in range(k) if mask >> i & 1]
        group_cost = sum(costs[i] for i in range(k) if mask >> i & 1)
        if group_cost >= best - EPS:
            continue
        removed = {
            position for action in chosen for position in prepared.bundles[action]
        }
        value, actions = _gated_flow(prepared, removed)
        if group_cost + value < best - EPS:
            best, best_actions = group_cost + value, [*chosen, *actions]
    if best == INF:
        return result([], INF, "infeasible", "enumeration", ungated=k)
    return result(best_actions, best, "optimal", "enumeration", ungated=k)


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
