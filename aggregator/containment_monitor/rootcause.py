"""Root cause v0: backward slice, spectrum ranking, prune level, forward closure.

For an alert on node z, the backward slice follows the alert type's influence
edges (data/session; plus authority for admission denials, resource for leases)
over edges recorded within the window, respecting time: an invocation's inputs
count only up to the moment it produced the output being explained. Candidates
are the slice's invocations and its externally sourced versions. Each is scored by

    ochiai(n) = failed(n) / sqrt(total_failed * (failed(n) + passed(n)))
              + untrusted_prior * [n has untrusted origin]
              + type_prior / (1 + d(n))

where failed/passed count the failing alert subjects and healthy (validator_pass)
subjects in the window whose slices contain n, and d(n) is n's distance to z
along the alert type's characteristic edge (the producer of a bad version is
nearest). Ties go to the earlier, more upstream node.

The recommended prune level is the smallest scope that explains the failures
attributed to the candidate's agent without discarding its healthy outputs:
version -> invocation -> session -> agent -> subtree. The forward closure from
that scope over taint edges is the contamination, including silent contamination
no alert has named.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from .config import MonitorConfig
from .model import (
    FAILURE_KINDS,
    PRIMARY_EDGE,
    SESSION,
    SLICE_EDGES,
    TAINT_EDGE_TYPES,
    Alert,
    inv,
    split,
    ver,
)
from .provenance import ProvenanceGraph

LEVELS = ("version", "invocation", "session", "agent", "subtree")


@dataclass
class Candidate:
    node: str
    score: float
    ochiai: float
    failed: int
    passed: int
    untrusted: bool
    affinity: float
    agent: str | None
    started_ms: int
    path: list[str]
    prune_level: str = "invocation"
    roots: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "node": self.node,
            "score": round(self.score, 6),
            "ochiai": round(self.ochiai, 6),
            "failed": self.failed,
            "passed": self.passed,
            "untrusted": self.untrusted,
            "affinity": round(self.affinity, 6),
            "agent": self.agent,
            "prune_level": self.prune_level,
            "roots": self.roots,
            "path": self.path,
        }


def subject_nodes(graph: ProvenanceGraph, subject: str) -> list[str]:
    """A task subject stands for its invocations (or its dispatcher before any)."""
    kind, identity = split(subject)
    if kind != "task":
        return [subject]
    attempts = graph.invocations_of_task(identity)
    if attempts:
        return [inv(a) for a in attempts]
    parent = graph.parent_attempt(identity)
    return [inv(parent)] if parent else [subject]


def backward_slice(
    graph: ProvenanceGraph, subjects: list[str], etypes, since_ms: int
) -> dict[str, tuple[str | None, int]]:
    """Node -> (next hop toward the subject, distance).

    Each invocation carries a bound: the latest time its influence matters (when
    it produced the output on the path). Its inputs used after that bound, and the
    session predecessors that started after it, are not in the slice.
    """
    found: dict[str, tuple[str | None, int]] = {s: (None, 0) for s in subjects}
    bound: dict[str, float] = {s: math.inf for s in subjects}
    queue = deque(subjects)
    while queue:
        node = queue.popleft()
        for source, etype, at in graph.predecessors(node, etypes, since_ms):
            if (
                node.startswith("inv:")
                and etype in TAINT_EDGE_TYPES
                and at > bound[node]
            ):
                continue
            if not source.startswith("inv:"):
                limit = math.inf  # a version's content is fixed when generated
            elif etype == SESSION:
                limit = bound[node]  # memory may hold anything up to that point
            else:
                limit = at  # the producer's influence ends when it generated this
            if source not in found or limit > bound[source]:
                found.setdefault(source, (node, found[node][1] + 1))
                bound[source] = limit
                queue.append(source)
    return found


def _distance(graph, subjects, etype, since_ms) -> dict[str, int]:
    return {
        node: hop[1]
        for node, hop in backward_slice(graph, subjects, (etype,), since_ms).items()
    }


def _candidate_info(
    graph: ProvenanceGraph, config: MonitorConfig, node: str
) -> tuple[str | None, int] | None:
    """(agent, time) for candidate kinds, else None. Externally submitted requests
    are candidates only when configured as untrusted; a version whose producer
    was never observed is a candidate of unknown origin."""
    kind, identity = split(node)
    if kind == "inv":
        row = graph.invocation(identity)
        return (row["agent_id"], row["started_ms"]) if row else None
    if kind == "ver":
        row = graph.version_row(identity)
        if (
            row
            and row["producer_attempt"] is None
            and (not row["external"] or config.external_requests_untrusted)
        ):
            return None, row["generated_ms"] or 0
    return None


def _untrusted(
    graph: ProvenanceGraph, config: MonitorConfig, node: str, agent: str | None
) -> bool:
    if agent is not None and agent in config.untrusted_agents:
        return True
    if not config.external_requests_untrusted:
        return False
    kind, identity = split(node)
    if kind == "ver":
        return True
    return any(
        (row := graph.version_row(split(source)[1])) is not None and row["external"]
        for source, _, _ in graph.predecessors(node, ("data",))
        if source.startswith("ver:")
    )


def rank(
    graph: ProvenanceGraph, config: MonitorConfig, alert: Alert, *, now_ms: int
) -> list[Candidate]:
    since = max(0, alert.observed_at_ms - config.root_cause_window_ms)
    etypes = SLICE_EDGES.get(alert.kind, SLICE_EDGES["operator"])
    subjects = subject_nodes(graph, alert.subject)
    main = backward_slice(graph, subjects, etypes, since)
    failing = {alert.subject}
    passing: set[str] = set()
    for kind, subject in graph.db.execute(
        "SELECT kind,subject FROM containment_alerts WHERE observed_ms>=? AND observed_ms<=?",
        (since, max(now_ms, alert.observed_at_ms)),
    ):
        if kind in FAILURE_KINDS:
            failing.add(subject)
        elif kind == "validator_pass":
            passing.add(subject)
    passing -= failing
    failed_slices = [
        set(backward_slice(graph, subject_nodes(graph, s), etypes, since))
        for s in sorted(failing)
    ]
    passed_slices = [
        set(backward_slice(graph, subject_nodes(graph, s), etypes, since))
        for s in sorted(passing)
    ]
    distance = _distance(graph, subjects, PRIMARY_EDGE.get(alert.kind, "data"), since)
    total_failed = len(failed_slices)
    # Score every candidate of every failing slice: the prune level of the top
    # candidates looks at failures elsewhere; the ranking lists this alert's slice.
    scored: dict[str, Candidate] = {}
    for node in set(main).union(*failed_slices):
        info = _candidate_info(graph, config, node)
        if info is None:
            continue
        agent, started = info
        failed = sum(node in s for s in failed_slices)
        passed = sum(node in s for s in passed_slices)
        ochiai = failed / math.sqrt(total_failed * (failed + passed)) if failed else 0.0
        untrusted = _untrusted(graph, config, node, agent)
        affinity = 1.0 / (1 + distance[node]) if node in distance else 0.0
        score = (
            ochiai + config.untrusted_prior * untrusted + config.type_prior * affinity
        )
        path: list[str] = []
        if node in main:
            path, hop = [node], main[node][0]
            while hop is not None:
                path.append(hop)
                hop = main[hop][0]
        scored[node] = Candidate(
            node,
            score,
            ochiai,
            failed,
            passed,
            untrusted,
            affinity,
            agent,
            started,
            path,
        )

    def order(candidate: Candidate) -> tuple:
        # Ties go to the earlier, more upstream node.
        return (-round(candidate.score, 9), candidate.started_ms, candidate.node)

    ranking = sorted((scored[n] for n in main if n in scored), key=order)
    top = ranking[: config.top_k]
    everyone = sorted(scored.values(), key=order)
    for candidate in top:
        candidate.prune_level, candidate.roots = prune(
            graph, config, candidate, everyone, alert, since
        )
    return top


def _org_children(
    graph: ProvenanceGraph, config: MonitorConfig, agent: str
) -> set[str]:
    if config.org_parent:
        return {child for child, parent in config.org_parent.items() if parent == agent}
    return {
        row[0]
        for row in graph.db.execute(
            "SELECT DISTINCT recipient_id FROM containment_tasks WHERE sender_id=? "
            "AND recipient_id IS NOT NULL AND recipient_id<>?",
            (agent, agent),
        )
    }


def subtree(graph: ProvenanceGraph, config: MonitorConfig, agent: str) -> set[str]:
    out, queue = {agent}, [agent]
    while queue:
        for child in _org_children(graph, config, queue.pop()):
            if child not in out:
                out.add(child)
                queue.append(child)
    return out


def _attempts(
    graph: ProvenanceGraph, agents: set[str], since: int, context: str | None = None
) -> list[str]:
    marks = ",".join("?" * len(agents))
    rows = graph.db.execute(
        f"SELECT attempt_id FROM containment_invocations WHERE agent_id IN ({marks}) "
        "AND started_ms>=? AND (? IS NULL OR context_id=?) ORDER BY started_ms",
        (*sorted(agents), since, context, context),
    )
    return [inv(row[0]) for row in rows]


def prune(
    graph: ProvenanceGraph,
    config: MonitorConfig,
    candidate: Candidate,
    ranked: list[Candidate],
    alert: Alert,
    since: int,
) -> tuple[str, list[str]]:
    kind, identity = split(candidate.node)
    if kind == "ver":
        return "version", [candidate.node]
    if alert.subject.startswith("ver:") and candidate.passed > 0:
        version = graph.version_row(split(alert.subject)[1])
        if version and version["producer_attempt"] == identity:
            return "version", [alert.subject]
    row = graph.invocation(identity)
    agent, context = row["agent_id"], row["context_id"]
    purely_failing = [
        c for c in ranked if c.failed and not c.passed and c.node.startswith("inv:")
    ]
    peers = [c for c in purely_failing if c.agent == agent]
    downstream = forward_closure(graph, [candidate.node])
    family = subtree(graph, config, agent) - {agent}
    independent = [
        c for c in purely_failing if c.agent in family and c.node not in downstream
    ]
    if independent:
        return "subtree", _attempts(graph, family | {agent}, since)
    if len(peers) <= 1:
        return "invocation", [candidate.node]
    contexts = {graph.invocation(split(c.node)[1])["context_id"] for c in peers}
    if contexts == {context} and context is not None:
        return "session", _attempts(graph, {agent}, since, context)
    return "agent", _attempts(graph, {agent}, since)


def forward_closure(graph: ProvenanceGraph, roots: list[str]) -> set[str]:
    """Time-respecting contamination of the roots (see ``provenance.taint_step``)."""
    return set(graph.closure(roots))


def contamination(graph: ProvenanceGraph, roots: list[str], failing: set[str]) -> dict:
    """Forward closure summary: what is tainted, what is still live, what is silent."""
    closure = forward_closure(graph, roots)
    by_kind: dict[str, list[str]] = {}
    for node in sorted(closure):
        by_kind.setdefault(split(node)[0], []).append(node)
    live = {
        "running": sorted(a for a in graph.running if inv(a) in closure),
        "pending_tasks": sorted(
            t
            for t, p in graph.pending.items()
            if (p.request_version and ver(p.request_version) in closure)
            or (p.sender_attempt and inv(p.sender_attempt) in closure)
        ),
        "pending_results": sorted(v for v in graph.results if ver(v) in closure),
    }
    committed = [
        node
        for node in by_kind.get("fx", [])
        if (row := graph.effect_row(split(node)[1]))
        and row["state"] in {"committed", "observed"}
    ]
    silent = sorted(
        node
        for node in closure - set(roots) - failing
        if node.startswith(("inv:", "fx:"))
    )
    return {
        "size": len(closure),
        "invocations": len(by_kind.get("inv", [])),
        "versions": len(by_kind.get("ver", [])),
        "effects": len(by_kind.get("fx", [])),
        "committed_effects": committed,
        "silent": silent,
        "live": live,
    }
