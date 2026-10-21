"""Projection Pi_h(G_P, X_t) -> G_C(t): live state plus authority-bounded next events.

Units are the live executions: running invocations (key = execution attempt) and
dispatched tasks whose invocation has not started (key = ``task:<id>``). Each unit
is node-split ``in:<key> -> out:<key>``; holding its task at its node removes the
split edge (agentd then refuses its claim, dispatch, completion and outbox).

Declared or recorded flows:
- ``req:<task> -> in:<task unit>``: an in-flight request (source if tainted);
- ``out:<child> -> in:<parent>`` / ``res:<version> -> in:<parent>``: a future or
  generated result, parked by holding the child task at the parent's node;
- ``ctx:<agent>:<context>`` and ``out:u -> in:t``: conversation continuation, into
  the agent's pending tasks of that context and its future (new) invocations;
- ``pe:<effect> -> fx:<effect>``: a pending effect (protected ones are sinks).

One layer of possible next events per unit goes through a STAR hub, one per
credential scope (agent, effective recipients, effect classes), instead of
all-to-all edges. The actuators match the star's edges:

    out:u -(egress u)-> Eg:u -(revoke credential a)-> S:<credential>
    S:<credential> -> A:a:b -(revoke grant a>b)-> M:a:b -> Fin:a>b -(admission b)-> Fout:a>b
    S:<credential> -> A:a:fx:c -(revoke grant a>fx:c)-> K:a:c

The egress block is per requester invocation; the grant filter is per (agent,
target), which is how agentd scopes a credential: the agent's grant, checked at
the sender's node when it dispatches. Inherited scopes narrow which targets a hub
reaches but have no separate actuator, so all of an agent's hubs share its gates.

In the phase-1 projection (``freeze``) the egress edge carries the reversible
``hold:egress`` instead of ``revoke:egress``: phase 1 places holds only. An
egress stopped by an action in force keeps that action in either projection
(a phase-1 admission label is held in the phase-2 graph too).

Horizon ``h``: ``Fout:a>b`` is a new invocation of b requested by a. With h = 1 it
is the horizon boundary (a sink for ``boundary="sink"``, a dead end for
``"defer"``); with h = k new invocations reached through k-1 levels are expanded
with b's own grant, through one hub per credential and level.

Sinks are per flow, never shared by flows of different origin: one boundary per
(requesting agent, target) and per (agent, effect class), so a flow that cannot be
stopped in time makes only its own sink unavoidable. An admission block at b is
then a grouped action over b's boundaries (solved exactly by the grouped solver).

The offered vocabulary is vertex-local by design: every action removes edges
of one invocation (task hold, egress revocation, result park), one effect, one
agent's credential, one grant pair or one agent's admission. No action removes
a whole delegation subtree across invocations: ``revoke:authority`` stays
mapped in control (an operator may send it) but is never offered here, because
its removal set spans every descendant's sites with different latencies and
has no gate at one port once deadlines split it (theory 3.3, 5.1(d)).
Descendants are cut through their own per-invocation actions and taint-aware
admission. Whether a grouped action is actually gated on the instance is
checked by the solver, not assumed.

Deadlines are earliest arrival times (time-dependent Dijkstra over lower-bound
delays); an action is feasible iff ``t_now + tau + L(site)`` is strictly earlier
or it is already in force (``graph.ControlGraph.masks``). Two splits keep that
exact where flows of different timing share an element:
- early windows: a unit's emissions before its own hold/egress lands are routed
  through private copies, one per window between the deadline thresholds of the
  downstream actions (result park at the parent, admission at each target), each
  with its own hub, gates and sinks;
- doomed arrivals: where flows still merge before an action (the expansion hub of
  new invocations of b, h >= 2), arrivals that precede the landing of b's gates
  are moved to a ``@doomed`` hub copy with its own gates and sinks, so one
  unstoppable arrival cannot make those gates look infeasible for later ones.
"""

from __future__ import annotations

import hashlib
import heapq
import math
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from .config import MonitorConfig
from .costs import CostModel
from .graph import ControlGraph, Edge
from .model import (
    REVOKE,
    Action,
    TaintedOutputs,
    fx,
    hold_effect,
    hold_egress,
    hold_task,
    inv,
    revoke_admission,
    revoke_credential,
    revoke_egress,
    revoke_grant,
    ver,
)
from .provenance import ProvenanceGraph

# The op:kind pairs a node enforces (its answer to describe).
Capabilities = Callable[[str], frozenset[str]]

INF = math.inf
MAX_SPLIT_ROUNDS = 4
MAX_DOOMED_ROUNDS = 8
# Mapped in control, never offered by the projection (see the module docstring;
# cancel is terminal and has no edge of its own).
NOT_OFFERED = frozenset({"revoke:authority", "cancel:task"})


@dataclass(frozen=True)
class Credential:
    agent: str
    recipients: frozenset[str]
    effects: frozenset[str]

    @property
    def id(self) -> str:
        material = (
            ",".join(sorted(self.recipients)) + "|" + ",".join(sorted(self.effects))
        )
        return f"{self.agent}#{hashlib.sha256(material.encode()).hexdigest()[:10]}"


@dataclass
class Unit:
    key: str
    agent: str
    node: str | None
    task_id: str | None
    context: str | None
    parent: str | None  # live parent unit that consumes this unit's result
    start_ms: float
    emit_ms: float
    pending: bool
    tainted: bool
    recipients: frozenset[str]
    effects: frozenset[str]

    def reaches(self, target: str) -> bool:
        return target in self.recipients

    @property
    def credential(self) -> Credential:
        return Credential(self.agent, self.recipients, self.effects)


class ScopeMirror:
    """Effective authority of invocations, and where a decision must be enforced.

    agentd exports each invocation's effective authority on ``run/started``: its
    grant intersected with what its task inherited (recipients and effect rules).
    Only where that export is missing or null (lists over 8 KiB) does the monitor
    fall back to configuration: the configured grant intersected with the
    dispatcher's scope, as agentd computes it. A task that has not started yet is
    bounded by its dispatcher's scope, since a child never exceeds its parent.
    An agent with no configured grant may reach every known agent (sound).
    """

    def __init__(self, graph: ProvenanceGraph, config: MonitorConfig):
        self.graph = graph
        self.config = config
        self.protected = config.protected_effect_classes
        self.known = frozenset(graph.known_agents())
        self._tasks: dict[str, tuple[frozenset[str], frozenset[str]]] = {}

    def grant(self, agent: str) -> frozenset[str]:
        return self.config.grants.get(agent, self.known)

    def effects(self, agent: str) -> frozenset[str]:
        """Protected effect classes an agent's configured grant allows."""
        return self.config.effect_scopes.get(agent, self.protected) & self.protected

    def of_attempt(self, attempt: str | None) -> tuple[frozenset[str], frozenset[str]]:
        """(recipients, protected effect classes) of an invocation."""
        row = self.graph.invocation(attempt) if attempt else None
        if row is None:
            return self.known, self.protected
        exported = self.graph.authority(attempt)
        if row["task_id"] is None:
            fallback = (self.grant(row["agent_id"]), self.effects(row["agent_id"]))
        else:
            fallback = self.of_task(row["task_id"], row["agent_id"])
        if exported is None:
            return fallback
        recipients, effects = fallback
        if exported["recipients"] is not None:
            recipients = frozenset(exported["recipients"])
        if exported["effects"] is not None:
            effects = (
                frozenset(rule["effect_class"] for rule in exported["effects"])
                & self.protected
            )
        return recipients, effects

    def of_task(
        self, task_id: str, agent: str
    ) -> tuple[frozenset[str], frozenset[str]]:
        """A task's scope from configuration, bounded by its dispatcher's scope."""
        if task_id in self._tasks:
            return self._tasks[task_id]
        own = (self.grant(agent), self.effects(agent))
        self._tasks[task_id] = own  # guards malformed cycles
        parent = self.graph.parent_attempt(task_id)
        if parent is not None:
            recipients, effects = self.of_attempt(parent)
            own = (own[0] & recipients, own[1] & effects)
        self._tasks[task_id] = own
        return own

    def sites(self, action: Action) -> tuple[str, ...]:
        """The nodes a decision is sent to, its own node first.

        agentd's revocations and filters bind only the node that applies them.
        ``grant a>b``: a's node refuses the dispatch, b's node parks a request
        already published. ``admission b``: b's node parks deliveries and every
        node of an agent that may dispatch to b refuses the dispatch.
        ``authority T``: every node that may hold T's descendants. Holds,
        cancels and egress or credential revocations act at one node.
        """
        others: set[str | None] = set()
        if action.op == REVOKE and action.kind == "grant":
            _, _, target = action.object_id.partition(">")
            if not target.startswith("fx:"):
                others.add(self.graph.node_of(target))
        elif action.op == REVOKE and action.kind == "admission":
            others.update(
                self.graph.node_of(agent)
                for agent in self.known
                if action.object_id in self.grant(agent)
            )
        elif action.op == REVOKE and action.kind == "authority":
            row = self.graph.task_row(action.object_id)
            reach = {row["recipient_id"]} if row and row["recipient_id"] else set()
            frontier = list(reach)
            while frontier:
                for target in self.grant(frontier.pop()) - reach:
                    reach.add(target)
                    frontier.append(target)
            others.update(self.graph.node_of(agent) for agent in reach)
        return (action.node_id, *sorted(n for n in others if n and n != action.node_id))


def live_units(
    graph: ProvenanceGraph, config: MonitorConfig, now_ms: float, scopes: ScopeMirror
) -> dict[str, Unit]:
    units: dict[str, Unit] = {}
    for attempt, live in graph.running.items():
        parent = graph.parent_attempt(live.task_id)
        start = float(live.started_ms)
        units[attempt] = Unit(
            attempt,
            live.agent_id,
            live.node_id,
            live.task_id,
            live.context_id,
            parent if parent in graph.running else None,
            start,
            max(now_ms, start + config.min_emission(live.agent_id)),
            False,
            inv(attempt) in graph.taint,
            *scopes.of_attempt(attempt),
        )
    for task_id, pending in graph.pending.items():
        node = pending.recipient_node or graph.node_of(pending.recipient_id)
        if pending.arrived_ms is not None:
            arrival = float(pending.arrived_ms)
        else:
            arrival = pending.created_ms + config.delivery(pending.sender_node, node)
        start = max(now_ms, arrival + config.min_start_ms)
        key = f"task:{task_id}"
        units[key] = Unit(
            key,
            pending.recipient_id,
            node,
            task_id,
            pending.context_id,
            pending.sender_attempt if pending.sender_attempt in graph.running else None,
            start,
            start + config.min_emission(pending.recipient_id),
            True,
            graph.tainted_request(pending),
            *scopes.of_task(task_id, pending.recipient_id),
        )
    return units


class _Builder:
    def __init__(
        self,
        graph,
        config,
        units,
        scopes,
        cg,
        capabilities,
        now_ms,
        tau,
        static,
        in_force,
        freeze=False,
    ):
        self.graph, self.config, self.units, self.scopes, self.cg = (
            graph,
            config,
            units,
            scopes,
            cg,
        )
        self.capabilities, self.now, self.tau = capabilities, now_ms, tau
        self.static, self.in_force = static, in_force
        self.freeze = freeze  # phase 1: reversible holds only
        # (flow, target, tag) -> horizon depth of that new-invocation boundary
        self.depth: dict[tuple[str, str, str], int] = {}
        self.expanded: set[tuple[str, str, str]] = set()

    def allowed(self, action: Action | None) -> Action | None:
        """The action if every node it must reach enforces it, else None."""
        if action is not None and action.capability in NOT_OFFERED:
            raise ValueError("action_not_offered")
        if action is None or not all(
            action.capability in self.capabilities(site)
            for site in self.scopes.sites(action)
        ):
            return None
        return action

    def t_ap(self, node: str | None) -> float:
        if self.static:
            return self.now
        return self.now + self.tau + self.config.control_latency(node) if node else INF

    def node_of(self, agent: str) -> str | None:
        return self.graph.node_of(agent)

    # -- possible next events ------------------------------------------------------
    def spoke(self, unit: Unit, tail: str, tag: str) -> None:
        if not unit.recipients and not unit.effects:
            return
        cg, site = self.cg, unit.node
        egress = cg.node(f"Eg:{unit.key}{tag}", kind="egress", unit=unit.key)
        block = None
        if site and unit.task_id:
            # An egress already stopped keeps its action: a revocation, or the
            # admission label of phase 1 (a hold) that the phase-2 graph must
            # see as held too. Otherwise phase 1 offers holds, phase 2 the
            # revocation.
            revoked = revoke_egress(site, unit.task_id)
            label = hold_egress(site, unit.task_id)
            if revoked in self.in_force:
                block = revoked
            elif label in self.in_force:
                block = label
            else:
                block = self.allowed(label if self.freeze else revoked)
        cg.edge(tail, egress, "egress", action=block, declared=False)
        self.credential_edge(egress, unit.credential, tag, depth=1)

    def credential_edge(
        self,
        tail: str,
        credential: Credential,
        tag: str,
        depth: int,
        fn: tuple = ("id",),
    ) -> None:
        site = self.node_of(credential.agent)
        action = (
            self.allowed(revoke_credential(site, credential.agent)) if site else None
        )
        self.cg.edge(
            tail,
            self.hub(credential, tag, depth),
            "spoke",
            fn=fn,
            action=action,
            declared=False,
        )

    def hub(self, credential: Credential, tag: str, depth: int) -> str:
        cg, agent = self.cg, credential.agent
        name = f"S:{credential.id}{tag}"
        if name in cg.nodes:
            return name
        cg.node(
            name, kind="hub", agent=agent, credential=credential, tag=tag, depth=depth
        )
        site = self.node_of(agent)
        for target in sorted(credential.recipients):
            gate = f"A:{agent}:{target}{tag}"
            if gate not in cg.nodes:
                cg.node(gate, kind="gate", agent=agent)
                granted = cg.node(f"M:{agent}:{target}{tag}", kind="granted")
                action = (
                    self.allowed(revoke_grant(site, agent, target)) if site else None
                )
                cg.edge(gate, granted, "gate", action=action, declared=False)
                delay = (
                    self.config.delivery(site, self.node_of(target))
                    + self.config.min_start_ms
                )
                cg.edge(
                    granted,
                    self.arrival(agent, target, tag, depth),
                    "deliver",
                    fn=("plus", delay),
                    declared=False,
                )
            cg.edge(name, gate, "hub", declared=False)
        for effect_class in sorted(credential.effects):
            gate = f"A:{agent}:fx:{effect_class}{tag}"
            if gate not in cg.nodes:
                cg.node(gate, kind="gate", agent=agent)
                sink = cg.node(f"K:{agent}:{effect_class}{tag}", kind="effect_class")
                action = (
                    self.allowed(revoke_grant(site, agent, f"fx:{effect_class}"))
                    if site
                    else None
                )
                # The effect gateway authorizes at emission; commit happens later.
                cg.edge(gate, sink, "gate", action=action, declared=False)
                cg.sinks.add(sink)
            cg.edge(name, gate, "hub", declared=False)
        return name

    def arrival(self, sender: str, target: str, tag: str, depth: int) -> str:
        """``Fin`` (arrival) and ``Fout`` (new invocation) of target requested by sender."""
        cg = self.cg
        flow = f"{sender}>{target}{tag}"
        entry, created = f"Fin:{flow}", f"Fout:{flow}"
        if entry not in cg.nodes:
            cg.node(entry, kind="arrival", agent=target, tag=tag)
            cg.node(created, kind="new_invocation", agent=target, tag=tag)
            site = self.node_of(target)
            action = self.allowed(revoke_admission(site, target)) if site else None
            cg.edge(entry, created, "admission", action=action, declared=False)
        key = (flow, target, tag)
        self.depth[key] = min(depth, self.depth.get(key, depth))
        return entry

    def expand(self) -> None:
        """Expand new invocations below the horizon, then mark the boundary."""
        queue = [(depth, *key) for key, depth in self.depth.items()]
        heapq.heapify(queue)
        while queue:
            depth, flow, target, tag = heapq.heappop(queue)
            key = (flow, target, tag)
            if (
                key in self.expanded
                or depth >= self.config.horizon
                or self.depth[key] != depth
            ):
                continue
            self.expanded.add(key)
            before = dict(self.depth)
            credential = Credential(
                target, self.scopes.grant(target), self.scopes.effects(target)
            )
            # A new invocation has no task yet: only its agent's credential applies,
            # from its earliest emission on.
            self.credential_edge(
                f"Fout:{flow}",
                credential,
                f"{tag}@new",
                depth + 1,
                fn=("plus", self.config.min_emission(target)),
            )
            for other, value in self.depth.items():
                if before.get(other) != value:
                    heapq.heappush(queue, (value, *other))
        for key in self.depth:
            created = f"Fout:{key[0]}"
            if key not in self.expanded and self.config.boundary == "sink":
                self.cg.sinks.add(created)
                self.cg.nodes[created]["boundary"] = True

    def tainted_outputs(self) -> None:
        """Below ``tainted_output_value`` 1: a shared-memory service agent's
        future outputs come from tainted memory once taint reaches its
        context, through any of its units or its tainted context. That outcome
        is a priced sink ``T:<agent>`` (loss: 1 - value per expected future
        request, each worth ``future_request_value``), so a cut may pay it or
        stop the taint before the context (``graph.Edge.penalty``)."""
        cg, config = self.cg, self.config
        loss = 1.0 - config.tainted_output_value
        if loss <= 0:
            return
        for agent, expected in sorted(config.future_requests.items()):
            if expected <= 0 or config.session_of(agent, None) != "*":
                continue
            token = TaintedOutputs(agent)
            sink = cg.node(f"T:{agent}", kind="tainted_outputs", agent=agent)
            cg.penalty_costs[token] = loss * config.future_request_value * expected
            # A unit taints the context when it starts with tainted input: a
            # queued one is stopped in time by its hold if that lands before
            # it can start; a live one already has.
            for unit in self.units.values():
                if unit.agent != agent:
                    continue
                hold = (
                    self.allowed(hold_task(unit.node, unit.task_id))
                    if unit.pending and unit.node and unit.task_id
                    else None
                )
                edge = cg.edge(
                    f"in:{unit.key}",
                    sink,
                    "tainted_outputs",
                    fn=("start", unit.key),
                    declared=False,
                )
                edge.penalty = (token, hold)
            if (agent, "*") in self.graph.tainted_sessions:
                edge = cg.edge(
                    f"ctx:{agent}:*", sink, "tainted_outputs", declared=False
                )
                edge.penalty = (token, None)

    def build(self) -> None:
        cg, graph, units, config = self.cg, self.graph, self.units, self.config
        for unit in units.values():
            entry = cg.node(f"in:{unit.key}", kind="unit_in", unit=unit.key)
            exit_ = cg.node(f"out:{unit.key}", kind="unit_out", unit=unit.key)
            hold = (
                self.allowed(hold_task(unit.node, unit.task_id))
                if unit.node and unit.task_id
                else None
            )
            cg.edge(entry, exit_, "split", fn=("emit", unit.key), action=hold)
            if unit.pending:
                if unit.tainted:
                    request = cg.node(
                        f"req:{unit.task_id}", kind="request", unit=unit.key
                    )
                    cg.sources.add(request)
                    cg.edge(request, entry, "request", fn=("start", unit.key))
                session = config.session_of(unit.agent, unit.context)
                if session and (unit.agent, session) in graph.tainted_sessions:
                    context = cg.node(f"ctx:{unit.agent}:{session}", kind="context")
                    cg.sources.add(context)
                    cg.edge(context, entry, "session", fn=("start", unit.key))
            elif unit.tainted:
                cg.sources.add(entry)
            if unit.parent is not None:
                parent = units[unit.parent]
                park = (
                    self.allowed(hold_task(parent.node, unit.task_id))
                    if parent.node and unit.task_id
                    else None
                )
                cg.edge(
                    exit_,
                    f"in:{parent.key}",
                    "result",
                    fn=("plus", config.delivery(unit.node, parent.node)),
                    action=park,
                )
            self.spoke(unit, exit_, "")
        followers: dict[tuple[str, str], list[Unit]] = {}
        for unit in units.values():
            session = config.session_of(unit.agent, unit.context)
            if unit.pending and session:
                followers.setdefault((unit.agent, session), []).append(unit)
        for unit in units.values():
            session = config.session_of(unit.agent, unit.context)
            if not unit.pending and session:
                for follower in followers.get((unit.agent, session), ()):
                    cg.edge(
                        f"out:{unit.key}",
                        f"in:{follower.key}",
                        "session",
                        fn=("start", follower.key),
                    )
        for agent, context in sorted(graph.tainted_sessions):
            # Any future task of this agent in this conversation resumes tainted
            # memory, including tasks not dispatched yet: a new invocation of it.
            # Under whole-context taint (a shared-memory agent, context "*")
            # every future invocation of the agent does, whoever requests it:
            # its next events (one level, whatever the horizon) are modelled,
            # so the context's protected classes are sinks that (C2) covers
            # (``enforcement.context_covers``) although it has no unit to label.
            whole = context == "*"
            source = cg.node(f"ctx:{agent}:{context}", kind="context")
            cg.nodes[source].update(agent=agent, whole=whole)
            cg.sources.add(source)
            cg.edge(
                source,
                self.arrival(f"ctx:{context}", agent, "", 0 if whole else 1),
                "session",
                declared=False,
            )
        self.tainted_outputs()
        for result in graph.results.values():
            parent = units.get(result.parent_attempt)
            if parent is None:
                continue
            producer = (
                graph.invocation(result.producer_attempt)
                if result.producer_attempt
                else None
            )
            name = cg.node(f"res:{result.version_id}", kind="result_version")
            park = (
                self.allowed(hold_task(parent.node, result.task_id))
                if parent.node
                else None
            )
            arrival = result.generated_ms + config.delivery(
                producer["node_id"] if producer else None, parent.node
            )
            cg.edge(
                name, f"in:{parent.key}", "result", fn=("fixed", arrival), action=park
            )
            if ver(result.version_id) in graph.taint:
                cg.sources.add(name)
        for effect in graph.effects.values():
            # The effect gateway keeps a scheduled effect and performs it at its
            # time unless its re-check then refuses it: an effect hold cancels
            # it; while the issuing invocation is live, a hold on its task denies
            # it too (and pauses the issuer). Either removes the flow, so the edge
            # is a series of the two. The gateway performs it even after the
            # issuer ended; then only the effect hold is modelled. Immediate
            # effects are decided at request time and are never pending.
            issuer = units.get(effect.attempt_id)
            requested = cg.node(f"pe:{effect.effect_id}", kind="pending_effect")
            target = cg.node(f"fx:{effect.effect_id}", kind="effect")
            pending = requested
            gate = self.commit_gate(effect)
            if gate is not None:
                # Already refused at its commit by a decision in force.
                pending = cg.node(f"pc:{effect.effect_id}", kind="pending_effect")
                cg.edge(requested, pending, "effect", action=gate)
            hold = self.allowed(hold_effect(effect.node_id, effect.effect_id))
            if issuer is None or not (issuer.node and issuer.task_id):
                cg.edge(
                    pending,
                    target,
                    "effect",
                    fn=("fixed", effect.scheduled_ms),
                    action=hold,
                )
            else:
                gated = cg.node(f"pg:{effect.effect_id}", kind="pending_effect")
                cg.edge(
                    pending,
                    gated,
                    "effect",
                    fn=("fixed", effect.scheduled_ms),
                    action=hold,
                )
                cg.edge(
                    gated,
                    target,
                    "effect",
                    action=self.allowed(hold_task(issuer.node, issuer.task_id)),
                )
            if effect.protected:
                cg.sinks.add(target)
            if fx(effect.effect_id) in graph.taint:
                cg.sources.add(requested)

    def commit_gate(self, effect) -> Action | None:
        """A decision in force that the gateway's commit re-check applies to a
        pending effect: its class revoked for the issuing agent, the agent's
        credential revoked, or the issuer's label (the effect is then held at
        its time, and a protected one is never admitted from the source side).
        Only decisions already in force are modelled here, never offered."""
        row = self.graph.invocation(effect.attempt_id)
        if row is None:
            return None
        agent, node = row["agent_id"], effect.node_id
        gates = [
            revoke_grant(node, agent, f"fx:{effect.effect_class}"),
            revoke_credential(node, agent),
        ]
        if row["task_id"] is not None:
            gates.append(hold_egress(row["node_id"], row["task_id"]))
        return next((gate for gate in gates if gate in self.in_force), None)

    # -- timing ---------------------------------------------------------------------
    def advance(self, fn: tuple, t: float) -> float:
        if t == INF:
            return INF
        kind = fn[0]
        if kind == "id":
            return t
        if kind == "plus":
            return t + fn[1]
        if kind == "fixed":
            return max(t, fn[1])
        if kind == "start":
            return max(t, self.units[fn[1]].start_ms)
        if kind == "emit":
            return max(t, self.units[fn[1]].emit_ms)
        if kind == "emit_after":
            return max(t, self.units[fn[1]].emit_ms, fn[2] + 1)
        raise ValueError(kind)

    def arrival_times(self) -> dict[str, float]:
        """Earliest arrivals; nothing crosses an edge removed by an action in force.

        A confirmed action keeps its edges closed until it is released, and a
        release follows the confirmation of the cut that replaces it, so flows
        behind it have d = infinity for the replacing actions (theory A.6):
        without this, an unheld immediate emission behind a held invocation
        would make every action that could replace the hold look untimely.
        """
        eat = {source: float(self.now) for source in self.cg.sources}
        queue = [(float(self.now), source) for source in self.cg.sources]
        heapq.heapify(queue)
        done: set[str] = set()
        while queue:
            t, node = heapq.heappop(queue)
            if node in done:
                continue
            done.add(node)
            for edge in self.cg.successors(node):
                if edge.action is not None and edge.action in self.in_force:
                    continue
                arrival = self.advance(edge.fn, t)
                if arrival < eat.get(edge.head, INF):
                    eat[edge.head] = arrival
                    heapq.heappush(queue, (arrival, edge.head))
        return eat

    def _thresholds(self, unit: Unit, landing: float) -> list[float]:
        """Emission times after which a downstream action of an early flow lands in time."""
        found = []
        for index in self.cg.out.get(f"out:{unit.key}", ()):
            edge = self.cg.edges[index]
            if edge.kind == "result" and edge.action is not None:
                found.append(self.t_ap(edge.action.node_id) - edge.fn[1])
        site = unit.node
        for target in unit.recipients:
            target_node = self.node_of(target)
            delay = self.config.delivery(site, target_node) + self.config.min_start_ms
            if target_node and self.allowed(revoke_admission(target_node, target)):
                found.append(self.t_ap(target_node) - delay)
            if (
                self.config.horizon > 1
                and site
                and self.allowed(revoke_grant(site, unit.agent, target))
            ):
                found.append(
                    self.t_ap(target_node) - delay - self.config.min_emission(target)
                )
        return found

    def split_early(self, eat: dict[str, float], done: set[str]) -> int:
        """Route emissions that precede the unit's own controls through window copies."""
        if self.static:
            return 0
        added = 0
        for unit in self.units.values():
            entry = f"in:{unit.key}"
            if unit.key in done or eat.get(entry, INF) == INF:
                continue
            split_edge = next(
                self.cg.edges[i]
                for i in self.cg.out[entry]
                if self.cg.edges[i].kind == "split"
            )
            if split_edge.action is not None and split_edge.action in self.in_force:
                continue
            landing = self.t_ap(unit.node)
            first = max(eat[entry], unit.emit_ms)
            if first > landing:
                continue
            done.add(unit.key)
            added += 1
            bounds = [first] + sorted(
                {
                    th + 1
                    for th in self._thresholds(unit, landing)
                    if first < th + 1 <= landing
                }
            )
            outputs = [self.cg.edges[i] for i in self.cg.out.get(f"out:{unit.key}", ())]
            for window, start in enumerate(bounds):
                tag = f"@early:{unit.key}#{window}"
                early = self.cg.node(
                    f"early:{unit.key}#{window}", kind="unit_early", unit=unit.key
                )
                fn = (
                    ("emit", unit.key)
                    if window == 0
                    else ("emit_after", unit.key, start - 1)
                )
                self.cg.edge(entry, early, "early", fn=fn)
                for edge in outputs:
                    if edge.kind == "egress":
                        self.spoke(unit, early, tag)
                    else:
                        self.cg.edge(
                            early, edge.head, edge.kind, fn=edge.fn, action=edge.action
                        )
            split_edge.fn = ("emit_after", unit.key, landing)
        return added

    def split_doomed(self, eat: dict[str, float]) -> int:
        """Move hub arrivals that precede the hub agent's gate landing to a copy."""
        if self.static:
            return 0
        incoming: dict[str, list[Edge]] = {}
        for edge in self.cg.edges:
            attrs = self.cg.nodes.get(edge.head, {})
            if attrs.get("kind") == "hub" and not attrs["tag"].endswith("@doomed"):
                incoming.setdefault(edge.head, []).append(edge)
        moved = 0
        for hub, edges in incoming.items():
            attrs = self.cg.nodes[hub]
            if len(edges) < 2:
                continue
            landing = self.t_ap(self.node_of(attrs["agent"]))
            doomed = [
                e for e in edges if self.advance(e.fn, eat.get(e.tail, INF)) <= landing
            ]
            copy_name = f"S:{attrs['credential'].id}{attrs['tag']}@doomed"
            if not doomed or (
                len(doomed) == len(edges) and copy_name not in self.cg.nodes
            ):
                continue  # not mixed: the shared arrival time is exact for all
            copy = self.hub(
                attrs["credential"], f"{attrs['tag']}@doomed", attrs["depth"]
            )
            for edge in doomed:
                edge.head = copy
                moved += 1
        if moved:
            self.expand()
        return moved


def project(
    graph: ProvenanceGraph,
    config: MonitorConfig,
    *,
    now_ms: float,
    capabilities: Capabilities,
    static: bool = False,
    in_force: Iterable[Action] = (),
    tau_ms: float | None = None,
    freeze: bool = False,
) -> ControlGraph:
    """G_C at ``now_ms``; ``freeze`` builds the phase-1 graph (egress holds)."""
    started = time.perf_counter()
    tau = config.tau_ms if tau_ms is None else tau_ms
    in_force = frozenset(in_force)
    scopes = ScopeMirror(graph, config)
    units = live_units(graph, config, now_ms, scopes)
    cg = ControlGraph(now_ms, static)
    builder = _Builder(
        graph,
        config,
        units,
        scopes,
        cg,
        capabilities,
        now_ms,
        tau,
        static,
        in_force,
        freeze,
    )
    builder.build()
    builder.expand()
    eat = builder.arrival_times()
    split: set[str] = set()
    for _ in range(MAX_SPLIT_ROUNDS):
        if not builder.split_early(eat, split):
            break
        builder.expand()
        eat = builder.arrival_times()
    doomed = 0
    for _ in range(MAX_DOOMED_ROUNDS):
        moved = builder.split_doomed(eat)
        if not moved:
            break
        doomed += moved
        eat = builder.arrival_times()
    cg.eat = eat
    n_cuttable = n_infeasible = 0
    for edge in cg.edges:
        edge.deadline = builder.advance(edge.fn, eat.get(edge.tail, INF))
        if edge.penalty is not None and edge.penalty[1] is not None:
            token, hold = edge.penalty
            if not (
                static or hold in in_force or builder.t_ap(hold.node_id) < edge.deadline
            ):
                edge.penalty = (token, None)  # cannot land before it starts
        if edge.action is None:
            continue
        n_cuttable += 1
        edge.t_ap = builder.t_ap(edge.action.node_id)
        edge.feasible = static or edge.action in in_force or edge.t_ap < edge.deadline
        n_infeasible += not edge.feasible
    declared = cg.closure(declared_only=True)
    cg.taint_est = {
        cg.nodes[name]["unit"]
        for name in declared
        if cg.nodes.get(name, {}).get("unit") is not None
    } | {key for key, unit in units.items() if unit.tainted}
    cg.cost = CostModel(
        units,
        cg.taint_est,
        config,
        result_parent={r.task_id: r.parent_attempt for r in graph.results.values()},
        effect_issuer={e.effect_id: e.attempt_id for e in graph.effects.values()},
    )
    cg.units = units
    masks = cg.masks()
    cg.stats = {
        "nodes": len(cg.nodes),
        "edges": len(cg.edges),
        "sources": len(cg.sources),
        "sinks": len(cg.sinks),
        "units": len(units),
        "hubs": sum(1 for attrs in cg.nodes.values() if attrs.get("kind") == "hub"),
        "potential_edges": sum(1 for edge in cg.edges if not edge.declared),
        "early_splits": len(split),
        "doomed_moves": doomed,
        "boundary": sum(1 for attrs in cg.nodes.values() if attrs.get("boundary")),
        "cuttable": n_cuttable,
        "infeasible": n_infeasible,
        # Actions whose timely mask is a strict, non-empty part of R(a): paid in
        # full, removing only what lands in time.
        "partial_actions": sum(
            1 for removal, timely in masks.values() if 0 < len(timely) < len(removal)
        ),
        "untimely_actions": sum(1 for _, timely in masks.values() if not timely),
        "horizon": config.horizon,
        "projection_ms": (time.perf_counter() - started) * 1000,
    }
    return cg


def frontier_block(
    cg: ControlGraph, ops: frozenset[str] = frozenset({"hold"})
) -> tuple[set[Action], set[str], set[str]]:
    """Phase-1 block (theory Lemma 7.1): the uncuttable closure U of the sources
    and every feasible action of the allowed ops on an edge leaving U.

    Returns (actions, sinks inside U, U). Sinks inside U are unreachable by any
    phase-1 block; the block still stops everything else that leaves U."""

    def removable(edge: Edge) -> bool:
        return edge.action is not None and edge.feasible and edge.action.op in ops

    closure = set(cg.sources)
    queue = list(closure)
    while queue:
        node = queue.pop()
        for edge in cg.successors(node):
            if not removable(edge) and edge.head not in closure:
                closure.add(edge.head)
                queue.append(edge.head)
    block = {
        edge.action
        for node in closure
        for edge in cg.successors(node)
        if edge.head not in closure and removable(edge)
    }
    return block, closure & cg.sinks, closure


def admission_freeze(
    cg: ControlGraph, closure: set[str], *, pause_pending: bool = True
) -> set[Action]:
    """Hold every source-side unit that may still emit, whatever the deadlines.

    Admission (theory 7.4, open world): a unit whose output ``out:u`` lies in the
    uncuttable closure U is on the source side of the phase-1 cut. Its emissions
    before the hold lands are crossings that reconciliation finds; every new
    dispatch, effect or result it requests after the hold is held by agentd as
    an event, admitted only per event (``admission``). A running unit keeps
    running under its egress label; a pending task is held before it can
    start (no spawn) for the freeze, or labelled too (``pause_pending`` False,
    the adaptive phase 1's labels Q). The actions are the ones G_C offers on
    those edges (``freeze`` projection).
    """
    frozen: set[Action] = set()
    for key, unit in cg.units.items():
        if f"out:{key}" not in closure:
            continue
        tail, kind = (
            (f"in:{key}", "split")
            if unit.pending and pause_pending
            else (f"out:{key}", "egress")
        )
        frozen.update(
            edge.action
            for edge in cg.successors(tail)
            if edge.kind == kind and edge.action is not None
        )
    return frozen
