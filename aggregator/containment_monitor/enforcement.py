"""Two-phase enforcement: frontier holds now, optimal cut next, confirm then release.

Protocol (theory note section 7.3):
P1. Hold the frontier of the tainted region at once (Lemma 7.1: every feasible
    hold on an edge leaving the uncuttable closure U of the sources) together
    with the admission freeze (every unit inside U that may still emit: its
    egress held, or a pending task held before it starts), wait for every node to
    confirm, reconcile taint from the events that arrived meanwhile, and repeat
    while the block is no longer a cut. From the confirmation on, agentd denies
    every new dispatch, effect and result of a source-side invocation until phase
    2 confirms a cut that covers it.
P2. Solve on G_C with the phase-1 holds counted as in force (feasible without
    latency), apply the new actions and wait for EVERY target node to confirm.
P3. Reconcile again; if the phase-2 set still separates G_C, release the phase-1
    holds that are not in it. Otherwise solve again with the phase-1 holds kept.

Confirm-then-release is also ingest-then-release: an action counts as *fenced*
only when the monitor has applied its nodes' events up to the journal position
each confirmation reports (``fence``), and reconciliation, the cut checks and
every release wait for that. Releases are only ever issued after all phase-2
actions are confirmed and fenced, so at every instant after P1 converges the
actions in force contain a valid cut (Theorem 7.3). A phase-2 action that is
not confirmed in force, whether rejected or unconfirmed, or not fenced in time,
keeps every phase-1 hold. Each decision is stamped with a fresh, monotone
policy version for the owner (agentd's fence).

An action is sent to each of its sites (one node, or every node that may hold
affected work for node-local revocations) and is in force only when every site
confirmed it. An unconfirmed site (sent, no confirmation) stays unconfirmed: it
is neither counted as in force nor released, and a later solve that still needs
the action issues it again under a newer version.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from . import solver
from .control import (
    ControlClient,
    ControlError,
    ControlUnconfirmed,
    execute,
    release,
)
from .graph import ControlGraph
from .config import PHASE1_LABELS, PHASE1_MODES
from .model import Action
from .projection import admission_freeze, frontier_block

if TYPE_CHECKING:
    from .strategies import StrategyResult


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class ActionRecord:
    """One action of one decision and what each of its sites confirmed.

    ``sites`` maps a node to its outcome: ``state`` (in_force, failed or
    unconfirmed), ``command_id``, ``t_sent``, ``t_applied`` (that node's clock),
    ``t_confirmed`` or ``t_returned`` (monitor clock), the rejection ``error``,
    the node's ``enforcement`` report, its ``journal`` position and
    ``t_ingested`` (monitor clock, once the monitor applied that node's events
    up to it). The aggregate is in force only when every site confirmed;
    ``t_applied`` is then the latest site's application time, and ``t_fenced``
    the time the last site was ingested.
    """

    action: Action
    incident_id: str
    phase: int
    policy_version: int
    t_alert: int
    t_decision: int
    t_sent: int | None = None
    t_applied: int | None = None  # node clock, as reported
    t_confirmed: int | None = None
    t_fenced: int | None = None  # monitor clock: every site's journal ingested
    t_release_sent: int | None = None
    t_release_applied: int | None = None  # node clock
    t_released: int | None = None
    # planned | in_force | failed | unconfirmed | released | release_failed
    # | release_unconfirmed
    state: str = "planned"
    error: str | None = None
    sites: dict[str, dict] = field(default_factory=dict)

    def settle(self) -> None:
        outcomes = list(self.sites.values())
        self.t_sent = min(o["t_sent"] for o in outcomes)
        failed = [o for o in outcomes if o["state"] == "failed"]
        if failed:
            self.state, self.error = "failed", failed[0]["error"]
        elif any(o["state"] == "unconfirmed" for o in outcomes):
            self.state, self.error = "unconfirmed", "confirmation_timeout"
        else:
            self.state = "in_force"
            self.t_applied = max(o["t_applied"] for o in outcomes)
            self.t_confirmed = max(o["t_confirmed"] for o in outcomes)

    def to_json(self) -> dict:
        return {
            "action": self.action.to_json(),
            "incident_id": self.incident_id,
            "phase": self.phase,
            "policy_version": self.policy_version,
            "t_alert": self.t_alert,
            "t_decision": self.t_decision,
            "t_sent": self.t_sent,
            "t_applied": self.t_applied,
            "t_confirmed": self.t_confirmed,
            "t_fenced": self.t_fenced,
            "t_release_sent": self.t_release_sent,
            "t_release_applied": self.t_release_applied,
            "t_released": self.t_released,
            "state": self.state,
            "error": self.error,
            "sites": self.sites,
        }

    @classmethod
    def from_json(cls, value: dict) -> ActionRecord:
        return cls(**{**value, "action": Action.from_json(value["action"])})


class EnforcementDriver:
    """Executes decisions concurrently and returns once every site answered.

    ``sites(action)`` names the nodes an action is sent to, its own node first.
    """

    def __init__(
        self,
        client: ControlClient,
        next_version: Callable[[], int],
        *,
        sites: Callable[[Action], tuple[str, ...]] = lambda action: (action.node_id,),
        clock: Callable[[], int] = now_ms,
        max_workers: int = 8,
    ):
        self.client = client
        self.next_version = next_version
        self.sites = sites
        self.clock = clock
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="containment-control"
        )

    def close(self) -> None:
        self._pool.shutdown(wait=True)

    def apply(
        self,
        actions: Iterable[Action],
        *,
        incident_id: str,
        phase: int,
        t_alert: int,
        t_decision: int,
    ) -> list[ActionRecord]:
        actions = sorted(set(actions))
        if not actions:
            return []
        version = self.next_version()
        records = [
            ActionRecord(action, incident_id, phase, version, t_alert, t_decision)
            for action in actions
        ]

        def run(job: tuple[ActionRecord, str]) -> tuple[ActionRecord, str, dict]:
            record, site = job
            outcome: dict = {"t_sent": self.clock()}
            try:
                confirmation = execute(
                    self.client, record.action, node_id=site, policy_version=version
                )
            except ControlUnconfirmed as error:
                outcome.update(state="unconfirmed", error=error.code)
            except ControlError as error:
                outcome.update(state="failed", error=error.code)
            else:
                outcome.update(
                    state="in_force",
                    command_id=confirmation.command_id,
                    t_applied=confirmation.applied_at_ms,
                    t_confirmed=self.clock(),
                    enforcement=dict(confirmation.enforcement),
                    journal=confirmation.journal,
                    t_ingested=None,
                )
            outcome.setdefault("t_returned", self.clock())
            return record, site, outcome

        jobs = [(r, site) for r in records for site in self.sites(r.action)]
        for record, site, outcome in self._pool.map(run, jobs):
            record.sites[site] = outcome
        for record in records:
            record.settle()
        return records

    def release(self, records: Iterable[ActionRecord]) -> list[ActionRecord]:
        """Release confirmed holds; one that does not confirm stays in force.

        An unconfirmed hold is released as well: one its node never applied
        (``hold_not_found``) was never in force and counts as released."""
        records = [r for r in records if r.state in {"in_force", "unconfirmed"}]
        if not records:
            return []
        version = self.next_version()

        def run(record: ActionRecord) -> ActionRecord:
            unconfirmed = record.state == "unconfirmed"
            record.t_release_sent = self.clock()
            try:
                confirmation = release(
                    self.client, record.action, policy_version=version
                )
            except ControlUnconfirmed as error:
                record.state, record.error = "release_unconfirmed", error.code
            except ControlError as error:
                if unconfirmed and error.code == "hold_not_found":
                    record.state = "released"
                    record.t_released = self.clock()
                    return record
                record.state, record.error = "release_failed", error.code
            else:
                record.state = "released"
                record.t_release_applied = confirmation.applied_at_ms
            record.t_released = self.clock()
            return record

        return list(self._pool.map(run, records))


@dataclass
class Decision:
    incident_id: str
    strategy: str
    # The mechanisms the strategy ran with (config.Mechanisms) and their profile.
    profile: str = "deployed"
    mechanisms: dict = field(default_factory=dict)
    # None: the strategy applies its rule directly (no phase 1).
    phase1_mode: str | None = "adaptive"
    phase1_labels: str = "frontier"
    receding_during_waits: bool = False
    # The cost model's value of a tainted unprotected output (1: pre-registered).
    tainted_output_value: float = 1.0
    status: str = "pending"
    rounds: int = 0
    phase1: list[ActionRecord] = field(default_factory=list)
    phase2: list[ActionRecord] = field(default_factory=list)
    released: list[ActionRecord] = field(default_factory=list)
    unavoidable: list[str] = field(default_factory=list)
    cut_valid: bool | None = None
    solve: dict = field(default_factory=dict)  # the last phase-2 solve
    # Every solve, in order: the at-alert cut of phase 1 (any phase1_mode but
    # freeze) and each phase-2 solve; "phase" says which.
    solves: list[dict] = field(default_factory=list)
    graph: dict = field(default_factory=dict)
    # Adaptive phase 1: why the freeze was (not) added (freeze_trigger) and
    # each round's convergence check (C1, C2, C4).
    phase1_trigger: dict = field(default_factory=dict)
    # Actions the receding trigger added while the protocol waited.
    receding: list[str] = field(default_factory=list)
    # Every convergence check (phase 1) and release re-check (phase 2): whether
    # the actions in force separated the sources from every avoidable sink
    # and every protected effect, and which sinks blocked it.
    convergence: list[dict] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "strategy": self.strategy,
            "profile": self.profile,
            "mechanisms": self.mechanisms,
            "phase1_mode": self.phase1_mode,
            "phase1_labels": self.phase1_labels,
            "receding_during_waits": self.receding_during_waits,
            "tainted_output_value": self.tainted_output_value,
            "phase1_trigger": self.phase1_trigger,
            "receding": self.receding,
            "convergence": self.convergence,
            "status": self.status,
            "rounds": self.rounds,
            "phase1": [r.to_json() for r in self.phase1],
            "phase2": [r.to_json() for r in self.phase2],
            "released": [r.to_json() for r in self.released],
            "unavoidable": self.unavoidable,
            "cut_valid": self.cut_valid,
            "solve": self.solve,
            "solves": self.solves,
            "graph": self.graph,
        }


def protected_sinks(cg: ControlGraph) -> frozenset[str]:
    """The sinks that are protected effects (pending, of a protected class)."""
    return frozenset(n for n in cg.sinks if cg.nodes.get(n, {}).get("kind") == "effect")


def context_sinks(cg: ControlGraph) -> frozenset[str]:
    """The protected classes of every whole-context tainted context (a
    shared-memory agent whose memory is tainted): the sinks its future
    invocations reach through its own credential, whatever an action does.

    Such a context has no unit to label (no invocation may be live at the
    alert), and every later invocation of the agent resumes it: unless these
    sinks are separated when phase 1 converges, a request arriving after
    tau_1 commits tainted commands until a re-solve catches it."""
    out: set[str] = set()
    # Every whole context, a source or one a source-side invocation's memory
    # write reaches (``projection`` memory edges); unreached ones never count.
    for source, attrs in list(cg.nodes.items()):
        if attrs.get("kind") != "context" or not attrs.get("whole"):
            continue
        prefix = f"K:{attrs['agent']}:"
        seen, queue = {source}, [source]
        while queue:
            node = queue.pop()
            for edge in cg.successors(node):
                head = edge.head
                if head in seen:
                    continue
                seen.add(head)
                if head in cg.sinks:
                    if head.startswith(prefix):
                        out.add(head)
                    continue
                if cg.nodes.get(head, {}).get("kind") == "arrival" and node != source:
                    continue  # another agent's new invocation
                queue.append(head)
    return frozenset(out)


def context_covers(cg: ControlGraph, removed: Iterable[Action] = ()) -> set[Action]:
    """(C2) for whole-context tainted contexts: the actions on the last hop
    into each of their protected classes that ``removed`` does not already
    separate (the revocation of that class for the agent), pushed and
    confirmed with the labels."""
    sinks = context_sinks(cg) & cg.closure(removed=set(removed))
    return {e.action for e in cg.edges if e.head in sinks and e.action is not None}


def cut_holds(cg: ControlGraph, actions: Iterable[Action]) -> tuple[bool, list[str]]:
    """Whether the actions separate the sources from every avoidable sink of cg
    and from every protected effect, doomed or not; also the sinks still
    reached (the ones that block convergence or a release).

    A sink the deadline-aware graph judges unavoidable is no reason to call a
    cut converged: an effect that is still reachable may fire with a tainted
    tail. Only unavoidable boundary sinks (new invocations at the horizon,
    which no action can separate once their creating flow is untimely) are
    left out, as in the solve. The protected classes of a whole-context
    tainted context count, doomed or not (``context_sinks``): its future
    invocations emit after the actions land.
    """
    instance = cg.instance()
    reached = solver.reached_sinks(instance, actions, _targets(cg, instance))
    return not reached, sorted(reached)


def convergence_sinks(cg: ControlGraph) -> frozenset[str]:
    """The sinks ``cut_holds`` checks on cg."""
    return _targets(cg, cg.instance())


def _targets(cg: ControlGraph, instance: solver.CutInstance) -> frozenset[str]:
    unavoidable = solver.unavoidable_sinks(instance)
    return frozenset(
        (instance.sinks - unavoidable) | protected_sinks(cg) | context_sinks(cg)
    )


def last_hops(cg: ControlGraph, sinks: Iterable[str]) -> set[Action]:
    """Every action on an edge into the sinks, timely or not (deadlines are
    lower bounds, so an untimely one may still land in time): the guard that
    a failed convergence check or re-check adds before it tries again."""
    sinks = set(sinks)
    return {e.action for e in cg.edges if e.head in sinks and e.action is not None}


def freeze_trigger(
    cg: ControlGraph,
    cut: StrategyResult,
    *,
    margin_rounds: int,
    control_latency: Callable[[str], float],
) -> dict:
    """Whether the adaptive phase 1 must add the frontier freeze to B_a.

    Yes when the at-alert solve leaves a protected sink (a pending effect)
    untimely or doomed, or when the earliest tainted-effect deadline falls
    within ``margin_rounds`` control rounds of B_a's landing (deadlines are
    lower bounds, so a cut that lands just in time is a guess).
    """
    effects = {n for n in cg.sinks if cg.nodes.get(n, {}).get("kind") == "effect"}
    untimely = sorted(set(cut.unavoidable) & effects)
    chosen = set(cut.actions)
    landing = max(
        (e.t_ap for e in cg.edges if e.action in chosen and e.t_ap is not None),
        default=float(cg.t_now),
    )
    rounds = [control_latency(a.node_id) for a in chosen] or [0.0]
    margin = margin_rounds * max(rounds)
    deadline = min(
        (e.deadline for e in cg.edges if e.head in effects and e.deadline < math.inf),
        default=math.inf,
    )
    near = deadline <= landing + margin
    return {
        "freeze": bool(untimely) or near,
        "untimely_effects": untimely,
        "earliest_effect_deadline_ms": None if deadline == math.inf else deadline,
        "landing_ms": landing,
        "margin_ms": margin,
    }


class TwoPhaseProtocol:
    """Orchestrates P1-P3 over callbacks owned by the monitor service.

    ``project(phase, in_force)`` builds G_C (phase 1: holds only, no solve
    budget); ``fence(records)`` ingests events until every confirmed record's
    nodes are ingested up to their journal positions (reconciling taint from
    them) and returns whether that happened in time (with ``receding`` it is
    called with ``on_poll``, which it calls while it waits); ``choose(cg)``
    returns the phase-2 strategy result. ``in_force`` maps actions to their
    records and is updated in place.

    Phase 1 (``phase1_mode``):

    - ``freeze``: the frontier freeze B_1 (frontier holds and admission freeze).
    - ``freeze_plus_cut``: B_1 and, in the first round, ``cut_at_alert``, the
      strategy's own cut solved on the phase-2 G_C at the alert (B_a), at once.
    - ``adaptive``: B_a and labels covering its source side S(B_a) (admission
      holds on every unit reachable from the sources through edges B_a does
      not remove in time); the freeze only when ``trigger`` says B_a is not
      safely timely, or when the convergence check (C1)-(C4) fails at tau_1
      (then the freeze is added and the check repeated). The service's
      projection reconciles taint under (J') in this mode.

    Labels (``phase1_labels``): ``frontier`` covers U (freeze modes) or
    S(B_a) (adaptive); ``forward_closure`` covers the forward closure of the
    sources in every mode, which removes the benign refused releases of the
    post-release label race (theory 8.6 (2c)).

    Every at-alert solve is recorded as a ``phase: 1`` entry of ``solves``.
    Phase 2 is the same in every mode: what it does not keep of the phase-1
    holds is released under the guarded release (a revocation cannot be
    released and stays). With ``receding``, a taint advance seen while the
    protocol waits for ingest adds the actions ``receding()`` returns (never a
    release); they are fenced before any release.
    """

    def __init__(self, driver: EnforcementDriver, *, max_rounds: int = 3):
        self.driver = driver
        self.max_rounds = max_rounds

    def run(
        self,
        decision: Decision,
        *,
        t_alert: int,
        project: Callable[[int, frozenset[Action]], ControlGraph],
        fence: Callable[..., bool],
        choose: Callable[[ControlGraph], StrategyResult],
        in_force: dict[Action, ActionRecord],
        two_phase: bool = True,
        phase1_mode: str = "freeze",
        phase1_labels: str = "frontier",
        cut_at_alert: Callable[[ControlGraph], StrategyResult] | None = None,
        trigger: Callable[[ControlGraph, StrategyResult], dict] | None = None,
        receding: Callable[[], set[Action]] | None = None,
        on_converged: Callable[[dict], None] | None = None,
    ) -> Decision:
        if phase1_mode not in PHASE1_MODES:
            raise ValueError(f"unknown phase-1 mode {phase1_mode!r}")
        if phase1_labels not in PHASE1_LABELS:
            raise ValueError(f"unknown phase-1 labels {phase1_labels!r}")
        clock = self.driver.clock
        phase = [1]

        def apply(actions: set[Action]) -> list[ActionRecord]:
            records = self.driver.apply(
                actions - in_force.keys(),
                incident_id=decision.incident_id,
                phase=phase[0],
                t_alert=t_alert,
                t_decision=clock(),
            )
            (decision.phase1 if phase[0] == 1 else decision.phase2).extend(records)
            in_force.update({r.action: r for r in records if r.state == "in_force"})
            return records

        added_records: list[ActionRecord] = []

        def add_receding() -> None:
            added = receding() - in_force.keys() if receding is not None else set()
            if added:
                records = apply(added)
                added_records.extend(records)
                decision.receding.extend(r.action.label() for r in records)

        def wait(records: list[ActionRecord]) -> bool:
            """Fence the records, then whatever the receding trigger added."""
            done: set[int] = set()
            while True:
                pending = [r for r in records if id(r) not in done]
                fenced = (
                    fence(pending, on_poll=add_receding)
                    if receding is not None
                    else fence(pending)
                )
                if not fenced:
                    return False
                done |= {id(r) for r in pending}
                added = [
                    r
                    for r in [*decision.phase1, *decision.phase2]
                    if r.state == "in_force"
                    and id(r) not in done
                    and r.t_fenced is None
                ]
                if not added:
                    return True
                records = added

        def at_alert() -> tuple[ControlGraph, StrategyResult]:
            cg = project(2, frozenset(in_force))
            started = time.perf_counter()
            result = cut_at_alert(cg)
            decision.solves.append(self._solve(decision, result, started, cg, phase=1))
            return cg, result

        def labels(cg: ControlGraph, removed: Iterable[Action]) -> set[Action]:
            """Admission holds (reversible) on every unit of the source side of
            ``removed`` on ``cg`` (theory A.6: Q covers S), or on the forward
            closure of the sources (``phase1_labels="forward_closure"``)."""
            frozen_cg = project(1, frozenset(in_force))
            side = (
                frozen_cg.closure()
                if phase1_labels == "forward_closure"
                else cg.closure(removed=set(removed))
            )
            # Labels, not pauses: a labelled unit runs and its new events are
            # admitted per event. A whole-context tainted context has no unit:
            # its protected classes are revoked for its agent instead.
            return admission_freeze(
                frozen_cg, side, pause_pending=False
            ) | context_covers(cg, removed)

        def freeze() -> set[Action]:
            cg = project(1, frozenset(in_force))
            block, _, closure = frontier_block(cg)
            side = cg.closure() if phase1_labels == "forward_closure" else closure
            return block | admission_freeze(cg, side)

        # The inputs of the last convergence check (the tau_1 record).
        evidence: dict = {}

        def report_converged(round_no: int, checks: dict) -> None:
            """tau_1: the literal check passed. Logged with its inputs (the
            fence positions per site, the label set's size, the sinks checked);
            nothing here changes what the protocol does."""
            if on_converged is None:
                return
            relied = [
                r for r in [*decision.phase1, *decision.phase2] if r.state == "in_force"
            ]
            fence: dict[str, dict] = {}
            for record in relied:
                for site, outcome in record.sites.items():
                    journal = outcome.get("journal")
                    if not journal:
                        continue
                    position = {
                        "source_epoch": str(journal["source_epoch"]),
                        "source_seq": int(journal["source_seq"]),
                    }
                    known = fence.get(site)
                    if (
                        known is None
                        or known["source_epoch"] != position["source_epoch"]
                        or position["source_seq"] > known["source_seq"]
                    ):
                        fence[site] = position
            on_converged(
                {
                    "phase": 1,
                    "round": round_no,
                    "tau1": clock(),
                    "t_alert": t_alert,
                    "phase1_mode": phase1_mode,
                    "checks": checks,
                    "fence": fence,
                    "actions_in_force": len(relied),
                    "labels": evidence.get("labels"),
                    "sinks_checked": evidence.get("sinks_checked", []),
                }
            )

        def converged(fenced: bool) -> dict:
            """The convergence check at tau_1 for an adaptive phase 1, literally
            (theory 8.6): (C1) every relied-on action confirmed and its journals
            ingested through the fence positions; (C2) labels confirmed at every
            site hosting a vertex of S(A(tau_1)), pushing what is missing; (C3)
            taint reconciled under (J') (the service's projection); (C4) A(tau_1)
            a cut on the reconciled graph, every admitted edge included."""
            check = {"C1": fenced, "C2": None, "C4": None}
            if not fenced:
                return check
            for _ in range(self.max_rounds):
                label_set = labels(project(2, frozenset(in_force)), in_force)
                missing = label_set - in_force.keys()
                if not missing:
                    check["C2"] = True
                    evidence["labels"] = len(label_set)
                    break
                records = apply(missing)
                if any(r.state != "in_force" for r in records) or not wait(records):
                    check["C2"] = False
                    return check
            else:
                check["C2"] = False
                return check
            cg = project(2, frozenset(in_force))
            held, blocking = cut_holds(cg, in_force)
            if not held:
                # A doomed sink still reached: guard its last hop and re-check.
                check["blocked_by"] = blocking
                guard = last_hops(cg, blocking) - in_force.keys()
                if guard:
                    check["guard"] = sorted(a.label() for a in guard)
                    records = apply(guard)
                    if all(r.state == "in_force" for r in records) and wait(records):
                        cg = project(2, frozenset(in_force))
                        held, blocking = cut_holds(cg, in_force)
            check["C4"] = held
            check["blocking_sinks"] = blocking
            evidence["sinks_checked"] = sorted(convergence_sinks(cg))
            return check

        if two_phase:
            frozen = phase1_mode != "adaptive"
            for round_ in range(self.max_rounds):
                decision.rounds += 1
                block: set[Action] = freeze() if frozen else set()
                if round_ == 0 and phase1_mode != "freeze":
                    cg, cut = at_alert()
                    block |= set(cut.actions)
                    if phase1_mode == "adaptive":
                        # Q covers S(B_a), pushed with B_a.
                        block |= labels(cg, cut.actions)
                        decision.phase1_trigger = trigger(cg, cut)
                        if decision.phase1_trigger["freeze"]:
                            frozen = True
                            block |= freeze()
                records = apply(block)
                fenced = wait(records)
                if phase1_mode == "adaptive":
                    check = converged(fenced)
                    decision.phase1_trigger.setdefault("checks", []).append(check)
                    held = all(check[c] for c in ("C1", "C2", "C4"))
                    decision.convergence.append(
                        {
                            "phase": 1,
                            "round": round_ + 1,
                            "converged": held,
                            "blocking_sinks": check.get("blocking_sinks", []),
                        }
                    )
                    if held:
                        report_converged(
                            round_ + 1,
                            {
                                "C1": True,
                                "C2": True,
                                "C3": "reconciled (J')",
                                "C4": True,
                            },
                        )
                    if not held and not frozen:
                        # On failure, add the frontier freeze and repeat.
                        frozen = True
                        decision.phase1_trigger["freeze_after_check"] = True
                else:
                    cg = project(1, frozenset(in_force))
                    held, blocking = cut_holds(cg, in_force)
                    decision.convergence.append(
                        {
                            "phase": 1,
                            "round": round_ + 1,
                            "converged": held,
                            "blocking_sinks": blocking,
                        }
                    )
                    if not held:
                        # A sink still reached, doomed or not: guard its last
                        # hop and check again (next round: a recomputed freeze).
                        guard = last_hops(cg, blocking) - in_force.keys()
                        if guard:
                            guard_fenced = wait(apply(guard))
                            fenced = fenced and guard_fenced
                            cg = project(1, frozenset(in_force))
                            held, blocking = cut_holds(cg, in_force)
                            decision.convergence.append(
                                {
                                    "phase": 1,
                                    "round": round_ + 1,
                                    "converged": held,
                                    "blocking_sinks": blocking,
                                    "guard": sorted(a.label() for a in guard),
                                }
                            )
                    if held and fenced:
                        # The freeze holds or labels every source-side unit.
                        evidence["labels"] = sum(
                            1
                            for a in in_force
                            if a.op == "hold" and a.kind in {"task", "egress"}
                        )
                        evidence["sinks_checked"] = sorted(convergence_sinks(cg))
                        report_converged(
                            round_ + 1,
                            {
                                "C1": True,
                                "C2": "frontier freeze",
                                "C3": "frontier freeze (no reconciliation)",
                                "C4": True,
                            },
                        )
                if held:
                    break
        phase[0] = 2
        phase1 = {r.action: r for r in decision.phase1 if r.state == "in_force"}
        for _ in range(self.max_rounds):
            decision.rounds += 1
            cg = project(2, frozenset(in_force))
            started = time.perf_counter()
            result = choose(cg)
            decision.solve = self._solve(decision, result, started, cg, phase=2)
            decision.solves.append(decision.solve)
            decision.graph = dict(cg.stats)
            decision.unavoidable = sorted(getattr(result, "unavoidable", ()))
            chosen = set(result.actions)
            records = apply(chosen)
            if any(r.state != "in_force" for r in records):
                # Rejected or unconfirmed: keep every phase-1 hold.
                decision.status = "phase2_unconfirmed"
                return decision
            confirmed = [
                r for r in [*decision.phase1, *decision.phase2] if r.state == "in_force"
            ]
            if not wait(confirmed) and two_phase:
                # Not every confirmed decision is in the log the monitor read:
                # what crossed before it is unknown, so nothing is released.
                decision.status = "ingest_timeout"
                return decision
            cg = project(2, frozenset(in_force))
            held, blocking = cut_holds(cg, chosen)
            decision.cut_valid = held
            if not two_phase:
                decision.status = "applied"
                return decision
            decision.convergence.append(
                {
                    "phase": 2,
                    "round": len(decision.solves),
                    "converged": held,
                    "blocking_sinks": blocking,
                }
            )
            if any(r.action not in in_force for r in added_records):
                # An action added during the waits is not confirmed: what it
                # was meant to stop may still flow, so nothing is released.
                decision.status = "receding_unconfirmed"
                return decision
            if not held:
                # Keep every hold; guard the doomed sinks still reached, and
                # solve again.
                guard = last_hops(cg, blocking) - in_force.keys()
                if guard:
                    apply(guard)
            if held:
                releasable = [
                    r for a, r in phase1.items() if a not in chosen and a.op == "hold"
                ]
                # A phase-1 hold sent without a confirmation may be in force at
                # its node: released too (the cut holds without it), so no
                # pause or label outlives the decision unseen.
                releasable += [
                    r
                    for r in decision.phase1
                    if r.state == "unconfirmed"
                    and r.action.op == "hold"
                    and r.action not in chosen
                    and r.action not in in_force
                ]
                decision.released = self.driver.release(releasable)
                for record in decision.released:
                    if record.state == "released":
                        in_force.pop(record.action, None)
                decision.status = "contained"
                return decision
        decision.status = "phase2_check_failed"  # phase-1 holds stay in force
        return decision

    @staticmethod
    def _solve(
        decision: Decision,
        result: StrategyResult,
        started: float,
        cg: ControlGraph,
        *,
        phase: int,
    ) -> dict:
        return {
            # solver, fast_path, fast_path_reason, exceptions, ...
            **result.report,
            "phase": phase,
            "strategy": decision.strategy,
            "ms": round((time.perf_counter() - started) * 1000, 3),
            "solver_ms": round(result.solve_ms, 3),
            "status": result.status,
            "method": result.method,
            "predicted_cost": result.predicted_cost,
            "actions": [a.to_json() for a in result.actions],
            "graph": dict(cg.stats),
        }
