"""Two-phase enforcement: frontier holds now, optimal cut next, confirm then release.

Protocol (theory note section 7.3):
P1. Hold the frontier of the tainted region at once (Lemma 7.1: every feasible
    hold on an edge leaving the uncuttable closure U of the sources), wait for every
    node to confirm, reconcile taint from the events that arrived meanwhile, and
    repeat while the block is no longer a cut.
P2. Solve on G_C with the phase-1 holds counted as in force (feasible without
    latency), apply the new actions and wait for EVERY target node to confirm.
P3. Reconcile again; if the phase-2 set still separates G_C, release the phase-1
    holds that are not in it. Otherwise solve again with the phase-1 holds kept.

Releases are only ever issued after all phase-2 confirmations, so at every
instant after P1 converges the actions in force contain a valid cut (Theorem
7.3). A phase-2 action that is not confirmed in force, whether rejected or
unconfirmed, keeps every phase-1 hold. Each decision is stamped with a fresh,
monotone policy version for the owner (agentd's fence).

An action is sent to each of its sites (one node, or every node that may hold
affected work for node-local revocations) and is in force only when every site
confirmed it. An unconfirmed site (sent, no confirmation) stays unconfirmed: it
is neither counted as in force nor released, and a later solve that still needs
the action issues it again under a newer version.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from . import solver
from .control import (
    ControlClient,
    ControlError,
    ControlUnconfirmed,
    execute,
    release,
)
from .graph import ControlGraph
from .model import Action
from .projection import frontier_block


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class ActionRecord:
    """One action of one decision and what each of its sites confirmed.

    ``sites`` maps a node to its outcome: ``state`` (in_force, failed or
    unconfirmed), ``command_id``, ``t_sent``, ``t_applied`` (that node's clock),
    ``t_confirmed`` or ``t_returned`` (monitor clock), the rejection ``error`` and
    the node's ``enforcement`` report. The aggregate is in force only when every
    site confirmed; ``t_applied`` is then the latest site's application time.
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
        """Release confirmed holds; one that does not confirm stays in force."""
        records = [r for r in records if r.state == "in_force"]
        if not records:
            return []
        version = self.next_version()

        def run(record: ActionRecord) -> ActionRecord:
            record.t_release_sent = self.clock()
            try:
                confirmation = release(
                    self.client, record.action, policy_version=version
                )
            except ControlUnconfirmed as error:
                record.state, record.error = "release_unconfirmed", error.code
            except ControlError as error:
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
    status: str = "pending"
    rounds: int = 0
    phase1: list[ActionRecord] = field(default_factory=list)
    phase2: list[ActionRecord] = field(default_factory=list)
    released: list[ActionRecord] = field(default_factory=list)
    unavoidable: list[str] = field(default_factory=list)
    cut_valid: bool | None = None
    solve: dict = field(default_factory=dict)
    graph: dict = field(default_factory=dict)

    def to_json(self) -> dict:
        return {
            "incident_id": self.incident_id,
            "strategy": self.strategy,
            "status": self.status,
            "rounds": self.rounds,
            "phase1": [r.to_json() for r in self.phase1],
            "phase2": [r.to_json() for r in self.phase2],
            "released": [r.to_json() for r in self.released],
            "unavoidable": self.unavoidable,
            "cut_valid": self.cut_valid,
            "solve": self.solve,
            "graph": self.graph,
        }


def cut_holds(cg: ControlGraph, actions: Iterable[Action]) -> tuple[bool, list[str]]:
    """Whether the actions separate every avoidable sink of cg; also the unavoidable."""
    instance = cg.instance()
    unavoidable = solver.unavoidable_sinks(instance)
    return solver.separated(instance, actions, instance.sinks - unavoidable), sorted(
        unavoidable
    )


class TwoPhaseProtocol:
    """Orchestrates P1-P3 over callbacks owned by the monitor service.

    ``project(phase, in_force)`` builds G_C (phase 1: holds only, no solve budget);
    ``reconcile()`` ingests events that arrived while waiting; ``choose(cg)``
    returns the phase-2 strategy result. ``in_force`` maps actions to their records
    and is updated in place.
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
        reconcile: Callable[[], None],
        choose: Callable[[ControlGraph], object],
        in_force: dict[Action, ActionRecord],
        two_phase: bool = True,
    ) -> Decision:
        clock = self.driver.clock
        if two_phase:
            for _ in range(self.max_rounds):
                decision.rounds += 1
                cg = project(1, frozenset(in_force))
                block, _ = frontier_block(cg)
                records = self.driver.apply(
                    block - in_force.keys(),
                    incident_id=decision.incident_id,
                    phase=1,
                    t_alert=t_alert,
                    t_decision=clock(),
                )
                decision.phase1.extend(records)
                in_force.update({r.action: r for r in records if r.state == "in_force"})
                reconcile()
                held, _ = cut_holds(project(1, frozenset(in_force)), in_force)
                if held:
                    break
        phase1 = {r.action: r for r in decision.phase1 if r.state == "in_force"}
        for _ in range(self.max_rounds):
            decision.rounds += 1
            cg = project(2, frozenset(in_force))
            started = time.perf_counter()
            result = choose(cg)
            decision.solve = {
                "strategy": decision.strategy,
                "ms": round((time.perf_counter() - started) * 1000, 3),
                "solver_ms": round(getattr(result, "solve_ms", 0.0), 3),
                "status": getattr(result, "status", None),
                "method": getattr(result, "method", None),
                "predicted_cost": getattr(result, "predicted_cost", None),
                "actions": [a.to_json() for a in result.actions],
            }
            decision.graph = dict(cg.stats)
            decision.unavoidable = sorted(getattr(result, "unavoidable", ()))
            chosen = set(result.actions)
            records = self.driver.apply(
                chosen - in_force.keys(),
                incident_id=decision.incident_id,
                phase=2,
                t_alert=t_alert,
                t_decision=clock(),
            )
            decision.phase2.extend(records)
            in_force.update({r.action: r for r in records if r.state == "in_force"})
            if any(r.state != "in_force" for r in records):
                # Rejected or unconfirmed: keep every phase-1 hold.
                decision.status = "phase2_unconfirmed"
                return decision
            reconcile()
            held, _ = cut_holds(project(2, frozenset(in_force)), chosen)
            decision.cut_valid = held
            if not two_phase:
                decision.status = "applied"
                return decision
            if held:
                releasable = [r for a, r in phase1.items() if a not in chosen]
                decision.released = self.driver.release(releasable)
                for record in decision.released:
                    if record.state == "released":
                        in_force.pop(record.action, None)
                decision.status = "contained"
                return decision
        decision.status = "phase2_check_failed"  # phase-1 holds stay in force
        return decision
