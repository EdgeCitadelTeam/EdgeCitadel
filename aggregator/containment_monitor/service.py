"""Monitor engine and its background thread.

``ContainmentMonitor`` is single-threaded and owns two connections: a read-only
view of the Core database (``trace_raw_events`` by ``ingest_seq``) and the monitor
database (G_P, cursor, alerts, incidents, actions). ``follow`` applies one batch of
events and advances the persisted cursor in ONE transaction, so every event is
applied exactly once; a failed batch rolls back and rebuilds the in-memory
frontier and taint index from the tables.

On an alert it ranks root causes, taints the forward closure of the recommended
scope, and runs the chosen strategy through the two-phase protocol. While an
incident is active, any frontier advance of the taint (a new tainted invocation,
dispatch, result or effect) triggers a receding-horizon re-solve with the actions
already in force kept as feasible elements.

``ContainmentMonitorService`` runs the engine in a thread like the projector and
exposes a lock-protected status snapshot and a thread-safe alert intake.
"""

from __future__ import annotations

import copy
import json
import os
import queue
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

from ..trace_payload_read import read_payload
from . import admission, rootcause
from .config import ConfigError, MonitorConfig
from .control import (
    Confirmation,
    ControlClient,
    ControlError,
    LocalAgentdControlClient,
    NatsControlClient,
    ObservingControlClient,
    UnconfiguredControlClient,
)
from .detectors import detect
from .fence import SourceProgress
from .enforcement import (
    ActionRecord,
    Decision,
    EnforcementDriver,
    TwoPhaseProtocol,
    freeze_trigger,
    now_ms,
)
from .graph import ControlGraph
from .model import (
    FAILURE_KINDS,
    STICKY_FLAG_STRATEGIES,
    Action,
    Alert,
    AlertError,
    hold_event,
    inv,
    parse_external_alert,
    ver,
)
from .projection import ScopeMirror, project
from .provenance import HeldEvent, ProvenanceGraph
from .strategies import DISPLAY, STRATEGIES, Context, with_mechanisms

BATCH = 256
MAX_CATCH_UP_BATCHES = 64
POLL_SECONDS = 0.1
SHUTDOWN_SECONDS = 10.0
RECENT_DECISIONS = 20
INGEST_POLL_SECONDS = 0.01


class DecisionLog:
    """Append-only JSON lines for the experiment ledger; one line per record."""

    def __init__(self, path: Path):
        self.path = path

    def append(self, record: dict) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            )
            handle.flush()


class LeaseRenewer:
    """Keeps agentd's fail-closed controller lease alive on the known nodes.

    A node with ``EDGECITADEL_CONTROLLER_LEASE_MS`` denies effects unless a
    renewal was applied within that period, so a monitor that stops (or is
    cut off) stops the fleet's effects. Renewal starts with the service, before
    Core's collector is ready and whether or not any alert ever arrives, and
    runs beside decisions on its own pool so an unreachable node never delays
    one; one renewal per node is in flight. Renewals carry the owner's current
    policy version without advancing it; one that races a newer decision is
    rejected as stale and the next round renews again.
    """

    def __init__(
        self,
        control: ControlClient,
        config: MonitorConfig,
        *,
        clock: Callable[[], int] = now_ms,
    ):
        self.control = control
        self.config = config
        self.clock = clock
        self._pool = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="containment-lease"
        )
        self._renewals: dict[str, Future[Confirmation]] = {}
        self._last_ms: int | None = None
        self.leases: dict[str, dict] = {}

    def close(self) -> None:
        self._pool.shutdown(wait=True)

    def renew(self, nodes: set[str], policy_version: int) -> None:
        for node, future in list(self._renewals.items()):
            if not future.done():
                continue
            del self._renewals[node]
            try:
                confirmation = future.result()
            except ControlError as error:
                self.leases[node] = {**self.leases.get(node, {}), "error": error.code}
            else:
                enforcement = confirmation.enforcement
                self.leases[node] = {
                    "until_ms": enforcement.get("controller_lease_until_ms"),
                    "enforced": enforcement.get("controller_lease_enforced"),
                    "renewed_at_ms": confirmation.applied_at_ms,
                    "error": None,
                }
        interval = self.config.controller_lease_renew_ms
        now = self.clock()
        if not interval or (
            self._last_ms is not None and now - self._last_ms < interval
        ):
            return
        self._last_ms = now
        self.control.refresh(nodes)
        version = max(1, policy_version)
        for node in sorted(nodes - self._renewals.keys()):
            if "renew:controller" in self.control.capabilities(node):
                self._renewals[node] = self._pool.submit(
                    self.control.send, node, "renew", {}, policy_version=version
                )


def stored_policy_version(db: sqlite3.Connection) -> int:
    return db.execute(
        "SELECT policy_version FROM containment_state WHERE singleton=1"
    ).fetchone()[0]


class ContainmentMonitor:
    def __init__(
        self,
        core: sqlite3.Connection,
        monitor: sqlite3.Connection,
        config: MonitorConfig,
        control: ControlClient,
        *,
        decision_log: DecisionLog | None = None,
        clock: Callable[[], int] = now_ms,
        batch: int = BATCH,
        wait: Callable[[float], None] = time.sleep,
        renewer: LeaseRenewer | None = None,
    ):
        self.core = core
        self.db = monitor
        self.config = config
        self.control = control
        self.log = decision_log
        self.clock = clock
        self.batch = batch
        self.wait = wait  # between ingest polls while fencing
        if control.owner != config.owner:
            # agentd fences and authenticates decisions per owner: one identity.
            raise ConfigError("control_owner_mismatch")
        ProvenanceGraph.initialize(monitor)
        self.graph = ProvenanceGraph(monitor, config, clock=clock)
        self.graph.reload()
        self.sources = SourceProgress(monitor)
        # Observe-only monitors decide and log but send no decision; they still
        # describe nodes and renew leases through the same client.
        decisions = control if config.enforce else ObservingControlClient(control)
        self.driver = EnforcementDriver(
            decisions, self._next_version, sites=self._sites, clock=clock
        )
        self.protocol = TwoPhaseProtocol(self.driver, max_rounds=config.max_rounds)
        self.in_force: dict[Action, ActionRecord] = {
            Action.from_json(json.loads(action)): ActionRecord.from_json(
                json.loads(record)
            )
            for action, record in monitor.execute(
                "SELECT action_json,record_json FROM containment_actions WHERE state='in_force'"
            )
        }
        self.fault: str | None = None
        self.decisions: list[dict] = []
        self.last_graph: dict = {}
        self.last_ranking: list[dict] = []
        self._last_resolve_ms = 0
        # Per held event kept, the reason last logged.
        self._kept: dict[str, str] = {}
        # The phase-1 mode of the decision being made: (J') reconciles taint
        # only for an adaptive phase 1.
        self._phase1_mode: str | None = None
        self._owns_renewer = renewer is None
        self.renewer = renewer or LeaseRenewer(control, config, clock=clock)

    @property
    def leases(self) -> dict[str, dict]:
        return self.renewer.leases

    def close(self) -> None:
        if self._owns_renewer:
            self.renewer.close()
        self.driver.close()

    # -- following --------------------------------------------------------------
    def cursor(self) -> int:
        return self.db.execute(
            "SELECT ingest_cursor FROM containment_state WHERE singleton=1"
        ).fetchone()[0]

    def follow(self) -> int:
        """Apply one batch of Core events; returns the number of rows consumed."""
        epoch = self.core.execute(
            "SELECT collector_epoch FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        known = self.db.execute(
            "SELECT collector_epoch FROM containment_state WHERE singleton=1"
        ).fetchone()[0]
        if known is not None and known != epoch:
            # A restored Core renumbers ingestion; the cursor no longer applies.
            self.fault = "core_epoch_changed"
            return 0
        cursor = self.cursor()
        rows = self.core.execute(
            "SELECT ingest_seq,node_id,source_epoch,source_seq FROM trace_raw_events "
            "WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
            (cursor, self.batch),
        ).fetchall()
        if not rows:
            return 0
        try:
            with self.graph.transaction():
                for seq, node_id, source_epoch, source_seq in rows:
                    self.sources.observe(node_id, source_epoch, source_seq)
                    payload = read_payload(self.core, seq)
                    event = payload["event"] if payload else None
                    if event is None:
                        continue  # expired payload: identity only, nothing to join
                    self.graph.apply(event, seq)
                    for alert in detect(event, self.graph):
                        self._store_alert(alert)
                self.sources.flush()
                self.db.execute(
                    "UPDATE containment_state SET ingest_cursor=?,collector_epoch=? WHERE singleton=1",
                    (rows[-1][0], epoch),
                )
        except BaseException:
            self.sources.reload()
            raise
        return len(rows)

    def catch_up(self) -> None:
        for _ in range(MAX_CATCH_UP_BATCHES):
            if not self.follow():
                return

    # -- alerts -------------------------------------------------------------------
    def _store_alert(self, alert: Alert) -> None:
        state = (
            "queued"
            if alert.kind in FAILURE_KINDS or alert.kind == "version_revoked"
            else "evidence"
        )
        self.db.execute(
            "INSERT OR IGNORE INTO containment_alerts(alert_id,kind,subject,origin,observed_ms,alert_json,state) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                alert.alert_id,
                alert.kind,
                alert.subject,
                alert.origin,
                alert.observed_at_ms,
                json.dumps(alert.to_json(), sort_keys=True),
                state,
            ),
        )
        if alert.kind == "version_revoked":
            self.graph.revoked.add(alert.subject.split(":", 1)[1])

    def submit(self, alert: Alert) -> None:
        with self.graph.transaction():
            self._store_alert(alert)

    def handle_alerts(self) -> list[Decision]:
        decisions = []
        while True:
            row = self.db.execute(
                "SELECT alert_id,alert_json FROM containment_alerts WHERE state='queued' "
                "ORDER BY observed_ms,alert_id LIMIT 1"
            ).fetchone()
            if row is None:
                return decisions
            alert = Alert.from_json(json.loads(row[1]))
            repeated = self.db.execute(
                "SELECT 1 FROM containment_alerts WHERE kind=? AND subject=? AND state='handled'",
                (alert.kind, alert.subject),
            ).fetchone()
            # A repeat (e.g. a stream of denials from one invocation) changes no
            # root or taint; the receding re-solve covers any new frontier.
            if repeated is None:
                decisions.append(self.respond(alert))
            with self.db:
                self.db.execute(
                    "UPDATE containment_alerts SET state='handled' WHERE alert_id=?",
                    (row[0],),
                )

    # -- response -------------------------------------------------------------------
    def _roots(
        self, alert: Alert, ranking: list[rootcause.Candidate]
    ) -> tuple[list[str], str]:
        if alert.root is not None:
            return [alert.root], "designated"
        if alert.kind == "revoked_version_consumed":
            return [ver(alert.detail["version_id"])], "revoked_version"
        if alert.kind == "version_revoked":
            return [alert.subject], "revoked_version"
        if ranking:
            return ranking[0].roots, ranking[0].prune_level
        return rootcause.subject_nodes(self.graph, alert.subject), "subject"

    def fence(
        self,
        records: list[ActionRecord],
        on_poll: Callable[[], None] | None = None,
    ) -> bool:
        """Follow Core until every confirmed record's nodes are ingested up to
        the journal positions they confirmed (taint is reconciled on the way),
        calling ``on_poll`` after each pass while it waits.

        Marks each site's ``t_ingested`` and each record's ``t_fenced``; returns
        False when ``ingest_timeout_ms`` passed first.
        """
        deadline = time.monotonic() + self.config.ingest_timeout_ms / 1000
        while True:
            self.catch_up()
            if on_poll is not None:
                on_poll()
            waiting = False
            for record in records:
                if record.state != "in_force" or record.t_fenced is not None:
                    continue
                for site, outcome in record.sites.items():
                    if outcome.get("t_ingested") is None:
                        if self.sources.covers(site, outcome.get("journal")):
                            outcome["t_ingested"] = self.clock()
                        else:
                            waiting = True
                if all(o.get("t_ingested") is not None for o in record.sites.values()):
                    record.t_fenced = max(
                        o["t_ingested"] for o in record.sites.values()
                    )
            if not waiting:
                return True
            if time.monotonic() >= deadline:
                return False
            self.wait(INGEST_POLL_SECONDS)

    def _sites(self, action: Action) -> tuple[str, ...]:
        return ScopeMirror(self.graph, self.config).sites(action)

    def _supported(self, action: Action) -> bool:
        return all(
            action.capability in self.control.capabilities(site)
            for site in self._sites(action)
        )

    def _project(
        self, phase: int, in_force: frozenset[Action], static: bool = False
    ) -> ControlGraph:
        capabilities = self.control.capabilities
        if phase == 1:
            # The frontier block is released later: reversible holds only.
            def capabilities(node_id: str) -> frozenset[str]:
                return frozenset(
                    c
                    for c in self.control.capabilities(node_id)
                    if c.startswith("hold:")
                )

        def build() -> ControlGraph:
            return project(
                self.graph,
                self.config,
                now_ms=self.clock(),
                capabilities=capabilities,
                static=static,
                in_force=in_force,
                tau_ms=0.0 if phase == 1 else None,
                freeze=phase == 1,
            )

        cg = build()
        if self._phase1_mode == "adaptive" and not static:
            # (J'): with an adaptive phase 1 the source side holds uncovered
            # edges whose commits arrive from journals with different delays;
            # reconcile conservatively before every solve and check.
            while self._reconcile_known_commits(cg, in_force):
                cg = build()
        self.last_graph = dict(cg.stats)
        return cg

    def _reconcile_known_commits(
        self, cg: ControlGraph, in_force: frozenset[Action]
    ) -> set[str]:
        """Taint what (J') says a known commit taints; returns the new taint."""
        side: dict[str, float] = {}
        for name in cg.closure(removed=set(in_force)):
            unit = cg.nodes.get(name, {}).get("unit")
            if unit is None or unit.startswith("task:"):
                continue
            arrival = cg.eat.get(f"in:{unit}", cg.t_now)
            side[inv(unit)] = min(side.get(inv(unit), arrival), arrival)
        return self.graph.reconcile_known_commits(side)

    def respond(
        self,
        alert: Alert,
        *,
        strategy: str | None = None,
        roots: list[str] | None = None,
    ) -> Decision:
        t_received = self.clock()
        self.control.refresh(self.graph.known_nodes())
        ranking = rootcause.rank(self.graph, self.config, alert, now_ms=t_received)
        if roots is None:
            roots, basis = self._roots(alert, ranking)
        else:
            basis = "explicit"
        failing = {alert.subject}
        summary = rootcause.contamination(self.graph, roots, failing)
        name = strategy or alert.strategy or self.config.strategy
        incident_id = str(uuid.uuid4())
        with self.graph.transaction():
            self.db.execute(
                "INSERT INTO containment_incidents(incident_id,alert_id,roots_json,strategy,state,created_ms) "
                "VALUES (?,?,?,?, 'active', ?)",
                (incident_id, alert.alert_id, json.dumps(roots), name, t_received),
            )
            self.graph.mark_roots(roots)
        if name in STICKY_FLAG_STRATEGIES:
            self.graph.flagged_agents |= self.graph.agents_of(roots)
        self.graph.frontier_advanced = False
        self.last_ranking = [c.to_json() for c in ranking]
        decision = self._enforce(incident_id, name, alert.observed_at_ms, roots)
        record = {
            "type": "decision",
            "alert": alert.to_json(),
            "t_received": t_received,
            "roots": roots,
            "root_basis": basis,
            "ranking": self.last_ranking,
            "contamination": summary,
            "strategy_display": DISPLAY[name],
            **decision.to_json(),
        }
        self._remember(record)
        return decision

    def _enforce(
        self, incident_id: str, name: str, t_alert: int, roots: list[str]
    ) -> Decision:
        mechanisms = self.config.mechanisms_for(name)
        mode = self.config.phase1_mode_for(name)
        decision = Decision(
            incident_id,
            name,
            profile=self.config.profile,
            mechanisms=mechanisms.to_json(),
            phase1_mode=mode,
            phase1_labels=self.config.phase1_labels,
            receding_during_waits=self.config.receding_during_waits,
            tainted_output_value=self.config.tainted_output_value,
        )

        def context(cg: ControlGraph) -> Context:
            return Context(
                self.graph,
                self.config,
                cg,
                lambda: self._project(2, frozenset(self.in_force), static=True),
                roots,
                self._supported,
            )

        def rule(cg: ControlGraph):
            return STRATEGIES[name](context(cg))

        def choose(cg: ControlGraph):
            ctx = context(cg)
            return with_mechanisms(ctx, STRATEGIES[name](ctx), mechanisms)

        def trigger(cg: ControlGraph, cut) -> dict:
            return freeze_trigger(
                cg,
                cut,
                margin_rounds=self.config.phase1_margin_rounds,
                control_latency=self.config.control_latency,
            )

        def receding() -> set[Action]:
            """A taint advance during a wait: what the strategy adds now."""
            if not self.graph.frontier_advanced:
                return set()
            self.graph.frontier_advanced = False
            return set(choose(self._project(2, frozenset(self.in_force))).actions)

        self._phase1_mode = mode
        try:
            self.protocol.run(
                decision,
                t_alert=t_alert,
                project=self._project,
                fence=self.fence,
                choose=choose,
                in_force=self.in_force,
                two_phase=mechanisms.phase1_freeze,
                phase1_mode=mode or "freeze",
                phase1_labels=self.config.phase1_labels,
                cut_at_alert=rule,
                trigger=trigger,
                receding=receding if self.config.receding_during_waits else None,
                on_converged=lambda record: self._log_converged(
                    incident_id, name, record
                ),
            )
        finally:
            self._phase1_mode = None
        self._persist(decision)
        return decision

    def _log_converged(self, incident_id: str, name: str, record: dict) -> None:
        """A ``converged`` record in the decision log when phase 1's literal
        convergence check passes: tau_1 and the check's inputs."""
        if self.log is not None:
            self.log.append(
                {
                    "type": "converged",
                    "incident_id": incident_id,
                    "strategy": name,
                    "t_decided": record["tau1"],
                    **record,
                }
            )

    def _persist(self, decision: Decision) -> None:
        self._persist_records([*decision.phase1, *decision.phase2, *decision.released])

    def _persist_records(self, records: list[ActionRecord]) -> None:
        with self.db:
            for record in records:
                state = {
                    "in_force": "in_force",
                    "released": "released",
                    "unconfirmed": "unconfirmed",
                    # A release that was not confirmed leaves the hold in force.
                    "release_failed": "in_force",
                    "release_unconfirmed": "in_force",
                }.get(record.state, "failed")
                self.db.execute(
                    "INSERT INTO containment_actions(action_json,incident_id,phase,state,policy_version,record_json) "
                    "VALUES (?,?,?,?,?,?) ON CONFLICT(action_json) DO UPDATE SET "
                    "incident_id=excluded.incident_id,phase=excluded.phase,state=excluded.state,"
                    "policy_version=excluded.policy_version,record_json=excluded.record_json",
                    (
                        json.dumps(record.action.to_json(), sort_keys=True),
                        record.incident_id,
                        record.phase,
                        state,
                        record.policy_version,
                        json.dumps(record.to_json(), sort_keys=True),
                    ),
                )

    def _remember(self, record: dict) -> None:
        self.decisions = [*self.decisions[-(RECENT_DECISIONS - 1) :], record]
        if self.log is not None:
            self.log.append(record)

    def _next_version(self) -> int:
        with self.db:
            self.db.execute(
                "UPDATE containment_state SET policy_version=policy_version+1 WHERE singleton=1"
            )
            return self.db.execute(
                "SELECT policy_version FROM containment_state WHERE singleton=1"
            ).fetchone()[0]

    def resolve_frontier(self) -> Decision | None:
        """Receding horizon: re-solve the newest active incident on taint advance."""
        if not self.graph.frontier_advanced:
            return None
        now = self.clock()
        if now - self._last_resolve_ms < self.config.tau_ms:
            return None
        row = self.db.execute(
            "SELECT incident_id,roots_json,strategy FROM containment_incidents WHERE state='active' "
            "ORDER BY created_ms DESC LIMIT 1"
        ).fetchone()
        self.graph.frontier_advanced = False
        if row is None:
            return None
        self._last_resolve_ms = now
        decision = self._enforce(row[0], row[2], now, json.loads(row[1]))
        self._remember({"type": "resolve", "t_received": now, **decision.to_json()})
        return decision

    # -- per-event admission (admission.py) ---------------------------------------
    def admit_held(self) -> list[dict]:
        """Decide every held event of a label not decided under the current
        policy version (a kept one again once anything changed); label the
        heads that need it, confirmed and fenced, then release the admitted
        events' holds together. Returns the ``admission`` records logged."""
        version = stored_policy_version(self.db)
        pending = [
            held
            for held in self.graph.held.values()
            if held.state == "held" or (held.decided_version or 0) < version
        ]
        if not pending or not self.config.enforce:
            return []
        incident = self.db.execute(
            "SELECT incident_id,created_ms,strategy FROM containment_incidents "
            "WHERE state='active' ORDER BY created_ms DESC LIMIT 1"
        ).fetchone()
        incident_id, t_alert, strategy = (
            incident if incident else ("", self.clock(), None)
        )
        # (J') as on the enforce path: the projections below reconcile known
        # commits when the incident's strategy runs an adaptive phase 1. Taint
        # (J') added lives in the index only, which a restart or a failed
        # ingest batch rebuilds from the roots, and a commit whose tail is on a
        # slow site's journal advances no frontier, so no re-solve reconciles
        # it before the next release is decided here.
        self._phase1_mode = (
            self.config.phase1_mode_for(strategy) if strategy is not None else None
        )
        try:
            return self._admit(pending, incident_id, t_alert)
        finally:
            self._phase1_mode = None

    def _admit(
        self, pending: list[HeldEvent], incident_id: str, t_alert: int
    ) -> list[dict]:
        """``admit_held``'s decisions, with the incident's phase-1 mode set."""
        protected = self.config.protected_effect_classes
        cg = self._project(2, frozenset(self.in_force))
        verdicts = {
            held.event_id: admission.verdict(
                held, cg, self.graph, frozenset(self.in_force), protected
            )
            for held in pending
        }
        labels = set().union(*(v.labels for v in verdicts.values()))
        pushed = {event_id: v.labels for event_id, v in verdicts.items()}
        label_records: list[ActionRecord] = []
        if labels:
            label_records = self.driver.apply(
                labels - self.in_force.keys(),
                incident_id=incident_id,
                phase=2,
                t_alert=t_alert,
                t_decision=self.clock(),
            )
            self.in_force.update(
                {r.action: r for r in label_records if r.state == "in_force"}
            )
            self._persist_records(label_records)
            # Labels covering the enlarged source side are fenced before the
            # releases that enlarge it (R).
            if self.fence(label_records):
                cg = self._project(2, frozenset(self.in_force))
                for held in pending:
                    if verdicts[held.event_id].labels <= self.in_force.keys():
                        verdicts[held.event_id] = admission.verdict(
                            held, cg, self.graph, frozenset(self.in_force), protected
                        )
        t_decided = self.clock()
        admitted = [held for held in pending if verdicts[held.event_id].admit]
        releases = self.driver.release(
            [
                ActionRecord(
                    hold_event(held.node_id, held.event_id),
                    incident_id,
                    2,
                    0,
                    t_alert,
                    t_decided,
                    state="in_force",
                )
                for held in admitted
            ]
        )
        version = stored_policy_version(self.db)
        records = []
        with self.graph.transaction():
            for held in pending:
                result = verdicts[held.event_id]
                release = next(
                    (r for r in releases if r.action.object_id == held.event_id), None
                )
                if release is None:
                    state = "kept"
                elif release.state == "released":
                    state = "admitted"
                elif release.state == "release_unconfirmed":
                    state = "held"  # decided again next time
                else:
                    state = "failed"
                self.graph.decide_held(held.event_id, state, version)
                if state == "kept" and self._kept.get(held.event_id) == result.reason:
                    continue  # logged when first kept for this reason
                self._kept[held.event_id] = result.reason
                records.append(
                    self._admission_record(
                        held,
                        result,
                        release,
                        [r for r in label_records if r.action in pushed[held.event_id]],
                    )
                )
        if self.log is not None:
            for record in records:
                self.log.append(record)
        return records

    def _admission_record(self, held, result, release, labels) -> dict:
        applied = release.t_release_applied if release else None
        return {
            "type": "admission",
            "event_id": held.event_id,
            "event": held.event,
            "node_id": held.node_id,
            "attempt_id": held.attempt_id,
            "task_id": held.task_id,
            "subject_id": held.subject_id,
            "effect_class": held.effect_class,
            "target": held.target,
            "recipient_id": held.recipient_id,
            "decision": "admit" if result.admit else "keep",
            "reason": result.reason,
            "blocking": result.blocking,
            # Labels pushed (and fenced) for this event's head first.
            "labels": [r.to_json() for r in labels],
            "t_held": held.held_ms,  # node clock
            "t_seen": held.seen_ms,
            "t_decided": self.clock(),
            "t_release_sent": release.t_release_sent if release else None,
            "t_release_applied": applied,  # node clock
            "release_state": release.state if release else None,
            "release_error": release.error if release else None,
            # Hold to admission, both on the node's clock.
            "latency_ms": applied - held.held_ms if applied is not None else None,
        }

    def renew_leases(self) -> None:
        """Renew controller leases on every known node (``LeaseRenewer``)."""
        self.renewer.renew(self.graph.known_nodes(), stored_policy_version(self.db))

    def step(self) -> None:
        if self.fault is not None:
            return
        self.catch_up()
        # Describe new nodes before an alert needs them (cached per node).
        self.control.refresh(self.graph.known_nodes())
        self.renew_leases()
        self.handle_alerts()
        self.resolve_frontier()
        self.admit_held()

    # -- observability -----------------------------------------------------------
    def status(self) -> dict:
        high = self.core.execute(
            "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        cursor = self.cursor()
        graph = self.graph
        last = self.decisions[-1] if self.decisions else None
        return {
            "fault": self.fault,
            "profile": self.config.profile,
            "phase1_mode": {
                name: self.config.phase1_mode_for(name) for name in STRATEGIES
            },
            "phase1_labels": self.config.phase1_labels,
            "receding_during_waits": self.config.receding_during_waits,
            "tainted_output_value": self.config.tainted_output_value,
            "mechanisms": {
                name: self.config.mechanisms_for(name).to_json() for name in STRATEGIES
            },
            "cursor": {
                "ingest_cursor": cursor,
                "core_ingest_seq": high,
                "lag": high - cursor,
            },
            "g_p": dict(graph.counts),
            "live": {
                "running": len(graph.running),
                "pending_tasks": len(graph.pending),
                "pending_results": len(graph.results),
                "pending_effects": len(graph.effects),
            },
            "taint": {
                "nodes": len(graph.taint),
                "sessions": len(graph.tainted_sessions),
            },
            # Per-event admission: events held by labels, by state.
            "held_events": dict(
                self.db.execute(
                    "SELECT state,COUNT(*) FROM containment_held GROUP BY state"
                ).fetchall()
            ),
            "g_c": self.last_graph,
            "last_solve": last.get("solve") if last else None,
            "last_decision": last,
            "root_cause": self.last_ranking,
            "in_force": [
                r.to_json()
                for r in sorted(self.in_force.values(), key=lambda r: r.action)
            ],
            # Sent but never confirmed: possibly in force at their nodes.
            "unconfirmed": [
                json.loads(record)
                for (record,) in self.db.execute(
                    "SELECT record_json FROM containment_actions WHERE state='unconfirmed' "
                    "ORDER BY action_json"
                )
            ],
            "incidents": [
                {
                    "incident_id": i,
                    "alert_id": a,
                    "roots": json.loads(r),
                    "strategy": s,
                    "state": st,
                }
                for i, a, r, s, st in self.db.execute(
                    "SELECT incident_id,alert_id,roots_json,strategy,state FROM containment_incidents "
                    "ORDER BY created_ms DESC LIMIT 20"
                )
            ],
            "policy_version": self.db.execute(
                "SELECT policy_version FROM containment_state WHERE singleton=1"
            ).fetchone()[0],
            "control": {
                "owner": self.control.owner,
                "capabilities": {
                    node: sorted(self.control.capabilities(node))
                    for node in sorted(graph.known_nodes())
                },
                "controller_leases": self.leases,
            },
            "coverage_gaps": graph.coverage_gaps,
            "blocked_dispatches": graph.blocked_dispatches,
            # Per node and source epoch: applied through which sequence, and
            # events waiting above a gap (fence.SourceProgress).
            "ingest": self.sources.status(),
        }


def open_core(path: Path) -> sqlite3.Connection:
    # mode=ro: the monitor never writes Core state, and cannot create an empty one.
    return sqlite3.connect(
        path.resolve().as_uri() + "?mode=ro",
        uri=True,
        timeout=0.5,
        check_same_thread=False,
    )


def open_monitor(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, timeout=5.0, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


class ContainmentMonitorService:
    """Owned monitor thread; construct explicitly and close off the event loop."""

    def __init__(
        self,
        core_db: Path,
        *,
        monitor_db: Path | None = None,
        config: MonitorConfig | None = None,
        control: ControlClient | None = None,
        decision_log: Path | None = None,
        poll_seconds: float = POLL_SECONDS,
    ):
        self.core_db = core_db.resolve()
        self.monitor_db = (
            monitor_db or self.core_db.parent / "containment.sqlite3"
        ).resolve()
        self.config = config or MonitorConfig()
        self.control = control or UnconfiguredControlClient(self.config.owner)
        self.decision_log = (
            decision_log or self.core_db.parent / "containment-decisions.jsonl"
        )
        self.poll_seconds = poll_seconds
        self._alerts: queue.Queue[Alert] = queue.Queue()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._status: dict = {"state": "idle", "fault": None}

    def submit_alert(self, payload: object) -> dict:
        alert = parse_external_alert(payload, now_ms=now_ms())
        self._alerts.put(alert)
        return {"alert_id": alert.alert_id, "kind": alert.kind, "state": "queued"}

    def status(self) -> dict:
        with self._lock:
            result = copy.deepcopy(self._status)
        result["worker_alive"] = bool(self._thread and self._thread.is_alive())
        return result

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._main, name="containment-monitor", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(SHUTDOWN_SECONDS)
            if self._thread.is_alive():
                raise RuntimeError("containment_monitor_shutdown_timeout")
        self.control.close()

    def _set(self, **values) -> None:
        with self._lock:
            self._status.update(values)

    def _ready(self) -> bool:
        if not self.core_db.is_file():
            return False
        # The collector creates its tables before it inserts the singleton row
        # that names the collector epoch; follow and status read that row.
        with open_core(self.core_db) as core:
            if (
                core.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_collector'"
                ).fetchone()
                is None
            ):
                return False
            return (
                core.execute(
                    "SELECT 1 FROM trace_collector WHERE singleton=1"
                ).fetchone()
                is not None
            )

    def _main(self) -> None:
        from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer

        try:
            with exclusive_writer(
                self.monitor_db.parent / (self.monitor_db.name + ".writer")
            ):
                monitor = open_monitor(self.monitor_db)
                ProvenanceGraph.initialize(monitor)
                # Leases are renewed from startup: nodes that enforce them deny
                # effects until the first renewal, whatever Core is doing.
                renewer = LeaseRenewer(self.control, self.config)
                try:
                    while not self._stop.is_set() and not self._ready():
                        renewer.renew(
                            set(self.config.agent_nodes.values()),
                            stored_policy_version(monitor),
                        )
                        self._set(
                            state="waiting",
                            fault="collector_unavailable",
                            control={
                                "owner": self.control.owner,
                                "controller_leases": copy.deepcopy(renewer.leases),
                            },
                        )
                        self._stop.wait(self.poll_seconds)
                    if not self._stop.is_set():
                        self._follow(monitor, renewer)
                finally:
                    renewer.close()
                    monitor.close()
        except WriterActiveError:
            self._set(state="paused", fault="monitor_writer_active")
        except sqlite3.Error:
            self._set(state="paused", fault="monitor_storage_unavailable")
        except Exception:  # noqa: BLE001 - thread boundary reports a fixed code, no payload text
            self._set(state="failed", fault="monitor_lifecycle_fault")

    def _follow(self, monitor: sqlite3.Connection, renewer: LeaseRenewer) -> None:
        core = open_core(self.core_db)
        engine = ContainmentMonitor(
            core,
            monitor,
            self.config,
            self.control,
            decision_log=DecisionLog(self.decision_log),
            renewer=renewer,
        )
        try:
            while not self._stop.is_set():
                while True:
                    try:
                        alert = self._alerts.get_nowait()
                    except queue.Empty:
                        break
                    engine.submit(alert)
                started = time.perf_counter()
                engine.step()
                self._set(
                    **engine.status(),
                    state="paused" if engine.fault else "running",
                    last_step_ms=round((time.perf_counter() - started) * 1000, 3),
                )
                self._stop.wait(self.poll_seconds)
        finally:
            engine.close()
            core.close()


def service_from_environment(core_db: Path) -> ContainmentMonitorService:
    """Build the service from ``EDGECITADEL_CONTAINMENT_CONFIG`` (optional JSON).

    ``{"monitor": {...MonitorConfig...}, "control": {"nats": {"key_file":
    "<owner key>", "confirmation_timeout_ms": 5000, "command_lifetime_ms":
    30000}}}`` signs commands with the owner's key and sends them on Core NATS
    (``NATS_URL``/``NATS_TOKEN``, as the collector). ``{"control": {"agentd":
    {"<node_id>": {"socket": "<agentd.sock>", "admin_token_file":
    "<admin.token>"}}}}`` uses co-located admin sockets instead. Without
    control, decisions are computed and logged but every action is recorded as
    not applied (``node_unreachable``).
    """
    path = os.environ.get("EDGECITADEL_CONTAINMENT_CONFIG")
    document = json.loads(Path(path).read_text()) if path else {}
    if not isinstance(document, dict) or set(document) - {"monitor", "control"}:
        raise ConfigError("invalid_containment_config")
    config = MonitorConfig.from_json(document.get("monitor", {}))
    return ContainmentMonitorService(
        core_db, config=config, control=_control(config, document.get("control", {}))
    )


def _control(config: MonitorConfig, document: object) -> ControlClient:
    if not isinstance(document, dict) or len(document) > 1:
        raise ConfigError("invalid_containment_control")
    if "nats" in document:
        settings = document["nats"]
        allowed = {"key_file", "confirmation_timeout_ms", "command_lifetime_ms"}
        if (
            not isinstance(settings, dict)
            or set(settings) - allowed
            or not isinstance(settings.get("key_file"), str)
            or not all(
                type(settings[key]) is int and settings[key] > 0
                for key in allowed - {"key_file"}
                if key in settings
            )
        ):
            raise ConfigError("invalid_containment_control")
        try:
            return NatsControlClient.from_key_file(
                os.environ["NATS_URL"],
                os.environ.get("NATS_TOKEN") or None,
                config.owner,
                Path(settings["key_file"]),
                confirmation_timeout_s=settings.get("confirmation_timeout_ms", 5000)
                / 1000,
                command_lifetime_ms=settings.get("command_lifetime_ms", 30_000),
            )
        except KeyError:
            raise ConfigError("containment_control_nats_unavailable") from None
        except (OSError, ValueError):
            raise ConfigError("invalid_containment_control_key") from None
    nodes = document.get("agentd", {})
    if not isinstance(nodes, dict) or not all(
        isinstance(v, dict) and set(v) == {"socket", "admin_token_file"}
        for v in nodes.values()
    ):
        raise ConfigError("invalid_containment_control")
    if not nodes:
        return UnconfiguredControlClient(config.owner)
    return LocalAgentdControlClient.from_sockets(
        config.owner,
        {
            node: (Path(v["socket"]), Path(v["admin_token_file"]))
            for node, v in nodes.items()
        },
    )


__all__ = [
    "AlertError",
    "ContainmentMonitor",
    "ContainmentMonitorService",
    "DecisionLog",
]
