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
from pathlib import Path

from ..trace_payload_read import read_payload
from . import rootcause
from .config import ConfigError, MonitorConfig
from .control import ControlClient, LocalAgentdControlClient, UnconfiguredControlClient
from .detectors import detect
from .enforcement import (
    ActionRecord,
    Decision,
    EnforcementDriver,
    TwoPhaseProtocol,
    now_ms,
)
from .graph import ControlGraph
from .model import FAILURE_KINDS, Action, Alert, AlertError, parse_external_alert, ver
from .projection import project
from .provenance import ProvenanceGraph
from .strategies import CUT_STRATEGIES, DISPLAY, STRATEGIES, Context

BATCH = 256
MAX_CATCH_UP_BATCHES = 64
POLL_SECONDS = 0.1
SHUTDOWN_SECONDS = 10.0
RECENT_DECISIONS = 20


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
    ):
        self.core = core
        self.db = monitor
        self.config = config
        self.control = control
        self.log = decision_log
        self.clock = clock
        self.batch = batch
        ProvenanceGraph.initialize(monitor)
        self.graph = ProvenanceGraph(monitor, config)
        self.graph.reload()
        self.driver = EnforcementDriver(
            control, config.owner, self._next_version, clock=clock
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

    def close(self) -> None:
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
            "SELECT ingest_seq FROM trace_raw_events WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
            (cursor, self.batch),
        ).fetchall()
        if not rows:
            return 0
        with self.graph.transaction():
            for (seq,) in rows:
                payload = read_payload(self.core, seq)
                event = payload["event"] if payload else None
                if event is None:
                    continue  # expired payload: identity only, nothing to join
                self.graph.apply(event, seq)
                for alert in detect(event, self.graph):
                    self._store_alert(alert)
            self.db.execute(
                "UPDATE containment_state SET ingest_cursor=?,collector_epoch=? WHERE singleton=1",
                (rows[-1][0], epoch),
            )
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

    def _project(
        self, phase: int, in_force: frozenset[Action], static: bool = False
    ) -> ControlGraph:
        capabilities = self.control.capabilities
        if phase == 1:
            capabilities = frozenset(c for c in capabilities if c.startswith("hold:"))
        cg = project(
            self.graph,
            self.config,
            now_ms=self.clock(),
            capabilities=capabilities,
            static=static,
            in_force=in_force,
            tau_ms=0.0 if phase == 1 else None,
        )
        self.last_graph = dict(cg.stats)
        return cg

    def respond(
        self,
        alert: Alert,
        *,
        strategy: str | None = None,
        roots: list[str] | None = None,
    ) -> Decision:
        t_received = self.clock()
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
        decision = Decision(incident_id, name)

        def choose(cg: ControlGraph):
            context = Context(
                self.graph,
                self.config,
                cg,
                lambda: self._project(2, frozenset(self.in_force), static=True),
                roots,
                self.control.capabilities,
            )
            return STRATEGIES[name](context)

        self.protocol.run(
            decision,
            t_alert=t_alert,
            project=self._project,
            reconcile=self.catch_up,
            choose=choose,
            in_force=self.in_force,
            two_phase=name in CUT_STRATEGIES,
        )
        self._persist(decision)
        return decision

    def _persist(self, decision: Decision) -> None:
        with self.db:
            for record in [*decision.phase1, *decision.phase2, *decision.released]:
                state = {"in_force": "in_force", "released": "released"}.get(
                    record.state, "failed"
                )
                if record.state == "release_failed":
                    state = "in_force"
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

    def step(self) -> None:
        if self.fault is not None:
            return
        self.catch_up()
        self.handle_alerts()
        self.resolve_frontier()

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
            "g_c": self.last_graph,
            "last_solve": last.get("solve") if last else None,
            "last_decision": last,
            "root_cause": self.last_ranking,
            "in_force": [
                r.to_json()
                for r in sorted(self.in_force.values(), key=lambda r: r.action)
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
            "control": {"capabilities": sorted(self.control.capabilities)},
            "coverage_gaps": graph.coverage_gaps,
            "blocked_dispatches": graph.blocked_dispatches,
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
        self.control = control or UnconfiguredControlClient()
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

    def _set(self, **values) -> None:
        with self._lock:
            self._status.update(values)

    def _ready(self) -> bool:
        if not self.core_db.is_file():
            return False
        with open_core(self.core_db) as core:
            return (
                core.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_collector'"
                ).fetchone()
                is not None
            )

    def _main(self) -> None:
        from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer

        try:
            while not self._stop.is_set() and not self._ready():
                self._set(state="waiting", fault="collector_unavailable")
                self._stop.wait(self.poll_seconds)
            if self._stop.is_set():
                return
            with exclusive_writer(
                self.monitor_db.parent / (self.monitor_db.name + ".writer")
            ):
                core, monitor = open_core(self.core_db), open_monitor(self.monitor_db)
                engine = ContainmentMonitor(
                    core,
                    monitor,
                    self.config,
                    self.control,
                    decision_log=DecisionLog(self.decision_log),
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
                            last_step_ms=round(
                                (time.perf_counter() - started) * 1000, 3
                            ),
                        )
                        self._stop.wait(self.poll_seconds)
                finally:
                    engine.close()
                    core.close()
                    monitor.close()
        except WriterActiveError:
            self._set(state="paused", fault="monitor_writer_active")
        except sqlite3.Error:
            self._set(state="paused", fault="monitor_storage_unavailable")
        except Exception:  # noqa: BLE001 - thread boundary reports a fixed code, no payload text
            self._set(state="failed", fault="monitor_lifecycle_fault")


def service_from_environment(core_db: Path) -> ContainmentMonitorService:
    """Build the service from ``EDGECITADEL_CONTAINMENT_CONFIG`` (optional JSON).

    ``{"monitor": {...MonitorConfig...}, "control": {"agentd": {"<node_id>":
    {"socket": "<agentd.sock>", "admin_token_file": "<admin.token>"}}}}``.
    Without control endpoints, decisions are computed and logged but every
    action is recorded as unconfirmed (``node_unreachable``).
    """
    path = os.environ.get("EDGECITADEL_CONTAINMENT_CONFIG")
    document = json.loads(Path(path).read_text()) if path else {}
    if not isinstance(document, dict) or set(document) - {"monitor", "control"}:
        raise ConfigError("invalid_containment_config")
    config = MonitorConfig.from_json(document.get("monitor", {}))
    nodes = document.get("control", {}).get("agentd", {})
    if not isinstance(nodes, dict) or not all(
        isinstance(v, dict) and set(v) == {"socket", "admin_token_file"}
        for v in nodes.values()
    ):
        raise ConfigError("invalid_containment_control")
    control: ControlClient = (
        LocalAgentdControlClient.from_sockets(
            {
                node: (Path(v["socket"]), Path(v["admin_token_file"]))
                for node, v in nodes.items()
            }
        )
        if nodes
        else UnconfiguredControlClient()
    )
    return ContainmentMonitorService(core_db, config=config, control=control)


__all__ = [
    "AlertError",
    "ContainmentMonitor",
    "ContainmentMonitorService",
    "DecisionLog",
]
