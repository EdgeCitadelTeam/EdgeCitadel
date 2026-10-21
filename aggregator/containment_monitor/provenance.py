"""Append-only provenance graph G_P, live state X_t and the taint index.

G_P is persisted in the monitor database (one row per invocation, task, version and
effect, one row per influence edge). Only the live frontier (running invocations,
dispatched-but-unstarted tasks, unconsumed results, pending effects) and the taint
index live in memory; ``reload`` rebuilds both from the tables, so the caller can
roll back a failed batch and resume exactly where the persisted cursor says.

Joins are order-independent: a version is keyed by its lineage ``version_id`` on
every node, a dispatched child by ``child_task_id``, an invocation by its execution
attempt. Events from different nodes may arrive in any order; each insert is
idempotent and the task state only advances (pending < running < terminal).

Edges (influence direction):
- ``inv -> ver`` data: the invocation generated the request or result version;
- ``ver -> inv`` data: the invocation used it;
- ``inv -> inv`` session: consecutive invocations of one agent in one conversation
  context (conversation memory carries over), unless the agent is session-isolated;
  for a shared-memory agent, consecutive invocations whatever their context;
- ``inv -> task`` and ``task -> inv`` authority: a dispatch grant and its execution;
- ``inv -> fx`` data: an observed tool call or an effect.
"""

from __future__ import annotations

import heapq
import json
import math
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from .config import MonitorConfig
from .model import AUTHORITY, DATA, SESSION, TAINT_EDGE_TYPES, fx, inv, split, task, ver

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS containment_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    ingest_cursor INTEGER NOT NULL,
    collector_epoch TEXT,
    policy_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS containment_invocations (
    attempt_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    task_id TEXT,
    trace_id TEXT,
    context_id TEXT,
    started_ms INTEGER NOT NULL,
    ended_ms INTEGER,
    outcome TEXT,
    authority_json TEXT,
    seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS containment_invocations_task ON containment_invocations(task_id);
CREATE INDEX IF NOT EXISTS containment_invocations_session
    ON containment_invocations(agent_id, context_id, started_ms);
CREATE TABLE IF NOT EXISTS containment_tasks (
    task_id TEXT PRIMARY KEY,
    recipient_id TEXT,
    recipient_node TEXT,
    sender_id TEXT,
    sender_node TEXT,
    parent_attempt_id TEXT,
    parent_task_id TEXT,
    context_id TEXT,
    trace_id TEXT,
    created_ms INTEGER,
    arrived_ms INTEGER,
    request_version TEXT,
    result_version TEXT,
    state TEXT NOT NULL CHECK(state IN ('pending', 'running', 'terminal')),
    seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS containment_tasks_parent ON containment_tasks(parent_attempt_id);
CREATE TABLE IF NOT EXISTS containment_versions (
    version_id TEXT PRIMARY KEY,
    entity TEXT,
    entity_task_id TEXT,
    producer_attempt TEXT,
    external INTEGER NOT NULL DEFAULT 0,
    generated_ms INTEGER,
    seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS containment_effects (
    effect_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    effect_class TEXT NOT NULL,
    node_id TEXT NOT NULL,
    protected INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending', 'committed', 'denied', 'observed')),
    scheduled_ms INTEGER,
    at_ms INTEGER NOT NULL,
    seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS containment_edges (
    src TEXT NOT NULL,
    dst TEXT NOT NULL,
    etype TEXT NOT NULL,
    at_ms INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    PRIMARY KEY(src, dst, etype)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS containment_edges_dst ON containment_edges(dst, etype);
CREATE TABLE IF NOT EXISTS containment_alerts (
    alert_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    origin TEXT NOT NULL,
    observed_ms INTEGER NOT NULL,
    alert_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued', 'handled', 'evidence'))
);
CREATE INDEX IF NOT EXISTS containment_alerts_kind ON containment_alerts(kind, observed_ms);
CREATE TABLE IF NOT EXISTS containment_incidents (
    incident_id TEXT PRIMARY KEY,
    alert_id TEXT NOT NULL,
    roots_json TEXT NOT NULL,
    strategy TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active', 'closed')),
    created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS containment_actions (
    action_json TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    phase INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('in_force', 'released', 'failed', 'unconfirmed')),
    policy_version INTEGER NOT NULL,
    record_json TEXT NOT NULL
);
"""

ROOT_TIME = 0


def taint_step(src: str, src_time: int, etype: str, at: int) -> int | None:
    """When taint crosses one edge, or None if it cannot.

    An invocation taints an output it generated only if it was already tainted when
    it generated it: both times come from the invocation's own node clock. A
    version taints every consumer from the moment of use (consumer's clock; the
    two nodes' clocks are never compared). Conversation memory carries taint to
    the next invocation of the session regardless of timing (conservative).
    """
    if etype == SESSION:
        return max(src_time, at)
    if src.startswith("inv:"):
        return at if src_time <= at else None
    return at


TASK_TERMINAL = frozenset(
    {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
)
RUN_TERMINAL = frozenset({"completed", "failed", "canceled", "interrupted", "unknown"})
STATE_RANK = {"pending": 0, "running": 1, "terminal": 2}


def occurred_ms(value: str) -> int:
    """Event times are UTC ``YYYY-MM-DDTHH:MM:SS.mmmZ`` (trace contract)."""
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


@dataclass
class Invocation:
    attempt_id: str
    agent_id: str
    node_id: str
    task_id: str | None
    context_id: str | None
    started_ms: int


@dataclass
class PendingTask:
    """A dispatched or submitted task whose invocation has not started.

    ``created_ms`` is the dispatch time on the sender's clock; ``arrived_ms`` the
    first evidence from the recipient's daemon, when observed.
    """

    task_id: str
    recipient_id: str
    recipient_node: str | None
    sender_attempt: str | None
    sender_node: str | None
    context_id: str | None
    created_ms: int
    arrived_ms: int | None
    request_version: str | None


PENDING_COLUMNS = (
    "task_id,recipient_id,recipient_node,parent_attempt_id,sender_node,context_id,"
    "COALESCE(created_ms,0),arrived_ms,request_version"
)


@dataclass
class PendingResult:
    """A generated child result its (live) parent invocation has not used yet."""

    version_id: str
    task_id: str
    producer_attempt: str | None
    parent_attempt: str
    generated_ms: int


@dataclass
class PendingEffect:
    """A scheduled effect agentd authorized but has not committed yet.

    ``node_id`` is the issuing invocation's node, whose effect gateway decides
    the commit and enforces an effect hold.
    """

    effect_id: str
    attempt_id: str
    effect_class: str
    node_id: str
    scheduled_ms: int
    protected: bool


class ProvenanceGraph:
    """G_P over one monitor connection. Mutations require an open transaction."""

    def __init__(self, db: sqlite3.Connection, config: MonitorConfig):
        self.db = db
        self.config = config
        self.running: dict[str, Invocation] = {}
        self.pending: dict[str, PendingTask] = {}
        self.results: dict[str, PendingResult] = {}
        self.effects: dict[str, PendingEffect] = {}
        self.agent_nodes: dict[str, str] = {}
        # Agents named as recipients by exported authority (they may be unseen).
        self.granted_agents: set[str] = set()
        self.revoked: set[str] = set()
        # Taint index: node -> earliest time it may carry taint (ROOT_TIME for roots).
        self.taint: dict[str, int] = {}
        self.tainted_sessions: set[tuple[str, str]] = set()
        # Set when taint reaches a live element; the service clears it after re-solving.
        self.frontier_advanced = False
        self.coverage_gaps = 0
        self.blocked_dispatches = 0
        self.counts = {"nodes": 0, "edges": 0}

    # -- lifecycle ------------------------------------------------------------
    @staticmethod
    def initialize(db: sqlite3.Connection) -> None:
        db.executescript(SCHEMA_SQL)
        with db:
            db.execute(
                "INSERT OR IGNORE INTO containment_state(singleton,ingest_cursor,collector_epoch,policy_version) "
                "VALUES (1,0,NULL,0)"
            )

    def reload(self) -> None:
        """Rebuild the in-memory frontier and taint index from persisted rows."""
        db = self.db
        self.agent_nodes = dict(self.config.agent_nodes)
        for agent, node in db.execute(
            "SELECT agent_id,node_id FROM containment_invocations ORDER BY started_ms"
        ):
            self.agent_nodes[agent] = node
        for agent, node in db.execute(
            "SELECT recipient_id,recipient_node FROM containment_tasks "
            "WHERE recipient_id IS NOT NULL AND recipient_node IS NOT NULL"
        ):
            self.agent_nodes.setdefault(agent, node)
        self.granted_agents = set()
        for (encoded,) in db.execute(
            "SELECT authority_json FROM containment_invocations "
            "WHERE authority_json IS NOT NULL"
        ):
            self.granted_agents.update(json.loads(encoded)["recipients"] or ())
        self.running = {
            row[0]: Invocation(*row)
            for row in db.execute(
                "SELECT attempt_id,agent_id,node_id,task_id,context_id,started_ms "
                "FROM containment_invocations WHERE ended_ms IS NULL"
            )
        }
        self.pending = {
            row[0]: PendingTask(*row)
            for row in db.execute(
                f"SELECT {PENDING_COLUMNS} FROM containment_tasks "
                "WHERE state='pending' AND recipient_id IS NOT NULL"
            )
        }
        self.results = {}
        for row in db.execute(
            "SELECT v.version_id,v.entity_task_id,v.producer_attempt,t.parent_attempt_id,"
            "COALESCE(v.generated_ms,0) FROM containment_versions v "
            "JOIN containment_tasks t ON t.task_id=v.entity_task_id "
            "WHERE v.entity='result' AND t.parent_attempt_id IS NOT NULL"
        ):
            self._consider_result(PendingResult(*row))
        self.effects = {
            row[0]: PendingEffect(*row[:5], bool(row[5]))
            for row in db.execute(
                "SELECT effect_id,attempt_id,effect_class,node_id,scheduled_ms,protected "
                "FROM containment_effects WHERE state='pending' AND scheduled_ms IS NOT NULL"
            )
        }
        self.revoked = {
            split(row[0])[1]
            for row in db.execute(
                "SELECT subject FROM containment_alerts WHERE kind='version_revoked'"
            )
        }
        self.taint = {}
        self.tainted_sessions = set()
        roots: list[str] = []
        for (encoded,) in db.execute(
            "SELECT roots_json FROM containment_incidents WHERE state='active'"
        ):
            roots.extend(json.loads(encoded))
        self.mark_roots(roots)
        self.frontier_advanced = False
        self.counts = {
            "nodes": sum(
                db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "containment_invocations",
                    "containment_tasks",
                    "containment_versions",
                    "containment_effects",
                )
            ),
            "edges": db.execute("SELECT COUNT(*) FROM containment_edges").fetchone()[0],
        }

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """One atomic batch; memory is rebuilt from disk if the batch fails."""
        try:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                yield
        except BaseException:
            self.reload()
            raise

    # -- event application ---------------------------------------------------
    def apply(self, event: dict, seq: int) -> None:
        kind = event["kind"]
        handler = {
            "run": self._run,
            "task": self._task,
            "dispatch": self._dispatch,
            "lineage": self._lineage,
            "tool": self._tool,
            "effect": self._effect,
        }.get(kind)
        if handler is not None:
            handler(event, seq, occurred_ms(event["occurred_at"]))
        elif kind == "coverage" and event["phase"] == "lost":
            self.coverage_gaps += 1

    def _run(self, event: dict, seq: int, at: int) -> None:
        attempt = event["execution_attempt_id"]
        if attempt is None:
            return
        if event["phase"] == "started":
            agent, node, task_id = event["agent_id"], event["node_id"], event["task_id"]
            # The invocation's effective authority as agentd computed it.
            authority = event["attributes"].get("authority")
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO containment_invocations"
                "(attempt_id,agent_id,node_id,task_id,trace_id,context_id,started_ms,"
                "authority_json,seq) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    attempt,
                    agent,
                    node,
                    task_id,
                    event["trace_id"],
                    event["context_id"],
                    at,
                    json.dumps(authority, sort_keys=True) if authority else None,
                    seq,
                ),
            ).rowcount
            if not inserted:
                return
            self.counts["nodes"] += 1
            self.agent_nodes[agent] = node
            if authority:
                self.granted_agents.update(authority["recipients"] or ())
            if task_id is not None:
                self._upsert_task(
                    task_id,
                    seq,
                    state="running",
                    recipient_id=agent,
                    recipient_node=node,
                    context_id=event["context_id"],
                    trace_id=event["trace_id"],
                )
                self._edge(task(task_id), inv(attempt), AUTHORITY, at, seq)
                self.pending.pop(task_id, None)
            self._session_edges(attempt, agent, event["context_id"], at, seq)
            ended = self.db.execute(
                "SELECT ended_ms FROM containment_invocations WHERE attempt_id=?",
                (attempt,),
            ).fetchone()[0]
            if ended is None:
                self.running[attempt] = Invocation(
                    attempt, agent, node, task_id, event["context_id"], at
                )
                if inv(attempt) in self.taint:
                    self.frontier_advanced = True
        elif event["phase"] in RUN_TERMINAL:
            self._end_invocation(attempt, at, event["phase"], seq)

    def _end_invocation(self, attempt: str, at: int, outcome: str, seq: int) -> None:
        updated = self.db.execute(
            "UPDATE containment_invocations SET ended_ms=?,outcome=? "
            "WHERE attempt_id=? AND ended_ms IS NULL",
            (at, outcome, attempt),
        ).rowcount
        if not updated:
            # One node exports in source order, so a start precedes its own end.
            return
        self.running.pop(attempt, None)
        for version, pending in list(self.results.items()):
            if pending.parent_attempt == attempt:
                del self.results[version]

    def _task(self, event: dict, seq: int, at: int) -> None:
        task_id, phase = event["task_id"], event["phase"]
        role = event["attributes"].get("source_role")
        fields: dict[str, object] = {
            "context_id": event["context_id"],
            "trace_id": event["trace_id"],
            "parent_task_id": event["parent_task_id"],
        }
        if role == "recipient":
            fields.update(
                recipient_id=event["agent_id"], recipient_node=event["node_id"]
            )
        if phase in {"created", "queued"} and role == "sender":
            fields.update(created_ms=at)
            if event["parent_task_id"] is None:
                fields.update(sender_id=event["agent_id"], sender_node=event["node_id"])
        if role == "daemon" and phase in {"queued", "offered"}:
            # The receiving daemon: the task has arrived at the recipient's node.
            fields.update(arrived_ms=at, recipient_node=event["node_id"])
        state = (
            "terminal"
            if phase in TASK_TERMINAL
            else "running"
            if phase == "running"
            else "pending"
        )
        self._upsert_task(task_id, seq, state=state, **fields)
        if state == "terminal":
            self.pending.pop(task_id, None)
            for attempt, live in list(self.running.items()):
                if live.task_id == task_id and role in {"recipient", "daemon"}:
                    self._end_invocation(attempt, at, "task_" + phase, seq)
        else:
            self._refresh_pending(task_id)

    def _dispatch(self, event: dict, seq: int, at: int) -> None:
        attributes = event["attributes"]
        if event["phase"] == "denied" and attributes.get("reason") == "execution_held":
            self.blocked_dispatches += 1
        if event["phase"] != "allowed" or "child_task_id" not in attributes:
            return
        child = attributes["child_task_id"]
        attempt = event["execution_attempt_id"]
        self._upsert_task(
            child,
            seq,
            state="pending",
            recipient_id=attributes["recipient_id"],
            sender_id=event["agent_id"],
            sender_node=event["node_id"],
            parent_attempt_id=attempt,
            parent_task_id=event["task_id"],
            context_id=event["context_id"],
            trace_id=event["trace_id"],
            created_ms=at,
        )
        if attempt is not None:
            self._edge(inv(attempt), task(child), AUTHORITY, at, seq)
        self._refresh_pending(child)

    def _lineage(self, event: dict, seq: int, at: int) -> None:
        attributes = event["attributes"]
        version, entity = attributes["version_id"], attributes["entity"]
        entity_task = attributes["entity_task_id"]
        attempt = event["execution_attempt_id"]
        generated = event["phase"] == "generated"
        self._count_new("containment_versions", "version_id", version)
        self.db.execute(
            "INSERT INTO containment_versions"
            "(version_id,entity,entity_task_id,producer_attempt,external,generated_ms,seq) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(version_id) DO UPDATE SET "
            "producer_attempt=COALESCE(producer_attempt,excluded.producer_attempt),"
            "generated_ms=COALESCE(generated_ms,excluded.generated_ms),"
            "external=MAX(external,excluded.external)",
            (
                version,
                entity,
                entity_task,
                attempt if generated else None,
                int(bool(attributes.get("external"))),
                at if generated else None,
                seq,
            ),
        )
        column = "request_version" if entity == "request" else "result_version"
        self.db.execute(
            f"UPDATE containment_tasks SET {column}=COALESCE({column},?) WHERE task_id=?",
            (version, entity_task),
        )
        if generated:
            self._edge(inv(attempt), ver(version), DATA, at, seq)
            if entity == "request":
                self._refresh_pending(entity_task)
            else:
                parent = self.db.execute(
                    "SELECT parent_attempt_id FROM containment_tasks WHERE task_id=?",
                    (entity_task,),
                ).fetchone()
                if parent and parent[0]:
                    self._consider_result(
                        PendingResult(version, entity_task, attempt, parent[0], at)
                    )
        else:
            self._edge(ver(version), inv(attempt), DATA, at, seq)
            pending = self.results.get(version)
            if pending is not None and pending.parent_attempt == attempt:
                del self.results[version]

    def _tool(self, event: dict, seq: int, at: int) -> None:
        # Observed tool calls happened already: history, not a controllable flow.
        attempt, span = event["execution_attempt_id"], event["span_id"]
        if event["phase"] != "started" or attempt is None or span is None:
            return
        effect_class = "tool:" + event["attributes"]["name"]
        self._record_effect(
            span, attempt, effect_class, event["node_id"], "observed", None, at, seq
        )

    def _effect(self, event: dict, seq: int, at: int) -> None:
        """agentd's effect gateway: ``requested``, then ``committed`` or ``denied``.

        An immediate effect is decided in the same transaction as its request, so
        only a scheduled one (``scheduled_at_ms``, the earliest commit) stays
        pending and controllable until its commit or denial arrives.
        """
        attributes = event["attributes"]
        effect_id, attempt = attributes["effect_id"], event["execution_attempt_id"]
        state = {
            "requested": "pending",
            "committed": "committed",
            "denied": "denied",
        }.get(event["phase"])
        if state is None or attempt is None:
            return
        scheduled = attributes["scheduled_at_ms"]
        self._record_effect(
            effect_id,
            attempt,
            attributes["effect_class"],
            event["node_id"],
            state,
            scheduled,
            at,
            seq,
        )

    def _record_effect(
        self, effect_id, attempt, effect_class, node, state, scheduled, at, seq
    ) -> None:
        protected = effect_class in self.config.protected_effect_classes
        self._count_new("containment_effects", "effect_id", effect_id)
        self.db.execute(
            "INSERT INTO containment_effects"
            "(effect_id,attempt_id,effect_class,node_id,protected,state,scheduled_ms,at_ms,seq) "
            "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(effect_id) DO UPDATE SET "
            "state=CASE WHEN containment_effects.state='pending' THEN excluded.state "
            "ELSE containment_effects.state END",
            (
                effect_id,
                attempt,
                effect_class,
                node,
                int(protected),
                state,
                scheduled,
                at,
                seq,
            ),
        )
        self._edge(inv(attempt), fx(effect_id), DATA, at, seq)
        current, scheduled = self.db.execute(
            "SELECT state,scheduled_ms FROM containment_effects WHERE effect_id=?",
            (effect_id,),
        ).fetchone()
        if current == "pending" and scheduled is not None:
            self.effects[effect_id] = PendingEffect(
                effect_id, attempt, effect_class, node, scheduled, protected
            )
        else:
            self.effects.pop(effect_id, None)

    # -- helpers ---------------------------------------------------------------
    def _count_new(self, table: str, column: str, key: str) -> None:
        if (
            self.db.execute(
                f"SELECT 1 FROM {table} WHERE {column}=?", (key,)
            ).fetchone()
            is None
        ):
            self.counts["nodes"] += 1

    def _upsert_task(
        self, task_id: str, seq: int, *, state: str, **fields: object
    ) -> None:
        row = self.db.execute(
            "SELECT state FROM containment_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            columns = ["task_id", "state", "seq", *fields]
            self.db.execute(
                f"INSERT INTO containment_tasks({','.join(columns)}) "
                f"VALUES ({','.join('?' * len(columns))})",
                (task_id, state, seq, *fields.values()),
            )
            self.counts["nodes"] += 1
            return
        # Fill unknown fields only; identities never change once observed.
        sets = [f"{key}=COALESCE({key},?)" for key in fields]
        new_state = state if STATE_RANK[state] > STATE_RANK[row[0]] else row[0]
        self.db.execute(
            f"UPDATE containment_tasks SET {', '.join([*sets, 'state=?'])} WHERE task_id=?",
            (*fields.values(), new_state, task_id),
        )

    def _refresh_pending(self, task_id: str) -> None:
        row = self.db.execute(
            f"SELECT {PENDING_COLUMNS},state FROM containment_tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if row is None or row[9] != "pending" or row[1] is None:
            self.pending.pop(task_id, None)
            return
        self.pending[task_id] = PendingTask(*row[:9])
        if row[2] is not None:
            self.agent_nodes.setdefault(row[1], row[2])
        if self.tainted_request(self.pending[task_id]):
            self.frontier_advanced = True

    def _consider_result(self, result: PendingResult) -> None:
        if result.parent_attempt not in self.running:
            return
        used = self.db.execute(
            "SELECT 1 FROM containment_edges WHERE src=? AND dst=? AND etype='data'",
            (ver(result.version_id), inv(result.parent_attempt)),
        ).fetchone()
        if used is None:
            self.results[result.version_id] = result
            if ver(result.version_id) in self.taint:
                self.frontier_advanced = True

    def _session_edges(
        self, attempt: str, agent: str, context: str | None, at: int, seq: int
    ) -> None:
        session = self.config.session_of(agent, context)
        if session is None:
            return
        # A shared-memory agent's invocations continue each other across contexts.
        scope = "" if session == "*" else "AND context_id=? "
        values = () if session == "*" else (context,)
        previous = self.db.execute(
            f"SELECT attempt_id FROM containment_invocations WHERE agent_id=? {scope}"  # noqa: S608
            "AND attempt_id<>? AND started_ms<=? ORDER BY started_ms DESC,attempt_id DESC LIMIT 1",
            (agent, *values, attempt, at),
        ).fetchone()
        following = self.db.execute(
            f"SELECT attempt_id FROM containment_invocations WHERE agent_id=? {scope}"  # noqa: S608
            "AND attempt_id<>? AND started_ms>? ORDER BY started_ms,attempt_id LIMIT 1",
            (agent, *values, attempt, at),
        ).fetchone()
        if previous:
            self._edge(inv(previous[0]), inv(attempt), SESSION, at, seq)
        if following:
            self._edge(inv(attempt), inv(following[0]), SESSION, at, seq)

    def _edge(self, src: str, dst: str, etype: str, at: int, seq: int) -> None:
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO containment_edges(src,dst,etype,at_ms,seq) VALUES (?,?,?,?,?)",
            (src, dst, etype, at, seq),
        )
        if cursor.rowcount:
            self.counts["edges"] += 1
            if etype in TAINT_EDGE_TYPES and src in self.taint:
                reached = taint_step(src, self.taint[src], etype, at)
                if reached is not None and reached < self.taint.get(dst, math.inf):
                    self._spread([(dst, reached)])

    # -- taint index -------------------------------------------------------------
    def mark_roots(self, roots: Iterable[str]) -> set[str]:
        """Taint roots from the beginning and propagate; returns newly tainted nodes."""
        return self._spread([(root, ROOT_TIME) for root in roots])

    def _spread(self, seeds: Iterable[tuple[str, int]]) -> set[str]:
        """Earliest taint times by Dijkstra over ``taint_step``; updates the index."""
        added: set[str] = set()
        heap = []
        for node, time_ in seeds:
            if time_ < self.taint.get(node, math.inf):
                added.update([] if node in self.taint else [node])
                self.taint[node] = time_
                heap.append((time_, node))
        heapq.heapify(heap)
        while heap:
            time_, node = heapq.heappop(heap)
            if time_ > self.taint[node]:
                continue
            for child, etype, at in self.successors_timed(node, TAINT_EDGE_TYPES):
                reached = taint_step(node, time_, etype, at)
                if reached is not None and reached < self.taint.get(child, math.inf):
                    if child not in self.taint:
                        added.add(child)
                    self.taint[child] = reached
                    heapq.heappush(heap, (reached, child))
        for node in added:
            kind, identity = split(node)
            if kind == "inv":
                row = self.db.execute(
                    "SELECT agent_id,context_id FROM containment_invocations WHERE attempt_id=?",
                    (identity,),
                ).fetchone()
                session = self.config.session_of(row[0], row[1]) if row else None
                if session is not None:
                    self.tainted_sessions.add((row[0], session))
                if identity in self.running:
                    self.frontier_advanced = True
            elif kind == "ver" and identity in self.results:
                self.frontier_advanced = True
            elif kind == "fx" and identity in self.effects:
                self.frontier_advanced = True
        if added and any(self.tainted_request(item) for item in self.pending.values()):
            self.frontier_advanced = True
        return added

    def reconcile_known_commits(self, source_side: Mapping[str, float]) -> set[str]:
        """Conservative reconciliation (J'): a known commit taints its head
        whenever its tail is on the controller's source side, whatever order
        the per-site journals arrived in. Returns the newly tainted nodes.

        ``source_side`` maps the source side's invocations to the earliest time
        they can be tainted. Two kinds of commits are known with their tail
        missing or not yet tainted:

        - a version some invocation used while its producer's journal has not
          arrived (no ``generated`` edge yet): its producer is the dispatching
          invocation for a request, the child's invocation for a result; if
          that producer is tainted or on the source side, the version is;
        - a commit (data or session edge) of a source-side invocation at or
          after the earliest time it can be tainted.

        Taint added this way is conservative (it may over-taint) and lives in
        the index only: ``reload`` rebuilds the index from the roots, and the
        next reconciliation adds it again.
        """
        seeds: list[tuple[str, int]] = []
        for version_id, entity, entity_task in self.db.execute(
            "SELECT version_id,entity,entity_task_id FROM containment_versions "
            "WHERE producer_attempt IS NULL AND external=0"
        ).fetchall():
            node = ver(version_id)
            if node in self.taint or entity_task is None:
                continue
            producer_task = entity_task
            if entity == "request":
                row = self.task_row(entity_task)
                producer_task = row["parent_task_id"] if row else None
            if producer_task is None:
                continue
            times = [
                self.taint[candidate]
                if candidate in self.taint
                else source_side.get(candidate)
                for candidate in map(inv, self.invocations_of_task(producer_task))
            ]
            known = [t for t in times if t is not None]
            if known:
                seeds.append((node, int(min(known))))
        for node, earliest in source_side.items():
            if node in self.taint:
                continue
            for child, _, at in self.successors_timed(node, TAINT_EDGE_TYPES):
                if at >= earliest and child not in self.taint:
                    seeds.append((child, at))
        return self._spread(seeds)

    def closure(self, roots: Iterable[str]) -> dict[str, int]:
        """The taint ``mark_roots`` would add, without changing the index."""
        found: dict[str, int] = {}
        heap = [(ROOT_TIME, root) for root in roots]
        for _, root in heap:
            found[root] = ROOT_TIME
        heapq.heapify(heap)
        while heap:
            time_, node = heapq.heappop(heap)
            if time_ > found[node]:
                continue
            for child, etype, at in self.successors_timed(node, TAINT_EDGE_TYPES):
                reached = taint_step(node, time_, etype, at)
                if reached is not None and reached < found.get(child, math.inf):
                    found[child] = reached
                    heapq.heappush(heap, (reached, child))
        return found

    def tainted_request(self, pending: PendingTask) -> bool:
        # The request version is recorded with the dispatch; the sender's taint
        # stands in only while that lineage has not arrived.
        if pending.request_version is not None:
            return ver(pending.request_version) in self.taint
        return (
            pending.sender_attempt is not None
            and inv(pending.sender_attempt) in self.taint
        )

    # -- queries --------------------------------------------------------------------
    def successors(self, node: str, etypes: Iterable[str]) -> list[str]:
        return [child for child, _, _ in self.successors_timed(node, etypes)]

    def successors_timed(
        self, node: str, etypes: Iterable[str]
    ) -> list[tuple[str, str, int]]:
        kinds = tuple(etypes)
        return self.db.execute(
            "SELECT dst,etype,at_ms FROM containment_edges WHERE src=? "
            f"AND etype IN ({','.join('?' * len(kinds))})",
            (node, *kinds),
        ).fetchall()

    def predecessors(
        self, node: str, etypes: Iterable[str], since_ms: int = 0
    ) -> list[tuple[str, str, int]]:
        kinds = tuple(etypes)
        return self.db.execute(
            "SELECT src,etype,at_ms FROM containment_edges WHERE dst=? AND at_ms>=? "
            f"AND etype IN ({','.join('?' * len(kinds))})",
            (node, since_ms, *kinds),
        ).fetchall()

    def _row(self, sql: str, params: tuple) -> dict | None:
        cursor = self.db.execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            return None
        return {column[0]: value for column, value in zip(cursor.description, row)}

    def invocation(self, attempt: str) -> dict | None:
        return self._row(
            "SELECT * FROM containment_invocations WHERE attempt_id=?", (attempt,)
        )

    def authority(self, attempt: str) -> dict | None:
        """The effective authority exported on the invocation's run/started."""
        row = self.db.execute(
            "SELECT authority_json FROM containment_invocations WHERE attempt_id=?",
            (attempt,),
        ).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def task_row(self, task_id: str) -> dict | None:
        return self._row("SELECT * FROM containment_tasks WHERE task_id=?", (task_id,))

    def version_row(self, version: str) -> dict | None:
        return self._row(
            "SELECT * FROM containment_versions WHERE version_id=?", (version,)
        )

    def effect_row(self, effect_id: str) -> dict | None:
        return self._row(
            "SELECT * FROM containment_effects WHERE effect_id=?", (effect_id,)
        )

    def invocations_of_task(self, task_id: str) -> list[str]:
        return [
            row[0]
            for row in self.db.execute(
                "SELECT attempt_id FROM containment_invocations WHERE task_id=? ORDER BY started_ms",
                (task_id,),
            )
        ]

    def parent_attempt(self, task_id: str | None) -> str | None:
        if task_id is None:
            return None
        row = self.db.execute(
            "SELECT parent_attempt_id FROM containment_tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        return row[0] if row else None

    def node_of(self, agent_id: str) -> str | None:
        return self.agent_nodes.get(agent_id) or self.config.agent_nodes.get(agent_id)

    def known_nodes(self) -> set[str]:
        return set(self.agent_nodes.values()) | set(self.config.agent_nodes.values())

    def known_agents(self) -> set[str]:
        agents = set(self.agent_nodes) | set(self.config.grants) | self.granted_agents
        for targets in self.config.grants.values():
            agents |= set(targets)
        agents |= {p.recipient_id for p in self.pending.values()}
        return agents
