"""Durable graph projection, separate from ingestion and execution authority.

The v2 graph projector is not enabled by Aggregator startup. Coverage, retention
and generation rebuild must be completed before rollout.
Projection/checkpoint changes use one Core SQLite transaction; ingestion never
waits for a projector acknowledgment and is not undone by projection failure.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from edgecitadel_agentd.trace_contract import event_sha256

from .trace_payload_read import read_payload
from .trace_graph_projection import entity_claims, relationship_claims, resolve_graph
from .trace_task_projection import (
    TERMINAL_STATES,
    TaskObservation,
    reduce_task,
    task_node,
)

VERSION = 2
MAX_BATCH = 64

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS trace_projection_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        version INTEGER NOT NULL,
        generation TEXT NOT NULL,
        collector_epoch TEXT NOT NULL,
        ingest_cursor INTEGER NOT NULL DEFAULT 0,
        change_cursor INTEGER NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS trace_projected_tasks (
        trace_id TEXT NOT NULL, task_id TEXT NOT NULL,
        node_json TEXT NOT NULL, ambiguous_live_state INTEGER NOT NULL,
        PRIMARY KEY(trace_id,task_id)
    )""",
    """CREATE TABLE IF NOT EXISTS trace_task_perspectives (
        trace_id TEXT NOT NULL, task_id TEXT NOT NULL,
        node_id TEXT NOT NULL, source_epoch TEXT NOT NULL,
        agent_key TEXT NOT NULL, source_role TEXT NOT NULL,
        source_seq INTEGER NOT NULL, ingest_seq INTEGER NOT NULL,
        phase TEXT NOT NULL, evidence_json TEXT NOT NULL,
        PRIMARY KEY(trace_id,task_id,node_id,source_epoch,agent_key,source_role)
    )""",
    """CREATE TABLE IF NOT EXISTS trace_task_outcomes (
        ingest_seq INTEGER PRIMARY KEY,
        trace_id TEXT NOT NULL, task_id TEXT NOT NULL,
        source_role TEXT NOT NULL, phase TEXT NOT NULL,
        evidence_json TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS trace_task_outcome_scope
        ON trace_task_outcomes(trace_id,task_id,source_role,ingest_seq)""",
    """CREATE TABLE IF NOT EXISTS trace_projection_changes (
        cursor INTEGER PRIMARY KEY,
        ingest_seq INTEGER NOT NULL UNIQUE,
        trace_id TEXT,
        kind TEXT NOT NULL CHECK(kind IN ('task_upsert','entity_upsert','relationships','payload_expired')),
        change_json TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS trace_projection_trace_changes
        ON trace_projection_changes(trace_id,cursor)""",
    """CREATE TABLE IF NOT EXISTS trace_entity_observations (
        trace_id TEXT NOT NULL,entity_id TEXT NOT NULL,ingest_seq INTEGER NOT NULL,
        node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,agent_key TEXT NOT NULL,
        source_seq INTEGER NOT NULL,terminal INTEGER NOT NULL,phase TEXT NOT NULL,
        node_json TEXT NOT NULL,evidence_json TEXT NOT NULL,
        PRIMARY KEY(trace_id,entity_id,ingest_seq)
    )""",
    """CREATE TABLE IF NOT EXISTS trace_projected_entities (
        trace_id TEXT NOT NULL,entity_id TEXT NOT NULL,node_json TEXT NOT NULL,
        ambiguous_live_state INTEGER NOT NULL,identity_conflict INTEGER NOT NULL,
        PRIMARY KEY(trace_id,entity_id)
    )""",
    """CREATE TABLE IF NOT EXISTS trace_relationship_claims (
        trace_id TEXT NOT NULL,edge_id TEXT NOT NULL,kind TEXT NOT NULL,
        parent_id TEXT NOT NULL,child_id TEXT NOT NULL,first_ingest_seq INTEGER NOT NULL,
        PRIMARY KEY(trace_id,edge_id)
    )""",
)


@dataclass(frozen=True)
class ProjectionState:
    generation: str
    collector_epoch: str
    ingest_cursor: int
    change_cursor: int


def _idle(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("projection_requires_idle_connection")


def _state(db: sqlite3.Connection) -> ProjectionState:
    row = db.execute(
        "SELECT version,generation,collector_epoch,ingest_cursor,change_cursor "
        "FROM trace_projection_state WHERE singleton=1"
    ).fetchone()
    if row is None or row[0] != VERSION:
        raise ValueError("projection_version_unavailable")
    collector = db.execute(
        "SELECT collector_epoch,ingest_seq FROM trace_collector WHERE singleton=1"
    ).fetchone()
    if collector is None or row[2] != collector[0] or row[3] > collector[1]:
        raise ValueError("projection_rebuild_required")
    return ProjectionState(*row[1:])


def initialize(db: sqlite3.Connection) -> ProjectionState:
    """Create derived tables atomically in an already initialized Core store."""
    _idle(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        epoch = db.execute(
            "SELECT collector_epoch FROM trace_collector WHERE singleton=1"
        ).fetchone()
        if epoch is None:
            raise ValueError("projection_collector_unavailable")
        for statement in SCHEMA:
            db.execute(statement)
        db.execute(
            "INSERT OR IGNORE INTO trace_projection_state"
            "(singleton,version,generation,collector_epoch) VALUES(1,?,?,?)",
            (VERSION, str(uuid4()), epoch[0]),
        )
        return _state(db)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _project_task(db: sqlite3.Connection, seq: int, event: dict) -> dict:
    projected = reduce_task([TaskObservation(seq, event)])
    evidence = projected.perspectives[0]
    scope = (event["trace_id"], event["task_id"])
    encoded = _json(evidence)
    db.execute(
        "INSERT INTO trace_task_perspectives VALUES(?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(trace_id,task_id,node_id,source_epoch,agent_key,source_role) "
        "DO UPDATE SET source_seq=excluded.source_seq,ingest_seq=excluded.ingest_seq,"
        "phase=excluded.phase,evidence_json=excluded.evidence_json "
        "WHERE excluded.source_seq>trace_task_perspectives.source_seq",
        (
            *scope,
            event["node_id"],
            event["source_epoch"],
            event["agent_id"] or "",
            evidence["source_role"],
            event["source_seq"],
            seq,
            event["phase"],
            encoded,
        ),
    )
    if event["phase"] in TERMINAL_STATES:
        db.execute(
            "INSERT INTO trace_task_outcomes VALUES(?,?,?,?,?,?)",
            (seq, *scope, evidence["source_role"], event["phase"], encoded),
        )
    count, phases = db.execute(
        "SELECT COUNT(*),COUNT(DISTINCT phase) FROM trace_task_outcomes "
        "WHERE trace_id=? AND task_id=?",
        scope,
    ).fetchone()
    table = "trace_task_outcomes" if count else "trace_task_perspectives"
    # Select only one evidence payload, even for a task with many terminal
    # observations. Candidate inspection is separately paged; change records
    # never embed a growing copy of the entire candidate history.
    first = db.execute(
        f"SELECT evidence_json,source_role FROM {table} WHERE trace_id=? AND task_id=? "
        "ORDER BY source_role='recipient' DESC,ingest_seq LIMIT 1",
        scope,
    ).fetchone()
    ambiguous = False
    if not count:
        live_phases = db.execute(
            "SELECT COUNT(DISTINCT phase) FROM trace_task_perspectives "
            "WHERE trace_id=? AND task_id=? AND (?<>'recipient' OR source_role='recipient')",
            (*scope, first[1]),
        ).fetchone()[0]
        ambiguous = live_phases > 1
    node = task_node(
        event["task_id"],
        None if ambiguous else json.loads(first[0]),
        outcome_count=count,
        conflict=phases > 1,
    )
    db.execute(
        "INSERT INTO trace_projected_tasks VALUES(?,?,?,?) "
        "ON CONFLICT(trace_id,task_id) DO UPDATE SET "
        "node_json=excluded.node_json,ambiguous_live_state=excluded.ambiguous_live_state",
        (*scope, _json(node), int(ambiguous)),
    )
    return {"node": node, "ambiguous_live_state": ambiguous, "observation": evidence}


def _project_entities(
    db: sqlite3.Connection, seq: int, event: dict, claims: list[dict]
) -> dict:
    evidence = {
        key: event[key]
        for key in (
            "node_id",
            "source_epoch",
            "event_id",
            "source_seq",
            "agent_id",
            "task_id",
            "execution_attempt_id",
            "span_id",
            "parent_span_id",
            "parent_task_id",
            "parent_run_id",
            "kind",
            "phase",
            "evidence_kind",
            "supersedes_event_id",
            "occurred_at",
            "duration_ms",
            "attributes",
        )
    }
    evidence["ingest_seq"] = seq
    updates = []
    for claim in claims:
        scope = (event["trace_id"], claim["id"])
        db.execute(
            "INSERT INTO trace_entity_observations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                *scope,
                seq,
                event["node_id"],
                event["source_epoch"],
                event["agent_id"] or "",
                event["source_seq"],
                int(claim["terminal"]),
                event["phase"],
                _json(claim["node"]),
                _json(evidence),
            ),
        )
        count, phases, kinds = db.execute(
            "SELECT SUM(terminal),COUNT(DISTINCT CASE WHEN terminal=1 THEN phase END),"
            "COUNT(DISTINCT json_extract(node_json,'$.kind')) "
            "FROM trace_entity_observations WHERE trace_id=? AND entity_id=?",
            scope,
        ).fetchone()
        if count:
            encoded = db.execute(
                "SELECT node_json FROM trace_entity_observations WHERE trace_id=? AND entity_id=? "
                "AND terminal=1 ORDER BY ingest_seq LIMIT 1",
                scope,
            ).fetchone()[0]
            ambiguous = False
        else:
            # Different source/epoch/agent perspectives are not comparable by
            # local source sequence. Only the latest within each is eligible.
            live = (
                "FROM trace_entity_observations e WHERE e.trace_id=? AND e.entity_id=? "
                "AND NOT EXISTS(SELECT 1 FROM trace_entity_observations n WHERE "
                "n.trace_id=e.trace_id AND n.entity_id=e.entity_id AND n.node_id=e.node_id "
                "AND n.source_epoch=e.source_epoch AND n.agent_key=e.agent_key "
                "AND n.source_seq>e.source_seq)"
            )
            encoded = db.execute(
                "SELECT node_json " + live + " ORDER BY ingest_seq LIMIT 1", scope
            ).fetchone()[0]
            ambiguous = (
                db.execute("SELECT COUNT(DISTINCT phase) " + live, scope).fetchone()[0]
                > 1
            )
        node = json.loads(encoded)
        node["outcome_candidate_count"] = count
        node["conflict"] = phases > 1
        if ambiguous or kinds > 1:
            node.update(state="unknown", agent_id=None, evidence_kind=None)
        if kinds > 1:
            node["kind"] = "unresolved"
        db.execute(
            "INSERT INTO trace_projected_entities VALUES(?,?,?,?,?) "
            "ON CONFLICT(trace_id,entity_id) DO UPDATE SET node_json=excluded.node_json,"
            "ambiguous_live_state=excluded.ambiguous_live_state,identity_conflict=excluded.identity_conflict",
            (*scope, _json(node), int(ambiguous), int(kinds > 1)),
        )
        updates.append(
            {
                "node": node,
                "ambiguous_live_state": ambiguous,
                "identity_conflict": kinds > 1,
            }
        )
    return {"node_updates": updates, "observation": evidence}


def _record_relationships(
    db: sqlite3.Connection, seq: int, event: dict, claims: list[dict]
) -> list[dict]:
    edges = relationship_claims(event, claims)
    for relation in edges:
        db.execute(
            "INSERT OR IGNORE INTO trace_relationship_claims VALUES(?,?,?,?,?,?)",
            (
                event["trace_id"],
                relation["id"],
                relation["kind"],
                relation["from"],
                relation["to"],
                seq,
            ),
        )
    return edges


def project_batch(db: sqlite3.Connection, *, limit: int = MAX_BATCH) -> ProjectionState:
    """Consume at most limit raw events; checkpoint, evidence and changes are atomic.

    Source-wide coverage/security are not yet projected. An expired payload gets an
    explicit unattributed history-gap record; its task/trace is never guessed.
    Duplicate/conflict receipts do not create additional accepted observations.
    """
    _idle(db)
    if type(limit) is not int or not 1 <= limit <= MAX_BATCH:
        raise ValueError("invalid_projection_batch_limit")
    with db:
        db.execute("BEGIN IMMEDIATE")
        state = _state(db)
        high = db.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0]
        rows = db.execute(
            "SELECT ingest_seq,node_id,source_epoch,event_id,source_seq,event_sha256 "
            "FROM trace_raw_events WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
            (state.ingest_cursor, limit),
        ).fetchall()
        cursor = state.change_cursor
        for seq, node, epoch, event_id, source_seq, digest in rows:
            payload = read_payload(db, seq)
            if payload is None:
                raise ValueError("projection_payload_unavailable")
            event = payload["event"]
            if event is None:
                kind, trace_id = "payload_expired", None
                change = {
                    "node_id": node,
                    "source_epoch": epoch,
                    "event_id": event_id,
                    "source_seq": source_seq,
                    "payload_expired_at_ms": payload["payload_expired_at_ms"],
                }
            else:
                if event_sha256(event) != digest or (
                    event["node_id"],
                    event["source_epoch"],
                    event["event_id"],
                    event["source_seq"],
                ) != (node, epoch, event_id, source_seq):
                    raise ValueError("projection_raw_identity_mismatch")
                entities = entity_claims(event)
                if event["kind"] == "task":
                    kind = "task_upsert"
                    change = _project_task(db, seq, event)
                elif entities:
                    kind = "entity_upsert"
                    change = _project_entities(db, seq, event, entities)
                elif event["kind"] == "link":
                    kind = "relationships"
                    change = {
                        "node_id": node,
                        "source_epoch": epoch,
                        "event_id": event_id,
                        "source_seq": source_seq,
                        "evidence_kind": event["evidence_kind"],
                        "supersedes_event_id": event["supersedes_event_id"],
                    }
                else:
                    continue
                trace_id = event["trace_id"]
                change["edge_claims"] = _record_relationships(db, seq, event, entities)
            cursor += 1
            db.execute(
                "INSERT INTO trace_projection_changes VALUES(?,?,?,?,?)",
                (cursor, seq, trace_id, kind, _json(change)),
            )
        # Under this writer transaction, no ingestion can cross our snapshot.
        # Exhausting raw rows permits crossing intervening duplicate/conflict
        # positions without pretending those receipts were additional events.
        through = rows[-1][0] if len(rows) == limit else high
        if through != state.ingest_cursor or cursor != state.change_cursor:
            db.execute(
                "UPDATE trace_projection_state SET ingest_cursor=?,change_cursor=? WHERE singleton=1",
                (through, cursor),
            )
        return ProjectionState(state.generation, state.collector_epoch, through, cursor)


def read_task(db: sqlite3.Connection, *, trace_id: str, task_id: str) -> dict:
    """Materialize one node and its generation/cursors from one closed snapshot."""
    _idle(db)
    with db:
        db.execute("BEGIN")
        state = _state(db)
        row = db.execute(
            "SELECT node_json,ambiguous_live_state FROM trace_projected_tasks "
            "WHERE trace_id=? AND task_id=?",
            (trace_id, task_id),
        ).fetchone()
    return {
        "state": state,
        "node": json.loads(row[0]) if row else None,
        "ambiguous_live_state": bool(row[1]) if row else False,
    }


def read_graph(db: sqlite3.Connection, *, trace_id: str) -> dict:
    """Materialize a small internal graph; oversize needs the future expansion API.

    No partial graph is returned as complete. Status is derived from the same
    committed node/claim snapshot, so late parents and contradictions are visible
    without mutating immutable relationship evidence or task authority.
    """
    _idle(db)
    with db:
        db.execute("BEGIN")
        state = _state(db)
        task_rows = db.execute(
            "SELECT node_json FROM trace_projected_tasks WHERE trace_id=? LIMIT 501",
            (trace_id,),
        ).fetchall()
        entity_rows = db.execute(
            "SELECT node_json FROM trace_projected_entities WHERE trace_id=? LIMIT 501",
            (trace_id,),
        ).fetchall()
        relations = db.execute(
            "SELECT edge_id,kind,parent_id,child_id FROM trace_relationship_claims WHERE trace_id=? LIMIT 1001",
            (trace_id,),
        ).fetchall()
    if len(task_rows) + len(entity_rows) > 500 or len(relations) > 1000:
        raise ValueError("projection_graph_expansion_required")
    graph = resolve_graph(
        [json.loads(row[0]) for row in (*task_rows, *entity_rows)],
        [
            {"id": identity, "kind": kind, "from": parent, "to": child}
            for identity, kind, parent, child in relations
        ],
    )
    return {"state": state, **graph}


def read_changes(
    db: sqlite3.Connection, *, generation: str, after: int, limit: int = 200
) -> dict:
    """Read committed internal changes; this is not yet a public cursor API."""
    _idle(db)
    if (
        type(after) is not int
        or after < 0
        or type(limit) is not int
        or not 1 <= limit <= 500
    ):
        raise ValueError("invalid_projection_page")
    with db:
        db.execute("BEGIN")
        state = _state(db)
        if generation != state.generation:
            raise ValueError("projection_generation_mismatch")
        if after > state.change_cursor:
            raise ValueError("projection_cursor_ahead")
        rows = db.execute(
            "SELECT cursor,ingest_seq,trace_id,kind,change_json FROM trace_projection_changes "
            "WHERE cursor>? ORDER BY cursor LIMIT ?",
            (after, limit),
        ).fetchall()
    return {
        "state": state,
        "changes": [
            {
                "cursor": cursor,
                "ingest_seq": seq,
                "trace_id": trace_id,
                "kind": kind,
                "change": json.loads(body),
            }
            for cursor, seq, trace_id, kind, body in rows
        ],
    }


def read_outcomes(
    db: sqlite3.Connection,
    *,
    generation: str,
    trace_id: str,
    task_id: str,
    as_of: int,
    after: int = 0,
    limit: int = 200,
) -> dict:
    """Page task candidates at a previously observed ingestion high-watermark."""
    _idle(db)
    if (
        type(after) is not int
        or after < 0
        or type(as_of) is not int
        or as_of < after
        or type(limit) is not int
        or not 1 <= limit <= 500
    ):
        raise ValueError("invalid_projection_page")
    with db:
        db.execute("BEGIN")
        state = _state(db)
        if generation != state.generation:
            raise ValueError("projection_generation_mismatch")
        if as_of > state.ingest_cursor:
            raise ValueError("projection_cursor_ahead")
        rows = db.execute(
            "SELECT evidence_json FROM trace_task_outcomes WHERE trace_id=? AND task_id=? "
            "AND ingest_seq>? AND ingest_seq<=? ORDER BY ingest_seq LIMIT ?",
            (trace_id, task_id, after, as_of, limit),
        ).fetchall()
    return {"state": state, "outcomes": [json.loads(row[0]) for row in rows]}
