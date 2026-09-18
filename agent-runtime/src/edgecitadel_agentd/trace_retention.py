"""Bounded journal reclamation with durable export-loss evidence."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from . import trace_capacity
from .trace_contract import TraceContractError
from .trace_journal import TraceJournal


def maintain_capacity(
    db: sqlite3.Connection, *, now_ms: int, expire_before_ms: int | None = None
) -> int:
    """One bounded reclamation batch; failed markers never discard payloads."""
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    used = db.execute("SELECT event_bytes FROM trace_storage_usage").fetchone()[0]
    pressure = used >= trace_capacity.NORMAL_LIMIT_BYTES * 9 // 10
    if not pressure and expire_before_ms is None:
        return 0
    db.execute("SAVEPOINT trace_reclamation")
    removed = 0
    try:
        for row in db.execute(
            "SELECT node_id FROM trace_sources WHERE active=1 ORDER BY node_id LIMIT 8"
        ).fetchall():
            removed += prune_retired_history(
                db,
                node_id=row[0],
                now_ms=now_ms,
                limit=64 - removed,
                before_ms=None if pressure else expire_before_ms,
            )
            if removed == 64:
                break
            removed += prune_active_history(
                db,
                node_id=row[0],
                now_ms=now_ms,
                limit=64 - removed,
                before_ms=None if pressure else expire_before_ms,
            )
            if removed == 64:
                break
        remaining_bytes = db.execute(
            "SELECT event_bytes FROM trace_storage_usage"
        ).fetchone()[0]
        if pressure and remaining_bytes >= used:
            db.execute("ROLLBACK TO trace_reclamation")
            db.execute("RELEASE trace_reclamation")
            return 0
        db.execute("RELEASE trace_reclamation")
    except Exception:
        db.execute("ROLLBACK TO trace_reclamation")
        db.execute("RELEASE trace_reclamation")
        raise
    return removed


def _ranges(positions: list[int]) -> list[dict[str, int]]:
    result: list[dict[str, int]] = []
    for position in sorted(set(positions)):
        if result and result[-1]["last"] + 1 == position:
            result[-1]["last"] = position
        else:
            result.append({"first": position, "last": position})
    return result


def prune_active_history(
    db: sqlite3.Connection,
    *,
    node_id: str,
    now_ms: int,
    limit: int = 64,
    before_ms: int | None = None,
) -> int:
    """Caller owns the write transaction. No old generation is silently retired."""
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if type(limit) is not int or not 1 <= limit <= 64:
        raise TraceContractError("invalid_retention_limit")
    active = db.execute(
        "SELECT s.source_epoch,g.export_generation,g.next_export_seq-1 AS through_seq "
        "FROM trace_sources s JOIN trace_export_generations g USING(node_id,source_epoch) "
        "WHERE s.node_id=? AND s.active=1 AND g.active=1",
        (node_id,),
    ).fetchone()
    if active is None:
        return 0
    epoch, generation = active["source_epoch"], active["export_generation"]
    age_scope = " AND j.received_at_ms<?" if before_ms is not None else ""
    order = "j.received_at_ms,j.source_seq" if before_ms is not None else "j.source_seq"
    values: list[Any] = [node_id, epoch, generation]
    if before_ms is not None:
        values.append(before_ms)
    values.append(limit)
    rows = db.execute(
        "SELECT j.* FROM trace_journal j WHERE j.node_id=? AND j.source_epoch=? "
        "AND json_extract(j.event_json,'$.kind') NOT IN ('coverage','source','security') "
        "AND NOT EXISTS (SELECT 1 FROM trace_bindings b WHERE b.closed_at_ms IS NULL "
        "AND b.execution_attempt_id=json_extract(j.event_json,'$.execution_attempt_id') "
        "AND json_extract(j.event_json,'$.kind')='run' AND json_extract(j.event_json,'$.phase')='started') "
        "AND NOT EXISTS (SELECT 1 FROM trace_spool p WHERE p.node_id=j.node_id "
        "AND p.source_epoch=j.source_epoch AND p.journal_event_id=j.event_id "
        "AND p.export_generation<>?)" + age_scope + " ORDER BY " + order + " LIMIT ?",
        values,
    ).fetchall()
    groups: dict[str | None, list[Any]] = defaultdict(list)
    for row in rows:
        groups[row["agent_id"]].append(row)
    journal = TraceJournal(db)
    for actor, candidates in groups.items():
        positions: list[int] = []
        for row in candidates:
            positions.extend(
                r[0]
                for r in db.execute(
                    "SELECT export_seq FROM trace_spool WHERE node_id=? AND source_epoch=? "
                    "AND journal_event_id=? AND state IN ('pending','broker_acked')",
                    (node_id, epoch, row["event_id"]),
                )
            )
        original = json.loads(candidates[0]["event_json"])
        marker = _loss_marker(
            original,
            actor=actor,
            generation=generation,
            through=active["through_seq"],
            positions=positions,
            now_ms=now_ms,
            before_ms=before_ms,
        )
        # Marker/export intent must fit reserved capacity before payload removal.
        journal.record(node_id, marker, selected=True)
        for row in candidates:
            db.execute(
                "UPDATE trace_spool SET state=CASE WHEN state IN ('pending','broker_acked') "
                "THEN 'lost_with_marker' ELSE state END,journal_event_id=NULL "
                "WHERE node_id=? AND source_epoch=? AND journal_event_id=?",
                (node_id, epoch, row["event_id"]),
            )
            db.execute(
                "DELETE FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                (node_id, epoch, row["event_id"]),
            )
    return len(rows)


def _loss_marker(
    original: dict[str, Any],
    *,
    actor: str | None,
    generation: str,
    through: int,
    positions: list[int],
    now_ms: int,
    before_ms: int | None,
) -> dict[str, Any]:
    marker = {
        **original,
        "event_id": str(uuid4()),
        "kind": "coverage",
        "phase": "lost" if positions else "unknown",
        "agent_id": actor,
        "trace_id": None,
        "context_id": None,
        "task_id": None,
        "parent_task_id": None,
        "parent_run_id": None,
        "execution_attempt_id": None,
        "span_id": None,
        "parent_span_id": None,
        "occurred_at": datetime.fromtimestamp(now_ms / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "duration_ms": None,
        "evidence_kind": "source_observed",
        "causes": [],
        "supersedes_event_id": None,
        "attributes": {
            "export_generation": generation,
            "through_export_seq": through,
            "reason": "quota_exceeded" if before_ms is None else "retention_expired",
            **({"lost_ranges": _ranges(positions)} if positions else {}),
        },
    }
    return marker


def prune_retired_history(
    db: sqlite3.Connection,
    *,
    node_id: str,
    now_ms: int,
    limit: int = 64,
    before_ms: int | None = None,
) -> int:
    """Reclaim retired epochs/multi-generation rows via the current writer.

    Up to eight export generations per candidate group. Larger fanout remains
    replayable until compaction can represent it within the bounded marker work.
    Caller owns the transaction; all markers precede any payload deletion.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if type(limit) is not int or not 1 <= limit <= 64:
        raise TraceContractError("invalid_retention_limit")
    active = db.execute(
        "SELECT source_epoch FROM trace_sources WHERE node_id=? AND active=1",
        (node_id,),
    ).fetchone()
    if active is None:
        return 0
    age = " AND j.received_at_ms<?" if before_ms is not None else ""
    params: list[Any] = [node_id]
    if before_ms is not None:
        params.append(before_ms)
    params.append(limit)
    rows = db.execute(
        "SELECT j.* FROM trace_journal j JOIN trace_sources s USING(node_id,source_epoch) "
        "WHERE j.node_id=? AND (s.active=0 OR EXISTS (SELECT 1 FROM trace_spool p "
        "JOIN trace_export_generations g USING(node_id,source_epoch,export_generation) "
        "WHERE p.node_id=j.node_id AND p.source_epoch=j.source_epoch AND p.journal_event_id=j.event_id AND g.active=0)) "
        "AND json_extract(j.event_json,'$.kind') NOT IN ('coverage','source','security') "
        "AND NOT EXISTS (SELECT 1 FROM trace_bindings b WHERE b.closed_at_ms IS NULL "
        "AND b.execution_attempt_id=json_extract(j.event_json,'$.execution_attempt_id') "
        "AND json_extract(j.event_json,'$.kind')='run' AND json_extract(j.event_json,'$.phase')='started')"
        + age
        + " ORDER BY j.received_at_ms,j.source_epoch,j.source_seq LIMIT ?",
        params,
    ).fetchall()
    groups: dict[tuple[str, str | None], list[Any]] = defaultdict(list)
    for row in rows:
        groups[(row["source_epoch"], row["agent_id"])].append(row)
    removed = 0
    journal = TraceJournal(db)
    for (epoch, actor), candidates in groups.items():
        placeholders = ",".join("?" for _ in candidates)
        generations = db.execute(
            "SELECT g.export_generation,g.next_export_seq-1 FROM trace_export_generations g "
            "WHERE g.node_id=? AND g.source_epoch=? AND EXISTS (SELECT 1 FROM trace_spool p "
            "WHERE p.node_id=g.node_id AND p.source_epoch=g.source_epoch AND p.export_generation=g.export_generation "
            "AND p.journal_event_id IN ("
            + placeholders
            + ")) ORDER BY g.export_generation LIMIT 9",
            (node_id, epoch, *(row["event_id"] for row in candidates)),
        ).fetchall()
        if not generations:
            generations = db.execute(
                "SELECT export_generation,next_export_seq-1 FROM trace_export_generations "
                "WHERE node_id=? AND source_epoch=? ORDER BY active DESC,rowid DESC LIMIT 1",
                (node_id, epoch),
            ).fetchall()
        if not generations or len(generations) > 8:
            continue
        for generation, through in generations:
            positions: list[int] = []
            for row in candidates:
                positions.extend(
                    r[0]
                    for r in db.execute(
                        "SELECT export_seq FROM trace_spool WHERE node_id=? AND source_epoch=? AND export_generation=? AND journal_event_id=? AND state IN ('pending','broker_acked')",
                        (node_id, epoch, generation, row["event_id"]),
                    )
                )
            marker = _loss_marker(
                json.loads(candidates[0]["event_json"]),
                actor=actor,
                generation=generation,
                through=through,
                positions=positions,
                now_ms=now_ms,
                before_ms=before_ms,
            )
            if epoch != active[0]:
                marker["attributes"]["affected_source_epoch"] = epoch
            journal.record(node_id, marker, selected=True)
        for row in candidates:
            db.execute(
                "UPDATE trace_spool SET state=CASE WHEN state IN ('pending','broker_acked') "
                "THEN 'lost_with_marker' ELSE state END,journal_event_id=NULL "
                "WHERE node_id=? AND source_epoch=? AND journal_event_id=?",
                (node_id, epoch, row["event_id"]),
            )
            db.execute(
                "DELETE FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                (node_id, epoch, row["event_id"]),
            )
        removed += len(candidates)
    return removed
