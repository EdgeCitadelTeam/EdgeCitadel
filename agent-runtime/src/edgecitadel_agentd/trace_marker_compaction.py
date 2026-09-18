"""Atomic coalescing of selected retention-loss evidence."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .trace_contract import TraceContractError, coverage_scope
from .trace_journal import TraceJournal


def coalesce_loss_markers(db: sqlite3.Connection, *, now_ms: int) -> int:
    """Inspect at most 64 markers; never cross actor, epoch or generation scopes.

    Only retention/quota loss markers with exactly one selected reference
    qualify. Replacements use the active writer, including for retired origins.
    Replaced pending marker positions become explicit losses in their own scope.
    Settled positions are not relabeled. Fragmented unions use multiple markers
    of at most 128 ranges; replacement never increases the marker count.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    rows = db.execute(
        "SELECT j.*,p.export_generation,p.export_seq,p.state,g.next_export_seq,writer.source_epoch AS writer_epoch "
        "FROM trace_journal j JOIN trace_sources writer ON writer.node_id=j.node_id AND writer.active=1 "
        "JOIN trace_spool p ON p.node_id=j.node_id AND p.source_epoch=j.source_epoch "
        "AND p.journal_event_id=j.event_id "
        "JOIN trace_export_generations g ON g.node_id=p.node_id AND g.source_epoch=p.source_epoch "
        "AND g.export_generation=p.export_generation "
        "WHERE p.state IN ('pending','broker_acked','core_settled') "
        "AND json_extract(j.event_json,'$.kind')='coverage' "
        "AND json_extract(j.event_json,'$.phase')='lost' "
        "AND json_extract(j.event_json,'$.attributes.reason') IN ('quota_exceeded','retention_expired') "
        "AND NOT EXISTS (SELECT 1 FROM trace_spool other WHERE other.node_id=j.node_id "
        "AND other.source_epoch=j.source_epoch AND other.journal_event_id=j.event_id "
        "AND other.export_generation<>p.export_generation) "
        "ORDER BY j.rowid LIMIT 64"
    ).fetchall()
    groups: dict[tuple[str, str, str, str | None, str, str], list[Any]] = defaultdict(
        list
    )
    for row in rows:
        _, affected_epoch, affected_generation = coverage_scope(
            json.loads(row["event_json"])
        )
        groups[
            (
                row["node_id"],
                row["source_epoch"],
                row["export_generation"],
                row["agent_id"],
                affected_epoch,
                affected_generation,
            )
        ].append(row)
    removed = 0
    for (
        node,
        epoch,
        generation,
        _actor,
        affected_epoch,
        affected_generation,
    ), candidates in groups.items():
        if len(candidates) < 2:
            continue
        writer_epoch = candidates[0]["writer_epoch"]
        events = [json.loads(row["event_json"]) for row in candidates]
        intervals = [
            (r["first"], r["last"])
            for event in events
            for r in event["attributes"]["lost_ranges"]
        ]
        origin_scope = (epoch, generation) == (affected_epoch, affected_generation)
        marker_positions = [
            (row["export_seq"], row["export_seq"])
            for row in candidates
            if row["state"] != "core_settled"
        ]
        if origin_scope:
            intervals.extend(marker_positions)
        merged: list[dict[str, int]] = []
        for first, last in sorted(intervals):
            if merged and first <= merged[-1]["last"] + 1:
                merged[-1]["last"] = max(last, merged[-1]["last"])
            else:
                merged.append({"first": first, "last": last})
        fragments = [
            merged[offset : offset + 128] for offset in range(0, len(merged), 128)
        ]
        replacement_count = len(fragments) + int(
            not origin_scope and bool(marker_positions)
        )
        if replacement_count > len(candidates):
            continue
        replacement = {
            **events[0],
            "event_id": str(uuid4()),
            "occurred_at": datetime.fromtimestamp(now_ms / 1000, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "attributes": {
                "export_generation": affected_generation,
                **(
                    {"affected_source_epoch": affected_epoch}
                    if affected_epoch != writer_epoch
                    else {}
                ),
                "through_export_seq": (
                    candidates[0]["next_export_seq"] - 1
                    if origin_scope
                    else max(e["attributes"]["through_export_seq"] for e in events)
                ),
                "reason": "quota_exceeded"
                if any(e["attributes"]["reason"] == "quota_exceeded" for e in events)
                else "retention_expired",
                "lost_ranges": merged,
            },
        }
        # Atomic replacement may reuse the old markers' quota capacity. Other
        # readers cannot observe deletions without the committed replacement.
        db.execute("SAVEPOINT trace_marker_coalescing")
        before = db.execute("SELECT event_bytes FROM trace_storage_usage").fetchone()[0]
        try:
            for row in candidates:
                key = (node, row["source_epoch"], row["event_id"])
                db.execute(
                    "DELETE FROM trace_spool WHERE node_id=? AND source_epoch=? AND journal_event_id=? AND state<>'core_settled'",
                    key,
                )
                db.execute(
                    "UPDATE trace_spool SET journal_event_id=NULL WHERE node_id=? AND source_epoch=? AND journal_event_id=? AND state='core_settled'",
                    key,
                )
                db.execute(
                    "DELETE FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                    key,
                )
            journal = TraceJournal(db)
            for fragment in fragments:
                journal.record(
                    node,
                    {
                        **replacement,
                        "event_id": str(uuid4()),
                        "attributes": {
                            **replacement["attributes"],
                            "lost_ranges": fragment,
                        },
                    },
                    selected=True,
                )
            if not origin_scope and marker_positions:
                # Marker positions belong to their original stream, which
                # may differ from both affected scope and current writer.
                current_ranges: list[dict[str, int]] = []
                for first, last in sorted(marker_positions):
                    if current_ranges and first == current_ranges[-1]["last"] + 1:
                        current_ranges[-1]["last"] = last
                    else:
                        current_ranges.append({"first": first, "last": last})
                journal.record(
                    node,
                    {
                        **replacement,
                        "event_id": str(uuid4()),
                        "attributes": {
                            "export_generation": generation,
                            **(
                                {"affected_source_epoch": epoch}
                                if epoch != writer_epoch
                                else {}
                            ),
                            "through_export_seq": candidates[0]["next_export_seq"] - 1,
                            "reason": replacement["attributes"]["reason"],
                            "lost_ranges": current_ranges,
                        },
                    },
                    selected=True,
                )
            after = db.execute(
                "SELECT event_bytes FROM trace_storage_usage"
            ).fetchone()[0]
            if after >= before:
                db.execute("ROLLBACK TO trace_marker_coalescing")
            else:
                removed += len(candidates) - replacement_count
            db.execute("RELEASE trace_marker_coalescing")
        except Exception:
            db.execute("ROLLBACK TO trace_marker_coalescing")
            db.execute("RELEASE trace_marker_coalescing")
            raise
    return removed
