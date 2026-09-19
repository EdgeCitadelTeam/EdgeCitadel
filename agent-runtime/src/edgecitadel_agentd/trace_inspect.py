"""Read-only, bounded operator inspection of an existing source telemetry store."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any

from .storage_sqlite import configure_scratch

MAX_STEPS = 100_000
MAX_SECONDS = 0.05
_SCOPE = "node_id=? AND source_epoch=? AND export_generation=?"


class InspectionError(ValueError):
    """Fixed diagnostic code; never include SQLite paths or payload text."""


def _budget(db: sqlite3.Connection) -> None:
    started = time.monotonic()
    steps = 0

    def expired() -> bool:
        nonlocal steps
        steps += 1000
        return steps >= MAX_STEPS or time.monotonic() - started >= MAX_SECONDS

    db.set_progress_handler(expired, 1000)


def inspect_source(
    path: Path,
    *,
    scope: tuple[str, str, str] | None = None,
    after_scope: tuple[str, str, str] | None = None,
    after: int = 0,
    limit: int = 32,
) -> dict[str, Any]:
    """Read one snapshot; scope statistics may be unavailable within the budget.

    This is a host-operator surface protected by filesystem permissions, not a
    connector RPC. It opens no network connection and performs no schema migration.
    Cursors are scoped export positions, not a count or completeness assertion.
    """
    if (
        type(limit) is not int
        or not 1 <= limit <= 32
        or type(after) is not int
        or not 0 <= after <= 2**53 - 1
        or (scope is None and after != 0)
        or (scope is not None and after_scope is not None)
        or any(
            len(value) != 3
            or any(
                not isinstance(item, str) or not 1 <= len(item) <= 128 for item in value
            )
            for value in (scope, after_scope)
            if value is not None
        )
    ):
        raise InspectionError("invalid_inspection_request")
    try:
        with closing(
            sqlite3.connect(
                path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.05
            )
        ) as db:
            configure_scratch(db)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            _budget(db)
            db.execute("BEGIN")
            if scope is None:
                rows = db.execute(
                    "SELECT node_id,source_epoch,export_generation,active,sync_fault,next_export_seq-1 AS assigned_through "
                    "FROM trace_export_generations WHERE (node_id,source_epoch,export_generation)>(?,?,?) "
                    "ORDER BY node_id,source_epoch,export_generation LIMIT ?",
                    (*(after_scope or ("", "", "")), limit + 1),
                ).fetchall()
                return {
                    "kind": "source_trace_scopes",
                    "scopes": [dict(row) for row in rows[:limit]],
                    "next_scope": list(rows[limit - 1])[:3]
                    if len(rows) > limit
                    else None,
                }
            generation = db.execute(
                f"SELECT active,sync_fault,next_export_seq-1 AS assigned_through FROM trace_export_generations WHERE {_SCOPE}",
                scope,
            ).fetchone()
            if generation is None:
                raise InspectionError("unknown_export_generation")
            settlement = db.execute(
                f"SELECT collector_epoch,applied_through FROM trace_source_settlements WHERE {_SCOPE}",
                scope,
            ).fetchone()
            recovery = db.execute(
                f"SELECT phase,scanned_through,assigned_through FROM trace_collector_recovery WHERE {_SCOPE}",
                scope,
            ).fetchone()
            rows = db.execute(
                "SELECT s.export_seq,s.event_id,s.event_sha256,s.state,s.core_outcome,j.event_json "
                "FROM trace_spool s LEFT JOIN trace_journal j ON "
                "j.node_id=s.node_id AND j.source_epoch=s.source_epoch AND j.event_id=s.journal_event_id "
                "WHERE s.node_id=? AND s.source_epoch=? AND s.export_generation=? AND s.export_seq>? "
                "ORDER BY s.export_seq LIMIT ?",
                (*scope, after, limit + 1),
            ).fetchall()
            records: list[dict[str, Any]] = []
            result: dict[str, Any] = {
                "kind": "source_trace_inspection",
                "scope": list(scope),
                "generation": dict(generation),
                "settlement": dict(settlement) if settlement else None,
                "recovery": dict(recovery) if recovery else None,
                "records": records,
                "next_export_seq": rows[limit - 1]["export_seq"]
                if len(rows) > limit
                else None,
                "coverage": "partial_local_evidence",
            }
            # Statistics scan the scope, not just the page. Never return a partial
            # aggregate as if exact, and never make a large scan block inspection.
            _budget(db)
            try:
                stats = db.execute(
                    "SELECT s.state,count(*) AS positions,coalesce(sum(j.event_bytes),0) AS retained_payload_bytes,"
                    "min(j.received_at_ms) AS oldest_received_at_ms "
                    "FROM trace_spool s LEFT JOIN trace_journal j ON "
                    "j.node_id=s.node_id AND j.source_epoch=s.source_epoch AND j.event_id=s.journal_event_id "
                    "WHERE s.node_id=? AND s.source_epoch=? AND s.export_generation=? GROUP BY s.state",
                    scope,
                ).fetchall()
                now = int(time.time() * 1000)
                result["summary"] = {
                    "state": "available",
                    "sampled_at_ms": now,
                    "by_state": {
                        row["state"]: {
                            "positions": row["positions"],
                            "retained_payload_bytes": row["retained_payload_bytes"],
                            "oldest_retained_age_ms": now - row["oldest_received_at_ms"]
                            if row["oldest_received_at_ms"] is not None
                            and row["oldest_received_at_ms"] <= now
                            else None,
                            "oldest_retained_age_state": (
                                "unavailable"
                                if row["oldest_received_at_ms"] is None
                                else "clock_skew"
                                if row["oldest_received_at_ms"] > now
                                else "observed"
                            ),
                        }
                        for row in stats
                    },
                }
            except sqlite3.OperationalError:
                result["summary"] = {
                    "state": "unavailable",
                    "fault": "inspection_query_budget_or_busy",
                }
            # All snapshot rows are materialized. Release its read lock before
            # parsing payloads; Python work is outside the SQL progress budget.
            db.set_progress_handler(None, 0)
            db.rollback()
            for row in rows[:limit]:
                record = dict(row)
                payload = record.pop("event_json")
                record["event"] = json.loads(payload) if payload is not None else None
                records.append(record)
            return result
    except (sqlite3.Error, OSError, json.JSONDecodeError):
        raise InspectionError("inspection_unavailable") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--scope", nargs=3, metavar=("NODE", "EPOCH", "GENERATION"))
    parser.add_argument(
        "--after-scope", nargs=3, metavar=("NODE", "EPOCH", "GENERATION")
    )
    parser.add_argument("--after", type=int, default=0)
    parser.add_argument("--limit", type=int, default=32)
    args = parser.parse_args()
    try:
        result = inspect_source(
            args.database,
            scope=tuple(args.scope) if args.scope else None,
            after_scope=tuple(args.after_scope) if args.after_scope else None,
            after=args.after,
            limit=args.limit,
        )
    except InspectionError as error:
        print(json.dumps({"error": str(error)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
