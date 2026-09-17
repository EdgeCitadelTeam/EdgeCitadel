"""Read committed Core telemetry evidence without migrations or network access."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Any
from uuid import uuid4

from .trace_payload_read import PayloadReadError, decode_payload, read_payload_parts

VIEWS = {
    "raw": "trace_raw_events",
    "positions": "trace_ingest_positions",
    "conflicts": "trace_ingest_conflicts",
    "rejected": "trace_rejected_positions",
}
MAX_STEPS = 100_000
MAX_SECONDS = 0.05


class InspectionError(ValueError):
    """Fixed operator diagnostic without database paths or exception text."""


def _budget(connection: sqlite3.Connection) -> None:
    started = time.monotonic()
    steps = 0

    def expired() -> bool:
        nonlocal steps
        steps += 1000
        return steps >= MAX_STEPS or time.monotonic() - started >= MAX_SECONDS

    connection.set_progress_handler(expired, 1000)


def inspect_core(
    path: Path,
    *,
    view: str = "raw",
    after: int = 0,
    collector_epoch: str | None = None,
    limit: int = 32,
) -> dict[str, Any]:
    """One read-only snapshot; an ingest cursor never asserts source completeness."""
    if (
        view not in VIEWS
        or type(after) is not int
        or not 0 <= after <= 2**53 - 1
        or type(limit) is not int
        or not 1 <= limit <= 32
        or (after > 0 and collector_epoch is None)
        or (
            collector_epoch is not None
            and (
                not isinstance(collector_epoch, str)
                or not 1 <= len(collector_epoch) <= 128
            )
        )
    ):
        raise InspectionError("invalid_inspection_request")
    try:
        with closing(
            sqlite3.connect(
                path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.05
            )
        ) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            _budget(db)
            db.execute("BEGIN")
            collector = db.execute(
                "SELECT collector_epoch,ingest_seq FROM trace_collector WHERE singleton=1"
            ).fetchone()
            if collector is None:
                raise InspectionError("collector_unavailable")
            if (
                collector_epoch is not None
                and collector_epoch != collector["collector_epoch"]
            ):
                raise InspectionError("collector_epoch_changed")
            if after > collector["ingest_seq"]:
                raise InspectionError("cursor_ahead_of_collector")
            rows = db.execute(
                f"SELECT * FROM {VIEWS[view]} WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
                (after, limit + 1),
            ).fetchall()
            records = []
            payloads = []
            for row in rows[:limit]:
                record = dict(row)
                if view == "raw":
                    record.pop("event_json")
                    resolved = read_payload_parts(db, record["ingest_seq"])
                    if resolved is None:
                        raise InspectionError("core_payload_unavailable")
                    payloads.append(resolved)
                records.append(record)
            # These tables contain six fixed accounting keys and three poison
            # reasons. Bounds remain explicit even for an unexpected database.
            usage = [
                dict(row)
                for row in db.execute(
                    "SELECT table_name,rows,payload_bytes FROM trace_capacity_usage ORDER BY table_name LIMIT 7"
                )
            ]
            poison = [
                dict(row)
                for row in db.execute(
                    "SELECT reason,observations,last_received_at_ms FROM trace_poison_counts ORDER BY reason LIMIT 4"
                )
            ]
            if len(usage) != 6 or len(poison) > 3:
                raise InspectionError("core_accounting_unavailable")
            # Older complete accounting has no migration table. Never migrate a
            # read-only inspection, but reject partial counters in newer stores.
            if db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='trace_capacity_backfill'"
            ).fetchone():
                backfill = db.execute(
                    "SELECT table_name,complete FROM trace_capacity_backfill LIMIT 7"
                ).fetchall()
                if (
                    len(backfill) != 6
                    or {row["table_name"] for row in backfill}
                    != {row["table_name"] for row in usage}
                    or any(row["complete"] != 1 for row in backfill)
                ):
                    raise InspectionError("core_accounting_unavailable")
        # Finish the bounded page and summaries in one snapshot, then close it
        # before JSON decoding can delay a shared-writer checkpoint.
        for record, parts in zip(records, payloads):
            record.update(decode_payload(parts))
        return {
            "kind": "core_trace_inspection",
            "view": view,
            "collector": dict(collector),
            "records": records,
            "next_ingest_seq": rows[limit - 1]["ingest_seq"]
            if len(rows) > limit
            else None,
            "coverage": "partial_committed_evidence",
            "usage": usage,
            "poison": poison,
        }
    except PayloadReadError:
        raise InspectionError("core_payload_unavailable") from None
    except (sqlite3.Error, OSError, json.JSONDecodeError):
        raise InspectionError("inspection_unavailable") from None


def inspect_source_progress(
    path: Path,
    *,
    scope: tuple[str, str, str],
    after_export_seq: int = 0,
    collector_epoch: str | None = None,
) -> dict[str, Any]:
    """Read one exact committed interval; never infer evidence before its base."""
    try:
        from edgecitadel_agentd.trace_contract import TraceContractError
        from edgecitadel_agentd.trace_settlement_pages import validate_page_request

        from .trace_settlement import settlement_page_reply
    except ImportError:
        raise InspectionError("progress_runtime_unavailable") from None
    if not isinstance(scope, (tuple, list)) or len(scope) != 3:
        raise InspectionError("invalid_inspection_request")
    request = {
        "schema_version": 2,
        "request_id": str(uuid4()),
        "node_id": scope[0],
        "source_epoch": scope[1],
        "export_generation": scope[2],
        "after_export_seq": after_export_seq,
        "collector_epoch": collector_epoch,
    }
    try:
        validate_page_request(request)
    except (TraceContractError, TypeError, ValueError):
        raise InspectionError("invalid_inspection_request") from None
    try:
        with closing(
            sqlite3.connect(
                path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.05
            )
        ) as db:
            db.execute("PRAGMA query_only=ON")
            _budget(db)
            reply = settlement_page_reply(db, request)
            if reply["status"] != "ok":
                raise InspectionError(
                    {
                        "unknown_source": "unknown_source",
                        "collector_changed": "collector_epoch_changed",
                    }.get(reply["code"], "inspection_unavailable")
                )
            return {
                "kind": "core_source_progress",
                "scope": list(scope),
                "coverage": "committed_interval_only",
                "page": reply["page"],
            }
    except (sqlite3.Error, OSError, TraceContractError, TypeError):
        raise InspectionError("inspection_unavailable") from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("--view", choices=VIEWS)
    parser.add_argument("--scope", nargs=3, metavar=("NODE", "EPOCH", "GENERATION"))
    parser.add_argument("--after-export-seq", type=int)
    parser.add_argument("--after", type=int, default=0)
    parser.add_argument("--collector-epoch")
    parser.add_argument("--limit", type=int, default=32)
    args = parser.parse_args()
    try:
        if args.scope is not None:
            if args.view is not None or args.after != 0 or args.limit != 32:
                raise InspectionError("invalid_inspection_request")
            result = inspect_source_progress(
                args.database,
                scope=tuple(args.scope),
                after_export_seq=args.after_export_seq
                if args.after_export_seq is not None
                else 0,
                collector_epoch=args.collector_epoch,
            )
        else:
            if args.after_export_seq is not None:
                raise InspectionError("invalid_inspection_request")
            result = inspect_core(
                args.database,
                view=args.view or "raw",
                after=args.after,
                collector_epoch=args.collector_epoch,
                limit=args.limit,
            )
    except InspectionError as error:
        print(json.dumps({"error": str(error)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
