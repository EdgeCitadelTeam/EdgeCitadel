"""One bounded cycle for a future owned projector service task.

This module starts no thread/task and is not enabled by Aggregator startup.
Cleanup precedes projection so new observations never use half-retired reducer
state. Each stage owns its own short transaction; raw ingestion stays separate.
"""

from __future__ import annotations

import sqlite3

from . import trace_projection_history as history
from . import trace_projection_retention as retention
from . import trace_projection_store as projection


def run_cycle(db: sqlite3.Connection, *, now_ms: int) -> dict:
    projection._idle(db)
    if type(now_ms) is not int or not 0 <= now_ms <= 2**53 - 1:
        raise ValueError("invalid_retention_time")
    cleanup = retention.cleanup_batch(db, now_ms=now_ms)
    if cleanup["deleted_rows"]:
        return {"phase": "cleanup", **cleanup}
    try:
        state = projection.project_batch(db)
    except ValueError as error:
        # Another maintenance writer may retire a trace between stages. The next
        # scheduled cycle can clean it; all other faults must remain visible.
        if str(error) != "projection_retirement_pending":
            raise
        return {"phase": "cleanup_pending"}
    retirement = retention.expire_one(db, now_ms=now_ms)
    compacted = history.expire_history_batch(db, now_ms=now_ms)
    return {
        "phase": "projection",
        "projected_ingest_cursor": state.ingest_cursor,
        "retirement": retirement,
        "history": compacted,
    }
