"""Binding and operation terminal state carried by their completion records."""

from __future__ import annotations

import sqlite3
from typing import Any

from .trace_completed import set_completed_metadata
from .trace_contract import TraceContractError
from .trace_reservations import Obligation


def install_views(db: sqlite3.Connection) -> None:
    # Ownership is assigned before completion. These lookups use the immutable
    # partial owner index, not a new index populated while spending the reserve.
    for table, alias, kind, key, fields in (
        (
            "trace_bindings",
            "b",
            "run",
            "binding_id",
            {"closed_at_ms": "binding.closed_at_ms"},
        ),
        (
            "trace_operations",
            "o",
            "operation",
            "span_id",
            {"terminal_event_id": "event.event_id", "phase": "event.phase"},
        ),
    ):
        columns = [row[1] for row in db.execute(f"PRAGMA table_info({table})")]
        expressions = []
        for name in columns:
            if name not in fields:
                expressions.append(f"{alias}.{name}")
                continue
            metadata = "binding" if kind == "run" else "operation"
            expressions.append(
                f"COALESCE((SELECT json_extract(CAST(record AS TEXT),'$.{fields[name]}') "
                f"FROM trace_completion_slots WHERE owner_kind='{kind}' AND owner_id={alias}.{key} "
                "AND owner_id<>'' AND purpose='terminal' AND filled=1 "
                f"AND json_extract(CAST(record AS TEXT),'$.{metadata}') IS NOT NULL),{alias}.{name}) AS {name}"
            )
        db.execute(
            f"CREATE VIEW IF NOT EXISTS {table}_all AS SELECT {','.join(expressions)} FROM {table} {alias}"
        )


def close_binding(db: sqlite3.Connection, binding_id: str, now_ms: int) -> None:
    set_completed_metadata(
        db,
        Obligation("run", binding_id, "terminal"),
        "binding",
        {"binding_id": binding_id, "closed_at_ms": now_ms},
    )


def close_operation(db: sqlite3.Connection, span_id: str, binding_id: str) -> None:
    set_completed_metadata(
        db,
        Obligation("operation", span_id, "terminal"),
        "operation",
        {"span_id": span_id, "binding_id": binding_id},
    )


def materialize(db: sqlite3.Connection, record: dict[str, Any]) -> None:
    """Called before the owner slot is freed, in the ordinary move transaction."""
    if "binding" in record:
        binding = record["binding"]
        changed = db.execute(
            "UPDATE trace_bindings SET closed_at_ms=? WHERE binding_id=?",
            (binding["closed_at_ms"], binding["binding_id"]),
        ).rowcount
        if changed != 1:
            raise TraceContractError("completion_binding_missing")
    if "operation" in record:
        operation, event = record["operation"], record["event"]
        changed = db.execute(
            "UPDATE trace_operations SET terminal_event_id=?,phase=? WHERE span_id=? AND binding_id=?",
            (
                event["event_id"],
                event["phase"],
                operation["span_id"],
                operation["binding_id"],
            ),
        ).rowcount
        if changed != 1:
            raise TraceContractError("completion_operation_missing")
