"""Legacy task event projection carried by the reserved canonical boundary."""

from __future__ import annotations

import json
import sqlite3
from uuid import UUID
from typing import Any

from .trace_completed import set_completed_metadata
from .trace_contract import TraceContractError
from .trace_reservations import Obligation, release_unused

EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "agent_id",
    "task_id",
    "trace_id",
    "attributes_json",
    "created_at_ms",
)


def install_view(db: sqlite3.Connection) -> None:
    columns = ",".join(EVENT_COLUMNS)
    projection = ",".join(
        f"json_extract(CAST(record AS TEXT),'$.legacy_event.{name}') AS {name}"
        for name in EVENT_COLUMNS
    )
    db.execute(
        f"CREATE VIEW IF NOT EXISTS events_all AS SELECT {columns} FROM events "
        f"UNION ALL SELECT {projection} FROM trace_completion_slots WHERE filled=1 "
        "AND json_extract(CAST(record AS TEXT),'$.legacy_event') IS NOT NULL"
    )


def attach_event(
    db: sqlite3.Connection, obligation: Obligation, value: dict[str, Any]
) -> None:
    if set(value) != set(EVENT_COLUMNS):
        raise TraceContractError("invalid_completion_legacy_event")
    if value["task_id"] != obligation.owner_id or obligation.kind not in {
        "task",
        "attempt",
    }:
        raise TraceContractError("invalid_completion_legacy_event")
    attributes = json.loads(value["attributes_json"])
    if not isinstance(attributes, dict):
        raise TraceContractError("invalid_completion_legacy_event")
    set_completed_metadata(db, obligation, "legacy_event", value)


def materialize(db: sqlite3.Connection, record: dict[str, Any]) -> None:
    if "legacy_event" in record:
        db.execute(
            f"INSERT INTO events({','.join(EVENT_COLUMNS)}) VALUES ({','.join('?' for _ in EVENT_COLUMNS)})",
            tuple(record["legacy_event"][key] for key in EVENT_COLUMNS),
        )
        if record["event"]["phase"] in {
            "completed",
            "failed",
            "rejected",
            "cancelled",
            "expired",
            "undeliverable",
        }:
            # This ordinary transaction can reclaim unused first-cycle capacity.
            # Filled records remain authoritative until separately materialized.
            for purpose in ("offered", "accepted", "running"):
                obligation = Obligation("task", record["event"]["task_id"], purpose)
                if db.execute(
                    "SELECT 1 FROM trace_completion_slots WHERE owner_kind=? AND owner_id=? AND purpose=? AND filled=0",
                    obligation.key,
                ).fetchone():
                    release_unused(db, obligation)
            for row in db.execute(
                "SELECT purpose FROM trace_completion_slots WHERE owner_kind='attempt' AND owner_id=? AND filled=0",
                (record["event"]["task_id"],),
            ).fetchall():
                release_unused(
                    db, Obligation("attempt", record["event"]["task_id"], row[0])
                )


def execution_obligation(
    db: sqlite3.Connection, task_id: str, session_id: str | None
) -> Obligation | None:
    """One reserved next boundary: execution starts, or acceptance is requeued.

    The initial task admission already owns this capacity. Later local attempts
    use their session identity, so an earlier filled record cannot be overwritten.
    """
    first = Obligation("task", task_id, "running")
    if db.execute(
        "SELECT 1 FROM trace_completion_slots WHERE owner_kind=? AND owner_id=? AND purpose=? AND filled=0",
        first.key,
    ).fetchone():
        return first
    if session_id is not None:
        return Obligation("attempt", task_id, UUID(session_id).hex)
    return None
