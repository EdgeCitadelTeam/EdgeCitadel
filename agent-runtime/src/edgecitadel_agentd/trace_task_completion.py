"""Legacy task event projection carried by the reserved canonical boundary."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .trace_completed import set_completed_metadata
from .trace_contract import TraceContractError
from .trace_reservations import Obligation

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
    if value["task_id"] != obligation.owner_id or obligation.kind != "task":
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
