"""Reserved local session/presence and connector audit records."""

from __future__ import annotations

import json
import sqlite3
from uuid import uuid4
from typing import Any

from .trace_contract import TraceContractError
from .trace_counters import MAX_COUNTER, encode_counter
from .trace_reservations import Obligation, fill
from .trace_task_completion import EVENT_COLUMNS

PRESENCE_COLUMNS = ("presence_id", "agent_id", "state", "reason", "observed_at_ms")


def install(db: sqlite3.Connection) -> None:
    db.execute(f"""CREATE TABLE trace_presence_counter (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        next_id BLOB NOT NULL CHECK(typeof(next_id)='blob' AND length(next_id)=20
            AND hex(next_id) GLOB '{"3[0-9]" * 20}'
            AND next_id BETWEEN X'{encode_counter(1).hex()}' AND X'{encode_counter(MAX_COUNTER).hex()}')
    )""")
    maximum = db.execute(
        "SELECT COALESCE(MAX(seq),0) FROM sqlite_sequence WHERE name='presence_history'"
    ).fetchone()[0]
    db.execute(
        "INSERT INTO trace_presence_counter VALUES (1,?)",
        (encode_counter(maximum + 1),),
    )
    db.execute("DROP VIEW events_all")
    legacy = ",".join(
        f"json_extract(CAST(record AS TEXT),'$.legacy_event.{key}') AS {key}"
        for key in EVENT_COLUMNS
    )
    local = ",".join(
        f"json_extract(CAST(record AS TEXT),'$.local.event.{key}') AS {key}"
        for key in EVENT_COLUMNS
    )
    db.execute(f"""CREATE VIEW events_all AS
        SELECT {",".join(EVENT_COLUMNS)} FROM events
        UNION ALL SELECT {legacy} FROM trace_completion_slots WHERE filled=1 AND json_extract(CAST(record AS TEXT),'$.legacy_event') IS NOT NULL
        UNION ALL SELECT {local} FROM trace_completion_slots WHERE filled=1 AND json_extract(CAST(record AS TEXT),'$.local.event') IS NOT NULL""")
    presence = ",".join(
        f"json_extract(CAST(record AS TEXT),'$.local.presence.{key}') AS {key}"
        for key in PRESENCE_COLUMNS
    )
    db.execute(f"""CREATE VIEW presence_history_all AS
        SELECT {",".join(PRESENCE_COLUMNS)} FROM presence_history
        UNION ALL SELECT {presence} FROM trace_completion_slots WHERE filled=1 AND json_extract(CAST(record AS TEXT),'$.local.presence') IS NOT NULL""")


def next_presence_id(db: sqlite3.Connection) -> int:
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    row = db.execute(
        "SELECT next_id FROM trace_presence_counter WHERE singleton=1"
    ).fetchone()
    if row is None:
        raise TraceContractError("presence_counter_missing")
    value = int(row[0])
    db.execute(
        "UPDATE trace_presence_counter SET next_id=? WHERE singleton=1",
        (encode_counter(value + 1),),
    )
    return value


def finish_session(
    db: sqlite3.Connection, session_id: str, now: int, reason: str | None
) -> None:
    row = db.execute(
        "SELECT s.connector_id,c.agent_id FROM sessions s JOIN connectors c USING(connector_id) WHERE s.session_id=?",
        (session_id,),
    ).fetchone()
    if row is None:
        raise TraceContractError("completion_session_missing")
    db.use_completion_workspace()
    local: dict[str, Any] = {
        "session_id": session_id,
        "connector_id": row[0],
        "agent_id": row[1],
    }
    if reason is not None:
        if reason not in {"native_session_closed", "session_lease_expired"}:
            raise TraceContractError("invalid_session_completion")
        local["presence"] = {
            "presence_id": next_presence_id(db),
            "agent_id": row[1],
            "state": "unavailable",
            "reason": reason,
            "observed_at_ms": now,
        }
    fill(db, Obligation("session", session_id, "close"), {"local": local})


def revoke_connector(
    db: sqlite3.Connection, connector_id: str, agent_id: str, now: int
) -> None:
    db.use_completion_workspace()
    event = {
        "event_id": str(uuid4()),
        "event_type": "connector.revoked",
        "agent_id": agent_id,
        "task_id": None,
        "trace_id": None,
        "attributes_json": json.dumps(
            {"connector_id": connector_id}, sort_keys=True, separators=(",", ":")
        ),
        "created_at_ms": now,
    }
    fill(
        db,
        Obligation("connector", connector_id, "revoke"),
        {"local": {"connector_id": connector_id, "agent_id": agent_id, "event": event}},
    )


def materialize(db: sqlite3.Connection, record: dict[str, Any]) -> None:
    local = record["local"]
    for field, table, columns in (
        ("event", "events", EVENT_COLUMNS),
        ("presence", "presence_history", PRESENCE_COLUMNS),
    ):
        if field in local:
            db.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(local[field][key] for key in columns),
            )
