"""Read Core payload layouts using only the standard library; never migrate."""

from __future__ import annotations

import json
import sqlite3
from typing import Any


class PayloadReadError(ValueError):
    """Fixed read-only layout diagnostic."""


def read_payload_parts(
    connection: sqlite3.Connection, ingest_seq: int
) -> tuple[str | None, int | None] | None:
    """Materialize encoded content and expiry from the caller’s snapshot."""
    if type(ingest_seq) is not int or ingest_seq < 1:
        raise ValueError("invalid_ingest_sequence")
    separated = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_payloads'"
    ).fetchone()
    if separated:
        row = connection.execute(
            "SELECT r.event_json,r.payload_expired_at_ms,p.event_json FROM trace_raw_events r "
            "LEFT JOIN trace_payloads p USING(ingest_seq) WHERE r.ingest_seq=?",
            (ingest_seq,),
        ).fetchone()
    else:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(trace_raw_events)")
        }
        expired_column = (
            "payload_expired_at_ms" if "payload_expired_at_ms" in columns else "NULL"
        )
        row = connection.execute(
            f"SELECT event_json,{expired_column},NULL FROM trace_raw_events WHERE ingest_seq=?",
            (ingest_seq,),
        ).fetchone()
    if row is None:
        return None
    inline, expired, payload = row
    if expired is not None:
        if inline or payload is not None:
            raise PayloadReadError("core_payload_layout_unavailable")
        return None, expired
    if bool(inline) == (payload is not None):
        raise PayloadReadError("core_payload_layout_unavailable")
    return inline or payload, None


def decode_payload(parts: tuple[str | None, int | None]) -> dict[str, Any]:
    """Decode materialized bytes without needing a database connection."""
    payload, expired = parts
    if expired is not None:
        return {
            "payload_state": "expired",
            "event": None,
            "payload_expired_at_ms": expired,
        }
    try:
        event = json.loads(payload)
    except (ValueError, TypeError) as error:
        raise PayloadReadError("core_payload_layout_unavailable") from error
    return {"payload_state": "retained", "event": event, "payload_expired_at_ms": None}


def read_payload(
    connection: sqlite3.Connection, ingest_seq: int
) -> dict[str, Any] | None:
    """Resolve one legacy, mixed or migrated record without changing storage."""
    parts = read_payload_parts(connection, ingest_seq)
    return None if parts is None else decode_payload(parts)
