"""Snapshot-bound observation pages for the trace read API.

The caller authorizes access before opening its read connection and supplies the
server-selected scope hash and persistent signing key. Cursor integrity is not
access authorization. This module never opens files, migrates or fetches content
references. It is not registered as an HTTP route yet.
"""

from __future__ import annotations

import re
import sqlite3

from edgecitadel_agentd.trace_contract import (
    MAX_RESPONSE_BYTES,
    TraceContractError,
    canonical_bytes,
    event_sha256,
    validate_read_response,
)
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor, encode_cursor

from . import trace_projection_history as history
from . import trace_projection_store as projection
from .trace_payload_read import read_payload
from .trace_projection_tables import select_tables

ERROR_STATUS = {
    "invalid_request": 400,
    "invalid_cursor": 400,
    "cursor_scope_mismatch": 400,
    "generation_changed": 409,
    "history_expired": 410,
    "not_found": 404,
    "unavailable": 503,
    "oversize_response": 503,
}


class TraceReadError(ValueError):
    def __init__(self, code: str, *, retained_from: int | None = None):
        self.status_code = ERROR_STATUS[code]
        self.response = {
            "schema_version": 1,
            "kind": "trace_error",
            "code": code,
            "resnapshot_required": code in {"generation_changed", "history_expired"},
            "retained_from": retained_from,
            "retryable": code == "unavailable",
        }
        super().__init__(code)


def read_events(
    connection: sqlite3.Connection,
    *,
    trace_id: str,
    as_of: str,
    signing_key: bytes,
    scope_hash: str,
    after: str | None = None,
    limit: int = 200,
) -> dict:
    """Read accepted observations in ingest order, never beyond the graph snapshot.

    All validation, membership and payload reads share one closed transaction.
    Expired payloads produce a retention error instead of silently shortening the
    history. Count and encoded-byte bounds both produce continuation tokens.
    """
    projection._idle(connection)
    if (
        not isinstance(trace_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", trace_id) is None
        or type(limit) is not int
        or not 1 <= limit <= 500
    ):
        raise TraceReadError("invalid_request")
    if (
        not isinstance(signing_key, bytes)
        or len(signing_key) < 32
        or not isinstance(scope_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", scope_hash) is None
    ):
        raise ValueError("trace_read_configuration_unavailable")
    retained_from = None
    try:
        with connection:
            connection.execute("BEGIN")
            tables = select_tables(connection)
            state = projection._state(tables)
            retained_from = history.floor(tables)

            def decode(token: str, kind: str) -> dict:
                return decode_cursor(
                    token,
                    signing_key,
                    CursorScope(kind, trace_id, scope_hash, state.generation),
                    retained_from=retained_from,
                )

            snapshot = decode(as_of, "graph")
            position = 0
            if after is not None:
                page = decode(after, "events")
                if (page["snapshot"], page["upper"]) != (
                    snapshot["snapshot"],
                    snapshot["upper"],
                ):
                    raise TraceReadError("cursor_scope_mismatch")
                position = page["position"]
            with history.at_cursor(
                tables,
                state,
                generation=state.generation,
                cursor=snapshot["snapshot"],
            ) as selected:
                if selected.ingest_cursor != snapshot["upper"]:
                    raise TraceReadError("invalid_cursor")
                run = tables.execute(
                    "SELECT expired_cursor FROM {trace_projection_runs} WHERE trace_id=?",
                    (trace_id,),
                ).fetchone()
                if run is None:
                    raise TraceReadError("not_found")
                if run[0] is not None:
                    raise TraceReadError("history_expired", retained_from=retained_from)
                rows = tables.execute(
                    "SELECT ingest_seq,node_id,source_epoch,event_id,event_sha256 "
                    "FROM {trace_projection_run_events} "
                    "WHERE trace_id=? AND ingest_seq>? AND ingest_seq<=? "
                    "ORDER BY ingest_seq LIMIT ?",
                    (trace_id, position, selected.ingest_cursor, limit + 1),
                ).fetchall()
                response = {
                    "schema_version": 1,
                    "kind": "trace_events",
                    "trace_id": trace_id,
                    "projection_generation": state.generation,
                    "as_of": as_of,
                    "events": [],
                    "next_cursor": None,
                }

                def continuation(last: int) -> str:
                    return encode_cursor(
                        {**snapshot, "kind": "events", "position": last}, signing_key
                    )

                # Reserve the largest possible continuation for this snapshot.
                # Canonical JSON numbers only get shorter at earlier positions.
                response["next_cursor"] = continuation(selected.ingest_cursor)
                size = len(canonical_bytes(response, limit=MAX_RESPONSE_BYTES))
                response["next_cursor"] = None
                last = position
                for index, (seq, node, epoch, identity, digest) in enumerate(rows):
                    if index == limit:
                        response["next_cursor"] = continuation(last)
                        break
                    raw = connection.execute(
                        "SELECT node_id,source_epoch,event_id,event_sha256,source_seq "
                        "FROM trace_raw_events WHERE ingest_seq=?",
                        (seq,),
                    ).fetchone()
                    if raw is None or raw[:4] != (node, epoch, identity, digest):
                        raise TraceReadError("unavailable")
                    payload = read_payload(connection, seq)
                    if payload is None:
                        raise TraceReadError("unavailable")
                    event = payload["event"]
                    if event is None:
                        raise TraceReadError(
                            "history_expired", retained_from=retained_from
                        )
                    if event_sha256(event) != digest or (
                        event["node_id"],
                        event["source_epoch"],
                        event["event_id"],
                        event["source_seq"],
                        event["trace_id"],
                    ) != (node, epoch, identity, raw[4], trace_id):
                        raise TraceReadError("unavailable")
                    additional = len(canonical_bytes(event)) + bool(response["events"])
                    if size + additional > MAX_RESPONSE_BYTES:
                        if not response["events"]:
                            raise TraceReadError("oversize_response")
                        response["next_cursor"] = continuation(last)
                        break
                    response["events"].append(event)
                    size += additional
                    last = seq
                validate_read_response(response)
                return response
    except TraceReadError:
        raise
    except TraceContractError as error:
        code = str(error)
        if code not in {
            "invalid_cursor",
            "cursor_scope_mismatch",
            "generation_changed",
            "history_expired",
        }:
            code = "unavailable"
        raise TraceReadError(
            code, retained_from=retained_from if code == "history_expired" else None
        ) from None
    except sqlite3.Error:
        raise TraceReadError("unavailable") from None
    except ValueError as error:
        code = {
            "projection_generation_mismatch": "generation_changed",
            "projection_cursor_expired": "history_expired",
            "projection_history_unavailable": "history_expired",
            "projection_cursor_ahead": "invalid_cursor",
        }.get(str(error), "unavailable")
        raise TraceReadError(
            code, retained_from=retained_from if code == "history_expired" else None
        ) from None
