"""Bounded raw-evidence scans for uncorrelated infrastructure observations."""

from __future__ import annotations

import re

from edgecitadel_agentd.trace_contract import TraceContractError, validate_read_response
from edgecitadel_agentd.trace_cursor import (
    CursorScope,
    cursor_scope_hash,
    decode_cursor,
    encode_cursor,
)
from .trace_event_pages import TraceReadError
from .trace_payload_read import read_payload


def read_infrastructure(
    connection,
    *,
    signing_key,
    scope_hash,
    cursor=None,
    source=None,
    family=None,
    since=None,
    until=None,
    limit=100,
):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise TraceReadError("invalid_request")
    if source is not None and re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", source) is None:
        raise TraceReadError("invalid_request")
    if family not in (None, "infrastructure", "broker", "security", "transport"):
        raise TraceReadError("invalid_request")
    # Filter by Core receipt time: stable across unsynchronized source clocks.
    for value in (since, until):
        if value is not None and (
            not isinstance(value, str) or re.fullmatch(r"[0-9]{1,16}", value) is None
        ):
            raise TraceReadError("invalid_request")
    low, high = int(since or 0), int(until or 9007199254740991)
    if low > high:
        raise TraceReadError("invalid_request")
    scope = cursor_scope_hash(
        {"source": source, "family": family, "since": low, "until": high},
        {"access": scope_hash},
    )
    try:
        with connection:
            connection.execute("BEGIN")
            epoch, upper = connection.execute(
                "SELECT collector_epoch,ingest_seq FROM trace_collector"
            ).fetchone()
            position = 0
            if cursor:
                claims = decode_cursor(
                    cursor,
                    signing_key,
                    CursorScope("infrastructure", None, scope, epoch),
                    retained_from=0,
                )
                position, upper = claims["position"], claims["upper"]
            response = {
                "schema_version": 1,
                "kind": "infrastructure_events",
                "events": [],
                "receipt_times": {},
                "expired_scanned": 0,
                "upper_position": upper,
                "next_cursor": None,
            }
            # Bound scan work even when no rows match. Empty pages can continue.
            rows = connection.execute(
                "SELECT ingest_seq,received_at_ms,node_id FROM trace_raw_events WHERE ingest_seq>? AND ingest_seq<=? ORDER BY ingest_seq LIMIT 500",
                (position, upper),
            ).fetchall()
            for seq, received, node in rows:
                position = seq
                if not low <= received <= high or (
                    source is not None and source != node
                ):
                    continue
                payload = read_payload(connection, seq)
                if payload["event"] is None:
                    response["expired_scanned"] += 1
                    continue
                event = payload["event"]
                if event["trace_id"] is not None or event["kind"] not in (
                    "infrastructure",
                    "broker",
                    "security",
                    "transport",
                ):
                    continue
                if family is not None and event["kind"] != family:
                    continue
                response["events"].append(event)
                response["receipt_times"][
                    "/".join(event[k] for k in ("node_id", "source_epoch", "event_id"))
                ] = received
                if len(response["events"]) == limit:
                    break
            if rows and position < upper:
                response["next_cursor"] = encode_cursor(
                    {
                        "schema_version": 1,
                        "kind": "infrastructure",
                        "trace_id": None,
                        "scope_hash": scope,
                        "projection_generation": epoch,
                        "snapshot": upper,
                        "upper": upper,
                        "position": position,
                        "key": "start",
                    },
                    signing_key,
                )
            validate_read_response(response)
            return response
    except TraceContractError as error:
        code = (
            error.code
            if error.code
            in {"invalid_cursor", "cursor_scope_mismatch", "generation_changed"}
            else "unavailable"
        )
        raise TraceReadError(code) from None
