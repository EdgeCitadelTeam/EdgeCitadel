"""Stable snapshot-bound run list; authentication and HTTP wiring remain external."""

from __future__ import annotations

import re
import sqlite3

from edgecitadel_agentd.trace_contract import (
    MAX_RESPONSE_BYTES,
    TraceContractError,
    canonical_bytes,
    validate_read_response,
)
from edgecitadel_agentd.trace_cursor import (
    CursorScope,
    cursor_scope_hash,
    decode_cursor,
    encode_cursor,
)

from . import trace_projection_coverage as coverage
from . import trace_projection_history as history
from . import trace_projection_store as projection
from .trace_event_pages import TraceReadError
from .trace_projection_tables import select_tables
from .trace_run_summary import OUTCOMES, root_summary

SCAN_LIMIT = 256


def read_list(
    connection: sqlite3.Connection,
    *,
    signing_key: bytes,
    access_policy: dict,
    cursor: str | None = None,
    agent_id: str | None = None,
    outcome: str | None = None,
    limit: int = 100,
) -> dict:
    """Freeze membership and filter evaluation; continue even after sparse scans.

    agent_id means any accepted participant, not only the root owner. The caller
    selects access_policy after authorizing the request; it is never client input.
    Page size may change between requests without changing filter/access scope.
    """
    projection._idle(connection)
    if (
        type(limit) is not int
        or not 1 <= limit <= 100
        or (
            agent_id is not None
            and (
                not isinstance(agent_id, str)
                or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", agent_id) is None
            )
        )
        or (
            outcome is not None
            and (not isinstance(outcome, str) or outcome not in OUTCOMES)
        )
    ):
        raise TraceReadError("invalid_request")
    if not isinstance(signing_key, bytes) or len(signing_key) < 32:
        raise ValueError("trace_read_configuration_unavailable")
    scope = cursor_scope_hash({"agent_id": agent_id, "outcome": outcome}, access_policy)
    floor = None
    try:
        with connection:
            connection.execute("BEGIN")
            tables = select_tables(connection)
            current = projection._state(tables)
            collector_high = connection.execute(
                "SELECT ingest_seq FROM trace_collector"
            ).fetchone()[0]
            floor = history.floor(tables)
            claims = (
                decode_cursor(
                    cursor,
                    signing_key,
                    CursorScope("list", None, scope, current.generation),
                    retained_from=floor,
                )
                if cursor is not None
                else None
            )
            with history.at_cursor(
                tables,
                current,
                generation=current.generation,
                cursor=claims["snapshot"] if claims else None,
            ) as state:
                boundary = (
                    (claims["position"], claims["key"])
                    if claims and claims["key"] != "start"
                    else None
                )
                if (
                    claims
                    and claims["key"] == "start"
                    and claims["position"] != state.change_cursor
                ):
                    raise TraceReadError("invalid_cursor")

                def token(position: int, key: str) -> str:
                    return encode_cursor(
                        {
                            "schema_version": 1,
                            "kind": "list",
                            "trace_id": None,
                            "scope_hash": scope,
                            "projection_generation": state.generation,
                            "snapshot": state.change_cursor,
                            "position": position,
                            "upper": state.change_cursor,
                            "key": key,
                        },
                        signing_key,
                    )

                rows = tables.execute(
                    "SELECT created_cursor,trace_id FROM {trace_projection_runs} "
                    "WHERE expired_cursor IS NULL "
                    + ("AND (created_cursor,trace_id)<(?,?) " if boundary else "")
                    + "ORDER BY created_cursor DESC,trace_id DESC LIMIT ?",
                    (*boundary, SCAN_LIMIT + 1) if boundary else (SCAN_LIMIT + 1,),
                ).fetchall()
                response = {
                    "schema_version": 1,
                    "kind": "trace_list",
                    "projection_generation": state.generation,
                    "snapshot_cursor": token(state.change_cursor, "start"),
                    "items": [],
                    "next_cursor": None,
                    "freshness": {
                        "ingest_cursor": collector_high,
                        "projection_cursor": current.change_cursor,
                        "oldest_unsettled_age_ms": None,
                    },
                }
                # Reserve a full-length continuation before appending summaries.
                response["next_cursor"] = token(state.change_cursor, "f" * 32)
                size = len(canonical_bytes(response, limit=MAX_RESPONSE_BYTES))
                response["next_cursor"] = None
                last = boundary
                for index, (created, trace_id) in enumerate(rows):
                    if index == SCAN_LIMIT or len(response["items"]) == limit:
                        response["next_cursor"] = token(*last)
                        break
                    if (
                        agent_id is not None
                        and not tables.execute(
                            "SELECT 1 FROM {trace_projection_run_events} WHERE trace_id=? AND agent_id=? LIMIT 1",
                            (trace_id, agent_id),
                        ).fetchone()
                    ):
                        last = (created, trace_id)
                        continue
                    summary = root_summary(tables, trace_id)
                    if outcome is not None and summary["outcome"] != outcome:
                        last = (created, trace_id)
                        continue
                    item = {
                        "trace_id": trace_id,
                        **summary,
                        "created_projection_cursor": created,
                        "coverage": coverage.run_coverage(
                            tables, trace_id, unresolved=False
                        )["coverage"],
                    }
                    additional = len(
                        canonical_bytes(item, limit=MAX_RESPONSE_BYTES)
                    ) + bool(response["items"])
                    if size + additional > MAX_RESPONSE_BYTES:
                        if not response["items"]:
                            raise TraceReadError("oversize_response")
                        response["next_cursor"] = token(*last)
                        break
                    response["items"].append(item)
                    size += additional
                    last = (created, trace_id)
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
            code, retained_from=floor if code == "history_expired" else None
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
            code, retained_from=floor if code == "history_expired" else None
        ) from None
