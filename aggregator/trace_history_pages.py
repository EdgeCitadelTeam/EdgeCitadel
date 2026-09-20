"""Discover retained run snapshots without treating browser visits as history.

Each page owns one SQLite read snapshot. Signed continuations freeze both ends
of the browse range; new commits do not move a page and compaction invalidates
an old range explicitly. Source-wide coverage changes can affect another run,
so scan a bounded number of global clocks rather than filtering their trace ID.
"""

from __future__ import annotations

import re
import sqlite3

from edgecitadel_agentd.trace_contract import TraceContractError, validate_read_response
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor, encode_cursor

from . import trace_projection_coverage as coverage
from . import trace_projection_history as history
from . import trace_projection_store as projection
from .trace_event_pages import TraceReadError
from .trace_projection_tables import ProjectionTables, select_tables

SCAN_LIMIT = 64


def _summary(
    tables: ProjectionTables,
    state: projection.ProjectionState,
    trace_id: str,
    cursor: int,
) -> dict:
    with history.at_cursor(tables, state, generation=state.generation, cursor=cursor):
        header = tables.execute(
            "SELECT expired_cursor FROM {trace_projection_runs} WHERE trace_id=?",
            (trace_id,),
        ).fetchone()
        trace_state = (
            "absent"
            if header is None
            else "expired"
            if header[0] is not None
            else "present"
        )
        return {
            "trace_state": trace_state,
            "coverage": coverage.run_coverage(tables, trace_id, unresolved=False)[
                "coverage"
            ]
            if trace_state == "present"
            else None,
        }


def read_history(
    connection: sqlite3.Connection,
    *,
    trace_id: str,
    signing_key: bytes,
    scope_hash: str,
    cursor: str | None = None,
    limit: int = 20,
) -> dict:
    """Return relevant snapshots newest first, including current and retained base.

    A direct row touch includes new observations even when the visible graph is
    unchanged. Coverage comparisons include commits attributed to other runs.
    Retired/absent boundaries have no graph token; older present entries remain
    browsable until history retention actually removes them.
    """
    projection._idle(connection)
    if (
        not isinstance(trace_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", trace_id) is None
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise TraceReadError("invalid_request")
    if (
        not isinstance(signing_key, bytes)
        or len(signing_key) < 32
        or not isinstance(scope_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", scope_hash) is None
    ):
        raise ValueError("trace_read_configuration_unavailable")
    floor = None
    try:
        with connection:
            connection.execute("BEGIN")
            tables = select_tables(connection)
            state = projection._state(tables)
            floor = history.floor(tables)
            ceiling = state.change_cursor
            before = ceiling + 1
            if cursor is not None:
                claims = decode_cursor(
                    cursor,
                    signing_key,
                    CursorScope("history", trace_id, scope_hash, state.generation),
                    retained_from=floor,
                )
                marker, saved_floor = claims["key"].split(":")
                if int(saved_floor) < floor:
                    raise TraceReadError("history_expired", retained_from=floor)
                if int(saved_floor) != floor or claims["snapshot"] > ceiling:
                    raise TraceReadError("invalid_cursor")
                ceiling = claims["snapshot"]
                before = ceiling + 1 if marker == "start" else claims["position"]
                if before <= floor:
                    raise TraceReadError("invalid_cursor")
            first_run_cursor = tables.execute(
                "SELECT min(cursor) FROM {trace_projection_history_rows} WHERE table_name='trace_projection_runs' "
                "AND json_extract(row_json,'$.trace_id')=? AND cursor<=?",
                (trace_id, ceiling),
            ).fetchone()[0]
            if first_run_cursor is None:
                raise TraceReadError("not_found")
            # Before its first retained header the run is absent, so neither
            # graph nor source-wide coverage has a run snapshot to change.
            # Keep the global retention floor in tokens for compaction fencing.
            run_floor = max(floor, first_run_cursor)

            def history_token(position: int, *, start: bool = False) -> str:
                return encode_cursor(
                    {
                        "schema_version": 1,
                        "kind": "history",
                        "trace_id": trace_id,
                        "scope_hash": scope_hash,
                        "projection_generation": state.generation,
                        "snapshot": ceiling,
                        "position": position,
                        "upper": ceiling,
                        "key": f"{'start' if start else 'before'}:{floor}",
                    },
                    signing_key,
                )

            response = {
                "schema_version": 1,
                "kind": "trace_history",
                "trace_id": trace_id,
                "projection_generation": state.generation,
                "snapshot_cursor": history_token(ceiling, start=True),
                "upper_position": ceiling,
                "retained_from": floor,
                "items": [],
                "next_cursor": None,
            }
            rows = tables.execute(
                "SELECT cursor,ingest_seq,received_at_ms FROM {trace_projection_history_cursors} "
                "WHERE cursor<? AND cursor>=? ORDER BY cursor DESC LIMIT ?",
                (before, run_floor, SCAN_LIMIT + 1),
            ).fetchall()
            current = None
            scanned = before
            for index, (position, ingest, received) in enumerate(rows):
                if index == SCAN_LIMIT or len(response["items"]) == limit:
                    break
                if position != scanned - 1:
                    raise TraceReadError("history_expired", retained_from=floor)
                if current is None:
                    current = _summary(tables, state, trace_id, position)
                previous = (
                    _summary(tables, state, trace_id, position - 1)
                    if position > floor
                    else None
                )
                touched = tables.execute(
                    "SELECT 1 FROM {trace_projection_history_rows} WHERE cursor=? "
                    "AND json_extract(row_json,'$.trace_id')=? LIMIT 1",
                    (position, trace_id),
                ).fetchone()
                if (
                    position == ceiling
                    or (position == floor and current["trace_state"] != "absent")
                    or (previous is not None and (touched or current != previous))
                ):
                    at = None
                    if current["trace_state"] == "present":
                        at = encode_cursor(
                            {
                                "schema_version": 1,
                                "kind": "graph",
                                "trace_id": trace_id,
                                "scope_hash": scope_hash,
                                "projection_generation": state.generation,
                                "snapshot": position,
                                "position": position,
                                "upper": ingest,
                                "key": None,
                            },
                            signing_key,
                        )
                    response["items"].append(
                        {
                            "position": position,
                            "at": at,
                            "trace_state": current["trace_state"],
                            "received_at_ms": received or None,
                            "is_retained_base": position == floor,
                        }
                    )
                current = previous
                scanned = position
            if scanned > run_floor:
                if not rows:
                    raise TraceReadError("history_expired", retained_from=floor)
                response["next_cursor"] = history_token(scanned)
            # At most 100 short metadata entries, each with one <=4096-byte
            # cursor, fit well inside the canonical 2 MiB response budget.
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
