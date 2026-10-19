"""Replay durable projection commits as atomic graph changes, without a broadcast.

Caller authorization and signing-key provisioning remain external. Large graph
updates carry an exact retained graph cursor; clients complete its expansion
before acknowledging the update. No partial commit is silently skipped.
"""

from __future__ import annotations

import json
import re
import sqlite3

from edgecitadel_agentd.trace_contract import (
    MAX_RESPONSE_BYTES,
    TraceContractError,
    canonical_bytes,
    validate_read_response,
)
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor, encode_cursor

from . import trace_projection_coverage as coverage
from . import trace_projection_history as history
from . import trace_projection_store as projection
from .trace_event_pages import TraceReadError
from .trace_graph_projection import resolve_graph
from .trace_projection_tables import ProjectionTables, select_tables

SCAN_LIMIT = 64


def _snapshot(
    tables: ProjectionTables,
    state: projection.ProjectionState,
    trace_id: str,
    cursor: int,
) -> dict:
    with history.at_cursor(
        tables, state, generation=state.generation, cursor=cursor
    ) as selected:
        header = tables.execute(
            "SELECT expired_cursor FROM {trace_projection_runs} WHERE trace_id=?",
            (trace_id,),
        ).fetchone()
        result = {
            "trace_state": "absent"
            if header is None
            else "expired"
            if header[0] is not None
            else "present",
            "ingest": selected.ingest_cursor,
            "nodes": [],
            "edges": [],
            "large": False,
            "coverage": {
                "partial": True,
                "catching_up": False,
                "gap": False,
                "unknown_sources": True,
                "unsupported_families": [],
                "reconciled_through": [],
            },
        }
        if result["trace_state"] != "present":
            return result
        result["coverage"] = coverage.run_coverage(tables, trace_id, unresolved=False)[
            "coverage"
        ]
        tasks = tables.execute(
            "SELECT node_json FROM {trace_projected_tasks} WHERE trace_id=? LIMIT 501",
            (trace_id,),
        ).fetchall()
        entities = tables.execute(
            "SELECT node_json FROM {trace_projected_entities} WHERE trace_id=? LIMIT 501",
            (trace_id,),
        ).fetchall()
        claims = tables.execute(
            "SELECT edge_id,kind,parent_id,child_id FROM {trace_relationship_claims} WHERE trace_id=? LIMIT 1001",
            (trace_id,),
        ).fetchall()
        if len(tasks) + len(entities) > 500 or len(claims) > 1000:
            result["large"] = True
            return result
        try:
            graph = resolve_graph(
                [json.loads(row[0]) for row in (*tasks, *entities)],
                [{"id": i, "kind": k, "from": p, "to": c} for i, k, p, c in claims],
            )
        except ValueError as error:
            if str(error) != "projection_graph_expansion_required":
                raise
            result["large"] = True
        else:
            result.update(nodes=graph["nodes"], edges=graph["edges"])
        return result


def _delta(before: list[dict], after: list[dict]) -> tuple[list[dict], list[str]]:
    old = {item["id"]: item for item in before}
    new = {item["id"]: item for item in after}
    return [new[key] for key in sorted(new) if old.get(key) != new[key]], sorted(
        old.keys() - new.keys()
    )


def _local_graph_patch(
    tables: ProjectionTables,
    trace_id: str,
    cursor: int,
) -> tuple[list[dict], list[dict]] | None:
    """Prove a bounded patch without changing any existing edge's resolution.

    Existing node updates preserve identity/kind. One previously unreferenced
    node may be added, with incoming edges from known existing nodes and at most
    one ancestry parent. Everything else uses the global snapshot resolver.
    """

    def previous(table: str, key: str) -> dict | None:
        row = tables.execute(
            "SELECT deleted,row_json FROM {trace_projection_history_rows} "
            "WHERE table_name=? AND row_key=? AND cursor<? ORDER BY cursor DESC LIMIT 1",
            (table, key, cursor),
        ).fetchone()
        return json.loads(row[1]) if row is not None and not row[0] else None

    rows = tables.execute(
        "SELECT table_name,row_key,deleted,row_json FROM {trace_projection_history_rows} "
        "WHERE cursor=? AND table_name IN ('trace_projected_tasks','trace_projected_entities') "
        "AND json_extract(row_json,'$.trace_id')=? LIMIT 501",
        (cursor, trace_id),
    ).fetchall()
    if len(rows) > 500:
        return None
    updates = []
    added = []
    for table, key, deleted, encoded in rows:
        if deleted:
            return None
        old = previous(table, key)
        after = json.loads(json.loads(encoded)["node_json"])
        if after["kind"] == "unresolved":
            return None
        if old is None:
            added.append(after["id"])
            updates.append(after)
        else:
            before = json.loads(old["node_json"])
            if before["id"] != after["id"] or before["kind"] != after["kind"]:
                return None
            if before != after:
                updates.append(after)
    # One event normally introduces one step. Multiple new nodes require the
    # general resolver rather than another local cycle/ancestry implementation.
    if len(added) > 1:
        return None
    claims = tables.execute(
        "SELECT row_key,deleted,row_json FROM {trace_projection_history_rows} WHERE cursor=? "
        "AND table_name='trace_relationship_claims' AND json_extract(row_json,'$.trace_id')=? LIMIT 1001",
        (cursor, trace_id),
    ).fetchall()
    if len(claims) > 1000 or (claims and not added):
        return None
    edges = []
    parents = set()
    for key, deleted, encoded in claims:
        if deleted or previous("trace_relationship_claims", key) is not None:
            return None
        claim = json.loads(encoded)
        if claim["child_id"] != added[0] or claim["parent_id"] == added[0]:
            return None
        if claim["kind"] != "join":
            parents.add(claim["parent_id"])
        parent = claim["parent_id"]
        table, identity = (
            ("trace_projected_tasks", parent[5:])
            if parent.startswith("task:")
            else ("trace_projected_entities", parent)
        )
        known = previous(table, json.dumps([trace_id, identity], separators=(",", ":")))
        if known is None or json.loads(known["node_json"])["kind"] == "unresolved":
            return None
        edges.append(
            {
                "id": claim["edge_id"],
                "kind": claim["kind"],
                "from": parent,
                "to": claim["child_id"],
                "status": "resolved",
            }
        )
    if len(parents) > 1:
        return None
    if added:
        # An apparently new real node may replace an unresolved endpoint. That
        # can change old edges or complete a cycle and must use global resolution.
        # Any older reference is enough to decline the optimization, including
        # retired claims. Avoid reconstructing all historical tables to prove it.
        referenced = tables.execute(
            "SELECT 1 FROM {trace_projection_history_rows} WHERE table_name='trace_relationship_claims' "
            "AND json_extract(row_json,'$.trace_id')=? AND cursor<? "
            "AND (json_extract(row_json,'$.parent_id')=? OR json_extract(row_json,'$.child_id')=?) LIMIT 1",
            (trace_id, cursor, added[0], added[0]),
        ).fetchone()
        if referenced:
            return None
    return sorted(updates, key=lambda node: node["id"]), sorted(
        edges, key=lambda edge: edge["id"]
    )


def read_changes(
    connection: sqlite3.Connection,
    *,
    trace_id: str,
    after: str,
    signing_key: bytes,
    scope_hash: str,
    limit: int = 200,
) -> dict:
    """Scan a bounded prefix; advance through_cursor only after fully served commits.

    A trace can be affected by source-wide receipt/coverage commits attributed to
    another trace, so never filter the durable log by its nullable trace_id. The
    result may be empty with a continuation after scanning unrelated commits.
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
    floor = None
    try:
        with connection:
            connection.execute("BEGIN")
            tables = select_tables(connection)
            state = projection._state(tables)
            floor = history.floor(tables)
            claims = decode_cursor(
                after,
                signing_key,
                CursorScope("changes", trace_id, scope_hash, state.generation),
                retained_from=floor,
            )
            if claims["upper"] > state.change_cursor:
                raise TraceReadError("invalid_cursor")
            start = claims["position"]
            before = _snapshot(tables, state, trace_id, start)
            rows = tables.execute(
                "SELECT cursor FROM {trace_projection_changes} WHERE cursor>? AND cursor<=? ORDER BY cursor LIMIT ?",
                (start, state.change_cursor, SCAN_LIMIT + 1),
            ).fetchall()

            def token(kind: str, position: int, ingest: int = 0) -> str:
                return encode_cursor(
                    {
                        "schema_version": 1,
                        "kind": kind,
                        "trace_id": trace_id,
                        "scope_hash": scope_hash,
                        "projection_generation": state.generation,
                        "snapshot": state.change_cursor
                        if kind == "changes"
                        else position,
                        "position": position,
                        "upper": state.change_cursor if kind == "changes" else ingest,
                        "key": None,
                    },
                    signing_key,
                )

            response = {
                "schema_version": 1,
                "kind": "trace_changes",
                "trace_id": trace_id,
                "projection_generation": state.generation,
                "changes": [],
                "next_cursor": token("changes", state.change_cursor),
                "through_cursor": token("changes", state.change_cursor),
            }
            size = len(canonical_bytes(response, limit=MAX_RESPONSE_BYTES))
            response["next_cursor"] = None
            through = start
            for index, (cursor,) in enumerate(rows):
                if index == SCAN_LIMIT or len(response["changes"]) == limit:
                    break
                # A retained change and its history clock are committed together.
                if cursor != through + 1:
                    raise TraceReadError("history_expired", retained_from=floor)
                current = _snapshot(tables, state, trace_id, cursor)
                touched = (
                    tables.execute(
                        "SELECT 1 FROM {trace_projection_history_rows} WHERE cursor=? AND json_extract(row_json,'$.trace_id')=? LIMIT 1",
                        (cursor, trace_id),
                    ).fetchone()
                    is not None
                )
                changed = touched or any(
                    current[key] != before[key]
                    for key in ("trace_state", "coverage", "large", "nodes", "edges")
                )
                if changed:
                    local_patch = (
                        _local_graph_patch(tables, trace_id, cursor)
                        if current["trace_state"] == before["trace_state"] == "present"
                        and current["large"]
                        and before["large"]
                        else None
                    )
                    mode = (
                        "clear"
                        if current["trace_state"] != "present"
                        else "snapshot"
                        if (current["large"] or before["large"]) and local_patch is None
                        else "patch"
                    )
                    nodes, removed_nodes = (
                        (local_patch[0], [])
                        if local_patch is not None
                        else _delta(before["nodes"], current["nodes"])
                        if mode == "patch"
                        else ([], [])
                    )
                    edges, removed_edges = (
                        (local_patch[1], [])
                        if local_patch is not None
                        else _delta(before["edges"], current["edges"])
                        if mode == "patch"
                        else ([], [])
                    )
                    change = {
                        "cursor": token("changes", cursor),
                        "at": token("graph", cursor, current["ingest"])
                        if mode != "clear"
                        else None,
                        "mode": mode,
                        "trace_state": current["trace_state"],
                        "ingest_high_watermark": current["ingest"],
                        "upsert_nodes": nodes,
                        "remove_node_ids": removed_nodes,
                        "upsert_edges": edges,
                        "remove_edge_ids": removed_edges,
                        "coverage": current["coverage"],
                    }
                    additional = len(
                        canonical_bytes(change, limit=MAX_RESPONSE_BYTES)
                    ) + bool(response["changes"])
                    if size + additional > MAX_RESPONSE_BYTES:
                        if not response["changes"]:
                            raise TraceReadError("oversize_response")
                        break
                    response["changes"].append(change)
                    size += additional
                before = current
                through = cursor
            if through < state.change_cursor:
                if not rows:
                    raise TraceReadError("history_expired", retained_from=floor)
                response["next_cursor"] = token("changes", through)
            response["through_cursor"] = token("changes", through)
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
