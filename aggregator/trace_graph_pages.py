"""Bounded graph snapshots and additive expansion pages; not HTTP wiring.

Authorization and persistent cursor-key ownership belong to the caller. All
queries, including global ancestry resolution, use one retained SQLite snapshot.
Pages bound returned data, not total SQL work; fleet-scale qualification remains
required before rollout.
"""

from __future__ import annotations

import json
import re
import sqlite3

from edgecitadel_agentd.trace_contract import TraceContractError, validate_read_response
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor, encode_cursor

from . import trace_projection_coverage as coverage
from . import trace_projection_history as history
from . import trace_projection_store as projection
from .trace_event_pages import TraceReadError
from .trace_graph_projection import resolve_graph, unresolved_node
from .trace_projection_tables import ProjectionTables, select_tables

# Include unknown endpoints as real page identities, without inventing evidence.
_NODES = """WITH actual AS (
    SELECT 'task:'||task_id AS id,node_json FROM {trace_projected_tasks} WHERE trace_id=:trace
    UNION ALL SELECT entity_id,node_json FROM {trace_projected_entities} WHERE trace_id=:trace
), identities AS (
    SELECT id FROM actual
    UNION SELECT parent_id FROM {trace_relationship_claims} WHERE trace_id=:trace
    UNION SELECT child_id FROM {trace_relationship_claims} WHERE trace_id=:trace
) """


def _node(identity: str, encoded: str | None) -> dict:
    return json.loads(encoded) if encoded is not None else unresolved_node(identity)


def _edge_page(tables: ProjectionTables, trace_id: str, offset: int) -> tuple:
    rows = tables.execute(
        "SELECT edge_id,kind,parent_id,child_id FROM {trace_relationship_claims} "
        "WHERE trace_id=? ORDER BY edge_id LIMIT 1001 OFFSET ?",
        (trace_id, offset),
    ).fetchall()
    endpoints: set[str] = set()
    selected = []
    for row in rows:
        extra = {row[2], row[3]} - endpoints
        if len(selected) == 1000 or len(endpoints) + len(extra) > 500:
            break
        selected.append(row)
        endpoints.update(extra)
    if not selected:
        raise TraceReadError("invalid_cursor")
    # Resolve against all claims, not only this page. Each unambiguous child has
    # one parent. UNION deduplicates visits so ancestry cycles always terminate.
    # A walk returning to its start identifies exactly a cycle member, not every
    # descendant which eventually reaches a cycle.
    status_rows = tables.execute(
        """WITH RECURSIVE claims AS (
            SELECT edge_id,kind,parent_id,child_id FROM {trace_relationship_claims}
            WHERE trace_id=?
        ), page AS (
            SELECT * FROM claims ORDER BY edge_id LIMIT ? OFFSET ?
        ), parents AS (
            SELECT child_id,min(parent_id) AS parent_id,count(DISTINCT parent_id) AS n
            FROM claims WHERE kind!='join' GROUP BY child_id
        ), walk(start,node) AS (
            SELECT page.child_id,parents.parent_id FROM page JOIN parents USING(child_id)
            WHERE page.kind!='join' AND parents.n=1
            UNION
            SELECT walk.start,parents.parent_id FROM walk JOIN parents ON parents.child_id=walk.node
            WHERE parents.n=1
        )
        SELECT page.edge_id,(page.kind!='join' AND (
            coalesce(parents.n,0)>1 OR EXISTS(SELECT 1 FROM walk WHERE start=page.child_id AND node=start)))
        FROM page LEFT JOIN parents USING(child_id)""",
        (trace_id, len(selected), offset),
    ).fetchall()
    invalid = dict(status_rows)
    # Endpoint names are parameters; none become SQL identifiers.
    names = sorted(endpoints)
    parameters = {f"node{i}": identity for i, identity in enumerate(names)}
    placeholders = ",".join(f":{name}" for name in parameters)
    actual = tables.execute(
        _NODES + f"SELECT id,node_json FROM actual WHERE id IN ({placeholders})",
        {"trace": trace_id, **parameters},
    ).fetchall()
    by_id = {identity: _node(identity, encoded) for identity, encoded in actual}
    for identity in endpoints:
        by_id.setdefault(identity, unresolved_node(identity))
    edges = [
        {
            "id": identity,
            "kind": kind,
            "from": parent,
            "to": child,
            "status": "invalid"
            if invalid[identity]
            else "unresolved"
            if any(by_id[key]["kind"] == "unresolved" for key in (parent, child))
            else "resolved",
        }
        for identity, kind, parent, child in selected
    ]
    return (
        sorted(by_id.values(), key=lambda node: node["id"]),
        edges,
        len(rows) > len(selected),
    )


def read_graph(
    connection: sqlite3.Connection,
    *,
    trace_id: str,
    signing_key: bytes,
    scope_hash: str,
    at: str | None = None,
    expand: str | None = None,
) -> dict:
    """Snapshot plus distinct event/playback and live-resume cursors.

    Large graphs page all node identities first, then all edges with their full
    endpoints. Expansion pages merge by node/edge ID only within the same `at`.
    An edge's resolved/invalid/unresolved status never depends on page order.
    """
    projection._idle(connection)
    if (
        not isinstance(trace_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", trace_id) is None
        or (at is not None and expand is not None)
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
            current = projection._state(tables)
            collector_high = connection.execute(
                "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
            ).fetchone()[0]
            floor = history.floor(tables)
            claims = None
            if at is not None or expand is not None:
                claims = decode_cursor(
                    expand if expand is not None else at,
                    signing_key,
                    CursorScope(
                        "expansion" if expand is not None else "graph",
                        trace_id,
                        scope_hash,
                        current.generation,
                    ),
                    retained_from=floor,
                )
            with history.at_cursor(
                tables,
                current,
                generation=current.generation,
                cursor=claims["snapshot"] if claims else None,
            ) as state:
                if claims and claims["upper"] != state.ingest_cursor:
                    raise TraceReadError("invalid_cursor")
                run = tables.execute(
                    "SELECT expired_cursor FROM {trace_projection_runs} WHERE trace_id=?",
                    (trace_id,),
                ).fetchone()
                if run is None:
                    raise TraceReadError("not_found")
                if run[0] is not None:
                    raise TraceReadError("history_expired", retained_from=floor)
                total = tables.execute(
                    _NODES + "SELECT count(*) FROM identities", {"trace": trace_id}
                ).fetchone()[0]
                branch = claims["key"] if expand is not None else "nodes"
                offset = claims["position"] if expand is not None else 0
                following = None
                if branch == "nodes":
                    rows = tables.execute(
                        _NODES
                        + "SELECT identities.id,actual.node_json FROM identities "
                        "LEFT JOIN actual USING(id) ORDER BY identities.id LIMIT 501 OFFSET :offset",
                        {"trace": trace_id, "offset": offset},
                    ).fetchall()
                    if not rows and offset:
                        raise TraceReadError("invalid_cursor")
                    nodes = [_node(*row) for row in rows[:500]]
                    edges = []
                    if len(rows) > 500:
                        following = ("nodes", offset + 500)
                    else:
                        relations = tables.execute(
                            "SELECT edge_id,kind,parent_id,child_id FROM {trace_relationship_claims} "
                            "WHERE trace_id=? ORDER BY edge_id LIMIT 1001",
                            (trace_id,),
                        ).fetchall()
                        if relations and offset == 0 and len(relations) <= 1000:
                            resolved = resolve_graph(
                                nodes,
                                [
                                    {"id": i, "kind": k, "from": p, "to": c}
                                    for i, k, p, c in relations
                                ],
                            )
                            nodes, edges = resolved["nodes"], resolved["edges"]
                        elif relations:
                            following = ("edges", 0)
                elif branch == "edges":
                    nodes, edges, more = _edge_page(tables, trace_id, offset)
                    if more:
                        following = ("edges", offset + len(edges))
                else:
                    raise TraceReadError("invalid_cursor")

                def token(kind: str, position: int, key: str | None = None) -> str:
                    return encode_cursor(
                        {
                            "schema_version": 1,
                            "kind": kind,
                            "trace_id": trace_id,
                            "scope_hash": scope_hash,
                            "projection_generation": state.generation,
                            "snapshot": state.change_cursor,
                            "position": position,
                            "upper": state.change_cursor
                            if kind == "changes"
                            else state.ingest_cursor,
                            "key": key,
                        },
                        signing_key,
                    )

                expansions = []
                if following:
                    branch, position = following
                    expansions.append(
                        {
                            "node_id": nodes[0]["id"],
                            "cursor": token("expansion", position, branch),
                            "remaining_nodes": max(0, total - position)
                            if branch == "nodes"
                            else 0,
                        }
                    )
                completeness = coverage.run_coverage(tables, trace_id, unresolved=False)
                response = {
                    "schema_version": 1,
                    "kind": "trace_graph",
                    "page_kind": "expansion" if expand is not None else "snapshot",
                    "trace_id": trace_id,
                    "projection_generation": state.generation,
                    "at": token("graph", state.change_cursor),
                    "ingest_high_watermark": state.ingest_cursor,
                    "nodes": nodes,
                    "edges": edges,
                    "coverage": completeness["coverage"],
                    "total_nodes": total,
                    "expansions": expansions,
                    "freshness": {
                        "ingest_cursor": collector_high,
                        "projection_cursor": current.change_cursor,
                        "oldest_unsettled_age_ms": None,
                        "collector_state": "unknown",
                    },
                    "resume_cursor": token("changes", state.change_cursor),
                }
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
