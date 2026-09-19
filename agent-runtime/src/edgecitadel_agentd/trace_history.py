"""Bounded local journal inspection; this is not the Core projection API."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .store import AgentdStore


def read_history(
    store: AgentdStore, *, connector_id: str, token: str, params: dict[str, Any]
) -> dict[str, Any]:
    from .store import StoreError

    if set(params) - {"trace_id", "source_epoch", "after_source_seq", "limit"}:
        raise StoreError("invalid local history request")
    trace_id = params.get("trace_id")
    epoch = params.get("source_epoch")
    after, limit = params.get("after_source_seq", 0), params.get("limit", 32)
    if (
        type(after) is not int
        or not 0 <= after <= 2**53 - 1
        or type(limit) is not int
        or not 1 <= limit <= 32
        or (
            trace_id is not None
            and (
                not isinstance(trace_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", trace_id) is None
            )
        )
        or (
            epoch is not None
            and (
                not isinstance(epoch, str)
                or re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                    epoch,
                )
                is None
            )
        )
        or (after and epoch is None)
    ):
        raise StoreError("invalid local history request")
    with store._lock:
        connector = store.authenticate(connector_id, token)
        if (
            connector["host_type"] != "managed-agent"
            and "edgecitadel_trace"
            not in json.loads(connector["capabilities_json"])["items"]
        ):
            raise StoreError("connector is not authorized for this operation")
        agent_id = connector["agent_id"]
        retention_values = [agent_id]
        retention_scope = ""
        if epoch is not None:
            retention_scope = " AND source_epoch=?"
            retention_values.append(epoch)
        # Actor-wide warning: a compact marker can account for several traces.
        history_pruned = (
            store._connection.execute(
                "SELECT 1 FROM trace_journal_all WHERE agent_id=?"
                + retention_scope
                + " AND json_extract(event_json,'$.kind')='coverage' "
                "AND json_extract(event_json,'$.attributes.reason') IN ('quota_exceeded','retention_expired') LIMIT 1",
                retention_values,
            ).fetchone()
            is not None
        )
        trace_scope = " AND trace_id=?" if trace_id is not None else ""
        source_values = [agent_id] if trace_id is None else [agent_id, trace_id]
        # Only epochs containing this actor's observations are discoverable.
        sources = store._connection.execute(
            "SELECT DISTINCT node_id,source_epoch FROM trace_journal_all WHERE agent_id=?"
            + trace_scope
            + " ORDER BY node_id,source_epoch LIMIT 101",
            source_values,
        ).fetchall()
        if len(sources) > 100:
            raise StoreError("local history source limit exceeded")
        if epoch is None and sources:
            latest = store._connection.execute(
                "SELECT j.source_epoch FROM trace_journal_all j WHERE agent_id=?"
                + trace_scope
                + " ORDER BY j.received_at_ms DESC,(SELECT s.rowid FROM trace_sources s WHERE s.node_id=j.node_id AND s.source_epoch=j.source_epoch) DESC,j.source_seq DESC LIMIT 1",
                source_values,
            ).fetchone()
            epoch = latest[0]
        visible = [dict(source) for source in sources]
        matching = [source for source in sources if source["source_epoch"] == epoch]
        events = []
        more = False
        reported_loss = False
        if matching:
            scope = " AND trace_id=?" if trace_id is not None else ""
            loss_values = [agent_id, epoch]
            if trace_id is not None:
                loss_values.append(trace_id)
            reported_loss = (
                store._connection.execute(
                    "SELECT 1 FROM trace_journal_all WHERE agent_id=? AND source_epoch=?"
                    + scope
                    + " AND json_extract(event_json,'$.kind')='coverage' "
                    "AND json_extract(event_json,'$.attributes.dropped_observations')>0 LIMIT 1",
                    loss_values,
                ).fetchone()
                is not None
            )
            values = [agent_id, epoch, after]
            if trace_id is not None:
                values.append(trace_id)
            values.append(limit + 1)
            rows = store._connection.execute(
                "SELECT event_json FROM trace_journal_all WHERE agent_id=? AND source_epoch=? "
                "AND source_seq>?" + scope + " ORDER BY source_seq LIMIT ?",
                values,
            ).fetchall()
            more = len(rows) > limit
            response_bytes = 0
            for row in rows[:limit]:
                event = json.loads(row[0])
                # Match the private socket's ASCII JSON encoding, including
                # expansion of Unicode. Leave room for the RPC envelope.
                event_bytes = len(json.dumps(event).encode())
                if response_bytes + event_bytes > 512 * 1024:
                    more = True
                    break
                events.append(event)
                response_bytes += event_bytes
        return {
            "schema_version": 1,
            "kind": "local_trace_history",
            "sources": visible,
            "source_epoch": epoch if matching else None,
            "events": events,
            "next_source_seq": events[-1]["source_seq"] if more else None,
            "coverage": {
                "partial": True,
                "scope": "local_agent_observations",
                "producer_loss": "reported" if reported_loss else "unknown",
                "local_history_pruned": history_pruned,
            },
        }
