"""Explicit root identities and owner outcomes; never infer outcome from children."""

from __future__ import annotations

import json

from .trace_payload_read import read_payload
from .trace_projection_tables import ProjectionTables

OUTCOMES = frozenset(
    {"completed", "failed", "rejected", "canceled", "expired", "undeliverable"}
)

_TASK_EVIDENCE = """WITH evidence AS (
    SELECT task_id,source_role,phase,evidence_json FROM {trace_task_perspectives} WHERE trace_id=:trace
    UNION ALL
    SELECT task_id,source_role,phase,evidence_json FROM {trace_task_outcomes} WHERE trace_id=:trace
), roots AS (
    SELECT task_id,source_role,phase,evidence_json FROM evidence e
    WHERE json_extract(evidence_json,'$.parent_task_id') IS NULL
    AND json_extract(evidence_json,'$.parent_run_id') IS NULL
    AND NOT EXISTS(SELECT 1 FROM {trace_relationship_claims} c WHERE c.trace_id=:trace
        AND c.child_id='task:'||e.task_id AND c.kind!='join')
) """


def root_summary(tables: ProjectionTables, trace_id: str) -> dict:
    # Two candidates suffice to prove ambiguity; do not pick the first terminal
    # task or a disconnected graph node as a root.
    roots = tables.execute(
        _TASK_EVIDENCE + "SELECT 'task:'||task_id AS id FROM roots GROUP BY task_id "
        "UNION SELECT entity_id FROM {trace_projected_entities} "
        "WHERE trace_id=:trace AND entity_id='run:'||:trace LIMIT 2",
        {"trace": trace_id},
    ).fetchall()
    result = {"root_task_id": None, "root_agent_id": None, "outcome": None}
    if len(roots) != 1:
        return result
    identity = roots[0][0]
    if identity.startswith("task:"):
        task_id = identity.removeprefix("task:")
        result["root_task_id"] = task_id
        agents = tables.execute(
            _TASK_EVIDENCE
            + "SELECT DISTINCT json_extract(evidence_json,'$.agent_id') FROM roots "
            "WHERE task_id=:task AND source_role='recipient' "
            "AND json_extract(evidence_json,'$.agent_id') IS NOT NULL LIMIT 2",
            {"trace": trace_id, "task": task_id},
        ).fetchall()
        # Recipient terminal evidence precedes daemon recovery. Sender deadlines
        # are a transport perspective and never supply a root execution outcome.
        phases = tables.execute(
            "SELECT DISTINCT phase FROM {trace_task_outcomes} o WHERE trace_id=? AND task_id=? "
            "AND (source_role='recipient' OR (source_role='daemon' AND NOT EXISTS("
            "SELECT 1 FROM {trace_task_outcomes} r WHERE r.trace_id=o.trace_id AND r.task_id=o.task_id "
            "AND r.source_role='recipient'))) LIMIT 2",
            (trace_id, task_id),
        ).fetchall()
    else:
        agents = tables.execute(
            "SELECT DISTINCT agent_key FROM {trace_entity_observations} "
            "WHERE trace_id=? AND entity_id=? AND agent_key!='' LIMIT 2",
            (trace_id, identity),
        ).fetchall()
        phases = tables.execute(
            "SELECT DISTINCT phase FROM {trace_entity_observations} "
            "WHERE trace_id=? AND entity_id=? AND terminal=1 LIMIT 2",
            (trace_id, identity),
        ).fetchall()
    if len(agents) == 1:
        result["root_agent_id"] = agents[0][0]
    if len(phases) == 1:
        phase = "canceled" if phases[0][0] == "cancelled" else phases[0][0]
        if phase in OUTCOMES:
            result["outcome"] = phase
    return result


def _request_text(value: object, depth: int = 0) -> str | None:
    if depth > 4:
        return None
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except ValueError:
            return value.strip() or None
        if isinstance(decoded, (dict, str)):
            return _request_text(decoded, depth + 1)
        return None
    if isinstance(value, dict):
        for field in ("title", "request", "body", "prompt", "input"):
            text = _request_text(value.get(field), depth + 1)
            if text:
                return text
    return None


def task_name(tables: ProjectionTables, trace_id: str, root_task_id: str | None) -> str:
    """Derive a bounded label from retained request evidence at this snapshot.

    The initial request window keeps list reads bounded. Never label a run from
    a result or tool output, and never retain a copy after payload expiry.
    """
    rows = tables.execute(
        "SELECT ingest_seq FROM {trace_projection_run_events} WHERE trace_id=? "
        "ORDER BY ingest_seq LIMIT 32",
        (trace_id,),
    ).fetchall()
    for (seq,) in rows:
        payload = read_payload(tables.connection, seq)
        event = payload and payload["event"]
        if not event:
            continue
        if root_task_id and event["task_id"] != root_task_id:
            continue
        command = event["attributes"].get("message_type") == "command"
        native_request = event["kind"] == "run" and event["phase"] == "started"
        if not (command or native_request):
            continue
        text = _request_text(event.get("content", {}).get("fields", {}))
        if text:
            words = text.split()
            name = " ".join(words[:8])
            shortened = len(words) > 8 or len(name) > 64
            return name[:64].rstrip() + ("…" if shortened else "")
    return "Task " + (root_task_id or trace_id)[:8]
