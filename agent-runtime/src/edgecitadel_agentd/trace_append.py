"""Authorized operation boundaries committed with span state and export intent."""

from __future__ import annotations

import hashlib
import json
import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_append_request,
    validate_rpc_reply,
)
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

OPERATION_SCHEMA_SQL = """
CREATE TABLE trace_operations (
    span_id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL REFERENCES trace_bindings(binding_id),
    kind TEXT NOT NULL CHECK(kind IN ('model','tool')),
    name TEXT NOT NULL,
    parent_span_id TEXT REFERENCES trace_operations(span_id),
    started_event_id TEXT,
    terminal_event_id TEXT,
    phase TEXT NOT NULL CHECK(phase IN ('started','finished','failed','interrupted'))
);
CREATE INDEX trace_operations_binding ON trace_operations(binding_id,span_id);
"""


def append_trace(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_append_request(params)).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        binding, _task = authorized_binding_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
            operation="append",
        )
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='append' AND scope=? AND request_id=?",
            (connector_id, binding["binding_id"], params["observation_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        observation = params["observation"]
        span_id, parent = observation["span_id"], observation["parent_span_id"]
        if parent is not None:
            owner = db.execute(
                "SELECT binding_id FROM trace_operations WHERE span_id=?", (parent,)
            ).fetchone()
            if owner is None or owner[0] != binding["binding_id"]:
                raise TraceContractError("span_parent_not_owned")
        span = db.execute(
            "SELECT * FROM trace_operations WHERE span_id=?", (span_id,)
        ).fetchone()
        phase = observation["phase"]
        if span is not None:
            if (
                span["binding_id"] != binding["binding_id"]
                or span["kind"] != observation["kind"]
                or span["name"] != observation["attributes"]["name"]
                or span["parent_span_id"] != parent
            ):
                raise TraceContractError("span_identity_mismatch")
            boundary = "started_event_id" if phase == "started" else "terminal_event_id"
            if span[boundary] is not None:
                raise TraceContractError("span_boundary_conflict")
        root = db.execute(
            "SELECT event_json FROM trace_journal_all WHERE trace_id=? "
            "AND json_extract(event_json,'$.execution_attempt_id')=? "
            "AND json_extract(event_json,'$.kind')='run' AND json_extract(event_json,'$.phase')='started'",
            (binding["trace_id"], binding["execution_attempt_id"]),
        ).fetchone()
        if root is None:
            raise TraceContractError("binding_evidence_missing")
        original = json.loads(root[0])
        stamped = TraceJournal(db).record(
            node_id,
            {
                **observation,
                "event_id": str(uuid4()),
                "agent_id": binding["agent_id"],
                "trace_id": binding["trace_id"],
                "context_id": binding["context_id"],
                "task_id": binding["task_id"],
                "parent_task_id": original["parent_task_id"],
                "parent_run_id": original["parent_run_id"],
                "execution_attempt_id": binding["execution_attempt_id"],
                "evidence_kind": "integration_reported",
                "causes": [],
                "supersedes_event_id": None,
            },
            selected=True,
        )
        event_id = stamped["event_id"]
        if span is None:
            db.execute(
                "INSERT INTO trace_operations(span_id,binding_id,kind,name,parent_span_id,started_event_id,terminal_event_id,phase) VALUES (?,?,?,?,?,?,?,?)",
                (
                    span_id,
                    binding["binding_id"],
                    observation["kind"],
                    observation["attributes"]["name"],
                    parent,
                    event_id if phase == "started" else None,
                    event_id if phase != "started" else None,
                    phase,
                ),
            )
        elif phase == "started":
            # A late start fills missing evidence but does not reopen a finished operation.
            db.execute(
                "UPDATE trace_operations SET started_event_id=? WHERE span_id=?",
                (event_id, span_id),
            )
        else:
            db.execute(
                "UPDATE trace_operations SET terminal_event_id=?,phase=? WHERE span_id=?",
                (event_id, phase, span_id),
            )
        reply = {
            "schema_version": 1,
            "operation": "append",
            "request_id": params["observation_id"],
            "status": "ok",
            "result": {
                key: stamped[key] for key in ("event_id", "source_epoch", "source_seq")
            },
        }
        validate_rpc_reply(
            reply, operation="append", request_id=params["observation_id"]
        )
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'append',?,?,?,?,?)",
            (
                connector_id,
                binding["binding_id"],
                params["observation_id"],
                digest,
                binding["binding_id"],
                canonical_bytes(reply).decode(),
            ),
        )
        return reply
