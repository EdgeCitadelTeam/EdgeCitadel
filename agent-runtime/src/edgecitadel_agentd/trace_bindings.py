"""Durable run bindings. Public store entrypoint owns authentication/transaction."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from . import trace_capacity
from .trace_authority import (
    BindingAuthority,
    ExecutionAuthority,
    SessionAuthority,
    authorize_bind,
    authorize_binding_operation,
)
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_binding_request,
    validate_rpc_reply,
)
from .trace_journal import TraceJournal
from .trace_reservations import Obligation, reserve

if TYPE_CHECKING:
    from .store import AgentdStore

BINDING_SCHEMA_SQL = """
CREATE TABLE trace_bindings (
    binding_id TEXT PRIMARY KEY,
    connector_id TEXT NOT NULL REFERENCES connectors(connector_id),
    agent_id TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(session_id),
    task_id TEXT REFERENCES tasks(task_id),
    trace_id TEXT NOT NULL,
    context_id TEXT,
    execution_attempt_id TEXT NOT NULL UNIQUE,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER
);
CREATE UNIQUE INDEX trace_execution_binding ON trace_bindings(task_id,session_id) WHERE task_id IS NOT NULL;
CREATE INDEX trace_binding_session ON trace_bindings(connector_id,session_id);
CREATE TABLE trace_requests (
    connector_id TEXT NOT NULL REFERENCES connectors(connector_id),
    operation TEXT NOT NULL,
    scope TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    binding_id TEXT NOT NULL REFERENCES trace_bindings(binding_id),
    result_json TEXT NOT NULL,
    PRIMARY KEY(connector_id,operation,scope,request_id)
);
"""


def bind_trace(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """node_id is daemon configuration, never a field accepted from the caller."""
    encoded = validate_binding_request(params)
    digest = hashlib.sha256(encoded).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now_ms = time.time_ns() // 1_000_000
        connector = store.authenticate(connector_id, token)
        session_row = db.execute(
            "SELECT * FROM sessions WHERE connector_id=? AND session_id=?",
            (connector_id, params["session_id"]),
        ).fetchone()
        if session_row is None:
            raise TraceContractError("session_unavailable")
        session = SessionAuthority(
            connector_id,
            connector["agent_id"],
            params["session_id"],
            False,
            session_row["closed_at_ms"] is not None,
            session_row["lease_expires_at_ms"],
            connector["host_type"] == "managed-agent"
            or "edgecitadel_trace"
            in json.loads(connector["capabilities_json"])["items"],
        )
        authorize_bind(session, now_ms=now_ms, task=None)
        task_row = None
        execution = None
        if params["task_id"] is not None:
            task_row = db.execute(
                "SELECT * FROM tasks WHERE task_id=?", (params["task_id"],)
            ).fetchone()
            if task_row is None:
                raise TraceContractError("execution_not_owned")
            if (
                task_row["recipient_id"] != session.agent_id
                or task_row["claimed_session_id"] != session.session_id
            ):
                raise TraceContractError("execution_not_owned")
            execution = ExecutionAuthority(
                task_row["task_id"],
                task_row["recipient_id"],
                task_row["claimed_session_id"],
                task_row["state"],
            )
            if (
                params["context_id"] is not None
                and params["context_id"] != task_row["context_id"]
            ):
                raise TraceContractError("binding_context_mismatch")
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='bind' AND scope='' AND request_id=?",
            (connector_id, params["request_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            row = db.execute(
                "SELECT * FROM trace_bindings_all WHERE binding_id=?",
                (previous["binding_id"],),
            ).fetchone()
            authorize_binding_operation(
                session,
                BindingAuthority(
                    row["connector_id"],
                    row["agent_id"],
                    row["session_id"],
                    row["task_id"],
                    row["closed_at_ms"] is not None,
                ),
                now_ms=now_ms,
                task=execution,
                operation="append",
            )
            return json.loads(previous["result_json"])
        authorize_bind(session, now_ms=now_ms, task=execution)
        existing = (
            None
            if execution is None
            else db.execute(
                "SELECT * FROM trace_bindings_all WHERE task_id=? AND session_id=?",
                (execution.task_id, session.session_id),
            ).fetchone()
        )
        if existing is not None and existing["closed_at_ms"] is not None:
            raise TraceContractError("binding_closed")
        if existing is not None and existing["connector_id"] != connector_id:
            raise TraceContractError("binding_not_owned")
        # A fresh request consumes a durable receipt even when it reuses an
        # execution binding. Existing authorized retries returned above.
        try:
            pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
        if existing is None:
            saved_context = db.execute(
                "SELECT context_json FROM trace_task_contexts WHERE task_id=?",
                (params["task_id"],),
            ).fetchone()
            correlation = json.loads(saved_context[0]) if saved_context else None
            result = {
                "binding_id": str(uuid4()),
                "trace_id": task_row["trace_id"] if task_row else uuid4().hex,
                "task_id": params["task_id"],
                "context_id": correlation["context_id"]
                if correlation
                else (task_row["context_id"] if task_row else params["context_id"]),
                "execution_attempt_id": str(uuid4()),
            }
            db.execute(
                "INSERT INTO trace_bindings(binding_id,connector_id,agent_id,session_id,task_id,trace_id,context_id,execution_attempt_id,opened_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    result["binding_id"],
                    connector_id,
                    session.agent_id,
                    session.session_id,
                    result["task_id"],
                    result["trace_id"],
                    result["context_id"],
                    result["execution_attempt_id"],
                    now_ms,
                ),
            )
            if db.workspace is not None:
                reserve(db, Obligation("run", result["binding_id"], "terminal"))
            payload = (
                store._decode_content(task_row["payload_json"]) if task_row else {}
            )
            metadata = payload.get("execution_context", {})
            if not isinstance(metadata, dict):
                raise TraceContractError("unsupported_execution_context")
            TraceJournal(db).record(
                node_id,
                {
                    "schema_version": 1,
                    "event_id": str(uuid4()),
                    "agent_id": session.agent_id,
                    "trace_id": result["trace_id"],
                    "task_id": result["task_id"],
                    "context_id": result["context_id"],
                    "parent_task_id": correlation["parent_task_id"]
                    if correlation
                    else payload.get("parent_task_id"),
                    "parent_run_id": correlation["parent_run_id"]
                    if correlation
                    else metadata.get("parent_run_id"),
                    "execution_attempt_id": result["execution_attempt_id"],
                    "span_id": None,
                    "parent_span_id": None,
                    "kind": "run",
                    "phase": "started",
                    "occurred_at": datetime.fromtimestamp(now_ms / 1000, UTC)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    "duration_ms": None,
                    "evidence_kind": "source_observed",
                    "causes": [],
                    "attributes": {},
                    "supersedes_event_id": None,
                },
                selected=True,
            )
        else:
            result = {
                key: existing[key]
                for key in (
                    "binding_id",
                    "trace_id",
                    "task_id",
                    "context_id",
                    "execution_attempt_id",
                )
            }
        reply = {
            "schema_version": 1,
            "operation": "bind",
            "request_id": params["request_id"],
            "status": "ok",
            "result": result,
        }
        validate_rpc_reply(reply, operation="bind", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'bind','',?,?,?,?)",
            (
                connector_id,
                params["request_id"],
                digest,
                result["binding_id"],
                canonical_bytes(reply).decode(),
            ),
        )
        return reply
