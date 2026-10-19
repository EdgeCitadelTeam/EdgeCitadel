"""Binding-authorized child dispatch with durable decision and retry evidence."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from . import trace_capacity
from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_dispatch_request,
    validate_rpc_reply,
)
from .trace_correlation import TaskTraceContext
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore


def dispatch_trace(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    digest = hashlib.sha256(validate_dispatch_request(params)).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        now = time.time_ns() // 1_000_000
        binding, task = authorized_binding_locked(
            store,
            connector_id=connector_id,
            token=token,
            binding_id=params["binding_id"],
            now=now,
            operation="dispatch",
            delegation_allowed=True,
        )
        previous = db.execute(
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='dispatch' AND scope=? AND request_id=?",
            (connector_id, binding["binding_id"], params["request_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        # Every fresh decision retains a receipt, including permission denials.
        # Resolve authorized retries above before closing new admission.
        try:
            pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
        connector = db.execute(
            "SELECT * FROM connectors WHERE connector_id=?", (connector_id,)
        ).fetchone()
        grant_version = None
        if connector["host_type"] == "managed-agent":
            owners = [
                r
                for r in store.list_managed_agents()
                if binding["agent_id"] in cast(list[str], r.get("agent_ids", []))
            ]
            policy = "managed_package_grant"
            grant_version = hashlib.sha256(
                json.dumps(owners, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            allowed = (
                connector_id == f"managed-{binding['agent_id']}"
                and len(owners) == 1
                and owners[0].get("desired_state") == "running"
                and params["recipient_id"]
                in cast(list[str], owners[0].get("outbound_agents", []))
            )
        else:
            policy = "native_connector_capability"
            capabilities = json.loads(connector["capabilities_json"])["items"]
            grant_version = hashlib.sha256(
                canonical_bytes(sorted(capabilities))
            ).hexdigest()
            allowed = "edgecitadel_delegate" in capabilities
        root = db.execute(
            "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND json_extract(event_json,'$.execution_attempt_id')=? AND json_extract(event_json,'$.kind')='run' AND json_extract(event_json,'$.phase')='started'",
            (binding["trace_id"], binding["execution_attempt_id"]),
        ).fetchone()
        if root is None:
            raise TraceContractError("binding_evidence_missing")
        base = {
            **json.loads(root[0]),
            "event_id": str(uuid4()),
            "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
        }
        journal = TraceJournal(db)
        decision = "allowed" if allowed else "denied"
        journal.record(
            node_id,
            {
                **base,
                "kind": "permission",
                "phase": decision,
                "attributes": {
                    "dispatch_id": params["request_id"],
                    "policy": policy,
                    "policy_version": "1",
                    "grant_version": grant_version,
                    "reason": None if allowed else "permission_denied",
                },
            },
            selected=True,
        )
        journal.record(
            node_id,
            {
                **base,
                "event_id": str(uuid4()),
                "kind": "dispatch",
                "phase": decision,
                "attributes": {
                    "dispatch_id": params["request_id"],
                    "recipient_id": params["recipient_id"],
                    "skill_id": params["skill_id"],
                    "grant_version": grant_version,
                    "reason": None if allowed else "permission_denied",
                },
            },
            selected=True,
        )
        if allowed:
            child_id = str(uuid4())
            if task is None:
                context = TaskTraceContext(
                    child_id,
                    binding["context_id"] or binding["binding_id"],
                    binding["trace_id"],
                    parent_run_id=binding["trace_id"],
                    context_origin="source_explicit"
                    if binding["context_id"]
                    else "legacy_default",
                )
            else:
                saved = db.execute(
                    "SELECT context_json FROM trace_task_contexts WHERE task_id=?",
                    (task.task_id,),
                ).fetchone()
                if saved:
                    parent = replace(
                        TaskTraceContext(**json.loads(saved[0])),
                        trace_id=binding["trace_id"],
                    )
                else:
                    current = store.get_task(task.task_id)
                    if cast(dict[str, Any], current["payload"]).get("parent_task_id"):
                        raise TraceContractError("correlation_unavailable")
                    parent = TaskTraceContext(
                        task.task_id,
                        binding["context_id"] or task.task_id,
                        binding["trace_id"],
                        context_origin="source_explicit"
                        if binding["context_id"]
                        else "legacy_default",
                    )
                context = parent.child(child_id)
            payload = {
                "body": params["request"],
                "execution_context": {
                    "schema_version": 1,
                    "context_origin": context.context_origin,
                    "parent_run_id": context.parent_run_id,
                },
            }
            if context.parent_task_id is not None:
                payload["parent_task_id"] = context.parent_task_id
            store.create_task(
                task_id=child_id,
                sender_id=binding["agent_id"],
                recipient_id=params["recipient_id"],
                payload=payload,
                skill_id=params["skill_id"],
                deadline_at_ms=params["deadline_at_ms"],
                trace_id=context.trace_id,
                context_id=context.context_id,
                correlation=context,
                trace_node_id=node_id,
            )
            reply = {
                "schema_version": 1,
                "operation": "dispatch",
                "request_id": params["request_id"],
                "status": "ok",
                "result": {
                    "task_id": child_id,
                    "trace_id": context.trace_id,
                    "context_id": context.context_id,
                    "parent_task_id": context.parent_task_id,
                    "parent_run_id": context.parent_run_id,
                    "state": "queued",
                },
            }
        else:
            reply = {
                "schema_version": 1,
                "operation": "dispatch",
                "request_id": params["request_id"],
                "status": "error",
                "code": "not_authorized",
                "retryable": False,
            }
        validate_rpc_reply(reply, operation="dispatch", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'dispatch',?,?,?,?,?)",
            (
                connector_id,
                binding["binding_id"],
                params["request_id"],
                digest,
                binding["binding_id"],
                canonical_bytes(reply).decode(),
            ),
        )
        return reply
