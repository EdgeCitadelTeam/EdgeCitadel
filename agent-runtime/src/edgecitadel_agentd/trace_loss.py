"""Durable, idempotent reports of producer-side observation uncertainty."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from . import trace_capacity
from .trace_binding_access import authorized_binding_locked
from .trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_loss_request,
    validate_rpc_reply,
)
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore


def report_loss(
    store: AgentdStore,
    *,
    node_id: str,
    connector_id: str,
    token: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    encoded = validate_loss_request(params)
    digest = hashlib.sha256(encoded).hexdigest()
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
            "SELECT * FROM trace_requests_all WHERE connector_id=? AND operation='loss' AND scope=? AND request_id=?",
            (connector_id, binding["binding_id"], params["request_id"]),
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            return json.loads(previous["result_json"])
        # New reports retain control payload and retry identity. Existing
        # authorized receipts above remain usable when fresh admission closes.
        try:
            pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
        except OSError as error:
            raise TraceContractError("storage_unavailable") from error
        if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
            raise TraceContractError("quota_exceeded")
        root = db.execute(
            "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND json_extract(event_json,'$.execution_attempt_id')=? AND json_extract(event_json,'$.kind')='run' AND json_extract(event_json,'$.phase')='started'",
            (binding["trace_id"], binding["execution_attempt_id"]),
        ).fetchone()
        if root is None:
            raise TraceContractError("binding_evidence_missing")
        journal = TraceJournal(db)
        epoch, generation = journal.initialize(node_id)
        through = db.execute(
            "SELECT next_export_seq-1 FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
            (node_id, epoch, generation),
        ).fetchone()[0]
        event = journal.record(
            node_id,
            {
                **json.loads(root[0]),
                "event_id": str(uuid4()),
                "kind": "coverage",
                "phase": "unknown",
                "span_id": None,
                "parent_span_id": None,
                "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "duration_ms": None,
                "evidence_kind": "integration_reported",
                "attributes": {
                    "export_generation": generation,
                    "through_export_seq": through,
                    "producer_id": params["producer_id"],
                    "dropped_observations": params["dropped_observations"],
                    "reason": "unknown",
                },
            },
            selected=True,
        )
        reply = {
            "schema_version": 1,
            "operation": "loss",
            "request_id": params["request_id"],
            "status": "ok",
            "result": {
                key: event[key] for key in ("event_id", "source_epoch", "source_seq")
            },
        }
        validate_rpc_reply(reply, operation="loss", request_id=params["request_id"])
        db.execute(
            "INSERT INTO trace_requests(connector_id,operation,scope,request_id,request_sha256,binding_id,result_json) VALUES (?,'loss',?,?,?,?,?)",
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
