"""Stable identities for historical evidence, isolated from executable task IDs."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import TYPE_CHECKING, Any

from . import trace_capacity
from .trace_authority import ImportAuthority, authorize_historical_import
from .trace_contract import validate_import_request, validate_rpc_reply
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

from .trace_contract import TraceContractError, canonical_bytes

_TYPES = frozenset({"run", "task", "context", "attempt", "span", "record"})


def historical_identity(
    *,
    namespace_id: str,
    import_source_id: str,
    agent_id: str,
    historical_run_id: str,
    kind: str,
    original_id: str,
) -> str:
    """Return a deterministic UUID-shaped identity in a persisted import namespace.

    The administrator grant owns namespace_id; request callers cannot choose it.
    It must survive backup/retry. The result is evidence identity, never a task
    capability. A run identity is rendered without hyphens for trace_id fields.
    """
    try:
        parsed = uuid.UUID(namespace_id)
    except (ValueError, AttributeError) as error:
        raise TraceContractError("invalid_import_namespace") from error
    if str(parsed) != namespace_id or kind not in _TYPES:
        raise TraceContractError("invalid_import_identity")
    fields = [import_source_id, agent_id, historical_run_id, original_id]
    if any(type(value) is not str or not 1 <= len(value) <= 128 for value in fields):
        raise TraceContractError("invalid_import_identity")
    digest = hashlib.sha256(
        b"edgecitadel-historical-identity-v1\0"
        + canonical_bytes([namespace_id, *fields[:3], kind, original_id])
    ).digest()
    # Existing v1 identity schemas accept UUIDv4 shape. Set its version/variant
    # bits explicitly; this is a deterministic hash ID, not a random credential.
    return str(uuid.UUID(bytes=digest[:16], version=4))


IMPORT_SCHEMA_SQL = """
CREATE TABLE trace_import_grants (
    import_source_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    namespace_id TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
    PRIMARY KEY(import_source_id,agent_id)
);
CREATE TABLE trace_import_records (
    namespace_id TEXT NOT NULL REFERENCES trace_import_grants(namespace_id),
    historical_run_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    PRIMARY KEY(namespace_id,historical_run_id,record_id)
);
"""


def _admit_metadata(store: AgentdStore) -> None:
    try:
        pressure = trace_capacity.physical_storage(store._connection)["pressure_bytes"]
    except OSError as error:
        raise TraceContractError("storage_unavailable") from error
    if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
        raise TraceContractError("quota_exceeded")


def configure_import(
    store: AgentdStore, *, administrator_authenticated: bool, params: dict[str, Any]
) -> dict[str, Any]:
    """Internal management entrypoint; service establishes administrator authority."""
    if not administrator_authenticated:
        raise TraceContractError("import_not_authorized")
    if (
        set(params) != {"import_source_id", "agent_id", "enabled"}
        or any(
            not isinstance(params.get(key), str)
            or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", params[key]) is None
            for key in ("import_source_id", "agent_id")
        )
        or type(params["enabled"]) is not bool
    ):
        raise TraceContractError("invalid_import_grant")
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        scope = (params["import_source_id"], params["agent_id"])
        row = db.execute(
            "SELECT namespace_id,enabled FROM trace_import_grants "
            "WHERE import_source_id=? AND agent_id=?",
            scope,
        ).fetchone()
        if row is None:
            _admit_metadata(store)
            db.execute(
                "INSERT INTO trace_import_grants VALUES (?,?,?,?)",
                (*scope, str(uuid.uuid4()), params["enabled"]),
            )
        elif bool(row["enabled"]) != params["enabled"]:
            # Revocation must remain possible under optional admission pressure.
            db.execute(
                "UPDATE trace_import_grants SET enabled=? "
                "WHERE import_source_id=? AND agent_id=?",
                (params["enabled"], *scope),
            )
        return dict(params)


def import_trace(
    store: AgentdStore,
    *,
    node_id: str,
    administrator_authenticated: bool,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Commit archive evidence and its durable retry identity; never executable work."""
    if not administrator_authenticated:
        raise TraceContractError("import_not_authorized")
    validate_import_request(params)
    digest = hashlib.sha256(
        canonical_bytes(
            {key: value for key, value in params.items() if key != "request_id"}
        )
    ).hexdigest()
    db = store._connection
    with store._lock, db:
        db.execute("BEGIN IMMEDIATE")
        grant = db.execute(
            "SELECT * FROM trace_import_grants WHERE import_source_id=? AND agent_id=?",
            (params["import_source_id"], params["agent_id"]),
        ).fetchone()
        if grant is None:
            raise TraceContractError("import_not_authorized")
        authorize_historical_import(
            ImportAuthority(
                administrator_authenticated,
                bool(grant["enabled"]),
                grant["import_source_id"],
                frozenset({grant["agent_id"]}),
            ),
            source_id=params["import_source_id"],
            agent_id=params["agent_id"],
        )
        key = (grant["namespace_id"], params["historical_run_id"], params["record_id"])
        previous = db.execute(
            "SELECT request_sha256,receipt_json FROM trace_import_records "
            "WHERE namespace_id=? AND historical_run_id=? AND record_id=?",
            key,
        ).fetchone()
        if previous is not None:
            if previous["request_sha256"] != digest:
                raise TraceContractError("idempotency_conflict")
            receipt = json.loads(previous["receipt_json"])
        else:
            _admit_metadata(store)

            def mapped(kind: str, original: str | None) -> str | None:
                if original is None:
                    return None
                return historical_identity(
                    namespace_id=grant["namespace_id"],
                    import_source_id=params["import_source_id"],
                    agent_id=params["agent_id"],
                    historical_run_id=params["historical_run_id"],
                    kind=kind,
                    original_id=original,
                )

            observation = params["observation"]
            run = historical_identity(
                namespace_id=grant["namespace_id"],
                import_source_id=params["import_source_id"],
                agent_id=params["agent_id"],
                historical_run_id=params["historical_run_id"],
                kind="run",
                original_id=params["historical_run_id"],
            ).replace("-", "")
            attributes = dict(observation["attributes"])
            # Archive references do not resolve to this daemon's encrypted content.
            if "local_content_ref" in attributes or "content_available" in attributes:
                attributes.pop("local_content_ref", None)
                attributes["content_available"] = False
            event = {
                **observation,
                "attributes": attributes,
                "event_id": mapped("record", params["record_id"]),
                "agent_id": params["agent_id"],
                "trace_id": run,
                "evidence_kind": "historical_import",
                "causes": [],
                "supersedes_event_id": None,
            }
            for field, kind in (
                ("task_id", "task"),
                ("parent_task_id", "task"),
                ("context_id", "context"),
                ("execution_attempt_id", "attempt"),
                ("span_id", "span"),
                ("parent_span_id", "span"),
            ):
                event[field] = mapped(kind, observation[field])
            # Only the current archive run has an explicitly resolved scope.
            event["parent_run_id"] = (
                run
                if observation["parent_run_id"]
                == params["historical_run_id"].replace("-", "")
                else None
            )
            stamped = TraceJournal(db).record(node_id, event, selected=True)
            # Imports of run/task evidence must not consume the control reserve.
            used = db.execute(
                "SELECT event_bytes FROM trace_storage_usage WHERE singleton=1"
            ).fetchone()[0]
            if used > trace_capacity.NORMAL_LIMIT_BYTES:
                raise TraceContractError("quota_exceeded")
            receipt = {
                name: stamped[name]
                for name in ("event_id", "source_epoch", "source_seq")
            }
            db.execute(
                "INSERT INTO trace_import_records VALUES (?,?,?,?,?)",
                (*key, digest, canonical_bytes(receipt).decode()),
            )
        reply = {
            "schema_version": 1,
            "operation": "import",
            "request_id": params["request_id"],
            "status": "ok",
            "result": receipt,
        }
        validate_rpc_reply(reply, operation="import", request_id=params["request_id"])
        return reply
