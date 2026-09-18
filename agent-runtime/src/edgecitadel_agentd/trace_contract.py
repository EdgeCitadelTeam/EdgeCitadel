"""Bounded v1 trace records; schema validity does not establish actor authority."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

MAX_EVENT_BYTES = 16 * 1024
MAX_WRAPPER_BYTES = 18 * 1024
MAX_DEPTH = 16
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
TASK_DISPLAY_STATES = {
    "created": "created",
    "queued": "queued",
    "offered": "offered",
    "accepted": "accepted",
    "running": "running",
    "completed": "completed",
    "failed": "failed",
    "rejected": "rejected",
    "cancelled": "canceled",
    "expired": "expired",
    "undeliverable": "undeliverable",
}


class TraceContractError(ValueError):
    """Stable, bounded diagnostic that never echoes caller content."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validators() -> dict[str, Draft202012Validator]:
    configured = os.environ.get("EDGECITADEL_SCHEMA_DIR")
    directory = (
        Path(configured)
        if configured
        else Path(__file__).resolve().parents[3] / "schemas"
    )
    schemas = {
        kind: json.loads((directory / f"trace-{kind}.v1.json").read_text())
        for kind in (
            "capability",
            "binding-request",
            "append-request",
            "finish-request",
            "loss-request",
            "dispatch-request",
            "import-request",
            "rpc-reply",
            "event",
            "export",
            "settlement",
            "settlement-request",
            "settlement-reply",
            "cursor",
            "read",
        )
    }
    # Resolve the one external schema from our bundled source, without network
    # retrieval or duplicating the event contract in the checked-in read schema.
    schemas["read"]["$defs"]["event"] = schemas["event"]
    schemas["settlement-reply"]["$defs"]["checkpoint"] = schemas["settlement"]
    return {
        kind: Draft202012Validator(schema, format_checker=FormatChecker())
        for kind, schema in schemas.items()
    }


def _compile_event_validators(
    schema: dict[str, Any],
) -> dict[str, Draft202012Validator]:
    """Partially evaluate only exact, else-free kind conditions from the schema.

    Every common constraint and unrecognized conditional stays in the resulting
    schema. Unknown or malformed kinds use the unchanged full validator.
    """
    families = schema.get("properties", {}).get("kind", {}).get("enum", [])
    conditions = {
        family: {"properties": {"kind": {"const": family}}}
        for family in families
        if isinstance(family, str)
    }
    result = {}
    for family, condition in conditions.items():
        applicable = []
        for rule in schema.get("allOf", []):
            if (
                isinstance(rule, dict)
                and set(rule) == {"if", "then"}
                and rule["if"] in conditions.values()
            ):
                if rule["if"] == condition:
                    applicable.append(rule["then"])
            else:
                applicable.append(rule)
        result[family] = Draft202012Validator(
            {**schema, "allOf": applicable}, format_checker=FormatChecker()
        )
    return result


_VALIDATORS = _validators()
_EVENT_VALIDATORS = _compile_event_validators(
    cast(dict[str, Any], _VALIDATORS["event"].schema)
)
_EXPORT_SCHEMA = cast(dict[str, Any], _VALIDATORS["export"].schema)
_EXPORT_HEADER_SCHEMA = {
    **_EXPORT_SCHEMA,
    "properties": {
        **_EXPORT_SCHEMA["properties"],
        "event": {"type": "object"},
    },
}
_VALIDATORS["export_header"] = Draft202012Validator(_EXPORT_HEADER_SCHEMA)


def canonical_bytes(value: object, *, limit: int = MAX_EVENT_BYTES) -> bytes:
    """v1 canonical JSON: UTF-8, sorted keys, no whitespace or floats.

    Reject non-JSON values and excessive depth before recursive serialization.
    All v1 numeric fields are integers; Unicode is preserved without normalization.
    """
    pending = [(value, 1)]
    visited = 0
    while pending:
        item, depth = pending.pop()
        visited += 1
        if depth > MAX_DEPTH:
            raise TraceContractError("excessive_depth")
        # Each JSON node takes at least one byte. This also bounds cyclic input
        # traversal and oversized container work before encoding.
        if visited > limit:
            raise TraceContractError("oversize_record")
        if type(item) is dict:
            if len(item) > limit or any(type(key) is not str for key in item):
                raise TraceContractError("invalid_json")
            if any(len(key) > limit for key in item):
                raise TraceContractError("oversize_record")
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) is list:
            if len(item) > limit:
                raise TraceContractError("oversize_record")
            pending.extend((child, depth + 1) for child in item)
        elif item is not None and type(item) not in (str, int, bool):
            raise TraceContractError("invalid_json")
        elif type(item) is str and len(item) > limit:
            raise TraceContractError("oversize_record")
        elif type(item) is int and abs(item) > 9_007_199_254_740_991:
            raise TraceContractError("invalid_integer")
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, UnicodeError) as error:
        raise TraceContractError("invalid_json") from error
    if len(encoded) > limit:
        raise TraceContractError("oversize_record")
    return encoded


def _validate(kind: str, value: object, limit: int) -> bytes:
    encoded = canonical_bytes(value, limit=limit)
    if type(value) is not dict:
        raise TraceContractError(f"invalid_{kind}")
    if type(value.get("schema_version")) is not int:
        raise TraceContractError(f"invalid_{kind}")
    if value["schema_version"] != 1:
        raise TraceContractError(f"unsupported_{kind}_version")
    validator = _VALIDATORS[kind]
    if kind == "event" and isinstance(value.get("kind"), str):
        validator = _EVENT_VALIDATORS.get(value["kind"], validator)
    if not validator.is_valid(value):
        raise TraceContractError(f"invalid_{kind}")
    return encoded


def _ranges(ranges: list[dict[str, int]], through: int) -> None:
    previous = 0
    for item in ranges:
        if not previous < item["first"] <= item["last"] <= through:
            raise TraceContractError("invalid_ranges")
        previous = item["last"]


def validate_event(event: dict[str, Any]) -> bytes:
    encoded = _validate("event", event, MAX_EVENT_BYTES)
    if event["supersedes_event_id"] == event["event_id"]:
        raise TraceContractError("self_supersession")
    if event["task_id"] is not None and event["task_id"] == event["parent_task_id"]:
        raise TraceContractError("self_parent")
    if event["span_id"] is not None and event["span_id"] == event["parent_span_id"]:
        raise TraceContractError("self_parent")
    if event["parent_task_id"] is not None and event["parent_run_id"] is not None:
        raise TraceContractError("ambiguous_parent")
    if (
        event["evidence_kind"] == "compatibility_synthesized"
        and event["duration_ms"] is not None
    ):
        raise TraceContractError("synthetic_duration")
    if event["kind"] == "coverage":
        attributes = event["attributes"]
        _ranges(attributes.get("lost_ranges", []), attributes["through_export_seq"])
        if event["phase"] == "lost" and not attributes.get("lost_ranges"):
            raise TraceContractError("missing_loss_ranges")
    if event["kind"] == "model":
        attributes = event["attributes"]
        missing_usage = any(
            attributes[key] is None for key in ("input_tokens", "output_tokens")
        )
        if missing_usage != (attributes["usage_unavailable_reason"] is not None):
            raise TraceContractError("inconsistent_usage_coverage")
    if event["kind"] == "link":
        attributes = event["attributes"]
        if attributes["from_task_id"] == attributes["to_task_id"]:
            raise TraceContractError("invalid_join")
    return encoded


def event_sha256(event: dict[str, Any]) -> str:
    return hashlib.sha256(validate_event(event)).hexdigest()


def validate_export_header(record: dict[str, Any]) -> bytes:
    """Validate a source position even when the enclosed payload is unsupported.

    Callers must still validate_event before ingestion as an accepted event.
    A header failure must never advance source settlement.
    """
    encoded = _validate("export_header", record, MAX_WRAPPER_BYTES)
    raw_hash = hashlib.sha256(
        canonical_bytes(record["event"], limit=MAX_WRAPPER_BYTES)
    ).hexdigest()
    if record["event_sha256"] != raw_hash:
        raise TraceContractError("hash_mismatch")
    return encoded


def validate_export(record: dict[str, Any]) -> bytes:
    encoded = validate_export_header(record)
    event = record["event"]
    event_encoded = validate_event(event)
    if any(record[key] != event[key] for key in ("node_id", "source_epoch")):
        raise TraceContractError("origin_mismatch")
    if record["event_sha256"] != hashlib.sha256(event_encoded).hexdigest():
        raise TraceContractError("hash_mismatch")
    return encoded


def validate_settlement(checkpoint: dict[str, Any]) -> bytes:
    encoded = _validate("settlement", checkpoint, MAX_WRAPPER_BYTES)
    through = checkpoint["settled_export_seq"]
    _ranges(checkpoint["rejected_ranges"], through)
    _ranges(checkpoint["lost_ranges"], through)
    combined = sorted(
        checkpoint["rejected_ranges"] + checkpoint["lost_ranges"],
        key=lambda item: item["first"],
    )
    _ranges(combined, through)
    return encoded


def validate_trace_capability(extension: dict[str, Any]) -> bytes:
    encoded = canonical_bytes(extension, limit=4096)
    if not _VALIDATORS["capability"].is_valid(extension):
        raise TraceContractError("invalid_capability")
    return encoded


def validate_binding_request(request: dict[str, Any]) -> bytes:
    return _validate("binding-request", request, 1024)


def validate_append_request(request: dict[str, Any]) -> bytes:
    encoded = _validate("append-request", request, MAX_EVENT_BYTES)
    observation = request["observation"]
    if observation["span_id"] is None:
        raise TraceContractError("missing_span_id")
    if observation["span_id"] == observation["parent_span_id"]:
        raise TraceContractError("self_parent")
    if observation["kind"] == "model":
        attrs = observation["attributes"]
        missing = any(attrs[key] is None for key in ("input_tokens", "output_tokens"))
        if missing != (attrs["usage_unavailable_reason"] is not None):
            raise TraceContractError("inconsistent_usage_coverage")
    return encoded


def validate_dispatch_request(request: dict[str, Any]) -> bytes:
    # Commands can contain raw user content locally; this request must never be
    # copied into exported event attributes. Unicode can occupy four UTF-8 bytes.
    return _validate("dispatch-request", request, 68 * 1024)


def validate_finish_request(request: dict[str, Any]) -> bytes:
    return _validate("finish-request", request, 1024)


def validate_loss_request(request: dict[str, Any]) -> bytes:
    return _validate("loss-request", request, 1024)


def validate_import_request(request: dict[str, Any]) -> bytes:
    encoded = _validate("import-request", request, MAX_EVENT_BYTES)
    observation = request["observation"]
    # Validate semantic relationships as well as shape, using temporary identity
    # solely for validation. The recorder supplies real immutable identity later.
    candidate = {
        **observation,
        "event_id": request["record_id"],
        "node_id": request["import_source_id"],
        "source_epoch": request["historical_run_id"],
        "source_seq": 1,
        "agent_id": request["agent_id"],
        "trace_id": request["historical_run_id"].replace("-", ""),
        "evidence_kind": "historical_import",
        "causes": [],
        "supersedes_event_id": None,
    }
    validate_event(candidate)
    return encoded


def validate_rpc_reply(
    reply: dict[str, Any], *, operation: str, request_id: str
) -> bytes:
    encoded = _validate("rpc-reply", reply, 2048)
    if reply["operation"] != operation or reply["request_id"] != request_id:
        raise TraceContractError("rpc_reply_mismatch")
    if reply["status"] == "ok" and operation == "dispatch":
        result = reply["result"]
        parent_task, parent_run = result["parent_task_id"], result["parent_run_id"]
        if (
            (parent_task is None) == (parent_run is None)
            or parent_task == result["task_id"]
            or (parent_run is not None and parent_run != result["trace_id"])
        ):
            raise TraceContractError("invalid_dispatch_parent")
    if reply["status"] == "error" and reply["retryable"] != (
        reply["code"] == "storage_unavailable"
    ):
        raise TraceContractError("invalid_retryability")
    return encoded


def validate_settlement_request(request: dict[str, Any]) -> bytes:
    return _validate("settlement-request", request, 1024)


def validate_settlement_reply(
    reply: dict[str, Any], *, request: dict[str, Any]
) -> bytes:
    """Bind a checkpoint to its request; this does not authenticate the Core.

    Errors never authorize retirement. Collector epoch comparison and durable
    application of successful checkpoints belong to the exporter transaction.
    """
    validate_settlement_request(request)
    encoded = _validate("settlement-reply", reply, MAX_WRAPPER_BYTES)
    if reply["request_id"] != request["request_id"]:
        raise TraceContractError("settlement_request_mismatch")
    if reply["status"] == "ok":
        checkpoint = reply["checkpoint"]
        validate_settlement(checkpoint)
        if any(
            checkpoint[key] != request[key]
            for key in ("node_id", "source_epoch", "export_generation")
        ):
            raise TraceContractError("settlement_origin_mismatch")
    return encoded


def validate_cursor_claims(claims: dict[str, Any]) -> bytes:
    encoded = _validate("cursor", claims, 2048)
    kind = claims["kind"]
    if kind in {"list", "changes"} and not (
        claims["position"] <= claims["snapshot"] == claims["upper"]
    ):
        raise TraceContractError("invalid_cursor")
    if kind == "graph" and claims["position"] != claims["snapshot"]:
        raise TraceContractError("invalid_cursor")
    if kind == "events" and claims["position"] > claims["upper"]:
        raise TraceContractError("invalid_cursor")
    return encoded


def validate_read_response(response: dict[str, Any]) -> bytes:
    encoded = _validate("read", response, MAX_RESPONSE_BYTES)
    kind = response["kind"]
    if kind == "trace_events":
        for event in response["events"]:
            validate_event(event)
            if event["trace_id"] != response["trace_id"]:
                raise TraceContractError("response_scope_mismatch")
    if kind == "trace_graph":
        nodes = response["nodes"]
        ids = {node["id"] for node in nodes}
        if len(ids) != len(nodes):
            raise TraceContractError("duplicate_graph_identity")
        edge_ids = {edge["id"] for edge in response["edges"]}
        if len(edge_ids) != len(response["edges"]):
            raise TraceContractError("duplicate_graph_identity")
        if any(
            edge["from"] not in ids or edge["to"] not in ids
            for edge in response["edges"]
        ):
            raise TraceContractError("missing_graph_endpoint")
        total = response["total_nodes"]
        if total is not None and (
            total < len(nodes)
            or (
                total > len(nodes)
                and not response["expansions"]
                and response["page_kind"] == "snapshot"
            )
        ):
            raise TraceContractError("unreachable_graph_expansion")
        if any(expansion["node_id"] not in ids for expansion in response["expansions"]):
            raise TraceContractError("missing_graph_endpoint")
        for node in nodes:
            if (
                node["kind"] == "task"
                and node["original_state"] is not None
                and node["state"] != TASK_DISPLAY_STATES[node["original_state"]]
            ):
                raise TraceContractError("invalid_state_mapping")
            if node["conflict"] and node["outcome_candidate_count"] < 2:
                raise TraceContractError("invalid_outcome_conflict")
        # Joins express dependency, never task/span nesting. Invalid observed
        # edges may remain visible for diagnosis but cannot enter ancestry.
        parents: dict[str, str] = {}
        for edge in response["edges"]:
            if edge["kind"] == "join" or edge["status"] != "resolved":
                continue
            if edge["to"] in parents and parents[edge["to"]] != edge["from"]:
                raise TraceContractError("ambiguous_graph_parent")
            parents[edge["to"]] = edge["from"]
        for start in parents:
            seen: set[str] = set()
            current = start
            while current in parents:
                if current in seen:
                    raise TraceContractError("cyclic_graph_ancestry")
                seen.add(current)
                current = parents[current]
    if kind == "trace_error":
        resnapshot = response["code"] in {"generation_changed", "history_expired"}
        if response["resnapshot_required"] != resnapshot:
            raise TraceContractError("invalid_read")
    return encoded


def coverage_scope(event: dict[str, Any]) -> tuple[str, str, str]:
    """Return the affected export scope, distinct from the marker's own origin."""
    validate_event(event)
    if event["kind"] != "coverage":
        raise TraceContractError("coverage_event_required")
    return (
        str(event["node_id"]),
        str(event["attributes"].get("affected_source_epoch", event["source_epoch"])),
        str(event["attributes"]["export_generation"]),
    )
