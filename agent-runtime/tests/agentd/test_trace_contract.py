from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from edgecitadel_agentd.trace_contract import (
    MAX_EVENT_BYTES,
    MAX_WRAPPER_BYTES,
    TraceContractError,
    canonical_bytes,
    event_sha256,
    validate_event,
    validate_export,
    validate_export_header,
    validate_settlement,
)

FIXTURES = json.loads(
    (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
)["fixtures"]


def event(kind: str = "task") -> dict:
    return deepcopy(next(item["event"] for item in FIXTURES if item["name"] == kind))


def export(value: dict) -> dict:
    return {
        "schema_version": 1,
        "node_id": value["node_id"],
        "source_epoch": value["source_epoch"],
        "export_generation": "00000000-0000-4000-8000-000000000800",
        "export_seq": 1,
        "event_sha256": hashlib.sha256(canonical_bytes(value)).hexdigest(),
        "event": value,
    }


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda fixture: fixture["name"])
def test_golden_events_have_stable_content_identity(fixture: dict) -> None:
    value = fixture["event"]
    assert event_sha256(value) == fixture["sha256"]
    assert event_sha256(dict(reversed(list(value.items())))) == fixture["sha256"]
    assert json.loads(validate_export(export(value)))["event"] == value


def test_export_retry_generation_does_not_change_event_identity() -> None:
    first = export(event())
    second = deepcopy(first)
    second["export_generation"] = "00000000-0000-4000-8000-000000000801"
    second["export_seq"] = 9
    assert validate_export(first) != validate_export(second)
    assert first["event_sha256"] == second["event_sha256"]
    second["event"]["phase"] = "completed"
    with pytest.raises(TraceContractError, match="^hash_mismatch$"):
        validate_export(second)


def test_valid_wrapper_can_identify_rejected_unsupported_payload() -> None:
    value = event()
    value["schema_version"] = 2
    record = export(value)
    validate_export_header(record)
    with pytest.raises(TraceContractError, match="^unsupported_event_version$"):
        validate_export(record)
    record["schema_version"] = 2
    with pytest.raises(TraceContractError, match="^unsupported_export_header_version$"):
        validate_export_header(record)


def test_valid_wrapper_can_identify_an_oversized_event_for_rejection() -> None:
    value = event()
    record = export(value)
    value["untrusted"] = "x" * MAX_EVENT_BYTES
    record["event_sha256"] = hashlib.sha256(
        canonical_bytes(value, limit=MAX_WRAPPER_BYTES)
    ).hexdigest()
    validate_export_header(record)
    with pytest.raises(TraceContractError, match="^oversize_record$"):
        validate_export(record)


@pytest.mark.parametrize(
    "key", ["prompt", "tool_arguments", "summary", "error_message"]
)
def test_free_text_and_raw_content_have_no_export_attribute_escape(key: str) -> None:
    value = event("tool")
    value["attributes"][key] = "SENTINEL_PRIVATE_BODY"
    with pytest.raises(TraceContractError) as result:
        validate_event(value)
    assert str(result.value) == "invalid_event"
    assert "SENTINEL" not in str(result.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("extra", "private"),
        ("occurred_at", "2026-02-30T00:00:00.000Z"),
        ("event_id", "not-a-uuid"),
        ("source_seq", True),
        ("schema_version", True),
        ("phase", "tool_finished"),
    ],
)
def test_invalid_event_fields_are_rejected(key: str, value: object) -> None:
    record = event()
    record[key] = value
    with pytest.raises(TraceContractError, match="^invalid_event$"):
        validate_event(record)


def test_origin_and_self_parent_cannot_pass_structural_validation() -> None:
    record = export(event())
    record["node_id"] = "edge-b"
    with pytest.raises(TraceContractError, match="^origin_mismatch$"):
        validate_export(record)
    value = event()
    value["parent_task_id"] = value["task_id"]
    with pytest.raises(TraceContractError, match="^self_parent$"):
        validate_event(value)
    value["parent_task_id"] = None
    value["evidence_kind"] = "compatibility_synthesized"
    value["duration_ms"] = 50
    with pytest.raises(TraceContractError, match="^synthetic_duration$"):
        validate_event(value)


@pytest.mark.parametrize(
    "key", ["event_id", "node_id", "agent_id", "trace_id", "occurred_at"]
)
def test_identity_patterns_reject_trailing_newline(key: str) -> None:
    value = event()
    value[key] += "\n"
    with pytest.raises(TraceContractError, match="^invalid_event$"):
        validate_event(value)


def test_byte_depth_and_non_json_boundaries() -> None:
    # The cap counts UTF-8 bytes, including JSON's quotes.
    value = "é" * ((MAX_EVENT_BYTES - 2) // 2)
    assert len(canonical_bytes(value)) == MAX_EVENT_BYTES
    with pytest.raises(TraceContractError, match="^oversize_record$"):
        canonical_bytes(value + "a")
    nested: object = None
    for _ in range(15):
        nested = [nested]
    canonical_bytes(nested)
    with pytest.raises(TraceContractError, match="^excessive_depth$"):
        canonical_bytes([nested])
    cyclic = []
    cyclic.append(cyclic)
    with pytest.raises(TraceContractError, match="^excessive_depth$"):
        canonical_bytes(cyclic)
    for invalid in (float("nan"), 1.0, {1: "key"}, {"x": object()}, "\ud800"):
        with pytest.raises(TraceContractError):
            canonical_bytes(invalid)


def test_missing_usage_is_explicit_and_separate_from_measured_zero() -> None:
    value = event("model")
    value["attributes"]["input_tokens"] = None
    with pytest.raises(TraceContractError, match="^inconsistent_usage_coverage$"):
        validate_event(value)
    value["attributes"]["usage_unavailable_reason"] = "not_reported"
    validate_event(value)
    value["attributes"]["input_tokens"] = 0
    with pytest.raises(TraceContractError, match="^inconsistent_usage_coverage$"):
        validate_event(value)
    value["attributes"]["usage_unavailable_reason"] = None
    validate_event(value)


def checkpoint() -> dict:
    return {
        "schema_version": 1,
        "node_id": "edge-a",
        "source_epoch": "00000000-0000-4000-8000-000000000100",
        "export_generation": "00000000-0000-4000-8000-000000000600",
        "collector_epoch": "00000000-0000-4000-8000-000000000900",
        "settled_export_seq": 5,
        "rejected_ranges": [{"first": 2, "last": 2}],
        "lost_ranges": [{"first": 3, "last": 4}],
    }


def test_settlement_ranges_are_bounded_disjoint_and_ordered() -> None:
    validate_settlement(checkpoint())
    for ranges in (
        [{"first": 2, "last": 3}],
        [{"first": 3, "last": 6}],
        [{"first": 4, "last": 3}],
        [{"first": 4, "last": 4}, {"first": 3, "last": 3}],
    ):
        value = checkpoint()
        value["lost_ranges"] = ranges
        with pytest.raises(TraceContractError, match="^invalid_ranges$"):
            validate_settlement(value)
    value = event("coverage")
    value["attributes"]["lost_ranges"] = []
    with pytest.raises(TraceContractError, match="^missing_loss_ranges$"):
        validate_event(value)


def test_correction_has_a_new_identity_and_explicit_supersession() -> None:
    original = event()
    correction = deepcopy(original)
    correction["event_id"] = "00000000-0000-4000-8000-000000000999"
    correction["source_seq"] += 1
    correction["supersedes_event_id"] = original["event_id"]
    correction["phase"] = "completed"
    assert event_sha256(original) != event_sha256(correction)
    validate_export(export(correction))
    correction["supersedes_event_id"] = correction["event_id"]
    with pytest.raises(TraceContractError, match="^self_supersession$"):
        validate_event(correction)


@pytest.mark.parametrize(
    "field",
    [
        "agent_id",
        "trace_id",
        "task_id",
        "context_id",
        "execution_attempt_id",
        "span_id",
    ],
)
def test_anonymous_denial_cannot_claim_authenticated_identity(field: str) -> None:
    value = event("security")
    value[field] = event("task")[field] or "00000000-0000-4000-8000-000000000999"
    with pytest.raises(TraceContractError):
        validate_event(value)


def test_anonymous_denial_does_not_export_credentials_or_request_text() -> None:
    value = event("security")
    value["attributes"]["message"] = "PRIVATE_AUTH_SENTINEL"
    with pytest.raises(TraceContractError) as error:
        validate_event(value)
    assert "PRIVATE_AUTH_SENTINEL" not in str(error.value)
