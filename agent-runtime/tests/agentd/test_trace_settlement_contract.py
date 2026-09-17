"""A mismatched checkpoint must never authorize local spool retirement."""

from copy import deepcopy
from uuid import uuid4

import pytest

from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    validate_settlement_reply,
    validate_settlement_request,
)


def exchange():
    request = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "node_id": "edge-1",
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
    }
    checkpoint = {k: v for k, v in request.items() if k != "request_id"}
    checkpoint.update(
        collector_epoch=str(uuid4()),
        settled_export_seq=3,
        rejected_ranges=[{"first": 2, "last": 2}],
        lost_ranges=[],
    )
    return request, {
        "schema_version": 1,
        "request_id": request["request_id"],
        "status": "ok",
        "checkpoint": checkpoint,
    }


def test_valid_checkpoint_and_empty_unknown_source():
    request, reply = exchange()
    validate_settlement_request(request)
    validate_settlement_reply(reply, request=request)
    # Zero is a legitimate known source checkpoint, not an unknown-source reply.
    reply["checkpoint"].update(settled_export_seq=0, rejected_ranges=[])
    validate_settlement_reply(reply, request=request)


@pytest.mark.parametrize(
    "field", ["node_id", "source_epoch", "export_generation", "request_id"]
)
def test_reply_from_another_request_or_source_fails(field):
    request, reply = exchange()
    target = reply if field == "request_id" else reply["checkpoint"]
    target[field] = "edge-2" if field == "node_id" else str(uuid4())
    with pytest.raises(TraceContractError, match="settlement_.*_mismatch"):
        validate_settlement_reply(reply, request=request)


@pytest.mark.parametrize(
    "code",
    [
        "unknown_source",
        "unsupported_version",
        "invalid_request",
        "temporarily_unavailable",
        "rate_limited",
    ],
)
def test_errors_are_bounded_and_have_no_checkpoint(code):
    request, success = exchange()
    reply = {
        "schema_version": 1,
        "request_id": request["request_id"],
        "status": "error",
        "code": code,
        "retry_after_ms": 500,
    }
    validate_settlement_reply(reply, request=request)
    contaminated = deepcopy(reply)
    contaminated["checkpoint"] = success["checkpoint"]
    with pytest.raises(TraceContractError):
        validate_settlement_reply(contaminated, request=request)
    for invalid in (0, 30001, True, "500"):
        reply["retry_after_ms"] = invalid
        with pytest.raises(TraceContractError):
            validate_settlement_reply(reply, request=request)


def test_reject_overlap_and_unvalidated_request():
    request, reply = exchange()
    reply["checkpoint"]["lost_ranges"] = [{"first": 2, "last": 3}]
    with pytest.raises(TraceContractError, match="invalid_ranges"):
        validate_settlement_reply(reply, request=request)
    request["credentials"] = "must-not-be-echoed"
    with pytest.raises(TraceContractError) as error:
        validate_settlement_request(request)
    assert "must-not-be-echoed" not in str(error.value)
