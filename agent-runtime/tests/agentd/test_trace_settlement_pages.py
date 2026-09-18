from uuid import uuid4

import pytest

from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    validate_settlement_reply,
)
from edgecitadel_agentd.trace_settlement_pages import (
    validate_page_reply,
    validate_page_request,
)


@pytest.fixture
def exchange():
    request = {
        "schema_version": 2,
        "request_id": str(uuid4()),
        "node_id": "edge-a",
        "source_epoch": str(uuid4()),
        "export_generation": str(uuid4()),
        "collector_epoch": str(uuid4()),
        "after_export_seq": 100,
    }
    page = {
        key: request[key]
        for key in (
            "schema_version",
            "node_id",
            "source_epoch",
            "export_generation",
            "collector_epoch",
            "after_export_seq",
        )
    }
    page.update(
        settled_export_seq=104,
        more=True,
        rejected_ranges=[{"first": 101, "last": 102}],
        lost_ranges=[{"first": 104, "last": 104}],
    )
    return request, {
        "schema_version": 2,
        "request_id": request["request_id"],
        "status": "ok",
        "page": page,
    }


def test_page_is_bounded_and_v1_cannot_mistake_it_for_checkpoint(exchange):
    request, reply = exchange
    assert len(validate_page_request(request)) <= 1024
    assert len(validate_page_reply(reply, request=request)) <= 18 * 1024
    legacy = {
        key: value
        for key, value in request.items()
        if key not in ("after_export_seq", "collector_epoch")
    }
    legacy["schema_version"] = 1
    with pytest.raises(TraceContractError):
        validate_settlement_reply(reply, request=legacy)


@pytest.mark.parametrize(
    "field",
    [
        "node_id",
        "source_epoch",
        "export_generation",
        "collector_epoch",
        "after_export_seq",
    ],
)
def test_page_cannot_change_scope_epoch_or_base(exchange, field):
    request, reply = exchange
    reply["page"][field] = (
        "edge-b"
        if field == "node_id"
        else (99 if field == "after_export_seq" else str(uuid4()))
    )
    with pytest.raises(TraceContractError):
        validate_page_reply(reply, request=request)


def test_continuation_requires_known_epoch(exchange):
    request, _ = exchange
    request["collector_epoch"] = None
    with pytest.raises(TraceContractError, match="missing_collector_epoch"):
        validate_page_request(request)
    request["after_export_seq"] = 0
    validate_page_request(request)


@pytest.mark.parametrize(
    "change",
    [
        "before_base",
        "overlap",
        "regress",
        "no_progress",
        "wrong_request",
        "extra_field",
    ],
)
def test_page_rejects_unsafe_range_or_correlation(exchange, change):
    request, reply = exchange
    page = reply["page"]
    if change == "before_base":
        page["rejected_ranges"][0]["first"] = 100
    elif change == "overlap":
        page["lost_ranges"] = [{"first": 101, "last": 102}]
    elif change == "regress":
        page["settled_export_seq"] = 99
    elif change == "no_progress":
        page.update(settled_export_seq=100, rejected_ranges=[], lost_ranges=[])
    elif change == "wrong_request":
        reply["request_id"] = str(uuid4())
    else:
        reply["checkpoint"] = page
    with pytest.raises(TraceContractError):
        validate_page_reply(reply, request=request)


def test_collector_changed_error_has_no_retirement_evidence(exchange):
    request, _ = exchange
    reply = {
        "schema_version": 2,
        "request_id": request["request_id"],
        "status": "error",
        "code": "collector_changed",
        "retry_after_ms": 500,
    }
    validate_page_reply(reply, request=request)
    reply["page"] = {}
    with pytest.raises(TraceContractError):
        validate_page_reply(reply, request=request)
