"""V2 interval settlement codec; pages never imply evidence before their base."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .trace_contract import TraceContractError, canonical_bytes, validate_settlement

SETTLEMENT_PAGE_SUBJECT = "edgecitadel.telemetry.settlement.v2"
_DIRECTORY = Path(
    os.environ.get(
        "EDGECITADEL_SCHEMA_DIR", Path(__file__).resolve().parents[3] / "schemas"
    )
)
_VALIDATORS = {
    kind: Draft202012Validator(
        json.loads((_DIRECTORY / f"trace-settlement-page-{kind}.v2.json").read_text())
    )
    for kind in ("request", "reply")
}


def validate_page_request(request: dict[str, Any]) -> bytes:
    encoded = canonical_bytes(request, limit=1024)
    if not _VALIDATORS["request"].is_valid(request):
        raise TraceContractError("invalid_settlement_page_request")
    if request["after_export_seq"] and request["collector_epoch"] is None:
        raise TraceContractError("missing_collector_epoch")
    return encoded


def validate_page_reply(reply: dict[str, Any], *, request: dict[str, Any]) -> bytes:
    validate_page_request(request)
    encoded = canonical_bytes(reply, limit=18 * 1024)
    if not _VALIDATORS["reply"].is_valid(reply):
        raise TraceContractError("invalid_settlement_page_reply")
    if reply["request_id"] != request["request_id"]:
        raise TraceContractError("settlement_request_mismatch")
    if reply["status"] == "error":
        return encoded
    page = reply["page"]
    if any(
        page[key] != request[key]
        for key in ("node_id", "source_epoch", "export_generation", "after_export_seq")
    ):
        raise TraceContractError("settlement_scope_mismatch")
    if (
        request["collector_epoch"] is not None
        and page["collector_epoch"] != request["collector_epoch"]
    ):
        raise TraceContractError("collector_epoch_mismatch")
    after, through = page["after_export_seq"], page["settled_export_seq"]
    if through < after or (page["more"] and through == after):
        raise TraceContractError("invalid_settlement_page_progress")
    if any(
        item["first"] <= after
        for key in ("rejected_ranges", "lost_ranges")
        for item in page[key]
    ):
        raise TraceContractError("settlement_range_before_page")
    # Reuse the established exact range ordering/disjointness semantics, while
    # explicitly removing page fields so no v1 client can mistake this for v1.
    checkpoint = {
        key: value
        for key, value in page.items()
        if key not in ("after_export_seq", "more")
    }
    checkpoint["schema_version"] = 1
    validate_settlement(checkpoint)
    return encoded
