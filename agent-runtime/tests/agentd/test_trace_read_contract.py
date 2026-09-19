from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from edgecitadel_agentd.store import LEGAL_TRANSITIONS
from edgecitadel_agentd.trace_contract import (
    TASK_DISPLAY_STATES,
    TraceContractError,
    validate_event,
    validate_read_response,
)
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor

DIRECTORY = Path(__file__).parents[1] / "fixtures/traces"
FIXTURE = json.loads((DIRECTORY / "read.v1.json").read_text())


@pytest.mark.parametrize(
    "response", FIXTURE["responses"], ids=lambda value: value["kind"]
)
def test_all_read_and_live_response_examples_validate(response: dict) -> None:
    assert json.loads(validate_read_response(response)) == response


def graph() -> dict:
    return deepcopy(FIXTURE["expected_graph"])


def test_graph_has_separate_snapshot_and_resume_cursors() -> None:
    value = graph()
    key = b"owned-cursor-fixture-key-32-bytes!!"
    expected = CursorScope(
        "graph", value["trace_id"], "b" * 64, value["projection_generation"]
    )
    snapshot = decode_cursor(value["at"], key, expected, retained_from=0)
    resumed = decode_cursor(
        value["resume_cursor"],
        key,
        CursorScope(
            "changes",
            expected.trace_id,
            expected.scope_hash,
            expected.projection_generation,
        ),
        retained_from=0,
    )
    assert snapshot["snapshot"] == resumed["position"]
    with pytest.raises(TraceContractError, match="cursor_scope_mismatch"):
        decode_cursor(
            value["at"],
            key,
            CursorScope(
                "changes",
                expected.trace_id,
                expected.scope_hash,
                expected.projection_generation,
            ),
            retained_from=0,
        )


def test_fixture_join_is_not_ancestry_and_root_outcome_is_explicit() -> None:
    for event in FIXTURE["input_events"]:
        validate_event(event)
    value = graph()
    validate_read_response(value)
    # The join points back toward the root, but must not form a parent cycle.
    assert value["edges"][-1]["kind"] == "join"
    assert value["nodes"][0]["state"] == "running"
    assert all(node["state"] == "completed" for node in value["nodes"][1:])
    value["edges"][-1]["kind"] = "parent_task"
    with pytest.raises(TraceContractError, match="cyclic_graph_ancestry"):
        validate_read_response(value)
    value["edges"][-1]["status"] = "invalid"
    validate_read_response(value)  # attributable invalid edge remains visible


def test_graph_budget_requires_reachable_expansion() -> None:
    value = graph()
    template = deepcopy(value["nodes"][0])
    value["nodes"] = [{**template, "id": f"node-{i}"} for i in range(500)]
    value["edges"] = []
    value["total_nodes"] = 500
    validate_read_response(value)
    value["nodes"].append({**template, "id": "node-500"})
    with pytest.raises(TraceContractError, match="invalid_read"):
        validate_read_response(value)
    value["nodes"].pop()
    value["total_nodes"] = 501
    with pytest.raises(TraceContractError, match="unreachable_graph_expansion"):
        validate_read_response(value)
    # A bounded snapshot can expose more work only through an explicit token.
    value["expansions"] = [
        {
            "node_id": "node-0",
            "cursor": FIXTURE["responses"][0]["snapshot_cursor"],
            "remaining_nodes": 1,
        }
    ]
    validate_read_response(value)  # signature/scope is checked by cursor codec/API


def test_event_page_boundaries_and_trace_isolation() -> None:
    response = deepcopy(
        next(r for r in FIXTURE["responses"] if r["kind"] == "trace_events")
    )
    one = response["events"][0]
    response["events"] = [one] * 500
    validate_read_response(response)
    response["events"].append(one)
    with pytest.raises(TraceContractError, match="invalid_read"):
        validate_read_response(response)
    response["events"] = [{**one, "trace_id": "b" * 32}]
    with pytest.raises(TraceContractError, match="response_scope_mismatch"):
        validate_read_response(response)


def test_duplicate_graph_identity_missing_endpoint_and_outcome_mapping() -> None:
    value = graph()
    value["nodes"].append(deepcopy(value["nodes"][0]))
    with pytest.raises(TraceContractError, match="duplicate_graph_identity"):
        validate_read_response(value)
    value = graph()
    value["edges"][0]["from"] = "missing"
    with pytest.raises(TraceContractError, match="missing_graph_endpoint"):
        validate_read_response(value)
    value = graph()
    value["nodes"][0]["original_state"] = "cancelled"
    value["nodes"][0]["state"] = "canceled"
    validate_read_response(value)
    value["nodes"][0]["state"] = "completed"
    with pytest.raises(TraceContractError, match="invalid_state_mapping"):
        validate_read_response(value)


def test_frozen_task_lifecycle_matches_execution_owner() -> None:
    lifecycle = json.loads((DIRECTORY / "lifecycle.v1.json").read_text())
    assert lifecycle["task_transitions"] == {
        state: sorted(targets) for state, targets in LEGAL_TRANSITIONS.items()
    }
    assert lifecycle["display_states"] == TASK_DISPLAY_STATES


def test_private_attributes_and_inconsistent_retention_errors_are_rejected() -> None:
    value = graph()
    value["nodes"][0]["raw_arguments"] = "SENTINEL_PRIVATE"
    with pytest.raises(TraceContractError) as error:
        validate_read_response(value)
    assert "SENTINEL_PRIVATE" not in str(error.value)
    error_response = deepcopy(FIXTURE["responses"][-1])
    error_response["resnapshot_required"] = False
    with pytest.raises(TraceContractError, match="invalid_read"):
        validate_read_response(error_response)


@pytest.mark.parametrize(
    "updates",
    [
        {"mode": "clear"},
        {"mode": "snapshot"},
        {"mode": "patch", "at": None},
        {"mode": "patch", "trace_state": "expired"},
    ],
)
def test_change_modes_cannot_mix_replacement_clear_and_patch(updates):
    response = deepcopy(
        next(r for r in FIXTURE["responses"] if r["kind"] == "trace_change")
    )
    response["change"].update(updates)
    with pytest.raises(TraceContractError, match="invalid_read"):
        validate_read_response(response)


@pytest.mark.parametrize("mutation", ["order", "scope", "state", "base"])
def test_history_entries_preserve_order_retention_and_snapshot_semantics(mutation):
    value = deepcopy(
        next(item for item in FIXTURE["responses"] if item["kind"] == "trace_history")
    )
    if mutation == "order":
        value["items"].reverse()
    elif mutation == "scope":
        value["retained_from"] = 9
    elif mutation == "state":
        value["items"][0]["at"] = None
    else:
        value["items"][0]["is_retained_base"] = True
    with pytest.raises(TraceContractError):
        validate_read_response(value)
