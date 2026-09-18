from __future__ import annotations

import json
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_correlation import TaskTraceContext
from edgecitadel_plugin_runtime.validator import default_validator

FIXTURES = json.loads(
    (Path(__file__).parents[1] / "fixtures/traces/delegation.v1.json").read_text()
)["fixtures"]


def message(task_id: str, sender: str, recipient: str, kind: str = "command") -> dict:
    value = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": kind,
        "task_id": task_id,
        "sender_id": sender,
        "recipient_id": recipient,
        "timestamp": "2026-09-16T12:00:00.000Z",
        "payload": {"body": "owned fixture"},
    }
    if kind == "result":
        value["task_state"] = "completed"
    return value


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda fixture: fixture["id"])
def test_three_levels_preserve_context_across_wire_and_results(fixture: dict) -> None:
    root, child, grandchild = fixture["task_ids"]
    contexts = [TaskTraceContext(root, fixture["context_id"], fixture["trace_id"])]
    edges = []
    for index, task_id in enumerate([root, child, grandchild]):
        if index:
            contexts.append(contexts[-1].child(task_id))
        context = contexts[-1]
        envelope = context.apply(
            message(task_id, f"agent-{index}", f"agent-{index + 1}")
        )
        # The existing legacy validator must accept the new producer's payload
        # metadata without unknown top-level envelope fields.
        default_validator().validate_envelope(envelope)
        received = TaskTraceContext.from_envelope(json.loads(json.dumps(envelope)))
        assert received == context
        assert received.hop_count == fixture["expected_hops"][index]
        assert received.trace_id == fixture["trace_id"]
        if received.parent_task_id:
            edges.append([received.parent_task_id, received.task_id])
        result = received.apply(
            message(task_id, f"agent-{index + 1}", f"agent-{index}", "result")
        )
        assert TaskTraceContext.from_envelope(result) == context
        assert result["payload"]["body"] == "owned fixture"
        cancel = received.apply(
            message(task_id, f"agent-{index}", f"agent-{index + 1}", "cancel")
        )
        assert TaskTraceContext.from_envelope(cancel) == context
    assert edges == fixture["expected_parent_edges"]


def test_two_roots_in_same_conversation_never_share_ancestry() -> None:
    contexts = [
        TaskTraceContext(
            item["task_ids"][0], item["context_id"], item["trace_id"]
        ).child(item["task_ids"][1])
        for item in FIXTURES
    ]
    assert contexts[0].context_id == contexts[1].context_id
    assert contexts[0].trace_id != contexts[1].trace_id
    assert contexts[0].parent_task_id != contexts[1].parent_task_id


def test_legacy_root_does_not_gain_invented_trace_evidence() -> None:
    task_id = str(uuid.uuid4())
    old = message(task_id, "sender", "worker")
    context = TaskTraceContext.from_envelope(old)
    assert context.context_id == task_id
    assert context.context_origin == "legacy_default"
    assert context.trace_id is None
    upgraded = context.child(str(uuid.uuid4()))
    wire = upgraded.apply(message(upgraded.task_id, "worker", "next"))
    assert "trace_id" not in wire["payload"]
    assert TaskTraceContext.from_envelope(wire).context_origin == "legacy_default"


def test_existing_receiver_executes_new_delegated_envelope(tmp_path: Path) -> None:
    fixture = FIXTURES[0]
    context = TaskTraceContext(
        fixture["task_ids"][0], fixture["context_id"], fixture["trace_id"]
    )
    context = context.child(fixture["task_ids"][1]).child(fixture["task_ids"][2])
    wire = context.apply(message(context.task_id, "sender", "worker"))
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        received = store.ingest_transport_envelope(wire)
        assert received["state"] == "offered"
        assert received["payload"]["body"] == "owned fixture"
        assert received["trace_id"] == context.trace_id
        for state in ("accepted", "running", "completed"):
            store.transition_task(
                task_id=context.task_id,
                state=state,
                actor_id="worker",
                result={"body": "owned complete"} if state == "completed" else None,
                queue_transport=False,
            )
        assert store.get_task(context.task_id)["result"] == {"body": "owned complete"}
    finally:
        store.close()


def test_native_observational_root_is_typed_without_parent_task() -> None:
    context = TaskTraceContext(
        str(uuid.uuid4()), str(uuid.uuid4()), "a" * 32, parent_run_id="a" * 32
    )
    wire = context.apply(message(context.task_id, "codex", "worker"))
    assert wire["type"] == "command" and wire["hop_count"] == 0
    assert "parent_task_id" not in wire["payload"]
    assert TaskTraceContext.from_envelope(wire) == context


def test_reserved_handler_output_cannot_change_task_correlation() -> None:
    item = FIXTURES[0]
    context = TaskTraceContext(
        item["task_ids"][0], item["context_id"], item["trace_id"]
    ).child(item["task_ids"][1])
    wire = message(context.task_id, "worker", "sender", "result")
    wire["payload"].update(
        trace_id="b" * 32,
        parent_task_id=str(uuid.uuid4()),
        execution_context={"schema_version": 99},
    )
    assert TaskTraceContext.from_envelope(context.apply(wire)) == context
    with pytest.raises(TraceContractError, match="task_correlation_mismatch"):
        context.apply(message(str(uuid.uuid4()), "worker", "sender"))


def test_bad_depth_parent_and_version_are_rejected() -> None:
    item = FIXTURES[0]
    context = TaskTraceContext(
        item["task_ids"][0], item["context_id"], item["trace_id"]
    )
    for changes in (
        {"hop_count": True},
        {"hop_count": 1},
        {"parent_task_id": context.task_id},
        {"parent_run_id": "b" * 32},
    ):
        with pytest.raises(TraceContractError):
            replace(context, **changes)
    wire = context.apply(message(context.task_id, "sender", "worker"))
    wire["payload"]["execution_context"]["schema_version"] = 2
    with pytest.raises(TraceContractError, match="unsupported_execution_context"):
        TaskTraceContext.from_envelope(wire)
