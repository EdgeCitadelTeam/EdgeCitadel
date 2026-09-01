"""Unit tests for the watchdog's synthesised-envelope shape."""

import json

import pytest

from edgecitadel_plugin_runtime import validator as validator_module
from edgecitadel_watchdog_plugin.synth import build_synth_envelope


def test_envelope_shape_heartbeat_staleness():
    env = build_synth_envelope(
        original_sender="aggregator",
        original_task_id="t1",
        original_context_id=None,
        offline_agent_id="gemma-1",
        trigger="heartbeat_staleness",
    )
    assert env["v"] == 1
    assert env["type"] == "result"
    assert env["sender_id"] == "watchdog-1"
    assert env["recipient_id"] == "aggregator"
    assert env["task_id"] == "t1"
    assert "context_id" not in env  # null context_id omitted
    assert env["task_state"] == "failed"
    assert env["payload"]["error"] == "recipient_offline"
    assert env["payload"]["offline_agent_id"] == "gemma-1"
    assert env["payload"]["trigger"] == "heartbeat_staleness"
    # timestamp + id present, valid shape
    assert len(env["id"]) == 36
    assert env["timestamp"].endswith("Z")


def test_envelope_shape_sticky_offline():
    env = build_synth_envelope(
        original_sender="ag2-1",
        original_task_id="t2",
        original_context_id="ctx-abc",
        offline_agent_id="gemma-1",
        trigger="sticky_offline",
    )
    assert env["context_id"] == "ctx-abc"
    assert env["payload"]["trigger"] == "sticky_offline"


def test_envelope_shape_advisory():
    env = build_synth_envelope(
        original_sender="aggregator",
        original_task_id="t3",
        original_context_id=None,
        offline_agent_id="gemma-1",
        trigger="max_deliveries_advisory",
    )
    assert env["payload"]["trigger"] == "max_deliveries_advisory"


def test_envelope_id_is_unique_per_call():
    a = build_synth_envelope(
        original_sender="aggregator",
        original_task_id="t1",
        original_context_id=None,
        offline_agent_id="gemma-1",
        trigger="heartbeat_staleness",
    )
    b = build_synth_envelope(
        original_sender="aggregator",
        original_task_id="t1",
        original_context_id=None,
        offline_agent_id="gemma-1",
        trigger="heartbeat_staleness",
    )
    assert a["id"] != b["id"]


@pytest.mark.parametrize(
    "context_id",
    [None, "6e088543-c9de-4459-a0fe-2191d20dfba1"],
    ids=["implicit-context", "explicit-context"],
)
def test_synth_correlation_preserves_actual_producer_shape(
    context_id: str | None,
) -> None:
    task_id = "899d8a29-8c6c-4fef-b491-1140d8371fef"
    env = build_synth_envelope(
        original_sender="aggregator",
        original_task_id=task_id,
        original_context_id=context_id,
        offline_agent_id="worker-1",
        trigger="heartbeat_staleness",
    )
    env = json.loads(json.dumps(env))

    expected_fields = {
        "v",
        "id",
        "type",
        "sender_id",
        "recipient_id",
        "task_id",
        "task_state",
        "timestamp",
        "payload",
    }
    if context_id is not None:
        expected_fields.add("context_id")
    assert set(env) == expected_fields
    validator_module.default_validator().validate_envelope(env)
    correlated = validator_module.normalize_task_correlation(env)
    assert correlated["context_id"] == (context_id or task_id)
    assert correlated["hop_count"] == 0
