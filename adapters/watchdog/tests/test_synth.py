"""Unit tests for the watchdog's synthesised-envelope shape."""
from adapters.watchdog.synth import build_synth_envelope


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
    a = build_synth_envelope(original_sender="aggregator", original_task_id="t1",
                             original_context_id=None,
                             offline_agent_id="gemma-1",
                             trigger="heartbeat_staleness")
    b = build_synth_envelope(original_sender="aggregator", original_task_id="t1",
                             original_context_id=None,
                             offline_agent_id="gemma-1",
                             trigger="heartbeat_staleness")
    assert a["id"] != b["id"]
