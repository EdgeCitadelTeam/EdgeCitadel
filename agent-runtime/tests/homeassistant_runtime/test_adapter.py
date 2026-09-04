from unittest.mock import MagicMock

import pytest

from edgecitadel_plugin_runtime.pull_consumer import Context
from edgecitadel_homeassistant_plugin.adapter import HomeAssistantWorker, handle


class FakeClient:
    def __init__(self):
        self.states = {
            "light.test": {"state": "off", "attributes": {"brightness": 100}}
        }
        self.calls = []

    def state(self, entity_id):
        return dict(self.states[entity_id])

    def set_light(self, entity_id, state, brightness=255):
        self.calls.append(("set_light", entity_id, state, brightness))
        self.states[entity_id] = {
            "state": state,
            "attributes": {"brightness": brightness},
        }


def worker():
    return HomeAssistantWorker(
        FakeClient(),
        allowed_lights={"light.test"},
        allowed_entities={"sensor.test"},
        allowed_cameras=set(),
        camera_rois={},
    )


def test_sequence_restores_original_light():
    w = worker()
    result = w.sequence(
        [
            {
                "operation": "set_light",
                "args": {"entity_id": "light.test", "state": "on", "brightness": 200},
            },
            {"operation": "get_state", "args": {"entity_id": "light.test"}},
        ],
        restore=True,
    )
    assert result["steps"][1]["result"]["state"] == "on"
    assert w.client.states["light.test"]["state"] == "off"
    assert result["restored_entities"] == ["light.test"]
    assert result["restore_errors"] == []


def test_sequence_rejects_non_allowlisted_light_before_actuation():
    w = worker()
    with pytest.raises(PermissionError):
        w.sequence(
            [
                {
                    "operation": "set_light",
                    "args": {"entity_id": "light.other", "state": "on"},
                }
            ],
            restore=True,
        )
    assert w.client.calls == []


def test_sequence_requires_steps():
    w = worker()
    with pytest.raises(ValueError, match="non-empty list"):
        w.sequence([], restore=True)


def test_set_light_confirms_requested_brightness():
    w = worker()
    w.client.states["light.test"] = {"state": "on", "attributes": {"brightness": 100}}
    w.client.set_light = MagicMock()
    with pytest.raises(TimeoutError):
        w.operation(
            "set_light",
            {
                "entity_id": "light.test",
                "state": "on",
                "brightness": 200,
                "confirm_timeout_sec": 0.01,
                "poll_sec": 0.01,
            },
        )


def test_handle_marks_restore_failure_failed():
    w = worker()
    w.sequence = MagicMock(
        return_value={
            "steps": [],
            "restored_entities": ["light.test"],
            "restore_errors": ["light.test"],
        }
    )

    import edgecitadel_homeassistant_plugin.adapter as module

    original = module._load_worker
    module._load_worker = lambda: w
    try:
        payload, state = __import__("asyncio").run(
            handle(
                {"type": "command", "payload": {"args": {"operation": "run_sequence"}}},
                MagicMock(spec=Context),
            )
        )
    finally:
        module._load_worker = original
    assert state == "failed"
    assert payload["result"]["restore_errors"] == ["light.test"]


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"type": "delegation", "sender_id": "planner-1", "payload": {}}
    payload, state = await handle(env, MagicMock(spec=Context))
    assert state == "rejected"
    assert payload["error"] == "unsupported_type"
