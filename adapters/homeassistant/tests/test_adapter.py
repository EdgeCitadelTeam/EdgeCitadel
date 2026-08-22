from unittest.mock import MagicMock

import pytest

from adapters._common.pull_consumer import Context
from adapters.homeassistant.adapter import HomeAssistantWorker, handle


class FakeClient:
    def __init__(self):
        self.states = {"light.test": {"state": "off", "attributes": {"brightness": 100}}}
        self.calls = []

    def state(self, entity_id):
        return dict(self.states[entity_id])

    def set_light(self, entity_id, state, brightness=255):
        self.calls.append(("set_light", entity_id, state, brightness))
        self.states[entity_id] = {"state": state, "attributes": {"brightness": brightness}}


def worker():
    return HomeAssistantWorker(FakeClient(), allowed_lights={"light.test"},
                               allowed_entities={"sensor.test"},
                               allowed_cameras=set(), camera_rois={})


def test_sequence_restores_original_light():
    w = worker()
    result = w.sequence([
        {"operation": "set_light", "args": {"entity_id": "light.test", "state": "on", "brightness": 200}},
        {"operation": "get_state", "args": {"entity_id": "light.test"}},
    ], restore=True)
    assert result["steps"][1]["result"]["state"] == "on"
    assert w.client.states["light.test"]["state"] == "off"
    assert result["restored_entities"] == ["light.test"]
    assert result["restore_errors"] == []


def test_sequence_rejects_non_allowlisted_light_before_actuation():
    w = worker()
    with pytest.raises(PermissionError):
        w.sequence([{"operation": "set_light", "args": {"entity_id": "light.other", "state": "on"}}], restore=True)
    assert w.client.calls == []


def test_sequence_requires_steps():
    w = worker()
    with pytest.raises(ValueError, match="non-empty list"):
        w.sequence([], restore=True)


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"type": "delegation", "sender_id": "planner-1", "payload": {}}
    payload, state = await handle(env, MagicMock(spec=Context))
    assert state == "rejected"
    assert payload["error"] == "unsupported_type"
