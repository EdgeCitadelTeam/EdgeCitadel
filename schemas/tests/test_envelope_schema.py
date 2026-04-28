"""Tests for the strict v0.1 envelope schema (A2A v1.0 vocabulary)."""
import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "envelope.v1.json"


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(SCHEMA_PATH.read_text())
    return Draft202012Validator(schema)


def _base(**over):
    doc = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "heartbeat",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {},
    }
    doc.update(over)
    return doc


class TestAccepts:
    def test_minimal_heartbeat(self, validator):
        validator.validate(_base())

    def test_status_with_agent_state(self, validator):
        validator.validate(_base(type="status", payload={"reason": "boot"},
                                 agent_state="online"))

    def test_command(self, validator):
        validator.validate(_base(
            type="command", recipient_id="gemma-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            payload={"body": "hello"}))

    def test_result_with_task_state(self, validator):
        validator.validate(_base(
            type="result", recipient_id="shell-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            task_state="completed",
            payload={"body": "done"}))

    def test_delegation_with_context_and_hop(self, validator):
        validator.validate(_base(
            type="delegation", recipient_id="worker-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            context_id="cccccccc-dddd-4eee-8fff-000000000000",
            hop_count=1,
            payload={"body": "subtask"}))

    def test_task_progress(self, validator):
        validator.validate(_base(
            type="task.progress",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            task_state="working",
            payload={"progress": 42, "message": "halfway"}))

    def test_cancel(self, validator):
        validator.validate(_base(
            type="cancel", recipient_id="gemma-1",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            payload={"task_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                     "reason": "user_aborted"}))


class TestRejects:
    def test_unknown_top_level_field(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(receiver_id="gemma-1"))

    def test_legacy_message_type(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(message_type="info"))

    def test_missing_recipient_on_command(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(type="command",
                                     task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                                     payload={"body": "x"}))

    def test_missing_task_state_on_result(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="shell-1",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                payload={"body": "done"}))

    def test_missing_hop_count_on_delegation(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="delegation", recipient_id="w",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                context_id="cccccccc-dddd-4eee-8fff-000000000000",
                payload={"body": "x"}))

    def test_wrong_v(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(v=2))

    def test_bad_task_state_enum(self, validator):
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="x",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                task_state="not-a-state", payload={}))

    def test_agent_state_on_result_rejected(self, validator):
        # agent_state must not appear on result; only on status
        with pytest.raises(ValidationError):
            validator.validate(_base(
                type="result", recipient_id="x",
                task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
                task_state="completed", agent_state="busy", payload={}))

    def test_task_state_on_status_rejected(self, validator):
        # task_state must not appear on status
        with pytest.raises(ValidationError):
            validator.validate(_base(type="status", agent_state="online",
                                     task_state="working", payload={}))

    def test_agent_state_on_heartbeat_rejected(self, validator):
        # agent_state must not appear on heartbeat - only on status
        with pytest.raises(ValidationError):
            validator.validate(_base(type="heartbeat", agent_state="online"))

    def test_task_state_on_broadcast_rejected(self, validator):
        # task_state has no meaning on broadcast envelopes
        with pytest.raises(ValidationError):
            validator.validate(_base(type="broadcast", task_state="working"))

    def test_hop_count_too_high_allowed_by_schema_refused_by_adapter(self, validator):
        # Schema allows any int; refusal at >=8 is adapter-level (Task 9 / 4.2).
        validator.validate(_base(
            type="delegation", recipient_id="x",
            task_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            context_id="cccccccc-dddd-4eee-8fff-000000000000",
            hop_count=99, payload={}))
