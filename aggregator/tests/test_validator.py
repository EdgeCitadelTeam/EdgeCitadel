import pytest
from aggregator.validator import EnvelopeValidator, ValidationError


@pytest.fixture(scope="module")
def validator(envelope_schema_path, card_schema_path):
    return EnvelopeValidator(envelope_schema_path, card_schema_path)


def _env(**over):
    base = {
        "v": 1, "id": "11111111-2222-4333-8444-555555555555",
        "type": "heartbeat", "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {}
    }
    base.update(over); return base


def test_accepts_valid(validator):
    validator.validate_envelope(_env())


def test_rejects_unknown_field(validator):
    with pytest.raises(ValidationError) as exc:
        validator.validate_envelope(_env(receiver_id="x"))
    assert "receiver_id" in str(exc.value) or "unexpected" in str(exc.value).lower()


def test_rejects_missing_type(validator):
    bad = _env(); del bad["type"]
    with pytest.raises(ValidationError):
        validator.validate_envelope(bad)


def test_register_card_must_match_sender_id(validator):
    env = _env(type="register", sender_id="shell-1",
               payload={"name": "shell-1", "description": "x", "version": "0.1",
                        "url": "nats://x", "provider": {"organization": "EC"},
                        "capabilities": {}, "securitySchemes": {},
                        "metadata": {"runtime.kind": "native",
                                     "runtime.roles": ["worker"],
                                     "runtime.conformance": "L1",
                                     "runtime.heartbeat_interval_sec": 30}})
    validator.validate_envelope(env)
    validator.validate_register(env)  # name == sender_id

    env["payload"]["name"] = "different"
    with pytest.raises(ValidationError, match="sender_id"):
        validator.validate_register(env)
