"""Agent Card schema tests — A2A v1.0 shape + EdgeCitadel metadata vocabulary."""
import json
from pathlib import Path
import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMA = json.loads((Path(__file__).resolve().parents[1]
                     / "agent-card.v1.json").read_text())


@pytest.fixture(scope="module")
def validator():
    return Draft202012Validator(SCHEMA)


def _card(**over):
    doc = {
        "name": "shell-1",
        "description": "Shell executor.",
        "version": "0.1.0",
        "url": "nats://edgecitadel/agents.shell-1.inbox",
        "provider": {"organization": "EdgeCitadel", "url": "https://edgecitadel.local"},
        "capabilities": {
            "streaming": False,
            "extensions": [{
                "uri": "https://edgecitadel.local/ext/nats-binding/v1",
                "description": "NATS JetStream transport binding.",
                "required": False,
                "params": {"subject_prefix": "agents.shell-1"}
            }]
        },
        "securitySchemes": {},
        "skills": [{"id": "shell.exec", "name": "shell-exec",
                    "description": "Run a shell command.",
                    "tags": ["shell"]}],
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 30
        }
    }
    doc.update(over)
    return doc


class TestAccepts:
    def test_minimal_native(self, validator):
        validator.validate(_card())

    def test_bridge_requires_upstream(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.kind": "bridge",
                         "runtime.upstream": "nous-hermes-agent"}
        validator.validate(c)

    def test_gateway_kind_accepted(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.kind": "gateway"}
        validator.validate(c)

    def test_conformance_l2_accepted(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.conformance": "L2"}
        validator.validate(c)

    def test_conformance_l3_accepted(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.conformance": "L3"}
        validator.validate(c)


class TestRejects:
    def test_missing_required_a2a_field(self, validator):
        c = _card(); del c["provider"]
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_bridge_without_upstream(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.kind": "bridge"}
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_bad_runtime_role(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.roles": ["not-a-role"]}
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_heartbeat_out_of_range(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.heartbeat_interval_sec": 5}
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_missing_conformance(self, validator):
        c = _card()
        m = dict(c["metadata"])
        m.pop("runtime.conformance")
        c["metadata"] = m
        with pytest.raises(ValidationError):
            validator.validate(c)

    def test_bad_conformance_value(self, validator):
        c = _card()
        c["metadata"] = {**c["metadata"], "runtime.conformance": "L4"}
        with pytest.raises(ValidationError):
            validator.validate(c)
