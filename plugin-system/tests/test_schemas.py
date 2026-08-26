from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError


SCHEMAS = Path(__file__).parents[1] / "schemas"


def validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def plugin_document() -> dict[str, object]:
    return {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentPlugin",
        "metadata": {
            "name": "example",
            "displayName": "Example",
            "description": "Example package.",
            "version": "0.1.0",
            "publisher": "local",
        },
        "compatibility": {
            "supervisorApi": ">=0.1.0,<0.2.0",
            "protocols": ["edgecitadel.plugin.v1"],
        },
        "runtime": {
            "command": ["python", "-m", "runtime"],
            "healthTimeoutSeconds": 10,
            "restartPolicy": "on-failure",
        },
        "skills": {"directory": "skills"},
        "agents": [
            {
                "id": "example-agent",
                "skillNames": ["example"],
                "listensBroadcast": False,
            }
        ],
        "permissions": {
            "knowledge": [],
            "messaging": {"outboundAgents": []},
            "network": {"outbound": []},
            "devices": [],
        },
        "security": {"sandbox": "restricted", "secrets": []},
        "extensions": {},
    }


def test_plugin_schema_accepts_separate_package_and_agent_identity():
    validator("agent-plugin.v1alpha1.schema.json").validate(plugin_document())


def test_plugin_schema_rejects_unknown_core_field():
    document = plugin_document()
    document["unexpected"] = True

    with pytest.raises(ValidationError):
        validator("agent-plugin.v1alpha1.schema.json").validate(document)


def test_binding_schema_accepts_and_requires_runtime_execution_name():
    document = {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentSkillBinding",
        "skillId": "example.placeholder",
        "version": "0.1.0",
        "execution": {"kind": "runtime-handler", "name": "placeholder"},
        "schemas": {
            "input": "schemas/input.json",
            "output": "schemas/output.json",
        },
        "requires": {"knowledge": [], "network": [], "devices": []},
        "extensions": {},
    }
    binding_validator = validator("agent-skill-binding.v1alpha1.schema.json")
    binding_validator.validate(document)

    del document["execution"]["name"]
    with pytest.raises(ValidationError):
        binding_validator.validate(document)


def test_lock_schema_requires_sorted_file_records_shape():
    document = {
        "lockVersion": 1,
        "package": {
            "id": "local.example",
            "version": "0.1.0",
            "protocol": "edgecitadel.plugin.v1",
        },
        "files": [{"path": "plugin.yaml", "sha256": "0" * 64}],
        "skills": [
            {
                "name": "placeholder",
                "skillId": "example.placeholder",
                "version": "0.1.0",
                "contentSha256": "1" * 64,
            }
        ],
    }
    validator("plugin-lock.v1.schema.json").validate(document)
