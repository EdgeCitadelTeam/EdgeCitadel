from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

# Runtime tests import built-in Plugins directly from their lock-validated source
# trees. Do not let the interpreter contaminate those packages with untracked
# bytecode and make validation depend on test order.
sys.dont_write_bytecode = True


@pytest.fixture
def valid_package(tmp_path: Path) -> Path:
    root = tmp_path / "example"
    skill = root / "skills" / "placeholder"
    schemas = skill / "schemas"
    schemas.mkdir(parents=True)
    (root / "plugin.yaml").write_text(
        yaml.safe_dump(
            {
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
                        "skillNames": ["placeholder"],
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
            },
            sort_keys=False,
        )
    )
    (skill / "SKILL.md").write_text(
        "---\nname: placeholder\n"
        "description: Use when validating an example package.\n"
        "compatibility: Requires EdgeCitadel plugin protocol v1.\n"
        "metadata:\n  version: '0.1.0'\n---\n# Procedure\nReturn a message.\n"
    )
    (skill / "binding.yaml").write_text(
        yaml.safe_dump(
            {
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
            },
            sort_keys=False,
        )
    )
    object_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
    }
    (schemas / "input.json").write_text(json.dumps(object_schema))
    (schemas / "output.json").write_text(json.dumps(object_schema))
    return root
