from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

SCHEMAS = Path(__file__).parents[1] / "schemas"


def validator(name: str) -> Draft202012Validator:
    schema = json.loads((SCHEMAS / name).read_text())
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(
        schema,
        format_checker=Draft202012Validator.FORMAT_CHECKER,
    )


def test_supervisor_source_package_is_importable() -> None:
    supervisor = importlib.import_module("edgecitadel_supervisor")

    assert supervisor.__doc__


def plugin_document(
    *,
    version: str = "0.1.0",
    agent_id: str = "example-agent",
    extensions: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentPlugin",
        "metadata": {
            "name": "example",
            "displayName": "Example",
            "description": "Example package.",
            "version": version,
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
                "id": agent_id,
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
        "extensions": {} if extensions is None else extensions,
    }


def binding_document(
    *,
    version: str = "0.1.0",
    extensions: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "apiVersion": "edgecitadel.io/v1alpha1",
        "kind": "AgentSkillBinding",
        "skillId": "example.placeholder",
        "version": version,
        "execution": {"kind": "runtime-handler", "name": "placeholder"},
        "schemas": {
            "input": "schemas/input.json",
            "output": "schemas/output.json",
        },
        "requires": {"knowledge": [], "network": [], "devices": []},
        "extensions": {} if extensions is None else extensions,
    }


def lock_document(*, version: str = "0.1.0") -> dict[str, object]:
    return {
        "lockVersion": 1,
        "package": {
            "id": "local.example",
            "version": version,
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


def test_plugin_schema_accepts_separate_package_and_agent_identity() -> None:
    validator("agent-plugin.v1alpha1.schema.json").validate(plugin_document())


@pytest.mark.parametrize("agent_id", ["agent_1", "a" * 64])
def test_plugin_schema_accepts_canonical_agent_ids(agent_id: str) -> None:
    validator("agent-plugin.v1alpha1.schema.json").validate(
        plugin_document(agent_id=agent_id)
    )


@pytest.mark.parametrize("agent_id", ["agent.1", "agent..1", "a" * 65])
def test_plugin_schema_rejects_noncanonical_agent_ids(agent_id: str) -> None:
    with pytest.raises(ValidationError):
        validator("agent-plugin.v1alpha1.schema.json").validate(
            plugin_document(agent_id=agent_id)
        )


def test_plugin_schema_rejects_unknown_core_field() -> None:
    document = plugin_document()
    document["unexpected"] = True

    with pytest.raises(ValidationError):
        validator("agent-plugin.v1alpha1.schema.json").validate(document)


def test_binding_schema_accepts_and_requires_runtime_execution_name() -> None:
    document = binding_document()
    binding_validator = validator("agent-skill-binding.v1alpha1.schema.json")
    binding_validator.validate(document)

    execution = document["execution"]
    assert isinstance(execution, dict)
    del execution["name"]
    with pytest.raises(ValidationError):
        binding_validator.validate(document)


def test_lock_schema_accepts_lock_record_shape() -> None:
    validator("plugin-lock.v1.schema.json").validate(lock_document())


@pytest.mark.parametrize(
    ("schema_name", "document"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            plugin_document(version="1.0.0-01"),
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            binding_document(version="1.0.0-01"),
        ),
        ("plugin-lock.v1.schema.json", lock_document(version="1.0.0-01")),
    ],
)
def test_schemas_reject_semver_numeric_prerelease_identifiers_with_leading_zeros(
    schema_name: str,
    document: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        validator(schema_name).validate(document)


@pytest.mark.parametrize(
    ("schema_name", "document"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            plugin_document(extensions={"x:<": {}}),
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            binding_document(extensions={"x:<": {}}),
        ),
    ],
)
def test_extension_maps_reject_malformed_absolute_uri_keys(
    schema_name: str,
    document: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        validator(schema_name).validate(document)


@pytest.mark.parametrize(
    ("schema_name", "document"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            plugin_document(extensions={"x:foo#bar#baz": {}}),
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            binding_document(extensions={"x:foo#bar#baz": {}}),
        ),
    ],
)
def test_extension_maps_reject_absolute_uri_keys_with_repeated_fragments(
    schema_name: str,
    document: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        validator(schema_name).validate(document)


@pytest.mark.parametrize(
    ("schema_name", "document"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            plugin_document(
                extensions={
                    "com.example.feature": {},
                    "https://[2001:db8::1]/schema": {},
                }
            ),
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            binding_document(
                extensions={
                    "com.example.feature": {},
                    "https://[2001:db8::1]/schema": {},
                }
            ),
        ),
    ],
)
def test_extension_maps_accept_reverse_domain_and_ipv6_absolute_uri_keys(
    schema_name: str,
    document: dict[str, object],
) -> None:
    validator(schema_name).validate(document)


@pytest.mark.parametrize(
    "control", ["\x00", "\n", "\x1b", "\x7f", "\x80", "\x9b", "\x9f"]
)
@pytest.mark.parametrize(
    ("schema_name", "document_factory", "path_location"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            plugin_document,
            ("skills", "directory"),
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            binding_document,
            ("schemas", "input"),
        ),
        (
            "plugin-lock.v1.schema.json",
            lock_document,
            ("files", 0, "path"),
        ),
    ],
)
def test_relative_paths_reject_control_characters(
    control: str,
    schema_name: str,
    document_factory: object,
    path_location: tuple[object, ...],
) -> None:
    assert callable(document_factory)
    document = document_factory()
    target: object = document
    for component in path_location[:-1]:
        assert isinstance(target, (dict, list))
        target = target[component]  # type: ignore[index]
    assert isinstance(target, (dict, list))
    target[path_location[-1]] = f"safe{control}path"  # type: ignore[index]

    with pytest.raises(ValidationError):
        validator(schema_name).validate(document)


@pytest.mark.parametrize(
    ("schema_name", "document"),
    [
        (
            "agent-plugin.v1alpha1.schema.json",
            {**plugin_document(), "skills": {"directory": "技能"}},
        ),
        (
            "agent-skill-binding.v1alpha1.schema.json",
            {
                **binding_document(),
                "schemas": {
                    "input": "schemas/输入.json",
                    "output": "schemas/输出.json",
                },
            },
        ),
        (
            "plugin-lock.v1.schema.json",
            {
                **lock_document(),
                "files": [{"path": "资料/café.json", "sha256": "0" * 64}],
            },
        ),
    ],
)
def test_relative_paths_accept_normal_unicode_names(
    schema_name: str, document: dict[str, object]
) -> None:
    validator(schema_name).validate(document)
