"""Non-executing validation for portable skills and EdgeCitadel bindings."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from .errors import (
    CompatibilityError,
    DuplicateSkillError,
    ManifestLoadError,
    ManifestValidationError,
    SkillDiscoveryError,
    format_path,
)
from .loader import (
    load_json,
    load_skill_markdown,
    load_yaml,
    reject_symlinks,
    require_plugin_root,
    resolve_package_path,
)

_PORTABLE_SKILL_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_PLUGIN_SCHEMA = "agent-plugin.v1alpha1.schema.json"
_BINDING_SCHEMA = "agent-skill-binding.v1alpha1.schema.json"
_SCHEMA_DIRECTORY = Path(__file__).resolve().parents[2] / "schemas"
SUPERVISOR_API_VERSION = Version("0.1.0")
SUPPORTED_PROTOCOLS = frozenset(
    {"edgecitadel.managed-agent.v1", "edgecitadel.plugin.v1"}
)


@dataclass(frozen=True)
class SkillRecord:
    name: str
    description: str
    skill_id: str
    version: str
    execution_name: str
    directory: Path
    skill_file: Path
    binding_file: Path
    input_schema: Path
    output_schema: Path


@dataclass(frozen=True)
class PackageRecord:
    root: Path
    manifest: dict[str, object]
    package_id: str
    package_version: str
    protocol: str
    skills: tuple[SkillRecord, ...]
    agent_skill_names: dict[str, tuple[str, ...]]


def validate_package(
    root: str | Path, *, verify_integrity: bool = True
) -> PackageRecord:
    """Validate a plugin package without importing or executing its contents."""
    plugin_root = require_plugin_root(root)
    reject_symlinks(plugin_root)

    try:
        manifest = load_yaml(plugin_root / "plugin.yaml")
    except ManifestLoadError:
        raise ManifestLoadError("Unable to load plugin manifest: plugin.yaml") from None

    try:
        validate_schema(manifest, _PLUGIN_SCHEMA)
    except ManifestValidationError as error:
        raise ManifestValidationError(
            f"Invalid plugin manifest at plugin.yaml: {error}"
        ) from None

    compatibility = cast(dict[str, object], manifest["compatibility"])
    protocol = _select_compatible_protocol(compatibility)
    _validate_package_protocol(manifest, protocol)

    skills_config = cast(dict[str, object], manifest["skills"])
    skills = discover_skills(plugin_root, cast(str, skills_config["directory"]))
    agents = cast(list[dict[str, object]], manifest["agents"])
    agent_skill_names = _build_agent_skill_names(agents, skills)

    metadata = cast(dict[str, object], manifest["metadata"])
    package = PackageRecord(
        root=plugin_root,
        manifest=manifest,
        package_id=f"{metadata['publisher']}.{metadata['name']}",
        package_version=cast(str, metadata["version"]),
        protocol=protocol,
        skills=skills,
        agent_skill_names=agent_skill_names,
    )
    if verify_integrity:
        from .inventory import verify_lock

        verify_lock(package)
    return package


def _validate_package_protocol(manifest: dict[str, object], protocol: str) -> None:
    kind = cast(str, manifest["kind"])
    required_protocol = {
        "AgentPlugin": "edgecitadel.plugin.v1",
        "ManagedAgent": "edgecitadel.managed-agent.v1",
    }[kind]
    if protocol != required_protocol:
        raise CompatibilityError(
            f"{kind} packages must use the {required_protocol} process protocol"
        )
    agents = cast(list[dict[str, object]], manifest["agents"])
    if kind == "ManagedAgent" and len(agents) != 1:
        raise ManifestValidationError(
            "ManagedAgent packages must declare exactly one Agent identity"
        )


def _select_compatible_protocol(compatibility: dict[str, object]) -> str:
    supervisor_api = cast(str, compatibility["supervisorApi"])
    if not supervisor_api.strip() or supervisor_api.lstrip().startswith("==="):
        raise ManifestValidationError(
            "Plugin manifest has an invalid supervisor API compatibility specifier"
        )
    try:
        supervisor_specifier = SpecifierSet(supervisor_api)
        supports_supervisor_api = SUPERVISOR_API_VERSION in supervisor_specifier
    except (InvalidSpecifier, InvalidVersion):
        raise ManifestValidationError(
            "Plugin manifest has an invalid supervisor API compatibility specifier"
        ) from None
    if not supports_supervisor_api:
        raise CompatibilityError(
            "Plugin does not support the current supervisor API version"
        )

    declared_protocols = cast(list[str], compatibility["protocols"])
    supported_protocols = tuple(
        protocol for protocol in declared_protocols if protocol in SUPPORTED_PROTOCOLS
    )
    if len(supported_protocols) != 1:
        raise CompatibilityError(
            "Plugin must declare exactly one supported process protocol"
        )
    return supported_protocols[0]


def _build_agent_skill_names(
    agents: list[dict[str, object]], skills: tuple[SkillRecord, ...]
) -> dict[str, tuple[str, ...]]:
    portable_skill_names = {skill.name for skill in skills}
    agent_skill_names: dict[str, tuple[str, ...]] = {}
    for agent in agents:
        agent_id = cast(str, agent["id"])
        if agent_id in agent_skill_names:
            raise ManifestValidationError(f"Duplicate agent ID: {agent_id}")

        skill_names = cast(list[str], agent["skillNames"])
        unknown_skill = next(
            (name for name in skill_names if name not in portable_skill_names),
            None,
        )
        if unknown_skill is not None:
            raise ManifestValidationError(
                f"Agent {agent_id} references unknown skill: {unknown_skill}"
            )
        agent_skill_names[agent_id] = tuple(skill_names)
    return agent_skill_names


def validate_schema(document: object, schema_name: str) -> None:
    """Validate a document with an authoritative bundled project schema."""
    if Path(schema_name).name != schema_name:
        raise ManifestValidationError(
            f"Unknown validation schema: {format_path(schema_name)}"
        )

    schema_path = _SCHEMA_DIRECTORY / schema_name
    try:
        schema_document = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, RecursionError, UnicodeError, ValueError, json.JSONDecodeError):
        raise ManifestValidationError(
            f"Unable to load validation schema: {format_path(schema_name)}"
        ) from None

    if not isinstance(schema_document, dict):
        raise ManifestValidationError(
            f"Validation schema is not an object: {format_path(schema_name)}"
        )

    try:
        Draft202012Validator.check_schema(schema_document)
        Draft202012Validator(
            schema_document,
            format_checker=Draft202012Validator.FORMAT_CHECKER,
        ).validate(document)
    except SchemaError:
        raise ManifestValidationError(
            f"Invalid authoritative validation schema: {format_path(schema_name)}"
        ) from None
    except ValidationError as error:
        location = ".".join(str(component) for component in error.absolute_path)
        context = f" at {format_path(location)}" if location else ""
        raise ManifestValidationError(
            f"Document failed validation against {format_path(schema_name)}{context}"
        ) from None


def validate_skill_metadata(metadata: dict[str, object], directory_name: str) -> None:
    """Validate recognized Agent Skills frontmatter fields."""
    name = metadata.get("name")
    if (
        not isinstance(name, str)
        or not 1 <= len(name) <= 64
        or _PORTABLE_SKILL_NAME.fullmatch(name) is None
    ):
        raise ManifestValidationError(
            "Agent Skill name must be 1-64 lowercase alphanumeric characters "
            "or single hyphen-separated components"
        )
    if name != directory_name:
        raise ManifestValidationError(
            "Agent Skill name must match its directory name: "
            f"{format_path(directory_name)}"
        )

    description = metadata.get("description")
    if (
        not isinstance(description, str)
        or not description.strip()
        or len(description) > 1024
    ):
        raise ManifestValidationError(
            "Agent Skill description must be a nonempty string of at most 1024 characters"
        )

    if "license" in metadata and not isinstance(metadata["license"], str):
        raise ManifestValidationError("Agent Skill license must be a string")

    compatibility = metadata.get("compatibility")
    if "compatibility" in metadata and (
        not isinstance(compatibility, str) or len(compatibility) > 500
    ):
        raise ManifestValidationError(
            "Agent Skill compatibility must be a string of at most 500 characters"
        )

    skill_metadata = metadata.get("metadata")
    if "metadata" in metadata and (
        not isinstance(skill_metadata, dict)
        or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in skill_metadata.items()
        )
    ):
        raise ManifestValidationError(
            "Agent Skill metadata must map strings to strings"
        )

    if "allowed-tools" in metadata and not isinstance(metadata["allowed-tools"], str):
        raise ManifestValidationError("Agent Skill allowed-tools must be a string")


def discover_skills(root: Path, skills_directory: str) -> tuple[SkillRecord, ...]:
    """Discover and validate immediate packaged skill directories."""
    plugin_root = require_plugin_root(root)
    reject_symlinks(plugin_root)
    skill_root = resolve_package_path(plugin_root, skills_directory)
    if not skill_root.is_dir():
        raise SkillDiscoveryError(
            f"Skills directory does not exist: {_package_relative(plugin_root, skill_root)}"
        )

    try:
        entries = sorted(skill_root.iterdir(), key=lambda path: path.name)
    except (OSError, RuntimeError, ValueError):
        raise SkillDiscoveryError(
            "Unable to inspect skills directory: "
            f"{_package_relative(plugin_root, skill_root)}"
        ) from None

    records: list[SkillRecord] = []
    portable_names: set[str] = set()
    skill_ids: set[str] = set()

    for directory in entries:
        if not directory.is_dir():
            continue

        skill_file = directory / "SKILL.md"
        binding_file = directory / "binding.yaml"
        relative_directory = _package_relative(plugin_root, directory)
        relative_skill_file = _package_relative(plugin_root, skill_file)
        relative_binding_file = _package_relative(plugin_root, binding_file)
        has_skill_file = skill_file.is_file()
        has_binding_file = binding_file.is_file()
        if not has_skill_file and not has_binding_file:
            raise SkillDiscoveryError(
                "Skill directory is incomplete; missing "
                f"{relative_skill_file} and {relative_binding_file}"
            )
        if not has_skill_file:
            raise SkillDiscoveryError(
                f"Skill directory {relative_directory} is missing {relative_skill_file}"
            )
        if not has_binding_file:
            raise SkillDiscoveryError(
                f"Skill directory {relative_directory} is missing {relative_binding_file}"
            )

        try:
            metadata, body = load_skill_markdown(skill_file)
        except ManifestLoadError:
            raise ManifestLoadError(
                f"Unable to load Agent Skill metadata: {relative_skill_file}"
            ) from None
        name_value = metadata.get("name")
        if isinstance(name_value, str) and name_value in portable_names:
            raise DuplicateSkillError(f"Duplicate portable skill name: {name_value}")
        record = _build_skill_record(
            plugin_root,
            directory,
            metadata,
            body,
        )
        portable_names.add(record.name)
        if record.skill_id in skill_ids:
            raise DuplicateSkillError(f"Duplicate A2A skill ID: {record.skill_id}")
        skill_ids.add(record.skill_id)
        records.append(record)

    return tuple(sorted(records, key=lambda record: record.name))


def _build_skill_record(
    plugin_root: Path,
    directory: Path,
    metadata: dict[str, object],
    body: str,
) -> SkillRecord:
    skill_file = directory / "SKILL.md"
    binding_file = directory / "binding.yaml"
    validate_skill_metadata(metadata, directory.name)
    name = cast(str, metadata["name"])
    description = cast(str, metadata["description"])
    if not body.strip():
        raise ManifestValidationError(
            "Agent Skill Markdown body must be nonempty: "
            f"{_package_relative(plugin_root, skill_file)}"
        )

    relative_binding = _package_relative(plugin_root, binding_file)
    try:
        binding = load_yaml(binding_file)
    except ManifestLoadError:
        raise ManifestLoadError(
            f"Unable to load skill binding: {relative_binding}"
        ) from None
    try:
        input_schema, output_schema = _resolve_binding_schema_paths(directory, binding)
        validate_schema(binding, _BINDING_SCHEMA)
    except ManifestValidationError as error:
        raise ManifestValidationError(
            f"Invalid skill binding at {relative_binding}: {error}"
        ) from None

    skill_metadata = metadata.get("metadata")
    if isinstance(skill_metadata, dict):
        metadata_version = skill_metadata.get("version")
        if metadata_version is not None and metadata_version != binding["version"]:
            raise ManifestValidationError(
                "Agent Skill metadata version must match binding version"
            )

    input_document = _load_referenced_schema(plugin_root, input_schema)
    output_document = _load_referenced_schema(plugin_root, output_schema)
    _validate_referenced_schema(
        input_document, _package_relative(plugin_root, input_schema)
    )
    _validate_referenced_schema(
        output_document, _package_relative(plugin_root, output_schema)
    )

    execution = cast(dict[str, object], binding["execution"])
    return SkillRecord(
        name=name,
        description=description,
        skill_id=cast(str, binding["skillId"]),
        version=cast(str, binding["version"]),
        execution_name=cast(str, execution["name"]),
        directory=directory,
        skill_file=skill_file,
        binding_file=binding_file,
        input_schema=input_schema,
        output_schema=output_schema,
    )


def _resolve_binding_schema_paths(
    skill_directory: Path, binding: dict[str, object]
) -> tuple[Path, Path]:
    schemas = binding.get("schemas")
    if not isinstance(schemas, dict):
        validate_schema(binding, _BINDING_SCHEMA)
        raise ManifestValidationError("Validated binding lacks a schemas mapping")

    input_reference = schemas.get("input")
    output_reference = schemas.get("output")
    if not isinstance(input_reference, str) or not isinstance(output_reference, str):
        validate_schema(binding, _BINDING_SCHEMA)
        raise ManifestValidationError("Validated binding lacks schema references")

    return (
        resolve_package_path(skill_directory, input_reference),
        resolve_package_path(skill_directory, output_reference),
    )


def _validate_referenced_schema(document: dict[str, object], path: str) -> None:
    try:
        Draft202012Validator.check_schema(document)
    except SchemaError:
        raise ManifestValidationError(
            f"Referenced JSON Schema is invalid: {format_path(path)}"
        ) from None

    stack: list[object] = [document]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            for key, nested_value in value.items():
                if (
                    key in {"$ref", "$dynamicRef"}
                    and isinstance(nested_value, str)
                    and not nested_value.startswith("#")
                ):
                    raise ManifestValidationError(
                        "Referenced JSON Schema references must use a local fragment: "
                        f"{format_path(path)}"
                    )
                stack.append(nested_value)
        elif isinstance(value, list):
            stack.extend(value)


def _load_referenced_schema(root: Path, path: Path) -> dict[str, object]:
    relative_path = _package_relative(root, path)
    try:
        return load_json(path)
    except ManifestLoadError:
        raise ManifestLoadError(
            f"Unable to load referenced JSON Schema: {relative_path}"
        ) from None


def _package_relative(root: Path, path: Path) -> str:
    return format_path(path.relative_to(root).as_posix())
