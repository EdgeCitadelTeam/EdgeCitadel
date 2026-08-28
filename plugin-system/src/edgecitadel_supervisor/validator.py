"""Non-executing validation for portable skills and EdgeCitadel bindings."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from .errors import (
    DuplicateSkillError,
    ManifestLoadError,
    ManifestValidationError,
    SkillDiscoveryError,
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
_BINDING_SCHEMA = "agent-skill-binding.v1alpha1.schema.json"
_SCHEMA_DIRECTORY = Path(__file__).resolve().parents[2] / "schemas"


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


def validate_schema(document: object, schema_name: str) -> None:
    """Validate a document with an authoritative bundled project schema."""
    if Path(schema_name).name != schema_name:
        raise ManifestValidationError(f"Unknown validation schema: {schema_name}")

    schema_path = _SCHEMA_DIRECTORY / schema_name
    try:
        schema_document = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, RecursionError, UnicodeError, ValueError, json.JSONDecodeError):
        raise ManifestValidationError(
            f"Unable to load validation schema: {schema_name}"
        ) from None

    if not isinstance(schema_document, dict):
        raise ManifestValidationError(
            f"Validation schema is not an object: {schema_name}"
        )

    try:
        Draft202012Validator.check_schema(schema_document)
        Draft202012Validator(
            schema_document,
            format_checker=Draft202012Validator.FORMAT_CHECKER,
        ).validate(document)
    except SchemaError:
        raise ManifestValidationError(
            f"Invalid authoritative validation schema: {schema_name}"
        ) from None
    except ValidationError as error:
        location = ".".join(str(component) for component in error.absolute_path)
        context = f" at {location}" if location else ""
        raise ManifestValidationError(
            f"Document failed validation against {schema_name}{context}"
        ) from None


def validate_skill_metadata(metadata: dict[str, object], directory_name: str) -> None:
    """Validate the required portable Agent Skills frontmatter fields."""
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
            f"Agent Skill name must match its directory name: {directory_name}"
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
            f"Unable to inspect skills directory: {skill_root}"
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

        metadata, body = load_skill_markdown(skill_file)
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
            f"Referenced JSON Schema is invalid: {path}"
        ) from None


def _load_referenced_schema(root: Path, path: Path) -> dict[str, object]:
    relative_path = _package_relative(root, path)
    try:
        return load_json(path)
    except ManifestLoadError:
        raise ManifestLoadError(
            f"Unable to load referenced JSON Schema: {relative_path}"
        ) from None


def _package_relative(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()
