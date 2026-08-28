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
        raise SkillDiscoveryError(f"Skills directory does not exist: {skill_root}")

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
        has_skill_file = skill_file.is_file()
        has_binding_file = binding_file.is_file()
        if not has_skill_file and not has_binding_file:
            continue
        if not has_skill_file:
            raise SkillDiscoveryError(
                f"Skill directory {directory.name} is missing SKILL.md: {directory}"
            )
        if not has_binding_file:
            raise SkillDiscoveryError(
                f"Skill directory {directory.name} is missing binding.yaml: {directory}"
            )

        metadata, body = load_skill_markdown(skill_file)
        name_value = metadata.get("name")
        if isinstance(name_value, str) and name_value in portable_names:
            raise DuplicateSkillError(f"Duplicate portable skill name: {name_value}")
        validate_skill_metadata(metadata, directory.name)
        name = cast(str, name_value)
        description = cast(str, metadata["description"])
        portable_names.add(name)
        if not body.strip():
            raise ManifestValidationError(
                f"Agent Skill Markdown body must be nonempty: {skill_file}"
            )

        binding = load_yaml(binding_file)
        try:
            input_schema, output_schema = _resolve_binding_schema_paths(
                directory, binding
            )
            validate_schema(binding, _BINDING_SCHEMA)
        except ManifestValidationError as error:
            raise ManifestValidationError(
                f"Invalid skill binding at {binding_file}: {error}"
            ) from None

        skill_id = cast(str, binding["skillId"])
        if skill_id in skill_ids:
            raise DuplicateSkillError(f"Duplicate A2A skill ID: {skill_id}")
        skill_ids.add(skill_id)

        input_document = load_json(input_schema)
        output_document = load_json(output_schema)
        _validate_referenced_schema(input_document, input_schema)
        _validate_referenced_schema(output_document, output_schema)

        execution = cast(dict[str, object], binding["execution"])
        records.append(
            SkillRecord(
                name=name,
                description=description,
                skill_id=skill_id,
                version=cast(str, binding["version"]),
                execution_name=cast(str, execution["name"]),
                directory=directory,
                skill_file=skill_file,
                binding_file=binding_file,
                input_schema=input_schema,
                output_schema=output_schema,
            )
        )

    return tuple(sorted(records, key=lambda record: record.name))


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


def _validate_referenced_schema(document: dict[str, object], path: Path) -> None:
    try:
        Draft202012Validator.check_schema(document)
    except SchemaError:
        raise ManifestValidationError(
            f"Referenced JSON Schema is invalid: {path}"
        ) from None
