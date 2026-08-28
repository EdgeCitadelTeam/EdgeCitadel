from __future__ import annotations

import json
import shutil
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from edgecitadel_supervisor.errors import (
    DuplicateSkillError,
    ManifestLoadError,
    ManifestValidationError,
    SkillDiscoveryError,
    UnsafePackagePathError,
)
from edgecitadel_supervisor.validator import (
    SkillRecord,
    discover_skills,
    validate_schema,
    validate_skill_metadata,
)


def _load_binding(root: Path, skill_name: str = "placeholder") -> dict[str, object]:
    path = root / "skills" / skill_name / "binding.yaml"
    document = yaml.safe_load(path.read_text())
    assert isinstance(document, dict)
    return document


def _write_binding(
    root: Path, document: dict[str, object], skill_name: str = "placeholder"
) -> None:
    path = root / "skills" / skill_name / "binding.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def _copy_skill(valid_package: Path, directory_name: str, portable_name: str) -> Path:
    source = valid_package / "skills" / "placeholder"
    destination = valid_package / "skills" / directory_name
    shutil.copytree(source, destination)
    skill_file = destination / "SKILL.md"
    skill_file.write_text(
        skill_file.read_text().replace("name: placeholder", f"name: {portable_name}")
    )
    return destination


def _assert_package_relative_error(
    error: Exception, root: Path, expected_path: str
) -> None:
    message = str(error)
    assert expected_path in message
    assert str(root) not in message


def test_discover_skills_combines_portable_and_binding_metadata(
    valid_package: Path,
) -> None:
    skills = discover_skills(valid_package, "skills")

    assert [(skill.name, skill.skill_id, skill.execution_name) for skill in skills] == [
        ("placeholder", "example.placeholder", "placeholder")
    ]
    assert skills[0].description == "Use when validating an example package."
    assert skills[0].version == "0.1.0"
    assert skills[0].directory == valid_package / "skills" / "placeholder"
    assert skills[0].skill_file == skills[0].directory / "SKILL.md"
    assert skills[0].binding_file == skills[0].directory / "binding.yaml"
    assert skills[0].input_schema == skills[0].directory / "schemas/input.json"
    assert skills[0].output_schema == skills[0].directory / "schemas/output.json"


def test_skill_record_is_frozen(valid_package: Path) -> None:
    skill = discover_skills(valid_package, "skills")[0]

    with pytest.raises(FrozenInstanceError):
        skill.name = "renamed"  # type: ignore[misc]


def test_skill_directory_must_match_frontmatter_name(valid_package: Path) -> None:
    skill = valid_package / "skills" / "placeholder" / "SKILL.md"
    skill.write_text(skill.read_text().replace("name: placeholder", "name: renamed"))

    with pytest.raises(ManifestValidationError, match="directory name"):
        discover_skills(valid_package, "skills")


def test_duplicate_portable_skill_names_are_rejected(valid_package: Path) -> None:
    _copy_skill(valid_package, "second", "placeholder")
    binding = _load_binding(valid_package, "second")
    binding["skillId"] = "example.second"
    _write_binding(valid_package, binding, "second")

    with pytest.raises(DuplicateSkillError, match="placeholder"):
        discover_skills(valid_package, "skills")


def test_duplicate_a2a_skill_ids_are_rejected(valid_package: Path) -> None:
    _copy_skill(valid_package, "second", "second")

    with pytest.raises(DuplicateSkillError, match="example.placeholder"):
        discover_skills(valid_package, "skills")


def test_schema_reference_cannot_escape_skill_directory(valid_package: Path) -> None:
    binding = valid_package / "skills" / "placeholder" / "binding.yaml"
    binding.write_text(
        binding.read_text().replace("schemas/input.json", "../../plugin.yaml")
    )

    with pytest.raises(UnsafePackagePathError):
        discover_skills(valid_package, "skills")


def test_absolute_schema_reference_is_rejected(valid_package: Path) -> None:
    document = _load_binding(valid_package)
    schemas = document["schemas"]
    assert isinstance(schemas, dict)
    schemas["input"] = str((valid_package / "plugin.yaml").resolve())
    _write_binding(valid_package, document)

    with pytest.raises(UnsafePackagePathError, match="absolute"):
        discover_skills(valid_package, "skills")


def test_skill_records_are_sorted_by_portable_name(valid_package: Path) -> None:
    _copy_skill(valid_package, "alpha", "alpha")
    binding = _load_binding(valid_package, "alpha")
    binding["skillId"] = "example.alpha"
    _write_binding(valid_package, binding, "alpha")

    assert [skill.name for skill in discover_skills(valid_package, "skills")] == [
        "alpha",
        "placeholder",
    ]


@pytest.mark.parametrize(
    "name",
    [
        "Uppercase",
        "leading-",
        "-trailing",
        "double--hyphen",
        "with_underscore",
        "a" * 65,
        "",
    ],
)
def test_agent_skill_name_must_use_portable_grammar(name: str) -> None:
    with pytest.raises(ManifestValidationError, match="name"):
        validate_skill_metadata({"name": name, "description": "Valid."}, name)


@pytest.mark.parametrize("name", ["a", "a" * 64, "one-two-3"])
def test_agent_skill_name_accepts_valid_limits(name: str) -> None:
    validate_skill_metadata({"name": name, "description": "Valid."}, name)


@pytest.mark.parametrize("description", [None, 1, "", " " * 4, "a" * 1025])
def test_agent_skill_description_must_be_nonempty_and_bounded(
    description: object,
) -> None:
    with pytest.raises(ManifestValidationError, match="description"):
        validate_skill_metadata(
            {"name": "placeholder", "description": description}, "placeholder"
        )


def test_agent_skill_description_accepts_1024_characters() -> None:
    validate_skill_metadata(
        {"name": "placeholder", "description": "a" * 1024}, "placeholder"
    )


def test_empty_markdown_body_is_rejected(valid_package: Path) -> None:
    skill = valid_package / "skills" / "placeholder" / "SKILL.md"
    skill.write_text(
        "---\nname: placeholder\n"
        "description: Use when validating an example package.\n---\n  \n\t\n"
    )

    with pytest.raises(ManifestValidationError, match="Markdown body") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/SKILL.md"
    )


def test_binding_is_validated_with_format_checker(valid_package: Path) -> None:
    document = _load_binding(valid_package)
    document["extensions"] = {"x:<": {}}
    _write_binding(valid_package, document)

    with pytest.raises(ManifestValidationError, match="binding.yaml") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/binding.yaml"
    )


def test_malformed_binding_error_uses_package_relative_path(
    valid_package: Path,
) -> None:
    binding = valid_package / "skills" / "placeholder" / "binding.yaml"
    binding.write_text("[")

    with pytest.raises(ManifestLoadError, match="binding.yaml") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/binding.yaml"
    )


def test_missing_referenced_schema_is_rejected(valid_package: Path) -> None:
    (valid_package / "skills" / "placeholder" / "schemas/input.json").unlink()

    with pytest.raises(ManifestLoadError, match="input.json") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/schemas/input.json"
    )


@pytest.mark.parametrize("contents", ["[]", "not-json"])
def test_referenced_schema_must_be_a_json_mapping(
    valid_package: Path, contents: str
) -> None:
    schema = valid_package / "skills" / "placeholder" / "schemas/input.json"
    schema.write_text(contents)

    with pytest.raises(ManifestLoadError, match="input.json") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/schemas/input.json"
    )


def test_referenced_schema_must_be_valid_draft_2020_12_schema(
    valid_package: Path,
) -> None:
    schema = valid_package / "skills" / "placeholder" / "schemas/input.json"
    schema.write_text(json.dumps({"type": 7}))

    with pytest.raises(ManifestValidationError, match="input.json") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/placeholder/schemas/input.json"
    )


def test_validate_schema_uses_authoritative_editable_source_layout(
    valid_package: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(valid_package)
    validate_schema(
        _load_binding(valid_package), "agent-skill-binding.v1alpha1.schema.json"
    )


def test_validate_schema_rejects_unknown_schema_name_safely() -> None:
    with pytest.raises(ManifestValidationError, match="unknown.schema.json") as error:
        validate_schema({"secret": "do-not-leak"}, "unknown.schema.json")

    assert "do-not-leak" not in str(error.value)


def test_validate_schema_error_does_not_include_document_content() -> None:
    document = {"secret": "do-not-leak"}

    with pytest.raises(ManifestValidationError) as error:
        validate_schema(document, "agent-skill-binding.v1alpha1.schema.json")

    assert "do-not-leak" not in str(error.value)


def test_symlinked_skill_content_is_rejected(valid_package: Path) -> None:
    target = valid_package / "target.json"
    target.write_text("{}")
    schema = valid_package / "skills" / "placeholder" / "schemas/input.json"
    schema.unlink()
    schema.symlink_to(target)

    with pytest.raises(UnsafePackagePathError, match="symbolic link"):
        discover_skills(valid_package, "skills")


def test_partial_skill_directory_is_rejected_deterministically(
    valid_package: Path,
) -> None:
    partial = valid_package / "skills" / "partial"
    partial.mkdir()
    (partial / "SKILL.md").write_text("---\nname: partial\n---\nbody\n")

    with pytest.raises(SkillDiscoveryError, match="partial.*binding.yaml") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/partial/binding.yaml"
    )


def test_non_directory_entries_are_ignored(valid_package: Path) -> None:
    (valid_package / "skills" / "README.md").write_text("Skills")

    skills = discover_skills(valid_package, "skills")

    assert [skill.name for skill in skills] == ["placeholder"]


def test_immediate_directory_with_only_nested_skill_is_rejected(
    valid_package: Path,
) -> None:
    nested = valid_package / "skills" / "container" / "nested"
    nested.mkdir(parents=True)
    (nested / "SKILL.md").write_text("invalid")
    (nested / "binding.yaml").write_text("invalid")

    with pytest.raises(SkillDiscoveryError, match="SKILL.md.*binding.yaml") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(error.value, valid_package, "skills/container")


def test_skill_directory_missing_skill_file_is_rejected(valid_package: Path) -> None:
    partial = valid_package / "skills" / "partial"
    partial.mkdir()
    (partial / "binding.yaml").write_text("invalid")

    with pytest.raises(SkillDiscoveryError, match="partial.*SKILL.md") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(
        error.value, valid_package, "skills/partial/SKILL.md"
    )


def test_missing_skills_directory_is_rejected(valid_package: Path) -> None:
    with pytest.raises(SkillDiscoveryError, match="missing") as error:
        discover_skills(valid_package, "missing")

    _assert_package_relative_error(error.value, valid_package, "missing")


def test_skill_record_declares_exact_public_fields() -> None:
    assert tuple(SkillRecord.__dataclass_fields__) == (
        "name",
        "description",
        "skill_id",
        "version",
        "execution_name",
        "directory",
        "skill_file",
        "binding_file",
        "input_schema",
        "output_schema",
    )
