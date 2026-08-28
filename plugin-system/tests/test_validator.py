from __future__ import annotations

import inspect
import json
import shutil
from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from edgecitadel_supervisor.errors import (
    CompatibilityError,
    DuplicateSkillError,
    ManifestLoadError,
    ManifestValidationError,
    SkillDiscoveryError,
    UnsafePackagePathError,
)
from edgecitadel_supervisor.validator import (
    SUPERVISOR_API_VERSION,
    SUPPORTED_PROTOCOLS,
    PackageRecord,
    SkillRecord,
    discover_skills,
    validate_package,
    validate_schema,
    validate_skill_metadata,
)


def _load_manifest(root: Path) -> dict[str, object]:
    document = yaml.safe_load((root / "plugin.yaml").read_text())
    assert isinstance(document, dict)
    return document


def _write_manifest(root: Path, document: dict[str, object]) -> None:
    (root / "plugin.yaml").write_text(yaml.safe_dump(document, sort_keys=False))


def _replace_compatibility(root: Path, value: object, field: str) -> None:
    document = _load_manifest(root)
    compatibility = document["compatibility"]
    assert isinstance(compatibility, dict)
    compatibility[field] = value
    _write_manifest(root, document)


def _replace_protocols(root: Path, protocols: list[str]) -> None:
    _replace_compatibility(root, protocols, "protocols")


def _replace_agent_skills(root: Path, skill_names: list[str]) -> None:
    document = _load_manifest(root)
    agents = document["agents"]
    assert isinstance(agents, list)
    agent = agents[0]
    assert isinstance(agent, dict)
    agent["skillNames"] = skill_names
    _write_manifest(root, document)


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


def test_validate_package_returns_package_and_skill_records(
    valid_package: Path,
) -> None:
    original_manifest = _load_manifest(valid_package)

    package = validate_package(str(valid_package), verify_integrity=False)

    assert isinstance(package, PackageRecord)
    assert package.root == valid_package.resolve()
    assert package.manifest == original_manifest
    assert package.package_id == "local.example"
    assert package.package_version == "0.1.0"
    assert package.protocol == "edgecitadel.plugin.v1"
    assert [skill.name for skill in package.skills] == ["placeholder"]
    assert package.agent_skill_names == {"example-agent": ("placeholder",)}
    assert _load_manifest(valid_package) == original_manifest


def test_package_record_is_frozen(valid_package: Path) -> None:
    package = validate_package(valid_package, verify_integrity=False)

    with pytest.raises(FrozenInstanceError):
        package.package_id = "renamed"  # type: ignore[misc]


def test_agent_skill_mapping_does_not_alias_manifest(valid_package: Path) -> None:
    package = validate_package(valid_package, verify_integrity=False)
    agents = package.manifest["agents"]
    assert isinstance(agents, list)
    agent = agents[0]
    assert isinstance(agent, dict)
    skill_names = agent["skillNames"]
    assert isinstance(skill_names, list)

    skill_names.append("missing")

    assert package.agent_skill_names == {"example-agent": ("placeholder",)}


def test_package_record_declares_exact_public_fields() -> None:
    assert tuple(PackageRecord.__dataclass_fields__) == (
        "root",
        "manifest",
        "package_id",
        "package_version",
        "protocol",
        "skills",
        "agent_skill_names",
    )


def test_package_compatibility_constants_are_exact() -> None:
    assert str(SUPERVISOR_API_VERSION) == "0.1.0"
    assert SUPPORTED_PROTOCOLS == frozenset({"edgecitadel.plugin.v1"})


def test_verify_integrity_is_forward_compatible_and_defaults_true(
    valid_package: Path,
) -> None:
    parameter = inspect.signature(validate_package).parameters["verify_integrity"]

    assert parameter.default is True
    assert validate_package(valid_package).package_id == "local.example"


def test_rejects_unsupported_supervisor_api(valid_package: Path) -> None:
    _replace_compatibility(valid_package, ">=9.0.0", "supervisorApi")

    with pytest.raises(CompatibilityError, match="supervisor API"):
        validate_package(valid_package, verify_integrity=False)


def test_rejects_malformed_supervisor_api_without_leaking_value(
    valid_package: Path,
) -> None:
    _replace_compatibility(valid_package, "do-not-leak", "supervisorApi")

    with pytest.raises(ManifestValidationError, match="supervisor API") as error:
        validate_package(valid_package, verify_integrity=False)

    assert "do-not-leak" not in str(error.value)


@pytest.mark.parametrize("supervisor_api", ["   ", "===not-a-version"])
def test_rejects_invalid_supervisor_api_forms_with_stable_redacted_error(
    valid_package: Path, supervisor_api: str
) -> None:
    _replace_compatibility(valid_package, supervisor_api, "supervisorApi")

    with pytest.raises(ManifestValidationError, match="supervisor API") as error:
        validate_package(valid_package, verify_integrity=False)

    assert supervisor_api not in str(error.value)


def test_rejects_unsupported_process_protocol(valid_package: Path) -> None:
    _replace_protocols(valid_package, ["edgecitadel.plugin.v9"])

    with pytest.raises(CompatibilityError, match="process protocol"):
        validate_package(valid_package, verify_integrity=False)


def test_selects_only_supported_declared_protocol(valid_package: Path) -> None:
    _replace_protocols(
        valid_package,
        ["vendor.plugin.v2", "edgecitadel.plugin.v1", "vendor.plugin.v1"],
    )

    package = validate_package(valid_package, verify_integrity=False)

    assert package.protocol == "edgecitadel.plugin.v1"


def test_rejects_ambiguous_supported_protocols(
    valid_package: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _replace_protocols(
        valid_package,
        ["edgecitadel.plugin.v1", "edgecitadel.plugin.v2"],
    )
    monkeypatch.setattr(
        "edgecitadel_supervisor.validator.SUPPORTED_PROTOCOLS",
        frozenset({"edgecitadel.plugin.v1", "edgecitadel.plugin.v2"}),
    )

    with pytest.raises(CompatibilityError, match="process protocol"):
        validate_package(valid_package, verify_integrity=False)


def test_duplicate_declared_protocols_remain_schema_invalid(
    valid_package: Path,
) -> None:
    _replace_protocols(
        valid_package,
        ["edgecitadel.plugin.v1", "edgecitadel.plugin.v1"],
    )

    with pytest.raises(ManifestValidationError, match="agent-plugin"):
        validate_package(valid_package, verify_integrity=False)


def test_duplicate_agent_ids_are_rejected_explicitly(valid_package: Path) -> None:
    document = _load_manifest(valid_package)
    agents = document["agents"]
    assert isinstance(agents, list)
    agents.append(dict(agents[0]))
    _write_manifest(valid_package, document)

    with pytest.raises(ManifestValidationError, match="Duplicate agent ID"):
        validate_package(valid_package, verify_integrity=False)


def test_rejects_unknown_agent_skill_name(valid_package: Path) -> None:
    _replace_agent_skills(valid_package, ["missing"])

    with pytest.raises(ManifestValidationError, match="unknown skill"):
        validate_package(valid_package, verify_integrity=False)


def test_agent_skill_mapping_preserves_declared_order(valid_package: Path) -> None:
    _copy_skill(valid_package, "second", "second")
    binding = _load_binding(valid_package, "second")
    binding["skillId"] = "example.second"
    _write_binding(valid_package, binding, "second")
    _replace_agent_skills(valid_package, ["second", "placeholder"])

    package = validate_package(valid_package, verify_integrity=False)

    assert package.agent_skill_names == {"example-agent": ("second", "placeholder")}


def test_empty_agent_skill_names_are_allowed_by_schema(valid_package: Path) -> None:
    _replace_agent_skills(valid_package, [])

    package = validate_package(valid_package, verify_integrity=False)

    assert package.agent_skill_names == {"example-agent": ()}


def test_duplicate_agent_skill_names_remain_schema_invalid(
    valid_package: Path,
) -> None:
    _replace_agent_skills(valid_package, ["placeholder", "placeholder"])

    with pytest.raises(ManifestValidationError, match="agent-plugin"):
        validate_package(valid_package, verify_integrity=False)


@pytest.mark.parametrize("contents", ["[", "- not-a-mapping\n"])
def test_malformed_plugin_manifest_uses_redacted_package_relative_error(
    valid_package: Path, contents: str
) -> None:
    (valid_package / "plugin.yaml").write_text(contents)

    with pytest.raises(ManifestLoadError, match="plugin.yaml") as error:
        validate_package(valid_package, verify_integrity=False)

    _assert_package_relative_error(error.value, valid_package, "plugin.yaml")
    assert contents.strip() not in str(error.value)


def test_missing_plugin_manifest_uses_redacted_package_relative_error(
    valid_package: Path,
) -> None:
    (valid_package / "plugin.yaml").unlink()

    with pytest.raises(ManifestLoadError, match="plugin.yaml") as error:
        validate_package(valid_package, verify_integrity=False)

    _assert_package_relative_error(error.value, valid_package, "plugin.yaml")


def test_nested_non_string_manifest_key_is_rejected_by_loader(
    valid_package: Path,
) -> None:
    document = _load_manifest(valid_package)
    document["extensions"] = {1: {}}
    _write_manifest(valid_package, document)

    with pytest.raises(ManifestLoadError, match="plugin.yaml") as error:
        validate_package(valid_package, verify_integrity=False)

    _assert_package_relative_error(error.value, valid_package, "plugin.yaml")
    assert "1" not in str(error.value)


def test_non_json_manifest_extension_is_rejected_by_loader(
    valid_package: Path,
) -> None:
    document = _load_manifest(valid_package)
    document["extensions"] = {"value": {"do-not-leak"}}
    _write_manifest(valid_package, document)

    with pytest.raises(ManifestLoadError, match="plugin.yaml") as error:
        validate_package(valid_package, verify_integrity=False)

    _assert_package_relative_error(error.value, valid_package, "plugin.yaml")
    assert "do-not-leak" not in str(error.value)


def test_symlinked_package_root_is_rejected_before_manifest_read(
    valid_package: Path, tmp_path: Path
) -> None:
    (valid_package / "plugin.yaml").write_text("[")
    symlink = tmp_path / "package-link"
    symlink.symlink_to(valid_package, target_is_directory=True)

    with pytest.raises(UnsafePackagePathError, match="symbolic link") as error:
        validate_package(symlink, verify_integrity=False)

    assert str(error.value).endswith(": .")
    assert str(symlink) not in str(error.value)
    assert str(valid_package) not in str(error.value)


def test_nested_symlink_is_rejected_before_manifest_read(
    valid_package: Path, tmp_path: Path
) -> None:
    (valid_package / "plugin.yaml").write_text("[")
    target = tmp_path / "outside"
    target.write_text("outside")
    (valid_package / "nested-link").symlink_to(target)

    with pytest.raises(UnsafePackagePathError, match="symbolic link") as error:
        validate_package(valid_package, verify_integrity=False)

    assert "nested-link" in str(error.value)
    assert str(valid_package) not in str(error.value)


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


def test_malformed_skill_frontmatter_uses_package_relative_path(
    valid_package: Path,
) -> None:
    skill = valid_package / "skills" / "placeholder" / "SKILL.md"
    skill.write_text("---\nname: [\n---\nbody\n")

    with pytest.raises(ManifestLoadError, match="SKILL.md") as error:
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


def test_skills_directory_inspection_error_uses_package_relative_path(
    valid_package: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    skills = (valid_package / "skills").resolve()
    original_iterdir = Path.iterdir

    def fail_for_skills(path: Path) -> Iterator[Path]:
        if path == skills:
            raise OSError("do-not-leak")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_for_skills)

    with pytest.raises(SkillDiscoveryError, match="skills") as error:
        discover_skills(valid_package, "skills")

    _assert_package_relative_error(error.value, valid_package, "skills")
    assert "do-not-leak" not in str(error.value)


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
