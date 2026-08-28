from __future__ import annotations

import json
from pathlib import Path

import pytest

from edgecitadel_supervisor.errors import (
    CompatibilityError,
    DuplicateSkillError,
    LockIntegrityError,
    ManifestLoadError,
    ManifestValidationError,
    PluginError,
    PluginNotFoundError,
    SkillDiscoveryError,
    UnsafePackagePathError,
)
from edgecitadel_supervisor.loader import (
    load_json,
    load_skill_markdown,
    load_yaml,
    reject_symlinks,
    require_plugin_root,
    resolve_package_path,
)


@pytest.mark.parametrize(
    "error_type",
    [
        PluginNotFoundError,
        ManifestLoadError,
        ManifestValidationError,
        CompatibilityError,
        UnsafePackagePathError,
        SkillDiscoveryError,
        DuplicateSkillError,
        LockIntegrityError,
    ],
)
def test_domain_errors_inherit_plugin_error(error_type: type[PluginError]) -> None:
    assert issubclass(error_type, PluginError)
    assert issubclass(error_type, RuntimeError)


def test_require_plugin_root_rejects_missing_directory(tmp_path: Path) -> None:
    missing = tmp_path / "missing-plugin"

    with pytest.raises(PluginNotFoundError, match="missing-plugin"):
        require_plugin_root(missing)


def test_require_plugin_root_rejects_regular_file(tmp_path: Path) -> None:
    root = tmp_path / "plugin"
    root.write_text("not a directory")

    with pytest.raises(PluginNotFoundError, match="not a directory"):
        require_plugin_root(root)


def test_require_plugin_root_returns_resolved_directory(tmp_path: Path) -> None:
    root = tmp_path / "plugin"
    root.mkdir()

    assert require_plugin_root(root) == root.resolve()


def test_require_plugin_root_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "plugin"
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(UnsafePackagePathError, match="symbolic link"):
        require_plugin_root(root)


def test_load_yaml_wraps_parser_failure(tmp_path: Path) -> None:
    path = tmp_path / "plugin.yaml"
    malformed = "metadata: ["
    path.write_text(malformed)

    with pytest.raises(ManifestLoadError) as error:
        load_yaml(path)

    assert path.name in str(error.value)
    assert malformed not in str(error.value)


def test_load_yaml_wraps_parser_recursion_failure(tmp_path: Path) -> None:
    path = tmp_path / "plugin.yaml"
    path.write_text("[" * 1_500 + "]" * 1_500)

    with pytest.raises(ManifestLoadError, match="plugin.yaml"):
        load_yaml(path)


def test_load_yaml_rejects_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "plugin.yaml"
    path.write_text("- one\n- two\n")

    with pytest.raises(ManifestLoadError, match="mapping.*plugin.yaml"):
        load_yaml(path)


def test_load_yaml_rejects_non_string_root_keys(tmp_path: Path) -> None:
    path = tmp_path / "plugin.yaml"
    path.write_text("1: value\n")

    with pytest.raises(ManifestLoadError, match="string keys.*plugin.yaml"):
        load_yaml(path)


def test_load_yaml_returns_mapping(tmp_path: Path) -> None:
    path = tmp_path / "plugin.yaml"
    path.write_text("metadata:\n  name: example\nenabled: true\n")

    assert load_yaml(path) == {
        "metadata": {"name": "example"},
        "enabled": True,
    }


def test_load_json_wraps_parser_failure(tmp_path: Path) -> None:
    path = tmp_path / "plugin.lock.json"
    malformed = '{"lockVersion":'
    path.write_text(malformed)

    with pytest.raises(ManifestLoadError) as error:
        load_json(path)

    assert path.name in str(error.value)
    assert malformed not in str(error.value)


def test_load_json_wraps_parser_recursion_failure(tmp_path: Path) -> None:
    path = tmp_path / "plugin.lock.json"
    path.write_text("[" * 500_000 + "]" * 500_000)

    with pytest.raises(ManifestLoadError, match="plugin.lock.json"):
        load_json(path)


def test_load_json_rejects_non_mapping_root(tmp_path: Path) -> None:
    path = tmp_path / "plugin.lock.json"
    path.write_text(json.dumps(["one", "two"]))

    with pytest.raises(ManifestLoadError, match="mapping.*plugin.lock.json"):
        load_json(path)


def test_load_json_returns_mapping(tmp_path: Path) -> None:
    path = tmp_path / "plugin.lock.json"
    document = {"lockVersion": 1, "files": ["plugin.yaml"]}
    path.write_text(json.dumps(document))

    assert load_json(path) == document


def test_load_skill_markdown_rejects_missing_frontmatter(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    path.write_text("# Procedure\n")

    with pytest.raises(ManifestLoadError, match="frontmatter.*SKILL.md"):
        load_skill_markdown(path)


def test_load_skill_markdown_rejects_crlf_frontmatter(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    path.write_bytes(b"---\r\nname: example\r\n---\r\n# Procedure\r\n")

    with pytest.raises(ManifestLoadError, match="frontmatter.*SKILL.md"):
        load_skill_markdown(path)


def test_load_skill_markdown_rejects_malformed_frontmatter(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    malformed = "name: ["
    path.write_text(f"---\n{malformed}\n---\n# Procedure\n")

    with pytest.raises(ManifestLoadError) as error:
        load_skill_markdown(path)

    assert path.name in str(error.value)
    assert malformed not in str(error.value)


def test_load_skill_markdown_wraps_parser_recursion_failure(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    nested = "[" * 1_500 + "]" * 1_500
    path.write_text(f"---\nmetadata: {nested}\n---\n# Procedure\n")

    with pytest.raises(ManifestLoadError, match="frontmatter.*SKILL.md"):
        load_skill_markdown(path)


def test_load_skill_markdown_rejects_unclosed_frontmatter(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    path.write_text("---\nname: example\n# Procedure\n")

    with pytest.raises(ManifestLoadError, match="frontmatter.*SKILL.md"):
        load_skill_markdown(path)


def test_load_skill_markdown_rejects_non_string_frontmatter_keys(
    tmp_path: Path,
) -> None:
    path = tmp_path / "SKILL.md"
    path.write_text("---\n1: value\n---\n# Procedure\n")

    with pytest.raises(ManifestLoadError, match="string keys.*SKILL.md"):
        load_skill_markdown(path)


def test_load_skill_markdown_returns_frontmatter_and_body(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    path.write_text(
        "---\nname: example\ndescription: Use for examples.\n---\n# Procedure\n"
    )

    metadata, body = load_skill_markdown(path)

    assert metadata == {
        "name": "example",
        "description": "Use for examples.",
    }
    assert body == "# Procedure\n"


def test_load_skill_markdown_preserves_body_newlines(tmp_path: Path) -> None:
    path = tmp_path / "SKILL.md"
    path.write_bytes(b"---\nname: example\n---\n# Procedure\r\n")

    _, body = load_skill_markdown(path)

    assert body == "# Procedure\r\n"


def test_resolve_package_path_rejects_absolute_path(tmp_path: Path) -> None:
    absolute = str((tmp_path / "outside").resolve())

    with pytest.raises(UnsafePackagePathError, match="absolute"):
        resolve_package_path(tmp_path, absolute)


def test_resolve_package_path_rejects_parent_traversal(tmp_path: Path) -> None:
    with pytest.raises(UnsafePackagePathError, match="outside package"):
        resolve_package_path(tmp_path, "../secret")


def test_resolve_package_path_wraps_invalid_path(tmp_path: Path) -> None:
    relative = "bad\0name"

    with pytest.raises(UnsafePackagePathError, match="resolve") as error:
        resolve_package_path(tmp_path, relative)

    assert repr(relative) in str(error.value)


def test_resolve_package_path_uses_base_within_root(tmp_path: Path) -> None:
    base = tmp_path / "skills" / "example"

    assert (
        resolve_package_path(tmp_path, "schemas/input.json", base=base)
        == (base / "schemas" / "input.json").resolve()
    )


def test_reject_symlinks_finds_nested_link(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_text("data")
    nested = tmp_path / "skills"
    nested.mkdir()
    (nested / "escape").symlink_to(target)

    with pytest.raises(UnsafePackagePathError, match="symbolic link"):
        reject_symlinks(tmp_path)


def test_reject_symlinks_accepts_clean_nested_tree(tmp_path: Path) -> None:
    skill = tmp_path / "skills" / "example"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("# Procedure\n")

    reject_symlinks(tmp_path)
