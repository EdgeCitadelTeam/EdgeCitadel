"""Safe, non-executing loaders for plugin package files."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import cast

import yaml

from .errors import ManifestLoadError, PluginNotFoundError, UnsafePackagePathError


def require_plugin_root(path: str | Path) -> Path:
    """Return the resolved plugin directory or raise a stable domain error."""
    candidate = Path(path)
    try:
        if candidate.is_symlink():
            raise UnsafePackagePathError("Plugin root must not be a symbolic link: .")
        root = candidate.resolve(strict=False)
        exists = root.exists()
        is_directory = root.is_dir()
    except UnsafePackagePathError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise PluginNotFoundError(
            f"Unable to inspect plugin root: {candidate}"
        ) from None

    if not exists:
        raise PluginNotFoundError(f"Plugin root does not exist: {root}")
    if not is_directory:
        raise PluginNotFoundError(f"Plugin root is not a directory: {root}")
    return root


def load_yaml(path: Path) -> dict[str, object]:
    """Load a YAML file whose document root must be a mapping."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, RecursionError, UnicodeError, ValueError, yaml.YAMLError):
        raise ManifestLoadError(f"Unable to load YAML file: {path}") from None

    if not isinstance(document, dict):
        raise ManifestLoadError(f"Expected a mapping in YAML file: {path}")
    try:
        _require_json_compatible_value(document, "YAML file", path)
    except RecursionError:
        raise ManifestLoadError(f"Unable to load YAML file: {path}") from None
    return document


def load_json(path: Path) -> dict[str, object]:
    """Load a JSON file whose document root must be an object mapping."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, RecursionError, UnicodeError, ValueError, json.JSONDecodeError):
        raise ManifestLoadError(f"Unable to load JSON file: {path}") from None

    if not isinstance(document, dict):
        raise ManifestLoadError(f"Expected a mapping in JSON file: {path}")
    try:
        _require_json_compatible_value(document, "JSON file", path)
    except RecursionError:
        raise ManifestLoadError(f"Unable to load JSON file: {path}") from None
    return document


def load_skill_markdown(path: Path) -> tuple[dict[str, object], str]:
    """Load YAML frontmatter and the untouched body of a skill Markdown file."""
    try:
        with path.open(encoding="utf-8", newline="") as skill_file:
            content = skill_file.read()
    except (OSError, UnicodeError, ValueError):
        raise ManifestLoadError(f"Unable to load skill Markdown: {path}") from None

    if not content.startswith("---\n"):
        raise ManifestLoadError(f"Missing YAML frontmatter in skill file: {path}")

    delimiter = "\n---\n"
    boundary = content.find(delimiter, len("---\n"))
    if boundary == -1:
        raise ManifestLoadError(f"Unclosed YAML frontmatter in skill file: {path}")

    frontmatter = content[len("---\n") : boundary]
    body = content[boundary + len(delimiter) :]
    try:
        metadata = yaml.safe_load(frontmatter)
    except (RecursionError, yaml.YAMLError):
        raise ManifestLoadError(
            f"Unable to parse YAML frontmatter in skill file: {path}"
        ) from None

    if not isinstance(metadata, dict):
        raise ManifestLoadError(
            f"Expected a mapping in YAML frontmatter for skill file: {path}"
        )
    try:
        _require_json_compatible_value(
            metadata, "YAML frontmatter for skill file", path
        )
    except RecursionError:
        raise ManifestLoadError(
            f"Unable to parse YAML frontmatter in skill file: {path}"
        ) from None
    return metadata, body


def resolve_package_path(
    root: Path, relative: str, *, base: Path | None = None
) -> Path:
    """Resolve a relative path while requiring it to remain inside a package."""
    relative_path = Path(relative)
    if relative_path.is_absolute():
        raise UnsafePackagePathError(f"Package path must not be absolute: {relative}")

    try:
        resolved_root = root.resolve(strict=False)
        candidate = ((base or root) / relative_path).resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        raise UnsafePackagePathError(
            f"Unable to resolve package path {relative!r} safely within: {root}"
        ) from None
    if not candidate.is_relative_to(resolved_root):
        raise UnsafePackagePathError(
            f"Package path resolves outside package: {relative}"
        )
    return candidate


def reject_symlinks(root: Path) -> None:
    """Reject symbolic links anywhere in a plugin package tree."""
    try:
        if root.is_symlink():
            raise UnsafePackagePathError("Package contains symbolic link: .")

        for path in root.rglob("*"):
            if path.is_symlink():
                relative_path = path.relative_to(root).as_posix()
                raise UnsafePackagePathError(
                    f"Package contains symbolic link: {relative_path}"
                )
    except UnsafePackagePathError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise UnsafePackagePathError(
            "Unable to inspect package paths safely within: ."
        ) from None


def _require_json_compatible_value(value: object, description: str, path: Path) -> None:
    value_type = type(value)
    if value is None or value_type is bool or value_type is str or value_type is int:
        return
    if value_type is float:
        if math.isfinite(cast(float, value)):
            return
    elif value_type is list:
        for nested_value in cast(list[object], value):
            _require_json_compatible_value(nested_value, description, path)
        return
    elif value_type is dict:
        mapping = cast(dict[object, object], value)
        if all(type(key) is str for key in mapping):
            for nested_value in mapping.values():
                _require_json_compatible_value(nested_value, description, path)
            return

    raise ManifestLoadError(f"Non-JSON-compatible data in {description}: {path}")
