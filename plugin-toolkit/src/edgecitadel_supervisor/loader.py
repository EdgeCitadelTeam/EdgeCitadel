"""Safe, non-executing loaders for plugin package files."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import cast

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode, Node, SequenceNode

from .errors import (
    ManifestLoadError,
    PluginNotFoundError,
    UnsafePackagePathError,
    contains_control_characters,
    format_path,
)

# Parsing limits bound CPU and memory consumption for untrusted package documents.
MAX_STRUCTURED_DOCUMENT_BYTES = 1024 * 1024
MAX_SKILL_MARKDOWN_BYTES = 2 * 1024 * 1024
MAX_SKILL_FRONTMATTER_BYTES = 64 * 1024
MAX_DOCUMENT_DEPTH = 64
MAX_DOCUMENT_NODES = 100_000


class _DuplicateKeyError(ValueError):
    """Internal signal for a duplicate JSON object member."""


class _SharedContainerAliasError(yaml.YAMLError):
    """Internal signal for a repeated YAML container node."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate keys in every mapping."""

    def construct_document(self, node: Node) -> object:
        stack = [node]
        seen_containers: set[int] = set()
        while stack:
            current = stack.pop()
            if isinstance(current, MappingNode):
                identity = id(current)
                if identity in seen_containers:
                    raise _SharedContainerAliasError
                seen_containers.add(identity)
                for key_node, value_node in current.value:
                    stack.extend((key_node, value_node))
            elif isinstance(current, SequenceNode):
                identity = id(current)
                if identity in seen_containers:
                    raise _SharedContainerAliasError
                seen_containers.add(identity)
                stack.extend(current.value)
        return super().construct_document(node)

    def construct_mapping(
        self, node: MappingNode, deep: bool = False
    ) -> dict[object, object]:
        if not isinstance(node, MappingNode):
            raise ConstructorError(
                None,
                None,
                f"expected a mapping node, but found {node.id}",
                node.start_mark,
            )
        self.flatten_mapping(node)
        mapping: dict[object, object] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                if key in mapping:
                    raise ConstructorError(
                        "while constructing a mapping",
                        node.start_mark,
                        "found duplicate key",
                        key_node.start_mark,
                    )
            except TypeError:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found unhashable key",
                    key_node.start_mark,
                ) from None
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


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
            f"Unable to inspect plugin root: {format_path(candidate)}"
        ) from None

    if not exists:
        raise PluginNotFoundError(f"Plugin root not found: {format_path(root)}")
    if not is_directory:
        raise PluginNotFoundError(
            f"Plugin root is not a directory: {format_path(root)}"
        )
    return root


def load_yaml(path: Path) -> dict[str, object]:
    """Load a YAML file whose document root must be a mapping."""
    try:
        content = _read_text_bounded(path, MAX_STRUCTURED_DOCUMENT_BYTES, "YAML file")
        document = yaml.load(content, Loader=_UniqueKeySafeLoader)
    except _SharedContainerAliasError:
        raise ManifestLoadError(
            f"Shared YAML container aliases are not allowed: {format_path(path)}"
        ) from None
    except (OSError, RecursionError, UnicodeError, ValueError, yaml.YAMLError):
        raise ManifestLoadError(
            f"Unable to load YAML file: {format_path(path)}"
        ) from None

    if not isinstance(document, dict):
        raise ManifestLoadError(f"Expected a mapping in YAML file: {format_path(path)}")
    _require_json_compatible_value(
        document, "YAML file", path, reject_shared_containers=True
    )
    return document


def load_json(path: Path) -> dict[str, object]:
    """Load a JSON file whose document root must be an object mapping."""
    document, _ = load_json_with_source(path)
    return document


def load_json_with_source(path: Path) -> tuple[dict[str, object], str]:
    """Load a JSON object and return the exact decoded source text."""
    try:
        content = _read_text_bounded(path, MAX_STRUCTURED_DOCUMENT_BYTES, "JSON file")
        document = json.loads(content, object_pairs_hook=_unique_json_object)
    except (OSError, RecursionError, UnicodeError, ValueError, json.JSONDecodeError):
        raise ManifestLoadError(
            f"Unable to load JSON file: {format_path(path)}"
        ) from None

    if not isinstance(document, dict):
        raise ManifestLoadError(f"Expected a mapping in JSON file: {format_path(path)}")
    _require_json_compatible_value(document, "JSON file", path)
    return document, content


def load_skill_markdown(path: Path) -> tuple[dict[str, object], str]:
    """Load YAML frontmatter and the untouched body of a skill Markdown file."""
    try:
        content = _read_text_bounded(path, MAX_SKILL_MARKDOWN_BYTES, "skill Markdown")
    except (OSError, UnicodeError, ValueError):
        raise ManifestLoadError(
            f"Unable to load skill Markdown: {format_path(path)}"
        ) from None

    if not content.startswith("---\n"):
        raise ManifestLoadError(
            f"Missing YAML frontmatter in skill file: {format_path(path)}"
        )

    delimiter = "\n---\n"
    boundary = content.find(delimiter, len("---\n"))
    if boundary == -1:
        raise ManifestLoadError(
            f"Unclosed YAML frontmatter in skill file: {format_path(path)}"
        )

    frontmatter = content[len("---\n") : boundary]
    body = content[boundary + len(delimiter) :]
    if len(frontmatter.encode("utf-8")) > MAX_SKILL_FRONTMATTER_BYTES:
        raise ManifestLoadError(
            f"YAML frontmatter exceeds size limit in skill file: {format_path(path)}"
        )
    try:
        metadata = yaml.load(frontmatter, Loader=_UniqueKeySafeLoader)
    except _SharedContainerAliasError:
        raise ManifestLoadError(
            "Shared YAML container aliases are not allowed in frontmatter: "
            f"{format_path(path)}"
        ) from None
    except (RecursionError, yaml.YAMLError):
        raise ManifestLoadError(
            f"Unable to parse YAML frontmatter in skill file: {format_path(path)}"
        ) from None

    if not isinstance(metadata, dict):
        raise ManifestLoadError(
            "Expected a mapping in YAML frontmatter for skill file: "
            f"{format_path(path)}"
        )
    _require_json_compatible_value(
        metadata,
        "YAML frontmatter for skill file",
        path,
        reject_shared_containers=True,
    )
    return metadata, body


def resolve_package_path(
    root: Path, relative: str, *, base: Path | None = None
) -> Path:
    """Resolve a relative path while requiring it to remain inside a package."""
    relative_path = Path(relative)
    if contains_control_characters(relative):
        raise UnsafePackagePathError(
            f"Package path contains control characters: {format_path(relative)}"
        )
    if relative_path.is_absolute():
        raise UnsafePackagePathError(
            f"Package path must not be absolute: {format_path(relative)}"
        )

    try:
        resolved_root = root.resolve(strict=False)
        candidate = ((base or root) / relative_path).resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        raise UnsafePackagePathError(
            "Unable to resolve package path "
            f"{format_path(repr(relative))} safely within: {format_path(root)}"
        ) from None
    if not candidate.is_relative_to(resolved_root):
        raise UnsafePackagePathError(
            f"Package path resolves outside package: {format_path(relative)}"
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
                    f"Package contains symbolic link: {format_path(relative_path)}"
                )
    except UnsafePackagePathError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise UnsafePackagePathError(
            "Unable to inspect package paths safely within: ."
        ) from None


def _read_text_bounded(path: Path, maximum_bytes: int, description: str) -> str:
    if path.stat().st_size > maximum_bytes:
        raise ManifestLoadError(
            f"{description} exceeds size limit: {format_path(path)}"
        )
    with path.open(encoding="utf-8", newline="") as source:
        content = source.read()
    if len(content.encode("utf-8")) > maximum_bytes:
        raise ManifestLoadError(
            f"{description} exceeds size limit: {format_path(path)}"
        )
    return content


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise _DuplicateKeyError
        document[key] = value
    return document


def _require_json_compatible_value(
    value: object,
    description: str,
    path: Path,
    *,
    reject_shared_containers: bool = False,
) -> None:
    stack: list[tuple[object, int]] = [(value, 1)]
    seen_containers: set[int] = set()
    traversed_nodes = 0

    while stack:
        current, depth = stack.pop()
        traversed_nodes += 1
        if traversed_nodes > MAX_DOCUMENT_NODES:
            raise ManifestLoadError(
                f"Document exceeds node limit in {description}: {format_path(path)}"
            )

        value_type = type(current)
        if (
            current is None
            or value_type is bool
            or value_type is str
            or value_type is int
        ):
            continue
        if value_type is float and math.isfinite(cast(float, current)):
            continue
        if value_type is not list and value_type is not dict:
            raise ManifestLoadError(
                f"Non-JSON-compatible data in {description}: {format_path(path)}"
            )
        if depth > MAX_DOCUMENT_DEPTH:
            raise ManifestLoadError(
                f"Document exceeds nesting limit in {description}: {format_path(path)}"
            )

        identity = id(current)
        if identity in seen_containers:
            if reject_shared_containers:
                raise ManifestLoadError(
                    "Shared YAML container aliases are not allowed in "
                    f"{description}: {format_path(path)}"
                )
            raise ManifestLoadError(
                f"Repeated container in {description}: {format_path(path)}"
            )
        seen_containers.add(identity)

        if value_type is list:
            stack.extend(
                (nested_value, depth + 1)
                for nested_value in reversed(cast(list[object], current))
            )
            continue

        mapping = cast(dict[object, object], current)
        if not all(type(key) is str for key in mapping):
            raise ManifestLoadError(
                f"Non-JSON-compatible data in {description}: {format_path(path)}"
            )
        stack.extend(
            (nested_value, depth + 1)
            for nested_value in reversed(tuple(mapping.values()))
        )
