"""Stable errors raised while inspecting plugin packages."""

from __future__ import annotations

import unicodedata
from pathlib import Path


def contains_control_characters(value: str) -> bool:
    """Return whether text contains any Unicode control character."""
    return any(unicodedata.category(character) == "Cc" for character in value)


def is_portable_relative_path(value: str) -> bool:
    """Return whether a POSIX-relative path is portable package metadata."""
    if (
        not value
        or contains_control_characters(value)
        or value.startswith("/")
        or "\\" in value
    ):
        return False
    if len(value) >= 3 and value[0].isalpha() and value[1:3] == ":/":
        return False
    return all(component not in {"", ".", ".."} for component in value.split("/"))


def format_path(path: str | Path) -> str:
    """Escape terminal control characters while preserving ordinary path text."""
    escaped: list[str] = []
    named_controls = {
        "\b": r"\b",
        "\t": r"\t",
        "\n": r"\n",
        "\f": r"\f",
        "\r": r"\r",
    }
    for character in str(path):
        codepoint = ord(character)
        if character in named_controls:
            escaped.append(named_controls[character])
        elif character == "\\":
            escaped.append(r"\\")
        elif unicodedata.category(character) == "Cc":
            escaped.append(f"\\x{codepoint:02x}")
        else:
            escaped.append(character)
    return "".join(escaped)


class PluginError(RuntimeError):
    """Base class for plugin package failures."""


class PluginNotFoundError(PluginError):
    """Raised when a plugin package root cannot be found."""


class ManifestLoadError(PluginError):
    """Raised when a plugin manifest cannot be loaded safely."""


class ManifestValidationError(PluginError):
    """Raised when a plugin manifest does not match its schema."""


class CompatibilityError(PluginError):
    """Raised when a plugin is incompatible with the supervisor."""


class UnsafePackagePathError(PluginError):
    """Raised when a package path crosses a safety boundary."""


class SkillDiscoveryError(PluginError):
    """Raised when packaged skills cannot be discovered."""


class DuplicateSkillError(PluginError):
    """Raised when packaged skills have duplicate identifiers."""


class LockIntegrityError(PluginError):
    """Raised when package content does not match its lock file."""
