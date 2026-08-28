"""Deterministic package integrity locks and non-executing inventory data."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import stat
from pathlib import Path
from typing import cast

from .errors import (
    LockIntegrityError,
    ManifestLoadError,
    ManifestValidationError,
    UnsafePackagePathError,
    format_path,
)
from .loader import load_json_with_source, load_yaml
from .validator import PackageRecord, SkillRecord, validate_schema

LOCK_FILENAME = "plugin.lock.json"
_LOCK_SCHEMA = "plugin-lock.v1.schema.json"
_PLUGIN_SCHEMA = "agent-plugin.v1alpha1.schema.json"
_HASH_CHUNK_SIZE = 1024 * 1024
_PORTABLE_SKILL_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")


def sha256_file(path: Path) -> str:
    """Return the lowercase SHA-256 digest of a file, read in 1 MiB chunks."""
    return _sha256_file(path, _redacted_path(path))


def package_files(root: Path) -> tuple[Path, ...]:
    """Return sorted regular package files, excluding the generated root lock."""
    try:
        if stat.S_ISLNK(root.stat(follow_symlinks=False).st_mode):
            raise UnsafePackagePathError("Package contains symbolic link: .")

        files: list[Path] = []
        for path in root.rglob("*"):
            relative_path = path.relative_to(root).as_posix()
            if _contains_control_characters(relative_path):
                raise UnsafePackagePathError(
                    "Package path contains control characters: "
                    f"{format_path(relative_path)}"
                )
            mode = path.stat(follow_symlinks=False).st_mode
            if stat.S_ISLNK(mode):
                raise UnsafePackagePathError(
                    f"Package contains symbolic link: {format_path(relative_path)}"
                )
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                raise UnsafePackagePathError(
                    "Package contains unsupported filesystem entry: "
                    f"{format_path(relative_path)}"
                )
            if relative_path != LOCK_FILENAME:
                files.append(path)
    except UnsafePackagePathError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise LockIntegrityError(
            "Unable to inspect package files safely within: ."
        ) from None

    return tuple(sorted(files, key=lambda path: path.relative_to(root).as_posix()))


def build_lock(package: PackageRecord) -> dict[str, object]:
    """Build deterministic lock data from a validated package record and its files."""
    files = [
        {
            "path": _package_relative(package.root, path),
            "sha256": _package_sha256(package.root, path),
        }
        for path in package_files(package.root)
    ]
    skills = [_skill_lock(package.root, skill) for skill in _ordered_skills(package)]
    return {
        "lockVersion": 1,
        "package": {
            "id": package.package_id,
            "version": package.package_version,
            "protocol": package.protocol,
        },
        "files": files,
        "skills": skills,
    }


def write_lock(package: PackageRecord) -> Path:
    """Write a package lock using the canonical JSON representation."""
    lock_path = package.root / LOCK_FILENAME
    content = json.dumps(build_lock(package), indent=2, sort_keys=True) + "\n"
    try:
        lock_path.write_text(content, encoding="utf-8", newline="")
    except (OSError, RuntimeError, UnicodeError, ValueError):
        raise LockIntegrityError(
            f"Unable to write plugin lock: {LOCK_FILENAME}"
        ) from None
    return lock_path


def verify_lock(package: PackageRecord) -> None:
    """Verify a canonical package record against its schema-valid lock file."""
    actual_files = package_files(package.root)
    lock, lock_source = _load_lock(package.root)
    duplicate_paths = _duplicate_file_paths(lock)
    duplicate_skills = _duplicate_skill_names(lock)

    try:
        validate_schema(lock, _LOCK_SCHEMA)
    except ManifestValidationError:
        duplicate_issues = _duplicate_issues(duplicate_paths, duplicate_skills)
        if duplicate_issues:
            raise LockIntegrityError("; ".join(duplicate_issues)) from None
        raise LockIntegrityError(
            f"Plugin lock failed schema validation: {LOCK_FILENAME}"
        ) from None

    locked_package = cast(dict[str, object], lock["package"])
    locked_files = cast(list[dict[str, object]], lock["files"])
    locked_skills = cast(list[dict[str, object]], lock["skills"])
    duplicate_issues = _duplicate_issues(duplicate_paths, duplicate_skills)
    if not duplicate_issues:
        canonical_source = json.dumps(lock, indent=2, sort_keys=True) + "\n"
        if lock_source != canonical_source:
            raise LockIntegrityError(
                f"Plugin lock is not exact canonical JSON: {LOCK_FILENAME}"
            )
    _require_canonical_lock_order(locked_files, locked_skills)
    issues = duplicate_issues
    issues.extend(_package_metadata_issues(package, locked_package))
    file_issues = _file_integrity_issues(package.root, actual_files, locked_files)
    issues.extend(file_issues)
    if file_issues:
        raise LockIntegrityError("; ".join(issues))
    issues.extend(_skill_integrity_issues(package, locked_skills))
    if issues:
        raise LockIntegrityError("; ".join(issues))


def build_inventory(package: PackageRecord) -> dict[str, object]:
    """Build deterministic JSON-compatible supervisor inventory data."""
    manifest = _load_inventory_manifest(package.root)
    return {
        "package": {
            "id": package.package_id,
            "version": package.package_version,
            "protocol": package.protocol,
        },
        "compatibility": copy.deepcopy(manifest["compatibility"]),
        "runtime": copy.deepcopy(manifest["runtime"]),
        "agents": [
            {"id": agent_id, "skillNames": list(package.agent_skill_names[agent_id])}
            for agent_id in sorted(package.agent_skill_names)
        ],
        "permissions": copy.deepcopy(manifest["permissions"]),
        "security": copy.deepcopy(manifest["security"]),
        "skills": [
            {
                "name": skill.name,
                "description": skill.description,
                "skillId": skill.skill_id,
                "version": skill.version,
                "executionName": skill.execution_name,
                "contentSha256": _package_sha256(package.root, skill.skill_file),
            }
            for skill in _ordered_skills(package)
        ],
    }


def _sha256_file(path: Path, display_path: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(_HASH_CHUNK_SIZE):
                digest.update(chunk)
    except (OSError, RuntimeError, ValueError):
        raise LockIntegrityError(
            f"Unable to hash package file: {display_path}"
        ) from None
    return digest.hexdigest()


def _package_sha256(root: Path, path: Path) -> str:
    return _sha256_file(path, _package_relative(root, path))


def _skill_lock(root: Path, skill: SkillRecord) -> dict[str, object]:
    return {
        "name": skill.name,
        "skillId": skill.skill_id,
        "version": skill.version,
        "contentSha256": _package_sha256(root, skill.skill_file),
    }


def _ordered_skills(package: PackageRecord) -> tuple[SkillRecord, ...]:
    return tuple(sorted(package.skills, key=lambda skill: skill.name))


def _load_lock(root: Path) -> tuple[dict[str, object], str]:
    try:
        return load_json_with_source(root / LOCK_FILENAME)
    except ManifestLoadError:
        raise LockIntegrityError(
            f"Unable to load plugin lock: {LOCK_FILENAME}"
        ) from None


def _load_inventory_manifest(root: Path) -> dict[str, object]:
    try:
        manifest = load_yaml(root / "plugin.yaml")
    except ManifestLoadError:
        raise LockIntegrityError(
            "Unable to load inventory manifest: plugin.yaml"
        ) from None
    try:
        validate_schema(manifest, _PLUGIN_SCHEMA)
    except ManifestValidationError:
        raise LockIntegrityError(
            "Inventory manifest failed schema validation: plugin.yaml"
        ) from None
    return copy.deepcopy(manifest)


def _duplicate_file_paths(lock: dict[str, object]) -> tuple[str, ...]:
    files = lock.get("files")
    if not isinstance(files, list):
        return ()
    paths = [
        path
        for entry in files
        if isinstance(entry, dict)
        and isinstance((path := entry.get("path")), str)
        and _is_safe_relative_path(path)
    ]
    return _duplicates(paths)


def _duplicate_skill_names(lock: dict[str, object]) -> tuple[str, ...]:
    skills = lock.get("skills")
    if not isinstance(skills, list):
        return ()
    names = [
        name
        for entry in skills
        if isinstance(entry, dict)
        and isinstance((name := entry.get("name")), str)
        and _is_portable_skill_name(name)
    ]
    return _duplicates(names)


def _duplicates(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return tuple(sorted(duplicates))


def _duplicate_issues(
    paths: tuple[str, ...], skill_names: tuple[str, ...]
) -> list[str]:
    issues: list[str] = []
    if paths:
        issues.append(f"Duplicate locked file paths: {', '.join(paths)}")
    if skill_names:
        issues.append(f"Duplicate locked skill names: {', '.join(skill_names)}")
    return issues


def _require_canonical_lock_order(
    locked_files: list[dict[str, object]],
    locked_skills: list[dict[str, object]],
) -> None:
    file_paths = [cast(str, entry["path"]) for entry in locked_files]
    if len(file_paths) == len(set(file_paths)) and file_paths != sorted(file_paths):
        raise LockIntegrityError(f"Noncanonical locked file order: {LOCK_FILENAME}")
    skill_names = [cast(str, entry["name"]) for entry in locked_skills]
    if len(skill_names) == len(set(skill_names)) and skill_names != sorted(skill_names):
        raise LockIntegrityError(f"Noncanonical locked skill order: {LOCK_FILENAME}")


def _package_metadata_issues(
    package: PackageRecord, locked_package: dict[str, object]
) -> list[str]:
    expected = {
        "id": package.package_id,
        "version": package.package_version,
        "protocol": package.protocol,
    }
    mismatched = sorted(
        field for field, value in expected.items() if locked_package[field] != value
    )
    if not mismatched:
        return []
    return [f"Mismatched package metadata: {', '.join(mismatched)}"]


def _file_integrity_issues(
    root: Path,
    actual_files: tuple[Path, ...],
    locked_files: list[dict[str, object]],
) -> list[str]:
    actual_by_path = {_package_relative(root, path): path for path in actual_files}
    locked_by_path = {
        cast(str, entry["path"]): cast(str, entry["sha256"]) for entry in locked_files
    }
    actual_paths = set(actual_by_path)
    locked_paths = set(locked_by_path)
    missing = sorted(locked_paths - actual_paths)
    unlisted = sorted(actual_paths - locked_paths)
    modified = sorted(
        path
        for path in actual_paths & locked_paths
        if _package_sha256(root, actual_by_path[path]) != locked_by_path[path]
    )

    issues: list[str] = []
    if missing:
        issues.append(
            f"Missing locked files: {', '.join(format_path(path) for path in missing)}"
        )
    if modified:
        issues.append(
            f"Modified files: {', '.join(format_path(path) for path in modified)}"
        )
    if unlisted:
        issues.append(
            "Unlisted package files: "
            f"{', '.join(format_path(path) for path in unlisted)}"
        )
    return issues


def _skill_integrity_issues(
    package: PackageRecord, locked_skills: list[dict[str, object]]
) -> list[str]:
    expected = {
        skill.name: _skill_lock(package.root, skill)
        for skill in _ordered_skills(package)
    }
    locked_by_name: dict[str, list[dict[str, object]]] = {}
    for skill in locked_skills:
        locked_by_name.setdefault(cast(str, skill["name"]), []).append(skill)

    expected_names = set(expected)
    locked_names = set(locked_by_name)
    missing = sorted(expected_names - locked_names)
    unexpected = sorted(locked_names - expected_names)
    mismatched = sorted(
        name
        for name in expected_names & locked_names
        if len(locked_by_name[name]) != 1 or locked_by_name[name][0] != expected[name]
    )

    issues: list[str] = []
    if missing:
        issues.append(f"Missing locked skills: {', '.join(missing)}")
    if unexpected:
        issues.append(f"Unexpected locked skills: {', '.join(unexpected)}")
    if mismatched:
        issues.append(f"Mismatched locked skills: {', '.join(mismatched)}")
    return issues


def _is_safe_relative_path(value: str) -> bool:
    if (
        not value
        or _contains_control_characters(value)
        or value.startswith("/")
        or "\\" in value
    ):
        return False
    if len(value) >= 3 and value[0].isalpha() and value[1:3] == ":/":
        return False
    components = value.split("/")
    return all(component not in {"", ".", ".."} for component in components)


def _is_portable_skill_name(value: str) -> bool:
    return 1 <= len(value) <= 64 and _PORTABLE_SKILL_NAME.fullmatch(value) is not None


def _package_relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except (OSError, RuntimeError, ValueError):
        raise LockIntegrityError(
            "Unable to inspect package file safely within: ."
        ) from None


def _redacted_path(path: Path) -> str:
    value = path.as_posix() if not path.is_absolute() else path.name
    return format_path(value)


def _contains_control_characters(value: str) -> bool:
    return any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
