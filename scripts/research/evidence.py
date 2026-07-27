"""Canonical, secret-safe evidence bundle writing and source provenance."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

_GENERATED_PREFIXES = (
    "docs/research/results/raw/",
    "docs/research/results/derived/",
    "docs/research/results/operator/",
    "docs/research/results/lab/",
    "tmp/",
    ".pytest_cache/",
    "__pycache__/",
)
_BEARER_PATTERN = re.compile(rb"\bbearer\s+[^\s\"']+", re.IGNORECASE)
_PRIVATE_KEY_PATTERN = re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----")
_TOKEN_KEY_PATTERN = re.compile(
    r"(?:token|secret|password|authorization|api[_-]?key)", re.IGNORECASE
)


@dataclass(frozen=True)
class SourceProvenance:
    commit: str
    git_dirty: bool
    source_sha256: str
    paths: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "commit": self.commit,
            "git_dirty": self.git_dirty,
            "source_sha256": self.source_sha256,
            "paths": list(self.paths),
        }


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _is_generated(path: str) -> bool:
    return any(
        path == prefix[:-1] or path.startswith(prefix) for prefix in _GENERATED_PREFIXES
    )


def _git_output(source_root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=source_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _source_paths(source_root: Path) -> tuple[str, ...]:
    candidates = _git_output(
        source_root,
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
    ).splitlines()
    return tuple(
        sorted(
            path
            for path in candidates
            if path and not _is_generated(path) and (source_root / path).is_file()
        )
    )


def _hash_sources(source_root: Path, paths: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for relative_path in paths:
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update((source_root / relative_path).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _git_dirty(source_root: Path) -> bool:
    changes = _git_output(source_root, "status", "--porcelain", "--untracked-files=all")
    return any(
        len(line) >= 4 and not _is_generated(line[3:]) for line in changes.splitlines()
    )


def capture_source_provenance(source_root: Path) -> SourceProvenance:
    root = source_root.resolve()
    paths = _source_paths(root)
    return SourceProvenance(
        commit=_git_output(root, "rev-parse", "HEAD").strip(),
        git_dirty=_git_dirty(root),
        source_sha256=_hash_sources(root, paths),
        paths=paths,
    )


def verify_source_provenance(source_root: Path, expected: SourceProvenance) -> bool:
    try:
        observed = capture_source_provenance(source_root)
    except (OSError, subprocess.CalledProcessError):
        return False
    return observed == expected


def write_json(path: Path, value: object) -> None:
    """Create a new immutable canonical JSON artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json(value) + b"\n"
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, encoded)
    finally:
        os.close(descriptor)


def write_jsonl(path: Path, values: Iterable[object]) -> None:
    """Create a new immutable canonical JSONL artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = b"".join(_canonical_json(value) + b"\n" for value in values)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, encoded)
    finally:
        os.close(descriptor)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _value_contains_secret(value: object, *, key: str = "") -> bool:
    if isinstance(value, Mapping):
        return any(
            _value_contains_secret(item, key=str(name)) for name, item in value.items()
        )
    if isinstance(value, list):
        return any(_value_contains_secret(item, key=key) for item in value)
    if isinstance(value, str) and _TOKEN_KEY_PATTERN.search(key):
        return bool(value) and (
            bool(re.fullmatch(r"[A-Fa-f0-9]{64}", value))
            or key.lower() in {"password", "authorization"}
            or "secret" in key.lower()
            or "token" in key.lower()
        )
    return False


def _contains_secret(path: Path) -> bool:
    contents = path.read_bytes()
    if _BEARER_PATTERN.search(contents) or _PRIVATE_KEY_PATTERN.search(contents):
        return True
    try:
        return _value_contains_secret(json.loads(contents))
    except UnicodeDecodeError:
        return False
    except json.JSONDecodeError:
        return False


def _raw_files(bundle: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            (
                path
                for path in bundle.rglob("*")
                if path.is_file() and path.name != "manifest.json"
            ),
            key=lambda path: path.relative_to(bundle).as_posix(),
        )
    )


def _valid_manifest(manifest: Mapping[str, object], schema_path: Path) -> bool:
    try:
        schema = json.loads(schema_path.read_text())
        Draft202012Validator(schema).validate(manifest)
    except (OSError, json.JSONDecodeError, ValueError):
        return False
    return True


def _atomic_manifest(path: Path, value: Mapping[str, object]) -> None:
    temporary = path.with_suffix(".tmp")
    encoded = _canonical_json(value) + b"\n"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, encoded)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def finalize_bundle(
    bundle: Path, manifest: Mapping[str, object], schema_path: Path
) -> str:
    """Validate and atomically seal a bundle, without changing an existing one."""
    manifest_path = bundle / "manifest.json"
    if manifest_path.exists() or not bundle.is_dir():
        return "INVALID"
    raw_files = _raw_files(bundle)
    if any(_contains_secret(path) for path in raw_files):
        return "INVALID"
    candidate: dict[str, Any] = dict(manifest)
    source = candidate.get("source")
    cleanup = candidate.get("cleanup")
    if (
        not isinstance(source, Mapping)
        or set(source) != {"commit", "git_dirty", "source_sha256", "paths"}
        or not isinstance(cleanup, Mapping)
        or cleanup.get("completed") is not True
    ):
        return "INVALID"
    candidate["status"] = "PASS"
    candidate["artifacts"] = {
        path.relative_to(bundle).as_posix(): file_sha256(path) for path in raw_files
    }
    candidate["manifest_sha256"] = hashlib.sha256(
        _canonical_json(candidate)
    ).hexdigest()
    if not _valid_manifest(candidate, schema_path):
        return "INVALID"
    _atomic_manifest(manifest_path, candidate)
    return "PASS"


__all__ = [
    "SourceProvenance",
    "capture_source_provenance",
    "file_sha256",
    "finalize_bundle",
    "verify_source_provenance",
    "write_json",
    "write_jsonl",
]
