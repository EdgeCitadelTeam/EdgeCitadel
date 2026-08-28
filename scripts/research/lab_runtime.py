"""Immutable fixture preparation and source provenance for lab runs."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, Sequence

from scripts.research.lab_config import LabConfigError, sha256_file, validate_run_id

IMAGE_ID_PREFIX = "sha256:"
LAB_SOURCE_PATHS = (
    ".dockerignore", "aggregator", "frontend", "e2e", "nginx", "scripts/research",
    "schemas",
)


class CommandRunner(Protocol):
    def __call__(self, argv: Sequence[str], *, cwd: Path) -> subprocess.CompletedProcess[str]: ...


@dataclass(frozen=True)
class FixtureImage:
    image_id: str
    dockerfile_sha256: str
    requirements_lock_sha256: str
    built_at: str


@dataclass(frozen=True)
class SourceProvenance:
    commit: str
    dirty: bool
    source_snapshot_sha256: str
    source_diff_sha256: str


def _git(repo_root: Path, *argv: str) -> str:
    return subprocess.run(["git", *argv], cwd=repo_root, check=True, capture_output=True, text=True).stdout


def capture_clean_source_provenance(repo_root: Path) -> SourceProvenance:
    root = repo_root.resolve()
    status = _git(root, "status", "--porcelain", "--", *LAB_SOURCE_PATHS)
    if status:
        raise LabConfigError("lab source paths must be clean")
    files = _git(root, "ls-files", "--", *LAB_SOURCE_PATHS).splitlines()
    digest = hashlib.sha256()
    for relative in sorted(files):
        path = root / relative
        if path.is_file():
            digest.update(relative.encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    diff = _git(root, "diff", "--binary", "HEAD", "--", *LAB_SOURCE_PATHS)
    return SourceProvenance(
        commit=_git(root, "rev-parse", "HEAD").strip(),
        dirty=False,
        source_snapshot_sha256=digest.hexdigest(),
        source_diff_sha256=hashlib.sha256(diff.encode()).hexdigest(),
    )


def build_fixture_image(repo_root: Path, run_id: str, runner: CommandRunner) -> FixtureImage:
    root = repo_root.resolve()
    validated_run_id = validate_run_id(run_id)
    dockerfile = root / "scripts/research/Dockerfile"
    requirements = root / "scripts/research/requirements.lock.txt"
    if not dockerfile.is_file() or not requirements.is_file():
        raise LabConfigError("fixture build inputs are missing")
    tag = f"edgecitadel-lab-fixture:{validated_run_id}"
    runner([
        "docker", "build", "--pull", "--file", str(dockerfile),
        "--label", "ai.edgecitadel.owner=artifact",
        "--label", f"ai.edgecitadel.run-id={validated_run_id}", "--tag", tag, str(root),
    ], cwd=root)
    try:
        completed = runner(["docker", "image", "inspect", "--format={{.Id}}", tag], cwd=root)
        image_id = completed.stdout.strip()
        if not image_id.startswith(IMAGE_ID_PREFIX) or len(image_id) != len(IMAGE_ID_PREFIX) + 64 or any(c not in "0123456789abcdef" for c in image_id[len(IMAGE_ID_PREFIX):]):
            raise LabConfigError("fixture image ID is not immutable")
    except BaseException:
        try:
            runner(["docker", "image", "rm", tag], cwd=root)
        finally:
            raise
    return FixtureImage(image_id, sha256_file(dockerfile), sha256_file(requirements), datetime.now(UTC).isoformat())


__all__ = ["CommandRunner", "FixtureImage", "LAB_SOURCE_PATHS", "SourceProvenance", "build_fixture_image", "capture_clean_source_provenance", "sha256_file"]
