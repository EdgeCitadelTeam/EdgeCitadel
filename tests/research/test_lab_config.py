"""Strict contracts for the reproducible lab's local runtime inputs."""

from __future__ import annotations

import json
import os
import subprocess
import hashlib
from dataclasses import asdict
from pathlib import Path

import pytest

from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.lab_config import (
    LAB_NGINX_IMAGE,
    LabConfigError,
    credential_sha256,
    credential_token,
    qualified_agent_id,
    validate_agent_id,
    validate_declared_host_id,
    validate_run_id,
    write_credential_file,
    write_private_json,
    write_service_env_file,
)
from scripts.research.lab_runtime import (
    LAB_SOURCE_PATHS,
    build_fixture_image,
    capture_clean_source_provenance,
    sha256_file,
)


def _git_repo(root: Path) -> Path:
    root.mkdir()
    for relative in LAB_SOURCE_PATHS:
        path = root / relative
        if path.suffix:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("source\n")
        else:
            path.mkdir(parents=True, exist_ok=True)
            (path / "tracked.txt").write_text(f"{relative}\n")
    (root / "scripts/research/Dockerfile").write_text("FROM scratch\n")
    (root / "scripts/research/requirements.lock.txt").write_text("# lock\n")
    subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.email", "tests@example.invalid"], cwd=root, check=True)
    subprocess.run(["git", "config", "user.name", "tests"], cwd=root, check=True)
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], cwd=root, check=True)
    return root


def test_slice_dependencies_are_exact_public_contracts() -> None:
    from scripts.research.check_artifact import check_bundle
    from scripts.research.evidence import finalize_bundle, write_json

    assert callable(check_bundle)
    assert callable(write_json)
    assert callable(finalize_bundle)
    assert os.access("scripts/research/run-python", os.X_OK)
    assert Path("scripts/research/requirements.lock.txt").is_file()
    assert Path("scripts/research/toolchain.json").is_file()
    assert LAB_NGINX_IMAGE.endswith("5616878291a2eed594aee8db4dade5878cf7edcb475e59193904b198d9b830de")
    assert Path("e2e/playwright.evidence.config.js").is_file()


def test_identifier_boundaries_are_exact() -> None:
    assert validate_run_id("ec-lab-01") == "ec-lab-01"
    assert validate_agent_id("shell-1") == "shell-1"
    assert validate_declared_host_id("controller-lab-01") == "controller-lab-01"
    for value in ("ab", "UPPER", "contains.dot", "../escape", "a" * 32):
        with pytest.raises(LabConfigError):
            validate_run_id(value)


def test_qualified_agent_id_never_exceeds_wire_limit() -> None:
    value = qualified_agent_id("r" * 31, "a" * 31)
    assert len(value) == 64


def test_credential_errors_never_echo_malformed_content(tmp_path: Path) -> None:
    credential = tmp_path / "nats.creds"
    malformed = "private-material-that-must-not-be-echoed=bad"
    credential.write_text(malformed + "\n")
    credential.chmod(0o600)
    with pytest.raises(LabConfigError) as error:
        credential_token(credential)
    assert malformed not in str(error.value)
    assert "line 1" in str(error.value)


def test_raw_credential_and_service_env_are_distinct_private_formats(tmp_path: Path) -> None:
    raw = tmp_path / "nats.creds"
    service_env = tmp_path / "service.env"
    token = "4" * 64
    write_credential_file(raw, token)
    write_service_env_file(service_env, raw)
    assert raw.read_bytes() == (token + "\n").encode()
    assert service_env.read_bytes() == ("NATS_TOKEN=" + token + "\n").encode()
    assert credential_token(raw) == token
    assert credential_sha256(raw) == hashlib.sha256(token.encode()).hexdigest()
    with pytest.raises(LabConfigError):
        credential_token(service_env)
    assert raw.stat().st_mode & 0o777 == 0o600
    assert service_env.stat().st_mode & 0o777 == 0o600


def test_private_config_serializes_no_crash_as_json_null(tmp_path: Path) -> None:
    path = tmp_path / "native-control.json"
    config = NativeControlConfig("ec-lab-01", "shell-1", "edgecitadel", "echo", 125, None, 1000, "/run/state/outcomes.sqlite", "/run/state/side-effects.sqlite")
    write_private_json(path, asdict(config))
    assert json.loads(path.read_text())["crash_point"] is None
    assert path.stat().st_mode & 0o777 == 0o600


def test_fixture_build_returns_and_uses_only_immutable_image_id(tmp_path: Path) -> None:
    repo_root = _git_repo(tmp_path / "source")
    calls: list[list[str]] = []

    def runner(argv: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, "sha256:" + "3" * 64 + "\n", "")

    image = build_fixture_image(repo_root, "ec-lab-01", runner)
    assert image.image_id == "sha256:" + "3" * 64
    assert image.dockerfile_sha256 == sha256_file(repo_root / "scripts/research/Dockerfile")
    assert image.requirements_lock_sha256 == sha256_file(repo_root / "scripts/research/requirements.lock.txt")
    assert calls[-1][-1] == "edgecitadel-lab-fixture:ec-lab-01"


def test_source_provenance_is_captured_before_outputs_and_is_path_scoped(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path / "source")
    before = capture_clean_source_provenance(repo)
    assert before.dirty is False
    (repo / "docs/research/results/lab/run-1").mkdir(parents=True)
    (repo / "docs/research/results/lab/run-1/raw.json").write_text("{}\n")
    assert capture_clean_source_provenance(repo) == before
    runtime = repo / "scripts/research/lab_runtime.py"
    runtime.write_text("# changed\n")
    with pytest.raises(LabConfigError, match="source paths must be clean"):
        capture_clean_source_provenance(repo)
