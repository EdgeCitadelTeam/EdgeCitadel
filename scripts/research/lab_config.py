"""Strict local inputs for the reproducible multi-agent lab."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

ID_RE = re.compile(r"^[a-z][a-z0-9-]{2,30}$")
TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
LAB_NGINX_IMAGE = (
    "nginx@sha256:"
    "5616878291a2eed594aee8db4dade5878cf7edcb475e59193904b198d9b830de"
)


class LabConfigError(ValueError):
    """A local lab input cannot be safely used."""


def _validate_identifier(value: str, label: str) -> str:
    if not isinstance(value, str) or ID_RE.fullmatch(value) is None:
        raise LabConfigError(f"invalid {label}")
    return value


def validate_run_id(value: str) -> str:
    return _validate_identifier(value, "run ID")


def validate_agent_id(value: str) -> str:
    return _validate_identifier(value, "agent ID")


def validate_declared_host_id(value: str) -> str:
    return _validate_identifier(value, "host ID")


def qualified_agent_id(run_id: str, agent_id: str) -> str:
    qualified = f"{validate_run_id(run_id)}--{validate_agent_id(agent_id)}"
    if len(qualified) > 64:
        raise LabConfigError("qualified agent ID exceeds 64 characters")
    return qualified


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _private_path(path: Path) -> None:
    try:
        info = path.lstat()
    except OSError as error:
        raise LabConfigError("credential file is unavailable") from error
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise LabConfigError("credential file must be regular")
    if stat.S_IMODE(info.st_mode) != 0o600:
        raise LabConfigError("credential file must have mode 0600")


def credential_token(credential_file: Path) -> str:
    if not isinstance(credential_file, Path):
        raise LabConfigError("credential file path is invalid")
    _private_path(credential_file)
    try:
        lines = credential_file.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError as error:
        raise LabConfigError("malformed credential file at line 1") from error
    if len(lines) != 1:
        raise LabConfigError(f"malformed credential file at line {min(len(lines), 2)}")
    token = lines[0]
    if token in {"changeme", "change-me", "test-token"} or TOKEN_RE.fullmatch(token) is None:
        raise LabConfigError("malformed credential file at line 1")
    return token


def _write_private(path: Path, contents: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, contents)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def write_credential_file(credential_file: Path, token: str) -> None:
    if TOKEN_RE.fullmatch(token) is None or token in {"changeme", "change-me", "test-token"}:
        raise LabConfigError("credential token is invalid")
    _write_private(credential_file, f"{token}\n".encode())


def write_service_env_file(service_env_file: Path, raw_credential_file: Path) -> None:
    _write_private(service_env_file, f"NATS_TOKEN={credential_token(raw_credential_file)}\n".encode())


def write_private_json(path: Path, value: object) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
    _write_private(path, encoded)


def credential_sha256(credential_file: Path) -> str:
    return hashlib.sha256(credential_token(credential_file).encode()).hexdigest()


@dataclass(frozen=True)
class ControllerConfig:
    run_id: str
    lab_variant: Literal["lifecycle", "operator-smoke", "operator-evidence"]
    controller_host_id: str
    compose_project: str
    bind_host: str
    advertised_host: str
    advertised_ip: str
    app_url: str
    agg_url: str
    nats_url: str
    monitor_url: str
    inventory_url: str
    controller_machine_id_sha256: str
    source_commit: str
    source_snapshot_sha256: str
    credential_sha256: str
    credential_file: Path
    fixture_image_id: str
    state_dir: Path
    evidence_dir: Path

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        for name in ("credential_file", "state_dir", "evidence_dir"):
            value[name] = str(value[name])
        return value


__all__ = [
    "ControllerConfig", "LAB_NGINX_IMAGE", "LabConfigError", "credential_sha256",
    "credential_token", "qualified_agent_id", "sha256_file", "validate_agent_id",
    "validate_declared_host_id", "validate_run_id", "write_credential_file",
    "write_private_json", "write_service_env_file",
]
