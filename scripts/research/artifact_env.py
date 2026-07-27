"""Run-owned resources for hermetic research artifact stacks."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from scripts.research.modes.base import Mode

_RUN_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_OWNER_LABEL = "ai.edgecitadel.owner=artifact"
_CAMPAIGN_LABEL = "ai.edgecitadel.campaign-id"
_COMPOSE_FILE = Path(__file__).with_name("docker-compose.artifact.yml")
_CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _scratch_root() -> Path:
    configured = os.environ.get("EC_ARTIFACT_SCRATCH_ROOT")
    if configured:
        return Path(configured)
    return Path(os.environ.get("TMPDIR", "/tmp")) / "edgecitadel-artifact"


def _compose_environment(values: Mapping[str, str]) -> dict[str, str]:
    return {"PATH": os.environ.get("PATH", os.defpath), **values}


def _validate_run_id(run_id: object) -> str:
    if type(run_id) is not str or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("invalid run_id")
    return run_id


def _validate_mode(mode: object) -> str:
    if type(mode) is not str or mode not in {item.value for item in Mode}:
        raise ValueError("invalid mode")
    return mode


def _write_private(path: Path, contents: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, contents)
    finally:
        os.close(descriptor)


def _write_owner_record(path: Path, value: dict[str, object]) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    temporary = path.with_suffix(".tmp")
    _write_private(temporary, encoded)
    os.replace(temporary, path)


def _directory_inventory(path: Path) -> str:
    entries = sorted(entry.relative_to(path).as_posix() for entry in path.rglob("*"))
    encoded = json.dumps(entries, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def cleanup_campaign_image(image_ref: str, campaign_id: str) -> OwnedResource:
    if (
        type(image_ref) is not str
        or not image_ref
        or any(char.isspace() for char in image_ref)
    ):
        raise ValueError("invalid image_ref")
    validated_campaign_id = _validate_run_id(campaign_id)
    label = subprocess.run(
        [
            "docker",
            "image",
            "inspect",
            image_ref,
            "--format",
            f'{{{{ index .Config.Labels "{_CAMPAIGN_LABEL}" }}}}',
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if label != validated_campaign_id:
        raise RuntimeError("campaign image ownership is invalid")
    containers = subprocess.run(
        ["docker", "ps", "--all", "--quiet", "--filter", f"ancestor={image_ref}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if containers:
        raise RuntimeError("campaign image is still in use")
    subprocess.run(["docker", "image", "rm", image_ref], check=True)
    return OwnedResource("image", image_ref)


@dataclass(frozen=True)
class OwnedResource:
    kind: Literal["container", "network", "volume", "image"]
    name: str


@dataclass(frozen=True)
class CleanupReport:
    attempted: tuple[OwnedResource, ...]
    remaining: tuple[OwnedResource, ...]
    credential_removed: bool
    state_removed: bool
    scratch_removed: bool
    recovery_record_removed: bool
    completed: bool


@dataclass(frozen=True)
class ArtifactEnvironment:
    run_id: str
    mode: str
    output_dir: Path
    scratch_dir: Path
    control_dir: Path
    state_dir: Path
    credential_file: Path
    owner_record: Path
    project: str
    compose_env: dict[str, str]
    resolved_config: Mapping[str, object]
    compose_file: Path = _COMPOSE_FILE
    command_runner: _CommandRunner = field(
        default=subprocess.run,
        repr=False,
        compare=False,
    )

    @classmethod
    def recover(cls, run_id: str) -> ArtifactEnvironment:
        validated_run_id = _validate_run_id(run_id)
        scratch_root = _scratch_root().resolve()
        owner_record = scratch_root / "owners" / f"{validated_run_id}.json"
        try:
            record = json.loads(owner_record.read_text())
        except (OSError, ValueError):
            raise ValueError("artifact recovery record is unavailable") from None
        if not isinstance(record, dict):
            raise ValueError("artifact recovery record is invalid")  # noqa: TRY004
        mode = _validate_mode(record.get("mode"))
        project = record.get("project")
        if project != f"{mode}-artifact-{validated_run_id}":
            raise ValueError("artifact recovery record is invalid")
        values = {
            key: record.get(key)
            for key in (
                "credential_file",
                "config_dir",
                "control_dir",
                "state_dir",
                "scratch_dir",
                "output_dir",
                "compose_file",
            )
        }
        paths: dict[str, Path] = {}
        for key, value in values.items():
            if type(value) is not str or not Path(value).is_absolute():
                raise ValueError("artifact recovery record is invalid")
            paths[key] = Path(value)
        for key in (
            "credential_file",
            "config_dir",
            "control_dir",
            "state_dir",
            "scratch_dir",
        ):
            if not paths[key].resolve().is_relative_to(scratch_root):
                raise ValueError("artifact recovery record is invalid")
        if paths["scratch_dir"] != scratch_root / validated_run_id:
            raise ValueError("artifact recovery record is invalid")
        resolved_config = record.get("resolved_config")
        if not isinstance(resolved_config, dict):
            raise ValueError("artifact recovery record is invalid")  # noqa: TRY004
        nats_config = (
            "/etc/nats/core.conf"
            if mode == Mode.CORE_ONLY.value
            else "/etc/nats/jetstream.conf"
        )
        return cls(
            run_id=validated_run_id,
            mode=mode,
            output_dir=paths["output_dir"],
            scratch_dir=paths["scratch_dir"],
            control_dir=paths["control_dir"],
            state_dir=paths["state_dir"],
            credential_file=paths["credential_file"],
            owner_record=owner_record,
            project=project,
            compose_env={
                "COMPOSE_PROJECT_NAME": project,
                "EC_RUN_ID": validated_run_id,
                "EC_ARTIFACT_IMAGE": "",
                "COMPOSE_PROFILES": mode,
                "EC_MODE": mode,
                "EC_NATS_CONFIG": nats_config,
                "EC_CREDENTIAL_FILE": str(paths["credential_file"]),
                "EC_CONFIG_DIR": str(paths["config_dir"]),
                "EC_CONTROL_DIR": str(paths["control_dir"]),
                "EC_STATE_DIR": str(paths["state_dir"]),
                "EC_OUTPUT_DIR": str(paths["output_dir"]),
            },
            resolved_config=resolved_config,
            compose_file=paths["compose_file"],
        )

    @classmethod
    def create(
        cls,
        run_id: str,
        mode: str,
        output_root: Path,
    ) -> ArtifactEnvironment:
        validated_run_id = _validate_run_id(run_id)
        validated_mode = _validate_mode(mode)
        if not isinstance(output_root, Path) or not output_root.is_absolute():
            raise ValueError("invalid output_root")
        output_dir = output_root / validated_run_id
        if output_dir.exists():
            raise ValueError("artifact output already exists")
        scratch_root = _scratch_root()
        scratch_dir = scratch_root / validated_run_id
        if scratch_dir.exists():
            raise ValueError("artifact scratch already exists")
        output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        output_dir.mkdir(mode=0o700)
        scratch_dir.mkdir(mode=0o700, parents=True)
        state_dir = scratch_dir / "state"
        state_dir.mkdir(mode=0o700)
        config_dir = scratch_dir / "config"
        config_dir.mkdir(mode=0o700)
        control_dir = scratch_dir / "control"
        control_dir.mkdir(mode=0o700)
        credential_file = scratch_dir / "transport-token"
        _write_private(credential_file, f"{secrets.token_hex(32)}\n".encode())
        native_config = config_dir / "native-control.json"
        _write_private(
            native_config,
            json.dumps(
                {
                    "agent_id": "worker-1",
                    "behavior": "echo",
                    "crash_point": None,
                    "delay_ms": 0,
                    "heartbeat_interval_ms": 1000,
                    "mode": validated_mode,
                    "outcome_db": "/state/outcomes.sqlite",
                    "run_id": validated_run_id,
                    "side_effect_db": "/state/side-effects.sqlite",
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode(),
        )
        _write_private(
            control_dir / "native-control.json",
            native_config.read_bytes(),
        )
        project = f"{validated_mode}-artifact-{validated_run_id}"
        nats_config = (
            "/etc/nats/core.conf"
            if validated_mode == Mode.CORE_ONLY.value
            else "/etc/nats/jetstream.conf"
        )
        compose_env = {
            "COMPOSE_PROJECT_NAME": project,
            "EC_RUN_ID": validated_run_id,
            "EC_ARTIFACT_IMAGE": "",
            "COMPOSE_PROFILES": validated_mode,
            "EC_MODE": validated_mode,
            "EC_NATS_CONFIG": nats_config,
            "EC_CREDENTIAL_FILE": str(credential_file),
            "EC_CONFIG_DIR": str(config_dir),
            "EC_CONTROL_DIR": str(control_dir),
            "EC_STATE_DIR": str(state_dir),
            "EC_OUTPUT_DIR": str(output_dir),
        }
        resolved_config = {
            "mode": validated_mode,
            "freshness_attestation": {
                "inventory_sha256": _directory_inventory(state_dir),
                "state_dir": str(state_dir),
            },
        }
        owners_dir = scratch_root / "owners"
        owners_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        owner_record = owners_dir / f"{validated_run_id}.json"
        _write_owner_record(
            owner_record,
            {
                "project": project,
                "mode": validated_mode,
                "compose_file": str(_COMPOSE_FILE),
                "credential_file": str(credential_file),
                "config_dir": str(config_dir),
                "control_dir": str(control_dir),
                "native_config": str(native_config),
                "state_dir": str(state_dir),
                "scratch_dir": str(scratch_dir),
                "output_dir": str(output_dir),
                "labels": [_OWNER_LABEL, f"ai.edgecitadel.run-id={validated_run_id}"],
                "campaign_image_ref": "",
                "resolved_config": resolved_config,
            },
        )
        if stat.S_IMODE(credential_file.stat().st_mode) != 0o600:
            raise RuntimeError("artifact credential permissions are invalid")
        return cls(
            run_id=validated_run_id,
            mode=validated_mode,
            output_dir=output_dir,
            scratch_dir=scratch_dir,
            control_dir=control_dir,
            state_dir=state_dir,
            credential_file=credential_file,
            owner_record=owner_record,
            project=project,
            compose_env=compose_env,
            resolved_config=resolved_config,
        )

    def start(self) -> None:
        self.start_topology(self.compose_file, {})

    def stop(self) -> None:
        self.command_runner(
            [
                "docker",
                "compose",
                "--project-name",
                self.project,
                "--file",
                str(self.compose_file),
                "down",
                "--volumes",
                "--remove-orphans",
            ],
            check=True,
            env=_compose_environment(self.compose_env),
        )

    def owned_resources(self) -> tuple[OwnedResource, ...]:
        labels = (
            "--filter",
            f"label={_OWNER_LABEL}",
            "--filter",
            f"label=ai.edgecitadel.run-id={self.run_id}",
        )
        queries: tuple[
            tuple[Literal["container", "network", "volume"], list[str]], ...
        ] = (
            ("container", ["docker", "ps", "--all", *labels, "--format", "{{.Names}}"]),
            ("network", ["docker", "network", "ls", *labels, "--format", "{{.Name}}"]),
            ("volume", ["docker", "volume", "ls", *labels, "--format", "{{.Name}}"]),
        )
        resources: list[OwnedResource] = []
        for kind, command in queries:
            result = self.command_runner(
                command,
                check=True,
                capture_output=True,
                text=True,
            )
            resources.extend(
                OwnedResource(kind, name) for name in result.stdout.splitlines() if name
            )
        return tuple(resources)

    def cleanup(self) -> CleanupReport:
        scratch_root = self.scratch_dir.parent.resolve()
        owners_root = scratch_root / "owners"
        for path in (
            self.credential_file,
            self.control_dir,
            self.state_dir,
            self.scratch_dir,
        ):
            if not path.resolve().is_relative_to(scratch_root):
                raise RuntimeError("artifact cleanup path is invalid")
        if not self.owner_record.resolve().is_relative_to(owners_root.resolve()):
            raise RuntimeError("artifact cleanup record is invalid")
        self.stop()
        remaining = self.owned_resources()
        if remaining:
            return CleanupReport(
                attempted=remaining,
                remaining=remaining,
                credential_removed=False,
                state_removed=False,
                scratch_removed=False,
                recovery_record_removed=False,
                completed=False,
            )
        credential_removed = False
        if self.credential_file.exists():
            self.credential_file.unlink()
            credential_removed = True
        state_removed = False
        if self.state_dir.exists():
            shutil.rmtree(self.state_dir)
            state_removed = True
        scratch_removed = False
        if self.scratch_dir.exists():
            shutil.rmtree(self.scratch_dir)
            scratch_removed = True
        recovery_record_removed = False
        if self.owner_record.exists():
            self.owner_record.unlink()
            recovery_record_removed = True
        return CleanupReport(
            attempted=(),
            remaining=(),
            credential_removed=credential_removed,
            state_removed=state_removed,
            scratch_removed=scratch_removed,
            recovery_record_removed=recovery_record_removed,
            completed=True,
        )

    def start_topology(
        self,
        compose_file: Path,
        env_overrides: Mapping[str, str],
    ) -> None:
        if not isinstance(compose_file, Path) or not compose_file.is_file():
            raise ValueError("invalid compose file")
        environment = _compose_environment({**self.compose_env, **dict(env_overrides)})
        self.command_runner(
            [
                "docker",
                "compose",
                "--project-name",
                self.project,
                "--file",
                str(compose_file),
                "up",
                "--detach",
                "--no-build",
                "--wait",
            ],
            check=True,
            env=environment,
        )
        object.__setattr__(self, "compose_file", compose_file)


__all__ = [
    "ArtifactEnvironment",
    "CleanupReport",
    "OwnedResource",
    "cleanup_campaign_image",
]
