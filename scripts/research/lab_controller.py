"""Persistent controller ownership records for the multi-agent lab."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import ipaddress
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Callable, Literal, Mapping, cast
from urllib.parse import quote

from scripts.research.check_artifact import check_bundle
from scripts.research.evidence import file_sha256, finalize_bundle, write_json
from scripts.research.artifact_env import OwnedResource
from scripts.research.artifact_env import ArtifactEnvironment
from scripts.research.lab_config import (
    ControllerConfig,
    LAB_NGINX_IMAGE,
    LabConfigError,
    credential_sha256,
    credential_token,
    validate_declared_host_id,
    validate_run_id,
    write_private_json,
    write_service_env_file,
)
from scripts.research.lab_observations import append_observation
from scripts.research.lab_contract import require_complete_lab_manifest
from scripts.research.lab_preflight import run_controller_preflight
from scripts.research.lab_qualification import LabQualification, qualify_bundle
from scripts.research.lab_runtime import (
    LAB_SOURCE_PATHS,
    build_fixture_image,
    capture_clean_source_provenance,
    sha256_file,
)

_PHASES = {"starting", "active", "stopping", "stopped", "failed"}
_TERMINAL_STATES = {"completed", "failed", "canceled", "rejected"}
_UTC_TIMESTAMP = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$"
)


class _TransientLabHTTPError(LabConfigError):
    pass


class _ImageCleanupError(LabConfigError):
    def __init__(self, remaining: tuple[OwnedResource, ...]) -> None:
        super().__init__("image cleanup failed")
        self.remaining = remaining


def _validated_acceptance(value: object, agent_id: str) -> tuple[str, str]:
    if not isinstance(value, Mapping) or value.get("recipient_id") != agent_id:
        raise LabConfigError("command response is invalid")
    task_id = value.get("task_id")
    accepted_at = value.get("accepted_at")
    if not isinstance(task_id, str) or not isinstance(accepted_at, str):
        raise LabConfigError("command response is invalid")
    try:
        parsed_task_id = uuid.UUID(task_id)
        parsed_time = datetime.fromisoformat(accepted_at.removesuffix("Z") + "+00:00")
    except ValueError:
        raise LabConfigError("command response is invalid") from None
    if (
        parsed_task_id.version != 4
        or parsed_task_id.variant != uuid.RFC_4122
        or str(parsed_task_id) != task_id
        or _UTC_TIMESTAMP.fullmatch(accepted_at) is None
        or parsed_time.utcoffset() != UTC.utcoffset(parsed_time)
    ):
        raise LabConfigError("command response is invalid")
    return task_id, accepted_at


@dataclass(frozen=True)
class ControllerOwnershipState:
    schema_version: str
    phase: Literal["starting", "active", "stopping", "stopped", "failed"]
    config: ControllerConfig
    compose_file: Path
    compose_environment: Mapping[str, str]
    artifact_scratch_root: Path
    raw_credential_file: Path
    service_env_file: Path
    owned_resources: tuple[OwnedResource, ...]
    completed_cleanup_steps: tuple[str, ...]
    exported_image_paths: tuple[Path, ...]
    controller_argv: tuple[str, ...]
    started_at: str


class _ResultReservation:
    def __init__(self, result_file: Path) -> None:
        self.result_path = result_file.resolve()
        self._reservation_path = self.result_path.with_name(
            f".{self.result_path.name}.reservation"
        )
        self._owned = False

    def __enter__(self) -> Path:
        self.result_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.result_path.parent, 0o700)
        try:
            descriptor = os.open(
                self._reservation_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except FileExistsError:
            raise LabConfigError("command result file is active") from None
        else:
            os.close(descriptor)
            self._owned = True
        if self.result_path.exists():
            self.__exit__(None, None, None)
            raise LabConfigError("command result file already exists")
        return self.result_path

    def __exit__(self, *_args: object) -> None:
        if self._owned:
            self._reservation_path.unlink(missing_ok=True)
            self._owned = False


def _state_dict(state: ControllerOwnershipState) -> dict[str, object]:
    return {
        "schema_version": state.schema_version,
        "phase": state.phase,
        "config": state.config.to_dict(),
        "compose_file": str(state.compose_file),
        "compose_environment": dict(state.compose_environment),
        "artifact_scratch_root": str(state.artifact_scratch_root),
        "raw_credential_file": str(state.raw_credential_file),
        "service_env_file": str(state.service_env_file),
        "owned_resources": [asdict(resource) for resource in state.owned_resources],
        "completed_cleanup_steps": list(state.completed_cleanup_steps),
        "exported_image_paths": [str(path) for path in state.exported_image_paths],
        "controller_argv": list(state.controller_argv),
        "started_at": state.started_at,
    }


def write_controller_state(state_file: Path, state: ControllerOwnershipState) -> None:
    if not state_file.is_absolute():
        raise LabConfigError("controller state path must be absolute")
    state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(state_file.parent, 0o700)
    temporary = state_file.with_suffix(state_file.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    encoded = (
        json.dumps(
            _state_dict(state), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, state_file)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    directory = os.open(state_file.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _path(value: object, label: str) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise LabConfigError(f"controller state {label} is invalid")
    return Path(value)


def _config(value: object) -> ControllerConfig:
    if not isinstance(value, dict):
        raise LabConfigError("controller state config is invalid")
    path_fields = {"credential_file", "state_dir", "evidence_dir"}
    try:
        values = dict(value)
        for name in path_fields:
            values[name] = _path(values[name], name)
        return ControllerConfig(**values)
    except (KeyError, TypeError, LabConfigError) as error:
        raise LabConfigError("controller state config is invalid") from error


def load_controller_state(state_file: Path) -> ControllerOwnershipState:
    try:
        value = json.loads(state_file.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("controller state is unavailable") from error
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "lab-controller-state.v1"
    ):
        raise LabConfigError("controller state is invalid")
    phase = value.get("phase")
    if phase not in _PHASES:
        raise LabConfigError("controller state phase is invalid")
    try:
        resources = tuple(
            OwnedResource(**resource) for resource in value["owned_resources"]
        )
        environment = value["compose_environment"]
        if not isinstance(environment, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in environment.items()
        ):
            raise LabConfigError("controller state environment is invalid")
        return ControllerOwnershipState(
            schema_version="lab-controller-state.v1",
            phase=phase,
            config=_config(value["config"]),
            compose_file=_path(value["compose_file"], "compose_file"),
            compose_environment=environment,
            artifact_scratch_root=_path(
                value["artifact_scratch_root"], "artifact_scratch_root"
            ),
            raw_credential_file=_path(
                value["raw_credential_file"], "raw_credential_file"
            ),
            service_env_file=_path(value["service_env_file"], "service_env_file"),
            owned_resources=resources,
            completed_cleanup_steps=tuple(
                str(item) for item in value["completed_cleanup_steps"]
            ),
            exported_image_paths=tuple(
                _path(item, "exported_image_path")
                for item in value["exported_image_paths"]
            ),
            controller_argv=tuple(str(item) for item in value["controller_argv"]),
            started_at=str(value["started_at"]),
        )
    except (KeyError, TypeError, LabConfigError) as error:
        raise LabConfigError("controller state is invalid") from error


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _image_id(tag: str, repo_root: Path) -> str:
    completed = subprocess.run(
        ["docker", "image", "inspect", "--format={{.Id}}", tag],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    image_id = completed.stdout.strip()
    if not image_id.startswith("sha256:") or len(image_id) != 71:
        raise LabConfigError("built image ID is not immutable")
    return image_id


def _run_text(
    runner: object,
    argv: list[str],
    *,
    cwd: Path | None = None,
) -> str:
    completed = runner(  # type: ignore[operator]
        argv,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LabConfigError(f"required command failed: {argv[0]}")
    return completed.stdout.strip()


def _validate_toolchain(
    repo_root: Path,
    lab_variant: str,
    *,
    runner: object = subprocess.run,
) -> dict[str, str]:
    versions = {
        "python": f"Python {platform.python_version()}",
        "docker": _run_text(runner, ["docker", "--version"]),
        "docker_compose": _run_text(
            runner, ["docker", "compose", "version", "--short"]
        ),
        "git": _run_text(runner, ["git", "--version"]),
        "node": "not-required",
        "npm": "not-required",
        "playwright": "not-required",
    }
    if lab_variant in {"operator-smoke", "operator-evidence"}:
        versions.update(
            node=_run_text(runner, ["node", "--version"]),
            npm=_run_text(runner, ["npm", "--version"]),
            playwright=_run_text(
                runner,
                ["npx", "--no-install", "playwright", "--version"],
                cwd=repo_root / "e2e",
            ),
        )
        expected = {
            "node": "v24.6.0",
            "npm": "11.5.1",
            "playwright": "Version 1.58.2",
        }
        for name, value in expected.items():
            if versions[name] != value:
                raise LabConfigError(f"lab {name} version must be {value}")
    return versions


def _validate_host_platform() -> None:
    if platform.system() != "Linux" or "Ubuntu 24.04" not in _os_release():
        raise LabConfigError("lab controller requires Ubuntu 24.04")
    if platform.machine() not in {"x86_64", "amd64"}:
        raise LabConfigError("lab controller requires x86_64")
    if not platform.python_version().startswith("3.12."):
        raise LabConfigError("lab controller requires Python 3.12")


def _validate_start_network(args: argparse.Namespace) -> tuple[str, str, str, str, str]:
    try:
        bind = ipaddress.IPv4Address(str(args.bind_host))
    except ipaddress.AddressValueError as error:
        raise LabConfigError("bind host must be IPv4") from error
    if bind.is_unspecified:
        raise LabConfigError("bind host cannot be unspecified")
    try:
        answers = socket.getaddrinfo(
            str(args.advertise_host), None, socket.AF_INET, socket.SOCK_STREAM
        )
        advertised_ip = str(ipaddress.IPv4Address(answers[0][4][0]))
    except (OSError, IndexError, ipaddress.AddressValueError) as error:
        raise LabConfigError("advertise host must resolve to IPv4") from error
    if ipaddress.IPv4Address(advertised_ip) != bind:
        raise LabConfigError("advertised IPv4 must match the reachable bind address")

    ports = tuple(
        getattr(args, name, None) for name in ("http_port", "nats_port", "monitor_port")
    )
    if bind.is_loopback:
        for value in ports:
            if value is not None and not 1 <= int(value) <= 65535:
                raise LabConfigError("lab port is invalid")
    else:
        if not getattr(args, "trusted_network_confirm", False):
            raise LabConfigError(
                "non-loopback binding requires trusted network confirmation"
            )
        if any(value is None for value in ports):
            raise LabConfigError("non-loopback binding requires explicit ports")
        if len({int(value) for value in ports}) != 3:
            raise LabConfigError("non-loopback ports must be distinct")
        if any(not 1 <= int(value) <= 65535 for value in ports):
            raise LabConfigError("lab port is invalid")
    return (
        str(bind),
        advertised_ip,
        *("" if value is None else str(value) for value in ports),
    )


def _validate_nats_configuration(
    state: ControllerOwnershipState,
    *,
    runner: object | None = None,
) -> None:
    if runner is None:
        runner = subprocess.run
    nats_config = Path(state.compose_environment["LAB_NATS_CONFIG"])
    token = credential_token(state.raw_credential_file)
    argv = [
        "docker",
        "run",
        "--rm",
        "--env-file",
        str(state.service_env_file),
        "--mount",
        f"type=bind,src={nats_config},dst=/etc/nats/nats.conf,readonly",
        state.compose_environment["LAB_NATS_IMAGE"],
        "-t",
        "-c",
        "/etc/nats/nats.conf",
    ]
    completed = runner(  # type: ignore[operator]
        argv, check=False, capture_output=True, text=True
    )
    output = f"{completed.stdout or ''}{completed.stderr or ''}"
    if token in output:
        raise LabConfigError("NATS validation output exposed the credential")
    _write_evidence_json(
        state.config.evidence_dir / "nats-validation.json",
        {
            "config_sha256": sha256_file(nats_config),
            "exit_status": completed.returncode,
        },
    )
    if completed.returncode != 0:
        raise LabConfigError("NATS configuration validation failed")


def _port(project: str, compose_file: Path, service: str, container_port: int) -> int:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "--project-name",
            project,
            "--file",
            str(compose_file),
            "port",
            service,
            str(container_port),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    value = completed.stdout.strip().rsplit(":", 1)[-1]
    if not value.isdigit() or not 1 <= int(value) <= 65535:
        raise LabConfigError("controller port resolution failed")
    return int(value)


def _controller_machine_id_sha256() -> str:
    machine_id = Path("/etc/machine-id")
    if not machine_id.is_file():
        raise LabConfigError("lab controller requires Linux /etc/machine-id")
    return hashlib.sha256(machine_id.read_bytes()).hexdigest()


def _completed_cleanup_step(
    state_file: Path,
    state: ControllerOwnershipState,
    step: str,
) -> ControllerOwnershipState:
    if step in state.completed_cleanup_steps:
        return state
    updated = replace(
        state,
        completed_cleanup_steps=state.completed_cleanup_steps + (step,),
    )
    write_controller_state(state_file, updated)
    return updated


def _load_complete_cleanup(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("controller cleanup receipt is unavailable") from error
    required = {
        "completed",
        "attempted",
        "remaining",
        "owned_resources_removed",
        "foreign_resources_touched",
        "credential_removed",
        "artifact_state_removed",
        "artifact_scratch_removed",
        "artifact_recovery_record_removed",
        "completed_at",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value.get("completed") is not True
        or not isinstance(value.get("attempted"), list)
        or value.get("remaining") != []
        or value.get("owned_resources_removed") is not True
        or value.get("foreign_resources_touched") is not False
        or value.get("credential_removed") is not True
        or value.get("artifact_state_removed") is not True
        or value.get("artifact_scratch_removed") is not True
        or value.get("artifact_recovery_record_removed") is not True
        or not isinstance(value.get("completed_at"), str)
    ):
        raise LabConfigError("controller cleanup receipt is invalid")
    return value


def _write_evidence_json(path: Path, value: object) -> None:
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise LabConfigError(
                f"retained evidence is invalid: {path.name}"
            ) from error
        if existing != value:
            raise LabConfigError(f"retained evidence differs: {path.name}")
        return
    write_json(path, value)


def _portable_value(state: ControllerOwnershipState, value: object) -> object:
    replacements = sorted(
        (
            (str(state.raw_credential_file), "<credential-file>"),
            (str(state.config.state_dir), "<run-state>"),
            (str(state.config.evidence_dir), "$EVIDENCE_DIR"),
            (str(state.artifact_scratch_root), "<artifact-state>"),
            (str(_repo_root().resolve()), "$SOURCE_ROOT"),
        ),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    if isinstance(value, str):
        result = value
        for original, replacement in replacements:
            result = result.replace(original, replacement)
        return result
    if isinstance(value, list):
        return [_portable_value(state, item) for item in value]
    if isinstance(value, tuple):
        return [_portable_value(state, item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _portable_value(state, item) for key, item in value.items()}
    return value


def _controller_command_snapshot(
    state: ControllerOwnershipState,
    node_reports: list[object],
    reservation_events: list[object],
) -> dict[str, object]:
    commands: dict[str, dict[str, object]] = {}
    command_root = state.config.evidence_dir / "raw/lab/commands"
    if command_root.is_dir():
        for path in sorted(command_root.glob("*.json")):
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as error:
                raise LabConfigError(
                    "controller command evidence is invalid"
                ) from error
            if not isinstance(value, dict) or not isinstance(value.get("task_id"), str):
                raise LabConfigError("controller command evidence is invalid")
            task_id = str(value["task_id"])
            previous = commands.get(task_id)
            if previous is None or value.get("status") == "completed":
                commands[task_id] = value
    for command in commands.values():
        if "qualification_kind" in command:
            continue
        qualification = "direct"
        accepted_at = command.get("accepted_at")
        agent_id = command.get("agent_id")
        reservation_id = command.get("reservation_id")
        matching = [
            item
            for item in reservation_events
            if isinstance(item, Mapping)
            and item.get("agent_id") == agent_id
            and item.get("reservation_id") == reservation_id
        ]
        retained = [
            item.get("observed_at")
            for item in matching
            if item.get("event") == "retained"
        ]
        resumed = [
            item.get("observed_at")
            for item in matching
            if item.get("event") == "resumed"
        ]
        if (
            isinstance(accepted_at, str)
            and len(retained) == 1
            and len(resumed) == 1
            and isinstance(retained[0], str)
            and isinstance(resumed[0], str)
            and retained[0] < accepted_at < resumed[0]
        ):
            qualification = "queued-reconnect"
        command["qualification_kind"] = qualification
    launches = [
        {
            name: report.get(name)
            for name in (
                "agent_id",
                "qualified_agent_id",
                "reservation_id",
                "declared_host_id",
            )
        }
        for report in node_reports
        if isinstance(report, Mapping)
    ]
    return {
        "launches": launches,
        "commands": [commands[key] for key in sorted(commands)],
    }


def _snapshot_stop_evidence(
    state: ControllerOwnershipState,
    *,
    opener: object = urllib.request.urlopen,
) -> None:
    raw = state.config.evidence_dir / "raw/lab"
    inventory_file = raw / "inventory.json"
    if inventory_file.is_file():
        inventory = _load_json_evidence(inventory_file)
    else:
        token = credential_token(state.raw_credential_file)
        inventory = _request_json(
            state.config.inventory_url,
            opener=opener,
            expected_status=200,
            token=token,
        )
    if (
        not isinstance(inventory, Mapping)
        or inventory.get("run_id") != state.config.run_id
        or not isinstance(inventory.get("reservations"), list)
        or not isinstance(inventory.get("reservation_events"), list)
        or not isinstance(inventory.get("node_reports"), list)
    ):
        raise LabConfigError("lab inventory snapshot is invalid")
    observation_snapshot = raw / "controller-observations.json"
    if observation_snapshot.is_file():
        observations = _load_json_evidence(observation_snapshot)
        if not isinstance(observations, list):
            raise LabConfigError("controller observation snapshot is invalid")
    else:
        try:
            observations = [
                json.loads(line)
                for line in (state.config.evidence_dir / "lab-observations.jsonl")
                .read_text()
                .splitlines()
                if line
            ]
        except (OSError, json.JSONDecodeError) as error:
            raise LabConfigError(
                "controller observation journal is unavailable"
            ) from error
    _write_evidence_json(inventory_file, dict(inventory))
    reservation_events = inventory["reservation_events"]
    node_reports = inventory["node_reports"]
    _write_evidence_json(
        raw / "reservation-events.json",
        reservation_events,
    )
    _write_evidence_json(raw / "node-reports.json", node_reports)
    _write_evidence_json(raw / "controller-observations.json", observations)
    _write_evidence_json(
        raw / "controller-commands.json",
        _controller_command_snapshot(state, node_reports, reservation_events),
    )


def _snapshot_failed_start_evidence(state: ControllerOwnershipState) -> None:
    raw = state.config.evidence_dir / "raw/lab"
    inventory = {
        "run_id": state.config.run_id,
        "reservations": [],
        "reservation_events": [],
        "node_reports": [],
    }
    _write_evidence_json(raw / "inventory.json", inventory)
    _write_evidence_json(raw / "reservation-events.json", [])
    _write_evidence_json(raw / "node-reports.json", [])
    _write_evidence_json(
        raw / "controller-commands.json", {"launches": [], "commands": []}
    )
    journal = state.config.evidence_dir / "lab-observations.jsonl"
    observations: list[object] = []
    if journal.is_file():
        try:
            observations = [
                json.loads(line) for line in journal.read_text().splitlines() if line
            ]
        except (OSError, json.JSONDecodeError) as error:
            raise LabConfigError("controller observation journal is invalid") from error
    _write_evidence_json(raw / "controller-observations.json", observations)
    compose_config = state.config.evidence_dir / "compose-config.yml"
    if not compose_config.exists():
        compose_config.write_text("status: start-failed\n", encoding="utf-8")
        os.chmod(compose_config, 0o600)


def _ensure_recovery_compose_inputs(state: ControllerOwnershipState) -> None:
    if not state.service_env_file.exists():
        state.service_env_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        state.service_env_file.write_text(
            f"NATS_TOKEN={state.config.credential_sha256}\n", encoding="utf-8"
        )
        os.chmod(state.service_env_file, 0o600)
    nats_config_value = state.compose_environment.get("LAB_NATS_CONFIG")
    if nats_config_value:
        nats_config = Path(nats_config_value)
        if not nats_config.exists():
            shutil.copyfile(
                _repo_root() / "scripts/research/nats-lab.conf.tpl", nats_config
            )
            os.chmod(nats_config, 0o600)
    data_dir_value = state.compose_environment.get("LAB_DATA_DIR")
    if data_dir_value:
        Path(data_dir_value).mkdir(parents=True, exist_ok=True, mode=0o700)


def _docker_inventory(runner: object) -> frozenset[OwnedResource]:
    command_runner = cast(Callable[..., subprocess.CompletedProcess[str]], runner)
    queries = (
        ("container", ["docker", "ps", "--all", "--format", "{{.Names}}"]),
        ("network", ["docker", "network", "ls", "--format", "{{.Name}}"]),
        ("volume", ["docker", "volume", "ls", "--format", "{{.Name}}"]),
        ("image", ["docker", "image", "ls", "--no-trunc", "--format", "{{.ID}}"]),
    )
    resources: set[OwnedResource] = set()
    for kind, argv in queries:
        completed = command_runner(
            argv,
            check=True,
            capture_output=True,
            text=True,
        )
        resources.update(
            OwnedResource(
                cast(Literal["container", "network", "volume", "image"], kind), name
            )
            for name in completed.stdout.splitlines()
            if name
        )
    return frozenset(resources)


def _remove_images_and_exports(
    state: ControllerOwnershipState,
    *,
    runner: object,
) -> None:
    command_runner = cast(Callable[..., subprocess.CompletedProcess[str]], runner)
    image_ids = tuple(
        dict.fromkeys(
            resource.name
            for resource in state.owned_resources
            if resource.kind == "image"
        )
    )
    remaining_images: list[OwnedResource] = []
    for image_name in image_ids:
        present = command_runner(
            ["docker", "image", "inspect", image_name],
            check=False,
            capture_output=True,
            text=True,
        )
        if present.returncode != 0:
            continue
        removed = command_runner(
            ["docker", "image", "rm", image_name],
            check=False,
            capture_output=True,
            text=True,
        )
        if removed.returncode != 0:
            still_present = command_runner(
                ["docker", "image", "inspect", image_name],
                check=False,
                capture_output=True,
                text=True,
            )
            if still_present.returncode == 0:
                remaining_images.append(OwnedResource("image", image_name))
    for output in state.exported_image_paths:
        try:
            output.unlink(missing_ok=True)
        except OSError:
            remaining_images.extend(
                OwnedResource("image", image_name) for image_name in image_ids
            )
    if remaining_images:
        raise _ImageCleanupError(tuple(dict.fromkeys(remaining_images)))


def _foreign_resources_touched(
    before: frozenset[OwnedResource],
    after: frozenset[OwnedResource],
    owned: frozenset[OwnedResource],
) -> bool:
    return bool((before - owned) - after)


def _remaining_owned_resources(
    resources: tuple[OwnedResource, ...],
    *,
    runner: object,
) -> tuple[OwnedResource, ...]:
    command_runner = cast(Callable[..., subprocess.CompletedProcess[str]], runner)
    nouns = {
        "container": "container",
        "network": "network",
        "volume": "volume",
        "image": "image",
    }
    remaining: list[OwnedResource] = []
    for resource in resources:
        completed = command_runner(
            ["docker", nouns[resource.kind], "inspect", resource.name],
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0:
            remaining.append(resource)
    return tuple(remaining)


def _resource_dicts(
    resources: tuple[OwnedResource, ...] | frozenset[OwnedResource],
) -> list[dict[str, str]]:
    return [
        {"kind": resource.kind, "name": resource.name}
        for resource in sorted(resources, key=lambda item: (item.kind, item.name))
    ]


def _load_resource_journal(path: Path) -> frozenset[OwnedResource]:
    try:
        value = json.loads(path.read_text())
        if not isinstance(value, list):
            raise TypeError
        resources = frozenset(OwnedResource(**item) for item in value)
    except (OSError, json.JSONDecodeError, TypeError) as error:
        raise LabConfigError("Docker inventory journal is invalid") from error
    return resources


def _artifact_cleanup_paths(
    state: ControllerOwnershipState,
) -> tuple[Path, Path, Path, Path]:
    run_scratch = state.artifact_scratch_root / state.config.run_id
    return (
        run_scratch,
        run_scratch / "state",
        state.raw_credential_file,
        state.artifact_scratch_root / "owners" / f"{state.config.run_id}.json",
    )


def _remove_private_controller_files(state: ControllerOwnershipState) -> None:
    paths = [state.service_env_file, state.config.state_dir / "controller.json"]
    nats_config = state.compose_environment.get("LAB_NATS_CONFIG")
    if nats_config is not None:
        path = Path(nats_config)
        if not path.is_absolute():
            raise LabConfigError("persisted NATS config path is invalid")
        paths.append(path)
    for path in paths:
        path.unlink(missing_ok=True)


def _load_json_evidence(path: Path) -> object:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError(
            f"required evidence is unavailable: {path.name}"
        ) from error


def _artifact_ref(bundle: Path, relative: str) -> dict[str, str]:
    path = bundle / relative
    if not path.is_file():
        raise LabConfigError(f"required evidence is unavailable: {path.name}")
    return {"path": relative, "sha256": file_sha256(path)}


def _os_release() -> str:
    try:
        values = {
            key: value.strip().strip('"')
            for key, value in (
                line.split("=", 1)
                for line in Path("/etc/os-release").read_text().splitlines()
                if "=" in line
            )
        }
    except OSError:
        return platform.platform()
    return values.get("PRETTY_NAME") or values.get("NAME") or platform.platform()


def _tool_version(
    runner: object,
    argv: list[str],
    *,
    cwd: Path | None = None,
) -> str:
    command_runner = cast(Callable[..., subprocess.CompletedProcess[str]], runner)
    completed = command_runner(
        argv,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    value = (completed.stdout or completed.stderr).strip().splitlines()
    if not value or not value[0]:
        raise LabConfigError("tool version is unavailable")
    return value[0]


def _write_task6_evidence(bundle: Path, manifest: Mapping[str, object]) -> None:
    """Derive the Task 6 review surface from canonical retained evidence."""
    compose_source = bundle / "compose-config.yml"
    compose_target = bundle / "compose.resolved.yml"
    try:
        rendered = compose_source.read_text()
    except OSError as error:
        raise LabConfigError("rendered Compose evidence is unavailable") from error
    if compose_target.exists():
        if compose_target.read_text() != rendered:
            raise LabConfigError("Task 6 Compose evidence differs")
    else:
        compose_target.write_text(rendered)
        os.chmod(compose_target, 0o600)

    controller = manifest.get("controller")
    nodes = manifest.get("nodes")
    dependencies = manifest.get("dependencies")
    images = manifest.get("images")
    cleanup = manifest.get("cleanup")
    if (
        not isinstance(controller, Mapping)
        or not isinstance(nodes, list)
        or not isinstance(dependencies, Mapping)
        or not isinstance(images, Mapping)
        or not isinstance(cleanup, Mapping)
    ):
        raise LabConfigError("Task 6 summary evidence is invalid")
    raw = bundle / "raw/lab"
    inventory = _load_json_evidence(raw / "inventory.json")
    commands = _load_json_evidence(raw / "controller-commands.json")
    summaries = {
        "versions.json": dict(dependencies),
        "images.json": dict(images),
        "identities.json": {"controller": dict(controller), "nodes": nodes},
        "network-paths.json": {
            "controller": dict(controller),
            "nodes": [
                {
                    "declared_host_id": node.get("declared_host_id"),
                    "network_path": node.get("network_path"),
                }
                for node in nodes
                if isinstance(node, Mapping)
            ],
        },
        "commands.json": commands,
        "inventory.json": inventory,
        "cleanup.json": dict(cleanup),
    }
    for name, value in summaries.items():
        _write_evidence_json(bundle / name, value)


def _build_lab_manifest(
    state: ControllerOwnershipState,
    cleanup: Mapping[str, object],
    *,
    runner: object = subprocess.run,
) -> dict[str, object]:
    bundle = state.config.evidence_dir
    raw = bundle / "raw/lab"
    nodes = _load_json_evidence(raw / "node-reports.json")
    if not isinstance(nodes, list):
        raise LabConfigError("node report evidence is invalid")
    compose_config = bundle / "compose-config.yml"
    if not compose_config.is_file():
        raise LabConfigError("rendered Compose evidence is unavailable")
    playwright: list[dict[str, str]] = []
    if state.config.lab_variant == "operator-smoke":
        playwright.append(_artifact_ref(bundle, "playwright-smoke.json"))
    elif state.config.lab_variant == "operator-evidence":
        playwright.append(_artifact_ref(bundle, "playwright-results.json"))
    images = {
        "nats": state.compose_environment.get("LAB_NATS_IMAGE", ""),
        "aggregator": state.compose_environment.get("LAB_AGGREGATOR_IMAGE", ""),
        "dashboard": state.compose_environment.get("LAB_DASHBOARD_IMAGE", ""),
        "nginx": state.compose_environment.get("LAB_NGINX_IMAGE", ""),
        "fixture": state.config.fixture_image_id,
    }
    start_record_path = bundle / "controller-start.json"
    if not start_record_path.is_file():
        start_record_path = bundle / "controller-facts.json"
    dependencies: Mapping[str, object] | None = None
    if start_record_path.is_file():
        start_record = _load_json_evidence(start_record_path)
        if isinstance(start_record, Mapping) and isinstance(
            start_record.get("dependencies"), Mapping
        ):
            dependencies = cast(Mapping[str, object], start_record["dependencies"])
    if dependencies is None:
        dependencies = {
            "python": platform.python_version(),
            "docker": _tool_version(runner, ["docker", "--version"]),
            "docker_compose": _tool_version(
                runner, ["docker", "compose", "version", "--short"]
            ),
            "git": _tool_version(runner, ["git", "--version"]),
            "node": _tool_version(runner, ["node", "--version"]),
            "npm": _tool_version(runner, ["npm", "--version"]),
            "playwright": _tool_version(
                runner,
                ["npx", "--no-install", "playwright", "--version"],
                cwd=_repo_root() / "e2e",
            ),
        }
    observations = {
        "reservation_events": _artifact_ref(bundle, "raw/lab/reservation-events.json"),
        "node_reports": _artifact_ref(bundle, "raw/lab/node-reports.json"),
        "controller_commands": _artifact_ref(
            bundle, "raw/lab/controller-commands.json"
        ),
        "playwright": playwright,
        "cleanup": _artifact_ref(bundle, "raw/lab/cleanup.json"),
    }
    manifest: dict[str, object] = {
        "schema_version": "research-manifest.v1",
        "evidence_kind": "lab",
        "lab_variant": state.config.lab_variant,
        "status": "PENDING",
        "run_id": state.config.run_id,
        "source": {
            "commit": state.config.source_commit,
            "git_dirty": False,
            "source_sha256": state.config.source_snapshot_sha256,
            "paths": list(LAB_SOURCE_PATHS),
        },
        "command": [cast(list[str], _portable_value(state, state.controller_argv))],
        "timing": {
            "started_at": state.started_at,
            "completed_at": cleanup["completed_at"],
        },
        "host": {"os": platform.system(), "architecture": platform.machine()},
        "dependencies": dependencies,
        "images": images,
        "compose_config_sha256": file_sha256(compose_config),
        "schemas": {"manifest": "schemas/research-manifest.v1.json"},
        "cleanup": dict(cleanup),
        "artifacts": {},
        "controller": {
            "project": state.config.compose_project,
            "bind_host": state.config.bind_host,
            "advertised_host": state.config.advertised_host,
            "advertised_ip": state.config.advertised_ip,
            "app_url": state.config.app_url or "unavailable",
            "nats_url": state.config.nats_url or "unavailable",
            "monitor_url": state.config.monitor_url or "unavailable",
            "inventory_url": state.config.inventory_url or "unavailable",
            "declared_host_id": state.config.controller_host_id,
            "machine_id_sha256": state.config.controller_machine_id_sha256,
            "hostname": socket.gethostname(),
            "os_release": _os_release(),
            "architecture": platform.machine(),
        },
        "nodes": nodes,
        "observations": observations,
    }
    if state.config.lab_variant == "operator-evidence":
        manifest["operator_evidence"] = {"report": playwright[0]}
    _write_task6_evidence(bundle, manifest)
    return cast(dict[str, object], _portable_value(state, manifest))


def _journal_images(
    state_file: Path,
    state: ControllerOwnershipState,
    *names: str,
) -> ControllerOwnershipState:
    resources = list(state.owned_resources)
    for name in names:
        resource = OwnedResource("image", name)
        if resource not in resources:
            resources.append(resource)
    updated = replace(state, owned_resources=tuple(resources))
    write_controller_state(state_file, updated)
    return updated


def _render_compose(
    state: ControllerOwnershipState,
    *,
    runner: object | None = None,
) -> None:
    if runner is None:
        runner = subprocess.run
    completed = runner(  # type: ignore[operator]
        [
            "docker",
            "compose",
            "--project-name",
            state.config.compose_project,
            "--file",
            str(state.compose_file),
            "config",
            "--no-env-resolution",
            "--resolve-image-digests",
        ],
        cwd=_repo_root(),
        env={**os.environ, **state.compose_environment},
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise LabConfigError("rendered Compose configuration failed")
    token = credential_token(state.raw_credential_file)
    if token in completed.stdout or token in (completed.stderr or ""):
        raise LabConfigError("rendered Compose configuration exposed the credential")
    portable = cast(str, _portable_value(state, completed.stdout))
    path = state.config.evidence_dir / "compose-config.yml"
    path.write_text(portable, encoding="utf-8")
    os.chmod(path, 0o600)


def _replace_private_json(path: Path, value: object) -> None:
    encoded = (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _invalid_start_manifest(
    state: ControllerOwnershipState,
    cleanup: Mapping[str, object],
    dependencies: Mapping[str, str],
) -> dict[str, object]:
    bundle = state.config.evidence_dir
    raw = bundle / "raw/lab"
    _write_evidence_json(raw / "reservation-events.json", [])
    _write_evidence_json(raw / "node-reports.json", [])
    _write_evidence_json(
        raw / "controller-commands.json", {"launches": [], "commands": []}
    )
    _write_evidence_json(raw / "cleanup.json", dict(cleanup))
    compose_config = bundle / "compose-config.yml"
    if not compose_config.exists():
        compose_config.write_text("status: start-failed\n", encoding="utf-8")
        os.chmod(compose_config, 0o600)

    def image_or_unavailable(value: str) -> str:
        if re.fullmatch(r"sha256:[a-f0-9]{64}", value):
            return value
        if re.fullmatch(r"[a-z0-9._/-]+@sha256:[a-f0-9]{64}", value):
            return value
        return "unavailable"

    config = state.config
    manifest: dict[str, object] = {
        "schema_version": "research-manifest.v1",
        "evidence_kind": "lab",
        "lab_variant": config.lab_variant,
        "status": "INVALID",
        "run_id": config.run_id,
        "source": {
            "commit": config.source_commit,
            "git_dirty": False,
            "source_sha256": config.source_snapshot_sha256,
            "paths": list(LAB_SOURCE_PATHS),
        },
        "command": [cast(list[str], _portable_value(state, state.controller_argv))],
        "timing": {
            "started_at": state.started_at,
            "completed_at": cleanup["completed_at"],
        },
        "host": {"os": platform.system(), "architecture": platform.machine()},
        "dependencies": dict(dependencies),
        "images": {
            "nats": image_or_unavailable(
                state.compose_environment.get("LAB_NATS_IMAGE", "")
            ),
            "aggregator": image_or_unavailable(
                state.compose_environment.get("LAB_AGGREGATOR_IMAGE", "")
            ),
            "dashboard": image_or_unavailable(
                state.compose_environment.get("LAB_DASHBOARD_IMAGE", "")
            ),
            "nginx": image_or_unavailable(
                state.compose_environment.get("LAB_NGINX_IMAGE", "")
            ),
            "fixture": (
                "unavailable"
                if config.fixture_image_id == "sha256:" + "0" * 64
                else image_or_unavailable(config.fixture_image_id)
            ),
        },
        "compose_config_sha256": file_sha256(compose_config),
        "schemas": {"manifest": "schemas/research-manifest.v1.json"},
        "cleanup": dict(cleanup),
        "artifacts": {},
        "controller": {
            "project": config.compose_project,
            "bind_host": config.bind_host,
            "advertised_host": config.advertised_host,
            "advertised_ip": config.advertised_ip,
            "app_url": config.app_url or "unavailable",
            "nats_url": config.nats_url or "unavailable",
            "monitor_url": config.monitor_url or "unavailable",
            "inventory_url": config.inventory_url or "unavailable",
            "declared_host_id": config.controller_host_id,
            "machine_id_sha256": config.controller_machine_id_sha256,
            "hostname": socket.gethostname(),
            "os_release": _os_release(),
            "architecture": platform.machine(),
        },
        "nodes": [],
        "observations": {
            "reservation_events": _artifact_ref(
                bundle, "raw/lab/reservation-events.json"
            ),
            "node_reports": _artifact_ref(bundle, "raw/lab/node-reports.json"),
            "controller_commands": _artifact_ref(
                bundle, "raw/lab/controller-commands.json"
            ),
            "playwright": [],
            "cleanup": _artifact_ref(bundle, "raw/lab/cleanup.json"),
        },
    }
    return cast(dict[str, object], _portable_value(state, manifest))


def _rollback_start(
    state_file: Path,
    state: ControllerOwnershipState,
    environment: ArtifactEnvironment,
    *,
    original_error: BaseException,
    dependencies: Mapping[str, str],
) -> None:
    failed = replace(state, phase="failed")
    cleanup_errors: list[str] = []
    try:
        write_controller_state(state_file, failed)
    except BaseException as error:
        cleanup_errors.append(f"failed-state journal failed: {error!r}")
    try:
        object.__setattr__(environment, "compose_file", failed.compose_file)
        object.__setattr__(
            environment,
            "compose_env",
            {**getattr(environment, "compose_env", {}), **failed.compose_environment},
        )
    except BaseException as error:
        cleanup_errors.append(f"cleanup recovery configuration failed: {error!r}")

    cleanup_report: object | None = None
    try:
        cleanup_report = environment.cleanup()
    except BaseException as error:
        cleanup_errors.append(f"artifact cleanup failed: {error!r}")

    image_remaining: tuple[OwnedResource, ...] = ()
    try:
        _remove_images_and_exports(failed, runner=subprocess.run)
    except _ImageCleanupError as error:
        image_remaining = error.remaining
        cleanup_errors.append(f"image cleanup failed: {error!r}")
    except BaseException as error:
        image_remaining = tuple(
            resource for resource in failed.owned_resources if resource.kind == "image"
        )
        cleanup_errors.append(f"image cleanup failed: {error!r}")

    nats_config_value = failed.compose_environment.get("LAB_NATS_CONFIG")
    private_paths = [
        failed.raw_credential_file,
        failed.service_env_file,
        state_file.parent / "controller.json",
    ]
    if nats_config_value:
        private_paths.append(Path(nats_config_value))
    pending = list(private_paths)
    for _ in range(2):
        retry: list[Path] = []
        for path in pending:
            try:
                path.unlink(missing_ok=True)
            except BaseException:
                retry.append(path)
        pending = retry
    for path in pending:
        cleanup_errors.append(f"private file cleanup failed: {path.name}")

    report_remaining = tuple(getattr(cleanup_report, "remaining", ()))
    report_attempted = tuple(getattr(cleanup_report, "attempted", ()))
    artifact_cleanup_complete = bool(
        cleanup_report is not None and getattr(cleanup_report, "completed", False)
    )
    observed_remaining: tuple[OwnedResource, ...] = ()
    if not artifact_cleanup_complete:
        try:
            observed_remaining = tuple(environment.owned_resources())
        except BaseException as error:
            cleanup_errors.append(f"owned-resource recovery query failed: {error!r}")
    attempted = tuple(
        dict.fromkeys(
            (
                *failed.owned_resources,
                *report_attempted,
                *observed_remaining,
            )
        )
    )
    remaining = tuple(
        dict.fromkeys(
            (
                *report_remaining,
                *observed_remaining,
                *image_remaining,
            )
        )
    )
    failed = replace(
        failed,
        owned_resources=tuple(dict.fromkeys((*failed.owned_resources, *remaining))),
    )
    try:
        write_controller_state(state_file, failed)
    except BaseException as error:
        cleanup_errors.append(f"remaining-resource journal failed: {error!r}")
    if not artifact_cleanup_complete:
        cleanup_errors.append("artifact cleanup reported incomplete")

    credential_removed = not failed.raw_credential_file.exists()
    artifact_state_removed = not getattr(
        environment, "state_dir", environment.scratch_dir / "state"
    ).exists()
    artifact_scratch_removed = not environment.scratch_dir.exists()
    artifact_record_removed = not environment.owner_record.exists()
    owned_removed = artifact_cleanup_complete and not remaining
    complete = all(
        (
            owned_removed,
            credential_removed,
            artifact_state_removed,
            artifact_scratch_removed,
            artifact_record_removed,
            not pending,
        )
    )

    cleanup = {
        "completed": complete,
        "attempted": _resource_dicts(attempted),
        "remaining": _resource_dicts(remaining),
        "owned_resources_removed": owned_removed,
        "foreign_resources_touched": False,
        "credential_removed": credential_removed,
        "artifact_state_removed": artifact_state_removed,
        "artifact_scratch_removed": artifact_scratch_removed,
        "artifact_recovery_record_removed": artifact_record_removed,
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    try:
        _write_evidence_json(state_file.parent / "cleanup.json", cleanup)
    except BaseException as error:
        cleanup_errors.append(f"cleanup receipt failed: {error!r}")
    if complete and failed.config.evidence_dir.is_dir():
        try:
            invalid_manifest = _invalid_start_manifest(failed, cleanup, dependencies)
            result = finalize_bundle(
                failed.config.evidence_dir,
                invalid_manifest,
                _repo_root() / "schemas/research-manifest.v1.json",
            )
            if (
                result != "INVALID"
                or not (failed.config.evidence_dir / "manifest.json").is_file()
            ):
                cleanup_errors.append("invalid bundle was not sealed")
        except BaseException as error:
            cleanup_errors.append(f"invalid finalization failed: {error!r}")
    for message in cleanup_errors:
        try:
            original_error.add_note(message)
        except AttributeError:
            break


def start_controller(args: argparse.Namespace) -> ControllerConfig:
    """Create a clean-source, run-owned controller and persist recovery state."""
    run_id = validate_run_id(str(args.run_id))
    host_id = validate_declared_host_id(str(args.host_id))
    if args.lab_variant not in {"lifecycle", "operator-smoke", "operator-evidence"}:
        raise LabConfigError("invalid lab variant")
    repo_root = _repo_root()
    state_root = Path(args.state_root).resolve()
    state_file = state_root / run_id / "controller-state.json"
    if state_file.exists():
        existing = load_controller_state(state_file)
        if existing.phase == "active":
            raise LabConfigError("controller already has an active state")
        raise LabConfigError("controller state requires recovery through stop")
    source = capture_clean_source_provenance(repo_root)
    if source.dirty:
        raise LabConfigError("lab source paths must be clean")
    _validate_host_platform()
    bind_host, advertised_ip, http_requested, nats_requested, monitor_requested = (
        _validate_start_network(args)
    )
    tool_versions = _validate_toolchain(repo_root, str(args.lab_variant))
    machine_id_sha256 = _controller_machine_id_sha256()
    state_dir = state_file.parent
    controller_config_file = state_dir / "controller.json"
    evidence_dir = repo_root / "data/research/results/lab" / run_id
    if evidence_dir.exists():
        raise LabConfigError("lab evidence directory already exists")
    environment: ArtifactEnvironment | None = None
    state: ControllerOwnershipState | None = None
    try:
        scratch_root = (
            Path(
                os.environ.get("EC_ARTIFACT_SCRATCH_ROOT", "/tmp/edgecitadel-artifact")
            )
            .expanduser()
            .resolve()
        )
        previous_scratch = os.environ.get("EC_ARTIFACT_SCRATCH_ROOT")
        os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = str(scratch_root)
        try:
            environment = ArtifactEnvironment.create(
                run_id, "edgecitadel", (repo_root / "tmp/research").resolve()
            )
        finally:
            if previous_scratch is None:
                os.environ.pop("EC_ARTIFACT_SCRATCH_ROOT", None)
            else:
                os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = previous_scratch
        credential_token(environment.credential_file)
        service_env = environment.scratch_dir / "service.env"
        write_service_env_file(service_env, environment.credential_file)
        nats_config = environment.scratch_dir / "nats-lab.conf"
        shutil.copyfile(repo_root / "scripts/research/nats-lab.conf.tpl", nats_config)
        os.chmod(nats_config, 0o600)
        data_dir = environment.scratch_dir / "lab-data"
        data_dir.mkdir(mode=0o700)
        aggregator_tag = f"edgecitadel-lab-aggregator:{run_id}"
        dashboard_tag = f"edgecitadel-lab-dashboard:{run_id}"
        toolchain = json.loads(
            (repo_root / "scripts/research/toolchain.json").read_text()
        )
        nats_image = str(toolchain["nats_image"])
        if "@sha256:" not in nats_image or "@sha256:" not in LAB_NGINX_IMAGE:
            raise LabConfigError("lab base images must use repository digests")
        token_hash = credential_sha256(environment.credential_file)
        placeholder = ControllerConfig(
            run_id=run_id,
            lab_variant=args.lab_variant,
            controller_host_id=host_id,
            compose_project=environment.project,
            bind_host=bind_host,
            advertised_host=str(args.advertise_host),
            advertised_ip=advertised_ip,
            app_url="",
            agg_url="",
            nats_url="",
            monitor_url="",
            inventory_url="",
            controller_machine_id_sha256=machine_id_sha256,
            source_commit=source.commit,
            source_snapshot_sha256=source.source_snapshot_sha256,
            credential_sha256=token_hash,
            credential_file=environment.credential_file,
            fixture_image_id="sha256:" + "0" * 64,
            state_dir=state_dir,
            evidence_dir=evidence_dir,
        )
        compose_file = repo_root / "scripts/research/docker-compose.lab.yml"
        compose_environment = {
            "LAB_RUN_ID": run_id,
            "LAB_BIND_HOST": bind_host,
            "LAB_HTTP_PORT": http_requested,
            "LAB_NATS_PORT": nats_requested,
            "LAB_MONITOR_PORT": monitor_requested,
            "LAB_SERVICE_ENV_FILE": str(service_env),
            "LAB_NATS_CONFIG": str(nats_config),
            "LAB_DATA_DIR": str(data_dir),
            "LAB_TOKEN_SHA256": token_hash,
            "LAB_NATS_IMAGE": nats_image,
            "LAB_AGGREGATOR_IMAGE": aggregator_tag,
            "LAB_DASHBOARD_IMAGE": dashboard_tag,
            "LAB_NGINX_IMAGE": LAB_NGINX_IMAGE,
        }
        state = ControllerOwnershipState(
            schema_version="lab-controller-state.v1",
            phase="starting",
            config=placeholder,
            compose_file=compose_file,
            compose_environment=compose_environment,
            artifact_scratch_root=scratch_root,
            raw_credential_file=environment.credential_file,
            service_env_file=service_env,
            owned_resources=(),
            completed_cleanup_steps=(),
            exported_image_paths=(),
            controller_argv=(
                "scripts/research/lab_controller.py",
                "start",
                "--run-id",
                run_id,
                "--host-id",
                host_id,
                "--lab-variant",
                str(args.lab_variant),
                "--bind-host",
                bind_host,
                "--advertise-host",
                str(args.advertise_host),
            ),
            started_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        write_controller_state(state_file, state)
        evidence_dir.mkdir(parents=True, mode=0o700)
        _write_evidence_json(
            evidence_dir / "controller-facts.json",
            {
                "source": {
                    "commit": source.commit,
                    "dirty": source.dirty,
                    "source_snapshot_sha256": source.source_snapshot_sha256,
                },
                "dependencies": tool_versions,
                "controller_machine_id_sha256": machine_id_sha256,
                "started_at": state.started_at,
            },
        )
        _validate_nats_configuration(state)

        state = _journal_images(state_file, state, aggregator_tag, dashboard_tag)
        subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(compose_file),
                "build",
                "aggregator",
                "dashboard",
            ],
            cwd=repo_root,
            env={**os.environ, **compose_environment},
            check=True,
        )
        aggregator_image = _image_id(aggregator_tag, repo_root)
        state = _journal_images(state_file, state, aggregator_image)
        dashboard_image = _image_id(dashboard_tag, repo_root)
        state = _journal_images(state_file, state, dashboard_image)

        fixture_tag = f"edgecitadel-lab-fixture:{run_id}"
        state = _journal_images(state_file, state, fixture_tag)
        fixture = build_fixture_image(
            repo_root,
            run_id,
            lambda argv, *, cwd: subprocess.run(
                argv, cwd=cwd, check=True, capture_output=True, text=True
            ),
        )
        placeholder = replace(placeholder, fixture_image_id=fixture.image_id)
        compose_environment = {
            **compose_environment,
            "LAB_AGGREGATOR_IMAGE": aggregator_image,
            "LAB_DASHBOARD_IMAGE": dashboard_image,
        }
        state = replace(
            state,
            config=placeholder,
            compose_environment=compose_environment,
        )
        write_controller_state(state_file, state)
        state = _journal_images(state_file, state, fixture.image_id)

        write_private_json(controller_config_file, placeholder.to_dict())
        _write_evidence_json(
            evidence_dir / "controller-start.json",
            {
                "source": {
                    "commit": source.commit,
                    "dirty": source.dirty,
                    "source_snapshot_sha256": source.source_snapshot_sha256,
                },
                "dependencies": tool_versions,
                "images": {
                    "nats": nats_image,
                    "aggregator": aggregator_image,
                    "dashboard": dashboard_image,
                    "nginx": LAB_NGINX_IMAGE,
                    "fixture": fixture.image_id,
                },
                "service_env_sha256": sha256_file(service_env),
                "controller_machine_id_sha256": machine_id_sha256,
                "started_at": state.started_at,
            },
        )
        append_observation(
            evidence_dir / "lab-observations.jsonl",
            {
                "event": "controller-starting",
                "agent_id": None,
                "reservation_id": None,
                "task_id": None,
                "data": {"run_id": run_id, "host_id": host_id},
            },
        )
        environment.start_topology(compose_file, compose_environment)
        http_port = (
            int(http_requested)
            if http_requested
            else _port(environment.project, compose_file, "nginx", 80)
        )
        nats_port = (
            int(nats_requested)
            if nats_requested
            else _port(environment.project, compose_file, "nats", 4222)
        )
        monitor_port = (
            int(monitor_requested)
            if monitor_requested
            else _port(environment.project, compose_file, "nats", 8222)
        )
        app_url = f"http://{advertised_ip}:{http_port}"
        config = replace(
            placeholder,
            app_url=app_url,
            agg_url=app_url,
            nats_url=f"nats://{advertised_ip}:{nats_port}",
            monitor_url=f"http://127.0.0.1:{monitor_port}",
            inventory_url=f"{app_url}/api/lab/status",
        )
        _replace_private_json(controller_config_file, config.to_dict())
        resources = tuple(
            dict.fromkeys((*environment.owned_resources(), *state.owned_resources))
        )
        state = replace(state, config=config, owned_resources=resources)
        write_controller_state(state_file, state)
        _render_compose(state)
        report_value = run_controller_preflight(config, environment.credential_file)
        if inspect.isawaitable(report_value):
            report = asyncio.run(report_value)
        else:
            report = report_value
        _write_evidence_json(evidence_dir / "preflight.json", report.to_dict())
        if not report.valid:
            raise LabConfigError("controller preflight failed")
        active = replace(state, phase="active")
        write_controller_state(state_file, active)
        return config
    except BaseException as error:
        if environment is not None and state is not None:
            _rollback_start(
                state_file,
                state,
                environment,
                original_error=error,
                dependencies=tool_versions,
            )
        raise


def stop_controller(
    state_file: Path,
    *,
    runner: object = subprocess.run,
    opener: object = urllib.request.urlopen,
) -> Mapping[str, object]:
    """Stop a run from its durable ownership state, not process memory."""
    state = load_controller_state(state_file)
    cleanup_file = state_file.parent / "cleanup.json"
    manifest_file = state.config.evidence_dir / "manifest.json"
    failed_start_recovery = cleanup_file.is_file() and not manifest_file.is_file()
    if state.phase == "stopped" and cleanup_file.is_file():
        return _load_complete_cleanup(cleanup_file)
    if state.phase not in {"starting", "active", "stopping", "failed"}:
        raise LabConfigError("controller state cannot be stopped")
    if state.phase != "stopping":
        state = replace(state, phase="stopping")
        write_controller_state(state_file, state)

    if (
        manifest_file.is_file()
        and "manifest-finalized" not in state.completed_cleanup_steps
    ):
        cleanup = _load_complete_cleanup(cleanup_file)
        report = check_bundle(
            state.config.evidence_dir,
            expected_kind="lab",
            source_root=_repo_root().resolve(),
        )
        if not report.valid:
            issue_codes = {issue.code for issue in report.issues}
            if (
                "EVIDENCE_STATUS_INVALID" not in issue_codes
                or not any(code.startswith("LAB_") for code in issue_codes)
                or any(
                    code != "EVIDENCE_STATUS_INVALID" and not code.startswith("LAB_")
                    for code in issue_codes
                )
            ):
                report.require_valid()
        state = _completed_cleanup_step(state_file, state, "manifest-finalized")
        state = replace(state, phase="stopped")
        write_controller_state(state_file, state)
        return cleanup

    cast_runner = runner
    environment = {**os.environ, **state.compose_environment}
    before_inventory_file = state_file.parent / "docker-inventory-before.json"
    if "inventory-snapshotted" not in state.completed_cleanup_steps:
        if failed_start_recovery:
            _snapshot_failed_start_evidence(state)
        else:
            _snapshot_stop_evidence(state, opener=opener)
        if before_inventory_file.is_file():
            before_inventory = _load_resource_journal(before_inventory_file)
        else:
            before_inventory = _docker_inventory(runner)
            write_private_json(
                before_inventory_file,
                _resource_dicts(before_inventory),
            )
        state = _completed_cleanup_step(
            state_file,
            state,
            "inventory-snapshotted",
        )
    else:
        before_inventory = _load_resource_journal(before_inventory_file)
    if "compose-down" not in state.completed_cleanup_steps:
        if failed_start_recovery:
            _ensure_recovery_compose_inputs(state)
        cast_runner(
            [
                "docker",
                "compose",
                "--project-name",
                state.config.compose_project,
                "--file",
                str(state.compose_file),
                "down",
                "--volumes",
                "--remove-orphans",
            ],
            check=True,
            env=environment,
            capture_output=True,
            text=True,
        )
        state = _completed_cleanup_step(state_file, state, "compose-down")
    if "artifact-cleanup" not in state.completed_cleanup_steps:
        token = (
            credential_token(state.raw_credential_file)
            if state.raw_credential_file.exists()
            else None
        )
        completed = cast_runner(
            [
                str(_repo_root() / "scripts/research/run-python"),
                str(_repo_root() / "scripts/research/run_artifact.py"),
                "cleanup",
                "--run-id",
                state.config.run_id,
                "--scratch-root",
                str(state.artifact_scratch_root),
            ],
            check=True,
            env=environment,
            capture_output=True,
            text=True,
        )
        if token is not None and (
            token in (completed.stdout or "") or token in (completed.stderr or "")
        ):
            raise LabConfigError("artifact cleanup output contained a credential")
        if any(path.exists() for path in _artifact_cleanup_paths(state)):
            raise LabConfigError("Slice 1 artifact cleanup left owned state")
        state = _completed_cleanup_step(state_file, state, "artifact-cleanup")
    if "images-exports-removed" not in state.completed_cleanup_steps:
        _remove_images_and_exports(state, runner=runner)
        state = _completed_cleanup_step(
            state_file,
            state,
            "images-exports-removed",
        )
    if "private-files-removed" not in state.completed_cleanup_steps:
        if state.raw_credential_file.exists():
            raise LabConfigError("raw credential survived Slice 1 cleanup")
        _remove_private_controller_files(state)
        state = _completed_cleanup_step(
            state_file,
            state,
            "private-files-removed",
        )
    if "owned-resources-verified" not in state.completed_cleanup_steps:
        remaining = _remaining_owned_resources(state.owned_resources, runner=runner)
        if remaining:
            raise LabConfigError("owned Docker resources remain after cleanup")
        state = _completed_cleanup_step(
            state_file,
            state,
            "owned-resources-verified",
        )
    else:
        remaining = ()
    if "foreign-resources-verified" not in state.completed_cleanup_steps:
        after_inventory = _docker_inventory(runner)
        foreign_touched = _foreign_resources_touched(
            before_inventory,
            after_inventory,
            frozenset(state.owned_resources),
        )
        if foreign_touched:
            raise LabConfigError("pre-existing foreign Docker resources disappeared")
        state = _completed_cleanup_step(
            state_file,
            state,
            "foreign-resources-verified",
        )
    else:
        foreign_touched = False
    run_scratch, artifact_state, raw_credential, owner_record = _artifact_cleanup_paths(
        state
    )
    if "cleanup-written" not in state.completed_cleanup_steps:
        if cleanup_file.is_file():
            try:
                cleanup = _load_complete_cleanup(cleanup_file)
            except LabConfigError:
                if not failed_start_recovery:
                    raise
                cleanup = {
                    "completed": True,
                    "attempted": _resource_dicts(state.owned_resources),
                    "remaining": _resource_dicts(remaining),
                    "owned_resources_removed": not remaining,
                    "foreign_resources_touched": foreign_touched,
                    "credential_removed": not raw_credential.exists(),
                    "artifact_state_removed": not artifact_state.exists(),
                    "artifact_scratch_removed": not run_scratch.exists(),
                    "artifact_recovery_record_removed": not owner_record.exists(),
                    "completed_at": datetime.now(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                _replace_private_json(cleanup_file, cleanup)
        else:
            cleanup = {
                "completed": True,
                "attempted": _resource_dicts(state.owned_resources),
                "remaining": _resource_dicts(remaining),
                "owned_resources_removed": not remaining,
                "foreign_resources_touched": foreign_touched,
                "credential_removed": not raw_credential.exists(),
                "artifact_state_removed": not artifact_state.exists(),
                "artifact_scratch_removed": not run_scratch.exists(),
                "artifact_recovery_record_removed": not owner_record.exists(),
                "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
            write_private_json(cleanup_file, cleanup)
        _write_evidence_json(
            state.config.evidence_dir / "raw/lab/cleanup.json",
            cleanup,
        )
        state = _completed_cleanup_step(state_file, state, "cleanup-written")
    else:
        cleanup = _load_complete_cleanup(cleanup_file)
    if "manifest-finalized" not in state.completed_cleanup_steps:
        manifest = _build_lab_manifest(state, cleanup, runner=runner)
        try:
            require_complete_lab_manifest(
                state.config.evidence_dir,
                manifest,
                _repo_root().resolve(),
            )
        except LabConfigError:
            manifest["status"] = "INVALID"
            if (
                finalize_bundle(
                    state.config.evidence_dir,
                    manifest,
                    _repo_root() / "schemas/research-manifest.v1.json",
                )
                != "INVALID"
            ):
                raise LabConfigError("invalid lab bundle unexpectedly finalized")
        else:
            if (
                finalize_bundle(
                    state.config.evidence_dir,
                    manifest,
                    _repo_root() / "schemas/research-manifest.v1.json",
                )
                != "PASS"
            ):
                raise LabConfigError("lab bundle finalization failed")
        state = _completed_cleanup_step(state_file, state, "manifest-finalized")
    state = replace(state, phase="stopped")
    write_controller_state(state_file, state)
    return cleanup


def export_fixture_image(
    state_file: Path,
    output: Path,
    result_file: Path,
    *,
    runner: object = subprocess.run,
) -> Mapping[str, str]:
    """Export the persisted immutable fixture image and journal the exact tar."""
    state = load_controller_state(state_file)
    if state.phase != "active":
        raise LabConfigError("controller state is not active")
    output_path = output.resolve()
    result_path = result_file.resolve()
    if not output_path.is_absolute() or not result_path.is_absolute():
        raise LabConfigError("export paths must be absolute")
    if output_path.exists() or result_path.exists():
        raise LabConfigError("export output already exists")
    image_id = state.config.fixture_image_id
    if (
        not image_id.startswith("sha256:")
        or len(image_id) != 71
        or any(
            character not in "0123456789abcdef"
            for character in image_id.removeprefix("sha256:")
        )
    ):
        raise LabConfigError("fixture image is not immutable")
    output_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(output_path.parent, 0o700)
    temporary = output_path.with_name(output_path.name + ".tmp")
    if temporary.exists():
        raise LabConfigError("export output is active")
    try:
        runner(
            ["docker", "image", "save", "--output", str(temporary), image_id],
            check=True,
            capture_output=True,
            text=True,
        )
        if not temporary.is_file():
            raise LabConfigError("fixture image export was not created")
        descriptor = os.open(temporary, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.chmod(temporary, 0o600)
        os.replace(temporary, output_path)
        result = {
            "fixture_image_id": image_id,
            "output": str(output_path),
            "sha256": sha256_file(output_path),
        }
        write_private_json(result_path, result)
        write_controller_state(
            state_file,
            replace(
                state, exported_image_paths=state.exported_image_paths + (output_path,)
            ),
        )
        return result
    except BaseException:
        temporary.unlink(missing_ok=True)
        if output_path.exists() and not result_path.exists():
            output_path.unlink(missing_ok=True)
        raise


def submit_command(
    state_file: Path,
    agent_id: str,
    body: str,
    expected_output: str,
    result_file: Path,
    *,
    opener: object = urllib.request.urlopen,
) -> Mapping[str, object]:
    """Submit one production HTTP command and retain its accepted task identity."""
    state = load_controller_state(state_file)
    if state.phase != "active":
        raise LabConfigError("controller state is not active")
    if not agent_id or not body or not expected_output or result_file.exists():
        raise LabConfigError("command inputs are invalid")
    request = urllib.request.Request(
        f"{state.config.agg_url}/api/command/{agent_id}",
        method="POST",
        data=json.dumps({"body": body}, sort_keys=True, separators=(",", ":")).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with opener(request, timeout=10) as response:
            if response.status != 202:
                raise LabConfigError("command was not accepted")
            accepted = json.loads(response.read())
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise LabConfigError("command request failed") from error
    task_id, accepted_at = _validated_acceptance(accepted, agent_id)
    result = {
        "run_id": state.config.run_id,
        "agent_id": agent_id,
        "task_id": task_id,
        "wire_copies": 1,
        "accepted_at": accepted_at,
        "terminal_at": None,
        "expected_output": expected_output,
        "status": "accepted",
    }
    write_private_json(result_file.resolve(), result)
    return result


def qualify_controller(
    state_file: Path, *, source_root: Path | None = None
) -> LabQualification:
    """Classify one stopped finalized bundle without mutating controller state."""
    state = load_controller_state(state_file)
    manifest = state.config.evidence_dir / "manifest.json"
    if (
        state.phase != "stopped"
        or "manifest-finalized" not in state.completed_cleanup_steps
        or not manifest.is_file()
    ):
        raise LabConfigError("controller bundle is not finalized")
    qualification, checker_valid = qualify_bundle(
        bundle=state.config.evidence_dir,
        source_root=(source_root or _repo_root()).resolve(),
    )
    if not checker_valid:
        raise LabConfigError("finalized lab bundle is invalid")
    return qualification


def await_command(
    state_file: Path,
    agent_id: str,
    task_id: str,
    expected_output: str,
    result_file: Path,
    *,
    opener: object = urllib.request.urlopen,
) -> Mapping[str, object]:
    """Reduce persisted task messages into one exact terminal receipt."""
    state = load_controller_state(state_file)
    if (
        state.phase != "active"
        or not agent_id
        or not task_id
        or not expected_output
        or result_file.exists()
    ):
        raise LabConfigError("await inputs are invalid")
    try:
        with opener(
            f"{state.config.agg_url}/api/messages?task_id={task_id}", timeout=10
        ) as response:
            if response.status != 200:
                raise LabConfigError("task messages are unavailable")
            messages = json.loads(response.read())
        with opener(
            f"{state.config.agg_url}/api/agents/{agent_id}/queue", timeout=10
        ) as response:
            if response.status != 200:
                raise LabConfigError("task queue is unavailable")
            queue = json.loads(response.read())
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise LabConfigError("await request failed") from error
    terminals = (
        [
            item
            for item in messages
            if isinstance(item, dict)
            and item.get("type") == "result"
            and item.get("task_state") == "completed"
            and isinstance(item.get("payload"), dict)
        ]
        if isinstance(messages, list)
        else []
    )
    if len(terminals) != 1 or terminals[0]["payload"].get("body") != expected_output:
        raise LabConfigError("task terminal is invalid")
    if (
        not isinstance(queue, dict)
        or queue.get("pending") != 0
        or queue.get("ack_pending") != 0
    ):
        raise LabConfigError("task queue is not drained")
    terminal_at = terminals[0].get("timestamp")
    if not isinstance(terminal_at, str):
        raise LabConfigError("task terminal is invalid")
    result = {
        "run_id": state.config.run_id,
        "agent_id": agent_id,
        "task_id": task_id,
        "expected_output": expected_output,
        "terminal_at": terminal_at,
        "status": "completed",
    }
    write_private_json(result_file.resolve(), result)
    return result


def _active_command_state(
    state_file: Path, result_file: Path
) -> ControllerOwnershipState:
    state = load_controller_state(state_file.resolve())
    if state.phase != "active":
        raise LabConfigError("controller state is not active")
    if (state.config.evidence_dir / "manifest.json").exists():
        raise LabConfigError("lab evidence bundle is finalized")
    if result_file.resolve().exists():
        raise LabConfigError("command result file already exists")
    return state


def _request_json(
    url: str,
    *,
    opener: object,
    expected_status: int,
    method: str = "GET",
    body: Mapping[str, object] | None = None,
    token: str | None = None,
) -> object:
    headers: dict[str, str] = {}
    data = None
    if body is not None:
        data = json.dumps(
            body, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with opener(request, timeout=10) as response:
            if response.status != expected_status:
                error_type = (
                    _TransientLabHTTPError
                    if 500 <= response.status <= 599
                    else LabConfigError
                )
                raise error_type(f"unexpected HTTP status {response.status}")
            payload = response.read()
    except LabConfigError:
        raise
    except urllib.error.HTTPError as error:
        error_type = (
            _TransientLabHTTPError if 500 <= error.code <= 599 else LabConfigError
        )
        raise error_type(f"unexpected HTTP status {error.code}") from None
    except (OSError, urllib.error.URLError) as error:
        raise _TransientLabHTTPError("lab HTTP request failed") from error
    try:
        return json.loads(payload)
    except json.JSONDecodeError as error:
        raise LabConfigError("lab HTTP response is invalid") from error


def _reservation_for_agent(
    state: ControllerOwnershipState,
    agent_id: str,
    *,
    opener: object,
) -> tuple[str, Mapping[str, object]]:
    inventory = _request_json(
        state.config.inventory_url,
        opener=opener,
        expected_status=200,
        token=credential_token(state.raw_credential_file),
    )
    if not isinstance(inventory, Mapping):
        raise LabConfigError("lab inventory response is invalid")
    reservations = inventory.get("reservations")
    matches = (
        [
            item
            for item in reservations
            if isinstance(item, Mapping)
            and item.get("agent_id") == agent_id
            and item.get("state") in {"active", "retained"}
            and isinstance(item.get("reservation_id"), str)
        ]
        if isinstance(reservations, list)
        else []
    )
    if len(matches) != 1:
        raise LabConfigError("unknown or unreserved agent")
    return str(matches[0]["reservation_id"]), inventory


def _timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _canonical_command(agent_id: str, body: str) -> tuple[str, str, bytes]:
    task_id = str(uuid.uuid4())
    wire_id = str(uuid.uuid4())
    envelope = {
        "v": 1,
        "id": wire_id,
        "type": "command",
        "sender_id": "aggregator",
        "recipient_id": agent_id,
        "task_id": task_id,
        "context_id": task_id,
        "hop_count": 0,
        "timestamp": _timestamp(),
        "payload": {"body": body},
    }
    encoded = json.dumps(
        envelope, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return task_id, envelope["timestamp"], encoded


async def _publish_duplicate_wires_async(
    state: ControllerOwnershipState,
    subject: str,
    payload: bytes,
    *,
    connection_factory: Callable[[], object] | None = None,
) -> None:
    if connection_factory is None:
        from nats.aio.client import Client as NATS

        connection_factory = NATS

    connection = connection_factory()
    await connection.connect(
        servers=[state.config.nats_url],
        token=credential_token(state.raw_credential_file),
    )
    try:
        jetstream = connection.jetstream()
        await jetstream.publish(subject, payload)
        await jetstream.publish(subject, payload)
        await connection.flush()
    except BaseException as error:
        try:
            await connection.drain()
        except BaseException as drain_error:
            error.add_note(f"NATS drain failed: {drain_error!r}")
        raise
    else:
        await connection.drain()


def _publish_duplicate_wires(
    state: ControllerOwnershipState,
    subject: str,
    payload: bytes,
    headers: object,
) -> None:
    if headers is not None:
        raise LabConfigError("duplicate wire publication forbids NATS headers")
    asyncio.run(_publish_duplicate_wires_async(state, subject, payload))


def _accepted_observation(
    state: ControllerOwnershipState,
    *,
    agent_id: str,
    reservation_id: str,
    task_id: str,
    accepted_at: str,
    expected_output: str,
    wire_copies: int,
    request_body_sha256: str,
) -> None:
    append_observation(
        state.config.evidence_dir / "lab-observations.jsonl",
        {
            "event": "command.accepted",
            "agent_id": agent_id,
            "reservation_id": reservation_id,
            "task_id": task_id,
            "data": {
                "accepted_at": accepted_at,
                "expected_output": expected_output,
                "request_body_sha256": request_body_sha256,
                "wire_copies": wire_copies,
            },
        },
    )


def _wire_observations(
    state: ControllerOwnershipState,
    *,
    agent_id: str,
    reservation_id: str,
    task_id: str,
    wire_copies: int,
) -> None:
    for copy_index in range(1, wire_copies + 1):
        append_observation(
            state.config.evidence_dir / "lab-observations.jsonl",
            {
                "event": "command.wire_submitted",
                "agent_id": agent_id,
                "reservation_id": reservation_id,
                "task_id": task_id,
                "data": {"copy_index": copy_index, "wire_copies": wire_copies},
            },
        )


def _accepted_record(
    state: ControllerOwnershipState, task_id: str
) -> tuple[str, str, int, str, str, str]:
    journal = state.config.evidence_dir / "lab-observations.jsonl"
    try:
        records = [
            json.loads(line) for line in journal.read_text().splitlines() if line
        ]
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("command acceptance observation is unavailable") from error
    matches = [
        item
        for item in records
        if isinstance(item, Mapping)
        and item.get("event") == "command.accepted"
        and item.get("task_id") == task_id
        and isinstance(item.get("data"), Mapping)
    ]
    if len(matches) != 1:
        raise LabConfigError("command acceptance observation is invalid")
    record = matches[0]
    data = record["data"]
    agent_id = record.get("agent_id")
    reservation_id = record.get("reservation_id")
    accepted_at = data.get("accepted_at")
    wire_copies = data.get("wire_copies")
    expected_output = data.get("expected_output")
    request_body_sha256 = data.get("request_body_sha256")
    if (
        not isinstance(agent_id, str)
        or not isinstance(reservation_id, str)
        or not isinstance(accepted_at, str)
        or wire_copies not in {1, 2}
        or not isinstance(expected_output, str)
        or not isinstance(request_body_sha256, str)
        or len(request_body_sha256) != 64
    ):
        raise LabConfigError("command acceptance observation is invalid")
    return (
        agent_id,
        reservation_id,
        int(wire_copies),
        accepted_at,
        expected_output,
        request_body_sha256,
    )


def _terminal_receipt(
    messages: object,
    queue: object,
    agent_id: str,
    task_id: str,
    expected_output: str,
    request_body_sha256: str,
    wire_copies: int,
) -> tuple[str, bool, str | None, int, bool]:
    if not isinstance(messages, list):
        raise LabConfigError("task messages are invalid")
    commands = [
        item
        for item in messages
        if isinstance(item, Mapping) and item.get("type") == "command"
    ]
    valid_commands = [
        item
        for item in commands
        if item.get("sender_id") == "aggregator"
        and item.get("recipient_id") == agent_id
        and item.get("task_id") == task_id
        and item.get("context_id") == task_id
        and item.get("hop_count") == 0
        and isinstance(item.get("payload"), Mapping)
        and hashlib.sha256(
            json.dumps(
                item["payload"].get("body"),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()
        == request_body_sha256
    ]
    if (wire_copies == 1 and not commands) or len(valid_commands) != len(commands):
        raise LabConfigError("task command identity is invalid")
    command_identities = {
        (
            item.get("sender_id"),
            item.get("recipient_id"),
            item.get("task_id"),
            item.get("context_id"),
            item.get("hop_count"),
            json.dumps(item.get("payload"), sort_keys=True, separators=(",", ":")),
        )
        for item in valid_commands
    }
    if commands and len(command_identities) != 1:
        raise LabConfigError("task command identity is conflicting")
    terminals = [
        item
        for item in messages
        if isinstance(item, Mapping)
        and item.get("type") == "result"
        and item.get("task_state") in _TERMINAL_STATES
    ]
    if terminals:
        if any(
            item.get("sender_id") != agent_id
            or item.get("recipient_id") != "aggregator"
            or item.get("task_id") != task_id
            or item.get("context_id") != task_id
            or item.get("hop_count") != 0
            for item in terminals
        ):
            raise LabConfigError("task terminal identity is invalid")
        identities = {
            (
                item.get("sender_id"),
                item.get("recipient_id"),
                item.get("task_id"),
                item.get("context_id"),
                item.get("hop_count"),
                item.get("task_state"),
                json.dumps(item.get("payload"), sort_keys=True, separators=(",", ":")),
            )
            for item in terminals
        }
        if len(identities) != 1:
            raise LabConfigError("task terminal is conflicting")
        terminal = terminals[0]
        payload = terminal.get("payload")
        if (
            terminal.get("task_state") != "completed"
            or not isinstance(payload, Mapping)
            or dict(payload) != {"body": expected_output}
            or any(not isinstance(item.get("timestamp"), str) for item in terminals)
        ):
            raise LabConfigError("task terminal is invalid")
        if (
            not isinstance(queue, Mapping)
            or queue.get("pending") != 0
            or queue.get("ack_pending") != 0
        ):
            raise LabConfigError("task queue is not drained")
        return (
            min(str(item["timestamp"]) for item in terminals),
            True,
            expected_output,
            len(identities),
            False,
        )
    if not isinstance(queue, Mapping):
        raise LabConfigError("task queue is invalid")
    return "", False, None, 0, False


def _inventory_events(
    inventory: object, agent_id: str, reservation_id: str
) -> tuple[Mapping[str, object], ...]:
    if not isinstance(inventory, Mapping) or not isinstance(
        inventory.get("reservation_events"), list
    ):
        raise LabConfigError("reservation event snapshot is invalid")
    return tuple(
        item
        for item in inventory["reservation_events"]
        if isinstance(item, Mapping)
        and item.get("agent_id") == agent_id
        and item.get("reservation_id") == reservation_id
    )


def _require_queued_reconnect(
    *,
    inventory: object,
    agent_id: str,
    reservation_id: str,
    accepted_at: str,
    terminal_at: str,
) -> None:
    events = _inventory_events(inventory, agent_id, reservation_id)
    retained = [item for item in events if item.get("event") == "retained"]
    resumed = [item for item in events if item.get("event") == "resumed"]
    if len(retained) != 1 or len(resumed) != 1:
        raise LabConfigError("queued reconnect reservation events are invalid")
    retained_sequence = retained[0].get("sequence")
    resumed_sequence = resumed[0].get("sequence")
    retained_at = retained[0].get("observed_at")
    resumed_at = resumed[0].get("observed_at")
    if (
        type(retained_sequence) is not int
        or type(resumed_sequence) is not int
        or retained_sequence >= resumed_sequence
        or not all(isinstance(value, str) for value in (retained_at, resumed_at))
        or not (str(retained_at) < accepted_at < str(resumed_at) < terminal_at)
    ):
        raise LabConfigError("queued reconnect event order is invalid")


def _await_run_command_reserved(
    state: ControllerOwnershipState,
    task_id: str,
    expected_output: str,
    result_path: Path,
    *,
    qualification_kind: str | None = None,
    opener: object = urllib.request.urlopen,
    timeout_s: float = 30.0,
    poll_interval_s: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
) -> Mapping[str, object]:
    if (
        not task_id
        or not expected_output
        or qualification_kind not in {None, "queued-reconnect"}
    ):
        raise LabConfigError("await inputs are invalid")
    (
        agent_id,
        reservation_id,
        wire_copies,
        accepted_at,
        recorded_output,
        request_body_sha256,
    ) = _accepted_record(state, task_id)
    if recorded_output != expected_output:
        raise LabConfigError("await expected output differs from acceptance")
    deadline = time.monotonic() + max(timeout_s, 0)
    terminal_at = ""
    while True:
        try:
            messages = _request_json(
                f"{state.config.agg_url}/api/messages?task_id={quote(task_id, safe='')}",
                opener=opener,
                expected_status=200,
            )
            queue = _request_json(
                f"{state.config.agg_url}/api/agents/{quote(agent_id, safe='')}/queue",
                opener=opener,
                expected_status=200,
            )
        except _TransientLabHTTPError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LabConfigError("task terminal deadline expired") from None
            sleeper(min(poll_interval_s, remaining))
            continue
        (
            terminal_at,
            complete,
            terminal_output,
            terminal_count,
            conflicting_terminal,
        ) = _terminal_receipt(
            messages,
            queue,
            agent_id,
            task_id,
            expected_output,
            request_body_sha256,
            wire_copies,
        )
        if complete:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise LabConfigError("task terminal deadline expired")
        sleeper(min(poll_interval_s, remaining))
    if qualification_kind == "queued-reconnect":
        while True:
            try:
                inventory = _request_json(
                    state.config.inventory_url,
                    opener=opener,
                    expected_status=200,
                    token=credential_token(state.raw_credential_file),
                )
            except _TransientLabHTTPError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LabConfigError("task terminal deadline expired") from None
                sleeper(min(poll_interval_s, remaining))
                continue
            break
        _require_queued_reconnect(
            inventory=inventory,
            agent_id=agent_id,
            reservation_id=reservation_id,
            accepted_at=accepted_at,
            terminal_at=terminal_at,
        )
    append_observation(
        state.config.evidence_dir / "lab-observations.jsonl",
        {
            "event": "command.terminal",
            "agent_id": agent_id,
            "reservation_id": reservation_id,
            "task_id": task_id,
            "data": {"terminal_at": terminal_at, "expected_output": expected_output},
        },
    )
    result = {
        "run_id": state.config.run_id,
        "agent_id": agent_id,
        "reservation_id": reservation_id,
        "task_id": task_id,
        "wire_copies": wire_copies,
        "http_status": 202 if wire_copies == 1 else None,
        "accepted_at": accepted_at,
        "terminal_at": terminal_at,
        "expected_output": expected_output,
        "terminal_output": terminal_output,
        "terminal_count": terminal_count,
        "conflicting_terminal": conflicting_terminal,
        "qualification_kind": qualification_kind or "direct",
        "status": "completed",
    }
    write_private_json(result_path, result)
    return result


def await_run_command(
    state_file: Path,
    task_id: str,
    expected_output: str,
    result_file: Path,
    *,
    qualification_kind: str | None = None,
    opener: object = urllib.request.urlopen,
    timeout_s: float = 30.0,
    poll_interval_s: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
) -> Mapping[str, object]:
    """Await one run-scoped command and write a fail-closed terminal receipt."""
    state = _active_command_state(state_file, result_file)
    with _ResultReservation(result_file) as result_path:
        return _await_run_command_reserved(
            state,
            task_id,
            expected_output,
            result_path,
            qualification_kind=qualification_kind,
            opener=opener,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            sleeper=sleeper,
        )


def command_run(
    state_file: Path,
    agent_id: str,
    body: str,
    expected_output: str,
    result_file: Path,
    *,
    wait: bool,
    wire_copies: int = 1,
    opener: object = urllib.request.urlopen,
    wire_publisher: Callable[[str, bytes, object], None] | None = None,
    timeout_s: float = 30.0,
    poll_interval_s: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
) -> Mapping[str, object]:
    """Submit a run-scoped command through HTTP or two identical raw wires."""
    state = _active_command_state(state_file, result_file)
    if not agent_id or not body or not expected_output or wire_copies not in {1, 2}:
        raise LabConfigError("command inputs are invalid")
    with _ResultReservation(result_file) as result_path:
        reservation_id, _ = _reservation_for_agent(state, agent_id, opener=opener)
        if wire_copies == 1:
            accepted = _request_json(
                f"{state.config.agg_url}/api/command/{quote(agent_id, safe='')}",
                opener=opener,
                expected_status=202,
                method="POST",
                body={"body": body},
            )
            task_id, accepted_at = _validated_acceptance(accepted, agent_id)
        else:
            task_id, accepted_at, payload = _canonical_command(agent_id, body)
            subject = f"agents.{agent_id}.inbox"
            if wire_publisher is None:
                _publish_duplicate_wires(state, subject, payload, None)
            else:
                wire_publisher(subject, payload, None)
                wire_publisher(subject, payload, None)
        _accepted_observation(
            state,
            agent_id=agent_id,
            reservation_id=reservation_id,
            task_id=task_id,
            accepted_at=accepted_at,
            expected_output=expected_output,
            wire_copies=wire_copies,
            request_body_sha256=hashlib.sha256(
                json.dumps(
                    body, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode()
            ).hexdigest(),
        )
        _wire_observations(
            state,
            agent_id=agent_id,
            reservation_id=reservation_id,
            task_id=task_id,
            wire_copies=wire_copies,
        )
        accepted_result = {
            "run_id": state.config.run_id,
            "agent_id": agent_id,
            "reservation_id": reservation_id,
            "task_id": task_id,
            "wire_copies": wire_copies,
            "http_status": 202 if wire_copies == 1 else None,
            "accepted_at": accepted_at,
            "terminal_at": None,
            "expected_output": expected_output,
            "terminal_output": None,
            "terminal_count": 0,
            "conflicting_terminal": False,
            "qualification_kind": "pending",
            "status": "accepted",
        }
        if not wait:
            write_private_json(result_path, accepted_result)
            return accepted_result
        return _await_run_command_reserved(
            state,
            task_id,
            expected_output,
            result_path,
            opener=opener,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            sleeper=sleeper,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--run-id", required=True)
    start.add_argument("--host-id", required=True)
    start.add_argument(
        "--lab-variant",
        choices=("lifecycle", "operator-smoke", "operator-evidence"),
        required=True,
    )
    start.add_argument("--bind-host", default="127.0.0.1")
    start.add_argument("--advertise-host", default="127.0.0.1")
    start.add_argument("--http-port", type=int)
    start.add_argument("--nats-port", type=int)
    start.add_argument("--monitor-port", type=int)
    start.add_argument("--trusted-network-confirm", action="store_true")
    start.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    status = commands.add_parser("status")
    status_state = status.add_mutually_exclusive_group(required=True)
    status_state.add_argument("--run-id")
    status_state.add_argument("--state-file", type=Path)
    status.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    status.add_argument("--json", action="store_true")
    stop = commands.add_parser("stop")
    stop_state = stop.add_mutually_exclusive_group(required=True)
    stop_state.add_argument("--run-id")
    stop_state.add_argument("--state-file", type=Path)
    stop.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    export = commands.add_parser("export-image")
    export_state = export.add_mutually_exclusive_group(required=True)
    export_state.add_argument("--run-id")
    export_state.add_argument("--state-file", type=Path)
    export.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--result-file", type=Path, required=True)
    command = commands.add_parser("command")
    command_state = command.add_mutually_exclusive_group(required=True)
    command_state.add_argument("--run-id")
    command_state.add_argument("--state-file", type=Path)
    command.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    command.add_argument("--agent-id", required=True)
    command.add_argument("--body", required=True)
    command.add_argument("--expected-output", required=True)
    command_wait = command.add_mutually_exclusive_group(required=True)
    command_wait.add_argument("--wait", action="store_true")
    command_wait.add_argument("--no-wait", action="store_true")
    command.add_argument("--wire-copies", type=int, choices=(1, 2), default=1)
    command.add_argument("--result-file", type=Path, required=True)
    await_parser = commands.add_parser("await")
    await_state = await_parser.add_mutually_exclusive_group(required=True)
    await_state.add_argument("--run-id")
    await_state.add_argument("--state-file", type=Path)
    await_parser.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    await_parser.add_argument("--task-id", required=True)
    await_parser.add_argument("--expected-output", required=True)
    await_parser.add_argument("--qualification-kind", choices=("queued-reconnect",))
    await_parser.add_argument("--result-file", type=Path, required=True)
    qualify = commands.add_parser("qualify")
    qualify_state = qualify.add_mutually_exclusive_group(required=True)
    qualify_state.add_argument("--run-id")
    qualify_state.add_argument("--state-file", type=Path)
    qualify.add_argument(
        "--state-root", type=Path, default=_repo_root() / "tmp/research/lab"
    )
    arguments = parser.parse_args()
    if arguments.command == "start":
        config = start_controller(arguments)
        print(config.state_dir / "controller.json")
        print(config.credential_file)
        print(config.app_url)
        print("controller: READY")
        return 0
    state_file = arguments.state_file or (
        arguments.state_root.resolve()
        / validate_run_id(arguments.run_id)
        / "controller-state.json"
    )
    if arguments.command == "stop":
        print(json.dumps(stop_controller(state_file), sort_keys=True))
        return 0
    if arguments.command == "export-image":
        print(
            json.dumps(
                export_fixture_image(
                    state_file, arguments.output, arguments.result_file
                ),
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "command":
        print(
            json.dumps(
                command_run(
                    state_file,
                    arguments.agent_id,
                    arguments.body,
                    arguments.expected_output,
                    arguments.result_file,
                    wait=arguments.wait,
                    wire_copies=arguments.wire_copies,
                ),
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "await":
        print(
            json.dumps(
                await_run_command(
                    state_file,
                    arguments.task_id,
                    arguments.expected_output,
                    arguments.result_file,
                    qualification_kind=arguments.qualification_kind,
                ),
                sort_keys=True,
            )
        )
        return 0
    if arguments.command == "qualify":
        try:
            qualification = qualify_controller(state_file)
        except LabConfigError:
            print("lab qualification: PRELIMINARY")
            return 2
        print(
            "lab qualification: "
            f"{'REMOTE QUALIFIED' if qualification.remote_qualified else 'PRELIMINARY'}"
        )
        return 0 if qualification.remote_qualified else 1
    state = load_controller_state(state_file)
    if arguments.json:
        print(json.dumps(_state_dict(state), sort_keys=True))
    else:
        print(f"controller: {state.phase}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ControllerOwnershipState",
    "await_command",
    "await_run_command",
    "command_run",
    "export_fixture_image",
    "load_controller_state",
    "qualify_controller",
    "start_controller",
    "stop_controller",
    "run_controller_preflight",
    "submit_command",
    "write_controller_state",
]
