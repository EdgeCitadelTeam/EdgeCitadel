"""Pinned deterministic fixture configuration for lab nodes."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import platform
import shutil
import socket
import subprocess
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.lab_config import (
    ControllerConfig,
    LabConfigError,
    credential_sha256,
    credential_token,
    validate_agent_id,
    validate_declared_host_id,
    validate_run_id,
    write_private_json,
)
from scripts.research.lab_runtime import capture_clean_source_provenance

_CRASH_POINTS = {
    "after-receive-before-handler",
    "after-side-effect-before-ledger-prepare",
    "after-ledger-prepare-before-result-publish",
    "after-result-publish-before-publish-mark",
    "after-publish-mark-before-inbound-commit",
    "during-handler-exception-conversion",
}
_SAFE_CONFLICT_DETAILS = {
    "agent_id has an active reservation",
    "reservation owner does not match",
}


@dataclass(frozen=True)
class NodeState:
    schema_version: str
    phase: Literal["starting", "active", "retained"]
    run_id: str
    agent_id: str
    qualified_agent_id: str
    reservation_id: str
    declared_host_id: str
    machine_id_sha256: str
    container_id: str
    container_name: str
    fixture_image_id: str
    config_path: Path
    state_dir: Path
    log_path: Path
    reservation_state: Literal["active", "retained"]
    started_at: str


def _machine_id_sha256() -> str:
    path = Path("/etc/machine-id")
    if not path.is_file():
        raise LabConfigError("lab node requires Linux /etc/machine-id")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _os_release() -> str:
    path = Path("/etc/os-release")
    try:
        values = {
            key: value.strip().strip('"')
            for line in path.read_text().splitlines()
            if "=" in line
            for key, value in (line.split("=", 1),)
        }
    except OSError as error:
        raise LabConfigError("node operating system release is unavailable") from error
    value = values.get("PRETTY_NAME")
    if not value:
        raise LabConfigError("node operating system release is unavailable")
    return value


def _network_path(controller: ControllerConfig) -> dict[str, str]:
    try:
        destination = str(ipaddress.ip_address(controller.advertised_ip))
    except ValueError as error:
        raise LabConfigError("controller advertised IP is invalid") from error
    completed = subprocess.run(
        ["ip", "route", "get", destination], check=True, capture_output=True, text=True,
    )
    route = completed.stdout.strip()
    fields = route.split()
    try:
        source_ip = fields[fields.index("src") + 1]
        interface = fields[fields.index("dev") + 1]
        ipaddress.ip_address(source_ip)
    except (ValueError, IndexError) as error:
        raise LabConfigError("node route discovery failed") from error
    if not interface:
        raise LabConfigError("node route discovery failed")
    return {
        "source_ip": source_ip,
        "destination_ip": destination,
        "interface": interface,
        "route_output_sha256": hashlib.sha256(route.encode()).hexdigest(),
        "controller_dns_name": controller.advertised_host,
    }


def _absolute(value: object, label: str) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise LabConfigError(f"node {label} is invalid")
    return Path(value)


def load_controller_config(path: Path) -> ControllerConfig:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("controller config is unavailable") from error
    if not isinstance(value, dict):
        raise LabConfigError("controller config is invalid")
    values = dict(value)
    try:
        for name in ("credential_file", "state_dir", "evidence_dir"):
            values[name] = _absolute(values[name], name)
        config = ControllerConfig(**values)
        validate_run_id(config.run_id)
        validate_declared_host_id(config.controller_host_id)
        if len(config.source_commit) != 40 or any(character not in "0123456789abcdef" for character in config.source_commit):
            raise LabConfigError("controller config source commit is invalid")
        if len(config.source_snapshot_sha256) != 64 or any(character not in "0123456789abcdef" for character in config.source_snapshot_sha256):
            raise LabConfigError("controller config source snapshot is invalid")
        if len(config.credential_sha256) != 64 or any(character not in "0123456789abcdef" for character in config.credential_sha256):
            raise LabConfigError("controller config credential hash is invalid")
        if not config.fixture_image_id.startswith("sha256:") or len(config.fixture_image_id) != 71:
            raise LabConfigError("controller config fixture image is invalid")
        return config
    except (KeyError, TypeError, LabConfigError) as error:
        raise LabConfigError("controller config is invalid") from error


def _node_state_path(controller: ControllerConfig, state_root: Path, agent_id: str) -> Path:
    if not state_root.is_absolute():
        raise LabConfigError("node state root must be absolute")
    qualified = f"{controller.run_id}--{validate_agent_id(agent_id)}"
    return state_root / controller.run_id / qualified / "node-state.json"


def _state_dict(state: NodeState) -> dict[str, object]:
    value = asdict(state)
    for name in ("config_path", "state_dir", "log_path"):
        value[name] = str(value[name])
    return value


def write_node_state(path: Path, state: NodeState) -> None:
    if not path.is_absolute():
        raise LabConfigError("node state path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, json.dumps(_state_dict(state), sort_keys=True, separators=(",", ":")).encode() + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def load_node_state(path: Path) -> NodeState:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise LabConfigError("node state is unavailable") from error
    if not isinstance(value, dict) or value.get("schema_version") != "lab-node-state.v1":
        raise LabConfigError("node state is invalid")
    try:
        values = dict(value)
        for name in ("config_path", "state_dir", "log_path"):
            values[name] = _absolute(values[name], name)
        state = NodeState(**values)
        validate_run_id(state.run_id)
        validate_agent_id(state.agent_id)
        validate_declared_host_id(state.declared_host_id)
        if state.phase not in {"starting", "active", "retained"}:
            raise LabConfigError("node state phase is invalid")
        return state
    except (KeyError, TypeError, LabConfigError) as error:
        raise LabConfigError("node state is invalid") from error


def _inventory_url(controller: ControllerConfig, suffix: str) -> str:
    base = controller.inventory_url.removesuffix("/status")
    if not base.endswith("/api/lab"):
        raise LabConfigError("controller inventory URL is invalid")
    return f"{base}{suffix}"


def _inventory_request(
    controller: ControllerConfig, credential_file: Path, method: str, suffix: str, body: dict[str, object]
) -> dict[str, object] | None:
    token = credential_token(credential_file)
    request = urllib.request.Request(
        _inventory_url(controller, suffix), method=method,
        data=json.dumps(body, sort_keys=True, separators=(",", ":")).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
    except urllib.error.HTTPError as error:
        if error.code == 409:
            try:
                conflict = json.loads(error.read())
            except (OSError, json.JSONDecodeError):
                conflict = None
            detail = conflict.get("detail") if isinstance(conflict, dict) else None
            if detail in _SAFE_CONFLICT_DETAILS:
                raise LabConfigError(str(detail)) from None
            raise LabConfigError("lab reservation is unavailable") from None
        raise LabConfigError("lab inventory request failed") from None
    except (OSError, urllib.error.URLError) as error:
        raise LabConfigError("lab inventory request failed") from error
    if not payload:
        return None
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as error:
        raise LabConfigError("lab inventory response is invalid") from error
    if not isinstance(decoded, dict):
        raise LabConfigError("lab inventory response is invalid")
    return decoded


def _reservation_body(controller: ControllerConfig, agent_id: str, reservation_id: str, host_id: str) -> dict[str, object]:
    return {
        "agent_id": agent_id,
        "qualified_agent_id": f"{controller.run_id}--{agent_id}",
        "reservation_id": reservation_id,
        "declared_host_id": host_id,
    }


def _validate_node_target(controller: ControllerConfig, credential_file: Path) -> str:
    if credential_sha256(credential_file) != controller.credential_sha256:
        raise LabConfigError("credential does not match controller config")
    machine_id = _machine_id_sha256()
    parsed = urlparse(controller.nats_url)
    if parsed.scheme != "nats" or not parsed.hostname:
        raise LabConfigError("controller NATS URL is invalid")
    if parsed.hostname in {"127.0.0.1", "::1", "localhost"} and machine_id != controller.controller_machine_id_sha256:
        raise LabConfigError("remote node cannot use a loopback controller URL")
    return machine_id


def doctor_node(args: argparse.Namespace) -> dict[str, object]:
    """Validate a node target without reserving an identity or starting a fixture."""
    controller = load_controller_config(Path(args.controller_config).resolve())
    credential_file = Path(args.credential_file).resolve()
    machine_id = _validate_node_target(controller, credential_file)
    if shutil.which("docker") is None:
        raise LabConfigError("docker is unavailable")
    subprocess.run(
        ["docker", "image", "inspect", controller.fixture_image_id],
        check=True,
        capture_output=True,
        text=True,
    )
    parsed = urlparse(controller.nats_url)
    if parsed.hostname is None:
        raise LabConfigError("controller NATS URL is invalid")
    report: dict[str, object] = {
        "schema_version": "lab-node-doctor.v1",
        "run_id": controller.run_id,
        "controller_host_id": controller.controller_host_id,
        "machine_id_sha256": machine_id,
        "fixture_image_id": controller.fixture_image_id,
        "controller_nats_host": parsed.hostname,
    }
    if not getattr(args, "publish", False):
        return report
    agent_id = validate_agent_id(str(args.agent_id))
    host_id = validate_declared_host_id(str(args.host_id))
    state_file = _node_state_path(controller, Path(args.state_root).resolve(), agent_id)
    state = load_node_state(state_file)
    if (
        state.phase not in {"active", "retained"}
        or state.declared_host_id != host_id
        or state.machine_id_sha256 != machine_id
        or state.fixture_image_id != controller.fixture_image_id
    ):
        raise LabConfigError("node state does not match publish target")
    source = capture_clean_source_provenance(_repo_root())
    if source.commit != controller.source_commit or source.source_snapshot_sha256 != controller.source_snapshot_sha256:
        raise LabConfigError("node source does not match controller config")
    published = {
        **_reservation_body(controller, state.agent_id, state.reservation_id, state.declared_host_id),
        "machine_id_sha256": machine_id,
        "hostname": socket.gethostname(),
        "os_release": _os_release(),
        "architecture": platform.machine(),
        "launcher_source_commit": source.commit,
        "source_snapshot_sha256": source.source_snapshot_sha256,
        "network_path": _network_path(controller),
        "preflight_valid": True,
        "lifecycle_state": state.phase,
        "cleanup": None,
        "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    response = _inventory_request(controller, credential_file, "POST", "/node-reports", published)
    if response is None or any(response.get(name) != published[name] for name in ("agent_id", "reservation_id", "declared_host_id")):
        raise LabConfigError("lab node report binding was not accepted")
    return response


def build_fixture_config(
    *, run_id: str, agent_id: str, behavior: str, delay_ms: int, crash_point: str | None
) -> NativeControlConfig:
    if crash_point is not None and crash_point not in _CRASH_POINTS:
        raise LabConfigError("invalid fixture crash point")
    if not isinstance(behavior, str) or not behavior or type(delay_ms) is not int or delay_ms < 0:
        raise LabConfigError("invalid fixture behavior")
    return NativeControlConfig(
        run_id=validate_run_id(run_id), agent_id=validate_agent_id(agent_id), mode="edgecitadel",
        behavior=behavior, delay_ms=delay_ms, crash_point=crash_point, heartbeat_interval_ms=1000,
        outcome_db="/run/state/outcomes.sqlite", side_effect_db="/run/state/side-effects.sqlite",
    )


def build_fixture_create_argv(
    *, controller: ControllerConfig, credential_file: Path, node_state_dir: Path,
    config_path: Path, container_name: str,
) -> tuple[str, ...]:
    if not controller.fixture_image_id.startswith("sha256:") or len(controller.fixture_image_id) != 71:
        raise LabConfigError("fixture image must be immutable")
    credential_token(credential_file)
    for path in (node_state_dir, config_path, credential_file):
        if not path.is_absolute():
            raise LabConfigError("node paths must be absolute")
    if not container_name.startswith("edgecitadel-node-"):
        raise LabConfigError("container name is invalid")
    qualified = f"{controller.run_id}--"
    if not container_name.removeprefix("edgecitadel-node-").startswith(qualified):
        raise LabConfigError("container name does not match controller run")
    return (
        "docker", "create", "--name", container_name, "--network", "host",
        "--label", "ai.edgecitadel.owner=research-lab-node",
        "--label", f"ai.edgecitadel.run-id={controller.run_id}",
        "--label", f"ai.edgecitadel.qualified-agent-id={container_name.removeprefix('edgecitadel-node-')}",
        "--env", f"NATS_URL={controller.nats_url}",
        "--env", "EC_CREDENTIAL_FILE=/run/secrets/transport-token",
        "--env", "EC_EVENT_LOG=/run/state/fixture.log",
        "--env", "EC_TERMINAL_RELEASE_DIR=/run/state/terminal-release",
        "--mount", f"type=bind,src={config_path},dst=/run/config/native-control.json,readonly",
        "--mount", f"type=bind,src={credential_file},dst=/run/secrets/transport-token,readonly",
        "--mount", f"type=bind,src={node_state_dir},dst=/run/state",
        "--read-only", "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m", controller.fixture_image_id,
        "python3", "-m", "scripts.research.fixtures.native_control", "--config", "/run/config/native-control.json",
    )


def _remove_fixture_container(container_name: str) -> None:
    argv = ["docker", "rm", "--force", container_name]
    completed = subprocess.run(argv, check=False, capture_output=True, text=True)
    if completed.returncode == 0:
        return
    stderr = completed.stderr.strip()
    if stderr in {
        f"Error response from daemon: No such container: {container_name}",
        f"Error: No such container: {container_name}",
    }:
        return
    raise subprocess.CalledProcessError(
        completed.returncode, argv, output=completed.stdout, stderr=completed.stderr
    )


def start_node(args: argparse.Namespace) -> NodeState:
    controller = load_controller_config(Path(args.controller_config).resolve())
    agent_id = validate_agent_id(str(args.agent_id))
    host_id = validate_declared_host_id(str(args.host_id))
    credential_file = Path(args.credential_file).resolve()
    machine_id = _validate_node_target(controller, credential_file)
    state_root = Path(args.state_root).resolve()
    state_file = _node_state_path(controller, state_root, agent_id)
    previous = load_node_state(state_file) if state_file.exists() else None
    if previous is not None and previous.phase in {"starting", "active"}:
        raise LabConfigError("node already has an active state")
    if previous is not None and (previous.declared_host_id != host_id or previous.machine_id_sha256 != machine_id):
        raise LabConfigError("retained node state owner does not match")

    qualified = f"{controller.run_id}--{agent_id}"
    node_dir = state_file.parent
    config_path = node_dir / "native-control.json"
    log_path = node_dir / "fixture.log"
    container_name = f"edgecitadel-node-{qualified}"
    reservation_id = previous.reservation_id if previous else str(uuid.uuid4())
    reservation = _reservation_body(controller, agent_id, reservation_id, host_id)
    resumed = previous is not None
    created = False
    try:
        _inventory_request(controller, credential_file, "POST", "/reservations", reservation)
        node_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(node_dir, 0o700)
        terminal_release_dir = node_dir / "terminal-release"
        terminal_release_dir.mkdir(mode=0o700, exist_ok=True)
        os.chmod(terminal_release_dir, 0o700)
        if not resumed:
            fixture = build_fixture_config(
                run_id=controller.run_id, agent_id=agent_id, behavior=args.behavior,
                delay_ms=args.delay_ms, crash_point=args.crash_point,
            )
            write_private_json(config_path, asdict(fixture))
        elif not config_path.is_file():
            raise LabConfigError("retained node config is unavailable")
        subprocess.run(["docker", "image", "inspect", controller.fixture_image_id], check=True, capture_output=True, text=True)
        argv = build_fixture_create_argv(
            controller=controller, credential_file=credential_file, node_state_dir=node_dir,
            config_path=config_path, container_name=container_name,
        )
        created_id = subprocess.run(argv, check=True, capture_output=True, text=True).stdout.strip()
        created = True
        container_id = subprocess.run(["docker", "start", created_id or container_name], check=True, capture_output=True, text=True).stdout.strip()
        if not container_id:
            raise LabConfigError("fixture container did not return an ID")
        state = NodeState(
            schema_version="lab-node-state.v1", phase="active", run_id=controller.run_id,
            agent_id=agent_id, qualified_agent_id=qualified, reservation_id=reservation_id,
            declared_host_id=host_id, machine_id_sha256=machine_id, container_id=container_id,
            container_name=container_name, fixture_image_id=controller.fixture_image_id,
            config_path=config_path, state_dir=node_dir, log_path=log_path,
            reservation_state="active", started_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        write_node_state(state_file, state)
        return state
    except BaseException:
        if created:
            subprocess.run(["docker", "rm", "--force", container_name], check=False, capture_output=True, text=True)
        try:
            if resumed:
                _inventory_request(controller, credential_file, "PATCH", f"/reservations/{agent_id}/retain", reservation)
            else:
                _inventory_request(controller, credential_file, "DELETE", f"/reservations/{agent_id}", reservation)
        except LabConfigError:
            pass
        if not resumed:
            shutil.rmtree(node_dir, ignore_errors=True)
        raise


def stop_node(args: argparse.Namespace) -> str:
    controller = load_controller_config(Path(args.controller_config).resolve())
    agent_id = validate_agent_id(str(args.agent_id))
    credential_file = Path(args.credential_file).resolve()
    _validate_node_target(controller, credential_file)
    state_file = _node_state_path(controller, Path(args.state_root).resolve(), agent_id)
    if not state_file.exists():
        return "node: already stopped"
    state = load_node_state(state_file)
    if state.run_id != controller.run_id or state.fixture_image_id != controller.fixture_image_id:
        raise LabConfigError("node state does not match controller config")
    reservation = _reservation_body(controller, state.agent_id, state.reservation_id, state.declared_host_id)
    _remove_fixture_container(state.container_name)
    if args.retain_reservation:
        _inventory_request(controller, credential_file, "PATCH", f"/reservations/{state.agent_id}/retain", reservation)
        retained = replace(state, phase="retained", container_id="", reservation_state="retained")
        write_node_state(state_file, retained)
        return "node: retained"
    _inventory_request(controller, credential_file, "DELETE", f"/reservations/{state.agent_id}", reservation)
    shutil.rmtree(state.state_dir, ignore_errors=True)
    return "node: stopped"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("start", "stop", "doctor"):
        command = commands.add_parser(name)
        command.add_argument("--controller-config", type=Path, required=True)
        command.add_argument("--credential-file", type=Path, required=True)
        command.add_argument("--agent-id", required=True)
        command.add_argument("--state-root", type=Path, default=Path("/tmp/edgecitadel-lab-node"))
    start = commands.choices["start"]
    start.add_argument("--host-id", required=True)
    start.add_argument("--behavior", default="echo")
    start.add_argument("--delay-ms", type=int, default=0)
    start.add_argument("--crash-point", choices=sorted(_CRASH_POINTS))
    stop = commands.choices["stop"]
    stop.add_argument("--retain-reservation", action="store_true")
    doctor = commands.choices["doctor"]
    doctor.add_argument("--host-id")
    doctor.add_argument("--publish", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "doctor":
        if arguments.publish and arguments.host_id is None:
            parser.error("doctor --publish requires --host-id")
        print(json.dumps(doctor_node(arguments), sort_keys=True))
        return 0
    if arguments.command == "start":
        state = start_node(arguments)
        print(state.container_name)
        print("node: READY")
        return 0
    print(stop_node(arguments))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "NodeState", "build_fixture_config", "build_fixture_create_argv", "load_controller_config",
    "doctor_node", "load_node_state", "start_node", "stop_node", "write_node_state",
]
