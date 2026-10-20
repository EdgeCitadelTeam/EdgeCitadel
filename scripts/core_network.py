"""Owned Core runtime identity and complete host publication policy.

No service is started by importing this module. CLI orchestration owns prompts,
credentials, readiness, and the transaction which applies these primitives.
"""

from __future__ import annotations

import fcntl
import hashlib
import ipaddress
import json
import os
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


class NetworkError(RuntimeError):
    """Invalid configuration or an ownership conflict; no implied retry."""


class RuntimeUnavailable(NetworkError):
    """A runtime dependency failed during a valid operation."""


DESCRIPTOR = "core-runtime.json"
OWNER_LABEL = "io.edgecitadel.core.owner"
FORMAT_VERSION = 1


def atomic_write(path: Path, content: str) -> None:
    """Private, unique staging files also make interrupted/concurrent writes safe."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path: Path, value: dict[str, Any]) -> None:
    atomic_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


@contextmanager
def lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise NetworkError(
                "Core runtime is busy; retry after the other operation completes"
            ) from error
        yield
    finally:
        os.close(fd)


def read_descriptor(runtime: Path, state: Path | None = None) -> dict[str, Any] | None:
    path = runtime / DESCRIPTOR
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise NetworkError(
            "Core runtime descriptor is unreadable; inspect it before retrying"
        ) from error
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != FORMAT_VERSION
    ):
        raise NetworkError("Core runtime descriptor format is unsupported")
    if (
        not isinstance(value.get("owner"), str)
        or not Path(value["owner"]).is_absolute()
        or value.get("phase")
        not in {"configured", "prepared", "applying", "ready", "failed", "stopped"}
        or not isinstance(value.get("project"), str)
        or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", value["project"])
        or type(value.get("generation")) is not int
        or value["generation"] < 1
        or value.get("runtime") != str(runtime.resolve())
        or not isinstance(value.get("compose_file"), str)
        or not Path(value["compose_file"]).is_absolute()
    ):
        raise NetworkError("Core runtime descriptor is invalid")
    applied = value.get("applied_generation")
    if (
        "agentd_restart_pending" in value
        and type(value["agentd_restart_pending"]) is not bool
    ):
        raise NetworkError("Core pending Agent service reconciliation is invalid")
    if applied is not None and (
        type(applied) is not int or not 1 <= applied <= value["generation"]
    ):
        raise NetworkError("Core applied generation is invalid")
    identity = value.get("docker")
    if identity is not None and (
        not isinstance(identity, dict)
        or not all(
            isinstance(identity.get(key), str) and identity[key]
            for key in ("context", "endpoint", "daemon_id")
        )
    ):
        raise NetworkError("Core saved Docker identity is invalid")
    if state is not None and value["owner"] != str((state / "node.json").resolve()):
        raise NetworkError("This Core runtime belongs to a different --state-dir")
    return value


def validate_policy(value: object) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != 1
    ):
        raise NetworkError("Core network policy format is unsupported")
    if value.get("mode") not in {"local", "tailscale", "custom"}:
        raise NetworkError("Core network mode is invalid")
    if type(value.get("generation")) is not int or value["generation"] < 1:
        raise NetworkError("Core network generation is invalid")
    if type(value.get("mqtt")) is not bool:
        raise NetworkError("Core MQTT policy is invalid")
    bind = value.get("bind_address")
    if value["mode"] == "local":
        if bind is not None:
            raise NetworkError("Local Core policy cannot contain a remote bind address")
    else:
        if not isinstance(bind, str) or "%" in bind:
            raise NetworkError("Core bind address must be an unscoped assigned IP")
        try:
            address = ipaddress.ip_address(bind)
        except ValueError as error:
            raise NetworkError("Core bind address must be an assigned IP") from error
        effective = getattr(address, "ipv4_mapped", None) or address
        if (
            effective.is_unspecified
            or effective.is_multicast
            or effective.is_loopback
            or effective.is_link_local
        ):
            raise NetworkError(
                "Core remote bind address must be a usable non-loopback IP"
            )
        if value["mode"] == "tailscale" and (
            address.version != 4 or address not in ipaddress.ip_network("100.64.0.0/10")
        ):
            raise NetworkError("Guided Tailscale requires this host's Tailscale IPv4")
    return dict(value)


def assigned_addresses() -> set[str]:
    """Read interface addresses without inferring any public/NAT address."""
    command = (
        ["ifconfig", "-a"]
        if sys.platform == "darwin"
        else ["ip", "-j", "address", "show"]
    )
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NetworkError("Local interface inspection failed or timed out") from error
    if result.returncode:
        raise NetworkError("Local interface inspection failed")
    try:
        if sys.platform == "darwin":
            values = re.findall(r"^\s+inet6?\s+(\S+)", result.stdout, re.MULTILINE)
        else:
            interfaces = json.loads(result.stdout)
            values = [
                item["local"]
                for interface in interfaces
                for item in interface.get("addr_info", [])
                if item.get("family") in {"inet", "inet6"}
            ]
        return {str(ipaddress.ip_address(value.split("%", 1)[0])) for value in values}
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise NetworkError(
            "Local interface inspection returned malformed addresses"
        ) from error


def resolve_addresses(hostname: str) -> set[str]:
    """DNS in a terminable helper: getaddrinfo itself has no timeout argument."""
    try:
        return {str(ipaddress.ip_address(hostname))}
    except ValueError:
        pass
    code = (
        "import json,socket,sys; "
        "print(json.dumps(sorted({r[4][0] for r in socket.getaddrinfo(sys.argv[1],None,type=socket.SOCK_STREAM)})))"
    )
    try:
        result = subprocess.run(
            [sys.executable, "-c", code, hostname],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NetworkError(
            "DNS lookup timed out or could not run; verify the hostname and retry"
        ) from error
    if result.returncode:
        return set()
    try:
        values = json.loads(result.stdout)
        if not isinstance(values, list) or not all(
            isinstance(value, str) for value in values
        ):
            raise ValueError("addresses must be strings")
        return {str(ipaddress.ip_address(value)) for value in values}
    except (ValueError, TypeError) as error:
        raise NetworkError("DNS lookup returned malformed addresses") from error


def publications(policy: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    policy = validate_policy(policy)
    addresses = ["127.0.0.1"]
    if policy["bind_address"] is not None:
        addresses.append(policy["bind_address"])
    result: dict[str, list[dict[str, Any]]] = {"nats": [], "nginx": []}
    for service, ports in (
        ("nats", [4222, 7422, 8222] + ([1883] if policy["mqtt"] else [])),
        ("nginx", [80]),
    ):
        for port in ports:
            for address in ["127.0.0.1"] if port == 8222 else addresses:
                result[service].append(
                    {
                        "host_ip": address,
                        "published": str(port),
                        "target": port,
                        "protocol": "tcp",
                    }
                )
    return result


def render_override(runtime: Path, owner: str, policy: dict[str, Any]) -> str:
    """Use !override for whole port lists; Compose's ordinary merge can widen access."""
    exposed = publications(policy)
    lines = ["services:"]
    for service in ("nats", "aggregator", "dashboard", "nginx"):
        lines.extend(
            [
                f"  {service}:",
                "    labels:",
                f"      {OWNER_LABEL}: {json.dumps(owner)}",
            ]
        )
        ports = exposed.get(service, [])
        if ports:
            lines.append("    ports: !override")
            lines.extend(f"      - {json.dumps(port)}" for port in ports)
        else:
            lines.append("    ports: !override []")
        mounts = []
        if service == "nats":
            mounts = [
                (runtime / "nats/nats.conf", "/etc/nats/nats.conf", True),
                (runtime / "nats/data", "/data", False),
            ]
        elif service == "aggregator":
            mounts = [(runtime / "data", "/data", False)]
        if mounts:
            lines.append("    volumes:")
            for source, target, readonly in mounts:
                lines.append(
                    "      - "
                    + json.dumps(
                        {
                            "type": "bind",
                            "source": str(source.resolve()),
                            "target": target,
                            "read_only": readonly,
                        }
                    )
                )
    return "\n".join(lines) + "\n"


def _read_command(command: list[str]) -> str:
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RuntimeUnavailable(
            "Docker inspection failed or timed out; check the local daemon"
        ) from error
    if result.returncode:
        raise RuntimeUnavailable(
            "Docker inspection failed; check the local daemon and Compose installation"
        )
    return result.stdout


def _json_command(command: list[str]) -> Any:
    try:
        return json.loads(_read_command(command))
    except ValueError as error:
        raise RuntimeUnavailable(
            "Docker returned malformed inspection output"
        ) from error


def _version(value: str) -> tuple[int, int, int]:
    match = re.match(r"v?(\d+)\.(\d+)\.(\d+)", value)
    if not match:
        raise NetworkError("Docker/Compose version could not be qualified")
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def docker_identity() -> dict[str, str]:
    context = (
        os.environ.get("DOCKER_CONTEXT")
        or _read_command(["docker", "context", "show"]).strip()
    )
    contexts = _json_command(["docker", "context", "inspect", context])
    try:
        endpoint = contexts[0]["Endpoints"]["docker"]["Host"]
    except (KeyError, IndexError, TypeError) as error:
        raise NetworkError("Docker context has no verifiable local endpoint") from error
    if not isinstance(endpoint, str) or not endpoint.startswith(
        ("unix://", "npipe://")
    ):
        raise NetworkError(
            "Guided setup requires a local Docker Engine/Desktop context; run setup on the target host"
        )
    if os.environ.get("DOCKER_HOST") not in {None, "", endpoint}:
        raise NetworkError(
            "DOCKER_HOST conflicts with the selected local Docker context"
        )
    info = _json_command(
        ["docker", "--context", context, "info", "--format", "{{json .}}"]
    )
    if (
        not isinstance(info, dict)
        or not isinstance(info.get("ID"), str)
        or not info["ID"]
    ):
        raise NetworkError("Docker daemon identity could not be established")
    if _version(str(info.get("ServerVersion", ""))) < (28, 0, 0):
        raise NetworkError("Managed Core isolation requires Docker Engine 28 or newer")
    compose_version = _read_command(
        ["docker", "--context", context, "compose", "version", "--short"]
    ).strip()
    if _version(compose_version) < (2, 24, 4):
        raise NetworkError("Managed Core ports require Docker Compose 2.24.4 or newer")
    return {
        "context": context,
        "endpoint": endpoint,
        "daemon_id": info["ID"],
        "engine_version": info["ServerVersion"],
        "compose_version": compose_version,
    }


def assert_identity(saved: object, current: dict[str, str]) -> None:
    if saved is None:
        return
    if not isinstance(saved, dict) or any(
        saved.get(key) != current[key] for key in ("context", "endpoint", "daemon_id")
    ):
        raise NetworkError(
            "Docker context or daemon changed; return to the recorded Core runtime before operating it"
        )


def source_project(identity: dict[str, str], compose_file: Path, fallback: str) -> str:
    """Discover source checkout identity; ownership/mount checks still precede apply."""
    context = identity["context"]
    ids = _read_command(
        [
            "docker",
            "--context",
            context,
            "ps",
            "-aq",
            "--filter",
            "label=com.docker.compose.project.config_files",
        ]
    ).split()
    projects: set[str] = set()
    if ids:
        containers = _json_command(["docker", "--context", context, "inspect", *ids])
        if not isinstance(containers, list):
            raise NetworkError("Source Core project inspection is invalid")
        for container in containers:
            labels = container.get("Config", {}).get("Labels", {}) or {}
            files = labels.get("com.docker.compose.project.config_files", "").split(",")
            if str(compose_file.resolve()) in files:
                project = labels.get("com.docker.compose.project")
                if not isinstance(project, str) or not re.fullmatch(
                    r"[a-z0-9][a-z0-9_-]*", project
                ):
                    raise NetworkError(
                        "Source Core has an invalid Compose project label"
                    )
                projects.add(project)
    if len(projects) > 1:
        raise NetworkError(
            "Multiple Compose projects reference this source checkout; resolve ownership before managed setup"
        )
    requested = os.environ.get("COMPOSE_PROJECT_NAME")
    if projects:
        project = next(iter(projects))
        if requested and requested != project:
            raise NetworkError(
                "COMPOSE_PROJECT_NAME conflicts with the existing source Core project"
            )
        return project
    project = requested or fallback
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
        raise NetworkError("Source Core Compose project name is invalid")
    return project


def compose_command(
    runtime: Path,
    descriptor: dict[str, Any],
    *arguments: str,
    env_file: Path | None = None,
    override_file: Path | None = None,
) -> list[str]:
    identity = descriptor.get("docker")
    if not isinstance(identity, dict) or not isinstance(identity.get("context"), str):
        raise NetworkError(
            "Core Docker identity is unverified; activate the configured Core first"
        )
    if os.environ.get("COMPOSE_FILE") or os.environ.get("COMPOSE_PROFILES"):
        raise NetworkError(
            "Ambient COMPOSE_FILE/COMPOSE_PROFILES cannot override managed Core policy"
        )
    if os.environ.get("COMPOSE_PROJECT_NAME") not in {None, "", descriptor["project"]}:
        raise NetworkError("COMPOSE_PROJECT_NAME conflicts with the owned Core project")
    base = descriptor.get("compose_file")
    if not isinstance(base, str) or not Path(base).is_absolute():
        raise NetworkError("Core Compose source is missing or invalid")
    return [
        "docker",
        "--context",
        identity["context"],
        "compose",
        "--project-name",
        descriptor["project"],
        "--env-file",
        str(env_file or runtime / ".env"),
        "-f",
        base,
        "-f",
        str(override_file or runtime / "docker-compose.managed.yml"),
        *arguments,
    ]


def preflight_model(
    runtime: Path,
    descriptor: dict[str, Any],
    policy: dict[str, Any],
    environment: dict[str, str],
) -> None:
    """Validate the final model using private temporary inputs before apply writes.

    Preserve interpolation inputs, but substitute inert credentials: config is
    a read-only Compose operation and needs no real NATS/admin authentication.
    """
    environment = {
        **environment,
        **{
            key: "preflight-placeholder"
            for key in (
                "NATS_TOKEN",
                "NATS_LEAF_USERNAME",
                "NATS_LEAF_PASSWORD",
                "EDGECITADEL_ADMIN_TOKEN",
            )
        },
    }
    with tempfile.TemporaryDirectory(prefix="edgecitadel-core-preflight-") as directory:
        staging = Path(directory)
        env_file = staging / ".env"
        override_file = staging / "managed.yml"
        atomic_write(
            env_file,
            "".join(
                f"{key}={json.dumps(value)}\n" for key, value in environment.items()
            ),
        )
        atomic_write(
            override_file, render_override(runtime, descriptor["owner"], policy)
        )
        model = _json_command(
            compose_command(
                runtime,
                descriptor,
                "config",
                "--format",
                "json",
                env_file=env_file,
                override_file=override_file,
            )
        )
        verify_model(model, policy)


@contextmanager
def project_lock(identity: dict[str, str], project: str) -> Iterator[None]:
    key = hashlib.sha256(f"{identity['daemon_id']}\n{project}".encode()).hexdigest()
    # Runtime lock is always acquired first, project lock second. The lock file
    # survives unlock; deleting it would permit concurrent owners of two inodes.
    with lock(Path.home() / ".edgecitadel/locks" / f"core-{key}.lock"):
        yield


def owned_containers(
    descriptor: dict[str, Any], *, allow_legacy: bool = False
) -> list[dict[str, Any]]:
    context = descriptor["docker"]["context"]
    ids = _read_command(
        [
            "docker",
            "--context",
            context,
            "ps",
            "-aq",
            "--filter",
            f"label=com.docker.compose.project={descriptor['project']}",
        ]
    ).split()
    if not ids:
        return []
    containers = _json_command(["docker", "--context", context, "inspect", *ids])
    if not isinstance(containers, list):
        raise NetworkError("Core container inspection is invalid")
    for container in containers:
        labels = container.get("Config", {}).get("Labels", {}) or {}
        if labels.get(OWNER_LABEL) == descriptor["owner"]:
            continue
        # Legacy adoption needs exact source and mutable mount provenance, not
        # merely a familiar project name. Unknown services are never adopted.
        service = labels.get("com.docker.compose.service")
        if (
            not allow_legacy
            or labels.get(OWNER_LABEL)
            or service not in {"nats", "aggregator", "dashboard", "nginx"}
        ):
            raise NetworkError(
                "Compose project contains a container owned by another deployment"
            )
        files = labels.get("com.docker.compose.project.config_files", "").split(",")
        if descriptor["compose_file"] not in files:
            raise NetworkError(
                "Legacy Core Compose source does not match this installation"
            )
        expected = descriptor.get("runtime")
        for target, suffix in (
            ("/data", "nats/data" if service == "nats" else "data"),
        ):
            if service in {"nats", "aggregator"} and not any(
                mount.get("Type") == "bind"
                and mount.get("Destination") == target
                and mount.get("Source") == str(Path(expected) / suffix)
                for mount in container.get("Mounts", [])
            ):
                raise NetworkError("Legacy Core data mounts do not match this runtime")
    return containers


def verify_model(model: dict[str, Any], policy: dict[str, Any]) -> None:
    expected = publications(policy)
    services = model.get("services", {})
    if set(services) != {"nats", "aggregator", "dashboard", "nginx"}:
        raise NetworkError(
            "Managed Core Compose services differ from the qualified topology"
        )
    for name, service in services.items():
        if service.get("network_mode") or service.get("privileged"):
            raise NetworkError(
                "Managed Core cannot use host networking or privileged services"
            )
        if set(service.get("networks", {"default": None})) != {"default"}:
            raise NetworkError("Managed Core requires only its default NAT bridge")
        actual = {_port_tuple(port) for port in service.get("ports", [])}
        allowed = {_port_tuple(port) for port in expected.get(name, [])}
        if actual != allowed:
            raise NetworkError(
                f"Effective {name} publications do not match the selected access policy"
            )
    networks = model.get("networks", {})
    if set(networks) != {"default"}:
        raise NetworkError("Managed Core requires exactly one default NAT bridge")
    default = networks["default"]
    if (
        default.get("external")
        or default.get("driver", "bridge") != "bridge"
        or default.get("driver_opts")
        or default.get("enable_ipv6")
    ):
        raise NetworkError("Managed Core bridge has unqualified routing options")


def _port_tuple(port: dict[str, Any]) -> tuple[str, str, int, str]:
    try:
        return (
            str(port.get("host_ip", "0.0.0.0")),
            str(port["published"]),
            int(port["target"]),
            str(port.get("protocol", "tcp")),
        )
    except (KeyError, ValueError, TypeError) as error:
        raise NetworkError("Compose returned an invalid publication") from error


def verify_bindings(
    containers: list[dict[str, Any]], policy: dict[str, Any], *, context: str
) -> None:
    expected = publications(policy)
    observed: set[str] = set()
    network_ids: set[str] = set()
    for container in containers:
        name = container["Config"]["Labels"].get("com.docker.compose.service")
        if name in observed or name not in {"nats", "aggregator", "dashboard", "nginx"}:
            raise NetworkError("Core has duplicate or unexpected services")
        observed.add(name)
        if not container.get("State", {}).get("Running"):
            raise RuntimeUnavailable(f"Core service {name} is not running")
        network_name = (
            container["Config"]["Labels"].get("com.docker.compose.project", "")
            + "_default"
        )
        if container.get("HostConfig", {}).get(
            "NetworkMode"
        ) != network_name or container.get("HostConfig", {}).get("Privileged"):
            raise NetworkError("Core service uses an unqualified network mode")
        attachments = container.get("NetworkSettings", {}).get("Networks", {})
        if set(attachments) != {network_name}:
            raise NetworkError("Core service has an unexpected network attachment")
        network_id = attachments[network_name].get("NetworkID")
        if not isinstance(network_id, str) or not network_id:
            raise NetworkError("Core bridge identity is unverified")
        network_ids.add(network_id)
        actual = set()
        for target, bindings in (
            container.get("NetworkSettings", {}).get("Ports", {}).items()
        ):
            number, protocol = target.split("/")
            for binding in bindings or []:
                actual.add(
                    (binding["HostIp"], binding["HostPort"], int(number), protocol)
                )
        if actual != {_port_tuple(port) for port in expected.get(name, [])}:
            raise NetworkError(
                f"Running {name} publications do not match the selected access policy"
            )
    if observed != {"nats", "aggregator", "dashboard", "nginx"}:
        raise RuntimeUnavailable("The owned Core stack is incomplete")
    if len(network_ids) != 1:
        raise NetworkError("Core services do not share exactly one owned bridge")
    networks = _json_command(
        ["docker", "--context", context, "network", "inspect", *sorted(network_ids)]
    )
    if not isinstance(networks, list) or len(networks) != 1:
        raise NetworkError("Core bridge inspection is invalid")
    bridge = networks[0]
    allowed_options = {
        "com.docker.network.bridge.gateway_mode_ipv4": "nat",
        "com.docker.network.bridge.gateway_mode_ipv6": "nat",
        "com.docker.network.bridge.enable_ip_masquerade": "true",
        "com.docker.network.bridge.enable_icc": "true",
        "com.docker.network.enable_ipv4": "true",
    }
    options = bridge.get("Options") or {}
    if (
        bridge.get("Driver") != "bridge"
        or bridge.get("EnableIPv6")
        or bridge.get("Internal")
        or not isinstance(options, dict)
        or any(allowed_options.get(key) != value for key, value in options.items())
    ):
        raise NetworkError("Running Core bridge has unqualified routing options")
