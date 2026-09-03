"""Unified newcomer CLI for EdgeCitadel source and packaged deployments."""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import plistlib
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import urlparse

try:
    from . import nats_leaf
    from .installation_assets import (
        AssetResolutionError,
        agent_packages_root,
        agent_platform_root,
        plugin_source,
        plugins_root,
    )
    from .plugin_installation import HOSTS, PluginResult, driver_for
except ImportError:  # Executed by the installed scripts/edgecitadel wrapper.
    import nats_leaf  # type: ignore[no-redef]
    from installation_assets import (  # type: ignore[no-redef]
        AssetResolutionError,
        agent_packages_root,
        agent_platform_root,
        plugin_source,
        plugins_root,
    )
    from plugin_installation import (  # type: ignore[no-redef]
        HOSTS,
        PluginResult,
        driver_for,
    )


VERSION = "0.1.0"
REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALL_ROOT = Path(os.environ.get("EDGECITADEL_INSTALL_ROOT", REPO_ROOT)).resolve()
INSTALL_DISTRIBUTION = os.environ.get("EDGECITADEL_DISTRIBUTION", "source")
IS_HOMEBREW = INSTALL_DISTRIBUTION == "homebrew"
IS_PIP = INSTALL_DISTRIBUTION == "pip"
CORE_RUNTIME_DIR = Path(
    os.environ.get(
        "EDGECITADEL_CORE_DIR",
        Path.home() / ".edgecitadel" / "core"
        if IS_HOMEBREW or IS_PIP
        else INSTALL_ROOT,
    )
).expanduser()
ENV_PATH = CORE_RUNTIME_DIR / ".env"
ENV_EXAMPLE_PATH = INSTALL_ROOT / ".env.example"
NODE_STATE_NAME = "node.json"
PLUGIN_STATE_NAME = "plugins.json"
MANAGED_AGENT_STATE_NAME = "managed-agents.json"
AGENTD_PROCESS_STATE_NAME = "process.json"
AGENTD_ADMIN_TOKEN_NAME = "admin.token"
AGENTD_ADMIN_OPERATIONS = frozenset(
    {
        "connector.register",
        "connector.configure",
        "connector.list",
        "connector.revoke",
        "managed.reconcile",
        "managed.list",
        "managed.connector.reissue",
    }
)
NATIVE_CONNECTOR_CAPABILITIES = (
    "edgecitadel_agents",
    "edgecitadel_delegate",
    "edgecitadel_inbox",
    "edgecitadel_task_status",
    "edgecitadel_task_update",
    "edgecitadel_trace",
    "edgecitadel_diagnose",
)
PLACEHOLDERS = {
    "NATS_TOKEN": {"", "change-me", "changeme"},
    "NATS_LEAF_USERNAME": {"", "change-me-leaf-user", "changeme"},
    "NATS_LEAF_PASSWORD": {"", "change-me-leaf-password", "changeme"},
    "EDGECITADEL_ADMIN_TOKEN": {"", "change-me-admin", "changeme"},
}


def _command_name() -> str:
    return "edgecitadel" if IS_HOMEBREW or IS_PIP else "./scripts/edgecitadel"


class UserError(RuntimeError):
    """Expected failure that should be shown without a traceback."""


class OperationalError(UserError):
    """A valid operation failed because runtime state was unavailable."""


def _asset_root(resolver: Any) -> Path:
    try:
        return resolver(INSTALL_ROOT)
    except AssetResolutionError as error:
        raise OperationalError(str(error)) from error


def _state_dir(value: str | None = None) -> Path:
    configured = value or os.environ.get("EDGECITADEL_STATE_DIR")
    return Path(configured).expanduser() if configured else Path.home() / ".edgecitadel"


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)


def _read_env(path: Path = ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value.strip().strip('"').strip("'")
    return values


def _secure_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content)
    temporary.chmod(0o600)
    temporary.replace(path)
    path.chmod(0o600)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    _secure_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        value = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise UserError(f"local state is invalid: {path}") from error
    if not isinstance(value, dict):
        raise UserError(f"local state is invalid: {path}")
    return value


def _ensure_env(path: Path = ENV_PATH) -> tuple[dict[str, str], bool]:
    if path.exists():
        lines = path.read_text().splitlines()
    else:
        if not ENV_EXAMPLE_PATH.exists():
            raise UserError(".env.example is missing; restore the source checkout")
        lines = ENV_EXAMPLE_PATH.read_text().splitlines()

    changed = not path.exists()
    seen: set[str] = set()
    output: list[str] = []
    generated = {key: f"ec_{secrets.token_hex(32)}" for key in PLACEHOLDERS}
    for line in lines:
        if "=" not in line or line.lstrip().startswith("#"):
            output.append(line)
            continue
        key, raw = line.split("=", 1)
        if key not in PLACEHOLDERS:
            output.append(line)
            continue
        seen.add(key)
        current = raw.strip().strip('"').strip("'")
        if current in PLACEHOLDERS[key]:
            output.append(f"{key}={generated[key]}")
            changed = True
        else:
            output.append(line)
    for key in PLACEHOLDERS:
        if key not in seen:
            output.append(f"{key}={generated[key]}")
            changed = True

    if changed:
        _secure_write(path, "\n".join(output) + "\n")
    else:
        path.chmod(0o600)
    return _read_env(path), changed


def _render_nats_config() -> None:
    source = INSTALL_ROOT / "nats" / "nats.conf.tpl"
    destination = CORE_RUNTIME_DIR / "nats" / "nats.conf"
    if not source.exists():
        raise UserError("NATS configuration template is missing from the installation")
    content = source.read_text()
    mqtt_enabled = os.environ.get("EC_ENABLE_MQTT", "0") == "1"
    if mqtt_enabled:
        rendered: list[str] = []
        inside = False
        for line in content.splitlines():
            if line == "# MQTT_BEGIN":
                inside = True
                continue
            if line == "# MQTT_END":
                inside = False
                continue
            rendered.append(line.removeprefix("# ") if inside else line)
        content = "\n".join(rendered) + "\n"
    _secure_write(destination, content)
    state = "ENABLED" if mqtt_enabled else "DISABLED"
    print(f"Rendered {destination} with MQTT ingress {state}.")


def _validate_core_nats_config(env: dict[str, str]) -> None:
    config = CORE_RUNTIME_DIR / "nats" / "nats.conf"
    binary = shutil.which("nats-server")
    if binary:
        result = subprocess.run(
            [binary, "-c", str(config), "-t"],
            env={**os.environ, **env},
            check=False,
            capture_output=True,
            text=True,
        )
    elif shutil.which("docker"):
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--env-file",
                str(ENV_PATH),
                "--mount",
                f"type=bind,source={config},target=/etc/nats/nats.conf,readonly",
                "nats:2.10-alpine",
                "-c",
                "/etc/nats/nats.conf",
                "-t",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    else:
        return
    if result.returncode != 0:
        raise UserError("generated Core NATS configuration did not validate")


def _write_compose_override() -> Path:
    path = CORE_RUNTIME_DIR / "docker-compose.runtime.yml"
    nats_config = json.dumps(str(CORE_RUNTIME_DIR / "nats" / "nats.conf"))
    nats_data = json.dumps(str(CORE_RUNTIME_DIR / "nats" / "data"))
    app_data = json.dumps(str(CORE_RUNTIME_DIR / "data"))
    content = (
        "services:\n"
        "  nats:\n"
        "    volumes:\n"
        "      - type: bind\n"
        f"        source: {nats_config}\n"
        "        target: /etc/nats/nats.conf\n"
        "        read_only: true\n"
        "      - type: bind\n"
        f"        source: {nats_data}\n"
        "        target: /data\n"
        "  aggregator:\n"
        "    volumes:\n"
        "      - type: bind\n"
        f"        source: {app_data}\n"
        "        target: /data\n"
    )
    _secure_write(path, content)
    return path


def _compose_command(*arguments: str) -> list[str]:
    if not (IS_HOMEBREW or IS_PIP):
        return ["docker", "compose", *arguments]
    override = _write_compose_override()
    return [
        "docker",
        "compose",
        "--project-name",
        "edgecitadel",
        "--env-file",
        str(ENV_PATH),
        "-f",
        str(INSTALL_ROOT / "docker-compose.yml"),
        "-f",
        str(override),
        *arguments,
    ]


def _run(command: Sequence[str], *, cwd: Path = REPO_ROOT) -> None:
    try:
        subprocess.run(list(command), cwd=cwd, check=True)
    except FileNotFoundError as error:
        raise UserError(f"required command is missing: {command[0]}") from error
    except subprocess.CalledProcessError as error:
        rendered = " ".join(command)
        raise UserError(f"command failed ({error.returncode}): {rendered}") from error


def _http_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 5,
) -> Any:
    encoded = None if body is None else json.dumps(body).encode()
    request_headers = {"Accept": "application/json", **(headers or {})}
    if encoded is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url, data=encoded, headers=request_headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        try:
            detail = json.loads(error.read()).get("detail", error.reason)
        except (json.JSONDecodeError, AttributeError):
            detail = error.reason
        raise UserError(f"core rejected the request: {detail}") from error
    except urllib.error.URLError as error:
        raise UserError(
            f"cannot reach EdgeCitadel core at {url}: {error.reason}"
        ) from error


def _wait_for_core(core_url: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            status = _http_json(f"{core_url}/api/system/status", timeout=2)
            if status.get("nats_connected") and status.get("jetstream_stream_ok"):
                return
            last_error = "NATS or JetStream is not ready"
        except UserError as error:
            last_error = str(error)
        time.sleep(1)
    raise UserError(
        f"core did not become ready within {timeout}s ({last_error}); "
        f"run '{_command_name()} doctor'"
    )


def _load_node(state_dir: Path) -> dict[str, Any]:
    path = state_dir / NODE_STATE_NAME
    if not path.exists():
        raise UserError(
            f"this host is not initialized; run '{_command_name()} create' "
            f"or '{_command_name()} join <invitation>'"
        )
    try:
        value = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise UserError(f"node state is invalid: {path}") from error
    if not isinstance(value, dict) or value.get("version") not in {1, 2}:
        raise UserError(f"node state is unsupported: {path}")
    if value.get("mode") not in {"core", "edge"}:
        raise UserError(f"node state is unsupported: {path}")
    normalized = dict(value)
    if normalized["mode"] == "edge":
        messaging_mode = normalized.get("messaging_mode", "single-client")
        if messaging_mode not in {"single-client", "nats_leaf"}:
            raise UserError(f"node state is unsupported: {path}")
        normalized["messaging_mode"] = messaging_mode
        if messaging_mode == "single-client":
            if not isinstance(normalized.get("nats_url"), str) or not isinstance(
                normalized.get("nats_token"), str
            ):
                raise UserError(f"node state is unsupported: {path}")
            normalized.setdefault("upstream_nats_url", normalized["nats_url"])
            normalized.setdefault("plugin_nats_url", normalized["nats_url"])
            normalized.setdefault("plugin_nats_token", normalized["nats_token"])
        else:
            required = {
                "upstream_nats_url",
                "plugin_nats_url",
                "plugin_nats_token",
                "jetstream_domain",
            }
            if not all(isinstance(normalized.get(key), str) for key in required):
                raise UserError(f"node state is unsupported: {path}")
            normalized.setdefault("nats_url", normalized["plugin_nats_url"])
            normalized.setdefault("nats_token", normalized["plugin_nats_token"])
    return normalized


def _invitation_encode(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return f"ecjoin://{encoded}"


def _invitation_decode(value: str) -> dict[str, Any]:
    if not value.startswith("ecjoin://"):
        raise UserError("invitation must start with ecjoin://")
    encoded = value.removeprefix("ecjoin://")
    try:
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        payload = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as error:
        raise UserError("invitation is malformed") from error
    required = {"version", "core_url", "nats_url", "token", "agent_id", "expires_at"}
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise UserError("invitation is incomplete")
    if payload["version"] != 1:
        raise UserError("invitation version is unsupported")
    try:
        expires_at = float(payload["expires_at"])
    except (TypeError, ValueError) as error:
        raise UserError("invitation expiry is malformed") from error
    if expires_at <= time.time():
        raise UserError("invitation has expired; ask the core operator for a new one")
    if urlparse(str(payload["core_url"])).scheme not in {"http", "https"}:
        raise UserError("invitation core URL is unsupported")
    if urlparse(str(payload["nats_url"])).scheme not in {"nats", "tls"}:
        raise UserError("invitation broker URL is unsupported")
    return payload


def _advertised_urls(host: str) -> tuple[str, str]:
    """Return Core and NATS URLs with a valid bracketed IPv6 authority."""
    value = host.rstrip("/")
    if "://" not in value:
        try:
            address = ipaddress.ip_address(value.strip("[]"))
        except ValueError:
            core_url = f"http://{value}"
        else:
            authority = f"[{address}]" if address.version == 6 else str(address)
            core_url = f"http://{authority}"
    else:
        core_url = value
    parsed = urlparse(core_url)
    try:
        parsed.port
    except ValueError as error:
        raise UserError(
            "advertised IPv6 hosts with a scheme must use brackets"
        ) from error
    if not parsed.hostname:
        raise UserError("advertised host is invalid")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        broker_authority = parsed.hostname
    else:
        broker_authority = f"[{address}]" if address.version == 6 else str(address)
    return core_url, f"nats://{broker_authority}:4222"


def command_create(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    existing_path = state_dir / NODE_STATE_NAME
    if existing_path.exists() and _load_node(state_dir)["mode"] != "core":
        raise UserError(
            "this host is already joined as an edge node; it cannot also become core"
        )
    if not args.no_start and shutil.which("docker") is None:
        raise UserError(
            "Docker is required to start a Core node; install Docker "
            f"Desktop/Engine, then rerun '{_command_name()} create'"
        )
    env, changed = _ensure_env()
    for directory in (CORE_RUNTIME_DIR / "data", CORE_RUNTIME_DIR / "nats" / "data"):
        directory.mkdir(parents=True, exist_ok=True)
    _render_nats_config()
    _validate_core_nats_config(env)

    core_url, nats_url = _advertised_urls(args.host)
    node = {
        "version": 1,
        "mode": "core",
        "core_url": core_url,
        "nats_url": nats_url,
        "nats_token": env["NATS_TOKEN"],
        "agent_id": "core",
        "created_at": int(time.time()),
    }
    if existing_path.exists():
        existing = _load_node(state_dir)
        node["created_at"] = existing.get("created_at", node["created_at"])
    _write_json(existing_path, node)

    if not args.no_start:
        _run(_compose_command("up", "--build", "-d"), cwd=INSTALL_ROOT)
        _wait_for_core(core_url, args.timeout)

    action = "generated" if changed else "preserved"
    print(f"EdgeCitadel core configured; secrets {action} and stored locally.")
    if args.no_start:
        print(
            "Core was not started (--no-start). Rerun without that option when ready."
        )
    else:
        print(f"Core is ready: {core_url}")
        print(
            f"Next: {_command_name()} invite --node-id <node-id> "
            "--host <reachable-host>"
        )
    return 0


def command_invite(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    if node["mode"] != "core":
        raise UserError("only a core node can create invitations")
    env = _read_env()
    admin_token = env.get("EDGECITADEL_ADMIN_TOKEN", "")
    if not admin_token:
        raise UserError("administrator credential is missing; rerun create")

    core_url, nats_url = _advertised_urls(args.host)
    response = _http_json(
        f"{node['core_url']}/api/enrollment/invitations",
        method="POST",
        body={
            "agent_id": args.agent_id,
            "expires_in_seconds": args.expires,
        },
        headers={"X-EdgeCitadel-Admin-Token": admin_token},
    )
    invitation = _invitation_encode(
        {
            "version": 1,
            "core_url": core_url,
            "nats_url": nats_url,
            "token": response["token"],
            "agent_id": response["agent_id"],
            "expires_at": response["expires_at"],
        }
    )
    print("Single-use invitation (contains a temporary enrollment secret):")
    print(invitation)
    print(f"Expires in {args.expires // 60} minute(s).")
    return 0


def command_join(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    state_path = state_dir / NODE_STATE_NAME
    requested_mode = getattr(args, "messaging_mode", "single-client")
    if state_path.exists():
        existing = _load_node(state_dir)
        if existing["mode"] == "edge":
            if existing["messaging_mode"] == requested_mode:
                print(
                    f"This host is already joined as {existing['agent_id']} "
                    f"with messaging mode {requested_mode}; no changes made."
                )
                return 0
            raise UserError(
                "this host is already joined with messaging mode "
                f"{existing['messaging_mode']}; requested {requested_mode}. "
                "join does not convert messaging topology"
            )
        raise UserError("this host is already initialized as a core node")

    invitation = _invitation_decode(args.invitation)
    binary: str | None = None
    if requested_mode == "nats_leaf":
        try:
            binary = nats_leaf.preflight(
                state_dir=state_dir,
                node_id=str(invitation["agent_id"]),
                upstream_nats_url=str(invitation["nats_url"]),
            )
        except nats_leaf.NatsLeafError as error:
            raise UserError(str(error)) from error
    response = _http_json(
        f"{invitation['core_url']}/api/enrollment/redeem",
        method="POST",
        body={"token": invitation["token"], "messaging_mode": requested_mode},
    )
    common = {
        "version": 2,
        "mode": "edge",
        "messaging_mode": requested_mode,
        "core_url": invitation["core_url"],
        "upstream_nats_url": invitation["nats_url"],
        "agent_id": response["agent_id"],
        "created_at": int(time.time()),
    }
    if requested_mode == "single-client":
        token = response.get("nats_token")
        if not isinstance(token, str) or not token:
            raise UserError("core returned an incomplete single-client enrollment")
        node = {
            **common,
            "plugin_nats_url": invitation["nats_url"],
            "plugin_nats_token": token,
            "nats_url": invitation["nats_url"],
            "nats_token": token,
        }
    else:
        leaf_username = response.get("leaf_username")
        leaf_password = response.get("leaf_password")
        if not isinstance(leaf_username, str) or not isinstance(leaf_password, str):
            raise UserError("core returned an incomplete nats_leaf enrollment")
        local_token = secrets.token_urlsafe(32)
        try:
            nats_leaf.configure_and_start(
                state_dir=state_dir,
                node_id=str(response["agent_id"]),
                upstream_nats_url=str(invitation["nats_url"]),
                local_token=local_token,
                leaf_username=leaf_username,
                leaf_password=leaf_password,
                binary=binary,
            )
            node = {
                **common,
                "plugin_nats_url": nats_leaf.plugin_url(),
                "plugin_nats_token": local_token,
                "jetstream_domain": nats_leaf.domain_for(str(response["agent_id"])),
                "nats_url": nats_leaf.plugin_url(),
                "nats_token": local_token,
            }
            _private_directory(state_dir)
            _write_json(state_path, node)
        except (nats_leaf.NatsLeafError, OSError) as error:
            nats_leaf.cleanup_failed_join(state_dir)
            raise UserError(
                "nats_leaf enrollment was redeemed but local setup failed; no node state "
                "was committed. On the Core, create a new invitation with "
                f"'{_command_name()} invite --node-id {invitation['agent_id']} "
                "--host <reachable-host>', then rerun join"
            ) from error
    if requested_mode == "single-client":
        _private_directory(state_dir)
        _write_json(state_path, node)
    print(f"This host joined EdgeCitadel as {node['agent_id']}.")
    print(f"Messaging mode: {requested_mode}")
    print(f"Next: {_command_name()} agent install <managed-agent-path-or-name>")
    return 0


def _tcp_ready(nats_url: str, timeout: float = 1) -> bool:
    parsed = urlparse(nats_url)
    if not parsed.hostname or not parsed.port:
        return False
    try:
        with socket.create_connection((parsed.hostname, parsed.port), timeout=timeout):
            return True
    except OSError:
        return False


def command_doctor(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    checks: list[dict[str, Any]] = []

    def add_check(check_id: str, name: str, ok: bool, detail: str) -> None:
        checks.append({"id": check_id, "name": name, "ok": bool(ok), "detail": detail})

    try:
        node = _load_node(state_dir)
        messaging_mode = node.get("messaging_mode", "core")
        add_check("node_configuration", "node configuration", True, node["mode"])
    except UserError as error:
        node = None
        messaging_mode = "unknown"
        add_check("node_configuration", "node configuration", False, str(error))

    if node:
        core_api_ok = False
        try:
            status = _http_json(f"{node['core_url']}/api/system/status", timeout=2)
            core_api_ok = bool(
                status.get("nats_connected") and status.get("jetstream_stream_ok")
            )
            add_check("core_api", "core API", core_api_ok, node["core_url"])
        except UserError as error:
            add_check("core_api", "core API", False, str(error))
        upstream_url = node.get("upstream_nats_url", node.get("nats_url", ""))
        core_nats_ok = bool(upstream_url and _tcp_ready(upstream_url))
        add_check(
            "core_nats", "Core NATS", core_nats_ok, upstream_url or "not configured"
        )

        local_observation: dict[str, Any] | None = None
        if messaging_mode == "nats_leaf":
            local_observation = nats_leaf.observe(state_dir)
            add_check(
                "local_nats_process",
                "Local NATS process",
                local_observation["process_running"],
                "running" if local_observation["process_running"] else "stopped",
            )
            add_check(
                "local_nats_client",
                "Local NATS client",
                local_observation["client_ready"],
                node["plugin_nats_url"],
            )
            add_check(
                "local_jetstream",
                "Local JetStream",
                local_observation["jetstream_ready"],
                node["jetstream_domain"],
            )
            add_check(
                "leaf_connection",
                "Leaf connection",
                local_observation["leaf_connected"],
                "connected" if local_observation["leaf_connected"] else "disconnected",
            )
            add_check(
                "local_agent_messaging",
                "Local agent messaging",
                local_observation["local_ready"],
                "available" if local_observation["local_ready"] else "unavailable",
            )
            cross_node = bool(local_observation["leaf_connected"] and core_api_ok)
            add_check(
                "cross_node_messaging",
                "Cross-node messaging",
                cross_node,
                "available" if cross_node else "paused",
            )
        elif node["mode"] == "edge":
            add_check("local_nats_process", "Local NATS", True, "not used")

        if node["mode"] == "edge":
            agentd_running, agentd_detail = _agentd_process_detail(state_dir)
            add_check(
                "edgecitadel_service",
                "EdgeCitadel service",
                agentd_running,
                agentd_detail,
            )
            if agentd_running:
                agentd_health = _agentd_rpc(state_dir, "health")
                transport = agentd_health.get("transport", {})
                transport_connected = bool(
                    isinstance(transport, dict) and transport.get("connected")
                )
                add_check(
                    "agentd_transport",
                    "Agent task transport",
                    transport_connected,
                    "connected" if transport_connected else "disconnected",
                )

        for plugin_id, record in sorted(
            _load_plugins(state_dir)["managed_agents"].items()
        ):
            enabled = record.get("enabled", True) is not False
            if not enabled:
                add_check(
                    f"managed_agent_{plugin_id}",
                    f"Managed Agent {plugin_id}",
                    True,
                    "disabled",
                )
                for declared_agent in record["inventory"]["agents"]:
                    agent_id = declared_agent["id"]
                    add_check(
                        f"agent_{agent_id}",
                        f"agent {agent_id}",
                        True,
                        "disabled with Managed Agent",
                    )
                continue
            running, process_detail = _plugin_process_detail(record)
            add_check(
                f"managed_agent_{plugin_id}",
                f"Managed Agent {plugin_id}",
                running,
                process_detail,
            )
            for declared_agent in record["inventory"]["agents"]:
                agent_id = declared_agent["id"]
                try:
                    agent = _http_json(
                        f"{node['core_url']}/api/agents/{agent_id}", timeout=1
                    )
                    online = agent.get("agent_state") == "online"
                    detail = agent.get("agent_state", "unknown")
                except UserError as error:
                    online, detail = False, str(error)
                add_check(f"agent_{agent_id}", f"agent {agent_id}", online, detail)

    try:
        plugins_root(INSTALL_ROOT)
        add_check(
            "plugin_assets",
            "Plugin distribution assets",
            True,
            "available",
        )
        for host in HOSTS:
            plugin_status = driver_for(
                host, INSTALL_ROOT, project_root=Path.cwd()
            ).status("user")
            optional_absence = plugin_status.state == "absent"
            add_check(
                f"plugin_{host}",
                f"Plugin {host}",
                plugin_status.state == "installed" or optional_absence,
                (
                    "not installed (optional)"
                    if optional_absence
                    else plugin_status.state
                ),
            )
    except AssetResolutionError as error:
        add_check("plugin_assets", "Plugin distribution assets", False, str(error))

    if node:
        agentd_running, _ = _agentd_process_detail(state_dir)
        if agentd_running:
            connector_inventory = _agentd_rpc(state_dir, "connector.list")
            if not isinstance(connector_inventory, list):
                connector_inventory = []
            for connector in connector_inventory:
                if not isinstance(connector, dict):
                    continue
                host_type = connector.get("host_type")
                if host_type not in HOSTS:
                    continue
                active = bool(
                    connector.get("session_active") and not connector.get("revoked")
                )
                add_check(
                    f"connector_{connector.get('connector_id', host_type)}",
                    f"Connector {connector.get('connector_id', host_type)}",
                    active,
                    "active" if active else "inactive",
                )

    all_ok = bool(checks and all(item["ok"] for item in checks))
    if all_ok:
        health = "healthy"
    elif (
        node
        and messaging_mode == "nats_leaf"
        and nats_leaf.observe(state_dir)["local_ready"]
    ):
        health = "degraded"
    else:
        health = "failed"

    if args.json:
        print(
            json.dumps(
                {
                    "ok": all_ok,
                    "status": health,
                    "node_role": node["mode"] if node else None,
                    "messaging_mode": messaging_mode,
                    "checks": checks,
                },
                indent=2,
            )
        )
    else:
        if node:
            print(f"Node role: {node['mode'].title()}")
            if node["mode"] == "edge":
                print(f"Messaging mode: {messaging_mode}")
                broker = (
                    "local"
                    if messaging_mode == "nats_leaf"
                    else node["plugin_nats_url"]
                )
                print(f"Managed Agent broker: {broker}")
            print(f"Status: {health}")
        for item in checks:
            marker = "PASS" if item["ok"] else "FAIL"
            print(f"{marker:4}  {item['name']}: {item['detail']}")
    return 0 if all_ok else 1


def command_down(args: argparse.Namespace) -> int:
    node = _load_node(_state_dir(args.state_dir))
    if node["mode"] != "core":
        raise UserError(
            "down controls the Docker stack and is only valid on a core node"
        )
    _run(_compose_command("down"), cwd=INSTALL_ROOT)
    print("EdgeCitadel core stopped. Local state and data were preserved.")
    return 0


def _plugins_path(state_dir: Path) -> Path:
    return state_dir / MANAGED_AGENT_STATE_NAME


def _legacy_plugins_path(state_dir: Path) -> Path:
    return state_dir / PLUGIN_STATE_NAME


def _load_plugins(state_dir: Path) -> dict[str, Any]:
    path = _plugins_path(state_dir)
    if not path.exists() and _legacy_plugins_path(state_dir).exists():
        legacy = _read_json(_legacy_plugins_path(state_dir), {})
        if legacy.get("version") != 1 or not isinstance(legacy.get("plugins"), dict):
            raise UserError(
                f"legacy plugin state is unsupported: {_legacy_plugins_path(state_dir)}"
            )
        _write_json(
            path,
            {"version": 2, "managed_agents": legacy["plugins"]},
        )
    state = _read_json(path, {"version": 2, "managed_agents": {}})
    if state.get("version") != 2 or not isinstance(state.get("managed_agents"), dict):
        raise UserError(f"Managed Agent state is unsupported: {path}")
    if "edgecitadel.watchdog" in state["managed_agents"]:
        state["managed_agents"].pop("edgecitadel.watchdog")
        _write_json(path, state)
    return state


def _toolkit_python(state_dir: Path) -> Path:
    managed = os.environ.get("EDGECITADEL_SUPERVISOR_PYTHON")
    if managed:
        python = Path(managed)
        if not python.exists():
            raise UserError(f"Homebrew Agent service runtime is missing: {python}")
        return python
    venv = state_dir / "supervisor"
    python = venv / "bin" / "python"
    marker = venv / ".edgecitadel-toolkit-version"
    expected = (
        f"{VERSION}|{Path(sys.executable).resolve()}|{INSTALL_ROOT.resolve()}|"
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}\n"
    )
    if python.exists() and marker.exists() and marker.read_text() == expected:
        return python

    print("Preparing the local Agent service...", file=sys.stderr)
    _run([sys.executable, "-m", "venv", str(venv)])
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            "-e",
            str(_asset_root(agent_platform_root)),
        ]
    )
    _secure_write(marker, expected)
    return python


def _agentd_state_dir(state_dir: Path) -> Path:
    return state_dir / "agentd"


def _agentd_process_path(state_dir: Path) -> Path:
    return _agentd_state_dir(state_dir) / AGENTD_PROCESS_STATE_NAME


def _agentd_admin_token(state_dir: Path) -> str:
    path = _agentd_state_dir(state_dir) / AGENTD_ADMIN_TOKEN_NAME
    if path.is_symlink() or not path.is_file():
        raise UserError("EdgeCitadel service management credential is unavailable")
    token = path.read_text().strip()
    if len(token) < 32 or len(token) > 1024:
        raise UserError("EdgeCitadel service management credential is invalid")
    return token


def _agentd_launchd_label(state_dir: Path) -> str:
    digest = hashlib.sha256(str(state_dir.resolve()).encode()).hexdigest()[:12]
    return f"com.edgecitadel.agentd.{digest}"


def _agentd_launchd_target(state_dir: Path) -> str:
    return f"gui/{os.getuid()}/{_agentd_launchd_label(state_dir)}"


def _agentd_uses_launchd() -> bool:
    return (
        sys.platform == "darwin"
        and (IS_HOMEBREW or IS_PIP)
        and shutil.which("launchctl") is not None
    )


def _agentd_launchd_path(state_dir: Path) -> Path:
    return _agentd_state_dir(state_dir) / "agentd.plist"


def _agentd_systemd_unit_name(state_dir: Path) -> str:
    digest = hashlib.sha256(str(state_dir.resolve()).encode()).hexdigest()[:12]
    return f"edgecitadel-agentd-{digest}.service"


def _agentd_uses_systemd() -> bool:
    return (
        sys.platform.startswith("linux")
        and (IS_HOMEBREW or IS_PIP)
        and shutil.which("systemctl") is not None
    )


def _agentd_systemd_path(state_dir: Path) -> Path:
    return _agentd_state_dir(state_dir) / _agentd_systemd_unit_name(state_dir)


def _systemd_quote(value: str | Path) -> str:
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def _render_agentd_systemd(state_dir: Path, python: Path) -> None:
    service_dir = _agentd_state_dir(state_dir)
    payload = "\n".join(
        (
            "[Unit]",
            "Description=EdgeCitadel host-local Agent service",
            "After=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            "ExecStart="
            f"{_systemd_quote(python)} -m edgecitadel_agentd --state-dir "
            f"{_systemd_quote(service_dir)}",
            f"WorkingDirectory={_systemd_quote(INSTALL_ROOT)}",
            "Restart=on-failure",
            "RestartSec=2",
            f"StandardOutput={_systemd_quote(f'append:{service_dir / "agentd.log"}')}",
            f"StandardError={_systemd_quote(f'append:{service_dir / "agentd.log"}')}",
            "UMask=0077",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        )
    )
    _secure_write(_agentd_systemd_path(state_dir), payload)


def _render_agentd_launchd(state_dir: Path, python: Path) -> None:
    service_dir = _agentd_state_dir(state_dir)
    payload = plistlib.dumps(
        {
            "Label": _agentd_launchd_label(state_dir),
            "ProgramArguments": [
                str(python),
                "-m",
                "edgecitadel_agentd",
                "--state-dir",
                str(service_dir),
            ],
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ProcessType": "Interactive",
            "StandardOutPath": str(service_dir / "agentd.log"),
            "StandardErrorPath": str(service_dir / "agentd.log"),
        },
        fmt=plistlib.FMT_XML,
        sort_keys=True,
    ).decode()
    _secure_write(_agentd_launchd_path(state_dir), payload)


def _launchd_loaded(state_dir: Path) -> bool:
    result = subprocess.run(
        ["launchctl", "print", _agentd_launchd_target(state_dir)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _systemd_loaded(state_dir: Path) -> bool:
    result = subprocess.run(
        [
            "systemctl",
            "--user",
            "is-active",
            "--quiet",
            _agentd_systemd_unit_name(state_dir),
        ],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _agentd_rpc(
    state_dir: Path,
    operation: str,
    *,
    auth_connector_id: str | None = None,
    auth_token: str | None = None,
    **params: object,
) -> Any:
    request: dict[str, object] = {"operation": operation, **params}
    if operation in AGENTD_ADMIN_OPERATIONS:
        request["admin_token"] = _agentd_admin_token(state_dir)
    if auth_connector_id is not None:
        request["connector_id"] = auth_connector_id
    if auth_token is not None:
        request["token"] = auth_token
    python = _toolkit_python(state_dir)
    result = subprocess.run(
        [
            str(python),
            "-m",
            "edgecitadel_agentd.rpc",
            "--state-dir",
            str(_agentd_state_dir(state_dir)),
        ],
        cwd=INSTALL_ROOT,
        input=json.dumps(request),
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise UserError("EdgeCitadel service returned an invalid response") from error
    if result.returncode != 0 or not response.get("ok"):
        raise UserError(
            str(response.get("error", "EdgeCitadel service operation failed"))
        )
    return response["result"]


def _agentd_process_detail(state_dir: Path) -> tuple[bool, str]:
    record = _read_json(_agentd_process_path(state_dir), {})
    pid = record.get("pid")
    identity = record.get("process_identity")
    if not isinstance(pid, int) or not _pid_running(pid):
        return False, "stopped"
    if not isinstance(identity, str) or identity != _process_identity(pid):
        return False, f"unverified pid {pid}"
    try:
        health = _agentd_rpc(state_dir, "health")
    except UserError:
        return False, f"pid {pid}, not ready"
    return health.get("status") == "ready", f"pid {pid}, {health.get('status')}"


def _start_agentd(state_dir: Path) -> dict[str, Any]:
    running, detail = _agentd_process_detail(state_dir)
    if running:
        return {
            "running": True,
            "detail": detail,
            "health": _agentd_rpc(state_dir, "health"),
        }
    record = _read_json(_agentd_process_path(state_dir), {})
    stale_pid = record.get("pid")
    if isinstance(stale_pid, int) and _pid_running(stale_pid):
        raise UserError(
            f"EdgeCitadel service has an unverified live PID {stale_pid}; "
            "verify that process manually before restarting"
        )
    service_dir = _agentd_state_dir(state_dir)
    _private_directory(service_dir)
    log_path = service_dir / "agentd.log"
    log_path.touch(mode=0o600, exist_ok=True)
    log_path.chmod(0o600)
    python = _toolkit_python(state_dir)
    process: subprocess.Popen[bytes] | None = None
    if _agentd_uses_launchd():
        _render_agentd_launchd(state_dir, python)
        if _launchd_loaded(state_dir):
            subprocess.run(
                ["launchctl", "bootout", _agentd_launchd_target(state_dir)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        result = subprocess.run(
            [
                "launchctl",
                "bootstrap",
                f"gui/{os.getuid()}",
                str(_agentd_launchd_path(state_dir)),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise UserError(
                "EdgeCitadel user service could not be loaded; "
                f"inspect {log_path} and retry '{_command_name()} service start'"
            )
    elif _agentd_uses_systemd():
        _render_agentd_systemd(state_dir, python)
        unit_name = _agentd_systemd_unit_name(state_dir)
        for command in (
            [
                "systemctl",
                "--user",
                "link",
                "--force",
                str(_agentd_systemd_path(state_dir)),
            ],
            ["systemctl", "--user", "daemon-reload"],
            ["systemctl", "--user", "enable", "--now", unit_name],
        ):
            result = subprocess.run(
                command, check=False, capture_output=True, text=True
            )
            if result.returncode != 0:
                raise UserError(
                    "EdgeCitadel systemd user service could not be loaded; "
                    f"inspect {log_path} and retry '{_command_name()} service start'"
                )
    else:
        log_handle = log_path.open("ab")
        try:
            process = subprocess.Popen(
                [
                    str(python),
                    "-m",
                    "edgecitadel_agentd",
                    "--state-dir",
                    str(service_dir),
                ],
                cwd=INSTALL_ROOT,
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            log_handle.close()
    deadline = time.monotonic() + 10
    last_error = "not ready"
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            last_error = f"process exited with status {process.returncode}"
            break
        try:
            health = _agentd_rpc(state_dir, "health")
            if process is not None:
                identity = _process_identity(process.pid)
                if not identity:
                    raise UserError(
                        "could not verify EdgeCitadel service process identity"
                    )
                _write_json(
                    _agentd_process_path(state_dir),
                    {
                        "version": 1,
                        "pid": process.pid,
                        "process_identity": identity,
                    },
                )
            running, observed = _agentd_process_detail(state_dir)
            if not running:
                raise UserError(observed)
            return {"running": True, "detail": observed, "health": health}
        except UserError as error:
            last_error = str(error)
            time.sleep(0.1)
    if _agentd_uses_launchd() and _launchd_loaded(state_dir):
        subprocess.run(
            ["launchctl", "bootout", _agentd_launchd_target(state_dir)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif _agentd_uses_systemd() and _agentd_systemd_path(state_dir).exists():
        subprocess.run(
            [
                "systemctl",
                "--user",
                "disable",
                "--now",
                _agentd_systemd_unit_name(state_dir),
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    elif process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    raise UserError(
        "EdgeCitadel service did not become ready; "
        f"inspect {log_path} and retry 'edgecitadel service start' ({last_error})"
    )


def _stop_agentd(state_dir: Path) -> None:
    record = _read_json(_agentd_process_path(state_dir), {})
    pid = record.get("pid")
    identity = record.get("process_identity")
    if not isinstance(pid, int) or not _pid_running(pid):
        if _agentd_uses_launchd() and _launchd_loaded(state_dir):
            subprocess.run(
                ["launchctl", "bootout", _agentd_launchd_target(state_dir)],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        elif _agentd_uses_systemd() and _agentd_systemd_path(state_dir).exists():
            subprocess.run(
                [
                    "systemctl",
                    "--user",
                    "disable",
                    "--now",
                    _agentd_systemd_unit_name(state_dir),
                ],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        _write_json(_agentd_process_path(state_dir), {"version": 1, "pid": None})
        return
    if not isinstance(identity, str) or identity != _process_identity(pid):
        raise UserError(f"refusing to stop unverified EdgeCitadel service PID {pid}")
    if _agentd_uses_launchd() and _launchd_loaded(state_dir):
        result = subprocess.run(
            ["launchctl", "bootout", _agentd_launchd_target(state_dir)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 0:
            raise UserError("EdgeCitadel user service could not be unloaded")
    elif _agentd_uses_systemd() and _systemd_loaded(state_dir):
        result = subprocess.run(
            [
                "systemctl",
                "--user",
                "disable",
                "--now",
                _agentd_systemd_unit_name(state_dir),
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode != 0:
            raise UserError("EdgeCitadel systemd user service could not be stopped")
    else:
        os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 5
    while _pid_running(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    if _pid_running(pid):
        raise UserError("EdgeCitadel service did not stop within 5 seconds")
    _write_json(_agentd_process_path(state_dir), {"version": 1, "pid": None})


def command_service(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    if args.action == "start":
        observation = _start_agentd(state_dir)
        _sync_managed_agent_state(state_dir, _load_plugins(state_dir))
    elif args.action == "stop":
        _stop_agentd(state_dir)
        observation = {"running": False, "detail": "stopped"}
    elif args.action == "restart":
        _stop_agentd(state_dir)
        observation = _start_agentd(state_dir)
        _sync_managed_agent_state(state_dir, _load_plugins(state_dir))
    else:
        running, detail = _agentd_process_detail(state_dir)
        observation = {"running": running, "detail": detail}
        if running:
            observation["health"] = _agentd_rpc(state_dir, "health")
    if args.json:
        print(json.dumps(observation, indent=2))
    else:
        print(f"EdgeCitadel service: {observation['detail']}")
    return 0 if observation["running"] or args.action == "stop" else 1


def _connector_token_path(state_dir: Path, connector_id: str) -> Path:
    if not connector_id or any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789-_"
        for character in connector_id
    ):
        raise UserError(
            "connector id must contain only lowercase letters, numbers, '-' or '_'"
        )
    return state_dir / "connectors" / f"{connector_id}.token"


def _connector_token(state_dir: Path, connector_id: str) -> str:
    path = _connector_token_path(state_dir, connector_id)
    try:
        token = path.read_text().strip()
    except OSError as error:
        raise UserError(
            f"Native connector {connector_id} is not registered; run "
            f"'{_command_name()} connector register {connector_id} --host-type <host>'"
        ) from error
    if not token:
        raise UserError(f"Native connector credential is empty: {path}")
    return token


def command_connector(args: argparse.Namespace) -> int:
    if args.connector_action == "path":
        try:
            path = plugin_source(INSTALL_ROOT, args.host_type)
        except AssetResolutionError as error:
            raise UserError(str(error)) from error
        print(
            "warning: 'connector path' is deprecated; use "
            f"'{_command_name()} plugin install {args.host_type}'",
            file=sys.stderr,
        )
        print(path)
        return 0
    state_dir = _state_dir(args.state_dir)
    _start_agentd(state_dir)
    if args.connector_action == "register":
        node = _load_node(state_dir)
        agent_id = args.agent_id or f"{node['agent_id']}-{args.host_type}"
        capabilities = list(NATIVE_CONNECTOR_CAPABILITIES)
        token_path = _connector_token_path(state_dir, args.connector_id)
        if token_path.is_file():
            _agentd_rpc(
                state_dir,
                "connector.configure",
                connector_id=args.connector_id,
                host_type=args.host_type,
                agent_id=agent_id,
                capabilities=capabilities,
            )
            action = "updated"
        else:
            response = _agentd_rpc(
                state_dir,
                "connector.register",
                connector_id=args.connector_id,
                host_type=args.host_type,
                agent_id=agent_id,
                capabilities=capabilities,
            )
            _secure_write(token_path, str(response["token"]) + "\n")
            action = "registered"
        print(f"Native connector {args.connector_id} {action} for {agent_id}.")
        return 0
    if args.connector_action == "list":
        connectors = _agentd_rpc(state_dir, "connector.list")
        if args.json:
            print(json.dumps(connectors, indent=2))
        elif not connectors:
            print("No Plugin Connectors registered.")
        else:
            for connector in connectors:
                state = "revoked" if connector["revoked"] else "registered"
                print(
                    f"{connector['connector_id']:24} {connector['host_type']:12} "
                    f"{state:10} session={'active' if connector['session_active'] else 'closed':6} "
                    f"agent={connector['agent_id']}"
                )
        return 0
    if args.connector_action == "status":
        connectors = _agentd_rpc(state_dir, "connector.list")
        connector = next(
            (
                item
                for item in connectors
                if item.get("connector_id") == args.connector_id
            ),
            None,
        )
        if connector is None:
            raise UserError(f"Native connector was not found: {args.connector_id}")
        if args.json:
            print(json.dumps(connector, indent=2))
        else:
            print(f"Native connector: {connector['connector_id']}")
            print(f"Host: {connector['host_type']}")
            print(f"Agent: {connector['agent_id']}")
            print(f"Credential: {'revoked' if connector['revoked'] else 'active'}")
            print(f"Session: {'active' if connector['session_active'] else 'closed'}")
        return 0 if not connector["revoked"] else 1
    _agentd_rpc(
        state_dir,
        "connector.revoke",
        connector_id=args.connector_id,
    )
    _connector_token_path(state_dir, args.connector_id).unlink(missing_ok=True)
    print(f"Native connector {args.connector_id} revoked.")
    return 0


def command_task(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _start_agentd(state_dir)
    if args.task_action == "list":
        result = _agentd_rpc(
            state_dir,
            "task.list",
            auth_connector_id=args.connector_id,
            auth_token=_connector_token(state_dir, args.connector_id),
            include_terminal=not args.pending,
        )
    elif args.task_action == "show":
        result = _agentd_rpc(
            state_dir,
            "task.get",
            auth_connector_id=args.connector_id,
            auth_token=_connector_token(state_dir, args.connector_id),
            task_id=args.task_id,
        )
    else:
        result = _agentd_rpc(
            state_dir,
            "task.transition",
            auth_connector_id=args.connector_id,
            auth_token=_connector_token(state_dir, args.connector_id),
            task_id=args.task_id,
            state="cancelled",
            reason=args.reason,
        )
    print(json.dumps(result, indent=2))
    return 0


def command_trace(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _start_agentd(state_dir)
    token = _connector_token(state_dir, args.connector_id)
    if args.trace_action == "list":
        result = _agentd_rpc(
            state_dir,
            "trace.list",
            auth_connector_id=args.connector_id,
            auth_token=token,
            limit=args.limit,
        )
    elif args.trace_action == "show":
        result = _agentd_rpc(
            state_dir,
            "trace.get",
            auth_connector_id=args.connector_id,
            auth_token=token,
            trace_id=args.trace_id,
        )
    else:
        result = _agentd_rpc(
            state_dir,
            "trace.purge",
            auth_connector_id=args.connector_id,
            auth_token=token,
            before_ms=args.before_ms,
        )
    print(json.dumps(result, indent=2))
    return 0


def command_native_mcp(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _start_agentd(state_dir)
    node = _load_node(state_dir)
    connector_id = args.connector_id or f"{args.host_type}-local"
    suffix = f"-{args.host_type}"
    agent_id = args.agent_id or (
        f"{str(node['agent_id'])[: 64 - len(suffix)].rstrip('_-')}{suffix}"
    )
    token_path = _connector_token_path(state_dir, connector_id)
    if token_path.is_file():
        _agentd_rpc(
            state_dir,
            "connector.configure",
            connector_id=connector_id,
            host_type=args.host_type,
            agent_id=agent_id,
            capabilities=list(NATIVE_CONNECTOR_CAPABILITIES),
        )
    else:
        registration = _agentd_rpc(
            state_dir,
            "connector.register",
            connector_id=connector_id,
            host_type=args.host_type,
            agent_id=agent_id,
            capabilities=list(NATIVE_CONNECTOR_CAPABILITIES),
        )
        _secure_write(token_path, str(registration["token"]) + "\n")
    python = _toolkit_python(state_dir)
    command = [
        str(python),
        "-m",
        "edgecitadel_agentd.mcp",
        "--state-dir",
        str(state_dir),
        "--host-type",
        args.host_type,
    ]
    command.extend(["--connector-id", connector_id, "--agent-id", agent_id])
    return subprocess.run(command, cwd=INSTALL_ROOT, check=False).returncode


def _plugin_python(state_dir: Path, plugin_id: str, record: dict[str, Any]) -> Path:
    """Return an isolated runtime when a Managed Agent declares dependencies."""
    runtime = record["inventory"]["runtime"]
    requirements = runtime.get("pythonRequirements")
    if requirements is None:
        return _toolkit_python(state_dir)
    if not isinstance(requirements, str):
        raise UserError(f"Managed Agent {plugin_id} has invalid Python requirements")

    plugin_root = Path(record["path"])
    requirements_path = plugin_root / requirements
    if not requirements_path.is_file():
        raise UserError(f"Managed Agent {plugin_id} is missing its Python requirements")
    version = record["inventory"]["package"]["version"]
    runtime_root = state_dir / "plugin-runtimes" / plugin_id / version
    python = runtime_root / "bin" / "python"
    marker = runtime_root / ".edgecitadel-runtime"
    fingerprint = hashlib.sha256(requirements_path.read_bytes()).hexdigest()
    expected = (
        f"{VERSION}|{Path(sys.executable).resolve()}|{INSTALL_ROOT.resolve()}|"
        f"{fingerprint}|"
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}\n"
    )
    if python.exists() and marker.exists() and marker.read_text() == expected:
        return python

    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    print(f"Preparing isolated Python runtime for Managed Agent {plugin_id}...")
    try:
        _run([sys.executable, "-m", "venv", str(runtime_root)])
        _run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--quiet",
                "--disable-pip-version-check",
                "-e",
                str(_asset_root(agent_platform_root)),
                "-r",
                str(requirements_path),
            ]
        )
        _secure_write(marker, expected)
    except (OSError, UserError):
        if runtime_root.exists():
            shutil.rmtree(runtime_root)
        raise
    return python


def _validate_plugin(source: Path, state_dir: Path) -> dict[str, Any]:
    if not source.is_dir():
        raise UserError(f"Agent Package directory does not exist: {source}")
    python = _toolkit_python(state_dir)
    command = [str(python), "-m", "edgecitadel_supervisor", "validate", str(source)]
    try:
        result = subprocess.run(
            command, cwd=INSTALL_ROOT, check=True, capture_output=True, text=True
        )
        value = json.loads(result.stdout)
    except subprocess.CalledProcessError as error:
        detail = error.stderr.strip() or "package validation failed"
        raise UserError(detail.removeprefix("error: ").strip()) from error
    except json.JSONDecodeError as error:
        raise UserError(
            "Agent service returned invalid Managed Agent inventory"
        ) from error
    return value


def _permission_lines(inventory: dict[str, Any]) -> list[str]:
    permissions = inventory["permissions"]
    return [
        f"knowledge: {', '.join(permissions['knowledge']) or 'none'}",
        f"message agents: {', '.join(permissions['messaging']['outboundAgents']) or 'none'}",
        f"network: {', '.join(permissions['network']['outbound']) or 'none'}",
        f"devices: {', '.join(permissions['devices']) or 'none'}",
        f"sandbox: {inventory['security']['sandbox']}",
        f"secrets: {', '.join(inventory['security']['secrets']) or 'none'}",
    ]


def _confirm_plugin(inventory: dict[str, Any], assume_yes: bool) -> None:
    package = inventory["package"]
    print(f"Managed Agent: {package['id']} {package['version']}")
    print("Requested permissions:")
    for line in _permission_lines(inventory):
        print(f"  {line}")
    if assume_yes:
        return
    if not sys.stdin.isatty():
        raise UserError(
            "permission approval is required; review above and rerun with --yes"
        )
    if input("Install and allow these permissions? [y/N] ").strip().lower() not in {
        "y",
        "yes",
    }:
        raise UserError("installation cancelled; no Managed Agent files were installed")


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _process_identity(pid: int | None) -> str | None:
    """Return a stable, non-sensitive identity for one live process instance."""
    if not _pid_running(pid):
        return None
    assert pid is not None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        if proc_stat.is_file():
            fields = proc_stat.read_text().rpartition(") ")[2].split()
            if len(fields) > 19:
                return hashlib.sha256(f"linux:{pid}:{fields[19]}".encode()).hexdigest()
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-o", "command=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "LC_ALL": "C"},
        )
    except OSError:
        return None
    description = result.stdout.strip()
    if result.returncode != 0 or not description:
        return None
    return hashlib.sha256(f"ps:{pid}:{description}".encode()).hexdigest()


def _plugin_process_owned(record: dict[str, Any]) -> bool:
    identity = record.get("process_identity")
    return isinstance(identity, str) and identity == _process_identity(
        record.get("pid")
    )


def _plugin_process_detail(record: dict[str, Any]) -> tuple[bool, str]:
    pid = record.get("pid")
    if not _pid_running(pid):
        return False, "stopped"
    if not _plugin_process_owned(record):
        return False, f"unverified pid {pid}"
    return True, f"pid {pid}"


def _plugin_record(
    state_dir: Path, plugin_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    state = _load_plugins(state_dir)
    record = state["managed_agents"].get(plugin_id)
    if not isinstance(record, dict):
        raise UserError(f"Managed Agent is not installed: {plugin_id}")
    return state, record


def _managed_agent_summary(plugin_id: str, record: dict[str, Any]) -> dict[str, object]:
    inventory = record["inventory"]
    package = inventory["package"]
    runtime = inventory["runtime"]
    summary: dict[str, object] = {
        "package_id": plugin_id,
        "version": package["version"],
        "kind": package.get("kind", "LegacyPackage"),
        "runtime_kind": runtime.get("kind", "legacy"),
        "desired_state": "running" if record.get("enabled") else "stopped",
        "agent_ids": [agent["id"] for agent in inventory["agents"]],
        "install_path": record["path"],
        "installed_at": record["installed_at"],
    }
    launch_path = record.get("launch_path")
    if package.get("kind") == "ManagedAgent" and isinstance(launch_path, str):
        summary["launch_path"] = launch_path
    return summary


def _managed_launch_path(state_dir: Path, plugin_id: str) -> Path:
    safe_id = plugin_id.replace(".", "-")
    return state_dir / "managed-launch" / f"{safe_id}.json"


def _write_managed_launch(
    state_dir: Path,
    plugin_id: str,
    *,
    argv: list[str],
    cwd: Path,
    environment: dict[str, str],
    log_path: Path,
    restart_policy: str,
) -> Path:
    path = _managed_launch_path(state_dir, plugin_id)
    _write_json(
        path,
        {
            "version": 1,
            "package_id": plugin_id,
            "argv": argv,
            "cwd": str(cwd),
            "environment": environment,
            "log_path": str(log_path),
            "restart_policy": restart_policy,
        },
    )
    return path


def _sync_managed_agent_state(state_dir: Path, state: dict[str, Any]) -> None:
    running, _detail = _agentd_process_detail(state_dir)
    if not running:
        return
    records = [
        _managed_agent_summary(plugin_id, record)
        for plugin_id, record in sorted(state["managed_agents"].items())
        if record.get("inventory", {}).get("package", {}).get("kind") == "ManagedAgent"
    ]
    _agentd_rpc(state_dir, "managed.reconcile", records=records)


def _prepare_managed_agent_service(args: argparse.Namespace, state_dir: Path) -> None:
    if getattr(args, "command", None) == "agent":
        _start_agentd(state_dir)
        _sync_managed_agent_state(state_dir, _load_plugins(state_dir))


_PLUGIN_BASE_ENVIRONMENT = frozenset(
    {
        "HOME",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "PATH",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        "TMPDIR",
        "TZ",
    }
)


def _declared_plugin_environment(record: dict[str, Any]) -> dict[str, str]:
    """Copy only baseline and manifest-declared values from the CLI process."""
    inventory = record["inventory"]
    runtime_names = inventory["runtime"].get("environmentVariables", [])
    secret_names = inventory["security"].get("secrets", [])
    allowed = (
        _PLUGIN_BASE_ENVIRONMENT | frozenset(runtime_names) | frozenset(secret_names)
    )
    return {name: os.environ[name] for name in allowed if name in os.environ}


def _start_plugin(state_dir: Path, plugin_id: str) -> None:
    node = _load_node(state_dir)
    state, record = _plugin_record(state_dir, plugin_id)
    if record["inventory"]["package"].get("kind") != "ManagedAgent":
        raise UserError(
            f"Legacy package {plugin_id} cannot be started; reinstall it as a Managed Agent"
        )
    if _pid_running(record.get("pid")):
        if _plugin_process_owned(record):
            print(
                f"Managed Agent {plugin_id} is already running (pid {record['pid']})."
            )
            return
        raise UserError(
            f"Managed Agent {plugin_id} has an unverified live PID {record.get('pid')}; "
            "refusing to start another runtime. Verify that process manually, "
            "terminate it if appropriate, then retry"
        )
    command = record["inventory"]["runtime"]["command"]
    if not isinstance(command, list) or not all(
        isinstance(item, str) for item in command
    ):
        raise UserError(f"Managed Agent {plugin_id} has an invalid runtime command")
    managed_protocol = True
    python = _plugin_python(state_dir, plugin_id, record)
    executable = str(python) if command[0] in {"python", "python3"} else command[0]
    logs_dir = state_dir / "logs"
    _private_directory(logs_dir)
    log_path = logs_dir / f"{plugin_id}.log"
    plugin_state_dir = state_dir / "plugin-state" / plugin_id
    _private_directory(plugin_state_dir)
    environment = {
        **_declared_plugin_environment(record),
        "EDGECITADEL_NODE_ID": node["agent_id"],
        "EDGECITADEL_PLUGIN_ID": plugin_id,
        "EDGECITADEL_PLUGIN_STATE_DIR": str(plugin_state_dir),
        "EDGECITADEL_SCHEMA_DIR": str(INSTALL_ROOT / "schemas"),
    }
    agent_id = record["inventory"]["agents"][0]["id"]
    environment.update(
        {
            "EDGECITADEL_STATE_DIR": str(state_dir),
            "EDGECITADEL_CONNECTOR_ID": f"managed-{agent_id}",
        }
    )
    started_at = time.time()
    restart_policy = record["inventory"]["runtime"].get("restartPolicy", "never")
    if managed_protocol:
        _start_agentd(state_dir)
        agent_id = record["inventory"]["agents"][0]["id"]
        connector_id = f"managed-{agent_id}"
        connectors = _agentd_rpc(state_dir, "connector.list")
        existing_connector = next(
            (item for item in connectors if item.get("connector_id") == connector_id),
            None,
        )
        inventory_skills = record["inventory"].get("skills", [])
        capabilities = [
            str(skill["skillId"])
            for skill in inventory_skills
            if isinstance(skill, dict) and skill.get("skillId")
        ] or [
            str(skill_name)
            for skill_name in record["inventory"]["agents"][0]["skillNames"]
        ]
        process_status = next(
            (
                item
                for item in _agentd_rpc(state_dir, "managed.list")
                if item.get("package_id") == plugin_id
            ),
            None,
        )
        if (
            process_status is not None
            and process_status.get("runtime_state") == "running"
            and existing_connector is not None
            and existing_connector.get("session_active")
        ):
            print(
                f"Managed Agent {plugin_id} is already running "
                f"({process_status.get('detail', 'ready')})."
            )
            return
        token_path = _connector_token_path(state_dir, connector_id)
        if existing_connector is None:
            registration = _agentd_rpc(
                state_dir,
                "connector.register",
                connector_id=connector_id,
                host_type="managed-agent",
                agent_id=agent_id,
                capabilities=capabilities,
            )
            _secure_write(token_path, str(registration["token"]) + "\n")
        elif existing_connector.get("revoked") or not token_path.is_file():
            replacement = _agentd_rpc(
                state_dir,
                "managed.connector.reissue",
                connector_id=connector_id,
                agent_id=agent_id,
            )
            _secure_write(token_path, str(replacement["token"]) + "\n")
        _agentd_rpc(
            state_dir,
            "connector.configure",
            connector_id=connector_id,
            host_type="managed-agent",
            agent_id=agent_id,
            capabilities=capabilities,
        )
        launch_path = _write_managed_launch(
            state_dir,
            plugin_id,
            argv=[executable, *command[1:]],
            cwd=Path(record["path"]),
            environment=environment,
            log_path=log_path,
            restart_policy=str(restart_policy),
        )
        record.update(
            {
                "pid": None,
                "process_identity": None,
                "enabled": True,
                "started_at": started_at,
                "launch_path": str(launch_path),
            }
        )
        _write_json(_plugins_path(state_dir), state)
        _sync_managed_agent_state(state_dir, state)
        deadline = (
            time.monotonic() + record["inventory"]["runtime"]["healthTimeoutSeconds"]
        )
        last_detail = "starting"
        while time.monotonic() < deadline:
            processes = _agentd_rpc(state_dir, "managed.list")
            process_status = next(
                (item for item in processes if item.get("package_id") == plugin_id),
                None,
            )
            if process_status is not None:
                last_detail = str(process_status.get("detail", "starting"))
                if process_status.get("runtime_state") == "failed":
                    break
            connectors = _agentd_rpc(state_dir, "connector.list")
            connector = next(
                (
                    item
                    for item in connectors
                    if item.get("connector_id") == connector_id
                ),
                None,
            )
            if (
                process_status is not None
                and process_status.get("runtime_state") == "running"
                and connector is not None
                and connector.get("session_active")
            ):
                print(
                    f"Managed Agent {plugin_id} started ({last_detail}); "
                    f"local session ready for {agent_id}"
                )
                return
            time.sleep(0.25)
        record["enabled"] = False
        _write_json(_plugins_path(state_dir), state)
        _sync_managed_agent_state(state_dir, state)
        raise UserError(
            f"Managed Agent {plugin_id} did not become ready ({last_detail}); "
            f"inspect {log_path} and run '{_command_name()} service status'"
        )


def _stop_plugin(state_dir: Path, plugin_id: str, *, quiet: bool = False) -> None:
    state, record = _plugin_record(state_dir, plugin_id)
    inventory = record.get("inventory", {})
    package = inventory.get("package", {}) if isinstance(inventory, dict) else {}
    if isinstance(package, dict) and package.get("kind") == "ManagedAgent":
        record.update({"pid": None, "process_identity": None, "enabled": False})
        _write_json(_plugins_path(state_dir), state)
        _sync_managed_agent_state(state_dir, state)
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            processes = _agentd_rpc(state_dir, "managed.list")
            observed = next(
                (item for item in processes if item.get("package_id") == plugin_id),
                None,
            )
            if observed is None or observed.get("runtime_state") == "stopped":
                if not quiet:
                    print(f"Managed Agent {plugin_id} stopped.")
                return
            if observed.get("runtime_state") == "failed":
                raise UserError(
                    f"Managed Agent {plugin_id} could not be stopped: "
                    f"{observed.get('detail', 'unknown failure')}"
                )
            time.sleep(0.1)
        raise UserError(
            f"Managed Agent {plugin_id} did not stop; run "
            f"'{_command_name()} service restart' and inspect its status"
        )
    pid = record.get("pid")
    if _pid_running(pid):
        if not _plugin_process_owned(record):
            raise UserError(
                f"Managed Agent {plugin_id} has an unverified live PID {pid}; refusing to "
                "signal a process EdgeCitadel does not own"
            )
        assert isinstance(pid, int)
        try:
            process_group = os.getpgid(pid)
        except OSError as error:
            raise UserError(
                f"Managed Agent {plugin_id} process group is unavailable"
            ) from error
        if process_group != pid:
            raise UserError(
                f"Managed Agent {plugin_id} PID {pid} is not its owned process-group leader"
            )
        if not _plugin_process_owned(record):
            raise UserError(
                f"Managed Agent {plugin_id} process identity changed before signaling"
            )
        os.killpg(process_group, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while _pid_running(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        if _pid_running(pid):
            os.killpg(process_group, signal.SIGKILL)
    record.update({"pid": None, "process_identity": None, "enabled": False})
    _write_json(_plugins_path(state_dir), state)
    _sync_managed_agent_state(state_dir, state)
    if not quiet:
        print(f"Managed Agent {plugin_id} stopped.")


def _assert_agent_ids_available(
    node: dict[str, Any], inventory: dict[str, Any], plugin_state: dict[str, Any]
) -> None:
    """Reject identities already owned by a different local/fleet Managed Agent."""
    plugin_id = inventory["package"]["id"]
    requested = {agent["id"] for agent in inventory["agents"]}
    for other_plugin_id, record in plugin_state["managed_agents"].items():
        if other_plugin_id == plugin_id:
            continue
        claimed = {agent["id"] for agent in record["inventory"]["agents"]}
        conflicts = sorted(requested & claimed)
        if conflicts:
            raise UserError(
                f"Agent identity already belongs to local Managed Agent {other_plugin_id}: "
                f"{', '.join(conflicts)}"
            )

    fleet = _http_json(f"{node['core_url']}/api/agents", timeout=2)
    if not isinstance(fleet, list):
        raise UserError("Core returned an invalid agent inventory")
    existing_same_plugin = plugin_id in plugin_state["managed_agents"]
    for agent in fleet:
        if not isinstance(agent, dict) or agent.get("agent_id") not in requested:
            continue
        card = agent.get("card")
        metadata = card.get("metadata", {}) if isinstance(card, dict) else {}
        if not isinstance(metadata, dict):
            metadata = {}
        same_owner = (
            metadata.get("edgecitadel.node_id") == node["agent_id"]
            and metadata.get("edgecitadel.plugin_id") == plugin_id
        )
        ownership_declared = bool(
            metadata.get("edgecitadel.node_id") or metadata.get("edgecitadel.plugin_id")
        )
        if same_owner or (existing_same_plugin and not ownership_declared):
            continue
        raise UserError(
            f"agent identity already exists in the Core registry: {agent['agent_id']}; "
            "remove or rename the existing Agent before installation"
        )


@contextmanager
def _installable_plugin_source(source: Path, state_dir: Path) -> Iterator[Path]:
    """Stage pip-bundled Agent Packages without installer-generated bytecode."""
    bundled_root = _asset_root(agent_packages_root).resolve()
    if not IS_PIP or not source.is_relative_to(bundled_root):
        yield source
        return

    _private_directory(state_dir)
    with tempfile.TemporaryDirectory(prefix=".plugin-source-", dir=state_dir) as root:
        staged = Path(root) / source.name
        shutil.copytree(
            source,
            staged,
            symlinks=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.py[co]"),
        )
        yield staged


def _managed_connector_id(inventory: dict[str, Any]) -> str | None:
    package = inventory.get("package")
    agents = inventory.get("agents")
    if (
        not isinstance(package, dict)
        or package.get("kind") != "ManagedAgent"
        or not isinstance(agents, list)
        or len(agents) != 1
        or not isinstance(agents[0], dict)
        or not isinstance(agents[0].get("id"), str)
    ):
        return None
    return f"managed-{agents[0]['id']}"


def _revoke_managed_connector(state_dir: Path, connector_id: str) -> None:
    connectors = _agentd_rpc(state_dir, "connector.list")
    connector = next(
        (item for item in connectors if item.get("connector_id") == connector_id),
        None,
    )
    if connector is not None and not connector.get("revoked"):
        _agentd_rpc(state_dir, "connector.revoke", connector_id=connector_id)
    _connector_token_path(state_dir, connector_id).unlink(missing_ok=True)


def _install_plugin_source(
    args: argparse.Namespace, state_dir: Path, node: dict[str, Any], source: Path
) -> int:
    inventory = _validate_plugin(source, state_dir)
    package = inventory["package"]
    plugin_id = package["id"]
    target = state_dir / "plugins" / plugin_id / package["version"]
    state = _load_plugins(state_dir)
    _assert_agent_ids_available(node, inventory, state)
    _confirm_plugin(inventory, args.yes)
    existing = state["managed_agents"].get(plugin_id)
    previous = json.loads(json.dumps(existing)) if existing else None
    previous_enabled = bool(existing and existing.get("enabled"))
    upgrading = bool(existing and existing.get("path") != str(target))
    created_target = False
    if not target.exists():
        executable_files = {
            path.relative_to(source)
            for path in source.rglob("*")
            if path.is_file() and path.stat().st_mode & 0o111
        }
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".installing")
        if temporary.exists():
            shutil.rmtree(temporary)
        shutil.copytree(source, temporary, symlinks=True)
        temporary.rename(target)
        created_target = True
        for path in target.rglob("*"):
            mode = 0o500 if path.is_dir() else 0o400
            if path.is_file() and path.relative_to(target) in executable_files:
                mode = 0o500
            path.chmod(mode)
        target.chmod(0o500)
    elif (source / "plugin.lock.json").read_bytes() != (
        target / "plugin.lock.json"
    ).read_bytes():
        raise UserError(
            f"Managed Agent {plugin_id} {package['version']} is already installed with different content; "
            "publish a new version or remove the installed copy first"
        )
    installed_inventory = _validate_plugin(target, state_dir)
    if existing is not None and not upgrading:
        print(f"Managed Agent {plugin_id} is already installed.")
        if not args.keep_disabled and not existing.get("enabled"):
            _start_plugin(state_dir, plugin_id)
        return 0
    if upgrading and previous_enabled:
        try:
            _stop_plugin(state_dir, plugin_id, quiet=True)
        except UserError:
            if created_target:
                _remove_managed_package_tree(target)
            raise
        state = _load_plugins(state_dir)
    state["managed_agents"][plugin_id] = {
        "path": str(target),
        "inventory": installed_inventory,
        "installed_at": existing.get("installed_at") if existing else int(time.time()),
        "enabled": False,
        "pid": None,
        "process_identity": None,
        "launch_path": None,
    }
    _write_json(_plugins_path(state_dir), state)
    _sync_managed_agent_state(state_dir, state)
    action = "upgraded" if upgrading else "installed"
    print(f"Managed Agent {plugin_id} {action} in the Agent service store.")
    try:
        if not args.keep_disabled:
            _start_plugin(state_dir, plugin_id)
        previous_connector_id = (
            _managed_connector_id(previous.get("inventory", {}))
            if isinstance(previous, dict)
            else None
        )
        installed_connector_id = _managed_connector_id(installed_inventory)
        if previous_connector_id and previous_connector_id != installed_connector_id:
            _revoke_managed_connector(state_dir, previous_connector_id)
    except UserError as error:
        rollback = _load_plugins(state_dir)
        if previous is None:
            rollback["managed_agents"].pop(plugin_id, None)
        else:
            previous.update(
                enabled=False,
                pid=None,
                process_identity=None,
            )
            rollback["managed_agents"][plugin_id] = previous
        _write_json(_plugins_path(state_dir), rollback)
        _sync_managed_agent_state(state_dir, rollback)
        previous_connector_id = (
            _managed_connector_id(previous.get("inventory", {}))
            if isinstance(previous, dict)
            else None
        )
        installed_connector_id = _managed_connector_id(installed_inventory)
        if installed_connector_id and installed_connector_id != previous_connector_id:
            _revoke_managed_connector(state_dir, installed_connector_id)
        _managed_launch_path(state_dir, plugin_id).unlink(missing_ok=True)
        runtime_root = (
            state_dir
            / "plugin-runtimes"
            / plugin_id
            / str(installed_inventory["package"]["version"])
        )
        if runtime_root.exists():
            shutil.rmtree(runtime_root)
        recovery = "previous state restored"
        if previous_enabled:
            try:
                _start_plugin(state_dir, plugin_id)
                recovery = "previous version restarted"
            except UserError:
                recovery = (
                    "previous version restored but could not restart; run "
                    f"'{_command_name()} agent start {plugin_id}'"
                )
        if created_target:
            _remove_managed_package_tree(target)
        raise UserError(
            f"Managed Agent {plugin_id} failed readiness; {recovery}. "
            f"Original failure: {error}"
        ) from error
    return 0


def _remove_managed_package_tree(target: Path) -> None:
    if not target.exists():
        return
    for path in sorted(target.rglob("*"), reverse=True):
        path.chmod(0o700 if path.is_dir() else 0o600)
    target.chmod(0o700)
    shutil.rmtree(target)


def command_plugin_install(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    requested_source = Path(args.source).expanduser()
    packages = _asset_root(agent_packages_root)
    bundled_source = packages / args.source
    example_source = packages / "examples" / args.source
    source = next(
        (
            candidate.resolve()
            for candidate in (requested_source, bundled_source, example_source)
            if candidate.exists()
        ),
        requested_source.resolve(),
    )
    with _installable_plugin_source(source, state_dir) as installable_source:
        return _install_plugin_source(args, state_dir, node, installable_source)


def command_plugin_list(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    plugins = _load_plugins(state_dir)["managed_agents"]
    if not plugins:
        print(
            f"No Agent Packages installed. Use: {_command_name()} agent install <path-or-name>"
        )
        return 0
    managed_status = {
        item["package_id"]: item for item in _agentd_rpc(state_dir, "managed.list")
    }
    for plugin_id, record in sorted(plugins.items()):
        package = record["inventory"]["package"]
        if package.get("kind") == "ManagedAgent":
            observation = managed_status.get(plugin_id, {})
            running = observation.get("runtime_state") == "running"
        else:
            running, _detail = _plugin_process_detail(record)
        agents = ",".join(item["id"] for item in record["inventory"]["agents"])
        print(f"{plugin_id:24} {'running' if running else 'stopped':8} agents={agents}")
    return 0


def command_plugin_status(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    _, record = _plugin_record(state_dir, args.plugin_id)
    package = record["inventory"]["package"]
    managed_protocol = package.get("kind") == "ManagedAgent"
    if managed_protocol:
        statuses = _agentd_rpc(state_dir, "managed.list")
        observation = next(
            (item for item in statuses if item.get("package_id") == args.plugin_id),
            {},
        )
        running = observation.get("runtime_state") == "running"
        process_detail = str(observation.get("detail", "stopped"))
        connectors = _agentd_rpc(state_dir, "connector.list")
    else:
        running, process_detail = _plugin_process_detail(record)
        connectors = []
    print(f"Managed Agent: {args.plugin_id}")
    print(f"process: {process_detail}")
    result = 0 if running else 1
    for declared_agent in record["inventory"]["agents"]:
        agent_id = declared_agent["id"]
        if managed_protocol:
            connector = next(
                (
                    item
                    for item in connectors
                    if item.get("agent_id") == agent_id and not item.get("revoked")
                ),
                None,
            )
            state = (
                "online"
                if connector is not None and connector.get("session_active")
                else "unavailable"
            )
        else:
            node = _load_node(state_dir)
            try:
                agent = _http_json(
                    f"{node['core_url']}/api/agents/{agent_id}", timeout=1
                )
                state = agent.get("agent_state", "unknown")
            except UserError:
                state = "not registered"
        print(f"agent {agent_id}: {state}")
        if state != "online":
            result = 1
    return result


def command_plugin_start(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    _start_plugin(state_dir, args.plugin_id)
    return 0


def command_plugin_stop(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    _stop_plugin(state_dir, args.plugin_id)
    return 0


def command_plugin_logs(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    _plugin_record(state_dir, args.plugin_id)
    path = state_dir / "logs" / f"{args.plugin_id}.log"
    if not path.exists():
        print("No logs yet.")
        return 0
    lines = path.read_text(errors="replace").splitlines()[-args.lines :]
    print("\n".join(lines))
    return 0


def command_plugin_remove(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    _load_node(state_dir)
    _prepare_managed_agent_service(args, state_dir)
    state, record = _plugin_record(state_dir, args.plugin_id)
    _stop_plugin(state_dir, args.plugin_id, quiet=True)
    state = _load_plugins(state_dir)
    record = state["managed_agents"].pop(args.plugin_id)
    package = record["inventory"]["package"]
    if package.get("kind") == "ManagedAgent":
        agent_id = record["inventory"]["agents"][0]["id"]
        connector_id = f"managed-{agent_id}"
        connector = next(
            (
                item
                for item in _agentd_rpc(state_dir, "connector.list")
                if item.get("connector_id") == connector_id
            ),
            None,
        )
        if connector is not None and not connector.get("revoked"):
            _agentd_rpc(state_dir, "connector.revoke", connector_id=connector_id)
        _connector_token_path(state_dir, connector_id).unlink(missing_ok=True)
        _managed_launch_path(state_dir, args.plugin_id).unlink(missing_ok=True)
    target = Path(record["path"])
    _remove_managed_package_tree(target)
    runtime_root = state_dir / "plugin-runtimes" / args.plugin_id
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    _write_json(_plugins_path(state_dir), state)
    _sync_managed_agent_state(state_dir, state)
    print(
        f"Managed Agent {args.plugin_id} and its dependency runtime were removed; "
        "logs and Agent data were preserved."
    )
    return 0


def _plugin_result_step(result: PluginResult) -> dict[str, Any]:
    return {
        "step": "plugin",
        "target": result.host,
        "state": result.state,
        "changed": result.changed,
        "evidence": {
            "status": result.status.to_dict(),
            "commands": [
                {
                    "argv": item.argv,
                    "returncode": item.returncode,
                    "stdout": item.stdout,
                    "stderr": item.stderr,
                    "timed_out": item.timed_out,
                }
                for item in result.evidence
            ],
        },
        "recovery_command": result.recovery_command,
    }


def _status_step(status: Any) -> dict[str, Any]:
    return {
        "step": "plugin",
        "target": status.host,
        "state": "failed" if status.state == "unsupported" else status.state,
        "changed": False,
        "evidence": {"status": status.to_dict()},
        "recovery_command": None,
    }


def _emit_steps(command: str, steps: list[dict[str, Any]], as_json: bool) -> None:
    successful = {
        "absent",
        "available",
        "installed",
        "unchanged",
        "planned",
        "skipped",
        "succeeded",
    }
    ok = all(step["state"] in successful for step in steps)
    document = {
        "schema_version": 1,
        "command": command,
        "ok": ok,
        "changed": any(step["changed"] for step in steps),
        "steps": steps,
    }
    if as_json:
        print(json.dumps(document, indent=2))
        return
    for step in steps:
        line = f"{step['step']} {step['target']}: {step['state']}"
        status = step["evidence"].get("status")
        if isinstance(status, dict) and status.get("detail"):
            line += f" ({status['detail']})"
        print(line)
        if step["recovery_command"]:
            print(f"  recovery: {step['recovery_command']}")


def _confirm_native_plans(plans: list[Any], assume_yes: bool) -> None:
    if not plans:
        return
    print("Plugin installation plan:", file=sys.stderr)
    for plan in plans:
        print(
            f"- {plan.host}: action={plan.action} scope={plan.scope} "
            f"source={plan.source}",
            file=sys.stderr,
        )
        if plan.target_file:
            print(f"  native settings target: {plan.target_file}", file=sys.stderr)
        print(
            f"  grants host capabilities: {', '.join(plan.capabilities)}",
            file=sys.stderr,
        )
        for operation in plan.operations:
            print(f"  command: {' '.join(operation)}", file=sys.stderr)
    if assume_yes:
        return
    if not sys.stdin.isatty():
        raise UserError("Plugin mutation requires a TTY confirmation or --yes")
    if input("Continue? [y/N] ").strip().lower() not in {"y", "yes"}:
        raise UserError("Plugin installation was not approved")


def command_native_plugin(args: argparse.Namespace) -> int:
    hosts = list(HOSTS) if args.plugin_action == "list" else [args.host]
    steps: list[dict[str, Any]] = []
    if args.plugin_action in {"list", "status"}:
        for host in hosts:
            driver = driver_for(host, INSTALL_ROOT, project_root=Path.cwd())
            steps.append(_status_step(driver.status(args.scope)))
        _emit_steps(f"plugin {args.plugin_action}", steps, args.json)
        if any(step["evidence"]["status"]["state"] == "unsupported" for step in steps):
            return 2
        return 1 if any(step["state"] == "unknown" for step in steps) else 0

    driver = driver_for(args.host, INSTALL_ROOT, project_root=Path.cwd())
    before = driver.status(args.scope)
    if before.state in {"unknown", "unsupported"}:
        result = PluginResult(
            args.host,
            "failed",
            False,
            before,
            recovery_command=(
                f"{_command_name()} plugin status {args.host} --scope {args.scope}"
            ),
        )
    elif args.plugin_action == "install" and before.state == "stale":
        result = PluginResult(
            args.host,
            "failed",
            False,
            before,
            recovery_command=(
                f"{_command_name()} plugin repair {args.host} --scope {args.scope}"
            ),
        )
    elif (
        args.plugin_action == "install"
        and before.state == "installed"
        or args.plugin_action == "remove"
        and before.state == "absent"
    ):
        result = driver.apply(args.plugin_action, args.scope)
    else:
        try:
            plan = driver.plan(args.plugin_action, args.scope)
        except (AssetResolutionError, ValueError) as error:
            raise UserError(str(error)) from error
        _confirm_native_plans([plan], args.yes or args.dry_run)
        if args.dry_run:
            result = PluginResult(args.host, "planned", False, before)
        else:
            result = driver.apply(args.plugin_action, args.scope)
    steps.append(_plugin_result_step(result))
    _emit_steps(f"plugin {args.plugin_action}", steps, args.json)
    if result.exit_code == 130:
        return 130
    if result.status.state == "unsupported":
        return 2
    return 0 if result.ok else 1


def _installation_step(
    step: str,
    target: str,
    state: str,
    changed: bool,
    evidence: dict[str, Any],
    recovery: str | None = None,
) -> dict[str, Any]:
    return {
        "step": step,
        "target": target,
        "state": state,
        "changed": changed,
        "evidence": evidence,
        "recovery_command": recovery,
    }


def _interactive_install_choices(args: argparse.Namespace) -> None:
    if args.create or args.invitation:
        return
    print("This host is not enrolled.", file=sys.stderr)
    choice = input("Create a new Core or join an existing one? [create/join] ").strip()
    if choice == "create":
        args.create = True
    elif choice == "join":
        args.invitation = input("Invitation: ").strip()
    else:
        raise UserError("choose 'create' or 'join'")


def command_install(args: argparse.Namespace) -> int:
    steps: list[dict[str, Any]] = []
    try:
        assets = {
            "agent_packages": str(agent_packages_root(INSTALL_ROOT)),
            "plugins": str(plugins_root(INSTALL_ROOT)),
            "agent_platform": str(agent_platform_root(INSTALL_ROOT)),
        }
    except AssetResolutionError as error:
        raise OperationalError(str(error)) from error
    steps.append(
        _installation_step("distribution", "edgecitadel", "succeeded", False, assets)
    )

    state_dir = _state_dir(args.state_dir)
    try:
        node = _load_node(state_dir)
        steps.append(
            _installation_step(
                "enrollment",
                str(node.get("agent_id", "host")),
                "unchanged",
                False,
                {"mode": node["mode"]},
            )
        )
    except UserError:
        if not args.create and not args.invitation:
            if args.json or not sys.stdin.isatty():
                raise UserError(
                    "an unenrolled host requires --create or --join <invitation>"
                )
            _interactive_install_choices(args)
        if args.dry_run:
            mode = "core" if args.create else "edge"
            steps.append(
                _installation_step("enrollment", mode, "planned", False, {"mode": mode})
            )
            node = None
        else:
            captured = StringIO()
            with redirect_stdout(captured):
                if args.create:
                    command_create(
                        argparse.Namespace(
                            host=args.host,
                            state_dir=args.state_dir,
                            no_start=False,
                            timeout=120,
                        )
                    )
                else:
                    command_join(
                        argparse.Namespace(
                            invitation=args.invitation,
                            state_dir=args.state_dir,
                            messaging_mode="single-client",
                        )
                    )
            node = _load_node(state_dir)
            steps.append(
                _installation_step(
                    "enrollment",
                    str(node["agent_id"]),
                    "succeeded",
                    True,
                    {"mode": node["mode"], "output": captured.getvalue().strip()},
                )
            )

    if args.dry_run:
        steps.append(
            _installation_step(
                "service", "agentd", "planned", False, {"action": "start"}
            )
        )
    else:
        running, _ = _agentd_process_detail(state_dir)
        try:
            observation = _start_agentd(state_dir)
        except UserError as error:
            raise OperationalError(str(error)) from error
        steps.append(
            _installation_step(
                "service",
                "agentd",
                "unchanged" if running else "succeeded",
                not running,
                observation,
                f"{_command_name()} service status",
            )
        )

    selected = list(dict.fromkeys(args.plugins or []))
    if not selected and not args.json and sys.stdin.isatty():
        available = [
            host
            for host in HOSTS
            if driver_for(host, INSTALL_ROOT, project_root=Path.cwd()).detect().state
            == "available"
        ]
        print(
            f"Available Plugin hosts: {', '.join(available) or 'none'}", file=sys.stderr
        )
        raw = input("Plugins to install (comma-separated, blank for none): ").strip()
        selected = [item.strip() for item in raw.split(",") if item.strip()]
    elif not selected and (args.yes or args.json or not sys.stdin.isatty()):
        raise UserError("non-interactive installation requires at least one --plugin")
    invalid = sorted(set(selected) - set(HOSTS))
    if invalid:
        raise UserError(f"unsupported Plugin host: {', '.join(invalid)}")

    planned: list[Any] = []
    drivers: list[Any] = []
    for host in selected:
        driver = driver_for(host, INSTALL_ROOT, project_root=Path.cwd())
        before = driver.status(args.scope)
        if before.state == "absent" and not before.available:
            steps.append(
                _plugin_result_step(
                    PluginResult(
                        host,
                        "skipped",
                        False,
                        before,
                    )
                )
            )
            continue
        if before.state in {"unknown", "unsupported"}:
            steps.append(
                _plugin_result_step(
                    PluginResult(
                        host,
                        "failed",
                        False,
                        before,
                        recovery_command=(
                            f"{_command_name()} plugin status {host} "
                            f"--scope {args.scope}"
                        ),
                    )
                )
            )
            break
        if before.state == "installed":
            steps.append(
                _plugin_result_step(PluginResult(host, "unchanged", False, before))
            )
            continue
        if before.state == "stale":
            steps.append(
                _plugin_result_step(
                    PluginResult(
                        host,
                        "failed",
                        False,
                        before,
                        recovery_command=(
                            f"{_command_name()} plugin repair {host} "
                            f"--scope {args.scope}"
                        ),
                    )
                )
            )
            break
        try:
            planned.append(driver.plan("install", args.scope))
        except (AssetResolutionError, ValueError) as error:
            raise UserError(str(error)) from error
        drivers.append(driver)
    if planned:
        _confirm_native_plans(planned, args.yes or args.dry_run)
    if args.dry_run:
        for plan, driver in zip(planned, drivers, strict=True):
            steps.append(
                _plugin_result_step(
                    PluginResult(plan.host, "planned", False, driver.status(args.scope))
                )
            )
    else:
        for driver in drivers:
            result = driver.apply("install", args.scope)
            steps.append(_plugin_result_step(result))
            if not result.ok:
                break

    if not args.dry_run and node is not None:
        connectors = _agentd_rpc(state_dir, "connector.list")
        for host in selected:
            connector = next(
                (
                    item
                    for item in connectors
                    if item.get("host_type") == host and not item.get("revoked")
                ),
                None,
            )
            active = bool(connector and connector.get("session_active"))
            steps.append(
                _installation_step(
                    "connector",
                    host,
                    "succeeded" if active else "skipped",
                    False,
                    {
                        "session": "active" if active else "inactive",
                        "detail": (
                            "Connector is active"
                            if active
                            else "start a new host session to activate the installed Plugin"
                        ),
                    },
                    None if active else f"{_command_name()} connector list",
                )
            )

    _emit_steps("install", steps, args.json)
    unsupported = any(
        step["evidence"].get("status", {}).get("state") == "unsupported"
        for step in steps
    )
    if unsupported:
        return 2
    failed = any(step["state"] in {"failed", "unknown", "degraded"} for step in steps)
    return 1 if failed else 0


def command_messaging(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    if node["mode"] != "edge" or node.get("messaging_mode") != "nats_leaf":
        raise UserError(
            "messaging lifecycle commands require an Edge in nats_leaf mode"
        )
    try:
        if args.action == "start":
            observation = nats_leaf.start(state_dir)
        elif args.action == "stop":
            nats_leaf.stop(state_dir)
            observation = nats_leaf.observe(state_dir)
        elif args.action == "restart":
            observation = nats_leaf.restart(state_dir)
        else:
            observation = nats_leaf.observe(state_dir)
    except nats_leaf.NatsLeafError as error:
        raise UserError(str(error)) from error
    if args.json:
        print(json.dumps({"messaging_mode": "nats_leaf", **observation}, indent=2))
    else:
        print("Messaging mode: nats_leaf")
        print(f"Local NATS: {observation['state']}")
        print(
            "Leaf connection: "
            + ("connected" if observation["leaf_connected"] else "disconnected")
        )
        print(
            "Local agent messaging: "
            + ("available" if observation["local_ready"] else "unavailable")
        )
        print(
            "Cross-node messaging: "
            + ("available" if observation["leaf_connected"] else "paused")
        )
    if args.action == "stop":
        return 0
    return 0 if observation["local_ready"] else 1


def _expiry_seconds(value: str) -> int:
    seconds = int(value)
    if not 60 <= seconds <= 86400:
        raise argparse.ArgumentTypeError("must be between 60 and 86400 seconds")
    return seconds


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="edgecitadel",
        description="Create, join, and operate an EdgeCitadel deployment.",
    )
    parser.add_argument("--version", action="version", version=f"edgecitadel {VERSION}")
    parser.add_argument("--verbose", action="store_true", help=argparse.SUPPRESS)
    subparsers = parser.add_subparsers(dest="command", required=True)

    install = subparsers.add_parser(
        "install", help="Enroll this host and install native host Plugins"
    )
    enrollment = install.add_mutually_exclusive_group()
    enrollment.add_argument("--create", action="store_true")
    enrollment.add_argument("--join", dest="invitation")
    install.add_argument("--plugin", dest="plugins", action="append", choices=HOSTS)
    install.add_argument("--scope", choices=("user", "project"), default="user")
    install.add_argument("--host", default="localhost", help="Core hostname")
    install.add_argument("--yes", action="store_true")
    install.add_argument("--dry-run", action="store_true")
    install.add_argument("--json", action="store_true")
    install.add_argument("--state-dir", help=argparse.SUPPRESS)
    install.set_defaults(func=command_install)

    create = subparsers.add_parser("create", help="Create or reconcile the first node")
    create.add_argument(
        "--host", default="localhost", help="Reachable hostname for this core"
    )
    create.add_argument("--state-dir", help=argparse.SUPPRESS)
    create.add_argument(
        "--no-start", action="store_true", help="Configure without starting Docker"
    )
    create.add_argument(
        "--timeout", type=int, default=120, help="Readiness timeout in seconds"
    )
    create.set_defaults(func=command_create)

    invite = subparsers.add_parser(
        "invite", help="Create a one-time edge-node invitation"
    )
    invite.add_argument(
        "--node-id",
        "--agent-id",
        dest="agent_id",
        required=True,
        help="Identity of the host being enrolled",
    )
    invite.add_argument(
        "--host", required=True, help="Core hostname reachable by the edge node"
    )
    invite.add_argument("--expires", type=_expiry_seconds, default=900)
    invite.add_argument("--state-dir", help=argparse.SUPPRESS)
    invite.set_defaults(func=command_invite)

    join = subparsers.add_parser("join", help="Join this host to an existing fleet")
    join.add_argument("invitation")
    join.add_argument(
        "--messaging-mode",
        choices=("single-client", "nats_leaf"),
        default="single-client",
        help="Managed Agent messaging topology (default: single-client)",
    )
    join.add_argument("--state-dir", help=argparse.SUPPRESS)
    join.set_defaults(func=command_join)

    doctor = subparsers.add_parser("doctor", help="Check node and fleet readiness")
    doctor.add_argument("--json", action="store_true")
    doctor.add_argument("--state-dir", help=argparse.SUPPRESS)
    doctor.set_defaults(func=command_doctor)

    status = subparsers.add_parser("status", help="Show concise node readiness")
    status.add_argument("--json", action="store_true")
    status.add_argument("--state-dir", help=argparse.SUPPRESS)
    status.set_defaults(func=command_doctor)

    down = subparsers.add_parser("down", help="Stop the core and preserve data")
    down.add_argument("--state-dir", help=argparse.SUPPRESS)
    down.set_defaults(func=command_down)

    service = subparsers.add_parser(
        "service", help="Operate the host-local EdgeCitadel service"
    )
    service.add_argument("action", choices=("start", "stop", "restart", "status"))
    service.add_argument("--json", action="store_true")
    service.add_argument("--state-dir", help=argparse.SUPPRESS)
    service.set_defaults(func=command_service)

    agent = subparsers.add_parser(
        "agent", help="Install and operate EdgeCitadel-managed Agents"
    )
    agent_commands = agent.add_subparsers(dest="agent_command", required=True)
    agent_install = agent_commands.add_parser(
        "install", help="Validate, approve, install, and start a Managed Agent"
    )
    agent_install.add_argument("source")
    agent_install.add_argument(
        "--yes", action="store_true", help="Approve the displayed permissions"
    )
    agent_install.add_argument(
        "--keep-disabled", action="store_true", help="Install without starting"
    )
    agent_install.add_argument("--state-dir", help=argparse.SUPPRESS)
    agent_install.set_defaults(func=command_plugin_install)
    agent_list = agent_commands.add_parser("list", help="List installed Agent Packages")
    agent_list.add_argument("--state-dir", help=argparse.SUPPRESS)
    agent_list.set_defaults(func=command_plugin_list)
    agent_status = agent_commands.add_parser(
        "status", help="Show one Managed Agent runtime"
    )
    agent_status.add_argument("plugin_id")
    agent_status.add_argument("--state-dir", help=argparse.SUPPRESS)
    agent_status.set_defaults(func=command_plugin_status)
    for action, func in (
        ("start", command_plugin_start),
        ("stop", command_plugin_stop),
    ):
        action_parser = agent_commands.add_parser(
            action, help=f"{action.title()} one Managed Agent"
        )
        action_parser.add_argument("plugin_id")
        action_parser.add_argument("--state-dir", help=argparse.SUPPRESS)
        action_parser.set_defaults(func=func)
    agent_logs = agent_commands.add_parser(
        "logs", help="Show recent Managed Agent output"
    )
    agent_logs.add_argument("plugin_id")
    agent_logs.add_argument("--lines", type=int, default=80)
    agent_logs.add_argument("--state-dir", help=argparse.SUPPRESS)
    agent_logs.set_defaults(func=command_plugin_logs)
    agent_remove = agent_commands.add_parser(
        "remove", help="Stop and remove a Managed Agent"
    )
    agent_remove.add_argument("plugin_id")
    agent_remove.add_argument("--state-dir", help=argparse.SUPPRESS)
    agent_remove.set_defaults(func=command_plugin_remove)

    native_plugin = subparsers.add_parser(
        "plugin", help="Install and inspect native host Plugins"
    )
    native_plugin_commands = native_plugin.add_subparsers(
        dest="plugin_action", required=True
    )
    native_plugin_list = native_plugin_commands.add_parser(
        "list", help="List native host Plugin installation state"
    )
    native_plugin_list.add_argument(
        "--scope", choices=("user", "project"), default="user"
    )
    native_plugin_list.add_argument("--json", action="store_true")
    native_plugin_list.set_defaults(func=command_native_plugin, host=None)
    native_plugin_status = native_plugin_commands.add_parser(
        "status", help="Show one native host Plugin installation"
    )
    native_plugin_status.add_argument("host", choices=HOSTS)
    native_plugin_status.add_argument(
        "--scope", choices=("user", "project"), default="user"
    )
    native_plugin_status.add_argument("--json", action="store_true")
    native_plugin_status.set_defaults(func=command_native_plugin)
    for action, help_text in (
        ("install", "Install through the host's native package manager"),
        ("repair", "Re-register the current packaged Plugin source"),
        ("remove", "Remove the Plugin through its native package manager"),
    ):
        action_parser = native_plugin_commands.add_parser(action, help=help_text)
        action_parser.add_argument("host", choices=HOSTS)
        action_parser.add_argument(
            "--scope", choices=("user", "project"), default="user"
        )
        action_parser.add_argument("--yes", action="store_true")
        action_parser.add_argument("--dry-run", action="store_true")
        action_parser.add_argument("--json", action="store_true")
        action_parser.set_defaults(func=command_native_plugin)

    connector = subparsers.add_parser(
        "connector", help="Register and inspect live Plugin sessions"
    )
    connector_commands = connector.add_subparsers(
        dest="connector_action", required=True
    )
    connector_path = connector_commands.add_parser("path", help=argparse.SUPPRESS)
    connector_path.add_argument("host_type", choices=("pi", "claude-code", "codex"))
    connector_path.set_defaults(func=command_connector)
    connector_register = connector_commands.add_parser(
        "register", help="Register a Plugin Connector session"
    )
    connector_register.add_argument("connector_id")
    connector_register.add_argument(
        "--host-type", choices=("pi", "claude-code", "codex"), required=True
    )
    connector_register.add_argument("--agent-id")
    connector_register.add_argument("--state-dir", help=argparse.SUPPRESS)
    connector_register.set_defaults(func=command_connector)
    connector_list = connector_commands.add_parser(
        "list", help="List Plugin Connector sessions"
    )
    connector_list.add_argument("--json", action="store_true")
    connector_list.add_argument("--state-dir", help=argparse.SUPPRESS)
    connector_list.set_defaults(func=command_connector)
    connector_status = connector_commands.add_parser(
        "status", help="Show one Plugin Connector registration and session"
    )
    connector_status.add_argument("connector_id")
    connector_status.add_argument("--json", action="store_true")
    connector_status.add_argument("--state-dir", help=argparse.SUPPRESS)
    connector_status.set_defaults(func=command_connector)
    connector_revoke = connector_commands.add_parser(
        "revoke", help="Revoke a Plugin Connector credential"
    )
    connector_revoke.add_argument("connector_id")
    connector_revoke.add_argument("--state-dir", help=argparse.SUPPRESS)
    connector_revoke.set_defaults(func=command_connector)

    task = subparsers.add_parser("task", help="Inspect local Agent task state")
    task_commands = task.add_subparsers(dest="task_action", required=True)
    task_list = task_commands.add_parser("list", help="List local tasks")
    task_list.add_argument("--connector-id", required=True)
    task_list.add_argument("--pending", action="store_true")
    task_list.add_argument("--state-dir", help=argparse.SUPPRESS)
    task_list.set_defaults(func=command_task)
    task_show = task_commands.add_parser("show", help="Show one local task")
    task_show.add_argument("task_id")
    task_show.add_argument("--connector-id", required=True)
    task_show.add_argument("--state-dir", help=argparse.SUPPRESS)
    task_show.set_defaults(func=command_task)
    task_cancel = task_commands.add_parser("cancel", help="Cancel one local task")
    task_cancel.add_argument("task_id")
    task_cancel.add_argument("--connector-id", required=True)
    task_cancel.add_argument("--reason")
    task_cancel.add_argument("--state-dir", help=argparse.SUPPRESS)
    task_cancel.set_defaults(func=command_task)

    trace = subparsers.add_parser("trace", help="Inspect local metadata-only traces")
    trace_commands = trace.add_subparsers(dest="trace_action", required=True)
    trace_list = trace_commands.add_parser("list", help="List local traces")
    trace_list.add_argument("--connector-id", required=True)
    trace_list.add_argument("--limit", type=int, default=100)
    trace_list.add_argument("--state-dir", help=argparse.SUPPRESS)
    trace_list.set_defaults(func=command_trace)
    trace_show = trace_commands.add_parser("show", help="Show one local trace")
    trace_show.add_argument("trace_id")
    trace_show.add_argument("--connector-id", required=True)
    trace_show.add_argument("--state-dir", help=argparse.SUPPRESS)
    trace_show.set_defaults(func=command_trace)
    trace_purge = trace_commands.add_parser(
        "purge", help="Delete local telemetry without deleting identity or tasks"
    )
    trace_purge.add_argument("--connector-id", required=True)
    trace_purge.add_argument("--before-ms", type=int)
    trace_purge.add_argument("--state-dir", help=argparse.SUPPRESS)
    trace_purge.set_defaults(func=command_trace)

    native_mcp = subparsers.add_parser(
        "native-mcp", help="Run the MCP bridge for a host Plugin"
    )
    native_mcp.add_argument(
        "--host-type", choices=("pi", "claude-code", "codex"), required=True
    )
    native_mcp.add_argument("--connector-id")
    native_mcp.add_argument("--agent-id")
    native_mcp.add_argument("--state-dir", help=argparse.SUPPRESS)
    native_mcp.set_defaults(func=command_native_mcp)

    messaging = subparsers.add_parser(
        "messaging", help="Operate the Edge-local NATS service"
    )
    messaging.add_argument("action", choices=("start", "stop", "restart", "status"))
    messaging.add_argument("--json", action="store_true")
    messaging.add_argument("--state-dir", help=argparse.SUPPRESS)
    messaging.set_defaults(func=command_messaging)
    return parser


def _emit_cli_error(args: argparse.Namespace, message: str) -> None:
    if getattr(args, "json", False):
        command = str(getattr(args, "command", "edgecitadel"))
        action = getattr(args, "plugin_action", None)
        if action:
            command += f" {action}"
        target = getattr(args, "host", None) or "edgecitadel"
        _emit_steps(
            command,
            [
                _installation_step(
                    "plugin" if args.command == "plugin" else "distribution",
                    str(target),
                    "failed",
                    False,
                    {"error": message},
                )
            ],
            True,
        )
    else:
        print(f"error: {message}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except OperationalError as error:
        _emit_cli_error(args, str(error))
        return 1
    except UserError as error:
        _emit_cli_error(args, str(error))
        return 2
    except KeyboardInterrupt:
        _emit_cli_error(args, "operation interrupted")
        return 130
    except Exception as error:  # pragma: no cover - defensive CLI boundary
        if getattr(args, "verbose", False):
            raise
        _emit_cli_error(
            args,
            f"unexpected failure: {type(error).__name__}; "
            "rerun with --verbose for technical detail",
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
