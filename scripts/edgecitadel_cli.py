"""Unified newcomer CLI for EdgeCitadel source and packaged deployments."""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
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
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import urlparse

try:
    from . import nats_leaf
except ImportError:  # Executed by the installed scripts/edgecitadel wrapper.
    import nats_leaf  # type: ignore[no-redef]


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
PLACEHOLDERS = {
    "NATS_TOKEN": {"", "change-me", "changeme"},
    "NATS_LEAF_USERNAME": {"", "change-me-leaf-user", "changeme"},
    "NATS_LEAF_PASSWORD": {"", "change-me-leaf-password", "changeme"},
    "OPENCLAW_TOKEN": {"", "change-me-scoped", "changeme"},
    "EDGECITADEL_ADMIN_TOKEN": {"", "change-me-admin", "changeme"},
}


def _command_name() -> str:
    return "edgecitadel" if IS_HOMEBREW or IS_PIP else "./scripts/edgecitadel"


class UserError(RuntimeError):
    """Expected failure that should be shown without a traceback."""


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
    print(f"Next: {_command_name()} plugin install <plugin-path-or-name>")
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

        for plugin_id, record in sorted(_load_plugins(state_dir)["plugins"].items()):
            enabled = record.get("enabled", True) is not False
            if not enabled:
                add_check(
                    f"plugin_{plugin_id}",
                    f"plugin {plugin_id}",
                    True,
                    "disabled",
                )
                for declared_agent in record["inventory"]["agents"]:
                    agent_id = declared_agent["id"]
                    add_check(
                        f"agent_{agent_id}",
                        f"agent {agent_id}",
                        True,
                        "disabled with plugin",
                    )
                continue
            running, process_detail = _plugin_process_detail(record)
            add_check(
                f"plugin_{plugin_id}",
                f"plugin {plugin_id}",
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
                print(f"Plugin broker: {broker}")
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
    return state_dir / PLUGIN_STATE_NAME


def _load_plugins(state_dir: Path) -> dict[str, Any]:
    state = _read_json(_plugins_path(state_dir), {"version": 1, "plugins": {}})
    if state.get("version") != 1 or not isinstance(state.get("plugins"), dict):
        raise UserError(f"plugin state is unsupported: {_plugins_path(state_dir)}")
    return state


def _toolkit_python(state_dir: Path) -> Path:
    managed = os.environ.get("EDGECITADEL_SUPERVISOR_PYTHON")
    if managed:
        python = Path(managed)
        if not python.exists():
            raise UserError(f"Homebrew Supervisor runtime is missing: {python}")
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

    print("Preparing the local Supervisor (first plugin command only)...")
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
            str(INSTALL_ROOT / "plugin-toolkit"),
        ]
    )
    _secure_write(marker, expected)
    return python


def _plugin_python(state_dir: Path, plugin_id: str, record: dict[str, Any]) -> Path:
    """Return an isolated runtime when a plugin declares Python dependencies."""
    runtime = record["inventory"]["runtime"]
    requirements = runtime.get("pythonRequirements")
    if requirements is None:
        return _toolkit_python(state_dir)
    if not isinstance(requirements, str):
        raise UserError(f"plugin {plugin_id} has invalid Python requirements")

    plugin_root = Path(record["path"])
    requirements_path = plugin_root / requirements
    if not requirements_path.is_file():
        raise UserError(f"plugin {plugin_id} is missing its Python requirements")
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
    print(f"Preparing isolated Python runtime for plugin {plugin_id}...")
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
                str(INSTALL_ROOT / "plugin-toolkit"),
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
        raise UserError(f"plugin directory does not exist: {source}")
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
        raise UserError("Supervisor returned invalid plugin inventory") from error
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
    print(f"Plugin: {package['id']} {package['version']}")
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
        raise UserError("installation cancelled; no plugin files were installed")


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


def _timestamp_epoch(value: object) -> float:
    if not isinstance(value, str):
        return 0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0


def _plugin_record(
    state_dir: Path, plugin_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    state = _load_plugins(state_dir)
    record = state["plugins"].get(plugin_id)
    if not isinstance(record, dict):
        raise UserError(f"plugin is not installed: {plugin_id}")
    return state, record


def _plugin_nats_environment(node: dict[str, Any]) -> dict[str, str]:
    environment = {
        "NATS_URL": node["plugin_nats_url"],
        "NATS_TOKEN": node["plugin_nats_token"],
    }
    domain = node.get("jetstream_domain")
    if isinstance(domain, str) and domain:
        environment["NATS_DOMAIN"] = domain
    return environment


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


def _ensure_plugin_inboxes(
    state_dir: Path, node: dict[str, Any], record: dict[str, Any]
) -> None:
    if node.get("messaging_mode") == "nats_leaf":
        try:
            observation = nats_leaf.observe(state_dir)
            if not observation["local_ready"]:
                nats_leaf.start(state_dir)
        except nats_leaf.NatsLeafError as error:
            raise UserError(
                f"Local NATS is unavailable; run '{_command_name()} messaging restart'"
            ) from error
    python = _toolkit_python(state_dir)
    agent_ids = [item["id"] for item in record["inventory"]["agents"]]
    environment = {
        **_declared_plugin_environment(record),
        **_plugin_nats_environment(node),
        "EDGECITADEL_AGENT_IDS": ",".join(agent_ids),
    }
    result = subprocess.run(
        [str(python), "-m", "edgecitadel_supervisor.nats_admin"],
        cwd=INSTALL_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise UserError(
            "destination inbox stream could not be reconciled; "
            f"run '{_command_name()} doctor' and retry"
        )


def _start_plugin(state_dir: Path, plugin_id: str) -> None:
    node = _load_node(state_dir)
    state, record = _plugin_record(state_dir, plugin_id)
    if _pid_running(record.get("pid")):
        if _plugin_process_owned(record):
            print(f"Plugin {plugin_id} is already running (pid {record['pid']}).")
            return
        raise UserError(
            f"plugin {plugin_id} has an unverified live PID {record.get('pid')}; "
            "refusing to start another runtime. Verify that process manually, "
            "terminate it if appropriate, then retry"
        )
    command = record["inventory"]["runtime"]["command"]
    if not isinstance(command, list) or not all(
        isinstance(item, str) for item in command
    ):
        raise UserError(f"plugin {plugin_id} has an invalid runtime command")
    _ensure_plugin_inboxes(state_dir, node, record)
    python = _plugin_python(state_dir, plugin_id, record)
    executable = str(python) if command[0] in {"python", "python3"} else command[0]
    logs_dir = state_dir / "logs"
    _private_directory(logs_dir)
    log_path = logs_dir / f"{plugin_id}.log"
    plugin_state_dir = state_dir / "plugin-state" / plugin_id
    _private_directory(plugin_state_dir)
    environment = {
        **_declared_plugin_environment(record),
        **_plugin_nats_environment(node),
        "EDGECITADEL_NODE_ID": node["agent_id"],
        "EDGECITADEL_PLUGIN_ID": plugin_id,
        "EDGECITADEL_PLUGIN_STATE_DIR": str(plugin_state_dir),
        "EDGECITADEL_SCHEMA_DIR": str(INSTALL_ROOT / "schemas"),
    }
    started_at = time.time()
    restart_policy = record["inventory"]["runtime"].get("restartPolicy", "never")
    runner = INSTALL_ROOT / "scripts" / "plugin_runner.py"
    if not runner.is_file():
        raise UserError("Plugin process runner is missing from the installation")
    with log_path.open("ab") as log_file:
        process = subprocess.Popen(
            [
                sys.executable,
                str(runner),
                "--restart-policy",
                restart_policy,
                "--",
                executable,
                *command[1:],
            ],
            cwd=record["path"],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    time.sleep(0.25)
    if process.poll() is not None:
        raise UserError(f"plugin {plugin_id} exited during startup; inspect {log_path}")
    process_identity = _process_identity(process.pid)
    if process_identity is None:
        os.killpg(process.pid, signal.SIGTERM)
        raise UserError(f"plugin {plugin_id} process identity could not be verified")
    record.update(
        {
            "pid": process.pid,
            "process_identity": process_identity,
            "enabled": True,
            "started_at": started_at,
        }
    )
    _write_json(_plugins_path(state_dir), state)
    agent_ids = [item["id"] for item in record["inventory"]["agents"]]
    deadline = time.monotonic() + record["inventory"]["runtime"]["healthTimeoutSeconds"]
    pending = set(agent_ids)
    while pending and time.monotonic() < deadline:
        if not _pid_running(process.pid):
            _stop_plugin(state_dir, plugin_id, quiet=True)
            raise UserError(
                f"plugin {plugin_id} exited during registration; inspect {log_path}"
            )
        for agent_id in list(pending):
            try:
                agent = _http_json(
                    f"{node['core_url']}/api/agents/{agent_id}", timeout=1
                )
                registered_now = (
                    _timestamp_epoch(agent.get("last_register")) >= started_at
                )
                heartbeat_now = (
                    _timestamp_epoch(agent.get("last_heartbeat")) >= started_at
                )
                if (
                    agent.get("agent_state") == "online"
                    and registered_now
                    and heartbeat_now
                ):
                    pending.remove(agent_id)
            except UserError:
                pass
        if pending:
            time.sleep(0.25)
    if pending:
        _stop_plugin(state_dir, plugin_id, quiet=True)
        raise UserError(
            f"plugin {plugin_id} started but agents did not become visible: "
            f"{', '.join(sorted(pending))}; inspect {log_path}"
        )
    print(
        f"Plugin {plugin_id} started (pid {process.pid}); visible agents: "
        f"{', '.join(agent_ids)}"
    )


def _stop_plugin(state_dir: Path, plugin_id: str, *, quiet: bool = False) -> None:
    state, record = _plugin_record(state_dir, plugin_id)
    pid = record.get("pid")
    if _pid_running(pid):
        if not _plugin_process_owned(record):
            raise UserError(
                f"plugin {plugin_id} has an unverified live PID {pid}; refusing to "
                "signal a process EdgeCitadel does not own"
            )
        assert isinstance(pid, int)
        try:
            process_group = os.getpgid(pid)
        except OSError as error:
            raise UserError(
                f"plugin {plugin_id} process group is unavailable"
            ) from error
        if process_group != pid:
            raise UserError(
                f"plugin {plugin_id} PID {pid} is not its owned process-group leader"
            )
        if not _plugin_process_owned(record):
            raise UserError(
                f"plugin {plugin_id} process identity changed before signaling"
            )
        os.killpg(process_group, signal.SIGTERM)
        deadline = time.monotonic() + 5
        while _pid_running(pid) and time.monotonic() < deadline:
            time.sleep(0.1)
        if _pid_running(pid):
            os.killpg(process_group, signal.SIGKILL)
    record.update({"pid": None, "process_identity": None, "enabled": False})
    _write_json(_plugins_path(state_dir), state)
    if not quiet:
        print(f"Plugin {plugin_id} stopped.")


def _assert_agent_ids_available(
    node: dict[str, Any], inventory: dict[str, Any], plugin_state: dict[str, Any]
) -> None:
    """Reject agent identities already owned by a different local/fleet Plugin."""
    plugin_id = inventory["package"]["id"]
    requested = {agent["id"] for agent in inventory["agents"]}
    for other_plugin_id, record in plugin_state["plugins"].items():
        if other_plugin_id == plugin_id:
            continue
        claimed = {agent["id"] for agent in record["inventory"]["agents"]}
        conflicts = sorted(requested & claimed)
        if conflicts:
            raise UserError(
                f"agent identity already belongs to local plugin {other_plugin_id}: "
                f"{', '.join(conflicts)}"
            )

    fleet = _http_json(f"{node['core_url']}/api/agents", timeout=2)
    if not isinstance(fleet, list):
        raise UserError("Core returned an invalid agent inventory")
    existing_same_plugin = plugin_id in plugin_state["plugins"]
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
    """Stage pip-bundled Plugins without installer-generated bytecode files."""
    bundled_root = (INSTALL_ROOT / "plugins").resolve()
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
    existing = state["plugins"].get(plugin_id)
    if existing and existing.get("path") != str(target):
        raise UserError(
            f"plugin {plugin_id} is already installed; "
            "remove it before changing versions"
        )
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
            f"plugin {plugin_id} {package['version']} is already installed with different content; "
            "publish a new version or remove the installed copy first"
        )
    installed_inventory = _validate_plugin(target, state_dir)
    state["plugins"][plugin_id] = {
        "path": str(target),
        "inventory": installed_inventory,
        "installed_at": existing.get("installed_at") if existing else int(time.time()),
        "enabled": bool(existing and existing.get("enabled")),
        "pid": existing.get("pid") if existing else None,
        "process_identity": existing.get("process_identity") if existing else None,
    }
    _write_json(_plugins_path(state_dir), state)
    print(f"Plugin {plugin_id} installed in the Supervisor-owned store.")
    if not args.keep_disabled:
        _start_plugin(state_dir, plugin_id)
    return 0


def command_plugin_install(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    requested_source = Path(args.source).expanduser()
    bundled_source = INSTALL_ROOT / "plugins" / args.source
    example_source = INSTALL_ROOT / "plugins" / "examples" / args.source
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
    plugins = _load_plugins(state_dir)["plugins"]
    if not plugins:
        print(
            f"No plugins installed. Use: {_command_name()} plugin install <path-or-name>"
        )
        return 0
    for plugin_id, record in sorted(plugins.items()):
        running, _detail = _plugin_process_detail(record)
        agents = ",".join(item["id"] for item in record["inventory"]["agents"])
        print(f"{plugin_id:24} {'running' if running else 'stopped':8} agents={agents}")
    return 0


def command_plugin_status(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    _, record = _plugin_record(state_dir, args.plugin_id)
    running, process_detail = _plugin_process_detail(record)
    print(f"plugin: {args.plugin_id}")
    print(f"process: {process_detail}")
    result = 0 if running else 1
    for declared_agent in record["inventory"]["agents"]:
        agent_id = declared_agent["id"]
        try:
            agent = _http_json(f"{node['core_url']}/api/agents/{agent_id}", timeout=1)
            state = agent.get("agent_state", "unknown")
        except UserError:
            state = "not registered"
        print(f"agent {agent_id}: {state}")
        if state != "online":
            result = 1
    return result


def command_plugin_start(args: argparse.Namespace) -> int:
    _start_plugin(_state_dir(args.state_dir), args.plugin_id)
    return 0


def command_plugin_stop(args: argparse.Namespace) -> int:
    _stop_plugin(_state_dir(args.state_dir), args.plugin_id)
    return 0


def command_plugin_logs(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
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
    state, record = _plugin_record(state_dir, args.plugin_id)
    _stop_plugin(state_dir, args.plugin_id, quiet=True)
    state = _load_plugins(state_dir)
    record = state["plugins"].pop(args.plugin_id)
    target = Path(record["path"])
    if target.exists():
        for path in sorted(target.rglob("*"), reverse=True):
            path.chmod(0o700 if path.is_dir() else 0o600)
        target.chmod(0o700)
        shutil.rmtree(target)
    runtime_root = state_dir / "plugin-runtimes" / args.plugin_id
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    _write_json(_plugins_path(state_dir), state)
    print(
        f"Plugin {args.plugin_id} and its dependency runtime were removed; "
        "logs and Plugin data were preserved."
    )
    return 0


def command_supervisor(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    plugins = _load_plugins(state_dir)["plugins"]
    if args.action == "status":
        return command_plugin_list(args)
    if args.action == "start":
        if node.get("messaging_mode") == "nats_leaf":
            try:
                nats_leaf.start(state_dir)
            except nats_leaf.NatsLeafError as error:
                raise UserError(str(error)) from error
        for plugin_id in sorted(plugins):
            _start_plugin(state_dir, plugin_id)
    else:
        for plugin_id in sorted(plugins):
            _stop_plugin(state_dir, plugin_id)
    if not plugins:
        print("No plugins installed.")
    return 0


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
        help="Plugin messaging topology (default: single-client)",
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

    plugin = subparsers.add_parser("plugin", help="Install and operate agent plugins")
    plugin_commands = plugin.add_subparsers(dest="plugin_command", required=True)
    install = plugin_commands.add_parser(
        "install", help="Validate, approve, install, and start a plugin"
    )
    install.add_argument("source")
    install.add_argument(
        "--yes", action="store_true", help="Approve the displayed permissions"
    )
    install.add_argument(
        "--keep-disabled", action="store_true", help="Install without starting"
    )
    install.add_argument("--state-dir", help=argparse.SUPPRESS)
    install.set_defaults(func=command_plugin_install)
    listing = plugin_commands.add_parser(
        "list", help="List installed plugins and agents"
    )
    listing.add_argument("--state-dir", help=argparse.SUPPRESS)
    listing.set_defaults(func=command_plugin_list)
    plugin_status = plugin_commands.add_parser(
        "status", help="Show one plugin and its agents"
    )
    plugin_status.add_argument("plugin_id")
    plugin_status.add_argument("--state-dir", help=argparse.SUPPRESS)
    plugin_status.set_defaults(func=command_plugin_status)
    for action, func in (
        ("start", command_plugin_start),
        ("stop", command_plugin_stop),
    ):
        action_parser = plugin_commands.add_parser(
            action, help=f"{action.title()} one plugin"
        )
        action_parser.add_argument("plugin_id")
        action_parser.add_argument("--state-dir", help=argparse.SUPPRESS)
        action_parser.set_defaults(func=func)
    logs = plugin_commands.add_parser("logs", help="Show recent plugin output")
    logs.add_argument("plugin_id")
    logs.add_argument("--lines", type=int, default=80)
    logs.add_argument("--state-dir", help=argparse.SUPPRESS)
    logs.set_defaults(func=command_plugin_logs)
    remove = plugin_commands.add_parser("remove", help="Stop and remove a plugin")
    remove.add_argument("plugin_id")
    remove.add_argument("--state-dir", help=argparse.SUPPRESS)
    remove.set_defaults(func=command_plugin_remove)

    supervisor = subparsers.add_parser("supervisor", help="Operate all local plugins")
    supervisor.add_argument("action", choices=("start", "stop", "status"))
    supervisor.add_argument("--state-dir", help=argparse.SUPPRESS)
    supervisor.set_defaults(func=command_supervisor)

    messaging = subparsers.add_parser(
        "messaging", help="Operate the Edge-local NATS service"
    )
    messaging.add_argument("action", choices=("start", "stop", "restart", "status"))
    messaging.add_argument("--json", action="store_true")
    messaging.add_argument("--state-dir", help=argparse.SUPPRESS)
    messaging.set_defaults(func=command_messaging)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except UserError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except Exception as error:  # pragma: no cover - defensive CLI boundary
        if getattr(args, "verbose", False):
            raise
        print(f"error: unexpected failure: {type(error).__name__}", file=sys.stderr)
        print("rerun with --verbose for technical detail", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
