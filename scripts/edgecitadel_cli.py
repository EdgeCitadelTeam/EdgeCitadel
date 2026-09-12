"""Unified newcomer CLI for EdgeCitadel source and packaged deployments."""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import math
import os
import plistlib
import pwd
import re
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
from contextlib import contextmanager, nullcontext, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any, Iterator, Sequence
from urllib.parse import urlparse

try:
    from . import core_network, nats_leaf
    from .installation_assets import (
        AssetResolutionError,
        agent_packages_root,
        agent_runtime_root,
        plugin_source,
        plugins_root,
    )
    from .plugin_installation import HOSTS, PluginResult, driver_for
except ImportError:  # Executed by the installed scripts/edgecitadel wrapper.
    import core_network  # type: ignore[no-redef]
    import nats_leaf  # type: ignore[no-redef]
    from installation_assets import (  # type: ignore[no-redef]
        AssetResolutionError,
        agent_packages_root,
        agent_runtime_root,
        plugin_source,
        plugins_root,
    )
    from plugin_installation import (  # type: ignore[no-redef]
        HOSTS,
        PluginResult,
        driver_for,
    )


VERSION = "0.4.0"
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
CORE_NATS_IMAGE = f"nats:{nats_leaf.NATS_SERVER_VERSION}-alpine"
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
    core_network.atomic_write(path, content)


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


def _render_nats_config(*, mqtt_enabled: bool | None = None) -> None:
    source = INSTALL_ROOT / "nats" / "nats.conf.tpl"
    destination = CORE_RUNTIME_DIR / "nats" / "nats.conf"
    if not source.exists():
        raise UserError("NATS configuration template is missing from the installation")
    content = source.read_text()
    if mqtt_enabled is None:
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
                CORE_NATS_IMAGE,
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
    descriptor = core_network.read_descriptor(CORE_RUNTIME_DIR)
    if descriptor is not None:
        core_network.assert_identity(
            descriptor.get("docker"), core_network.docker_identity()
        )
        return core_network.compose_command(CORE_RUNTIME_DIR, descriptor, *arguments)
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _http_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 5,
    local_admin: bool = False,
) -> Any:
    encoded = None if body is None else json.dumps(body).encode()
    request_headers = {"Accept": "application/json", **(headers or {})}
    if encoded is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url, data=encoded, headers=request_headers, method=method
    )
    try:
        opener = (
            urllib.request.build_opener(
                urllib.request.ProxyHandler({}), _NoRedirect()
            ).open
            if local_admin
            else urllib.request.urlopen
        )
        with opener(request, timeout=timeout) as response:
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
    except OSError as error:
        raise UserError(f"cannot reach EdgeCitadel core at {url}: {error}") from error


def _wait_for_core(core_url: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            status = _http_json(
                f"{core_url}/api/system/status",
                timeout=min(2, max(0.01, deadline - time.monotonic())),
                local_admin=urlparse(core_url).hostname
                in {"127.0.0.1", "localhost", "::1"},
            )
            if status.get("nats_connected") and status.get("jetstream_stream_ok"):
                return
            last_error = "NATS or JetStream is not ready"
        except UserError as error:
            last_error = str(error)
        time.sleep(min(1, max(0, deadline - time.monotonic())))
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
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] not in {1, 2}
    ):
        raise UserError(f"node state is unsupported: {path}")
    if value.get("mode") not in {"core", "edge"}:
        raise UserError(f"node state is unsupported: {path}")
    normalized = dict(value)
    if normalized["mode"] == "core":
        if normalized["version"] != 1 or not all(
            isinstance(normalized.get(key), str) and normalized[key]
            for key in ("core_url", "nats_url", "nats_token")
        ):
            raise UserError(f"Core node state is unsupported: {path}")
        _advertised_urls(normalized["core_url"])
        try:
            broker = urlparse(normalized["nats_url"])
            if (
                broker.scheme not in {"nats", "tls"}
                or not broker.hostname
                or not broker.port
            ):
                raise ValueError("invalid broker endpoint")
        except ValueError as error:
            raise UserError(f"Core node state is unsupported: {path}") from error
        if "core_network" in normalized:
            core_network.validate_policy(normalized["core_network"])
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
    if type(payload["version"]) is not int or payload["version"] != 1:
        raise UserError("invitation version is unsupported")
    try:
        expires_at = float(payload["expires_at"])
    except (TypeError, ValueError) as error:
        raise UserError("invitation expiry is malformed") from error
    if not math.isfinite(expires_at):
        raise UserError("invitation expiry is malformed")
    if expires_at <= time.time():
        print(
            "Invitation appears expired according to this computer's clock; the Core decides whether it is still valid.",
            file=sys.stderr,
        )
    if not all(
        isinstance(payload.get(key), str) and payload[key]
        for key in ("core_url", "nats_url", "token", "agent_id")
    ):
        raise UserError("invitation fields are malformed")
    if urlparse(payload["core_url"]).scheme not in {"http", "https"}:
        raise UserError("invitation core URL is unsupported")
    _advertised_urls(payload["core_url"])
    try:
        broker = urlparse(payload["nats_url"])
        if (
            broker.scheme not in {"nats", "tls"}
            or not broker.hostname
            or not broker.port
            or broker.username is not None
            or broker.password is not None
            or broker.path not in {"", "/"}
            or broker.query
            or broker.fragment
        ):
            raise ValueError("invalid broker endpoint")
    except ValueError as error:
        raise UserError("invitation broker URL is unsupported") from error
    return payload


def _tailscale_binary() -> str:
    binary = shutil.which("tailscale")
    if binary:
        return binary
    if sys.platform == "darwin":
        for candidate in (
            Path("/Applications/Tailscale.app/Contents/MacOS/Tailscale"),
            Path.home() / "Applications/Tailscale.app/Contents/MacOS/Tailscale",
        ):
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
    raise UserError(
        "Tailscale CLI was not found; install and connect Tailscale, then retry"
    )


def _detect_tailscale_ipv4() -> str:
    """Read only this machine's usable address, without logging peer inventory."""
    binary = _tailscale_binary()
    try:
        result = subprocess.run(
            [binary, "status", "--json"],
            timeout=5,
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "TAILSCALE_BE_CLI": "1"},
        )
    except subprocess.TimeoutExpired as error:
        raise UserError(
            "Tailscale status timed out after 5 seconds; retry when it responds"
        ) from error
    except OSError as error:
        raise UserError(
            "Tailscale status could not run; check the local installation"
        ) from error
    if result.returncode:
        raise UserError("Tailscale daemon is unavailable; start Tailscale and retry")
    try:
        status = json.loads(result.stdout)
    except (ValueError, TypeError) as error:
        raise UserError("Tailscale returned malformed status") from error
    if not isinstance(status, dict):
        raise UserError("Tailscale returned malformed status")
    backend = status.get("BackendState")
    if backend in {"NeedsLogin", "NeedsMachineAuth"}:
        raise UserError(
            "Tailscale requires sign-in or machine authorization; finish setup and retry"
        )
    if backend != "Running":
        raise UserError("Tailscale is stopped or not ready; connect it and retry")
    record = status.get("Self")
    if not isinstance(record, dict) or record.get("Online") is not True:
        raise UserError(
            "Tailscale has no online self record; connect this machine and retry"
        )
    addresses = record.get("TailscaleIPs")
    if not isinstance(addresses, list):
        raise UserError("Tailscale returned malformed self addresses")
    ipv4: set[str] = set()
    ipv6 = False
    for value in addresses:
        if not isinstance(value, str):
            raise UserError("Tailscale returned malformed self addresses")
        try:
            address = ipaddress.ip_address(value)
        except ValueError as error:
            raise UserError("Tailscale returned malformed self addresses") from error
        if address.version == 4 and address in ipaddress.ip_network("100.64.0.0/10"):
            ipv4.add(str(address))
        elif address.version == 6:
            ipv6 = True
    if len(ipv4) > 1:
        raise UserError(
            "Tailscale returned multiple self IPv4 addresses; resolve the ambiguity before retrying"
        )
    if not ipv4:
        if ipv6:
            raise UserError(
                "IPv6-only Tailscale is not supported by guided setup; a self IPv4 address is required"
            )
        raise UserError(
            "Tailscale has no usable self IPv4 address; connect this machine and retry"
        )
    return next(iter(ipv4))


def _advertised_urls(host: str) -> tuple[str, str]:
    """Return Core and NATS URLs with a valid bracketed IPv6 authority."""
    if (
        not isinstance(host, str)
        or not host
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in host
        )
    ):
        raise UserError("advertised host is invalid")
    value = host
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
    try:
        parsed = urlparse(core_url)
        port = parsed.port
    except ValueError as error:
        raise UserError(
            "advertised IPv6 hosts with a scheme must use brackets"
        ) from error
    if (
        not parsed.hostname
        or parsed.scheme not in {"http", "https"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or "?" in core_url
        or "#" in core_url
        or port == 0
        or "%" in parsed.hostname
    ):
        raise UserError("advertised host is invalid")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        hostname = parsed.hostname.rstrip(".")
        labels = hostname.split(".")
        if len(hostname) > 253 or not all(
            re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?", label)
            for label in labels
        ):
            raise UserError("advertised hostname is invalid")
        broker_authority = parsed.hostname.lower()
    else:
        effective_address = getattr(address, "ipv4_mapped", None) or address
        if (
            effective_address.is_unspecified
            or effective_address.is_multicast
            or effective_address.is_link_local
        ):
            raise UserError("advertised address must be a usable unicast address")
        broker_authority = f"[{address}]" if address.version == 6 else str(address)
    core_url = f"{parsed.scheme}://{broker_authority}"
    if port is not None:
        core_url += f":{port}"
    return core_url, f"nats://{broker_authority}:4222"


def _setup_input(prompt: str) -> str:
    print(prompt, end="", file=sys.stderr, flush=True)
    try:
        return input("").strip()
    except EOFError as error:
        raise KeyboardInterrupt from error


def _setup_interactive(args: argparse.Namespace) -> bool:
    return bool(
        sys.stdin.isatty()
        and not any(getattr(args, key, False) for key in ("yes", "json", "dry_run"))
    )


def _deployment_menu() -> str:
    print(
        "This starts the full Core on this computer and requires Docker.\n\n"
        "How would you like to deploy your NATS server?\n\n"
        "1. Local only\n"
        "   Run a shared server for multiple agents on this computer.\n"
        "   Agents on other computers cannot connect.\n"
        "2. Accessible over Tailscale\n"
        "   Run a server that local and remote agents can connect to.\n"
        "   All computers must first join the same Tailscale network.\n"
        "   We’ll detect and fill in this computer’s Tailscale address.\n"
        "3. Accessible at your own IP or hostname\n"
        "   Run a server on an AWS instance, a LAN machine, or another host.\n"
        "   Enter an address that your agents’ computers can reach.\n"
        "   You’re responsible for configuring network access.\n",
        file=sys.stderr,
    )
    while True:
        selected = _setup_input("Choose 1, 2, or 3: ")
        if selected in {"1", "2", "3"}:
            return {"1": "local", "2": "tailscale", "3": "custom"}[selected]
        print("Please choose 1, 2, or 3.", file=sys.stderr)


def _is_loopback_host(hostname: str) -> bool:
    if hostname.rstrip(".").lower() == "localhost":
        return True
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return bool((getattr(address, "ipv4_mapped", None) or address).is_loopback)


def _resolve_core_setup(
    args: argparse.Namespace, existing: dict[str, Any] | None
) -> dict[str, Any]:
    """Resolve in memory only; caller rechecks state under ownership locks."""
    host = getattr(args, "host", None)
    mode = getattr(args, "network", None)
    bind = getattr(args, "bind_address", None)
    interactive = _setup_interactive(args)
    explicit = host is not None or mode is not None or bind is not None
    if getattr(args, "invitation", None) and explicit:
        raise UserError("Core network options cannot be used with --join")
    if existing is not None:
        if existing.get("mode") != "core":
            raise UserError("this host is already an Edge; it cannot also become Core")
        if not explicit:
            if "core_network" in existing:
                core_network.validate_policy(existing["core_network"])
            return dict(existing)
    if mode not in {None, "local", "tailscale", "custom"}:
        raise UserError("Core network mode is invalid")
    if bind is not None and mode not in {None, "custom"}:
        raise UserError("--bind-address applies only to custom access")
    if mode == "tailscale" and host is not None:
        raise UserError(
            "Tailscale detects this host's address; --host is not supported"
        )
    if mode is None and host is not None:
        core_url, _ = _advertised_urls(host)
        mode = (
            "local"
            if _is_loopback_host(urlparse(core_url).hostname or "")
            else "custom"
        )
    if mode is None and bind is not None:
        raise UserError(
            "--bind-address requires --network custom or an explicit remote --host"
        )
    guided = mode is None and interactive
    if mode is None:
        mode = _deployment_menu() if interactive else "local"
    while mode == "tailscale":
        try:
            host = _detect_tailscale_ipv4()
            print(f"This computer's Tailscale address: {host}", file=sys.stderr)
            bind = host
            break
        except UserError as error:
            if not interactive:
                raise
            print(str(error), file=sys.stderr)
            choice = _setup_input(
                "Retry detection or return to menu? [retry/menu] "
            ).lower()
            if choice == "menu":
                mode = _deployment_menu()
                guided = True
            elif choice != "retry":
                print("Please choose retry or menu.", file=sys.stderr)
    if mode == "local":
        if bind is not None:
            raise UserError("Local access cannot use --bind-address")
        host = host or "127.0.0.1"
        core_url, nats_url = _advertised_urls(host)
        if not _is_loopback_host(urlparse(core_url).hostname or ""):
            raise UserError("Local access requires a loopback --host")
    else:
        if not host and interactive:
            host = _setup_input("Hostname or IP reachable by your agents: ")
        if not host:
            raise UserError("Custom access requires --host <reachable-host>")
        if guided and ("://" in host or "/" in host):
            raise UserError(
                "Enter a hostname or IP in the guide; advanced API URLs require --host"
            )
        core_url, nats_url = _advertised_urls(host)
        if guided and urlparse(core_url).port is not None:
            raise UserError(
                "Enter a hostname or IP in the guide; advanced API ports require --host"
            )
        hostname = urlparse(core_url).hostname or ""
        if _is_loopback_host(hostname):
            raise UserError("Custom access cannot advertise a loopback address")
        addresses = core_network.resolve_addresses(hostname)
        if any(_is_loopback_host(address) for address in addresses):
            raise UserError("Custom hostname resolves to a loopback address")
        for address in addresses:
            _advertised_urls(address)
        local = core_network.assigned_addresses()
        if bind is None:
            candidates = sorted(addresses & local)
            if len(candidates) == 1:
                bind = candidates[0]
            elif interactive:
                if candidates:
                    print(
                        "Assigned candidates: " + ", ".join(candidates), file=sys.stderr
                    )
                else:
                    print(
                        "The advertised address is not assigned here (or DNS is unresolved). Enter this computer's listening IP; NAT/proxy addresses are not bind addresses.",
                        file=sys.stderr,
                    )
                bind = _setup_input("Assigned bind IP: ")
            else:
                raise UserError(
                    "Advertised address has no unique assigned bind; supply --bind-address <local-ip>"
                )
        try:
            bind = str(ipaddress.ip_address(bind))
        except ValueError as error:
            raise UserError(
                "--bind-address must be an assigned IP, not a hostname"
            ) from error
        if bind not in local:
            raise UserError("Selected bind address is not assigned to this computer")
        if mode == "custom":
            print(
                "Custom access requires an operator-protected network. This command does not configure TLS, firewall rules, or a public HTTPS proxy.",
                file=sys.stderr,
            )
        if not addresses:
            print(
                "Advertised DNS is unresolved; remote access remains unverified.",
                file=sys.stderr,
            )
        elif len(addresses) > 1:
            print(
                "Advertised DNS has multiple answers; the selected bind is fixed, and remote access remains unverified.",
                file=sys.stderr,
            )
    previous = existing.get("core_network") if existing else None
    generation = previous["generation"] if isinstance(previous, dict) else 0
    mqtt = (
        previous.get("mqtt", False)
        if isinstance(previous, dict)
        else os.environ.get("EC_ENABLE_MQTT", "0") == "1"
    )
    policy = {
        "version": 1,
        "mode": mode,
        "bind_address": bind,
        "generation": generation or 1,
        "mqtt": mqtt,
    }
    core_network.validate_policy(policy)
    candidate = {
        **(existing or {}),
        "core_url": core_url,
        "nats_url": nats_url,
        "core_network": policy,
    }
    changed = existing is not None and any(
        candidate.get(key) != existing.get(key)
        for key in ("core_url", "nats_url", "core_network")
    )
    if changed:
        policy["generation"] = generation + 1
        old = (
            previous.get("mode", "legacy/unknown")
            if isinstance(previous, dict)
            else "legacy/unknown"
        )
        print(
            f"Core access change: {old} ({existing.get('core_url')}) -> {mode} ({core_url}). Existing invitations keep their old addresses; regenerate them for the new endpoint.",
            file=sys.stderr,
        )
        if not getattr(args, "yes", False):
            if not interactive:
                raise UserError(
                    "Changing existing Core access requires explicit options and --yes"
                )
            if _setup_input("Apply this access change? [yes/no] ").lower() != "yes":
                raise KeyboardInterrupt
    return candidate


def _legacy_create(args: argparse.Namespace) -> int:
    core_url, nats_url = _advertised_urls(args.host)
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
    if not args.no_start:
        _validate_core_nats_config(env)

    node = {
        "version": 1,
        "mode": "core",
        "core_url": core_url,
        "nats_url": nats_url,
        "nats_token": env["NATS_TOKEN"],
        "local_core_url": "http://127.0.0.1",
        "plugin_nats_url": "nats://127.0.0.1:4222",
        "plugin_nats_token": env["NATS_TOKEN"],
        "agent_id": "core",
        "created_at": int(time.time()),
    }
    if existing_path.exists():
        existing = _load_node(state_dir)
        node = existing
    else:
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


def command_create(args: argparse.Namespace) -> int:
    timeout = getattr(args, "timeout", 120)
    if not isinstance(timeout, int) or timeout <= 0:
        raise UserError("--timeout must be positive")
    state_dir = _state_dir(args.state_dir).resolve()
    runtime = CORE_RUNTIME_DIR.resolve()
    node_path = state_dir / NODE_STATE_NAME
    existing = _load_node(state_dir) if node_path.exists() else None
    pending = core_network.read_descriptor(runtime, state_dir)
    resume = existing
    if (
        pending
        and pending["phase"] in {"prepared", "applying", "failed", "stopped"}
        and not any(
            getattr(args, field, None) is not None
            for field in ("network", "host", "bind_address")
        )
    ):
        resume = _read_json(runtime / "core-candidate.json", {})
        if (
            resume.get("mode") != "core"
            or core_network.validate_policy(resume.get("core_network"))["generation"]
            != pending["generation"]
        ):
            raise UserError(
                "Core apply recovery state is inconsistent; inspect the saved candidate"
            )
    candidate = _resolve_core_setup(args, resume)
    if "core_network" not in candidate:
        # No-option legacy reruns keep their historic composition and endpoints.
        print(
            "Core access policy is legacy/unknown; select --network explicitly to manage exposure.",
            file=sys.stderr,
        )
        legacy_args = argparse.Namespace(**vars(args))
        legacy_args.host = candidate["core_url"]
        return _legacy_create(legacy_args)
    selected = candidate["core_network"]
    no_start = getattr(args, "no_start", False)
    if not no_start and shutil.which("docker") is None:
        raise UserError(
            "Docker is required to start a Core node; install Docker Desktop/Engine, then rerun create"
        )
    with core_network.lock(runtime / ".core-runtime.lock"):
        current = _load_node(state_dir) if node_path.exists() else None
        if current != existing:
            raise UserError(
                "Core state changed while setup was open; rerun to review the current settings"
            )
        descriptor = core_network.read_descriptor(runtime, state_dir)
        if descriptor is None:
            project = (
                "edgecitadel"
                if IS_PIP or IS_HOMEBREW
                else re.sub(r"[^a-z0-9_-]", "", INSTALL_ROOT.name.lower()).lstrip("-_")
            )
            if not project:
                raise UserError(
                    "Installation directory cannot determine a Compose project name"
                )
            descriptor = {
                "version": 1,
                "owner": str(node_path),
                "runtime": str(runtime),
                "compose_file": str((INSTALL_ROOT / "docker-compose.yml").resolve()),
                "project": project,
                "docker": None,
                "generation": selected["generation"],
                "applied_generation": None,
                "phase": "configured",
            }
        if (
            no_start
            and existing
            and ("core_network" not in existing or descriptor.get("docker") is not None)
        ):
            if candidate != existing:
                raise UserError(
                    "--no-start cannot change an applied or legacy policy; rerun without --no-start"
                )
            print(
                "Core configuration preserved; no runtime checks or Docker operations performed (--no-start)."
            )
            return 0
        identity = None if no_start else core_network.docker_identity()
        discover_source = (
            identity is not None
            and descriptor.get("docker") is None
            and not (IS_PIP or IS_HOMEBREW)
        )
        if identity is not None:
            core_network.assert_identity(descriptor.get("docker"), identity)
            if discover_source:
                descriptor["project"] = core_network.source_project(
                    identity, Path(descriptor["compose_file"]), descriptor["project"]
                )
        with (
            core_network.project_lock(identity, descriptor["project"])
            if identity
            else nullcontext()
        ):
            applying = False
            recreate = descriptor["phase"] in {"applying", "failed"} or (
                descriptor["phase"] == "stopped"
                and descriptor.get("applied_generation") != selected["generation"]
            )
            try:
                if identity is not None:
                    if (
                        discover_source
                        and core_network.source_project(
                            identity,
                            Path(descriptor["compose_file"]),
                            descriptor["project"],
                        )
                        != descriptor["project"]
                    ):
                        raise UserError(
                            "Source Core project changed during setup; retry to review ownership"
                        )
                    descriptor["docker"] = identity
                    core_network.owned_containers(
                        descriptor,
                        allow_legacy=existing is not None
                        and "core_network" not in existing,
                    )
                    # Validate ambient inputs before credentials or policy writes.
                    core_network.compose_command(runtime, descriptor, "config")
                    if (
                        selected["mode"] == "tailscale"
                        and _detect_tailscale_ipv4() != selected["bind_address"]
                    ):
                        raise UserError(
                            "Tailscale self address changed during setup; rerun and review the address"
                        )
                    if (
                        selected["bind_address"]
                        and selected["bind_address"]
                        not in core_network.assigned_addresses()
                    ):
                        raise UserError(
                            "Selected bind address is no longer assigned; restore the interface before retrying"
                        )
                env_before = _read_env()
                if (
                    existing
                    and existing.get("nats_token")
                    and env_before.get("NATS_TOKEN") != existing["nats_token"]
                ):
                    raise UserError(
                        "Core node and runtime credentials disagree; restore the matching local configuration before retrying"
                    )
                if (
                    "EC_ENABLE_MQTT" in os.environ
                    and (os.environ["EC_ENABLE_MQTT"] == "1") != selected["mqtt"]
                ):
                    raise UserError(
                        "EC_ENABLE_MQTT conflicts with the saved Core policy"
                    )
                if identity is not None:
                    core_network.preflight_model(
                        runtime, descriptor, selected, env_before
                    )
                env, changed = _ensure_env()
                for directory in (runtime / "data", runtime / "nats/data"):
                    directory.mkdir(parents=True, exist_ok=True)
                candidate.update(
                    {
                        "version": 1,
                        "mode": "core",
                        "agent_id": "core",
                        "nats_token": env["NATS_TOKEN"],
                        "local_core_url": "http://127.0.0.1",
                        "plugin_nats_url": "nats://127.0.0.1:4222",
                        "plugin_nats_token": env["NATS_TOKEN"],
                        "created_at": candidate.get("created_at", int(time.time())),
                    }
                )
                if (
                    not no_start
                    and existing
                    and (
                        existing.get("plugin_nats_url", existing.get("nats_url")),
                        existing.get("plugin_nats_token", existing.get("nats_token")),
                    )
                    != (candidate["plugin_nats_url"], candidate["plugin_nats_token"])
                ):
                    running, _ = _agentd_process_detail(state_dir)
                    if running:
                        descriptor["agentd_restart_pending"] = True
                descriptor.update(
                    generation=selected["generation"],
                    phase="configured" if no_start else "prepared",
                )
                _write_json(runtime / "core-candidate.json", candidate)
                _write_json(runtime / core_network.DESCRIPTOR, descriptor)
                _render_nats_config(mqtt_enabled=selected["mqtt"])
                _secure_write(
                    runtime / "docker-compose.managed.yml",
                    core_network.render_override(
                        runtime, descriptor["owner"], selected
                    ),
                )
                if no_start:
                    _write_json(node_path, candidate)
                    print(
                        f"Core configured for {selected['mode']} access; not started (--no-start). Runtime identity and remote access are unverified."
                    )
                    return 0
                model = core_network._json_command(
                    core_network.compose_command(
                        runtime, descriptor, "config", "--format", "json"
                    )
                )
                core_network.verify_model(model, selected)
                _validate_core_nats_config(env)
                descriptor["phase"] = "applying"
                _write_json(runtime / core_network.DESCRIPTOR, descriptor)
                applying = True
                apply_arguments = ["up", "--build", "-d"]
                if recreate:
                    # A failed bind can leave a restartable container with
                    # incomplete networking. Reapply its declared configuration.
                    apply_arguments.append("--force-recreate")
                _run(
                    core_network.compose_command(runtime, descriptor, *apply_arguments),
                    cwd=INSTALL_ROOT,
                )
                containers = core_network.owned_containers(descriptor)
                core_network.verify_bindings(
                    containers, selected, context=identity["context"]
                )
                _wait_for_core(candidate["local_core_url"], timeout)
                for port in (4222, 7422):
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=2):
                            pass
                    except OSError as error:
                        raise OperationalError(
                            f"Core local port {port} is not reachable"
                        ) from error
                _write_json(node_path, candidate)
                descriptor.update(
                    phase="ready", applied_generation=selected["generation"]
                )
                _write_json(runtime / core_network.DESCRIPTOR, descriptor)
                applying = False
                if descriptor.get("agentd_restart_pending", False):
                    # Persisted before node commit: retry even if a prior run
                    # saved the endpoint or stopped agentd before interruption.
                    _stop_agentd(state_dir)
                    _start_agentd(state_dir)
                    descriptor["agentd_restart_pending"] = False
                    _write_json(runtime / core_network.DESCRIPTOR, descriptor)
                print(
                    f"Core ready for {selected['mode']} access: {candidate['core_url']}"
                )
                print(
                    "Local API, internal NATS/JetStream, and host client/Leaf ports are ready. Remote access is not yet verified."
                )
                print(
                    f"Secrets {'generated' if changed else 'preserved'}. Next for local agents: {_command_name()} install --create --plugin <host> --yes"
                )
                return 0
            except BaseException as error:
                if applying:
                    # A failed restriction never restores the old, broader map.
                    try:
                        core_network.owned_containers(descriptor, allow_legacy=True)
                        _run(
                            core_network.compose_command(
                                runtime, descriptor, "stop", "nginx", "nats"
                            ),
                            cwd=INSTALL_ROOT,
                        )
                    except Exception:
                        print(
                            "Could not confirm the affected Core services stopped; inspect their bindings before retrying.",
                            file=sys.stderr,
                        )
                    descriptor["phase"] = "failed"
                    _write_json(runtime / core_network.DESCRIPTOR, descriptor)
                    print(
                        f"Core apply failed; data and credentials retained. Retry: {_command_name()} create --state-dir {state_dir}",
                        file=sys.stderr,
                    )
                    if isinstance(error, UserError):
                        raise OperationalError(str(error)) from error
                raise


@contextmanager
def _core_administration(state_dir: Path, node: dict[str, Any]) -> Iterator[str]:
    descriptor = core_network.read_descriptor(CORE_RUNTIME_DIR, state_dir)
    identity = core_network.docker_identity()
    managed = descriptor is not None
    if descriptor is None:
        descriptor = {
            "owner": str((state_dir / NODE_STATE_NAME).resolve()),
            "project": "edgecitadel"
            if IS_PIP or IS_HOMEBREW
            else INSTALL_ROOT.name.lower(),
            "compose_file": str((INSTALL_ROOT / "docker-compose.yml").resolve()),
            "runtime": str(CORE_RUNTIME_DIR.resolve()),
            "docker": identity,
        }
    else:
        core_network.assert_identity(descriptor.get("docker"), identity)
        if descriptor["phase"] != "ready" or descriptor.get(
            "applied_generation"
        ) != node.get("core_network", {}).get("generation"):
            raise UserError(
                "Core is configured but not verified ready; rerun create before inviting an Edge"
            )
    with core_network.project_lock(identity, descriptor["project"]):
        containers = core_network.owned_containers(descriptor, allow_legacy=not managed)
        if managed:
            core_network.verify_bindings(
                containers, node["core_network"], context=identity["context"]
            )
        else:
            nginx = [
                item
                for item in containers
                if item["Config"]["Labels"].get("com.docker.compose.service") == "nginx"
                and item.get("State", {}).get("Running")
            ]
            if len(nginx) != 1 or not any(
                binding["HostPort"] == "80"
                and binding["HostIp"] in {"0.0.0.0", "127.0.0.1"}
                for binding in nginx[0]
                .get("NetworkSettings", {})
                .get("Ports", {})
                .get("80/tcp", [])
                or []
            ):
                raise UserError(
                    "Legacy Core local administration endpoint could not be verified"
                )
        status = _http_json(
            "http://127.0.0.1/api/system/status", timeout=2, local_admin=True
        )
        if (
            not isinstance(status, dict)
            or not status.get("nats_connected")
            or not status.get("jetstream_stream_ok")
        ):
            raise OperationalError("Core local API/NATS is not ready for enrollment")
        yield "http://127.0.0.1"


def command_invite(args: argparse.Namespace) -> int:
    with core_network.lock(CORE_RUNTIME_DIR / ".core-runtime.lock"):
        return _command_invite_locked(args)


def _command_invite_locked(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    node = _load_node(state_dir)
    if node["mode"] != "core":
        raise UserError("only a core node can create invitations")
    if node.get("core_network", {}).get("mode") == "local":
        raise UserError(
            "Local-only Core cannot invite remote hosts; explicitly change --network to tailscale or custom first"
        )
    if getattr(args, "host", None):
        core_url, nats_url = _advertised_urls(args.host)
        if _is_loopback_host(urlparse(core_url).hostname or ""):
            raise UserError("Remote invitations cannot advertise a loopback address")
    else:
        core_url, _ = _advertised_urls(node["core_url"])
        nats_url = node["nats_url"]
    env = _read_env()
    admin_token = env.get("EDGECITADEL_ADMIN_TOKEN", "")
    if not admin_token:
        raise UserError("administrator credential is missing; rerun create")

    with _core_administration(state_dir, node) as local_url:
        response = _http_json(
            f"{local_url}/api/enrollment/invitations",
            method="POST",
            body={"agent_id": args.agent_id, "expires_in_seconds": args.expires},
            headers={"X-EdgeCitadel-Admin-Token": admin_token},
            timeout=2,
            local_admin=True,
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
    print(
        "Single-use invitation (contains a temporary enrollment secret):",
        file=sys.stderr,
    )
    print(invitation)
    print(f"Expires in {args.expires // 60} minute(s).", file=sys.stderr)
    return 0


def _probe_endpoint(url: str, *, port: int | None = None, timeout: float = 2) -> bool:
    try:
        parsed = urlparse(url)
        selected_port = (
            port
            or parsed.port
            or {"http": 80, "https": 443, "nats": 4222, "tls": 4222}.get(parsed.scheme)
        )
        if not parsed.hostname or not selected_port:
            return False
        addresses = core_network.resolve_addresses(parsed.hostname)
        deadline = time.monotonic() + timeout
        for address in sorted(addresses):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                with socket.create_connection(
                    (address, selected_port), timeout=remaining
                ):
                    return True
            except OSError:
                continue
    except (ValueError, core_network.NetworkError):
        pass
    return False


def _join_preflight(invitation: dict[str, Any], mode: str) -> None:
    if not _probe_endpoint(invitation["core_url"]):
        raise OperationalError(
            "Core enrollment API is unreachable; no redemption was attempted. Retry this invitation after restoring access, if still valid."
        )
    port = 7422 if mode == "nats_leaf" else urlparse(invitation["nats_url"]).port
    if not _probe_endpoint(invitation["nats_url"], port=port):
        raise OperationalError(
            f"Core {'Leaf' if mode == 'nats_leaf' else 'client'} port {port} is unreachable; no redemption was attempted. Restore access before retrying this invitation."
        )


def _joined_transport_result(node: dict[str, Any]) -> int:
    endpoint = node["plugin_nats_url"]
    if not _tcp_ready(endpoint, timeout=2):
        print(
            "Enrollment is saved, but the configured broker is unavailable. Restore connectivity and use doctor/service start; do not redeem this invitation again."
        )
        return 1
    print(
        "Configured broker TCP port is reachable. Authenticated agent messaging and remote request/reply are not verified by this check."
    )
    return 0


@contextmanager
def _replace_join_state(
    state_dir: Path, existing: dict[str, Any] | None
) -> Iterator[None]:
    """Keep old fleet credentials and queued work out of the new enrollment."""
    if existing is None:
        yield
        return
    backup_root = state_dir / "enrollment-backups"
    _private_directory(backup_root)
    backup = Path(tempfile.mkdtemp(prefix="previous-", dir=backup_root))
    running, _ = _agentd_process_detail(state_dir)
    leaf = existing.get("messaging_mode") == "nats_leaf"
    inventory = _load_plugins(state_dir)
    enabled = [
        name
        for name, record in inventory["managed_agents"].items()
        if record.get("enabled")
    ]
    names = (NODE_STATE_NAME, "agentd", "connectors", "managed-launch", "nats_leaf")
    moved: list[str] = []
    stopped: list[str] = []
    prepared = False
    service_stopped = False
    leaf_stopped = False
    _write_json(backup / MANAGED_AGENT_STATE_NAME, inventory)
    try:
        # Keep installed packages, pausing their runtimes while credentials change.
        for name in enabled:
            stopped.append(name)
            _stop_plugin(state_dir, name, quiet=True)
        _stop_agentd(state_dir)
        service_stopped = True
        if leaf:
            nats_leaf.stop(state_dir)
            leaf_stopped = True
        for name in names:
            path = state_dir / name
            if path.exists():
                path.rename(backup / name)
                moved.append(name)
        prepared = True
        yield
    except BaseException:
        # Preserve failed replacement files as well as the original credentials.
        failed = backup / "failed-replacement"
        _private_directory(failed)
        for name in names if prepared else moved:
            path = state_dir / name
            if path.exists():
                path.rename(failed / name)
            if name in moved:
                (backup / name).rename(path)
        if leaf_stopped:
            nats_leaf.start(state_dir)
        if service_stopped and running:
            _start_agentd(state_dir)
        for name in stopped:
            _start_plugin(state_dir, name)
        print("Previous local enrollment restored.", file=sys.stderr)
        raise
    print(f"Previous enrollment saved in {backup}.")
    try:
        _start_agentd(state_dir)
        for name in enabled:
            _start_plugin(state_dir, name)
    except (UserError, OSError) as error:
        raise OperationalError(
            "New enrollment is saved, but service activation failed. Run "
            f"'{_command_name()} service restart' and restart any stopped Managed Agents; "
            "do not redeem the invitation again."
        ) from error
    print("Restart your native agent host sessions to reconnect installed Plugins.")


def command_join(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    state_path = state_dir / NODE_STATE_NAME
    requested_mode = getattr(args, "messaging_mode", "single-client")
    existing = _load_node(state_dir) if state_path.exists() else None
    invitation = _invitation_decode(args.invitation)
    invitation_digest = hashlib.sha256(args.invitation.encode()).hexdigest()
    if existing and existing.get("invitation_digest") == invitation_digest:
        if existing.get("messaging_mode") != requested_mode:
            raise UserError("a new invitation is required to change messaging mode")
        print(
            f"This host is already joined as {existing['agent_id']}; no changes made."
        )
        return _joined_transport_result(existing)
    _join_preflight(invitation, requested_mode)
    binary: str | None = None
    if requested_mode == "nats_leaf" and existing is None:
        try:
            binary = nats_leaf.preflight(
                state_dir=state_dir,
                node_id=str(invitation["agent_id"]),
                upstream_nats_url=str(invitation["nats_url"]),
            )
        except nats_leaf.NatsLeafError as error:
            raise UserError(str(error)) from error
    recovery = f"On the Core, create a new invitation with '{_command_name()} invite --node-id {invitation['agent_id']}'. Do not retry the redemption POST."
    try:
        response = _http_json(
            f"{invitation['core_url']}/api/enrollment/redeem",
            method="POST",
            body={"token": invitation["token"], "messaging_mode": requested_mode},
            timeout=2,
        )
    except (UserError, ValueError, OSError) as error:
        cause = error.__cause__
        if isinstance(cause, urllib.error.HTTPError) and cause.code in {
            400,
            401,
            403,
            409,
            410,
        }:
            raise UserError(
                f"Enrollment was rejected; the invitation may be invalid, expired, or already used. {recovery}"
            ) from error
        raise OperationalError(
            f"Enrollment response was lost or unreadable; invitation consumption is uncertain. {recovery}"
        ) from error
    if (
        not isinstance(response, dict)
        or response.get("agent_id") != invitation["agent_id"]
    ):
        raise OperationalError(
            f"Core returned an incomplete enrollment; invitation consumption is uncertain. {recovery}"
        )
    common = {
        "version": 2,
        "mode": "edge",
        "messaging_mode": requested_mode,
        "core_url": invitation["core_url"],
        "upstream_nats_url": invitation["nats_url"],
        "agent_id": response["agent_id"],
        "created_at": int(time.time()),
        "invitation_digest": invitation_digest,
    }
    if requested_mode == "single-client":
        token = response.get("nats_token")
        if not isinstance(token, str) or not token:
            raise OperationalError(
                f"Core returned an incomplete single-client enrollment; invitation consumption is uncertain. {recovery}"
            )
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
        if (
            not isinstance(leaf_username, str)
            or not leaf_username
            or not isinstance(leaf_password, str)
            or not leaf_password
        ):
            raise OperationalError(
                f"Core returned an incomplete nats_leaf enrollment; invitation consumption is uncertain. {recovery}"
            )
    with _replace_join_state(state_dir, existing):
        if requested_mode == "nats_leaf":
            local_token = secrets.token_urlsafe(32)
            try:
                if binary is None:
                    binary = nats_leaf.preflight(
                        state_dir=state_dir,
                        node_id=str(response["agent_id"]),
                        upstream_nats_url=str(invitation["nats_url"]),
                    )
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
                    "nats_leaf enrollment was redeemed but local setup failed; new enrollment "
                    "was not committed. On the Core, create a new invitation with "
                    f"'{_command_name()} invite --node-id {invitation['agent_id']}', then rerun join"
                ) from error
        if requested_mode == "single-client":
            try:
                _private_directory(state_dir)
                _write_json(state_path, node)
            except OSError as error:
                raise OperationalError(
                    "Redemption succeeded but saving local state failed. Inspect the state directory before continuing; do not redeem again blindly. If credentials are missing, request a new invitation on the Core."
                ) from error
    print(f"This host enrolled in EdgeCitadel as {node['agent_id']}.")
    print(f"Messaging mode: {requested_mode}")
    print(f"Next: {_command_name()} agent install <managed-agent-path-or-name>")
    return _joined_transport_result(node)


def _tcp_ready(nats_url: str, timeout: float = 1) -> bool:
    return _probe_endpoint(nats_url, timeout=timeout)


def command_doctor(args: argparse.Namespace) -> int:
    state_dir = _state_dir(args.state_dir)
    checks: list[dict[str, Any]] = []

    def add_check(check_id: str, name: str, ok: bool, detail: str) -> None:
        checks.append({"id": check_id, "name": name, "ok": bool(ok), "detail": detail})

    try:
        node = _load_node(state_dir)
        messaging_mode = node.get("messaging_mode", "core")
        add_check("node_configuration", "node configuration", True, node["mode"])
    except (UserError, core_network.NetworkError) as error:
        node = None
        messaging_mode = "unknown"
        add_check("node_configuration", "node configuration", False, str(error))

    if node:
        managed_core = node["mode"] == "core" and "core_network" in node
        owned_core = True
        api_url = "http://127.0.0.1" if managed_core else node["core_url"]
        if managed_core:
            try:
                with core_network.lock(CORE_RUNTIME_DIR / ".core-runtime.lock"):
                    descriptor = core_network.read_descriptor(
                        CORE_RUNTIME_DIR, state_dir
                    )
                    if (
                        not descriptor
                        or descriptor["phase"] != "ready"
                        or descriptor.get("applied_generation")
                        != node["core_network"]["generation"]
                    ):
                        raise UserError(
                            "Core is configured but its applied generation is not ready; rerun create"
                        )
                    identity = core_network.docker_identity()
                    core_network.assert_identity(descriptor.get("docker"), identity)
                    with core_network.project_lock(identity, descriptor["project"]):
                        core_network.verify_bindings(
                            core_network.owned_containers(descriptor),
                            node["core_network"],
                            context=identity["context"],
                        )
                add_check(
                    "core_runtime",
                    "Owned Core runtime",
                    True,
                    "identity, generation and publications verified",
                )
            except (UserError, core_network.NetworkError) as error:
                owned_core = False
                add_check("core_runtime", "Owned Core runtime", False, str(error))
        core_api_ok = False
        try:
            if not owned_core:
                raise UserError(
                    "not probed: Core runtime identity or applied policy is unverified"
                )
            status = _http_json(
                f"{api_url}/api/system/status", timeout=2, local_admin=managed_core
            )
            core_api_ok = bool(
                status.get("nats_connected") and status.get("jetstream_stream_ok")
            )
            add_check("core_api", "core API", core_api_ok, api_url)
        except UserError as error:
            add_check("core_api", "core API", False, str(error))
        upstream_url = node.get("upstream_nats_url", node.get("nats_url", ""))
        if managed_core:
            upstream_url = "nats://127.0.0.1:4222"
        if messaging_mode == "nats_leaf":
            parsed = urlparse(upstream_url)
            hostname = parsed.hostname or ""
            authority = f"[{hostname}]" if ":" in hostname else hostname
            upstream_url = f"nats://{authority}:7422"
        core_nats_ok = bool(owned_core and upstream_url and _tcp_ready(upstream_url))
        add_check(
            "core_leaf_port" if messaging_mode == "nats_leaf" else "core_nats",
            "Core Leaf TCP port"
            if messaging_mode == "nats_leaf"
            else "Core NATS TCP port",
            core_nats_ok,
            upstream_url if owned_core else "not probed: runtime unverified",
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
                "Local broker readiness",
                local_observation["local_ready"],
                "available" if local_observation["local_ready"] else "unavailable",
            )
            cross_node = bool(local_observation["leaf_connected"] and core_api_ok)
            add_check(
                "cross_node_messaging",
                "Cross-node link",
                cross_node,
                "link connected; authenticated request/reply unverified"
                if cross_node
                else "paused",
            )
        elif node["mode"] == "edge":
            add_check("local_nats_process", "Local NATS", True, "not used")

        if node["mode"] in {"edge", "core"}:
            agentd_running, agentd_detail = _agentd_process_detail(state_dir)
            add_check(
                "edgecitadel_service",
                "EdgeCitadel service",
                agentd_running or node["mode"] == "core",
                agentd_detail
                if agentd_running or node["mode"] == "edge"
                else "not running (optional for server-only Core)",
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
                        f"{api_url}/api/agents/{agent_id}",
                        timeout=1,
                        local_admin=managed_core,
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
    state_dir = _state_dir(args.state_dir)
    descriptor = core_network.read_descriptor(CORE_RUNTIME_DIR, state_dir)
    node = (
        _load_node(state_dir)
        if (state_dir / NODE_STATE_NAME).exists() or descriptor is None
        else None
    )
    if node is not None and node["mode"] != "core":
        raise UserError(
            "down controls the Docker stack and is only valid on a core node"
        )
    if descriptor is not None:
        with core_network.lock(CORE_RUNTIME_DIR / ".core-runtime.lock"):
            descriptor = core_network.read_descriptor(CORE_RUNTIME_DIR, state_dir)
            if descriptor.get("docker") is None:
                raise UserError(
                    "Core has no applied Docker identity; no owned stack can be stopped"
                )
            identity = core_network.docker_identity()
            core_network.assert_identity(descriptor.get("docker"), identity)
            with core_network.project_lock(identity, descriptor["project"]):
                core_network.owned_containers(descriptor)
                _run(
                    core_network.compose_command(CORE_RUNTIME_DIR, descriptor, "down"),
                    cwd=INSTALL_ROOT,
                )
                descriptor["phase"] = "stopped"
                _write_json(CORE_RUNTIME_DIR / core_network.DESCRIPTOR, descriptor)
    else:
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
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}|runtime-copy-v1\n"
    )
    if python.exists() and marker.exists() and marker.read_text() == expected:
        return python

    print("Preparing the local Agent service...", file=sys.stderr)
    source = venv / "runtime-source"
    # An interrupted older copy may retain read-only directory modes, which
    # would otherwise prevent venv --clear from removing its generated files.
    for copied in (source, venv / "schemas"):
        if not copied.is_symlink():
            for directory, _, _ in os.walk(copied):
                Path(directory).chmod(0o700)
    _run([sys.executable, "-m", "venv", "--clear", str(venv)])
    # Editable builds write metadata beside their source, while the runtime
    # loads schemas relative to that source. Keep both in a private writable
    # copy inside this venv; never modify potentially read-only bundled assets.
    shutil.copytree(
        _asset_root(agent_runtime_root),
        source,
        ignore=shutil.ignore_patterns(
            ".venv",
            ".git",
            "build",
            "dist",
            "*.egg-info",
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
        ),
    )
    shutil.copytree(
        INSTALL_ROOT / "schemas",
        venv / "schemas",
        ignore=shutil.ignore_patterns("__pycache__", ".pytest_cache"),
    )
    # copytree preserves read-only Cellar modes. Both build metadata creation
    # and later venv cleanup need writable directories in these private copies.
    for copied in (source, venv / "schemas"):
        for directory, _, _ in os.walk(copied):
            Path(directory).chmod(0o700)
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            "-e",
            str(source),
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


def _systemd_linger_enabled(loginctl: str, user: str) -> bool:
    result = subprocess.run(
        [loginctl, "show-user", user, "--property=Linger", "--value"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0 and result.stdout.strip() == "yes"


def _ensure_agentd_systemd_linger() -> None:
    loginctl = shutil.which("loginctl")
    user = pwd.getpwuid(os.getuid()).pw_name
    recovery = f"sudo loginctl enable-linger {user}"
    if loginctl is None:
        raise UserError(
            "persistent EdgeCitadel user services require loginctl; "
            f"run '{recovery}', then retry"
        )
    if _systemd_linger_enabled(loginctl, user):
        return
    result = subprocess.run(
        [loginctl, "enable-linger", user],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not _systemd_linger_enabled(loginctl, user):
        raise UserError(
            "EdgeCitadel could not enable persistent systemd user services; "
            f"run '{recovery}', then retry"
        )
    print(f"Enabled persistent systemd user services for {user}.", file=sys.stderr)


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
            f"WorkingDirectory={INSTALL_ROOT}",
            "Restart=on-failure",
            "RestartSec=2",
            f"StandardOutput=append:{service_dir / 'agentd.log'}",
            f"StandardError=append:{service_dir / 'agentd.log'}",
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
    request: dict[str, object] = {"operation": operation, "params": params}
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
    uses_systemd = _agentd_uses_systemd()
    if uses_systemd:
        _ensure_agentd_systemd_linger()
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
    elif uses_systemd:
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
                str(_asset_root(agent_runtime_root)),
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
        "outbound_agents": inventory.get("permissions", {})
        .get("messaging", {})
        .get("outboundAgents", []),
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
    print("\nPlugin setup: review the installation plan.", file=sys.stderr)
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
    if input("Install these Plugins? [y/N] ").strip().lower() not in {"y", "yes"}:
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


def _interactive_choice(
    prompt: str, choices: tuple[str, ...], *, default: str | None = None
) -> str:
    while True:
        choice = _setup_input(prompt).lower()
        if not choice and default:
            return default
        if choice in choices:
            return choice
        print(f"Please choose {' or '.join(choices)}.", file=sys.stderr)


def _interactive_install_choices(args: argparse.Namespace) -> None:
    if args.create or args.invitation:
        return
    print("EdgeCitadel guided setup", file=sys.stderr)
    print("\nStep 1: Choose this host's role.", file=sys.stderr)
    print(
        "  join   Connect this host to an existing Core (no Docker needed).",
        file=sys.stderr,
    )
    print("  create Start a new Core on this host (Docker required).", file=sys.stderr)
    choice = _interactive_choice("Join or create? [join/create] ", ("join", "create"))
    if choice == "create":
        args.create = True
        print("\nStep 2: Configure the new Core.", file=sys.stderr)
    else:
        print("\nStep 2: Join the existing Core.", file=sys.stderr)
        print("Paste the one-time invitation created on the Core.", file=sys.stderr)
        args.invitation = input("Invitation: ").strip()
        if not args.invitation:
            raise UserError("an invitation is required to join a Core")
        print("\nStep 3: Choose Edge messaging.", file=sys.stderr)
        print(
            "  single-client Connect directly to Core; simplest and the default.",
            file=sys.stderr,
        )
        print(
            "  nats_leaf    Keep same-host messaging available during Core outages; "
            "EdgeCitadel installs a pinned local NATS when needed.",
            file=sys.stderr,
        )
        args.messaging_mode = _interactive_choice(
            "Messaging mode [single-client/nats_leaf] (default: single-client): ",
            ("single-client", "nats_leaf"),
            default="single-client",
        )


def _interactive_plugin_choices() -> list[str]:
    detected = [
        driver_for(host, INSTALL_ROOT, project_root=Path.cwd()).detect()
        for host in HOSTS
    ]
    available = [status.host for status in detected if status.state == "available"]
    attention = [
        status
        for status in detected
        if status.available and status.state in {"unknown", "unsupported"}
    ]
    print("\nPlugin setup: choose native agent hosts to connect.", file=sys.stderr)
    print(
        "Select only hosts already installed on this machine; blank skips Plugins.",
        file=sys.stderr,
    )
    print(f"Available Plugin hosts: {', '.join(available) or 'none'}", file=sys.stderr)
    if attention:
        print("Plugin hosts needing attention:", file=sys.stderr)
        for status in attention:
            print(f"  {status.host}: {status.detail}", file=sys.stderr)
    raw = input("Plugins to install (comma-separated, blank for none): ").strip()
    return [item.strip() for item in raw.split(",") if item.strip()]


def command_install(args: argparse.Namespace) -> int:
    messaging_mode = getattr(args, "messaging_mode", "single-client")
    network_options = any(
        getattr(args, field, None) is not None
        for field in ("network", "host", "bind_address")
    )
    if getattr(args, "invitation", None) and network_options:
        raise UserError("Core network options cannot be used with --join")
    if args.create and messaging_mode != "single-client":
        raise UserError("--messaging-mode applies only when joining an Edge")
    steps: list[dict[str, Any]] = []
    try:
        assets = {
            "agent_packages": str(agent_packages_root(INSTALL_ROOT)),
            "plugins": str(plugins_root(INSTALL_ROOT)),
            "agent_runtime": str(agent_runtime_root(INSTALL_ROOT)),
        }
    except AssetResolutionError as error:
        raise OperationalError(str(error)) from error
    steps.append(
        _installation_step("distribution", "edgecitadel", "succeeded", False, assets)
    )

    state_dir = _state_dir(args.state_dir)
    try:
        node = _load_node(state_dir)
    except UserError:
        if (state_dir / NODE_STATE_NAME).exists():
            raise  # Never overwrite corrupt or unsupported state.
        node = None
    if node is not None and not args.invitation:
        if args.create:
            if node["mode"] != "core":
                raise UserError("An enrolled Edge cannot also become Core")
            if args.dry_run:
                planned = _resolve_core_setup(args, node)
                steps.append(
                    _installation_step(
                        "core_access",
                        "core",
                        "planned",
                        False,
                        {
                            "core_network": planned.get("core_network"),
                            "runtime": "unverified",
                        },
                    )
                )
            elif network_options or "core_network" in node:
                captured = StringIO()
                with redirect_stdout(captured):
                    command_create(
                        argparse.Namespace(
                            **{**vars(args), "no_start": False, "timeout": 120}
                        )
                    )
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
    else:
        if not args.create and not args.invitation:
            if args.json or args.yes or not sys.stdin.isatty():
                raise UserError(
                    "an unenrolled host requires --create or --join <invitation>"
                )
            _interactive_install_choices(args)
            messaging_mode = getattr(args, "messaging_mode", "single-client")
        if args.dry_run:
            mode = "core" if args.create else "edge"
            evidence = {"mode": mode}
            if mode == "edge":
                evidence["messaging_mode"] = messaging_mode
            else:
                planned = _resolve_core_setup(args, None)
                evidence["core_network"] = planned["core_network"]
                evidence["runtime"] = "unverified"
            steps.append(
                _installation_step("enrollment", mode, "planned", False, evidence)
            )
            node = None
        else:
            captured = StringIO()
            with redirect_stdout(captured):
                if args.create:
                    command_create(
                        argparse.Namespace(
                            host=args.host,
                            network=getattr(args, "network", None),
                            bind_address=getattr(args, "bind_address", None),
                            yes=args.yes,
                            json=args.json,
                            state_dir=args.state_dir,
                            no_start=False,
                            timeout=120,
                        )
                    )
                else:
                    join_result = command_join(
                        argparse.Namespace(
                            invitation=args.invitation,
                            state_dir=args.state_dir,
                            messaging_mode=messaging_mode,
                        )
                    )
                    if join_result:
                        raise OperationalError(captured.getvalue().strip())
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
        selected = _interactive_plugin_choices()
    elif not selected and (args.yes or args.json or not sys.stdin.isatty()):
        raise UserError("non-interactive installation requires at least one --plugin")
    invalid = sorted(set(selected) - set(HOSTS))
    if invalid:
        raise UserError(f"unsupported Plugin host: {', '.join(invalid)}")

    planned: list[Any] = []
    drivers: list[tuple[Any, str]] = []
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
        action = "repair" if before.state == "stale" else "install"
        try:
            planned.append(driver.plan(action, args.scope))
        except (AssetResolutionError, ValueError) as error:
            raise UserError(str(error)) from error
        drivers.append((driver, action))
    if planned:
        _confirm_native_plans(planned, args.yes or args.dry_run)
    if args.dry_run:
        for plan, (driver, _action) in zip(planned, drivers, strict=True):
            steps.append(
                _plugin_result_step(
                    PluginResult(plan.host, "planned", False, driver.status(args.scope))
                )
            )
    else:
        for driver, action in drivers:
            result = driver.apply(action, args.scope)
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
    install.add_argument(
        "--messaging-mode",
        choices=("single-client", "nats_leaf"),
        default="single-client",
        help="Managed Agent messaging topology when joining (default: single-client)",
    )
    install.add_argument("--plugin", dest="plugins", action="append", choices=HOSTS)
    install.add_argument("--scope", choices=("user", "project"), default="user")
    install.add_argument("--host", help="Core hostname")
    install.add_argument("--network", choices=("local", "tailscale", "custom"))
    install.add_argument(
        "--bind-address", help="Assigned listening IP for custom access"
    )
    install.add_argument("--yes", action="store_true")
    install.add_argument("--dry-run", action="store_true")
    install.add_argument("--json", action="store_true")
    install.add_argument("--state-dir", help=argparse.SUPPRESS)
    install.set_defaults(func=command_install)

    create = subparsers.add_parser("create", help="Create or reconcile the first node")
    create.add_argument("--host", help="Reachable hostname for this core")
    create.add_argument("--network", choices=("local", "tailscale", "custom"))
    create.add_argument(
        "--bind-address", help="Assigned listening IP for custom access"
    )
    create.add_argument(
        "--yes", action="store_true", help="Acknowledge an explicit access change"
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
        "--host",
        help="Advanced advertised override (defaults to the saved Core endpoints)",
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
    except core_network.RuntimeUnavailable as error:
        _emit_cli_error(args, str(error))
        return 1
    except (UserError, core_network.NetworkError) as error:
        _emit_cli_error(args, str(error))
        return 2
    except (KeyboardInterrupt, EOFError):
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
