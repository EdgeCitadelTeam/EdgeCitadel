"""Edge-local NATS Leaf configuration, lifecycle, and health probes."""

from __future__ import annotations

import hashlib
import io
import json
import os
import platform
import plistlib
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse


CLIENT_PORT = 4223
MONITOR_PORT = 8223
LEAF_PORT = 7422
SERVICE_LABEL = "com.edgecitadel.nats-leaf"
NATS_SERVER_VERSION = "2.14.6"
NATS_SERVER_RELEASE_URL = (
    f"https://github.com/nats-io/nats-server/releases/download/v{NATS_SERVER_VERSION}"
)
# Published in the official v2.14.6 release's SHA256SUMS file.
NATS_SERVER_CHECKSUMS = {
    ("darwin", "amd64"): (
        "239d4f3314334cc86cb12d7c252cf9b3461364451cc8ec5a39fb4915acd717c1"
    ),
    ("darwin", "arm64"): (
        "291b9aa8c342b3cdcc53d872585bd467de4b5c9acf375066d882eb5412a09d55"
    ),
    ("linux", "amd64"): (
        "61c3d55f69f61ec616b75782250936445f2819e9e5f2ae6159b10a31abd2200c"
    ),
    ("linux", "arm64"): (
        "3ff6e463762db64186a36cf0276dae8320509e995151ad0153ba9c9f67eee3f9"
    ),
}
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
MAX_BINARY_BYTES = 64 * 1024 * 1024
LIFECYCLE_STATES = frozenset(
    {
        "unconfigured",
        "configuring",
        "stopped",
        "starting",
        "local_ready",
        "leaf_connected",
        "degraded",
        "stopping",
        "failed",
    }
)


class NatsLeafError(RuntimeError):
    """Expected local-broker lifecycle failure."""


def _private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)


def _secure_write(path: Path, content: str | bytes) -> None:
    _private_directory(path.parent)
    temporary = path.with_name(path.name + ".tmp")
    if isinstance(content, bytes):
        temporary.write_bytes(content)
    else:
        temporary.write_text(content, encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)
    path.chmod(0o600)


def paths(state_dir: Path) -> dict[str, Path]:
    root = state_dir / "nats_leaf"
    return {
        "root": root,
        "config": root / "nats.conf",
        "credentials": root / "credentials.json",
        "data": root / "data",
        "log": root / "nats.log",
        "pid": root / "nats.pid",
        "lifecycle": root / "lifecycle.json",
        "plist": root / f"{SERVICE_LABEL}.plist",
    }


def domain_for(node_id: str) -> str:
    digest = hashlib.sha256(node_id.encode("utf-8")).hexdigest()[:16]
    return f"edge_{digest}"


def plugin_url() -> str:
    return f"nats://127.0.0.1:{CLIENT_PORT}"


def monitor_url() -> str:
    return f"http://127.0.0.1:{MONITOR_PORT}"


def leaf_endpoint(upstream_nats_url: str) -> str:
    parsed = urlparse(upstream_nats_url)
    if parsed.scheme not in {"nats", "tls"} or not parsed.hostname:
        raise NatsLeafError("upstream NATS URL is invalid")
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{LEAF_PORT}"


def render_config(
    *,
    state_dir: Path,
    node_id: str,
    upstream_nats_url: str,
    local_token: str,
    leaf_username: str,
    leaf_password: str,
) -> str:
    if not all((node_id, local_token, leaf_username, leaf_password)):
        raise NatsLeafError("local NATS configuration values are incomplete")
    value_paths = paths(state_dir)
    remote = (
        "nats-leaf://"
        f"{quote(leaf_username, safe='')}:{quote(leaf_password, safe='')}@"
        f"{leaf_endpoint(upstream_nats_url)}"
    )
    quoted = json.dumps
    return (
        f"server_name: {quoted('edgecitadel-' + node_id)}\n"
        f"listen: 127.0.0.1:{CLIENT_PORT}\n"
        f"http: 127.0.0.1:{MONITOR_PORT}\n"
        f"pid_file: {quoted(str(value_paths['pid']))}\n"
        "\n"
        "jetstream {\n"
        f"  domain: {quoted(domain_for(node_id))}\n"
        f"  store_dir: {quoted(str(value_paths['data']))}\n"
        "  max_mem: 256MB\n"
        "  max_file: 1GB\n"
        "}\n"
        "\n"
        "authorization {\n"
        f"  token: {quoted(local_token)}\n"
        "}\n"
        "\n"
        "leafnodes {\n"
        '  reconnect: "1s"\n'
        "  remotes: [\n"
        "    {\n"
        f"      url: {quoted(remote)}\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )


def _managed_binary(state_dir: Path) -> Path:
    return (
        state_dir
        / "runtime"
        / "nats-server"
        / f"v{NATS_SERVER_VERSION}"
        / "nats-server"
    )


def _platform_asset() -> tuple[str, str, str, str]:
    if sys.platform == "darwin":
        system = "darwin"
    elif sys.platform.startswith("linux"):
        system = "linux"
    else:
        system = ""
    machine = platform.machine().lower()
    architecture = {
        "amd64": "amd64",
        "x86_64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }.get(machine, "")
    checksum = NATS_SERVER_CHECKSUMS.get((system, architecture))
    if not checksum:
        platform_name = f"{sys.platform}/{machine or 'unknown'}"
        raise NatsLeafError(
            f"automatic NATS installation does not support {platform_name}; "
            "install nats-server on PATH or set EDGECITADEL_NATS_SERVER"
        )
    asset_name = f"nats-server-v{NATS_SERVER_VERSION}-{system}-{architecture}.tar.gz"
    return system, architecture, asset_name, checksum


def _download_release_asset(asset_name: str) -> bytes:
    url = f"{NATS_SERVER_RELEASE_URL}/{asset_name}"
    request = urllib.request.Request(
        url, headers={"User-Agent": f"EdgeCitadel/{NATS_SERVER_VERSION}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw_length = response.headers.get("Content-Length")
            try:
                content_length = int(raw_length) if raw_length else None
            except ValueError:
                content_length = None
            if content_length is not None and content_length > MAX_ARCHIVE_BYTES:
                raise NatsLeafError("the NATS release archive is unexpectedly large")
            content = response.read(MAX_ARCHIVE_BYTES + 1)
    except NatsLeafError:
        raise
    except (OSError, urllib.error.URLError) as error:
        raise NatsLeafError(
            "could not download NATS Server; check network access, install "
            "nats-server on PATH, or set EDGECITADEL_NATS_SERVER"
        ) from error
    if len(content) > MAX_ARCHIVE_BYTES:
        raise NatsLeafError("the NATS release archive is unexpectedly large")
    return content


def _binary_from_archive(content: bytes, expected_member: str) -> bytes:
    try:
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            matches = [
                member
                for member in archive.getmembers()
                if member.name == expected_member and member.isfile()
            ]
            if (
                len(matches) != 1
                or matches[0].size <= 0
                or matches[0].size > MAX_BINARY_BYTES
            ):
                raise NatsLeafError("the NATS release archive has an invalid layout")
            extracted = archive.extractfile(matches[0])
            if extracted is None:
                raise NatsLeafError("the NATS release archive has an invalid layout")
            binary = extracted.read(MAX_BINARY_BYTES + 1)
    except NatsLeafError:
        raise
    except (OSError, tarfile.TarError) as error:
        raise NatsLeafError("the NATS release archive could not be read") from error
    if len(binary) > MAX_BINARY_BYTES or len(binary) != matches[0].size:
        raise NatsLeafError("the NATS release archive has an invalid binary")
    return binary


def _verify_managed_binary(binary: Path) -> None:
    try:
        result = subprocess.run(
            [str(binary), "--version"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise NatsLeafError("the managed NATS Server binary could not run") from error
    output = f"{result.stdout} {result.stderr}".split()
    if result.returncode != 0 or f"v{NATS_SERVER_VERSION}" not in output:
        raise NatsLeafError(
            f"the managed NATS Server binary is not version {NATS_SERVER_VERSION}"
        )


def _install_managed_binary(target: Path, binary: bytes) -> None:
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".nats-server-", dir=target.parent
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as output:
            output.write(binary)
        temporary.chmod(0o700)
        _verify_managed_binary(temporary)
        temporary.replace(target)
        target.chmod(0o700)
    except NatsLeafError:
        raise
    except OSError as error:
        raise NatsLeafError(
            f"could not install the managed NATS Server under {target.parent}"
        ) from error
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def _provision_binary(state_dir: Path) -> str:
    system, architecture, asset_name, checksum = _platform_asset()
    target = _managed_binary(state_dir)
    try:
        for directory in (
            state_dir,
            state_dir / "runtime",
            target.parent.parent,
            target.parent,
        ):
            _private_directory(directory)
    except OSError as error:
        raise NatsLeafError(
            f"could not prepare managed NATS Server state under {state_dir}"
        ) from error
    print(
        "No usable nats-server was found; downloading checksum-verified "
        f"NATS Server v{NATS_SERVER_VERSION} for {system}-{architecture}.",
        file=sys.stderr,
    )
    content = _download_release_asset(asset_name)
    actual_checksum = hashlib.sha256(content).hexdigest()
    if actual_checksum != checksum:
        raise NatsLeafError("the NATS release archive failed SHA-256 verification")
    root_name = asset_name.removesuffix(".tar.gz")
    binary = _binary_from_archive(content, f"{root_name}/nats-server")
    _install_managed_binary(target, binary)
    print(f"Installed local NATS at {target}.", file=sys.stderr)
    return str(target.resolve())


def _binary(state_dir: Path) -> str:
    configured = os.environ.get("EDGECITADEL_NATS_SERVER")
    if configured:
        binary = Path(configured).expanduser()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise NatsLeafError(
                "EDGECITADEL_NATS_SERVER must point to an executable file"
            )
        return str(binary.resolve())
    system_binary = shutil.which("nats-server")
    if system_binary:
        return str(Path(system_binary).resolve())
    managed = _managed_binary(state_dir)
    if managed.is_file() and os.access(managed, os.X_OK):
        try:
            _verify_managed_binary(managed)
            return str(managed.resolve())
        except NatsLeafError:
            pass
    return _provision_binary(state_dir)


def _validate_config(binary: str, config_path: Path) -> None:
    result = subprocess.run(
        [binary, "-c", str(config_path), "-t"],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise NatsLeafError("generated local NATS configuration did not validate")


def _port_available(port: int) -> bool:
    candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        candidate.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        candidate.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        candidate.close()


def preflight(*, state_dir: Path, node_id: str, upstream_nats_url: str) -> str:
    binary = _binary(state_dir)
    for port in (CLIENT_PORT, MONITOR_PORT):
        if not _port_available(port):
            raise NatsLeafError(
                f"loopback port {port} is already in use; stop the conflicting service and rerun join"
            )
    content = render_config(
        state_dir=state_dir,
        node_id=node_id,
        upstream_nats_url=upstream_nats_url,
        local_token="preflight-local-token",
        leaf_username="preflight-leaf-user",
        leaf_password="preflight-leaf-password",
    )
    with tempfile.TemporaryDirectory(prefix="edgecitadel-nats-leaf-") as temporary:
        config_path = Path(temporary) / "nats.conf"
        config_path.write_text(content, encoding="utf-8")
        config_path.chmod(0o600)
        _validate_config(binary, config_path)
    return binary


def _write_lifecycle(state_dir: Path, state: str, detail: str) -> None:
    if state not in LIFECYCLE_STATES:
        raise ValueError("invalid nats_leaf lifecycle state")
    _secure_write(
        paths(state_dir)["lifecycle"],
        json.dumps(
            {
                "version": 1,
                "state": state,
                "detail": detail,
                "updated_at": int(time.time()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def _service_mode(state_dir: Path) -> str:
    configured = os.environ.get("EDGECITADEL_NATS_SERVICE_MODE")
    if configured:
        if configured not in {"launchd", "process"}:
            raise NatsLeafError("EDGECITADEL_NATS_SERVICE_MODE is invalid")
        return configured
    default_state = (Path.home() / ".edgecitadel").resolve()
    return (
        "launchd"
        if sys.platform == "darwin" and state_dir.resolve() == default_state
        else "process"
    )


def _render_plist(binary: str, state_dir: Path) -> bytes:
    value_paths = paths(state_dir)
    document: dict[str, Any] = {
        "Label": SERVICE_LABEL,
        "ProgramArguments": [binary, "-c", str(value_paths["config"])],
        "RunAtLoad": True,
        "KeepAlive": {"SuccessfulExit": False},
        "ProcessType": "Background",
        "StandardOutPath": str(value_paths["log"]),
        "StandardErrorPath": str(value_paths["log"]),
    }
    return plistlib.dumps(document, sort_keys=True)


def _launchd_target() -> str:
    return f"gui/{os.getuid()}/{SERVICE_LABEL}"


def _launchd_loaded() -> bool:
    result = subprocess.run(
        ["launchctl", "print", _launchd_target()],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def _start_launchd(binary: str, state_dir: Path) -> None:
    value_paths = paths(state_dir)
    _secure_write(value_paths["plist"], _render_plist(binary, state_dir))
    if _launchd_loaded():
        removed = subprocess.run(
            ["launchctl", "bootout", _launchd_target()],
            check=False,
            capture_output=True,
            text=True,
        )
        if removed.returncode != 0:
            raise NatsLeafError("launchd could not reconcile the local NATS service")
    result = subprocess.run(
        ["launchctl", "bootstrap", f"gui/{os.getuid()}", str(value_paths["plist"])],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise NatsLeafError("launchd could not start the local NATS service")


def _start_process(binary: str, state_dir: Path) -> None:
    value_paths = paths(state_dir)
    _private_directory(value_paths["log"].parent)
    log = value_paths["log"].open("ab")
    try:
        subprocess.Popen(
            [binary, "-c", str(value_paths["config"])],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        log.close()
    value_paths["log"].chmod(0o600)


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_pid(state_dir: Path) -> int | None:
    path = paths(state_dir)["pid"]
    try:
        value = int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError, OSError):
        return None
    return value if value > 0 else None


def _owned_pid(state_dir: Path) -> int | None:
    pid = _read_pid(state_dir)
    if not _pid_running(pid):
        return None
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        check=False,
        capture_output=True,
        text=True,
    )
    command = result.stdout
    if "nats-server" not in command or str(paths(state_dir)["config"]) not in command:
        return None
    return pid


def _http_json(path: str, timeout: float = 1.0) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(monitor_url() + path, timeout=timeout) as response:
            value = json.loads(response.read())
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def observe(state_dir: Path) -> dict[str, Any]:
    value_paths = paths(state_dir)
    pid = _read_pid(state_dir)
    process_running = _pid_running(pid)
    client_ready = False
    try:
        with socket.create_connection(("127.0.0.1", CLIENT_PORT), timeout=0.5):
            client_ready = True
    except OSError:
        pass
    health = _http_json("/healthz")
    jetstream = _http_json("/jsz")
    leaf = _http_json("/leafz")
    leaf_count = leaf.get("leafnodes", 0) if leaf else 0
    leaf_connected = isinstance(leaf_count, int) and leaf_count > 0
    local_ready = bool(process_running and client_ready and health and jetstream)
    if local_ready and leaf_connected:
        state = "leaf_connected"
    elif local_ready:
        state = "degraded"
    elif process_running:
        state = "starting"
    elif value_paths["config"].exists():
        state = "stopped"
    else:
        state = "unconfigured"
    return {
        "state": state,
        "process_running": process_running,
        "client_ready": client_ready,
        "jetstream_ready": bool(jetstream),
        "leaf_connected": leaf_connected,
        "local_ready": local_ready,
        "pid": pid,
    }


def wait_ready(
    state_dir: Path, *, timeout: float = 20.0, require_leaf: bool = True
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = observe(state_dir)
    while time.monotonic() < deadline:
        last = observe(state_dir)
        if last["local_ready"] and (last["leaf_connected"] or not require_leaf):
            _write_lifecycle(
                state_dir,
                "leaf_connected" if last["leaf_connected"] else "local_ready",
                "runtime probes passed",
            )
            return last
        time.sleep(0.2)
    if last["local_ready"] and not last["leaf_connected"]:
        _write_lifecycle(
            state_dir, "degraded", "local messaging ready; Leaf disconnected"
        )
        raise NatsLeafError(
            "local NATS started but the authenticated Leaf connection is unavailable"
        )
    _write_lifecycle(state_dir, "failed", "local NATS readiness timed out")
    raise NatsLeafError("local NATS did not become ready before the startup timeout")


def configure_and_start(
    *,
    state_dir: Path,
    node_id: str,
    upstream_nats_url: str,
    local_token: str,
    leaf_username: str,
    leaf_password: str,
    binary: str | None = None,
) -> dict[str, Any]:
    executable = binary or _binary(state_dir)
    value_paths = paths(state_dir)
    for directory in (state_dir, value_paths["root"], value_paths["data"]):
        _private_directory(directory)
    _write_lifecycle(state_dir, "configuring", "rendering local NATS configuration")
    _secure_write(
        value_paths["credentials"],
        json.dumps(
            {
                "version": 1,
                "leaf_username": leaf_username,
                "leaf_password": leaf_password,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )
    _secure_write(
        value_paths["config"],
        render_config(
            state_dir=state_dir,
            node_id=node_id,
            upstream_nats_url=upstream_nats_url,
            local_token=local_token,
            leaf_username=leaf_username,
            leaf_password=leaf_password,
        ),
    )
    _validate_config(executable, value_paths["config"])
    _write_lifecycle(state_dir, "starting", "starting local NATS service")
    if _service_mode(state_dir) == "launchd":
        _start_launchd(executable, state_dir)
    else:
        _start_process(executable, state_dir)
    return wait_ready(state_dir)


def start(state_dir: Path) -> dict[str, Any]:
    value_paths = paths(state_dir)
    if not value_paths["config"].exists():
        raise NatsLeafError("local NATS is not configured; join with nats_leaf first")
    current = observe(state_dir)
    if current["local_ready"]:
        return current
    executable = _binary(state_dir)
    _validate_config(executable, value_paths["config"])
    _write_lifecycle(state_dir, "starting", "starting local NATS service")
    if _service_mode(state_dir) == "launchd":
        _start_launchd(executable, state_dir)
    else:
        _start_process(executable, state_dir)
    return wait_ready(state_dir, require_leaf=False)


def stop(state_dir: Path) -> None:
    _write_lifecycle(state_dir, "stopping", "stopping local NATS service")
    if _service_mode(state_dir) == "launchd" and _launchd_loaded():
        result = subprocess.run(
            ["launchctl", "bootout", _launchd_target()],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise NatsLeafError("launchd could not stop the local NATS service")
    else:
        pid = _owned_pid(state_dir)
        if pid:
            os.kill(pid, signal.SIGTERM)
            deadline = time.monotonic() + 10
            while _pid_running(pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            if _pid_running(pid):
                os.kill(pid, signal.SIGKILL)
        stale = paths(state_dir)["pid"]
        if stale.exists() and not _pid_running(_read_pid(state_dir)):
            stale.unlink()
    _write_lifecycle(state_dir, "stopped", "local NATS service stopped")


def restart(state_dir: Path) -> dict[str, Any]:
    stop(state_dir)
    return start(state_dir)


def cleanup_failed_join(state_dir: Path) -> None:
    value_paths = paths(state_dir)
    try:
        if value_paths["config"].exists():
            stop(state_dir)
    except (NatsLeafError, OSError):
        pass
    for key in ("config", "credentials", "pid", "plist"):
        try:
            value_paths[key].unlink()
        except FileNotFoundError:
            pass
    if value_paths["data"].exists():
        shutil.rmtree(value_paths["data"])
    _write_lifecycle(
        state_dir, "failed", "join rolled back; create a new invitation and retry"
    )
