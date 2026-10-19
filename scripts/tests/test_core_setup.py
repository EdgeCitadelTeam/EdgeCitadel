"""The deployment guide's pure resolver and actual terminal behavior."""

from __future__ import annotations

import json
import os
import pty
import subprocess
import sys
import threading
from argparse import Namespace
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from scripts import edgecitadel_cli as cli


def args(**kwargs):
    return Namespace(
        **{
            "host": None,
            "network": None,
            "bind_address": None,
            "yes": False,
            "json": False,
            "dry_run": False,
            **kwargs,
        }
    )


@pytest.fixture(autouse=True)
def deterministic_network(monkeypatch):
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(
        cli.core_network,
        "assigned_addresses",
        lambda: {"127.0.0.1", "100.80.0.1", "192.168.1.2"},
    )
    monkeypatch.setattr(
        cli.core_network,
        "resolve_addresses",
        lambda host: {host} if host[0].isdigit() else {"192.168.1.2"},
    )
    monkeypatch.setattr(cli, "_detect_tailscale_ipv4", lambda: "100.80.0.1")


@pytest.mark.parametrize(
    "options", [{}, {"yes": True}, {"json": True}, {"dry_run": True}]
)
def test_fresh_automation_is_local_without_prompt_or_detection(monkeypatch, options):
    def forbidden(*args):
        pytest.fail("automated local setup prompted or detected Tailscale")

    monkeypatch.setattr(cli, "_setup_input", forbidden)
    monkeypatch.setattr(cli, "_detect_tailscale_ipv4", forbidden)
    result = cli._resolve_core_setup(args(**options), None)
    assert result["core_network"]["mode"] == "local"
    assert result["core_network"]["bind_address"] is None


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1", "::ffff:127.0.0.1"])
def test_compatible_loopback_hosts_resolve_local(host):
    result = cli._resolve_core_setup(args(host=host), None)
    assert result["core_network"]["mode"] == "local"


def test_tailscale_uses_detected_self_for_advertisement_and_binding():
    result = cli._resolve_core_setup(args(network="tailscale"), None)
    assert result["core_url"] == "http://100.80.0.1"
    assert result["core_network"]["bind_address"] == "100.80.0.1"


@pytest.mark.parametrize(
    "options",
    [
        {"network": "local", "host": "core.example"},
        {"network": "local", "bind_address": "192.168.1.2"},
        {"network": "tailscale", "host": "100.80.0.2"},
        {"network": "tailscale", "bind_address": "192.168.1.2"},
        {"network": "custom"},
        {"network": "custom", "host": "::ffff:127.0.0.1"},
        {"bind_address": "192.168.1.2"},
        {"host": "core.example", "bind_address": "0.0.0.0"},
        {"invitation": "ecjoin://test", "network": "local"},
    ],
)
def test_invalid_option_combinations_fail_before_apply(options):
    with pytest.raises((cli.UserError, cli.core_network.NetworkError)):
        cli._resolve_core_setup(args(**options), None)


def test_custom_advanced_api_preserves_port_and_separate_nats_endpoint():
    result = cli._resolve_core_setup(args(host="https://core.example:8443/"), None)
    assert result["core_url"] == "https://core.example:8443"
    assert result["nats_url"] == "nats://core.example:4222"
    assert result["core_network"]["bind_address"] == "192.168.1.2"


def test_dns_ambiguity_requires_explicit_bind_in_automation(monkeypatch):
    monkeypatch.setattr(
        cli.core_network,
        "resolve_addresses",
        lambda host: {"100.80.0.1", "192.168.1.2"},
    )
    with pytest.raises(cli.UserError, match="unique assigned"):
        cli._resolve_core_setup(args(host="core.example"), None)
    result = cli._resolve_core_setup(
        args(host="core.example", bind_address="192.168.1.2"), None
    )
    assert result["core_network"]["bind_address"] == "192.168.1.2"


def test_dns_loopback_cannot_be_advertised_as_remote(monkeypatch):
    monkeypatch.setattr(
        cli.core_network, "resolve_addresses", lambda host: {"::ffff:127.0.0.1"}
    )
    with pytest.raises(cli.UserError, match="resolves to a loopback"):
        cli._resolve_core_setup(
            args(host="core.example", bind_address="192.168.1.2"), None
        )


def test_unresolved_dns_with_explicit_assigned_bind_is_visibly_unverified(
    monkeypatch, capsys
):
    monkeypatch.setattr(cli.core_network, "resolve_addresses", lambda host: set())
    result = cli._resolve_core_setup(
        args(host="core.example", bind_address="192.168.1.2"), None
    )
    assert result["core_network"]["bind_address"] == "192.168.1.2"
    assert "DNS is unresolved" in capsys.readouterr().err


def test_legacy_rerun_is_not_implicitly_converted():
    existing = {
        "version": 1,
        "mode": "core",
        "core_url": "http://localhost",
        "nats_url": "nats://localhost:4222",
        "nats_token": "test-only",
    }
    assert cli._resolve_core_setup(args(), existing) == existing
    with pytest.raises(cli.UserError, match="--yes"):
        cli._resolve_core_setup(args(network="local"), existing)
    converted = cli._resolve_core_setup(args(network="local", yes=True), existing)
    assert converted["core_network"]["generation"] == 1
    assert converted["nats_token"] == existing["nats_token"]


def test_managed_rerun_does_not_redetect_or_change_generation(monkeypatch):
    existing = {
        "mode": "core",
        **cli._resolve_core_setup(args(network="tailscale"), None),
    }

    def forbidden():
        pytest.fail("saved network choice was redetected during resolution")

    monkeypatch.setattr(cli, "_detect_tailscale_ipv4", forbidden)
    assert cli._resolve_core_setup(args(), existing) == existing
    changed = cli._resolve_core_setup(args(network="local", yes=True), existing)
    assert changed["core_network"]["generation"] == 2


@pytest.mark.parametrize(
    "input_bytes,expected",
    [
        (b"\n1\n", "local"),
        (b"2\n", "tailscale"),
        (b"3\ncore.example\n", "custom"),
        (b"\x04", None),
    ],
)
def test_real_pty_guide_all_branches_blank_and_eof(input_bytes, expected):
    root = Path(__file__).resolve().parents[2]
    code = """
import json
from argparse import Namespace
from scripts import edgecitadel_cli as cli
cli._detect_tailscale_ipv4 = lambda: "100.80.0.1"
cli.core_network.assigned_addresses = lambda: {"100.80.0.1", "192.168.1.2"}
cli.core_network.resolve_addresses = lambda host: {host} if host[0].isdigit() else {"192.168.1.2"}
try:
    result = cli._resolve_core_setup(Namespace(), None)
except KeyboardInterrupt:
    raise SystemExit(130)
print(json.dumps(result))
"""
    master, slave = pty.openpty()
    try:
        process = subprocess.Popen(
            [sys.executable, "-c", code],
            cwd=root,
            stdin=slave,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        os.close(slave)
        slave = -1
        os.write(master, input_bytes)
        stdout, stderr = process.communicate(timeout=5)
        assert stderr.count("How would you like to deploy your NATS server?") == 1
        assert "Choose 1, 2, or 3:" in stderr
        if expected is None:
            assert process.returncode == 130
            assert stdout == ""
        else:
            assert process.returncode == 0, stderr
            assert json.loads(stdout)["core_network"]["mode"] == expected
    finally:
        os.close(master)
        if slave >= 0:
            os.close(slave)


@pytest.fixture
def owned_runtime(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    state = tmp_path / "state"
    monkeypatch.setattr(cli, "CORE_RUNTIME_DIR", runtime)
    monkeypatch.setattr(cli.shutil, "which", lambda name: "/test/" + name)
    monkeypatch.setattr(cli, "IS_PIP", True)
    credentials = {"NATS_TOKEN": "test-only", "EDGECITADEL_ADMIN_TOKEN": "test-admin"}
    monkeypatch.setattr(cli, "_read_env", lambda: credentials)
    monkeypatch.setattr(cli, "_ensure_env", lambda: (credentials, False))
    monkeypatch.setattr(cli, "_render_nats_config", lambda **kwargs: None)
    monkeypatch.setattr(cli, "_validate_core_nats_config", lambda env: None)
    monkeypatch.setattr(cli, "_wait_for_core", lambda *args: None)
    monkeypatch.setattr(cli, "_agentd_process_detail", lambda state: (False, "stopped"))
    monkeypatch.setattr(
        cli.socket, "create_connection", lambda *args, **kwargs: nullcontext()
    )
    identity = {
        "context": "test",
        "daemon_id": "test-core-runtime",
        "endpoint": "unix:///test/docker.sock",
    }
    monkeypatch.setattr(cli.core_network, "docker_identity", lambda: identity)
    monkeypatch.setattr(
        cli.core_network, "owned_containers", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        cli.core_network, "verify_bindings", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(cli.core_network, "project_lock", lambda *args: nullcontext())
    monkeypatch.setattr(cli.core_network, "preflight_model", lambda *args: None)

    def model(command):
        candidate = json.loads((runtime / "core-candidate.json").read_text())
        published = cli.core_network.publications(candidate["core_network"])
        return {
            "services": {
                name: {"ports": published.get(name, [])}
                for name in ("nats", "aggregator", "dashboard", "nginx")
            },
            "networks": {"default": {}},
        }

    monkeypatch.setattr(cli.core_network, "_json_command", model)
    calls = []
    monkeypatch.setattr(cli, "_run", lambda command, **kwargs: calls.append(command))
    return runtime, state, calls


def test_managed_create_records_ready_and_preserves_data_on_change(owned_runtime):
    runtime, state, calls = owned_runtime
    assert (
        cli.command_create(args(state_dir=str(state), network="local", no_start=False))
        == 0
    )
    first = json.loads((state / "node.json").read_text())
    sentinel = runtime / "data/keep-me"
    sentinel.write_text("persistent data")
    assert (
        cli.command_create(
            args(state_dir=str(state), network="tailscale", yes=True, no_start=False)
        )
        == 0
    )
    second = json.loads((state / "node.json").read_text())
    descriptor = json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())
    assert first["nats_token"] == second["nats_token"]
    assert first["created_at"] == second["created_at"]
    assert second["core_network"]["generation"] == descriptor["applied_generation"] == 2
    assert descriptor["phase"] == "ready"
    assert sentinel.read_text() == "persistent data"
    assert all(command[-3:] == ["up", "--build", "-d"] for command in calls)


def test_failed_apply_stops_exposure_and_resumes_saved_candidate(
    owned_runtime, monkeypatch
):
    runtime, state, calls = owned_runtime

    def fail_start(command, **kwargs):
        calls.append(command)
        if "up" in command:
            raise cli.UserError("injected partial Compose recreation")

    monkeypatch.setattr(cli, "_run", fail_start)
    with pytest.raises(cli.OperationalError, match="partial Compose"):
        cli.command_create(
            args(state_dir=str(state), network="tailscale", no_start=False)
        )
    assert not (state / "node.json").exists()
    assert calls[-1][-3:] == ["stop", "nginx", "nats"]
    candidate = json.loads((runtime / "core-candidate.json").read_text())
    assert (
        json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())["phase"]
        == "failed"
    )
    monkeypatch.setattr(cli, "_run", lambda command, **kwargs: calls.append(command))
    assert cli.command_create(args(state_dir=str(state), no_start=False)) == 0
    resumed = json.loads((state / "node.json").read_text())
    assert resumed["core_network"] == candidate["core_network"]
    assert resumed["nats_token"] == candidate["nats_token"]
    assert "--force-recreate" in calls[-1]


def test_no_start_cannot_change_applied_core_without_docker(owned_runtime, monkeypatch):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="local", no_start=False))
    before = (state / "node.json").read_bytes()
    count = len(calls)
    monkeypatch.setattr(
        cli.core_network,
        "docker_identity",
        lambda: pytest.fail("no-start queried Docker"),
    )
    with pytest.raises(cli.UserError, match="--no-start cannot change"):
        cli.command_create(
            args(state_dir=str(state), network="tailscale", yes=True, no_start=True)
        )
    assert (state / "node.json").read_bytes() == before
    assert len(calls) == count


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("point", ["candidate", "prepared", "node", "ready"])
@pytest.mark.parametrize("after", [False, True])
def test_process_crash_at_state_commit_boundaries(
    owned_runtime, monkeypatch, existing, point, after
):
    runtime, state, calls = owned_runtime
    if existing:
        cli.command_create(
            args(state_dir=str(state), network="tailscale", no_start=False)
        )
    previous = (state / "node.json").read_bytes() if existing else None
    target = "local" if existing else "tailscale"
    generation = 2 if existing else 1
    original_write = cli._write_json
    child = os.fork()
    if child == 0:

        def write(path, value):
            selected = (
                (point == "candidate" and path.name == "core-candidate.json")
                or (point == "node" and path == state / "node.json")
                or (
                    path.name == cli.core_network.DESCRIPTOR
                    and value.get("phase") == point
                )
            )
            if selected and not after:
                os._exit(73)
            original_write(path, value)
            if selected and after:
                os._exit(73)

        monkeypatch.setattr(cli, "_write_json", write)
        try:
            cli.command_create(
                args(state_dir=str(state), network=target, yes=True, no_start=False)
            )
        finally:
            os._exit(74)
    _, status = os.waitpid(child, 0)
    assert os.waitstatus_to_exitcode(status) == 73
    node_committed = point == "ready" or (point == "node" and after)
    if not node_committed:
        assert (
            (state / "node.json").read_bytes()
            if (state / "node.json").exists()
            else None
        ) == previous
    # Before the prepared descriptor commits, the old policy is authoritative
    # and the operator retries their explicit choice. Afterward, the saved
    # candidate must resume without requiring that choice again.
    prepared_committed = point in {"node", "ready"} or (point == "prepared" and after)
    retry = {} if prepared_committed else {"network": target, "yes": True}
    assert cli.command_create(args(state_dir=str(state), no_start=False, **retry)) == 0
    node = json.loads((state / "node.json").read_text())
    descriptor = json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())
    assert node["core_network"]["mode"] == target
    assert (
        node["core_network"]["generation"]
        == descriptor["applied_generation"]
        == generation
    )
    assert node["nats_token"] == "test-only"
    assert descriptor["phase"] == "ready"


def test_down_after_failed_first_create_preserves_pending_policy(
    owned_runtime, monkeypatch
):
    runtime, state, calls = owned_runtime

    def fail_start(command, **kwargs):
        if "up" in command:
            raise cli.UserError("injected bind race")

    monkeypatch.setattr(cli, "_run", fail_start)
    with pytest.raises(cli.OperationalError, match="bind race"):
        cli.command_create(
            args(state_dir=str(state), network="tailscale", no_start=False)
        )
    assert not (state / "node.json").exists()
    monkeypatch.setattr(cli, "_run", lambda command, **kwargs: calls.append(command))
    assert cli.command_down(args(state_dir=str(state))) == 0
    assert calls[-1][-1] == "down"
    assert not (state / "node.json").exists()
    assert cli.command_create(args(state_dir=str(state), no_start=False)) == 0
    assert (
        json.loads((state / "node.json").read_text())["core_network"]["mode"]
        == "tailscale"
    )


def test_different_state_cannot_claim_same_runtime(owned_runtime):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="local", no_start=True))
    before = (runtime / cli.core_network.DESCRIPTOR).read_bytes()
    with pytest.raises(cli.core_network.NetworkError, match="different --state-dir"):
        cli.command_create(
            args(state_dir=str(state.parent / "other"), network="local", no_start=True)
        )
    assert (runtime / cli.core_network.DESCRIPTOR).read_bytes() == before
    assert calls == []


def test_running_daemon_restarts_when_endpoint_reconciles(owned_runtime, monkeypatch):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="tailscale", no_start=False))
    node = json.loads((state / "node.json").read_text())
    node["plugin_nats_url"] = "nats://100.80.0.1:4222"
    (state / "node.json").write_text(json.dumps(node))
    monkeypatch.setattr(
        cli, "_agentd_process_detail", lambda state: (True, "owned ready daemon")
    )
    restarts = []
    monkeypatch.setattr(cli, "_stop_agentd", lambda state: restarts.append("stop"))
    monkeypatch.setattr(cli, "_start_agentd", lambda state: restarts.append("start"))
    cli.command_create(args(state_dir=str(state), no_start=False))
    assert restarts == ["stop", "start"]


@pytest.mark.parametrize("interruption", ["node_commit", "service_start"])
def test_daemon_reconciliation_survives_interruption(
    owned_runtime, monkeypatch, interruption
):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="tailscale", no_start=False))
    node = json.loads((state / "node.json").read_text())
    node["plugin_nats_url"] = "nats://100.80.0.1:4222"
    cli._write_json(state / "node.json", node)
    running = [True]
    monkeypatch.setattr(
        cli, "_agentd_process_detail", lambda state: (running[0], "owned")
    )
    monkeypatch.setattr(
        cli, "_stop_agentd", lambda state: running.__setitem__(0, False)
    )
    writes = cli._write_json
    interrupted = [False]

    def write(path, value):
        writes(path, value)
        if (
            interruption == "node_commit"
            and path == state / "node.json"
            and not interrupted[0]
        ):
            interrupted[0] = True
            raise KeyboardInterrupt()

    def start(state):
        if interruption == "service_start" and not interrupted[0]:
            interrupted[0] = True
            raise KeyboardInterrupt()
        running[0] = True

    monkeypatch.setattr(cli, "_write_json", write)
    monkeypatch.setattr(cli, "_start_agentd", start)
    with pytest.raises(KeyboardInterrupt):
        cli.command_create(args(state_dir=str(state), no_start=False))
    descriptor = json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())
    assert descriptor["agentd_restart_pending"] is True
    assert cli.command_create(args(state_dir=str(state), no_start=False)) == 0
    assert running[0]
    assert not json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())[
        "agentd_restart_pending"
    ]


def test_source_activation_uses_discovered_project_under_lock(
    owned_runtime, monkeypatch
):
    runtime, state, calls = owned_runtime
    monkeypatch.setattr(cli, "IS_PIP", False)
    monkeypatch.setattr(cli, "IS_HOMEBREW", False)
    discoveries = []
    locks = []
    monkeypatch.setattr(
        cli.core_network,
        "source_project",
        lambda *args: discoveries.append(args) or "original-project",
    )
    monkeypatch.setattr(
        cli.core_network,
        "project_lock",
        lambda identity, project: locks.append(project) or nullcontext(),
    )
    cli.command_create(args(state_dir=str(state), network="local", no_start=False))
    descriptor = json.loads((runtime / cli.core_network.DESCRIPTOR).read_text())
    assert descriptor["project"] == "original-project"
    assert locks == ["original-project"]
    assert len(discoveries) == 2
    assert all(
        command[command.index("--project-name") + 1] == "original-project"
        for command in calls
    )


def test_saved_invitation_endpoints_and_local_admin_are_separate(
    owned_runtime, monkeypatch, capsys
):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="tailscale", no_start=False))
    monkeypatch.setattr(
        cli, "_core_administration", lambda *args: nullcontext("http://127.0.0.1")
    )
    requests = []

    def request(url, **kwargs):
        requests.append((url, kwargs))
        return {"agent_id": "test-edge", "token": "ephemeral", "expires_at": 9999999999}

    monkeypatch.setattr(cli, "_http_json", request)
    capsys.readouterr()
    cli.command_invite(args(state_dir=str(state), agent_id="test-edge", expires=900))
    output = capsys.readouterr().out
    invitation = cli._invitation_decode(output.strip())
    assert output.count("\n") == 1
    assert invitation["core_url"] == "http://100.80.0.1"
    assert requests[0][0] == "http://127.0.0.1/api/enrollment/invitations"
    assert requests[0][1]["local_admin"] is True


def test_local_invitation_rejected_before_admin_request(owned_runtime, monkeypatch):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="local", no_start=True))
    monkeypatch.setattr(
        cli,
        "_core_administration",
        lambda *args: pytest.fail("local mode reached admin"),
    )
    with pytest.raises(cli.UserError, match="Local-only"):
        cli.command_invite(
            args(
                state_dir=str(state),
                host="remote.example",
                agent_id="test-edge",
                expires=900,
            )
        )


def test_local_admin_bypasses_proxy_and_never_follows_redirect(monkeypatch):
    observed = []

    class Destination(BaseHTTPRequestHandler):
        def do_GET(self):
            observed.append("destination-or-proxy")
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    destination = ThreadingHTTPServer(("127.0.0.1", 0), Destination)

    class Origin(Destination):
        def do_GET(self):
            observed.append("owned-origin")
            assert self.headers["X-EdgeCitadel-Admin-Token"] == "test-only-admin"
            self.send_response(302)
            self.send_header(
                "Location", f"http://127.0.0.1:{destination.server_port}/redirect"
            )
            self.end_headers()

    origin = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    servers = [origin, destination]
    threads = [
        threading.Thread(target=server.serve_forever, daemon=True) for server in servers
    ]
    for thread in threads:
        thread.start()
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{destination.server_port}")
    monkeypatch.setenv("http_proxy", f"http://127.0.0.1:{destination.server_port}")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    try:
        with pytest.raises(cli.UserError, match="rejected"):
            cli._http_json(
                f"http://127.0.0.1:{origin.server_port}/",
                headers={"X-EdgeCitadel-Admin-Token": "test-only-admin"},
                timeout=2,
                local_admin=True,
            )
        assert observed == ["owned-origin"]
    finally:
        for server, thread in zip(servers, threads):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


def test_managed_core_doctor_uses_owned_local_endpoints(
    owned_runtime, monkeypatch, capsys
):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="tailscale", no_start=False))
    monkeypatch.setattr(cli, "HOSTS", ())
    observed = []
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda url, **kwargs: observed.append((url, kwargs))
        or {"nats_connected": True, "jetstream_stream_ok": True},
    )
    monkeypatch.setattr(
        cli, "_tcp_ready", lambda url, **kwargs: observed.append((url, kwargs)) or True
    )
    capsys.readouterr()
    cli.command_doctor(Namespace(state_dir=str(state), json=True))
    report = json.loads(capsys.readouterr().out)
    checks = {item["id"]: item for item in report["checks"]}
    assert checks["core_runtime"]["ok"] is True
    assert all("100.80.0.1" not in url for url, _ in observed)
    assert observed[0] == (
        "http://127.0.0.1/api/system/status",
        {"timeout": 2, "local_admin": True},
    )
    assert checks["edgecitadel_service"]["ok"] is True
    assert "optional" in checks["edgecitadel_service"]["detail"]


def test_unapplied_core_doctor_does_not_probe_unowned_services(
    owned_runtime, monkeypatch, capsys
):
    runtime, state, calls = owned_runtime
    cli.command_create(args(state_dir=str(state), network="local", no_start=True))
    monkeypatch.setattr(cli, "HOSTS", ())
    monkeypatch.setattr(
        cli, "_http_json", lambda *args, **kwargs: pytest.fail("unowned API was probed")
    )
    monkeypatch.setattr(
        cli,
        "_tcp_ready",
        lambda *args, **kwargs: pytest.fail("unowned broker was probed"),
    )
    capsys.readouterr()
    assert cli.command_doctor(Namespace(state_dir=str(state), json=True)) == 1
    checks = {
        item["id"]: item for item in json.loads(capsys.readouterr().out)["checks"]
    }
    assert checks["core_runtime"]["ok"] is False
    assert "not probed" in checks["core_api"]["detail"]


def test_rejected_preflight_model_does_not_generate_credentials_or_state(
    owned_runtime, monkeypatch
):
    runtime, state, calls = owned_runtime

    def reject(*args):
        raise cli.core_network.NetworkError("unqualified Compose topology")

    monkeypatch.setattr(cli.core_network, "preflight_model", reject)
    monkeypatch.setattr(
        cli,
        "_ensure_env",
        lambda: pytest.fail("preflight rejection generated credentials"),
    )
    with pytest.raises(cli.core_network.NetworkError, match="unqualified"):
        cli.command_create(args(state_dir=str(state), network="local", no_start=False))
    assert not state.exists()
    assert not (runtime / cli.core_network.DESCRIPTOR).exists()
    assert not (runtime / "core-candidate.json").exists()
    assert not (runtime / ".env").exists()
    assert calls == []
