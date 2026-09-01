from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import edgecitadel_cli as cli


REPO_ROOT = Path(__file__).parents[2]


def test_pip_bundled_plugin_staging_excludes_installer_bytecode(tmp_path, monkeypatch):
    install_root = tmp_path / "share" / "edgecitadel"
    source = install_root / "plugins" / "examples" / "echo"
    (source / "runtime" / "__pycache__").mkdir(parents=True)
    (source / "plugin.yaml").write_text("package: {}\n")
    (source / "runtime" / "__main__.py").write_text("pass\n")
    (source / "runtime" / "__pycache__" / "__main__.pyc").write_bytes(b"cache")
    monkeypatch.setattr(cli, "INSTALL_ROOT", install_root)
    monkeypatch.setattr(cli, "IS_PIP", True)

    with cli._installable_plugin_source(source, tmp_path / "state") as staged:
        staged_root = staged.parent
        assert staged != source
        assert (staged / "plugin.yaml").is_file()
        assert (staged / "runtime" / "__main__.py").is_file()
        assert not (staged / "runtime" / "__pycache__").exists()

    assert not staged_root.exists()


def test_ensure_env_is_idempotent_and_preserves_custom_values(tmp_path, monkeypatch):
    example = tmp_path / ".env.example"
    target = tmp_path / ".env"
    example.write_text(
        "NATS_TOKEN=change-me\n"
        "OPENCLAW_TOKEN=custom-openclaw\n"
        "EDGECITADEL_ADMIN_TOKEN=change-me-admin\n"
        "EC_ENABLE_MQTT=0\n"
    )
    monkeypatch.setattr(cli, "ENV_EXAMPLE_PATH", example)

    first, changed = cli._ensure_env(target)
    first_source = target.read_text()
    second, changed_again = cli._ensure_env(target)

    assert changed is True
    assert changed_again is False
    assert first == second
    assert first["OPENCLAW_TOKEN"] == "custom-openclaw"
    assert first["NATS_TOKEN"].startswith("ec_")
    assert first_source == target.read_text()
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_invitation_round_trip_and_expiry():
    value = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "macmini-agent",
            "expires_at": time.time() + 60,
        }
    )
    assert value.startswith("ecjoin://")
    assert cli._invitation_decode(value)["agent_id"] == "macmini-agent"

    expired = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "macmini-agent",
            "expires_at": time.time() - 1,
        }
    )
    with pytest.raises(cli.UserError, match="expired"):
        cli._invitation_decode(expired)

    malformed_expiry = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "macmini-agent",
            "expires_at": {"not": "a timestamp"},
        }
    )
    with pytest.raises(cli.UserError, match="expiry is malformed"):
        cli._invitation_decode(malformed_expiry)


@pytest.mark.parametrize(
    ("host", "core_url", "nats_url"),
    [
        ("2001:db8::1", "http://[2001:db8::1]", "nats://[2001:db8::1]:4222"),
        (
            "https://[2001:db8::2]:8443/",
            "https://[2001:db8::2]:8443",
            "nats://[2001:db8::2]:4222",
        ),
        ("core.example:8080", "http://core.example:8080", "nats://core.example:4222"),
    ],
)
def test_advertised_urls_normalize_ipv6(host, core_url, nats_url):
    assert cli._advertised_urls(host) == (core_url, nats_url)


def test_advertised_urls_reject_unbracketed_ipv6_with_scheme():
    with pytest.raises(cli.UserError, match="must use brackets"):
        cli._advertised_urls("http://2001:db8::1")


def test_join_writes_restrictive_state_and_is_idempotent(tmp_path, monkeypatch, capsys):
    invitation = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "macmini-agent",
            "expires_at": time.time() + 60,
        }
    )
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {
            "agent_id": "macmini-agent",
            "nats_token": "broker-secret",
        },
    )
    args = Namespace(invitation=invitation, state_dir=str(tmp_path))

    assert cli.command_join(args) == 0
    state_path = tmp_path / "node.json"
    state = json.loads(state_path.read_text())
    assert state["nats_token"] == "broker-secret"
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600

    assert cli.command_join(args) == 0
    assert "already joined" in capsys.readouterr().out


def test_join_does_not_write_partial_state_when_redeem_fails(tmp_path, monkeypatch):
    invitation = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "macmini-agent",
            "expires_at": time.time() + 60,
        }
    )
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(cli.UserError("rejected")),
    )
    args = Namespace(invitation=invitation, state_dir=str(tmp_path))

    with pytest.raises(cli.UserError, match="rejected"):
        cli.command_join(args)
    assert not (tmp_path / "node.json").exists()


def _inventory() -> dict:
    return {
        "package": {
            "id": "local.demo",
            "version": "0.1.0",
            "protocol": "edgecitadel.plugin.v1",
        },
        "runtime": {
            "command": ["python", "-m", "runtime"],
            "healthTimeoutSeconds": 2,
            "restartPolicy": "on-failure",
        },
        "agents": [{"id": "demo-agent", "skillNames": ["demo"]}],
        "permissions": {
            "knowledge": [],
            "messaging": {"outboundAgents": []},
            "network": {"outbound": []},
            "devices": [],
        },
        "security": {"sandbox": "restricted", "secrets": []},
    }


def _write_node(path: Path) -> None:
    cli._write_json(
        path / "node.json",
        {
            "version": 1,
            "mode": "edge",
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "nats_token": "secret",
            "agent_id": "edge-one",
        },
    )


def test_plugin_install_requires_explicit_noninteractive_permission_approval(
    tmp_path, monkeypatch
):
    state_dir = tmp_path / "state"
    source = tmp_path / "source"
    source.mkdir()
    (source / "plugin.yaml").write_text("test")
    (source / "plugin.lock.json").write_text("{}")
    _write_node(state_dir)
    monkeypatch.setattr(cli, "_validate_plugin", lambda *args: _inventory())
    monkeypatch.setattr(cli, "_http_json", lambda *_args, **_kwargs: [])
    args = Namespace(
        source=str(source), state_dir=str(state_dir), yes=False, keep_disabled=True
    )

    with pytest.raises(cli.UserError, match="permission approval"):
        cli.command_plugin_install(args)

    assert not (state_dir / "plugins").exists()


def test_plugin_install_copies_to_managed_store_and_is_idempotent(
    tmp_path, monkeypatch
):
    state_dir = tmp_path / "state"
    source = tmp_path / "source"
    source.mkdir()
    (source / "plugin.yaml").write_text("test")
    (source / "plugin.lock.json").write_text("{}")
    _write_node(state_dir)
    monkeypatch.setattr(cli, "_validate_plugin", lambda *args: _inventory())
    monkeypatch.setattr(cli, "_http_json", lambda *_args, **_kwargs: [])
    args = Namespace(
        source=str(source), state_dir=str(state_dir), yes=True, keep_disabled=True
    )

    assert cli.command_plugin_install(args) == 0
    assert cli.command_plugin_install(args) == 0

    target = state_dir / "plugins" / "local.demo" / "0.1.0"
    plugin_state = json.loads((state_dir / "plugins.json").read_text())
    assert target.joinpath("plugin.yaml").read_text() == "test"
    assert plugin_state["plugins"]["local.demo"]["path"] == str(target)
    assert stat.S_IMODE(target.stat().st_mode) == 0o500


def test_plugin_install_preserves_read_only_executable_entrypoint(
    tmp_path, monkeypatch
):
    state_dir = tmp_path / "state"
    source = tmp_path / "source"
    source.mkdir()
    (source / "plugin.yaml").write_text("test")
    (source / "plugin.lock.json").write_text("{}")
    entrypoint = source / "run"
    entrypoint.write_text("#!/bin/sh\nexit 0\n")
    entrypoint.chmod(0o755)
    inventory = _inventory()
    inventory["runtime"]["command"] = ["./run"]
    _write_node(state_dir)
    monkeypatch.setattr(cli, "_validate_plugin", lambda *args: inventory)
    monkeypatch.setattr(cli, "_http_json", lambda *_args, **_kwargs: [])

    cli.command_plugin_install(
        Namespace(
            source=str(source), state_dir=str(state_dir), yes=True, keep_disabled=True
        )
    )

    installed = state_dir / "plugins" / "local.demo" / "0.1.0" / "run"
    assert stat.S_IMODE(installed.stat().st_mode) == 0o500


def test_plugin_install_rejects_agent_id_claimed_by_local_plugin(monkeypatch):
    state = {
        "plugins": {
            "other.plugin": {
                "inventory": {"agents": [{"id": "demo-agent"}]},
            }
        }
    }
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *_args, **_kwargs: pytest.fail("fleet must not be queried"),
    )

    with pytest.raises(cli.UserError, match="belongs to local plugin other.plugin"):
        cli._assert_agent_ids_available(
            {"core_url": "http://core.test", "agent_id": "edge-one"},
            _inventory(),
            state,
        )


def test_plugin_install_rejects_agent_id_owned_elsewhere_in_fleet(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *_args, **_kwargs: [
            {
                "agent_id": "demo-agent",
                "card": {
                    "metadata": {
                        "edgecitadel.node_id": "edge-two",
                        "edgecitadel.plugin_id": "other.plugin",
                    }
                },
            }
        ],
    )

    with pytest.raises(cli.UserError, match="already exists in the Core registry"):
        cli._assert_agent_ids_available(
            {"core_url": "http://core.test", "agent_id": "edge-one"},
            _inventory(),
            {"plugins": {}},
        )


def test_plugin_install_accepts_same_node_and_plugin_fleet_owner(monkeypatch):
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *_args, **_kwargs: [
            {
                "agent_id": "demo-agent",
                "card": {
                    "metadata": {
                        "edgecitadel.node_id": "edge-one",
                        "edgecitadel.plugin_id": "local.demo",
                    }
                },
            }
        ],
    )

    cli._assert_agent_ids_available(
        {"core_url": "http://core.test", "agent_id": "edge-one"},
        _inventory(),
        {"plugins": {}},
    )


def test_plugin_remove_stops_and_deletes_managed_copy(tmp_path, monkeypatch):
    state_dir = tmp_path / "state"
    source = tmp_path / "source"
    source.mkdir()
    (source / "plugin.yaml").write_text("test")
    (source / "plugin.lock.json").write_text("{}")
    _write_node(state_dir)
    monkeypatch.setattr(cli, "_validate_plugin", lambda *args: _inventory())
    monkeypatch.setattr(cli, "_http_json", lambda *_args, **_kwargs: [])
    cli.command_plugin_install(
        Namespace(
            source=str(source), state_dir=str(state_dir), yes=True, keep_disabled=True
        )
    )
    runtime_root = state_dir / "plugin-runtimes" / "local.demo" / "0.1.0"
    runtime_root.mkdir(parents=True)
    (runtime_root / "dependency").write_text("managed")
    plugin_data = state_dir / "plugin-state" / "local.demo" / "outcomes.db"
    plugin_data.parent.mkdir(parents=True)
    plugin_data.write_text("preserved")

    assert (
        cli.command_plugin_remove(
            Namespace(plugin_id="local.demo", state_dir=str(state_dir))
        )
        == 0
    )
    assert not (state_dir / "plugins" / "local.demo" / "0.1.0").exists()
    assert not (state_dir / "plugin-runtimes" / "local.demo").exists()
    assert plugin_data.read_text() == "preserved"


@pytest.mark.parametrize("distribution", ["homebrew", "pip"])
def test_installed_create_keeps_mutable_core_files_outside_install_root(
    tmp_path, distribution
):
    core_dir = tmp_path / "core"
    state_dir = tmp_path / "state"
    environment = {
        **os.environ,
        "EDGECITADEL_DISTRIBUTION": distribution,
        "EDGECITADEL_INSTALL_ROOT": str(REPO_ROOT),
        "EDGECITADEL_CORE_DIR": str(core_dir),
    }
    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "edgecitadel_cli.py"),
        "create",
        "--no-start",
        "--state-dir",
        str(state_dir),
    ]

    first = subprocess.run(command, env=environment, check=True, capture_output=True)
    env_source = (core_dir / ".env").read_text()
    second = subprocess.run(command, env=environment, check=True, capture_output=True)

    assert first.returncode == second.returncode == 0
    assert (core_dir / "nats" / "nats.conf").exists()
    assert (core_dir / "data").is_dir()
    assert (core_dir / "nats" / "data").is_dir()
    assert env_source == (core_dir / ".env").read_text()
    assert stat.S_IMODE((core_dir / ".env").stat().st_mode) == 0o600
    assert stat.S_IMODE((state_dir / "node.json").stat().st_mode) == 0o600


@pytest.mark.parametrize("distribution", ["homebrew", "pip"])
def test_installed_compose_override_redirects_all_mutable_mounts(
    tmp_path, monkeypatch, distribution
):
    core_dir = tmp_path / "core"
    monkeypatch.setattr(cli, "IS_HOMEBREW", distribution == "homebrew")
    monkeypatch.setattr(cli, "IS_PIP", distribution == "pip")
    monkeypatch.setattr(cli, "CORE_RUNTIME_DIR", core_dir)
    monkeypatch.setattr(cli, "ENV_PATH", core_dir / ".env")
    monkeypatch.setattr(cli, "INSTALL_ROOT", REPO_ROOT)

    command = cli._compose_command("config")
    override = (core_dir / "docker-compose.runtime.yml").read_text()

    assert command[:4] == ["docker", "compose", "--project-name", "edgecitadel"]
    assert "--env-file" in command
    assert str(core_dir / "nats" / "nats.conf") in override
    assert str(core_dir / "nats" / "data") in override
    assert str(core_dir / "data") in override
    assert str(REPO_ROOT / "docker-compose.yml") in command


def test_create_reports_missing_docker_before_writing_state(tmp_path, monkeypatch):
    monkeypatch.setattr(cli.shutil, "which", lambda _: None)
    args = Namespace(
        state_dir=str(tmp_path / "state"),
        no_start=False,
        host="localhost",
        timeout=1,
    )

    with pytest.raises(cli.UserError, match="Docker is required"):
        cli.command_create(args)

    assert not (tmp_path / "state" / "node.json").exists()


def test_join_parser_uses_exact_messaging_mode_names():
    parser = cli._build_parser()
    assert (
        parser.parse_args(["join", "ecjoin://value"]).messaging_mode == "single-client"
    )
    assert (
        parser.parse_args(
            ["join", "ecjoin://value", "--messaging-mode", "nats_leaf"]
        ).messaging_mode
        == "nats_leaf"
    )
    with pytest.raises(SystemExit):
        parser.parse_args(["join", "ecjoin://value", "--messaging-mode", "leaf"])


def test_v1_edge_state_normalizes_without_rewrite(tmp_path):
    _write_node(tmp_path)
    path = tmp_path / "node.json"
    original = path.read_bytes()

    node = cli._load_node(tmp_path)

    assert node["messaging_mode"] == "single-client"
    assert node["plugin_nats_url"] == "nats://core.test:4222"
    assert node["plugin_nats_token"] == "secret"
    assert path.read_bytes() == original


def test_join_rejects_conflicting_messaging_mode_without_mutation(tmp_path):
    _write_node(tmp_path)
    original = (tmp_path / "node.json").read_bytes()

    with pytest.raises(cli.UserError, match="already joined.*single-client.*nats_leaf"):
        cli.command_join(
            Namespace(
                invitation="not-consulted",
                messaging_mode="nats_leaf",
                state_dir=str(tmp_path),
            )
        )

    assert (tmp_path / "node.json").read_bytes() == original


def test_nats_leaf_join_commits_v2_only_after_local_readiness(tmp_path, monkeypatch):
    invitation = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "edge-one",
            "expires_at": time.time() + 60,
        }
    )
    observed_body = {}

    def redeem(*args, **kwargs):
        observed_body.update(kwargs["body"])
        return {
            "agent_id": "edge-one",
            "leaf_username": "leaf-user",
            "leaf_password": "leaf-password",
        }

    configured = {}
    monkeypatch.setattr(cli, "_http_json", redeem)
    monkeypatch.setattr(
        cli.nats_leaf, "preflight", lambda **kwargs: "/test/nats-server"
    )
    monkeypatch.setattr(
        cli.nats_leaf,
        "configure_and_start",
        lambda **kwargs: configured.update(kwargs) or {"local_ready": True},
    )

    assert (
        cli.command_join(
            Namespace(
                invitation=invitation,
                messaging_mode="nats_leaf",
                state_dir=str(tmp_path),
            )
        )
        == 0
    )

    state = json.loads((tmp_path / "node.json").read_text())
    assert observed_body == {"token": "t" * 43, "messaging_mode": "nats_leaf"}
    assert state["version"] == 2
    assert state["messaging_mode"] == "nats_leaf"
    assert state["plugin_nats_url"] == "nats://127.0.0.1:4223"
    assert state["upstream_nats_url"] == "nats://core.test:4222"
    assert state["jetstream_domain"] == cli.nats_leaf.domain_for("edge-one")
    assert "leaf_username" not in state and "leaf_password" not in state
    assert configured["binary"] == "/test/nats-server"


def test_nats_leaf_join_rolls_back_when_local_start_fails(tmp_path, monkeypatch):
    invitation = cli._invitation_encode(
        {
            "version": 1,
            "core_url": "http://core.test",
            "nats_url": "nats://core.test:4222",
            "token": "t" * 43,
            "agent_id": "edge-one",
            "expires_at": time.time() + 60,
        }
    )
    monkeypatch.setattr(
        cli.nats_leaf, "preflight", lambda **kwargs: "/test/nats-server"
    )
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {
            "agent_id": "edge-one",
            "leaf_username": "leaf-user",
            "leaf_password": "leaf-password",
        },
    )
    monkeypatch.setattr(
        cli.nats_leaf,
        "configure_and_start",
        lambda **kwargs: (_ for _ in ()).throw(cli.nats_leaf.NatsLeafError("failed")),
    )
    cleaned = []
    monkeypatch.setattr(
        cli.nats_leaf, "cleanup_failed_join", lambda path: cleaned.append(path)
    )

    with pytest.raises(cli.UserError, match="redeemed.*no node state"):
        cli.command_join(
            Namespace(
                invitation=invitation,
                messaging_mode="nats_leaf",
                state_dir=str(tmp_path),
            )
        )

    assert cleaned == [tmp_path]
    assert not (tmp_path / "node.json").exists()


def test_plugin_nats_environment_never_includes_leaf_credentials():
    environment = cli._plugin_nats_environment(
        {
            "plugin_nats_url": "nats://127.0.0.1:4223",
            "plugin_nats_token": "local-token",
            "jetstream_domain": "edge_domain",
            "leaf_username": "must-not-leak",
            "leaf_password": "must-not-leak",
        }
    )

    assert environment == {
        "NATS_URL": "nats://127.0.0.1:4223",
        "NATS_TOKEN": "local-token",
        "NATS_DOMAIN": "edge_domain",
    }


def test_declared_plugin_environment_excludes_unrelated_secrets(monkeypatch):
    monkeypatch.setenv("PATH", "/safe/bin")
    monkeypatch.setenv("HERMES_TOKEN", "hermes-secret")
    monkeypatch.setenv("HA_TOKEN", "unrelated-secret")
    monkeypatch.setenv("HERMES_MODEL", "local-model")
    record = {
        "inventory": {
            "runtime": {"environmentVariables": ["HERMES_MODEL"]},
            "security": {"secrets": ["HERMES_TOKEN"]},
        }
    }

    environment = cli._declared_plugin_environment(record)

    assert environment["PATH"] == "/safe/bin"
    assert environment["HERMES_TOKEN"] == "hermes-secret"
    assert environment["HERMES_MODEL"] == "local-model"
    assert "HA_TOKEN" not in environment


def test_plugin_start_uses_only_declared_host_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", "/safe/bin")
    monkeypatch.setenv("HERMES_TOKEN", "declared-secret")
    monkeypatch.setenv("UNRELATED_SECRET", "must-not-leak")
    inventory = _inventory()
    inventory["runtime"]["environmentVariables"] = ["HERMES_MODEL"]
    inventory["security"]["secrets"] = ["HERMES_TOKEN"]
    record = {
        "path": str(tmp_path),
        "inventory": inventory,
        "pid": None,
    }
    state = {"plugins": {"local.demo": record}}
    node = {
        "agent_id": "edge-one",
        "core_url": "http://core.test",
        "plugin_nats_url": "nats://core.test:4222",
        "plugin_nats_token": "plugin-token",
    }
    captured = {}

    class Process:
        pid = 123

        @staticmethod
        def poll():
            return None

    def popen(*args, **kwargs):
        captured["command"] = args[0]
        captured["start_new_session"] = kwargs["start_new_session"]
        captured.update(kwargs["env"])
        return Process()

    monkeypatch.setattr(cli, "_load_node", lambda *_: node)
    monkeypatch.setattr(cli, "_plugin_record", lambda *_: (state, record))
    monkeypatch.setattr(cli, "_ensure_plugin_inboxes", lambda *_: None)
    monkeypatch.setattr(cli, "_plugin_python", lambda *_: Path(sys.executable))
    monkeypatch.setattr(cli, "_pid_running", lambda pid: pid == 123)
    monkeypatch.setattr(cli, "_process_identity", lambda pid: "owned-123")
    monkeypatch.setattr(cli, "_write_json", lambda *_: None)
    monkeypatch.setattr(cli.subprocess, "Popen", popen)
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *_args, **_kwargs: {
            "agent_state": "online",
            "last_register": "9999-01-01T00:00:00Z",
            "last_heartbeat": "9999-01-01T00:00:00Z",
        },
    )

    cli._start_plugin(tmp_path, "local.demo")

    assert captured["HERMES_TOKEN"] == "declared-secret"
    assert captured["NATS_TOKEN"] == "plugin-token"
    assert "UNRELATED_SECRET" not in captured
    assert captured["command"][1].endswith("scripts/plugin_runner.py")
    assert captured["command"][2:5] == ["--restart-policy", "on-failure", "--"]
    assert captured["start_new_session"] is True
    assert record["process_identity"] == "owned-123"


def test_plugin_stop_signals_only_verified_owned_process_group(tmp_path, monkeypatch):
    record = {"pid": 321, "process_identity": "owned", "enabled": True}
    state = {"plugins": {"local.demo": record}}
    running = iter((True, True, False, False))
    signals: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(cli, "_plugin_record", lambda *_args: (state, record))
    monkeypatch.setattr(cli, "_pid_running", lambda _pid: next(running))
    monkeypatch.setattr(cli, "_process_identity", lambda _pid: "owned")
    monkeypatch.setattr(cli.os, "getpgid", lambda _pid: 321)
    monkeypatch.setattr(
        cli.os, "killpg", lambda group, signum: signals.append((group, signum))
    )
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli, "_write_json", lambda *_args: None)

    cli._stop_plugin(tmp_path, "local.demo")

    assert signals == [(321, signal.SIGTERM)]
    assert record["pid"] is None
    assert record["process_identity"] is None
    assert record["enabled"] is False


def test_plugin_stop_refuses_reused_unverified_pid(tmp_path, monkeypatch):
    record = {"pid": 321, "process_identity": "original", "enabled": True}
    state = {"plugins": {"local.demo": record}}
    monkeypatch.setattr(cli, "_plugin_record", lambda *_args: (state, record))
    monkeypatch.setattr(cli, "_pid_running", lambda _pid: True)
    monkeypatch.setattr(cli, "_process_identity", lambda _pid: "reused")
    monkeypatch.setattr(
        cli.os,
        "killpg",
        lambda *_args: pytest.fail("an unowned process group must not be signaled"),
    )

    with pytest.raises(cli.UserError, match="unverified live PID"):
        cli._stop_plugin(tmp_path, "local.demo")

    assert record["pid"] == 321
    assert record["enabled"] is True


def test_process_identity_changes_with_process_start_description(monkeypatch):
    class Result:
        returncode = 0

        def __init__(self, stdout):
            self.stdout = stdout

    descriptions = iter(
        (
            Result("Mon Sep  1 01:00:00 2026 plugin-runner"),
            Result("Mon Sep  1 01:00:01 2026 plugin-runner"),
        )
    )
    monkeypatch.setattr(cli, "_pid_running", lambda _pid: True)
    monkeypatch.setattr(Path, "is_file", lambda _path: False)
    monkeypatch.setattr(
        cli.subprocess, "run", lambda *_args, **_kwargs: next(descriptions)
    )

    first = cli._process_identity(321)
    second = cli._process_identity(321)

    assert first is not None
    assert second is not None
    assert first != second


def test_plugin_python_builds_and_reuses_requirements_scoped_runtime(
    tmp_path, monkeypatch
):
    plugin_root = tmp_path / "plugin"
    plugin_root.mkdir()
    requirements = plugin_root / "requirements.txt"
    requirements.write_text("httpx>=0.27\n")
    record = {
        "path": str(plugin_root),
        "inventory": {
            "package": {"version": "1.2.3"},
            "runtime": {"pythonRequirements": "requirements.txt"},
        },
    }
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        if command[1:3] == ["-m", "venv"]:
            runtime_python = Path(command[3]) / "bin" / "python"
            runtime_python.parent.mkdir(parents=True)
            runtime_python.touch()

    monkeypatch.setattr(cli, "_run", run)

    first = cli._plugin_python(tmp_path, "edgecitadel.gemma", record)
    second = cli._plugin_python(tmp_path, "edgecitadel.gemma", record)

    assert first == second
    assert first == (
        tmp_path / "plugin-runtimes" / "edgecitadel.gemma" / "1.2.3" / "bin" / "python"
    )
    assert len(commands) == 2
    assert commands[1][-2:] == ["-r", str(requirements)]
    assert (first.parents[1] / ".edgecitadel-runtime").stat().st_mode & 0o777 == 0o600

    upgraded_root = tmp_path / "upgraded-cellar"
    monkeypatch.setattr(cli, "INSTALL_ROOT", upgraded_root)
    assert cli._plugin_python(tmp_path, "edgecitadel.gemma", record) == first
    assert len(commands) == 4
    assert str(upgraded_root / "plugin-toolkit") in commands[3]


def test_plugin_python_rejects_missing_declared_requirements(tmp_path):
    record = {
        "path": str(tmp_path / "plugin"),
        "inventory": {
            "package": {"version": "1.0.0"},
            "runtime": {"pythonRequirements": "requirements.txt"},
        },
    }

    with pytest.raises(cli.UserError, match="missing its Python requirements"):
        cli._plugin_python(tmp_path, "edgecitadel.missing", record)


def test_plugin_install_resolves_bundled_plugin_before_example(tmp_path, monkeypatch):
    install_root = tmp_path / "install"
    bundled = install_root / "plugins" / "gemma"
    example = install_root / "plugins" / "examples" / "gemma"
    bundled.mkdir(parents=True)
    example.mkdir(parents=True)
    monkeypatch.setattr(cli, "INSTALL_ROOT", install_root)
    monkeypatch.setattr(cli, "_load_node", lambda state_dir: {})
    observed = {}

    def validate(source, state_dir):
        observed["source"] = source
        raise cli.UserError("stop after source resolution")

    monkeypatch.setattr(cli, "_validate_plugin", validate)
    args = Namespace(source="gemma", state_dir=str(tmp_path / "state"))

    with pytest.raises(cli.UserError, match="stop after source resolution"):
        cli.command_plugin_install(args)

    assert observed["source"] == bundled.resolve()


def test_doctor_json_classifies_leaf_disconnect_as_degraded(
    tmp_path, monkeypatch, capsys
):
    cli._write_json(
        tmp_path / "node.json",
        {
            "version": 2,
            "mode": "edge",
            "messaging_mode": "nats_leaf",
            "core_url": "http://core.test",
            "upstream_nats_url": "nats://core.test:4222",
            "plugin_nats_url": "nats://127.0.0.1:4223",
            "plugin_nats_token": "local-token",
            "jetstream_domain": "edge_domain",
            "agent_id": "edge-one",
        },
    )
    observation = {
        "state": "degraded",
        "process_running": True,
        "client_ready": True,
        "jetstream_ready": True,
        "leaf_connected": False,
        "local_ready": True,
        "pid": 123,
    }
    monkeypatch.setattr(cli.nats_leaf, "observe", lambda _: observation)
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {"nats_connected": False, "jetstream_stream_ok": False},
    )
    monkeypatch.setattr(cli, "_tcp_ready", lambda *args, **kwargs: False)

    assert cli.command_doctor(Namespace(state_dir=str(tmp_path), json=True)) == 1

    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "degraded"
    assert report["messaging_mode"] == "nats_leaf"
    checks = {item["id"]: item for item in report["checks"]}
    assert checks["local_agent_messaging"]["ok"] is True
    assert checks["leaf_connection"]["ok"] is False
    assert checks["cross_node_messaging"]["detail"] == "paused"


def test_doctor_treats_disabled_plugin_and_agents_as_non_failing(
    tmp_path, monkeypatch, capsys
):
    cli._write_json(
        tmp_path / "node.json",
        {
            "version": 2,
            "mode": "edge",
            "messaging_mode": "single-client",
            "core_url": "http://core.test",
            "upstream_nats_url": "nats://core.test:4222",
            "plugin_nats_url": "nats://core.test:4222",
            "plugin_nats_token": "plugin-token",
            "nats_url": "nats://core.test:4222",
            "nats_token": "plugin-token",
            "agent_id": "edge-one",
        },
    )
    cli._write_json(
        tmp_path / "plugins.json",
        {
            "version": 1,
            "plugins": {
                "edgecitadel.disabled": {
                    "enabled": False,
                    "pid": None,
                    "inventory": {"agents": [{"id": "disabled-agent"}]},
                }
            },
        },
    )
    requested_urls: list[str] = []

    def http_json(url, **_kwargs):
        requested_urls.append(url)
        return {"nats_connected": True, "jetstream_stream_ok": True}

    monkeypatch.setattr(cli, "_http_json", http_json)
    monkeypatch.setattr(cli, "_tcp_ready", lambda *_args, **_kwargs: True)

    assert cli.command_doctor(Namespace(state_dir=str(tmp_path), json=True)) == 0

    report = json.loads(capsys.readouterr().out)
    checks = {item["id"]: item for item in report["checks"]}
    assert report["status"] == "healthy"
    assert checks["plugin_edgecitadel.disabled"] == {
        "id": "plugin_edgecitadel.disabled",
        "name": "plugin edgecitadel.disabled",
        "ok": True,
        "detail": "disabled",
    }
    assert checks["agent_disabled-agent"]["ok"] is True
    assert checks["agent_disabled-agent"]["detail"] == "disabled with plugin"
    assert requested_urls == ["http://core.test/api/system/status"]


def test_messaging_stop_is_successful_when_local_service_is_stopped(
    tmp_path, monkeypatch
):
    cli._write_json(
        tmp_path / "node.json",
        {
            "version": 2,
            "mode": "edge",
            "messaging_mode": "nats_leaf",
            "core_url": "http://core.test",
            "upstream_nats_url": "nats://core.test:4222",
            "plugin_nats_url": "nats://127.0.0.1:4223",
            "plugin_nats_token": "local-token",
            "jetstream_domain": "edge_domain",
            "agent_id": "edge-one",
        },
    )
    stopped: list[Path] = []
    monkeypatch.setattr(cli.nats_leaf, "stop", lambda path: stopped.append(path))
    monkeypatch.setattr(
        cli.nats_leaf,
        "observe",
        lambda _: {
            "state": "stopped",
            "process_running": False,
            "client_ready": False,
            "jetstream_ready": False,
            "leaf_connected": False,
            "local_ready": False,
            "pid": None,
        },
    )

    result = cli.command_messaging(
        Namespace(state_dir=str(tmp_path), action="stop", json=False)
    )

    assert result == 0
    assert stopped == [tmp_path]
