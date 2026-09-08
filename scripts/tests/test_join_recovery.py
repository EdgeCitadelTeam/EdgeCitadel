"""Match onboarding evidence and single-use redemption recovery to the guide."""

from __future__ import annotations

import json
import time
import urllib.error
from argparse import Namespace
from pathlib import Path

import pytest

from scripts import edgecitadel_cli as cli


def test_install_stops_after_join_reports_saved_but_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "INSTALL_ROOT", Path(__file__).resolve().parents[2])

    def join(args):
        print(
            "Enrollment saved; broker unavailable. Restore connectivity; do not redeem again."
        )
        return 1

    monkeypatch.setattr(cli, "command_join", join)
    monkeypatch.setattr(
        cli,
        "_start_agentd",
        lambda state: pytest.fail("continued after unavailable join"),
    )
    args = cli._build_parser().parse_args(
        [
            "install",
            "--join",
            "ecjoin://test",
            "--plugin",
            "codex",
            "--yes",
            "--state-dir",
            str(tmp_path),
        ]
    )
    with pytest.raises(
        cli.OperationalError, match="Enrollment saved; broker unavailable"
    ):
        cli.command_install(args)


def invitation():
    return {
        "version": 1,
        "core_url": "http://core.example",
        "nats_url": "nats://core.example:4222",
        "token": "test-only-invitation",
        "agent_id": "test-edge",
        "expires_at": time.time() + 60,
    }


@pytest.mark.parametrize(
    "mode,ports", [("single-client", [None, 4222]), ("nats_leaf", [None, 7422])]
)
def test_preflight_requires_only_topology_specific_paths(monkeypatch, mode, ports):
    probes = []
    monkeypatch.setattr(
        cli,
        "_probe_endpoint",
        lambda url, **kwargs: probes.append((url, kwargs.get("port"))) or True,
    )
    cli._join_preflight(invitation(), mode)
    assert [port for _, port in probes] == ports
    assert probes[0][0] == "http://core.example"
    assert 8222 not in ports


@pytest.mark.parametrize("failure", ["api", "broker"])
def test_failed_preflight_never_redeems_or_writes_state(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(
        cli,
        "_probe_endpoint",
        lambda url, **kwargs: failure == "broker" and url.startswith("http"),
    )
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: pytest.fail("preflight failure reached redemption"),
    )
    with pytest.raises(cli.OperationalError, match="no redemption was attempted"):
        cli.command_join(
            Namespace(
                invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
            )
        )
    assert not (tmp_path / "node.json").exists()


@pytest.mark.parametrize(
    "outcome", [None, [], {}, {"agent_id": "wrong"}, {"agent_id": "test-edge"}, "lost"]
)
def test_uncertain_redemption_is_never_retried(tmp_path, monkeypatch, outcome):
    monkeypatch.setattr(cli, "_join_preflight", lambda *args: None)
    requests = []

    def redeem(*args, **kwargs):
        requests.append(kwargs)
        if outcome == "lost":
            raise cli.UserError("connection reset") from ConnectionResetError()
        return outcome

    monkeypatch.setattr(cli, "_http_json", redeem)
    with pytest.raises(
        cli.OperationalError, match="consumption is uncertain.*Do not retry"
    ):
        cli.command_join(
            Namespace(
                invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
            )
        )
    assert len(requests) == 1
    assert not (tmp_path / "node.json").exists()


def test_rejected_invitation_retains_combined_server_diagnosis(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "_join_preflight", lambda *args: None)

    def rejected(*args, **kwargs):
        cause = urllib.error.HTTPError(
            "http://core.example", 403, "Forbidden", {}, None
        )
        raise cli.UserError("Core rejected redemption") from cause

    monkeypatch.setattr(cli, "_http_json", rejected)
    with pytest.raises(cli.UserError, match="invalid, expired, or already used"):
        cli.command_join(
            Namespace(
                invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
            )
        )


def test_post_redemption_broker_failure_preserves_credentials_and_rerun_is_honest(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(cli, "_join_preflight", lambda *args: None)
    requests = []
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: requests.append(kwargs)
        or {"agent_id": "test-edge", "nats_token": "test-only-broker"},
    )
    monkeypatch.setattr(cli, "_tcp_ready", lambda *args, **kwargs: False)
    args = Namespace(
        invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
    )
    assert cli.command_join(args) == 1
    node = json.loads((tmp_path / "node.json").read_text())
    assert node["plugin_nats_token"] == "test-only-broker"
    assert cli.command_join(args) == 1
    assert len(requests) == 1
    assert "do not redeem this invitation again" in capsys.readouterr().out


def test_successful_tcp_probe_does_not_claim_authenticated_messaging(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.setattr(cli, "_join_preflight", lambda *args: None)
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {
            "agent_id": "test-edge",
            "nats_token": "test-only-broker",
        },
    )
    monkeypatch.setattr(cli, "_tcp_ready", lambda *args, **kwargs: True)
    assert (
        cli.command_join(
            Namespace(
                invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
            )
        )
        == 0
    )
    assert "not verified by this check" in capsys.readouterr().out


def test_probe_resolves_in_bounded_helper_then_connects_to_literal_ip(monkeypatch):
    from contextlib import nullcontext

    monkeypatch.setattr(
        cli.core_network, "resolve_addresses", lambda host: {"192.0.2.10"}
    )
    observed = []
    monkeypatch.setattr(
        cli.socket,
        "create_connection",
        lambda address, **kwargs: observed.append((address, kwargs["timeout"]))
        or nullcontext(),
    )
    assert cli._probe_endpoint("https://core.example")
    assert observed[0][0] == ("192.0.2.10", 443)
    assert 0 < observed[0][1] <= 2


@pytest.fixture
def replacement(tmp_path, monkeypatch):
    old = {
        "version": 1,
        "mode": "core",
        "agent_id": "core",
        "core_url": "http://localhost",
        "nats_url": "nats://localhost:4222",
        "nats_token": "old-broker",
    }
    cli._write_json(tmp_path / "node.json", old)
    for directory in ("agentd", "connectors", "managed-launch"):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / "old-state").write_text("old fleet data")
    calls = []
    monkeypatch.setattr(cli, "_join_preflight", lambda *args: None)
    monkeypatch.setattr(cli, "_tcp_ready", lambda *args, **kwargs: True)
    monkeypatch.setattr(cli, "_agentd_process_detail", lambda path: (True, "ready"))
    monkeypatch.setattr(cli, "_stop_agentd", lambda path: calls.append("stop"))
    monkeypatch.setattr(cli, "_start_agentd", lambda path: calls.append("start") or {})
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: calls.append("redeem")
        or {"agent_id": "test-edge", "nats_token": "new-broker"},
    )
    args = Namespace(
        invitation=cli._invitation_encode(invitation()), state_dir=str(tmp_path)
    )
    return args, old, calls


@pytest.mark.parametrize("old_mode", ["core", "edge"])
def test_new_invitation_replaces_enrollment_and_isolates_old_runtime(
    tmp_path, monkeypatch, replacement, old_mode
):
    args, old, calls = replacement
    old["mode"] = old_mode
    cli._write_json(tmp_path / "node.json", old)
    plugin = tmp_path / "native-plugin-marker"
    plugin.write_text("installed")
    assert cli.command_join(args) == 0
    node = cli._load_node(tmp_path)
    assert (node["mode"], node["agent_id"], node["nats_token"]) == (
        "edge",
        "test-edge",
        "new-broker",
    )
    assert calls == ["redeem", "stop", "start"]
    backups = list((tmp_path / "enrollment-backups").iterdir())
    assert len(backups) == 1
    assert json.loads((backups[0] / "node.json").read_text()) == old
    assert backups[0].stat().st_mode & 0o777 == 0o700
    for directory in ("agentd", "connectors", "managed-launch"):
        assert (backups[0] / directory / "old-state").exists()
        assert not (tmp_path / directory / "old-state").exists()
    assert plugin.read_text() == "installed"
    assert cli.command_join(args) == 0
    assert calls == ["redeem", "stop", "start"]


@pytest.mark.parametrize("response", [{"agent_id": "wrong"}, {"agent_id": "test-edge"}])
def test_invalid_replacement_response_leaves_existing_service_untouched(
    tmp_path, monkeypatch, replacement, response
):
    args, old, calls = replacement
    monkeypatch.setattr(cli, "_http_json", lambda *args, **kwargs: response)
    with pytest.raises(cli.OperationalError, match="incomplete"):
        cli.command_join(args)
    assert json.loads((tmp_path / "node.json").read_text()) == old
    assert calls == []
    assert not (tmp_path / "enrollment-backups").exists()


def test_replacement_write_failure_restores_old_credentials_and_service(
    tmp_path, monkeypatch, replacement
):
    args, old, calls = replacement
    write = cli._write_json

    def fail_new_node(path, value):
        if path == tmp_path / "node.json" and value.get("mode") == "edge":
            raise OSError("disk full")
        write(path, value)

    monkeypatch.setattr(cli, "_write_json", fail_new_node)
    with pytest.raises(cli.OperationalError, match="saving local state failed"):
        cli.command_join(args)
    assert json.loads((tmp_path / "node.json").read_text()) == old
    assert (tmp_path / "connectors" / "old-state").exists()
    assert calls == ["redeem", "stop", "start"]


def test_leaf_replacement_stops_old_broker_before_new_preflight(
    tmp_path, monkeypatch, replacement
):
    args, old, calls = replacement
    old.update(
        version=2,
        mode="edge",
        messaging_mode="nats_leaf",
        plugin_nats_url="nats://localhost:4223",
        plugin_nats_token="old-local",
        upstream_nats_url=old["nats_url"],
        jetstream_domain="old",
    )
    cli._write_json(tmp_path / "node.json", old)
    args.messaging_mode = "nats_leaf"
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {
            "agent_id": "test-edge",
            "leaf_username": "leaf",
            "leaf_password": "new",
        },
    )
    monkeypatch.setattr(cli.nats_leaf, "stop", lambda path: calls.append("leaf-stop"))
    monkeypatch.setattr(
        cli.nats_leaf,
        "preflight",
        lambda **kwargs: calls.append("leaf-preflight") or "/test/nats-server",
    )
    monkeypatch.setattr(
        cli.nats_leaf,
        "configure_and_start",
        lambda **kwargs: calls.append("leaf-start"),
    )
    assert cli.command_join(args) == 0
    assert calls == ["stop", "leaf-stop", "leaf-preflight", "leaf-start", "start"]
    assert cli._load_node(tmp_path)["messaging_mode"] == "nats_leaf"


@pytest.mark.parametrize("dry_run", [False, True])
def test_install_honors_explicit_invitation_with_installed_plugin(
    tmp_path, monkeypatch, replacement, capsys, dry_run
):
    from scripts import plugin_installation as plugins

    args, old, calls = replacement

    class InstalledDriver:
        def status(self, scope):
            return plugins.PluginStatus("codex", "installed", True, scope=scope)

        def apply(self, *args):
            pytest.fail("installed plugin should not be reinstalled")

    monkeypatch.setattr(cli, "driver_for", lambda *args, **kwargs: InstalledDriver())
    monkeypatch.setattr(cli, "_agentd_rpc", lambda *args, **kwargs: [])
    args = cli._build_parser().parse_args(
        [
            "install",
            "--join",
            args.invitation,
            "--plugin",
            "codex",
            "--yes",
            "--json",
            "--state-dir",
            str(tmp_path),
            *(["--dry-run"] if dry_run else []),
        ]
    )
    assert cli.command_install(args) == 0
    result = json.loads(capsys.readouterr().out)
    enrollment = next(step for step in result["steps"] if step["step"] == "enrollment")
    assert enrollment["state"] == ("planned" if dry_run else "succeeded")
    assert cli._load_node(tmp_path)["mode"] == ("core" if dry_run else "edge")
    assert ("redeem" in calls) is not dry_run


def test_replacement_allows_revoked_native_connector_to_register_again(
    tmp_path, monkeypatch, replacement
):
    monkeypatch.syspath_prepend(
        str(Path(__file__).resolve().parents[2] / "agent-runtime" / "src")
    )
    from edgecitadel_agentd.store import AgentdStore, StoreError

    args, old, calls = replacement
    database = tmp_path / "agentd" / "agentd.sqlite3"
    store = AgentdStore(database)
    old_token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="core-codex",
        capabilities=["edgecitadel_agents"],
    )
    store.revoke_connector("codex-local")
    store.close()
    (tmp_path / "connectors" / "codex-local.token").write_text(old_token)

    assert cli.command_join(args) == 0
    assert not (tmp_path / "connectors" / "codex-local.token").exists()
    store = AgentdStore(database)
    try:
        new_token = store.register_connector(
            connector_id="codex-local",
            host_type="codex",
            agent_id="test-edge-codex",
            capabilities=["edgecitadel_agents"],
        )
        assert (
            store.authenticate("codex-local", new_token)["agent_id"]
            == "test-edge-codex"
        )
        with pytest.raises(StoreError):
            store.authenticate("codex-local", old_token)
    finally:
        store.close()


def test_leaf_setup_failure_restores_previous_enrollment(
    tmp_path, monkeypatch, replacement
):
    args, old, calls = replacement
    args.messaging_mode = "nats_leaf"
    monkeypatch.setattr(
        cli,
        "_http_json",
        lambda *args, **kwargs: {
            "agent_id": "test-edge",
            "leaf_username": "leaf",
            "leaf_password": "new",
        },
    )
    monkeypatch.setattr(
        cli.nats_leaf, "preflight", lambda **kwargs: "/test/nats-server"
    )

    def fail_leaf(**kwargs):
        (tmp_path / "nats_leaf").mkdir()
        (tmp_path / "nats_leaf" / "credentials.json").write_text("new credentials")
        raise cli.nats_leaf.NatsLeafError("cannot start")

    monkeypatch.setattr(cli.nats_leaf, "configure_and_start", fail_leaf)
    monkeypatch.setattr(cli.nats_leaf, "cleanup_failed_join", lambda path: None)
    with pytest.raises(cli.UserError, match="new enrollment.*not committed"):
        cli.command_join(args)
    assert json.loads((tmp_path / "node.json").read_text()) == old
    assert not (tmp_path / "nats_leaf").exists()
    assert calls == ["stop", "start"]


def test_replacement_restarts_enabled_managed_agents(
    tmp_path, monkeypatch, replacement
):
    args, old, calls = replacement
    monkeypatch.setattr(
        cli,
        "_load_plugins",
        lambda path: {
            "version": 1,
            "managed_agents": {"echo": {"enabled": True}, "paused": {"enabled": False}},
        },
    )
    monkeypatch.setattr(
        cli, "_stop_plugin", lambda path, name, **kwargs: calls.append("stop-" + name)
    )
    monkeypatch.setattr(
        cli, "_start_plugin", lambda path, name: calls.append("start-" + name)
    )
    assert cli.command_join(args) == 0
    assert calls == ["redeem", "stop-echo", "stop", "start", "start-echo"]


def test_service_activation_failure_keeps_new_enrollment(
    tmp_path, monkeypatch, replacement
):
    args, old, calls = replacement

    def fail_service(path):
        raise cli.UserError("cannot start")

    monkeypatch.setattr(cli, "_start_agentd", fail_service)
    with pytest.raises(cli.OperationalError, match="New enrollment is saved"):
        cli.command_join(args)
    assert cli._load_node(tmp_path)["agent_id"] == "test-edge"
    assert calls.count("redeem") == 1
