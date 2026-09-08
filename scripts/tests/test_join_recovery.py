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
    args.invitation = "already-consumed-do-not-read"
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
