"""Fail-closed research labels for retained lab evidence."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import scripts.research.lab_qualification as qualification_module
from scripts.research.check_artifact import CheckReport
from scripts.research.lab_qualification import classify_lab, qualify_bundle


def _manifest() -> dict[str, object]:
    controller = {
        "declared_host_id": "controller-lab-01",
        "machine_id_sha256": "1" * 64,
        "advertised_host": "controller-lab.internal",
        "advertised_ip": "100.64.10.10",
    }
    local = {
        "declared_host_id": "controller-lab-01",
        "machine_id_sha256": "1" * 64,
        "agent_id": "shell-controller",
        "reservation_id": "controller-reservation",
        "preflight_valid": True,
        "os_release": "Ubuntu 24.04.1 LTS",
        "architecture": "x86_64",
        "launcher_source_commit": "3" * 40,
        "source_snapshot_sha256": "4" * 64,
    }
    remote = {
        "declared_host_id": "gateway-lab-02",
        "machine_id_sha256": "2" * 64,
        "agent_id": "shell-remote",
        "reservation_id": "remote-reservation",
        "preflight_valid": True,
        "os_release": "Ubuntu 24.04.1 LTS",
        "architecture": "x86_64",
        "launcher_source_commit": "3" * 40,
        "source_snapshot_sha256": "4" * 64,
        "server_observed_peer_ip": "100.64.10.11",
        "network_path": {
            "source_ip": "100.64.10.11",
            "destination_ip": "100.64.10.10",
            "interface": "tailscale0",
            "route_output_sha256": "5" * 64,
            "controller_dns_name": "controller-lab.internal",
        },
    }
    paths = {
        "reservation_events": "raw/lab/reservation-events.json",
        "node_reports": "raw/lab/node-reports.json",
        "controller_commands": "raw/lab/controller-commands.json",
        "cleanup": "raw/lab/cleanup.json",
    }
    return {
        "status": "PASS",
        "lab_variant": "lifecycle",
        "source": {"commit": "3" * 40, "source_sha256": "4" * 64},
        "controller": controller,
        "nodes": [local, remote],
        "observations": {
            **{
                name: {"path": path, "sha256": str(index) * 64}
                for index, (name, path) in enumerate(paths.items(), start=6)
            },
            "playwright": [],
        },
        "artifacts": {
            path: str(index) * 64 for index, path in enumerate(paths.values(), start=6)
        },
        "controller_commands": {
            "launches": [
                {
                    "agent_id": node["agent_id"],
                    "reservation_id": node["reservation_id"],
                    "declared_host_id": node["declared_host_id"],
                }
                for node in (local, remote)
            ],
            "commands": [
                {
                    "agent_id": "shell-controller",
                    "reservation_id": "controller-reservation",
                    "task_id": "task-1",
                    "status": "completed",
                    "wire_copies": 1,
                    "http_status": 202,
                    "expected_output": "edgecitadel:controller-01",
                    "terminal_output": "edgecitadel:controller-01",
                    "terminal_count": 1,
                    "conflicting_terminal": False,
                    "accepted_at": "2026-07-25T00:00:01Z",
                    "terminal_at": "2026-07-25T00:00:02Z",
                    "qualification_kind": "direct",
                },
                {
                    "agent_id": "shell-remote",
                    "reservation_id": "remote-reservation",
                    "task_id": "task-2",
                    "status": "completed",
                    "wire_copies": 1,
                    "http_status": 202,
                    "expected_output": "edgecitadel:remote-01",
                    "terminal_output": "edgecitadel:remote-01",
                    "terminal_count": 1,
                    "conflicting_terminal": False,
                    "accepted_at": "2026-07-25T00:00:03Z",
                    "terminal_at": "2026-07-25T00:00:04Z",
                    "qualification_kind": "direct",
                },
                {
                    "agent_id": "shell-remote",
                    "reservation_id": "remote-reservation",
                    "task_id": "task-3",
                    "status": "completed",
                    "wire_copies": 1,
                    "http_status": 202,
                    "expected_output": "edgecitadel:queued-remote-01",
                    "terminal_output": "edgecitadel:queued-remote-01",
                    "terminal_count": 1,
                    "conflicting_terminal": False,
                    "accepted_at": "2026-07-25T00:00:06Z",
                    "terminal_at": "2026-07-25T00:00:09Z",
                    "qualification_kind": "queued-reconnect",
                },
            ],
        },
        "reservation_events": [
            {
                "sequence": 1,
                "agent_id": "shell-remote",
                "reservation_id": "remote-reservation",
                "declared_host_id": "gateway-lab-02",
                "event": "retained",
                "observed_at": "2026-07-25T00:00:05Z",
            },
            {
                "sequence": 2,
                "agent_id": "shell-remote",
                "reservation_id": "remote-reservation",
                "declared_host_id": "gateway-lab-02",
                "event": "resumed",
                "observed_at": "2026-07-25T00:00:08Z",
            },
        ],
    }


def _classify(manifest: dict[str, object], *, valid: bool = True):
    return classify_lab(manifest=manifest, check_report=CheckReport(valid, ()))


def test_two_same_host_reports_are_preliminary() -> None:
    manifest = _manifest()
    nodes = manifest["nodes"]
    assert isinstance(nodes, list)
    nodes[1]["declared_host_id"] = "controller-lab-01"
    nodes[1]["machine_id_sha256"] = "1" * 64
    result = _classify(manifest)
    assert result.status == "preliminary"
    assert result.same_host_two_node is True
    assert "distinct_declared_hosts_required" in result.reasons


def test_two_checkout_paths_on_one_machine_remain_preliminary() -> None:
    manifest = _manifest()
    nodes = manifest["nodes"]
    assert isinstance(nodes, list)
    nodes[0]["checkout_path"] = "/srv/controller"
    nodes[1]["checkout_path"] = "/srv/gateway"
    nodes[1]["machine_id_sha256"] = "1" * 64
    result = _classify(manifest)
    assert result.same_host_two_node is True
    assert "distinct_machine_ids_required" in result.reasons


def test_distinct_declared_hosts_without_distinct_machine_ids_are_preliminary() -> None:
    manifest = _manifest()
    nodes = manifest["nodes"]
    assert isinstance(nodes, list)
    nodes[1]["machine_id_sha256"] = "1" * 64
    result = _classify(manifest)
    assert result.remote_qualified is False
    assert "distinct_machine_ids_required" in result.reasons
    evidence = manifest["controller_commands"]
    assert isinstance(evidence, dict)
    evidence["launches"][1]["declared_host_id"] = "wrong-host"
    result = _classify(manifest)
    assert "launch_host_binding_mismatch" in result.reasons


def test_invalid_remote_route_facts_are_preliminary() -> None:
    for route_change in ("loopback", "missing", "peer-mismatch"):
        manifest = _manifest()
        nodes = manifest["nodes"]
        assert isinstance(nodes, list)
        remote = nodes[1]
        if route_change == "loopback":
            remote["network_path"]["source_ip"] = "127.0.0.1"
        elif route_change == "missing":
            remote.pop("network_path")
        else:
            remote["server_observed_peer_ip"] = "100.64.10.12"
        result = _classify(manifest)
        assert result.remote_qualified is False
        assert any(
            reason.startswith("remote_route") or reason == "remote_peer_mismatch"
            for reason in result.reasons
        )


def test_missing_successful_command_to_either_host_is_preliminary() -> None:
    for agent_id in ("shell-controller", "shell-remote"):
        manifest = _manifest()
        evidence = manifest["controller_commands"]
        assert isinstance(evidence, dict)
        evidence["commands"] = [
            item for item in evidence["commands"] if item["agent_id"] != agent_id
        ]
        result = _classify(manifest)
        assert result.remote_qualified is False
        assert "host_command_evidence_missing" in result.reasons
    for field, invalid in (
        ("http_status", 200),
        ("terminal_output", "edgecitadel:wrong"),
        ("terminal_count", 2),
        ("conflicting_terminal", True),
    ):
        manifest = _manifest()
        evidence = manifest["controller_commands"]
        assert isinstance(evidence, dict)
        evidence["commands"][1][field] = invalid
        result = _classify(manifest)
        assert "host_command_evidence_missing" in result.reasons


def test_missing_disconnect_queue_reconnect_terminal_order_is_preliminary() -> None:
    manifest = deepcopy(_manifest())
    manifest["reservation_events"] = manifest["reservation_events"][:-1]
    result = _classify(manifest)
    assert result.remote_qualified is False
    assert "queued_reconnect_order_missing" in result.reasons


def test_invalid_check_report_or_non_pass_manifest_is_preliminary() -> None:
    for invalid_kind in ("check", "status"):
        manifest = _manifest()
        if invalid_kind == "status":
            manifest["status"] = "INVALID"
        result = _classify(manifest, valid=invalid_kind != "check")
        assert result.remote_qualified is False
        assert result.status == "preliminary"
    manifest = _manifest()
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts.pop("raw/lab/controller-commands.json")
    result = _classify(manifest)
    assert "observation_artifacts_incomplete" in result.reasons


def test_complete_two_host_lifecycle_and_runbook_contract(
    tmp_path: Path, monkeypatch
) -> None:
    complete = _manifest()
    result = _classify(complete)
    assert result.status == "remote-qualified"
    assert result.remote_qualified is True
    assert result.same_host_two_node is False
    assert result.reasons == ()

    bundle = tmp_path / "bundle"
    raw = bundle / "raw/lab"
    raw.mkdir(parents=True)
    commands = complete.pop("controller_commands")
    events = complete.pop("reservation_events")
    (raw / "controller-commands.json").write_text(json.dumps(commands) + "\n")
    (raw / "reservation-events.json").write_text(json.dumps(events) + "\n")
    (bundle / "manifest.json").write_text(json.dumps(complete) + "\n")
    monkeypatch.setattr(
        qualification_module,
        "check_bundle",
        lambda *_args, **_kwargs: CheckReport(True, ()),
    )
    loaded, checker_valid = qualify_bundle(bundle=bundle, source_root=tmp_path)
    assert checker_valid is True
    assert loaded.status == "remote-qualified"
