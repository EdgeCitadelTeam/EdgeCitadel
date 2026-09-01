"""Exact fixture-container contracts for deterministic lab nodes."""

from __future__ import annotations

import json
import io
import subprocess
import urllib.error
from argparse import Namespace
from dataclasses import replace
from pathlib import Path

import pytest

from scripts.research.lab_config import (
    ControllerConfig,
    LabConfigError,
    credential_sha256,
    credential_token,
    write_credential_file,
    write_private_json,
)
from scripts.research.lab_node import (
    NodeState,
    build_fixture_config,
    build_fixture_create_argv,
    doctor_node,
    load_node_state,
    start_node,
    stop_node,
    write_node_state,
)
from scripts.research.lab_runtime import SourceProvenance


def _controller(tmp_path: Path) -> ControllerConfig:
    credential = tmp_path / "transport-token"
    write_credential_file(credential, "4" * 64)
    return ControllerConfig(
        run_id="ec-lab-01",
        lab_variant="lifecycle",
        controller_host_id="controller-lab-01",
        compose_project="edgecitadel-artifact-ec-lab-01",
        bind_host="127.0.0.1",
        advertised_host="127.0.0.1",
        advertised_ip="127.0.0.1",
        app_url="http://127.0.0.1:18080",
        agg_url="http://127.0.0.1:18080",
        nats_url="nats://127.0.0.1:14222",
        monitor_url="http://127.0.0.1:18222",
        inventory_url="http://127.0.0.1:18080/api/lab/status",
        controller_machine_id_sha256="a" * 64,
        source_commit="d" * 40,
        source_snapshot_sha256="e" * 64,
        credential_sha256=credential_sha256(credential),
        credential_file=credential,
        fixture_image_id="sha256:" + "c" * 64,
        state_dir=tmp_path / "controller-state",
        evidence_dir=tmp_path / "evidence",
    )


def test_fixture_config_keeps_no_crash_as_json_null() -> None:
    config = build_fixture_config(
        run_id="ec-lab-01",
        agent_id="fixture-1",
        behavior="echo",
        delay_ms=125,
        crash_point=None,
    )
    assert json.loads(json.dumps(config.__dict__))["crash_point"] is None
    assert config.outcome_db == "/run/state/outcomes.sqlite"


def test_fixture_create_argv_uses_only_immutable_image_and_secret_file(
    tmp_path: Path,
) -> None:
    controller = _controller(tmp_path)
    node_state = tmp_path / "node-state"
    config = node_state / "native-control.json"
    argv = build_fixture_create_argv(
        controller=controller,
        credential_file=controller.credential_file,
        node_state_dir=node_state,
        config_path=config,
        container_name="edgecitadel-node-ec-lab-01--fixture-1",
    )
    rendered = "\0".join(argv)
    assert controller.fixture_image_id in argv
    assert "--network\0host" in rendered
    assert "NATS_URL=nats://127.0.0.1:14222" in rendered
    assert "EC_CREDENTIAL_FILE=/run/secrets/transport-token" in rendered
    assert "EC_EVENT_LOG=/run/state/fixture.log" in rendered
    assert "EC_TERMINAL_RELEASE_DIR=/run/state/terminal-release" in rendered
    assert "ai.edgecitadel.qualified-agent-id=ec-lab-01--fixture-1" in rendered
    assert credential_token(controller.credential_file) not in rendered
    assert not any("edgecitadel-lab-fixture:" in part for part in argv)


def test_start_and_stop_persist_reservation_bound_node_state(
    tmp_path: Path, monkeypatch
) -> None:
    controller = _controller(tmp_path)
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    inventory_calls: list[tuple[str, str]] = []

    def inventory(_controller, _credential, method, suffix, _body):
        inventory_calls.append((method, suffix))
        return None

    def runner(argv, **_kwargs):
        command = list(argv)
        if command[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, "", "")
        if command[:2] == ["docker", "create"]:
            return subprocess.CompletedProcess(argv, 0, "container-1\n", "")
        if command[:2] == ["docker", "start"]:
            return subprocess.CompletedProcess(argv, 0, "container-1\n", "")
        assert command[:3] == ["docker", "rm", "--force"]
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "a" * 64
    )
    monkeypatch.setattr("scripts.research.lab_node._inventory_request", inventory)
    monkeypatch.setattr("scripts.research.lab_node.subprocess.run", runner)
    arguments = Namespace(
        controller_config=controller_path,
        credential_file=controller.credential_file,
        host_id="controller-lab-01",
        agent_id="fixture-1",
        behavior="echo",
        delay_ms=0,
        crash_point=None,
        state_root=tmp_path / "node-state",
    )
    state = start_node(arguments)
    state_file = tmp_path / "node-state/ec-lab-01/ec-lab-01--fixture-1/node-state.json"
    assert load_node_state(state_file) == state
    assert credential_token(controller.credential_file) not in state_file.read_text()
    assert (state.state_dir / "terminal-release").is_dir()
    assert inventory_calls == [("POST", "/reservations")]

    arguments.retain_reservation = False
    assert stop_node(arguments) == "node: stopped"
    assert not state_file.exists()
    assert inventory_calls == [
        ("POST", "/reservations"),
        ("DELETE", "/reservations/fixture-1"),
    ]


def test_stop_retries_after_remote_failure_when_container_is_already_absent(
    tmp_path: Path, monkeypatch
) -> None:
    controller = _controller(tmp_path)
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    state_file = tmp_path / "node-state/ec-lab-01/ec-lab-01--fixture-1/node-state.json"
    state = NodeState(
        schema_version="lab-node-state.v1",
        phase="active",
        run_id=controller.run_id,
        agent_id="fixture-1",
        qualified_agent_id="ec-lab-01--fixture-1",
        reservation_id="reservation-1",
        declared_host_id="controller-lab-01",
        machine_id_sha256="a" * 64,
        container_id="container-1",
        container_name="edgecitadel-node-ec-lab-01--fixture-1",
        fixture_image_id=controller.fixture_image_id,
        config_path=state_file.parent / "native-control.json",
        state_dir=state_file.parent,
        log_path=state_file.parent / "fixture.log",
        reservation_state="active",
        started_at="2026-07-27T00:00:00Z",
    )
    write_node_state(state_file, state)
    inventory_calls: list[dict[str, object]] = []

    def inventory(_controller, _credential, method, suffix, body):
        assert (method, suffix) == ("PATCH", "/reservations/fixture-1/retain")
        inventory_calls.append(body)
        if len(inventory_calls) == 1:
            raise LabConfigError("lab inventory request failed")
        return None

    removals = 0

    def runner(argv, **_kwargs):
        nonlocal removals
        assert list(argv) == ["docker", "rm", "--force", state.container_name]
        removals += 1
        if removals == 1:
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(
            argv,
            1,
            "",
            f"Error response from daemon: No such container: {state.container_name}\n",
        )

    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "a" * 64
    )
    monkeypatch.setattr("scripts.research.lab_node._inventory_request", inventory)
    monkeypatch.setattr("scripts.research.lab_node.subprocess.run", runner)
    arguments = Namespace(
        controller_config=controller_path,
        credential_file=controller.credential_file,
        agent_id="fixture-1",
        state_root=tmp_path / "node-state",
        retain_reservation=True,
    )

    with pytest.raises(LabConfigError, match="inventory request failed"):
        stop_node(arguments)
    assert load_node_state(state_file) == state

    assert stop_node(arguments) == "node: retained"
    retained = load_node_state(state_file)
    assert retained.phase == "retained"
    assert retained.reservation_id == state.reservation_id
    assert inventory_calls == [inventory_calls[0], inventory_calls[0]]


def test_stop_does_not_hide_unexpected_docker_removal_failure(
    tmp_path: Path, monkeypatch
) -> None:
    controller = _controller(tmp_path)
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    state_file = tmp_path / "node-state/ec-lab-01/ec-lab-01--fixture-1/node-state.json"
    state = NodeState(
        schema_version="lab-node-state.v1",
        phase="active",
        run_id=controller.run_id,
        agent_id="fixture-1",
        qualified_agent_id="ec-lab-01--fixture-1",
        reservation_id="reservation-1",
        declared_host_id="controller-lab-01",
        machine_id_sha256="a" * 64,
        container_id="container-1",
        container_name="edgecitadel-node-ec-lab-01--fixture-1",
        fixture_image_id=controller.fixture_image_id,
        config_path=state_file.parent / "native-control.json",
        state_dir=state_file.parent,
        log_path=state_file.parent / "fixture.log",
        reservation_state="active",
        started_at="2026-07-27T00:00:00Z",
    )
    write_node_state(state_file, state)
    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "a" * 64
    )
    monkeypatch.setattr(
        "scripts.research.lab_node._inventory_request",
        lambda *_: (_ for _ in ()).throw(AssertionError("inventory must not run")),
    )
    monkeypatch.setattr(
        "scripts.research.lab_node.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(
            argv, 1, "", "permission denied\n"
        ),
    )
    arguments = Namespace(
        controller_config=controller_path,
        credential_file=controller.credential_file,
        agent_id="fixture-1",
        state_root=tmp_path / "node-state",
        retain_reservation=False,
    )

    with pytest.raises(subprocess.CalledProcessError):
        stop_node(arguments)
    assert load_node_state(state_file) == state


def test_inventory_preserves_only_known_safe_conflict_detail(
    tmp_path: Path, monkeypatch
) -> None:
    from scripts.research.lab_node import _inventory_request

    controller = _controller(tmp_path)

    def conflict(*_args, **_kwargs):
        raise urllib.error.HTTPError(
            controller.inventory_url,
            409,
            "Conflict",
            {},
            io.BytesIO(b'{"detail":"agent_id has an active reservation"}'),
        )

    monkeypatch.setattr("scripts.research.lab_node.urllib.request.urlopen", conflict)
    with pytest.raises(ValueError, match="agent_id has an active reservation"):
        _inventory_request(
            controller, controller.credential_file, "POST", "/reservations", {}
        )


def test_start_rejects_remote_loopback_before_inventory_or_docker(
    tmp_path: Path, monkeypatch
) -> None:
    controller = _controller(tmp_path)
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "d" * 64
    )
    monkeypatch.setattr(
        "scripts.research.lab_node._inventory_request",
        lambda *_: (_ for _ in ()).throw(AssertionError("inventory")),
    )
    arguments = Namespace(
        controller_config=controller_path,
        credential_file=controller.credential_file,
        host_id="gateway-lab-02",
        agent_id="fixture-1",
        behavior="echo",
        delay_ms=0,
        crash_point=None,
        state_root=tmp_path / "node-state",
    )
    try:
        start_node(arguments)
    except ValueError as error:
        assert "loopback" in str(error)
    else:
        raise AssertionError("expected loopback rejection")


def test_doctor_reports_safe_remote_target_without_reserving_or_creating(
    tmp_path: Path, monkeypatch
) -> None:
    controller = _controller(tmp_path)
    controller = ControllerConfig(
        **{
            **controller.__dict__,
            "nats_url": "nats://192.0.2.44:4222",
            "advertised_host": "controller.research.test",
        }
    )
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    calls: list[list[str]] = []

    def runner(argv, **_kwargs):
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "sha256:" + "c" * 64 + "\n", "")

    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "d" * 64
    )
    monkeypatch.setattr(
        "scripts.research.lab_node.shutil.which",
        lambda command: "/usr/bin/docker" if command == "docker" else None,
    )
    monkeypatch.setattr("scripts.research.lab_node.subprocess.run", runner)
    monkeypatch.setattr(
        "scripts.research.lab_node._inventory_request",
        lambda *_: (_ for _ in ()).throw(AssertionError("doctor must not reserve")),
    )

    report = doctor_node(
        Namespace(
            controller_config=controller_path,
            credential_file=controller.credential_file,
        )
    )

    assert report == {
        "schema_version": "lab-node-doctor.v1",
        "run_id": "ec-lab-01",
        "controller_host_id": "controller-lab-01",
        "machine_id_sha256": "d" * 64,
        "fixture_image_id": "sha256:" + "c" * 64,
        "controller_nats_host": "192.0.2.44",
    }
    assert calls == [["docker", "image", "inspect", controller.fixture_image_id]]


def test_doctor_publish_binds_active_node_to_source_and_route_facts(
    tmp_path: Path, monkeypatch
) -> None:
    controller = replace(
        _controller(tmp_path),
        nats_url="nats://192.0.2.44:4222",
        advertised_host="controller.research.test",
        advertised_ip="192.0.2.44",
    )
    controller_path = tmp_path / "controller.json"
    write_private_json(controller_path, controller.to_dict())
    state_file = tmp_path / "node-state/ec-lab-01/ec-lab-01--fixture-1/node-state.json"
    state = NodeState(
        schema_version="lab-node-state.v1",
        phase="active",
        run_id=controller.run_id,
        agent_id="fixture-1",
        qualified_agent_id="ec-lab-01--fixture-1",
        reservation_id="reservation-1",
        declared_host_id="gateway-lab-02",
        machine_id_sha256="d" * 64,
        container_id="container-1",
        container_name="edgecitadel-node-ec-lab-01--fixture-1",
        fixture_image_id=controller.fixture_image_id,
        config_path=state_file.parent / "native-control.json",
        state_dir=state_file.parent,
        log_path=state_file.parent / "fixture.log",
        reservation_state="active",
        started_at="2026-07-27T00:00:00Z",
    )
    write_node_state(state_file, state)
    published: list[dict[str, object]] = []

    def inventory(_controller, _credential, method, suffix, body):
        assert (method, suffix) == ("POST", "/node-reports")
        published.append(body)
        return {**body, "server_observed_peer_ip": "192.0.2.45"}

    monkeypatch.setattr(
        "scripts.research.lab_node._machine_id_sha256", lambda: "d" * 64
    )
    monkeypatch.setattr(
        "scripts.research.lab_node.shutil.which", lambda _: "/usr/bin/docker"
    )
    monkeypatch.setattr(
        "scripts.research.lab_node.subprocess.run",
        lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    monkeypatch.setattr("scripts.research.lab_node._inventory_request", inventory)
    monkeypatch.setattr(
        "scripts.research.lab_node._os_release", lambda: "Ubuntu 24.04.1 LTS"
    )
    monkeypatch.setattr(
        "scripts.research.lab_node.socket.gethostname", lambda: "gateway-lab-02"
    )
    monkeypatch.setattr("scripts.research.lab_node.platform.machine", lambda: "x86_64")
    monkeypatch.setattr(
        "scripts.research.lab_node.capture_clean_source_provenance",
        lambda _: SourceProvenance(
            controller.source_commit, False, controller.source_snapshot_sha256, "f" * 64
        ),
    )
    monkeypatch.setattr(
        "scripts.research.lab_node._network_path",
        lambda _: {
            "source_ip": "192.0.2.45",
            "destination_ip": "192.0.2.44",
            "interface": "eth0",
            "route_output_sha256": "a" * 64,
            "controller_dns_name": "controller.research.test",
        },
    )

    report = doctor_node(
        Namespace(
            controller_config=controller_path,
            credential_file=controller.credential_file,
            agent_id="fixture-1",
            host_id="gateway-lab-02",
            state_root=tmp_path / "node-state",
            publish=True,
        )
    )

    assert report["server_observed_peer_ip"] == "192.0.2.45"
    assert published[0]["reservation_id"] == "reservation-1"
    assert published[0]["launcher_source_commit"] == controller.source_commit
    assert published[0]["network_path"]["destination_ip"] == "192.0.2.44"
    assert credential_token(controller.credential_file) not in json.dumps(published[0])
