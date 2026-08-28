"""Durable ownership-state contracts for the lab controller."""

from __future__ import annotations

from pathlib import Path
from argparse import Namespace
from dataclasses import replace
import json
import os
import shutil
import sys
from types import SimpleNamespace
from datetime import UTC, datetime
import subprocess

import pytest

import scripts.research.lab_controller as controller_module
from scripts.research.artifact_env import OwnedResource
from scripts.research.check_artifact import ArtifactIssue, CheckReport
from scripts.research.lab_config import ControllerConfig
from scripts.research.lab_controller import (
    ControllerOwnershipState,
    export_fixture_image,
    load_controller_state,
    qualify_controller,
    start_controller,
    stop_controller,
    write_controller_state,
)
from scripts.research.lab_config import LabConfigError
from scripts.research.lab_runtime import FixtureImage, SourceProvenance
from scripts.research.lab_qualification import LabQualification
from scripts.research.preflight import PreflightReport


def _config(tmp_path: Path) -> ControllerConfig:
    return ControllerConfig(
        run_id="ec-lab-01", lab_variant="lifecycle", controller_host_id="controller-lab-01",
        compose_project="edgecitadel-artifact-ec-lab-01", bind_host="127.0.0.1",
        advertised_host="127.0.0.1", advertised_ip="127.0.0.1", app_url="http://127.0.0.1:18080",
        agg_url="http://127.0.0.1:18080", nats_url="nats://127.0.0.1:14222",
        monitor_url="http://127.0.0.1:18222", inventory_url="http://127.0.0.1:18080/api/lab/status",
        controller_machine_id_sha256="a" * 64, source_commit="d" * 40, source_snapshot_sha256="e" * 64,
        credential_sha256="b" * 64,
        credential_file=tmp_path / "scratch/transport-token", fixture_image_id="sha256:" + "c" * 64,
        state_dir=tmp_path / "state", evidence_dir=tmp_path / "evidence",
    )


def _ownership_state(
    tmp_path: Path,
    *,
    phase: str = "active",
    completed_cleanup_steps: tuple[str, ...] = (),
) -> tuple[Path, ControllerOwnershipState]:
    scratch_root = tmp_path / "scratch"
    raw_credential = scratch_root / "ec-lab-01/transport-token"
    config = replace(_config(tmp_path), credential_file=raw_credential)
    state_file = tmp_path / "lab/ec-lab-01/controller-state.json"
    state = ControllerOwnershipState(
        schema_version="lab-controller-state.v1",
        phase=phase,
        config=config,
        compose_file=tmp_path / "docker-compose.lab.yml",
        compose_environment={"LAB_RUN_ID": "ec-lab-01"},
        artifact_scratch_root=scratch_root,
        raw_credential_file=raw_credential,
        service_env_file=scratch_root / "ec-lab-01/service.env",
        owned_resources=(),
        completed_cleanup_steps=completed_cleanup_steps,
        exported_image_paths=(),
        controller_argv=("lab_controller.py", "start"),
        started_at="2026-07-27T00:00:00Z",
    )
    write_controller_state(state_file, state)
    return state_file, state


class _FakeStartEnvironment:
    def __init__(self, scratch_root: Path, repo_root: Path, timeline: list[str]) -> None:
        self.run_id = "ec-lab-01"
        self.scratch_dir = scratch_root / self.run_id
        self.credential_file = self.scratch_dir / "transport-token"
        self.owner_record = scratch_root / "owners/ec-lab-01.json"
        self.project = "edgecitadel-artifact-ec-lab-01"
        self.timeline = timeline
        self.fail_topology = False
        self.cleanup_calls = 0
        self.cleanup_completed = True
        self.cleanup_remaining: tuple[OwnedResource, ...] = ()
        self.cleanup_compose_file: Path | None = None
        self.cleanup_compose_env: dict[str, str] = {}
        self.repo_root = repo_root

    def create(self) -> _FakeStartEnvironment:
        self.timeline.append("environment-create")
        self.scratch_dir.mkdir(parents=True)
        self.credential_file.write_text("s" * 64 + "\n")
        self.credential_file.chmod(0o600)
        self.owner_record.parent.mkdir(parents=True, exist_ok=True)
        self.owner_record.write_text("{}\n")
        return self

    def start_topology(self, compose_file: Path, environment: object) -> None:
        del compose_file, environment
        self.timeline.append("compose-up")
        if self.fail_topology:
            raise RuntimeError("compose start failed")

    def owned_resources(self) -> tuple[OwnedResource, ...]:
        return (OwnedResource("network", "edgecitadel-artifact-ec-lab-01_default"),)

    def cleanup(self) -> object:
        self.cleanup_calls += 1
        self.cleanup_compose_file = getattr(self, "compose_file", None)
        self.cleanup_compose_env = dict(getattr(self, "compose_env", {}))
        self.timeline.append("artifact-cleanup")
        if self.cleanup_completed:
            shutil.rmtree(self.scratch_dir, ignore_errors=True)
            self.owner_record.unlink(missing_ok=True)
        return SimpleNamespace(
            attempted=self.cleanup_remaining,
            remaining=self.cleanup_remaining,
            credential_removed=self.cleanup_completed,
            state_removed=self.cleanup_completed,
            scratch_removed=self.cleanup_completed,
            recovery_record_removed=self.cleanup_completed,
            completed=self.cleanup_completed,
        )


def _start_harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    variant: str = "lifecycle",
) -> tuple[Namespace, _FakeStartEnvironment, Path, list[str]]:
    repo_root = tmp_path / "repo"
    (repo_root / "scripts/research").mkdir(parents=True)
    (repo_root / "scripts/research/nats-lab.conf.tpl").write_text(
        'authorization { token: "$NATS_TOKEN" }\n'
    )
    (repo_root / "scripts/research/docker-compose.lab.yml").write_text("services: {}\n")
    (repo_root / "scripts/research/toolchain.json").write_text(json.dumps({
        "nats_image": "nats@sha256:" + "a" * 64,
        "python_version": "3.12",
        "uv_version": "0.8.13",
    }))
    (repo_root / "schemas").mkdir()
    (repo_root / "schemas/research-manifest.v1.json").write_text("{}\n")
    timeline: list[str] = []
    scratch_root = (tmp_path / "artifact-scratch").resolve()
    environment = _FakeStartEnvironment(scratch_root, repo_root, timeline)

    def create(run_id: str, mode: str, output_root: Path) -> _FakeStartEnvironment:
        assert (run_id, mode) == ("ec-lab-01", "edgecitadel")
        assert output_root.is_absolute()
        assert os.environ["EC_ARTIFACT_SCRATCH_ROOT"] == str(scratch_root)
        return environment.create()

    monkeypatch.setattr(controller_module, "_repo_root", lambda: repo_root)
    monkeypatch.setattr(
        controller_module,
        "ArtifactEnvironment",
        SimpleNamespace(create=create),
    )
    monkeypatch.setattr(
        controller_module,
        "capture_clean_source_provenance",
        lambda root: timeline.append("source") or SourceProvenance(
            "d" * 40, False, "e" * 64, "f" * 64
        ),
    )
    monkeypatch.setattr(controller_module, "_controller_machine_id_sha256", lambda: "1" * 64)
    monkeypatch.setattr(controller_module, "_validate_host_platform", lambda: None, raising=False)
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(scratch_root))
    args = Namespace(
        run_id="ec-lab-01",
        host_id="controller-lab-01",
        lab_variant=variant,
        bind_host="127.0.0.1",
        advertise_host="127.0.0.1",
        state_root=tmp_path / "lab",
        http_port=None,
        nats_port=None,
        monitor_port=None,
        trusted_network_confirm=False,
    )
    return args, environment, repo_root, timeline


def _successful_preflight(config: ControllerConfig) -> PreflightReport:
    return PreflightReport(
        valid=True,
        checked_at="2026-07-27T00:00:01Z",
        checks=(),
        errors=(),
        config_snapshot=config.to_dict(),
    )


def test_controller_ownership_state_round_trips_atomically(tmp_path: Path) -> None:
    state = ControllerOwnershipState(
        schema_version="lab-controller-state.v1", phase="active", config=_config(tmp_path),
        compose_file=tmp_path / "docker-compose.lab.yml", compose_environment={"LAB_RUN_ID": "ec-lab-01"},
        artifact_scratch_root=tmp_path / "scratch", raw_credential_file=tmp_path / "scratch/transport-token",
        service_env_file=tmp_path / "state/service.env", owned_resources=(OwnedResource("network", "lab-net"),),
        completed_cleanup_steps=("compose-down",), exported_image_paths=(tmp_path / "fixture.tar",),
        controller_argv=("lab_controller.py", "start"), started_at="2026-07-27T00:00:00Z",
    )
    path = tmp_path / "state/controller-state.json"
    write_controller_state(path, state)
    assert load_controller_state(path) == state
    assert path.stat().st_mode & 0o777 == 0o600


def test_controller_preflight_passes_raw_credential_path_to_shared_preflight(
    tmp_path: Path, monkeypatch
) -> None:
    from scripts.research import lab_preflight

    real_shared_preflight = lab_preflight.run_preflight
    config = _config(tmp_path)
    config.credential_file.parent.mkdir(parents=True)
    raw_token = "x" * 64
    config.credential_file.write_text(raw_token + "\n")
    config.credential_file.chmod(0o600)
    captured = {}

    async def shared(request):
        captured["request"] = request
        return PreflightReport(
            valid=True,
            checked_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            checks=(),
            errors=(),
            config_snapshot=request.resolved_config,
        )

    replies = {
        "/api/system/status": {
            "nats_connected": True, "jetstream_stream_ok": True,
        },
        "/api/lab/status": {
            "run_id": config.run_id,
            "reservations": [], "reservation_events": [], "node_reports": [],
        },
        "/api/registry": [
            {"agent_id": "fixture-1", "agent_state": "online"},
        ],
        "/varz": {"server_id": "nats-1"},
    }

    class Response:
        status = 200

        def __init__(self, value):
            self.value = value

        def read(self):
            return json.dumps(self.value).encode()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def opener(request, **_kwargs):
        return Response(next(value for suffix, value in replies.items() if request.full_url.endswith(suffix)))

    monkeypatch.setattr(lab_preflight, "run_preflight", shared)
    report = __import__("asyncio").run(lab_preflight.run_controller_preflight(
        config,
        config.credential_file,
        ("fixture-1",),
        opener=opener,
    ))

    assert captured["request"].credential_file is config.credential_file
    assert captured["request"].resolved_config == config.to_dict()
    assert report.valid is True
    assert {check["name"] for check in report.checks} == {
        "system_status_semantic",
        "lab_inventory_authenticated",
        "registry_ready",
        "mqtt_not_listening",
        "fixture_image_immutable",
    }
    assert raw_token not in json.dumps(report.to_dict())
    assert report.config_snapshot["credential_file"] == "<credential-file>"
    assert report.config_snapshot["state_dir"] == "<run-state>"

    config.credential_file.write_text("malformed\n")
    config.credential_file.chmod(0o600)
    malformed_replies = {
        "/api/system/status": {"nats_connected": True, "jetstream_stream_ok": True},
        "/api/registry": [42, {"agent_id": None, "agent_state": "online"}],
        "/varz": {"mqtt": {}},
    }

    class Response:
        status = 200

        def __init__(self, value):
            self.value = value

        def read(self):
            return json.dumps(self.value).encode()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def malformed_opener(request, **_kwargs):
        return Response(next(
            value
            for suffix, value in malformed_replies.items()
            if request.full_url.endswith(suffix)
        ))

    monkeypatch.setattr(lab_preflight, "run_preflight", real_shared_preflight)
    malformed_report = __import__("asyncio").run(lab_preflight.run_controller_preflight(
        config, config.credential_file, opener=malformed_opener
    ))

    assert malformed_report.valid is False
    assert "registry_ready failed" in malformed_report.errors
    assert "lab_inventory_authenticated failed" in malformed_report.errors
    assert "malformed" not in json.dumps(malformed_report.to_dict())


def test_start_rejects_an_existing_active_state_before_runtime_work(tmp_path: Path) -> None:
    config = _config(tmp_path)
    state_file = tmp_path / "lab/ec-lab-01/controller-state.json"
    write_controller_state(state_file, ControllerOwnershipState(
        schema_version="lab-controller-state.v1", phase="active", config=config,
        compose_file=tmp_path / "docker-compose.lab.yml", compose_environment={},
        artifact_scratch_root=tmp_path / "scratch", raw_credential_file=config.credential_file,
        service_env_file=tmp_path / "service.env", owned_resources=(), completed_cleanup_steps=(),
        exported_image_paths=(), controller_argv=(), started_at="2026-07-27T00:00:00Z",
    ))
    with pytest.raises(LabConfigError, match="active"):
        start_controller(Namespace(
            run_id="ec-lab-01", host_id="controller-lab-01", lab_variant="lifecycle",
            bind_host="127.0.0.1", advertise_host="127.0.0.1", state_root=tmp_path / "lab",
        ))


def test_interrupted_state_replacement_preserves_a_complete_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file, old = _ownership_state(tmp_path)
    new = replace(old, phase="stopping")
    real_replace = controller_module.os.replace

    def interrupted(_source: Path, _destination: Path) -> None:
        raise OSError("simulated interruption")

    monkeypatch.setattr(controller_module.os, "replace", interrupted)
    with pytest.raises(OSError, match="simulated interruption"):
        write_controller_state(state_file, new)
    assert load_controller_state(state_file) == old
    assert not state_file.with_suffix(".json.tmp").exists()

    monkeypatch.setattr(controller_module.os, "replace", real_replace)
    write_controller_state(state_file, new)
    assert load_controller_state(state_file) == new

    stale = state_file.with_suffix(".json.tmp")
    stale.write_text('{"phase":"partial"')
    write_controller_state(state_file, old)
    assert load_controller_state(state_file) == old
    assert not stale.exists()


def test_start_validates_tools_addresses_and_source_before_outputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_host_validator = controller_module._validate_host_platform
    args, _environment, repo_root, timeline = _start_harness(tmp_path, monkeypatch)
    version_calls: list[list[str]] = []
    replies = {
        ("docker", "--version"): "Docker version 28.0.0",
        ("docker", "compose", "version", "--short"): "2.38.2",
        ("git", "--version"): "git version 2.50.1",
        ("node", "--version"): "v24.6.0",
        ("npm", "--version"): "11.5.1",
        ("npx", "--no-install", "playwright", "--version"): "Version 1.58.2",
    }

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        version_calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, replies[tuple(argv)] + "\n", "")

    versions = controller_module._validate_toolchain(
        repo_root, "operator-evidence", runner=runner
    )
    assert versions["node"] == "v24.6.0"
    assert versions["npm"] == "11.5.1"
    assert versions["playwright"] == "Version 1.58.2"
    assert version_calls[-3:] == [
        ["node", "--version"],
        ["npm", "--version"],
        ["npx", "--no-install", "playwright", "--version"],
    ]

    monkeypatch.setattr(controller_module.platform, "system", lambda: "Darwin")
    with pytest.raises(LabConfigError, match="Ubuntu"):
        real_host_validator()
    monkeypatch.setattr(controller_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(controller_module, "_os_release", lambda: "Ubuntu 24.04.1 LTS")
    monkeypatch.setattr(controller_module.platform, "machine", lambda: "arm64")
    with pytest.raises(LabConfigError, match="x86_64"):
        real_host_validator()
    monkeypatch.setattr(controller_module.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(controller_module.platform, "python_version", lambda: "3.11.9")
    with pytest.raises(LabConfigError, match="Python 3.12"):
        real_host_validator()

    monkeypatch.setattr(
        controller_module.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("100.64.10.10", 0))],
    )
    remote = Namespace(**{
        **vars(args),
        "bind_host": "100.64.10.10",
        "advertise_host": "controller-lab.internal",
        "http_port": 18080,
        "nats_port": 14222,
        "monitor_port": 18222,
        "trusted_network_confirm": True,
    })
    network = controller_module._validate_start_network(remote)
    assert network == ("100.64.10.10", "100.64.10.10", "18080", "14222", "18222")
    with pytest.raises(LabConfigError, match="trusted network"):
        controller_module._validate_start_network(
            Namespace(**{**vars(remote), "trusted_network_confirm": False})
        )
    monkeypatch.setattr(
        controller_module.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("100.64.10.11", 0))],
    )
    with pytest.raises(LabConfigError, match="bind address"):
        controller_module._validate_start_network(remote)
    with pytest.raises(LabConfigError, match="unspecified"):
        controller_module._validate_start_network(
            Namespace(**{**vars(args), "bind_host": "0.0.0.0"})
        )

    monkeypatch.setattr(
        controller_module,
        "capture_clean_source_provenance",
        lambda _root: (_ for _ in ()).throw(LabConfigError("source paths must be clean")),
    )
    with pytest.raises(LabConfigError, match="source paths must be clean"):
        start_controller(args)
    assert timeline == []
    assert not (args.state_root / args.run_id).exists()
    assert not (repo_root / "data/research/results/lab/ec-lab-01").exists()


def test_start_journals_raw_and_service_secrets_before_acquisitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, environment, _repo_root, timeline = _start_harness(tmp_path, monkeypatch)
    monkeypatch.setattr(
        controller_module, "_validate_toolchain", lambda *_args, **_kwargs: {
            "python": "Python 3.12.11", "docker": "Docker 28.0.0",
            "docker_compose": "2.38.2", "git": "git version 2.50.1",
            "node": "not-required", "npm": "not-required",
            "playwright": "not-required",
        }
    )
    observed: dict[str, object] = {}
    real_unlink = Path.unlink
    unlink_failures = {"count": 0}

    def unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.name == "transport-token" and unlink_failures["count"] < 2:
            unlink_failures["count"] += 1
            raise OSError("persistent rollback credential unlink failure")
        real_unlink(path, *args, **kwargs)

    environment.cleanup_completed = False
    monkeypatch.setattr(Path, "unlink", unlink)

    def stop_after_journal(state: ControllerOwnershipState, **_kwargs: object) -> object:
        persisted = load_controller_state(args.state_root / args.run_id / "controller-state.json")
        raw = persisted.raw_credential_file.read_bytes()
        service = persisted.service_env_file.read_bytes()
        observed.update(state=persisted, raw=raw, service=service)
        assert persisted.phase == "starting"
        assert raw == b"s" * 64 + b"\n"
        assert service == b"NATS_TOKEN=" + b"s" * 64 + b"\n"
        serialized = json.dumps(controller_module._state_dict(persisted))
        assert "s" * 64 not in serialized
        assert service.decode() not in serialized
        timeline.append("nats-validation")
        raise RuntimeError("stop after journal")

    monkeypatch.setattr(
        controller_module,
        "_validate_nats_configuration",
        stop_after_journal,
        raising=False,
    )
    monkeypatch.setattr(controller_module, "finalize_bundle", lambda *_args: "INVALID")
    with pytest.raises(RuntimeError, match="stop after journal"):
        start_controller(args)
    assert observed["state"]
    assert timeline[:3] == ["source", "environment-create", "nats-validation"]
    failed = load_controller_state(args.state_root / args.run_id / "controller-state.json")
    assert failed.phase == "failed"
    assert failed.artifact_scratch_root == environment.scratch_dir.parent
    assert failed.raw_credential_file.exists()
    assert not failed.service_env_file.exists()

    def recovery_runner(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if "run_artifact.py" in " ".join(argv):
            shutil.rmtree(environment.scratch_dir, ignore_errors=True)
            environment.owner_record.unlink(missing_ok=True)
        if "inspect" in argv:
            return subprocess.CompletedProcess(argv, 1, "", "not found")
        return subprocess.CompletedProcess(argv, 0, "", "")

    cleanup = stop_controller(
        args.state_root / args.run_id / "controller-state.json",
        runner=recovery_runner,
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("failed-start recovery must not call HTTP")
        ),
    )
    assert cleanup["completed"] is True
    assert load_controller_state(
        args.state_root / args.run_id / "controller-state.json"
    ).phase == "stopped"


def test_nats_validation_precedes_build_and_retains_only_status_and_hash(
    tmp_path: Path
) -> None:
    state_file, state = _ownership_state(tmp_path, phase="starting")
    del state_file
    state.config.evidence_dir.mkdir(parents=True)
    state.raw_credential_file.parent.mkdir(parents=True)
    state.raw_credential_file.write_text("n" * 64 + "\n")
    state.raw_credential_file.chmod(0o600)
    state.service_env_file.write_text("NATS_TOKEN=" + "n" * 64 + "\n")
    nats_config = state.service_env_file.with_name("nats-lab.conf")
    nats_config.write_text('authorization { token: "$NATS_TOKEN" }\n')
    state = replace(state, compose_environment={
        "LAB_NATS_CONFIG": str(nats_config),
        "LAB_NATS_IMAGE": "nats@sha256:" + "a" * 64,
    })
    calls: list[list[str]] = []

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 1, "invalid config\n", "")

    with pytest.raises(LabConfigError, match="NATS configuration validation failed"):
        controller_module._validate_nats_configuration(state, runner=runner)
    assert calls == [[
        "docker", "run", "--rm", "--env-file", str(state.service_env_file),
        "--mount", f"type=bind,src={nats_config},dst=/etc/nats/nats.conf,readonly",
        state.compose_environment["LAB_NATS_IMAGE"], "-t", "-c", "/etc/nats/nats.conf",
    ]]
    receipt = json.loads((state.config.evidence_dir / "nats-validation.json").read_text())
    assert receipt == {
        "config_sha256": controller_module.sha256_file(nats_config),
        "exit_status": 1,
    }
    assert "n" * 64 not in json.dumps(receipt)


def test_start_builds_immutable_images_and_retains_runtime_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, environment, _repo_root, timeline = _start_harness(tmp_path, monkeypatch)
    monkeypatch.setattr(controller_module, "_validate_toolchain", lambda *_a, **_k: {
        "python": "Python 3.12.11", "docker": "Docker 28.0.0",
        "docker_compose": "2.38.2", "git": "git version 2.50.1",
        "node": "not-required", "npm": "not-required", "playwright": "not-required",
    })
    image_ids = {
        "edgecitadel-lab-aggregator:ec-lab-01": "sha256:" + "2" * 64,
        "edgecitadel-lab-dashboard:ec-lab-01": "sha256:" + "3" * 64,
    }
    calls: list[list[str]] = []

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        if argv[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, image_ids[argv[-1]] + "\n", "")
        if argv[:2] == ["docker", "compose"] and "config" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                f"services:\n  source: {_repo_root}\n  scratch: {environment.scratch_dir}\n",
                "",
            )
        if argv[:2] == ["docker", "compose"] and "port" in argv:
            port = {"80": "18080", "4222": "14222", "8222": "18222"}[argv[-1]]
            return subprocess.CompletedProcess(argv, 0, f"127.0.0.1:{port}\n", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(controller_module.subprocess, "run", runner)
    monkeypatch.setattr(
        controller_module,
        "build_fixture_image",
        lambda *_args, **_kwargs: FixtureImage(
            "sha256:" + "4" * 64, "5" * 64, "6" * 64, "2026-07-27T00:00:00Z"
        ),
    )
    monkeypatch.setattr(
        controller_module,
        "run_controller_preflight",
        lambda config, _credential: _successful_preflight(config),
    )

    config = start_controller(args)

    assert config.advertised_ip == "127.0.0.1"
    assert config.app_url == "http://127.0.0.1:18080"
    assert timeline[-1] == "compose-up"
    state = load_controller_state(args.state_root / args.run_id / "controller-state.json")
    assert state.phase == "active"
    assert state.compose_environment["LAB_AGGREGATOR_IMAGE"] == image_ids[
        "edgecitadel-lab-aggregator:ec-lab-01"
    ]
    assert state.compose_environment["LAB_DASHBOARD_IMAGE"] == image_ids[
        "edgecitadel-lab-dashboard:ec-lab-01"
    ]
    image_names = {item.name for item in state.owned_resources if item.kind == "image"}
    assert image_names == {
        "edgecitadel-lab-aggregator:ec-lab-01",
        "edgecitadel-lab-dashboard:ec-lab-01",
        "edgecitadel-lab-fixture:ec-lab-01",
        *image_ids.values(),
        config.fixture_image_id,
    }
    assert any("build" in call for call in calls if call[:2] == ["docker", "compose"])
    assert (config.evidence_dir / "controller-start.json").is_file()
    assert (config.evidence_dir / "compose-config.yml").is_file()
    assert (config.evidence_dir / "preflight.json").is_file()
    assert (config.evidence_dir / "lab-observations.jsonl").is_file()
    rendered = (config.evidence_dir / "compose-config.yml").read_text()
    assert str(_repo_root) not in rendered
    assert str(environment.scratch_dir.parent) not in rendered
    assert "$SOURCE_ROOT" in rendered or "<artifact-state>" in rendered
    assert environment.cleanup_calls == 0


def test_image_inspect_failure_removes_exact_tags_and_private_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for failure in ("application", "fixture"):
        case = tmp_path / failure
        args, environment, _repo_root, _timeline = _start_harness(case, monkeypatch)
        monkeypatch.setattr(controller_module, "_validate_toolchain", lambda *_a, **_k: {
            "python": "Python 3.12.11", "docker": "Docker 28.0.0",
            "docker_compose": "2.38.2", "git": "git version 2.50.1",
            "node": "not-required", "npm": "not-required", "playwright": "not-required",
        })
        calls: list[list[str]] = []

        def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            calls.append(list(argv))
            if argv[:3] == ["docker", "image", "inspect"]:
                if failure == "application" and "aggregator" in argv[-1]:
                    return subprocess.CompletedProcess(argv, 0, "mutable\n", "")
                return subprocess.CompletedProcess(argv, 0, "sha256:" + "2" * 64 + "\n", "")
            return subprocess.CompletedProcess(argv, 0, "", "")

        monkeypatch.setattr(controller_module.subprocess, "run", runner)
        if failure == "fixture":
            monkeypatch.setattr(
                controller_module,
                "build_fixture_image",
                lambda *_a, **_k: (_ for _ in ()).throw(LabConfigError("fixture inspect failed")),
            )
        monkeypatch.setattr(controller_module, "finalize_bundle", lambda *_args: "INVALID")
        with pytest.raises((LabConfigError, RuntimeError)):
            start_controller(args)
        state = load_controller_state(args.state_root / args.run_id / "controller-state.json")
        assert state.phase == "failed"
        assert not state.raw_credential_file.exists()
        assert not state.service_env_file.exists()
        removed = {call[-1] for call in calls if call[:3] == ["docker", "image", "rm"]}
        assert "edgecitadel-lab-aggregator:ec-lab-01" in removed
        assert "edgecitadel-lab-dashboard:ec-lab-01" in removed
        if failure == "fixture":
            assert "edgecitadel-lab-fixture:ec-lab-01" in removed
        assert environment.cleanup_calls == 1


def test_compose_start_failure_rolls_back_once_and_persists_failed_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, environment, _repo_root, _timeline = _start_harness(tmp_path, monkeypatch)
    environment.fail_topology = True
    environment.cleanup_completed = False
    environment.cleanup_remaining = (
        OwnedResource("network", "edgecitadel-artifact-ec-lab-01_default"),
    )
    monkeypatch.setattr(controller_module, "_validate_toolchain", lambda *_a, **_k: {
        "python": "Python 3.12.11", "docker": "Docker 28.0.0",
        "docker_compose": "2.38.2", "git": "git version 2.50.1",
        "node": "not-required", "npm": "not-required", "playwright": "not-required",
    })
    removed: list[str] = []
    removal_attempts: dict[str, int] = {}

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[:3] == ["docker", "image", "inspect"]:
            if removal_attempts.get(argv[-1], 0):
                return subprocess.CompletedProcess(argv, 0, "present\n", "")
            return subprocess.CompletedProcess(argv, 0, "sha256:" + ("2" if "aggregator" in argv[-1] else "3") * 64 + "\n", "")
        if argv[:3] == ["docker", "image", "rm"]:
            removed.append(argv[-1])
            removal_attempts[argv[-1]] = removal_attempts.get(argv[-1], 0) + 1
            if argv[-1] == "edgecitadel-lab-aggregator:ec-lab-01":
                return subprocess.CompletedProcess(argv, 1, "", "in use")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(controller_module.subprocess, "run", runner)
    monkeypatch.setattr(
        controller_module, "build_fixture_image", lambda *_a, **_k: FixtureImage(
            "sha256:" + "4" * 64, "5" * 64, "6" * 64, "2026-07-27T00:00:00Z"
        )
    )
    monkeypatch.setattr(controller_module, "finalize_bundle", lambda *_args: "INVALID")
    with pytest.raises(RuntimeError, match="compose start failed"):
        start_controller(args)
    state = load_controller_state(args.state_root / args.run_id / "controller-state.json")
    assert state.phase == "failed"
    assert environment.cleanup_calls == 1
    assert environment.cleanup_compose_file == state.compose_file
    assert environment.cleanup_compose_env["LAB_AGGREGATOR_IMAGE"] == (
        "sha256:" + "2" * 64
    )
    assert set(removed) >= {item.name for item in state.owned_resources if item.kind == "image"}
    cleanup = json.loads(
        (args.state_root / args.run_id / "cleanup.json").read_text()
    )
    assert cleanup["completed"] is False
    assert cleanup["owned_resources_removed"] is False
    assert cleanup["remaining"] == [
        {
            "kind": "image",
            "name": "edgecitadel-lab-aggregator:ec-lab-01",
        },
        {
            "kind": "network",
            "name": "edgecitadel-artifact-ec-lab-01_default",
        },
    ]

    def recovery_runner(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if "run_artifact.py" in " ".join(argv):
            shutil.rmtree(environment.scratch_dir, ignore_errors=True)
            environment.owner_record.unlink(missing_ok=True)
        if "inspect" in argv:
            return subprocess.CompletedProcess(argv, 1, "", "not found")
        return subprocess.CompletedProcess(argv, 0, "", "")

    recovered = stop_controller(
        args.state_root / args.run_id / "controller-state.json",
        runner=recovery_runner,
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("failed-start recovery must not call HTTP")
        ),
    )
    assert recovered["completed"] is True
    assert load_controller_state(
        args.state_root / args.run_id / "controller-state.json"
    ).phase == "stopped"


def test_failed_preflight_removes_secrets_before_one_invalid_finalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args, environment, _repo_root, _timeline = _start_harness(tmp_path, monkeypatch)
    monkeypatch.setattr(controller_module, "_validate_toolchain", lambda *_a, **_k: {
        "python": "Python 3.12.11", "docker": "Docker 28.0.0",
        "docker_compose": "2.38.2", "git": "git version 2.50.1",
        "node": "not-required", "npm": "not-required", "playwright": "not-required",
    })

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, "sha256:" + ("2" if "aggregator" in argv[-1] else "3") * 64 + "\n", "")
        if argv[:2] == ["docker", "compose"] and "config" in argv:
            return subprocess.CompletedProcess(argv, 0, "services: {}\n", "")
        if argv[:2] == ["docker", "compose"] and "port" in argv:
            port = {"80": "18080", "4222": "14222", "8222": "18222"}[argv[-1]]
            return subprocess.CompletedProcess(argv, 0, f"127.0.0.1:{port}\n", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(controller_module.subprocess, "run", runner)
    monkeypatch.setattr(
        controller_module, "build_fixture_image", lambda *_a, **_k: FixtureImage(
            "sha256:" + "4" * 64, "5" * 64, "6" * 64, "2026-07-27T00:00:00Z"
        )
    )
    monkeypatch.setattr(
        controller_module,
        "run_controller_preflight",
        lambda config, _credential: PreflightReport(
            False, "2026-07-27T00:00:01Z", (), ("registry_ready failed",),
            config.to_dict(),
        ),
    )
    finalizations: list[str] = []
    real_finalizer = controller_module.finalize_bundle
    real_schema = Path(__file__).resolve().parents[2] / "schemas/research-manifest.v1.json"
    (_repo_root / "schemas/research-manifest.v1.json").write_bytes(
        real_schema.read_bytes()
    )
    real_unlink = Path.unlink
    controller_unlink = {"raised": False}

    def unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.name == "controller.json" and not controller_unlink["raised"]:
            controller_unlink["raised"] = True
            raise OSError("transient controller config unlink failure")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)

    def finalizer(_bundle: Path, manifest: object, _schema: Path) -> str:
        state = load_controller_state(args.state_root / args.run_id / "controller-state.json")
        assert not state.raw_credential_file.exists()
        assert not state.service_env_file.exists()
        assert manifest["status"] == "INVALID"
        assert real_finalizer(
            _bundle,
            manifest,
            _repo_root / "schemas/research-manifest.v1.json",
        ) == "INVALID"
        assert (_bundle / "manifest.json").is_file()
        finalizations.append("INVALID")
        return "INVALID"

    monkeypatch.setattr(controller_module, "finalize_bundle", finalizer)
    with pytest.raises(LabConfigError, match="preflight"):
        start_controller(args)
    assert finalizations == ["INVALID"]
    assert environment.cleanup_calls == 1
    state_file = args.state_root / args.run_id / "controller-state.json"
    assert load_controller_state(state_file).phase == "failed"
    cleanup = stop_controller(
        state_file,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sealed INVALID recovery must not call Docker")
        ),
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sealed INVALID recovery must not call HTTP")
        ),
    )
    assert cleanup["completed"] is True
    assert load_controller_state(state_file).phase == "stopped"


def test_fresh_process_stop_uses_only_persisted_ownership_and_preserves_foreign_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file, state = _ownership_state(tmp_path)
    image_id = "sha256:" + "1" * 64
    image_tag = "edgecitadel-lab-fixture:ec-lab-01"
    owned = (
        OwnedResource("network", "owned-net"),
        OwnedResource("image", image_tag),
        OwnedResource("image", image_id),
    )
    export = tmp_path / "exports/fixture.tar"
    export.parent.mkdir()
    export.write_bytes(b"fixture")
    nats_config = state.service_env_file.with_name("nats-lab.conf")
    state = replace(
        state,
        compose_environment={
            **state.compose_environment,
            "LAB_NATS_CONFIG": str(nats_config),
        },
        owned_resources=owned,
        exported_image_paths=(export,),
    )
    write_controller_state(state_file, state)
    run_scratch = state.artifact_scratch_root / state.config.run_id
    run_scratch.mkdir(parents=True)
    state.raw_credential_file.write_text("a" * 64 + "\n")
    state.raw_credential_file.chmod(0o600)
    state.service_env_file.write_text("NATS_TOKEN=" + "a" * 64 + "\n")
    nats_config.write_text("authorization {}\n")
    owner = state.artifact_scratch_root / "owners" / f"{state.config.run_id}.json"
    owner.parent.mkdir()
    owner.write_text("{}\n")
    state.config.evidence_dir.mkdir(parents=True)
    (state.config.evidence_dir / "lab-observations.jsonl").write_text("")
    inventory = {
        "run_id": state.config.run_id,
        "reservations": [],
        "reservation_events": [],
        "node_reports": [],
    }
    timeline: list[str] = []
    calls: list[list[str]] = []
    after_cleanup = False
    removed_images: set[str] = set()
    evidence_dir = state.config.evidence_dir
    run_id = state.config.run_id
    artifact_scratch_root = state.artifact_scratch_root

    class Response:
        status = 200

        def read(self) -> bytes:
            return json.dumps(inventory).encode()

        def __enter__(self) -> Response:
            timeline.append("snapshot")
            return self

        def __exit__(self, *_args: object) -> bool:
            return False

    def opener(request: object, **_kwargs: object) -> Response:
        assert getattr(request, "headers")["Authorization"] == "Bearer " + "a" * 64
        return Response()

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal after_cleanup
        calls.append(list(argv))
        if argv[:2] == ["docker", "compose"]:
            raw = evidence_dir / "raw/lab"
            assert all(
                (raw / name).is_file()
                for name in (
                    "inventory.json",
                    "reservation-events.json",
                    "node-reports.json",
                    "controller-observations.json",
                )
            )
            timeline.append("compose")
        elif "run_artifact.py" in " ".join(argv):
            assert argv == [
                str(controller_module._repo_root() / "scripts/research/run-python"),
                str(controller_module._repo_root() / "scripts/research/run_artifact.py"),
                "cleanup",
                "--run-id",
                run_id,
                "--scratch-root",
                str(artifact_scratch_root),
            ]
            timeline.append("artifact")
            shutil.rmtree(run_scratch)
            owner.unlink()
            after_cleanup = True
        elif argv[:4] == ["docker", "network", "ls", "--format"]:
            value = "foreign-net\n" if after_cleanup else "owned-net\nforeign-net\n"
            return subprocess.CompletedProcess(argv, 0, value, "")
        elif argv[:3] == ["docker", "image", "ls"]:
            value = "" if after_cleanup else image_id + "\n"
            return subprocess.CompletedProcess(argv, 0, value, "")
        elif argv[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(
                argv,
                1 if argv[3] in removed_images else 0,
                "",
                "",
            )
        elif argv[:3] == ["docker", "image", "rm"]:
            removed_images.add(argv[3])
            return subprocess.CompletedProcess(argv, 0, "", "")
        elif len(argv) >= 3 and argv[2] == "inspect":
            return subprocess.CompletedProcess(argv, 1, "", "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    finalizer_calls: list[str] = []
    monkeypatch.setattr(
        controller_module,
        "_build_lab_manifest",
        lambda _state, cleanup, **_kwargs: {"status": "PENDING"},
    )
    monkeypatch.setattr(
        controller_module,
        "require_complete_lab_manifest",
        lambda *_args: finalizer_calls.append("require"),
    )
    monkeypatch.setattr(
        controller_module,
        "finalize_bundle",
        lambda *_args: finalizer_calls.append("finalize") or "PASS",
    )
    del state

    cleanup = stop_controller(state_file, runner=runner, opener=opener)

    assert timeline == ["snapshot", "compose", "artifact"]
    assert cleanup["owned_resources_removed"] is True
    assert cleanup["foreign_resources_touched"] is False
    assert cleanup["remaining"] == []
    assert export.exists() is False
    assert ["docker", "image", "rm", image_tag] in calls
    assert ["docker", "image", "rm", image_id] in calls
    assert not any("--rmi" in call for call in calls)
    assert finalizer_calls == ["require", "finalize"]
    assert load_controller_state(state_file).phase == "stopped"

    removal_state = replace(
        load_controller_state(state_file),
        owned_resources=(OwnedResource("image", image_tag),),
    )

    def failed_removal(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        if argv[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(argv, 0, "present\n", "")
        if argv[:3] == ["docker", "image", "rm"]:
            return subprocess.CompletedProcess(argv, 1, "", "in use")
        raise AssertionError(argv)

    with pytest.raises(LabConfigError, match="image cleanup failed"):
        controller_module._remove_images_and_exports(
            removal_state,
            runner=failed_removal,
        )


def test_stop_resume_skips_completed_compose_and_artifact_steps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        controller_module,
        "_build_lab_manifest",
        lambda *_args, **_kwargs: {"status": "PENDING"},
    )
    monkeypatch.setattr(
        controller_module,
        "require_complete_lab_manifest",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        controller_module,
        "finalize_bundle",
        lambda *_args, **_kwargs: "PASS",
    )

    for phase in ("starting", "stopping", "failed"):
        state_file, _ = _ownership_state(
            tmp_path / phase,
            phase=phase,
            completed_cleanup_steps=(
                "inventory-snapshotted",
                "compose-down",
                "artifact-cleanup",
            ),
        )
        (state_file.parent / "docker-inventory-before.json").write_text("[]\n")

        def runner(
            argv: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            if argv[:2] == ["docker", "compose"] or "run_artifact.py" in " ".join(argv):
                raise AssertionError("completed cleanup step repeated")
            return subprocess.CompletedProcess(argv, 0, "", "")

        cleanup = stop_controller(state_file, runner=runner)

        assert cleanup["completed"] is True
        assert load_controller_state(state_file).phase == "stopped"

    partial_root = tmp_path / "partial-snapshot"
    partial_state_file, partial_state = _ownership_state(
        partial_root,
        phase="stopping",
    )
    partial_state.config.evidence_dir.mkdir(parents=True)
    (partial_state.config.evidence_dir / "lab-observations.jsonl").write_text("")
    retained_inventory = {
        "run_id": partial_state.config.run_id,
        "reservations": [],
        "reservation_events": [{"sequence": 1, "event": "reserved"}],
        "node_reports": [],
    }
    raw = partial_state.config.evidence_dir / "raw/lab"
    raw.mkdir(parents=True)
    (raw / "inventory.json").write_text(json.dumps(retained_inventory) + "\n")
    (partial_state_file.parent / "docker-inventory-before.json").write_text("[]\n")
    partial_calls: list[list[str]] = []

    def partial_runner(
        argv: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        partial_calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    partial_cleanup = stop_controller(
        partial_state_file,
        runner=partial_runner,
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retained inventory must be authoritative")
        ),
    )
    assert partial_cleanup["completed"] is True
    assert json.loads((raw / "reservation-events.json").read_text()) == [
        {"sequence": 1, "event": "reserved"}
    ]
    assert any("run_artifact.py" in " ".join(call) for call in partial_calls)

    receipt_root = tmp_path / "partial-receipt"
    verification_steps = (
        "inventory-snapshotted",
        "compose-down",
        "artifact-cleanup",
        "images-exports-removed",
        "private-files-removed",
        "owned-resources-verified",
        "foreign-resources-verified",
    )
    receipt_state_file, receipt_state = _ownership_state(
        receipt_root,
        phase="stopping",
        completed_cleanup_steps=verification_steps,
    )
    (receipt_state_file.parent / "docker-inventory-before.json").write_text("[]\n")
    receipt_state.config.evidence_dir.mkdir(parents=True)
    retained_cleanup = {
        "completed": True,
        "attempted": [],
        "remaining": [],
        "owned_resources_removed": True,
        "foreign_resources_touched": False,
        "credential_removed": True,
        "artifact_state_removed": True,
        "artifact_scratch_removed": True,
        "artifact_recovery_record_removed": True,
        "completed_at": "2026-07-27T00:00:06Z",
    }
    (receipt_state_file.parent / "cleanup.json").write_text(
        json.dumps(retained_cleanup) + "\n"
    )
    receipt_cleanup = stop_controller(
        receipt_state_file,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("journaled verification must not repeat Docker calls")
        ),
    )
    assert receipt_cleanup == retained_cleanup
    assert json.loads(
        (receipt_state.config.evidence_dir / "raw/lab/cleanup.json").read_text()
    ) == retained_cleanup


def test_stopped_state_returns_cached_cleanup_without_docker_or_refinalizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file, state = _ownership_state(tmp_path, phase="stopped")
    cleanup = {
        "completed": True,
        "attempted": [],
        "remaining": [],
        "owned_resources_removed": True,
        "foreign_resources_touched": False,
        "credential_removed": True,
        "artifact_state_removed": True,
        "artifact_scratch_removed": True,
        "artifact_recovery_record_removed": True,
        "completed_at": "2026-07-27T00:00:06Z",
    }
    (state_file.parent / "cleanup.json").write_text(json.dumps(cleanup) + "\n")
    monkeypatch.setattr(
        controller_module,
        "finalize_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached stop refinalized")
        ),
        raising=False,
    )

    assert stop_controller(
        state_file,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached stop called Docker")
        ),
    ) == cleanup
    assert load_controller_state(state_file) == state

    steps = (
        "inventory-snapshotted",
        "compose-down",
        "artifact-cleanup",
        "images-exports-removed",
        "private-files-removed",
        "owned-resources-verified",
        "foreign-resources-verified",
        "cleanup-written",
    )
    recovery_state_file, recovery_state = _ownership_state(
        tmp_path / "recovery",
        phase="stopping",
        completed_cleanup_steps=steps,
    )
    recovery_state.config.evidence_dir.mkdir(parents=True)
    (recovery_state.config.evidence_dir / "manifest.json").write_text("{}\n")
    recovery_cleanup = {
        "completed": True,
        "attempted": [],
        "remaining": [],
        "owned_resources_removed": True,
        "foreign_resources_touched": False,
        "credential_removed": True,
        "artifact_state_removed": True,
        "artifact_scratch_removed": True,
        "artifact_recovery_record_removed": True,
        "completed_at": "2026-07-27T00:00:06Z",
    }
    (recovery_state_file.parent / "cleanup.json").write_text(
        json.dumps(recovery_cleanup) + "\n"
    )
    checks: list[tuple[Path, str, Path]] = []

    def checker(
        bundle: Path, *, expected_kind: str, source_root: Path
    ) -> CheckReport:
        checks.append((bundle, expected_kind, source_root))
        return CheckReport(True, ())

    monkeypatch.setattr(controller_module, "check_bundle", checker, raising=False)
    monkeypatch.setattr(
        controller_module,
        "finalize_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("existing manifest was refinalized")
        ),
        raising=False,
    )

    assert stop_controller(
        recovery_state_file,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("completed step repeated")
        ),
    ) == recovery_cleanup
    recovered = load_controller_state(recovery_state_file)
    assert recovered.phase == "stopped"
    assert recovered.completed_cleanup_steps[-1] == "manifest-finalized"
    assert checks == [
        (
            recovery_state.config.evidence_dir,
            "lab",
            controller_module._repo_root().resolve(),
        )
    ]

    invalid_state_file, invalid_state = _ownership_state(
        tmp_path / "invalid-recovery",
        phase="stopping",
        completed_cleanup_steps=steps,
    )
    invalid_state.config.evidence_dir.mkdir(parents=True)
    (invalid_state.config.evidence_dir / "manifest.json").write_text(
        '{"status":"INVALID"}\n'
    )
    (invalid_state_file.parent / "cleanup.json").write_text(
        json.dumps(recovery_cleanup) + "\n"
    )
    monkeypatch.setattr(
        controller_module,
        "check_bundle",
        lambda *_args, **_kwargs: CheckReport(
            False,
            (
                ArtifactIssue(
                    "EVIDENCE_STATUS_INVALID",
                    "manifest.json",
                    "bundle is explicitly invalid",
                ),
                ArtifactIssue(
                    "LAB_RESERVATION_HISTORY_INCOMPLETE",
                    "raw/lab/reservation-events.json",
                    "reservation history is incomplete",
                ),
            ),
        ),
    )
    assert stop_controller(
        invalid_state_file,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sealed invalid recovery repeated Docker")
        ),
    ) == recovery_cleanup
    assert load_controller_state(invalid_state_file).phase == "stopped"


def test_successful_stop_writes_complete_cleanup_and_finalizes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_file, state = _ownership_state(tmp_path)
    run_scratch = state.artifact_scratch_root / state.config.run_id
    run_scratch.mkdir(parents=True)
    state.raw_credential_file.write_text("a" * 64 + "\n")
    state.raw_credential_file.chmod(0o600)
    state.service_env_file.write_text("NATS_TOKEN=" + "a" * 64 + "\n")
    owner = state.artifact_scratch_root / "owners" / f"{state.config.run_id}.json"
    owner.parent.mkdir()
    owner.write_text("{}\n")
    state.config.evidence_dir.mkdir(parents=True)
    timeline: list[str] = []
    inventories = iter((frozenset(), frozenset()))

    def snapshot(_state: ControllerOwnershipState, *, opener: object) -> None:
        del opener
        timeline.append("snapshot")

    def runner(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        if argv[:2] == ["docker", "compose"]:
            timeline.append("compose")
        elif "run_artifact.py" in " ".join(argv):
            timeline.append("artifact")
            state.raw_credential_file.unlink()
            owner.unlink()
            for child in tuple(run_scratch.iterdir()):
                child.unlink()
            run_scratch.rmdir()
        return subprocess.CompletedProcess(argv, 0, "", "")

    manifest = {"status": "PENDING"}
    finalizer_calls: list[str] = []
    monkeypatch.setattr(controller_module, "_snapshot_stop_evidence", snapshot, raising=False)
    monkeypatch.setattr(
        controller_module,
        "_docker_inventory",
        lambda _runner: next(inventories),
        raising=False,
    )
    monkeypatch.setattr(
        controller_module,
        "_build_lab_manifest",
        lambda _state, cleanup, **_kwargs: manifest,
        raising=False,
    )
    monkeypatch.setattr(
        controller_module,
        "require_complete_lab_manifest",
        lambda *_args: finalizer_calls.append("require"),
        raising=False,
    )
    monkeypatch.setattr(
        controller_module,
        "finalize_bundle",
        lambda *_args: finalizer_calls.append("finalize") or "PASS",
    )

    cleanup = stop_controller(
        state_file,
        runner=runner,
        opener=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("snapshot is patched")
        ),
    )

    assert timeline[:3] == ["snapshot", "compose", "artifact"]
    assert set(cleanup) == {
        "completed",
        "attempted",
        "remaining",
        "owned_resources_removed",
        "foreign_resources_touched",
        "credential_removed",
        "artifact_state_removed",
        "artifact_scratch_removed",
        "artifact_recovery_record_removed",
        "completed_at",
    }
    assert cleanup["completed"] is True
    assert cleanup["remaining"] == []
    assert finalizer_calls == ["require", "finalize"]
    stopped = load_controller_state(state_file)
    assert stopped.phase == "stopped"
    assert stopped.completed_cleanup_steps.count("manifest-finalized") == 1


def test_export_uses_persisted_immutable_fixture_and_journals_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config(tmp_path)
    state_file = tmp_path / "lab/ec-lab-01/controller-state.json"
    write_controller_state(state_file, ControllerOwnershipState(
        schema_version="lab-controller-state.v1", phase="active", config=config,
        compose_file=tmp_path / "docker-compose.lab.yml", compose_environment={},
        artifact_scratch_root=tmp_path / "scratch", raw_credential_file=config.credential_file,
        service_env_file=tmp_path / "service.env", owned_resources=(), completed_cleanup_steps=(),
        exported_image_paths=(), controller_argv=(), started_at="2026-07-27T00:00:00Z",
    ))
    output = tmp_path / "exports/fixture.tar"
    result_file = tmp_path / "exports/result.json"
    calls: list[list[str]] = []

    def runner(argv, **_kwargs):
        calls.append(list(argv))
        Path(argv[argv.index("--output") + 1]).write_bytes(b"fixture-image")
        return __import__("subprocess").CompletedProcess(argv, 0, "", "")

    result = export_fixture_image(state_file, output, result_file, runner=runner)

    assert calls == [["docker", "image", "save", "--output", str(output) + ".tmp", config.fixture_image_id]]
    assert result == {
        "fixture_image_id": config.fixture_image_id,
        "output": str(output),
        "sha256": "8810c61dc998e2ef39791e76b6377bcb78d6b99809e08aeff51ba98216529ece",
    }
    assert result_file.read_text().strip()
    assert load_controller_state(state_file).exported_image_paths == (output,)

    raw_token = "status-must-not-print-this-token"
    config.credential_file.parent.mkdir(parents=True)
    config.credential_file.write_text(raw_token + "\n")
    config.credential_file.chmod(0o600)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lab_controller.py",
            "status",
            "--state-file",
            str(state_file),
            "--json",
        ],
    )
    assert controller_module.main() == 0
    assert raw_token not in capsys.readouterr().out

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lab_controller.py",
            "status",
            "--run-id",
            "ec-lab-01",
            "--state-root",
            str(tmp_path / "lab"),
            "--json",
        ],
    )
    assert controller_module.main() == 0
    assert raw_token not in capsys.readouterr().out

    invalid_root = tmp_path / "invalid"
    invalid_config = replace(
        _config(invalid_root), fixture_image_id="sha256:" + "z" * 64
    )
    invalid_state_file = invalid_root / "lab/ec-lab-01/controller-state.json"
    write_controller_state(invalid_state_file, ControllerOwnershipState(
        schema_version="lab-controller-state.v1", phase="active", config=invalid_config,
        compose_file=invalid_root / "docker-compose.lab.yml", compose_environment={},
        artifact_scratch_root=invalid_root / "scratch",
        raw_credential_file=invalid_config.credential_file,
        service_env_file=invalid_root / "service.env", owned_resources=(),
        completed_cleanup_steps=(),
        exported_image_paths=(), controller_argv=(), started_at="2026-07-27T00:00:00Z",
    ))

    with pytest.raises(LabConfigError, match="not immutable"):
        export_fixture_image(
            invalid_state_file,
            invalid_root / "fixture.tar",
            invalid_root / "result.json",
            runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("docker")),
        )


def test_qualify_reads_only_a_stopped_finalized_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_file, state = _ownership_state(
        tmp_path,
        phase="stopped",
        completed_cleanup_steps=("manifest-finalized",),
    )
    state.config.evidence_dir.mkdir(parents=True)
    (state.config.evidence_dir / "manifest.json").write_text("{}\n")
    calls: list[tuple[Path, Path]] = []
    expected = SimpleNamespace(remote_qualified=True)

    def fake_qualify(*, bundle: Path, source_root: Path):
        calls.append((bundle, source_root))
        return expected, True

    monkeypatch.setattr(controller_module, "qualify_bundle", fake_qualify)
    assert qualify_controller(state_file, source_root=tmp_path) is expected
    assert calls == [(state.config.evidence_dir, tmp_path.resolve())]


def test_qualify_cli_prints_exactly_one_classification_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        controller_module,
        "qualify_controller",
        lambda _state_file: LabQualification(
            "remote-qualified", False, True, ()
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "lab_controller.py",
            "qualify",
            "--run-id",
            "ec-lab-01",
            "--state-root",
            str(tmp_path),
        ],
    )
    assert controller_module.main() == 0
    assert capsys.readouterr().out == "lab qualification: REMOTE QUALIFIED\n"


def test_task6_summary_surface_is_derived_from_canonical_evidence(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle"
    raw = bundle / "raw/lab"
    raw.mkdir(parents=True)
    (bundle / "compose-config.yml").write_text("services: {}\n")
    (raw / "inventory.json").write_text('{"reservations": []}\n')
    (raw / "controller-commands.json").write_text('{"commands": []}\n')
    cleanup = {"completed": True, "remaining": []}
    manifest = {
        "dependencies": {"python": "3.12.11"},
        "images": {"fixture": "sha256:" + "a" * 64},
        "controller": {"declared_host_id": "controller-lab-01"},
        "nodes": [{"declared_host_id": "gateway-lab-02", "network_path": {}}],
        "cleanup": cleanup,
    }

    controller_module._write_task6_evidence(bundle, manifest)

    assert (bundle / "compose.resolved.yml").read_text() == "services: {}\n"
    assert json.loads((bundle / "versions.json").read_text()) == manifest["dependencies"]
    assert json.loads((bundle / "images.json").read_text()) == manifest["images"]
    assert json.loads((bundle / "identities.json").read_text()) == {
        "controller": manifest["controller"],
        "nodes": manifest["nodes"],
    }
    assert json.loads((bundle / "network-paths.json").read_text()) == {
        "controller": manifest["controller"],
        "nodes": [{"declared_host_id": "gateway-lab-02", "network_path": {}}],
    }
    assert json.loads((bundle / "commands.json").read_text()) == {"commands": []}
    assert json.loads((bundle / "inventory.json").read_text()) == {"reservations": []}
    assert json.loads((bundle / "cleanup.json").read_text()) == cleanup
