"""Ownership contract tests for the hermetic artifact environment."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import warnings
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import TypeVar

import pytest

from scripts.research import artifact_env
from scripts.research.artifact_env import ArtifactEnvironment, OwnedResource
from scripts.research.coordinator_restart import (
    acknowledge_restart,
    wait_for_restart_request,
)

_F = TypeVar("_F", bound=Callable[..., object])


def _docker_test(function: _F) -> _F:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pytest.PytestUnknownMarkWarning)
        decorator = pytest.mark.docker
    return decorator(function)


def _require_explicit_docker(request: pytest.FixtureRequest) -> None:
    if request.config.option.markexpr != "docker":
        pytest.skip("run explicitly with -m docker")
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        pytest.skip("Docker is unavailable")
    if result.returncode != 0:
        pytest.skip("Docker is unavailable")


def test_create_owns_a_private_run_directory_and_credential(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )

    assert environment.project == "edgecitadel-artifact-ec-20260725-test-a"
    assert environment.credential_file.stat().st_mode & 0o777 == 0o600
    credential_bytes = environment.credential_file.read_bytes()
    assert len(credential_bytes) == 65
    assert re.fullmatch(rb"[0-9a-f]{64}\n", credential_bytes)
    assert environment.compose_env["COMPOSE_PROJECT_NAME"] == environment.project
    assert environment.compose_env["EC_RUN_ID"] == "ec-20260725-test-a"
    assert environment.output_dir == tmp_path / "raw" / "ec-20260725-test-a"
    assert environment.control_dir == environment.scratch_dir / "control"
    assert (environment.control_dir / "native-control.json").is_file()
    assert environment.resolved_config["freshness_attestation"] == {
        "inventory_sha256": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
        "state_dir": str(environment.state_dir),
    }
    owner_record = json.loads(environment.owner_record.read_text())
    assert (
        environment.credential_file.read_text()
        not in environment.owner_record.read_text()
    )
    assert owner_record["resolved_config"] == environment.resolved_config
    assert environment.owner_record.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("run_id", ("", "../escape", "nested/run", "run;rm", "run id"))
def test_create_rejects_unsafe_or_empty_run_ids(
    run_id: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))

    with pytest.raises(ValueError, match="invalid run_id"):
        ArtifactEnvironment.create(run_id, "edgecitadel", tmp_path / "raw")


def test_create_rejects_unknown_modes_and_reused_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))

    with pytest.raises(ValueError, match="invalid mode"):
        ArtifactEnvironment.create("ec-20260725-test-a", "not-a-mode", tmp_path / "raw")
    ArtifactEnvironment.create("ec-20260725-test-a", "edgecitadel", tmp_path / "raw")
    with pytest.raises(ValueError, match="artifact output already exists"):
        ArtifactEnvironment.create(
            "ec-20260725-test-a", "edgecitadel", tmp_path / "raw"
        )


def test_recover_reconstructs_only_the_recorded_run_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    created = ArtifactEnvironment.create(
        "ec-20260725-test-a", "edgecitadel", tmp_path / "raw"
    )

    recovered = ArtifactEnvironment.recover("ec-20260725-test-a")

    assert recovered.run_id == created.run_id
    assert recovered.project == created.project
    assert recovered.credential_file == created.credential_file
    assert recovered.control_dir == created.control_dir
    assert recovered.state_dir == created.state_dir
    assert recovered.output_dir == created.output_dir
    assert recovered.resolved_config == created.resolved_config


def test_create_canonicalizes_a_symlinked_scratch_root_for_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backing = tmp_path / "backing"
    backing.mkdir()
    link = tmp_path / "scratch-link"
    link.symlink_to(backing, target_is_directory=True)
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(link))

    created = ArtifactEnvironment.create(
        "ec-20260727-canonical", "core-only", tmp_path / "raw"
    )
    recovered = ArtifactEnvironment.recover("ec-20260727-canonical")

    assert created.scratch_dir == backing / "ec-20260727-canonical"
    assert recovered.scratch_dir == created.scratch_dir
    assert recovered.owner_record == created.owner_record


@pytest.mark.parametrize(
    ("mode", "service"),
    (
        ("central-relay", "controller"),
        ("core-only", "nats"),
        ("edgecitadel", "nats"),
        ("all-durable", "nats"),
    ),
)
def test_restart_coordinator_recreates_only_the_mode_coordinator(
    mode: str,
    service: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-restart", mode, tmp_path / "raw"
    )
    calls: list[list[str]] = []

    def runner(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    object.__setattr__(environment, "command_runner", runner)

    environment.restart_coordinator()

    assert calls == [
        [
            "docker",
            "compose",
            "--project-name",
            environment.project,
            "--file",
            str(environment.compose_file),
            "up",
            "--detach",
            "--no-build",
            "--force-recreate",
            "--no-deps",
            "--wait",
            service,
        ]
    ]


def test_cleanup_is_idempotent_and_preserves_raw_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )
    calls: list[list[str]] = []

    def runner(
        command: list[str],
        **_: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    environment = replace(environment, command_runner=runner)
    raw_file = environment.output_dir / "raw.jsonl"
    raw_file.write_text("{}\n")

    first = environment.cleanup()
    assert first.completed is True
    assert first.credential_removed is True
    assert first.state_removed is True
    assert first.scratch_removed is True
    assert first.recovery_record_removed is True
    assert raw_file.read_text() == "{}\n"
    assert calls == [
        [
            "docker",
            "compose",
            "--project-name",
            environment.project,
            "--file",
            str(environment.compose_file),
            "down",
            "--volumes",
            "--remove-orphans",
        ],
        [
            "docker",
            "ps",
            "--all",
            "--filter",
            "label=ai.edgecitadel.owner=artifact",
            "--filter",
            "label=ai.edgecitadel.run-id=ec-20260725-test-a",
            "--format",
            "{{.Names}}",
        ],
        [
            "docker",
            "network",
            "ls",
            "--filter",
            "label=ai.edgecitadel.owner=artifact",
            "--filter",
            "label=ai.edgecitadel.run-id=ec-20260725-test-a",
            "--format",
            "{{.Name}}",
        ],
        [
            "docker",
            "volume",
            "ls",
            "--filter",
            "label=ai.edgecitadel.owner=artifact",
            "--filter",
            "label=ai.edgecitadel.run-id=ec-20260725-test-a",
            "--format",
            "{{.Name}}",
        ],
    ]

    second = environment.cleanup()
    assert second.completed is True
    assert second.credential_removed is False
    assert second.state_removed is False
    assert second.scratch_removed is False
    assert second.recovery_record_removed is False


def test_owned_resources_ignore_campaign_images_and_report_labeled_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )
    output = iter(("controller\nworker\n", "network\n", "state\n"))

    def runner(
        command: list[str],
        **_: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, next(output), "")

    environment = replace(environment, command_runner=runner)

    assert environment.owned_resources() == (
        OwnedResource("container", "controller"),
        OwnedResource("container", "worker"),
        OwnedResource("network", "network"),
        OwnedResource("volume", "state"),
    )


def test_campaign_image_cleanup_requires_its_exact_label_and_no_containers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def runner(
        command: list[str],
        **_: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        output = "campaign-1\n" if command[1:3] == ["image", "inspect"] else ""
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(artifact_env.subprocess, "run", runner)

    assert artifact_env.cleanup_campaign_image(
        "artifact@sha256:abc", "campaign-1"
    ) == OwnedResource(
        "image",
        "artifact@sha256:abc",
    )
    assert calls == [
        [
            "docker",
            "image",
            "inspect",
            "artifact@sha256:abc",
            "--format",
            '{{ index .Config.Labels "ai.edgecitadel.campaign-id" }}',
        ],
        [
            "docker",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            "ancestor=artifact@sha256:abc",
        ],
        ["docker", "image", "rm", "artifact@sha256:abc"],
    ]


def test_campaign_image_cleanup_refuses_an_image_with_live_containers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def runner(
        command: list[str],
        **_: object,
    ) -> subprocess.CompletedProcess[str]:
        output = (
            "campaign-1\n" if command[1:3] == ["image", "inspect"] else "container-id\n"
        )
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr(artifact_env.subprocess, "run", runner)

    with pytest.raises(RuntimeError, match="campaign image is still in use"):
        artifact_env.cleanup_campaign_image("artifact@sha256:abc", "campaign-1")


def test_start_topology_uses_an_argv_command_and_owned_compose_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )
    calls: list[tuple[list[str], dict[str, str]]] = []

    def runner(
        command: list[str],
        *,
        check: bool,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert check is True
        calls.append((command, env))
        return subprocess.CompletedProcess(command, 0, "", "")

    compose_file = tmp_path / "compose.yml"
    compose_file.write_text("services: {}\n")
    environment = replace(environment, command_runner=runner)
    environment.start_topology(compose_file, {"EC_ARTIFACT_IMAGE": "image@sha256:abc"})

    assert calls == [
        (
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(compose_file),
                "up",
                "--detach",
                "--no-build",
                "--wait",
            ],
            {
                "PATH": os.environ["PATH"],
                **environment.compose_env,
                "EC_ARTIFACT_IMAGE": "image@sha256:abc",
            },
        )
    ]


@_docker_test
@pytest.mark.parametrize("mode", ("central-relay", "core-only"))
def test_docker_topology_starts_with_a_numeric_leading_transport_token(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        f"ec-20260727-numeric-token-{mode}", mode, tmp_path / "raw"
    )
    environment.credential_file.write_bytes(b"5e" + (b"0" * 62) + b"\n")

    try:
        environment.start()
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_topologies_are_isolated_and_leave_no_owned_resources(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    first = ArtifactEnvironment.create(
        "ec-20260726-docker-a", "core-only", tmp_path / "raw"
    )
    second = ArtifactEnvironment.create(
        "ec-20260726-docker-b", "core-only", tmp_path / "raw"
    )

    try:
        first.start()
        second.start()
        first_resources = set(first.owned_resources())
        second_resources = set(second.owned_resources())
        assert first.project != second.project
        assert first.credential_file != second.credential_file
        assert first.state_dir != second.state_dir
        assert first_resources.isdisjoint(second_resources)
        assert {resource.kind for resource in first_resources} == {
            "container",
            "network",
            "volume",
        }
        container_names = [
            resource.name
            for resource in (*first_resources, *second_resources)
            if resource.kind == "container"
        ]
        for name in container_names:
            result = subprocess.run(
                ["docker", "port", name],
                check=True,
                capture_output=True,
                text=True,
            )
            assert result.stdout == ""
    finally:
        assert second.cleanup().completed is True
        assert first.cleanup().completed is True


@_docker_test
def test_docker_runner_executes_a_direct_w1_cell(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260726-runner-w1", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W1",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        observation = json.loads(result.stdout)["observation"]
        assert observation["timed_out"] is False
        assert observation["logical_terminals"] == 1
    finally:
        assert environment.cleanup().completed is True


@pytest.mark.parametrize(
    ("mode", "timeout_seconds", "logical_terminals", "timed_out"),
    (
        ("central-relay", 5, 1, False),
        ("core-only", 5, 0, True),
        ("edgecitadel", 35, 1, False),
        ("all-durable", 35, 1, False),
    ),
)
@_docker_test
def test_docker_runner_executes_w7_with_a_host_owned_restart(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    timeout_seconds: int,
    logical_terminals: int,
    timed_out: bool,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        f"ec-20260727-runner-w7-{mode}", mode, tmp_path / "raw"
    )

    try:
        environment.start()
        runner = subprocess.Popen(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W7",
                "--timeout-seconds",
                str(timeout_seconds),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        wait_for_restart_request(environment.control_dir, 10)
        environment.restart_coordinator()
        acknowledge_restart(environment.control_dir)
        stdout, stderr = runner.communicate(timeout=45)
        assert runner.returncode == 0, stderr
        observation = json.loads(stdout)["observation"]
        assert observation["accepted"] == 1, stdout
        assert observation["logical_terminals"] == logical_terminals, stdout
        assert observation["timed_out"] is timed_out, stdout
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_runner_executes_direct_w6c_collisions(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-runner-w6c", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W6c",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        observation = json.loads(result.stdout)["observation"]
        assert observation["accepted"] == 3
        assert observation["poison"] == 2
        assert observation["executions"] == 0
        assert observation["timed_out"] is True
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_runner_executes_supervised_w5_crash_points(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-runner-w5", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W5",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        observation = json.loads(result.stdout)["observation"]
        assert observation["initiated"] == 6
        assert observation["timed_out"] is False
        assert observation["inapplicable_crash_points"] == [
            "after-publish-mark-before-inbound-commit"
        ]
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_runner_recovers_supervised_w8_actuator_side_effect(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-runner-w8", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W8",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        observation = json.loads(result.stdout)["observation"]
        assert observation["timed_out"] is False
        assert observation["handler_attempts"] == 2
        assert observation["side_effects"] == 2
        assert observation["logical_terminals"] == 1
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_runner_executes_supervised_w6b_semantic_retry(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-runner-w6b", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--project-name",
                environment.project,
                "--file",
                str(environment.compose_file),
                "exec",
                "--no-TTY",
                "runner",
                "python",
                "-m",
                "scripts.research.in_container_runner",
                "--config",
                "/run/edgecitadel/config/native-control.json",
                "--workload",
                "W6b",
            ],
            check=True,
            capture_output=True,
            text=True,
            env={"PATH": os.environ["PATH"], **environment.compose_env},
        )
        observation = json.loads(result.stdout)["observation"]
        assert observation["timed_out"] is False
        assert observation["accepted"] == 2
        assert observation["publication_attempts"] == 2
        assert observation["logical_terminals"] == 1
    finally:
        assert environment.cleanup().completed is True


@_docker_test
def test_docker_supervisor_activates_the_external_native_worker(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_explicit_docker(request)
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [
            "docker",
            "build",
            "--file",
            str(root / "scripts/research/Dockerfile"),
            "--tag",
            "edgecitadel-research-artifact:local",
            str(root),
        ],
        check=True,
    )
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260727-supervisor", "core-only", tmp_path / "raw"
    )

    try:
        environment.start()
        template = environment.control_dir / "native-control.json"
        active = environment.control_dir / "active-native-control.json"
        temporary = active.with_suffix(".tmp")
        temporary.write_bytes(template.read_bytes())
        temporary.chmod(0o600)
        temporary.replace(active)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = environment.control_dir / "worker-status.txt"
            events = environment.state_dir / "worker-events.jsonl"
            status_text = status.read_text() if status.is_file() else ""
            if (
                status.is_file()
                and events.is_file()
                and "status=running\n" in status_text
                and "generation=\n" not in status_text
            ):
                break
            time.sleep(0.05)
        else:
            raise AssertionError("supervised native worker did not become ready")
        assert "fixture.ready" in events.read_text()
    finally:
        assert environment.cleanup().completed is True
