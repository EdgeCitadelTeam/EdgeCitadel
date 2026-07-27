"""Validated profile configuration contracts."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, ValidationError

from scripts.research import run_artifact
from scripts.research.run_artifact import main


def test_preliminary_campaign_fixes_the_paper_profile_contract() -> None:
    campaign = yaml.safe_load(
        Path("scripts/research/configs/campaigns/preliminary-x86-lan.yaml").read_text()
    )
    schema = Draft202012Validator(
        json.loads(
            Path("scripts/research/configs/schema/campaign.schema.json").read_text()
        )
    )

    schema.validate(campaign)
    assert campaign["campaign_id"] == "preliminary-x86-lan"
    assert campaign["hardware_profile"] == "x86_64-controller"
    assert campaign["network_profile"] == "lan"
    assert campaign["resource_components"] == [
        "controller",
        "broker",
        "worker",
        "observer",
    ]


def test_campaign_schema_rejects_a_missing_workload_timeout() -> None:
    campaign = yaml.safe_load(
        Path("scripts/research/configs/campaigns/preliminary-x86-lan.yaml").read_text()
    )
    campaign["workload_timeouts"].pop("W8")
    schema = Draft202012Validator(
        json.loads(
            Path("scripts/research/configs/schema/campaign.schema.json").read_text()
        )
    )

    with pytest.raises(ValidationError):
        schema.validate(campaign)


def test_quick_lifecycle_captures_source_once_and_cleans_every_fresh_environment(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=source_root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=source_root,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "test"], cwd=source_root, check=True)
    (source_root / "tracked.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "."], cwd=source_root, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "initial"], cwd=source_root, check=True
    )
    created: list[object] = []
    executed: list[str] = []
    observed_scratch_roots: list[str | None] = []

    class Cleanup:
        completed = True

    class Environment:
        def __init__(self, run_id: str, output_root: Path) -> None:
            self.output_dir = output_root / run_id
            self.output_dir.mkdir(parents=True)
            self.cleaned = False

        def cleanup(self) -> Cleanup:
            self.cleaned = True
            return Cleanup()

    def factory(run_id: str, _: str, output_root: Path) -> Environment:
        observed_scratch_roots.append(os.environ.get("EC_ARTIFACT_SCRATCH_ROOT"))
        environment = Environment(run_id, output_root)
        created.append(environment)
        return environment

    def runner(repetition: object, _: object, __: object) -> None:
        executed.append(repetition.run_id)

    output_root = tmp_path / "results"
    result_file = tmp_path / "result.json"
    scratch_root = tmp_path / "scratch"
    assert (
        main(
            [
                "run",
                "--profile",
                "quick",
                "--source-root",
                str(source_root),
                "--output-root",
                str(output_root),
                "--result-file",
                str(result_file),
                "--scratch-root",
                str(scratch_root),
            ],
            factory,
            runner,
        )
        == 0
    )
    result = json.loads(result_file.read_text())
    assert len(created) == len(executed) == 22
    assert all(environment.cleaned for environment in created)
    assert result["source"]["git_dirty"] is False
    assert len(result["bundle_paths"]) == 22
    assert observed_scratch_roots == [str(scratch_root)] * 22
    assert os.environ.get("EC_ARTIFACT_SCRATCH_ROOT") != str(scratch_root)


def test_real_repetition_runner_starts_topology_and_persists_runner_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Environment:
        mode = "core-only"
        project = "core-only-artifact-ec-20260725-00000"
        compose_file = tmp_path / "compose.yml"
        control_dir = tmp_path / "control"

        def __init__(self) -> None:
            self.output_dir = tmp_path / "bundle"
            self.output_dir.mkdir()
            self.compose_env = {"COMPOSE_PROJECT_NAME": self.project}
            self.started = False

        def start(self) -> None:
            self.started = True

    environment = Environment()
    cell = run_artifact.build_schedule(profile="quick").repetitions[1].cell
    repetition = run_artifact.Repetition("ec-20260725-00000", 0, True, cell)
    completed = subprocess.CompletedProcess(
        ["docker"],
        0,
        '{"events":[{"event":"fixture.ready"}],"observation":{"accepted":1},"workload":"W1"}',
        "",
    )
    calls: list[list[str]] = []

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return completed

    monkeypatch.setattr(run_artifact.subprocess, "run", run)

    run_artifact.run_repetition(repetition, environment, object())

    assert environment.started is True
    assert calls == [
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
            "--ablation",
            cell.ablation,
            "--timeout-seconds",
            str(cell.timeout_seconds),
        ]
    ]
    assert json.loads((environment.output_dir / "events.jsonl").read_text()) == {
        "event": "fixture.ready"
    }
    assert json.loads((environment.output_dir / "trials.jsonl").read_text()) == {
        "block": 0,
        "measured": True,
        "observation": {"accepted": 1},
        "run_id": repetition.run_id,
    }


def test_main_uses_real_environment_and_repetition_runner_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=source_root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=source_root,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "test"], cwd=source_root, check=True)
    (source_root / "tracked.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "."], cwd=source_root, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "initial"], cwd=source_root, check=True
    )
    created: list[object] = []
    executed: list[str] = []

    class Cleanup:
        completed = True

    class Environment:
        def __init__(self, run_id: str, output_root: Path) -> None:
            self.output_dir = output_root / run_id
            self.output_dir.mkdir(parents=True)

        def cleanup(self) -> Cleanup:
            return Cleanup()

    def factory(run_id: str, _: str, output_root: Path) -> Environment:
        environment = Environment(run_id, output_root)
        created.append(environment)
        return environment

    def runner(repetition: object, _: object, __: object) -> None:
        executed.append(repetition.run_id)

    monkeypatch.setattr(run_artifact.ArtifactEnvironment, "create", factory)
    monkeypatch.setattr(run_artifact, "run_repetition", runner, raising=False)

    assert (
        main(
            [
                "run",
                "--profile",
                "quick",
                "--source-root",
                str(source_root),
                "--output-root",
                str(tmp_path / "results"),
            ]
        )
        == 0
    )
    assert len(created) == len(executed) == 22


def test_paper_profile_rejects_dirty_source_before_environment_creation(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=source_root, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.invalid"],
        cwd=source_root,
        check=True,
    )
    subprocess.run(["git", "config", "user.name", "test"], cwd=source_root, check=True)
    (source_root / "tracked.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "."], cwd=source_root, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "initial"], cwd=source_root, check=True
    )
    (source_root / "untracked.py").write_text("VALUE = 2\n")
    created = False

    def factory(_: str, __: str, ___: Path) -> object:
        nonlocal created
        created = True
        raise AssertionError("paper must reject before environment creation")

    assert (
        main(
            [
                "run",
                "--profile",
                "paper",
                "--source-root",
                str(source_root),
                "--output-root",
                str(tmp_path / "results"),
                "--campaign-config",
                str(
                    Path(
                        "scripts/research/configs/campaigns/preliminary-x86-lan.yaml"
                    ).resolve()
                ),
            ],
            factory,
            None,
        )
        == 2
    )
    assert created is False


def test_cleanup_command_recovers_and_cleans_the_declared_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Cleanup:
        completed = True

    class Environment:
        def __init__(self) -> None:
            self.cleaned = False

        def cleanup(self) -> Cleanup:
            self.cleaned = True
            return Cleanup()

    environment = Environment()
    monkeypatch.setattr(
        run_artifact.ArtifactEnvironment, "recover", lambda _: environment
    )

    assert (
        main(
            [
                "cleanup",
                "--run-id",
                "ec-20260725-example",
                "--scratch-root",
                str(tmp_path / "scratch"),
            ]
        )
        == 0
    )
    assert environment.cleaned is True


def test_artifact_compose_starts_the_internal_central_relay_controller() -> None:
    compose = Path("scripts/research/docker-compose.artifact.yml").read_text()

    assert "central_relay_server:create_app_from_environment" in compose
    assert "RELAY_URL: http://controller:8000" in compose
    assert "EC_RUN_ID: ${EC_RUN_ID:?}" in compose
    assert "ports:" not in compose
