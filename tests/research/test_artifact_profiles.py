"""Validated profile configuration contracts."""

from __future__ import annotations

import json
import os
import platform
import subprocess
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, ValidationError

from scripts.research import run_artifact
from scripts.research.preflight import PreflightReport
from scripts.research.run_artifact import main
from scripts.research.workload_matrix import MatrixCell, classify_outcome


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


@pytest.mark.parametrize(
    ("system", "machine", "expected"),
    (
        ("Darwin", "arm64", "paper profile requires Linux"),
        ("Linux", "aarch64", "paper profile requires x86_64"),
    ),
)
def test_paper_host_gate_rejects_unsupported_platforms(
    monkeypatch: pytest.MonkeyPatch,
    system: str,
    machine: str,
    expected: str,
) -> None:
    monkeypatch.setattr(platform, "system", lambda: system)
    monkeypatch.setattr(platform, "machine", lambda: machine)

    assert run_artifact._paper_host_error() == expected


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


def test_resource_application_bytes_sum_only_canonical_publication_receipts() -> None:
    assert run_artifact._application_bytes(
        [
            {"event": "fixture.ready"},
            {
                "event": "transport.publication_accepted",
                "data": {"receipt": {"application_bytes": 12}},
            },
            {
                "event": "transport.publication_accepted",
                "data": {"receipt": {"application_bytes": 8}},
            },
        ]
    ) == 20


def test_transport_resource_deltas_use_paired_monotonic_snapshots() -> None:
    assert run_artifact._transport_resource_deltas(
        {
            "initial_transport": {
                "mode": "edgecitadel",
                "connection_bytes": {"in_bytes": 10, "out_bytes": 20},
                "storage_bytes": 100,
                "message_count": 2,
            },
            "final_transport": {
                "mode": "edgecitadel",
                "connection_bytes": {"in_bytes": 30, "out_bytes": 50},
                "storage_bytes": 125,
                "message_count": 4,
            },
        }
    ) == {
        "nats_connection_bytes": 50,
        "http_bytes": 0,
        "storage_bytes": 25,
        "message_count_delta": 2,
    }


def test_paper_campaign_uses_the_predeclared_id_for_directory_and_bundles() -> None:
    config_path = Path(
        "scripts/research/configs/campaigns/preliminary-x86-lan.yaml"
    ).resolve()
    schedule = run_artifact.build_schedule(
        profile="paper",
        campaign_config=config_path,
    )
    config = run_artifact._resolved_campaign_config(
        "paper",
        schedule,
        config_path,
    )

    assert run_artifact._campaign_directory_name("paper", schedule, config) == (
        "preliminary-x86-lan"
    )


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
    campaign_inputs_ready: list[bool] = []

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
        campaign_root = output_root / "quick-20260725"
        campaign_inputs_ready.append(
            all(
                (campaign_root / name).is_file()
                for name in (
                    "campaign.json",
                    "campaign-config.json",
                    "schedule.jsonl",
                )
            )
        )

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
    assert campaign_inputs_ready == [True] * 22
    assert os.environ.get("EC_ARTIFACT_SCRATCH_ROOT") != str(scratch_root)
    campaign_root = Path(result["campaign_path"])
    schedule = [
        json.loads(line)
        for line in (campaign_root / "schedule.jsonl").read_text().splitlines()
    ]
    assert len(schedule) == 22
    assert schedule[0] == {
        "block": 0,
        "cell": {
            "ablation": "full-contract",
            "mode": "central-relay",
            "timeout_seconds": 30,
            "variant": "primary",
            "workload": "W1",
        },
        "measured": False,
        "run_id": "ec-20260725-00000",
    }
    manifests = [
        json.loads((environment.output_dir / "manifest.json").read_text())
        for environment in created
    ]
    assert all(manifest["status"] == "PASS" for manifest in manifests)
    assert {manifest["campaign_id"] for manifest in manifests} == {"quick-20260725"}
    assert all(manifest["cleanup"] == {"completed": True} for manifest in manifests)
    assert all(
        manifest["metric_contract"] == {"status": "not_collected"}
        for manifest in manifests
    )
    assert all(
        manifest["campaign_contract"]["campaign_sha256"]
        and manifest["campaign_contract"]["schedule_sha256"]
        and manifest["campaign_contract"]["config_sha256"]
        for manifest in manifests
    )
    assert all(manifest["manifest_sha256"] for manifest in manifests)


def test_real_repetition_runner_starts_topology_and_persists_runner_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Environment:
        run_id = "ec-20260725-00000"
        mode = "core-only"
        project = "core-only-artifact-ec-20260725-00000"
        compose_file = tmp_path / "compose.yml"
        control_dir = tmp_path / "control"

        def __init__(self) -> None:
            self.output_dir = tmp_path / "bundle"
            self.output_dir.mkdir()
            self.compose_env = {"COMPOSE_PROJECT_NAME": self.project}
            self.credential_file = tmp_path / "credential"
            self.resolved_config = {"mode": self.mode}
            self.started = False

        def start(self) -> None:
            self.started = True

    environment = Environment()
    cell = run_artifact.build_schedule(profile="quick").repetitions[1].cell
    repetition = run_artifact.Repetition("ec-20260725-00000", 0, True, cell)
    runner_observation = {
        "initiated": 1,
        "accepted": 1,
        "delivered": 1,
        "executions": 1,
        "logical_terminals": 1,
        "distinct_terminal_ids": 1,
        "publication_attempts": 1,
        "wire_deliveries": 1,
        "timed_out": False,
        "started_monotonic_ns": 10_000,
        "ended_monotonic_ns": 110_000,
    }
    completed = subprocess.CompletedProcess(
        ["docker"],
        0,
        json.dumps(
            {
                "events": [{"event": "fixture.ready"}],
                "observation": runner_observation,
                "workload": "W1",
            }
        ),
        "",
    )
    calls: list[list[str]] = []

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return completed

    async def preflight(_: object) -> PreflightReport:
        return PreflightReport(True, "2026-07-27T00:00:00Z", (), (), {})

    monkeypatch.setattr(run_artifact.subprocess, "run", run)
    monkeypatch.setattr(run_artifact, "run_prestart_preflight", preflight)

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
        "schema_version": "research-trial.v1",
        "block": 0,
        "cell": {
            "ablation": cell.ablation,
            "mode": cell.mode,
            "timeout_seconds": cell.timeout_seconds,
            "variant": cell.variant,
            "workload": cell.workload,
        },
        "measured": True,
        "trial_id": repetition.run_id,
        "observation": {
            **runner_observation,
            "latency_ns": 100_000,
            "outcome": "completed",
        },
        "timing": {
            "started_monotonic_ns": 10_000,
            "ended_monotonic_ns": 110_000,
        },
        "events_artifact": "events.jsonl",
        "resource_artifact": "resources.json",
        "invariant_results": {"outcome_consistent": True},
        "run_id": repetition.run_id,
    }
    assert json.loads((environment.output_dir / "resources.json").read_text()) == {
        "status": "not_collected"
    }


@pytest.mark.parametrize(
    ("workload", "observation", "expected"),
    (
        (
            "W1",
            {
                "initiated": 1,
                "accepted": 1,
                "delivered": 1,
                "executions": 2,
                "logical_terminals": 1,
                "distinct_terminal_ids": 1,
                "publication_attempts": 1,
                "wire_deliveries": 1,
                "timed_out": False,
            },
            "failed",
        ),
        (
            "W6c",
            {
                "initiated": 1,
                "accepted": 3,
                "delivered": 0,
                "executions": 0,
                "logical_terminals": 0,
                "distinct_terminal_ids": 0,
                "publication_attempts": 3,
                "wire_deliveries": 0,
                "poison": 2,
                "timed_out": True,
            },
            "completed",
        ),
        (
            "W1",
            {
                "initiated": 1,
                "accepted": 1,
                "delivered": 0,
                "executions": 0,
                "logical_terminals": 0,
                "distinct_terminal_ids": 0,
                "publication_attempts": 1,
                "wire_deliveries": 0,
                "timed_out": True,
            },
            "timeout",
        ),
        (
            "W2",
            {
                "initiated": 1,
                "accepted": 1,
                "delivered": 1,
                "executions": 2,
                "logical_terminals": 1,
                "distinct_terminal_ids": 1,
                "publication_attempts": 1,
                "wire_deliveries": 1,
                "timed_out": False,
            },
            "completed",
        ),
        (
            "W8",
            {
                "initiated": 1,
                "accepted": 1,
                "delivered": 1,
                "executions": 2,
                "side_effects": 2,
                "prepared_outcomes": 1,
                "logical_terminals": 1,
                "distinct_terminal_ids": 1,
                "publication_attempts": 2,
                "wire_deliveries": 1,
                "timed_out": False,
            },
            "completed",
        ),
    ),
)
def test_runner_classifies_semantic_outcomes(
    workload: str,
    observation: dict[str, object],
    expected: str,
) -> None:
    base_cell = run_artifact.build_schedule(profile="matrix-smoke").repetitions[0].cell
    cell = MatrixCell(
        workload,
        base_cell.mode,
        base_cell.variant,
        base_cell.ablation,
        30,
    )

    assert classify_outcome(cell, observation) == expected


def test_real_repetition_runner_records_valid_prestart_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = run_artifact.ArtifactEnvironment.create(
        "ec-20260727-preflight", "core-only", tmp_path / "raw"
    )
    cell = run_artifact.build_schedule(profile="quick").repetitions[1].cell
    repetition = run_artifact.Repetition(environment.run_id, 0, True, cell)
    completed = subprocess.CompletedProcess(
        ["docker"],
        0,
        '{"events":[],"observation":{"accepted":1,"initial_transport":{"mode":"core-only","connection_bytes":{"in_bytes":0,"out_bytes":0},"storage_bytes":0,"message_count":0},"final_transport":{"mode":"core-only","connection_bytes":{"in_bytes":0,"out_bytes":0},"storage_bytes":0,"message_count":0},"started_monotonic_ns":1,"ended_monotonic_ns":2},"workload":"W1"}',
        "",
    )
    monkeypatch.setattr(run_artifact.ArtifactEnvironment, "start", lambda _: None)
    monkeypatch.setattr(
        run_artifact.subprocess, "run", lambda *_args, **_kwargs: completed
    )
    monkeypatch.setattr(
        run_artifact,
        "_run_with_host_resource_sampling",
        lambda *_args: (completed.stdout, {"status": "partial", "active_window": {}}),
    )

    run_artifact.run_repetition(repetition, environment, object())

    preflight = json.loads((environment.output_dir / "preflight.json").read_text())
    assert preflight["valid"] is True
    assert {check["name"] for check in preflight["checks"]} == {
        "credential",
        "mode",
        "agents",
        "freshness_attestation",
        "resolved_config_mode",
    }


def test_real_bundle_manifest_declares_partial_host_metric_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = run_artifact.ArtifactEnvironment.create(
        "ec-20260727-metrics", "core-only", tmp_path / "raw"
    )
    repetition = run_artifact.Repetition(
        environment.run_id,
        0,
        True,
        run_artifact.build_schedule(profile="quick").repetitions[1].cell,
    )

    manifest = run_artifact._bundle_manifest(
        repetition,
        environment,
        run_artifact.capture_source_provenance(Path.cwd()),
        "quick",
        type("Cleanup", (), {"completed": True})(),
        "quick-20260725",
        {},
    )

    assert manifest["metric_contract"] == {
        "status": "partial",
        "components": ["controller", "broker", "worker", "observer"],
        "sampler_interval_ms": 100,
        "idle_baseline_seconds": 2,
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
