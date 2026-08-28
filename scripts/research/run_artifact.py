"""Fixed research campaign schedules and artifact lifecycle entry point."""

from __future__ import annotations

import asyncio
import json
import os
import platform
import random
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol, cast

import yaml  # type: ignore[import-untyped]

from scripts.research.artifact_env import ArtifactEnvironment
from scripts.research.coordinator_restart import (
    acknowledge_restart,
    wait_for_restart_request,
)
from scripts.research.docker_metrics import build_docker_component_reader
from scripts.research.evidence import (
    SourceProvenance,
    capture_source_provenance,
    file_sha256,
    finalize_bundle,
    verify_source_provenance,
    write_json,
    write_jsonl,
)
from scripts.research.metrics import ResourceSampler, SystemClock
from scripts.research.preflight import PreflightRequest, run_prestart_preflight
from scripts.research.workload_matrix import (
    MatrixCell,
    classify_outcome,
    required_matrix_cells,
)

_MANIFEST_SCHEMA = Path(__file__).parents[2] / "schemas/research-manifest.v1.json"
_RESOURCE_COMPONENTS = ("controller", "broker", "worker", "observer")
_HOST_METRIC_COVERAGE = (
    "cpu_seconds",
    "peak_rss_bytes",
    "rss_seconds",
    "rx_bytes",
    "tx_bytes",
    "application_bytes",
    "nats_connection_bytes",
    "http_bytes",
    "storage_bytes",
    "message_count_delta",
    "sampler_cpu_seconds",
)


@dataclass(frozen=True)
class Repetition:
    run_id: str
    block: int
    measured: bool
    cell: MatrixCell


@dataclass(frozen=True)
class Schedule:
    profile: str
    seed: int
    cells: tuple[MatrixCell, ...]
    repetitions: tuple[Repetition, ...]
    warmup_blocks: int
    measured_blocks: int
    inferential: bool

    @property
    def warmup_count(self) -> int:
        return sum(not repetition.measured for repetition in self.repetitions)

    @property
    def measured_count(self) -> int:
        return sum(repetition.measured for repetition in self.repetitions)


class _CleanupReport(Protocol):
    completed: bool


class _RunEnvironment(Protocol):
    run_id: str
    mode: str
    output_dir: Path
    credential_file: Path
    project: str
    compose_file: Path
    compose_env: Mapping[str, str]
    control_dir: Path
    resolved_config: Mapping[str, object]

    def start(self) -> None: ...

    def restart_coordinator(self) -> None: ...

    def cleanup(self) -> _CleanupReport: ...


def _run_id(seed: int, index: int) -> str:
    return f"ec-{seed}-{index:05d}"


def _primary_cell(workload: str, mode: str) -> MatrixCell:
    return next(
        cell
        for cell in required_matrix_cells()
        if cell.workload == workload and cell.mode == mode and cell.variant == "primary"
    )


def _edge_ablation_cells() -> tuple[MatrixCell, ...]:
    return tuple(
        cell
        for cell in required_matrix_cells()
        if cell.workload in {"W6a", "W6b"} and cell.mode == "edgecitadel"
    )


def _read_campaign_config(path: Path) -> dict[str, object]:
    try:
        raw = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as error:
        raise ValueError("invalid campaign config") from error
    if not isinstance(raw, dict):
        raise TypeError("invalid campaign config")
    return raw


def build_schedule(
    *,
    profile: str,
    seed: int | None = None,
    campaign_config: Path | None = None,
) -> Schedule:
    all_cells = required_matrix_cells()
    if profile == "quick":
        actual_seed = 20260725 if seed is None else seed
        modes = ("central-relay", "core-only", "edgecitadel", "all-durable")
        warmup = tuple(_primary_cell("W1", mode) for mode in modes)
        measured = (
            tuple(_primary_cell("W1", mode) for _ in range(3) for mode in modes)
            + _edge_ablation_cells()
        )
        cells = warmup + measured
        quick_repetitions = tuple(
            Repetition(
                run_id=_run_id(actual_seed, index),
                block=0,
                measured=index >= len(warmup),
                cell=cell,
            )
            for index, cell in enumerate(cells)
        )
        return Schedule("quick", actual_seed, cells, quick_repetitions, 4, 18, False)
    if profile == "matrix-smoke":
        actual_seed = 20260725 if seed is None else seed
        return Schedule(
            "matrix-smoke",
            actual_seed,
            all_cells,
            tuple(
                Repetition(_run_id(actual_seed, index), 0, False, cell)
                for index, cell in enumerate(all_cells)
            ),
            46,
            0,
            False,
        )
    if profile != "paper" or campaign_config is None:
        raise ValueError("invalid profile configuration")
    config = _read_campaign_config(campaign_config)
    configured_seed = config.get("seed")
    configured_warmup_blocks = config.get("warmup_blocks")
    configured_measured_blocks = config.get("measured_blocks")
    if (
        type(configured_seed) is not int
        or type(configured_warmup_blocks) is not int
        or type(configured_measured_blocks) is not int
        or configured_seed < 0
        or configured_warmup_blocks < 1
        or configured_measured_blocks < 1
    ):
        raise ValueError("invalid campaign config")
    actual_seed = configured_seed
    warmup_blocks = configured_warmup_blocks
    measured_blocks = configured_measured_blocks
    paper_repetitions: list[Repetition] = []
    for block in range(warmup_blocks + measured_blocks):
        ordered_cells = list(all_cells)
        random.Random(actual_seed + block).shuffle(ordered_cells)
        paper_repetitions.extend(
            Repetition(
                _run_id(actual_seed, len(paper_repetitions)),
                block,
                block >= warmup_blocks,
                cell,
            )
            for cell in ordered_cells
        )
    return Schedule(
        "paper",
        actual_seed,
        all_cells,
        tuple(paper_repetitions),
        warmup_blocks,
        measured_blocks,
        True,
    )


def _argument_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Run or clean hermetic research artifacts.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run a fixed artifact profile")
    run.add_argument(
        "--profile", choices=("quick", "matrix-smoke", "paper"), required=True
    )
    run.add_argument("--campaign-config", type=Path)
    run.add_argument("--source-root", type=Path)
    run.add_argument("--output-root", type=Path)
    run.add_argument("--scratch-root", type=Path)
    run.add_argument("--result-file", type=Path)
    cleanup = commands.add_parser("cleanup", help="clean one owned artifact run")
    cleanup.add_argument("--run-id", required=True)
    cleanup.add_argument("--scratch-root", type=Path)
    return parser


def _parse_run_args(
    arguments: Namespace,
) -> tuple[str, Path, Path, Path | None, Path | None]:
    source_option = arguments.source_root
    output_option = arguments.output_root
    if (
        source_option is not None
        and not source_option.is_absolute()
        or output_option is not None
        and not output_option.is_absolute()
        or arguments.campaign_config is not None
        and not arguments.campaign_config.is_absolute()
        or arguments.result_file is not None
        and not arguments.result_file.is_absolute()
    ):
        raise ValueError("invalid artifact roots")
    source_root = (source_option or Path.cwd()).resolve()
    output_root = (output_option or source_root / "data/research/results/raw").resolve()
    if not source_root.is_dir():
        raise ValueError("invalid artifact roots")
    return (
        arguments.profile,
        source_root,
        output_root,
        arguments.campaign_config.resolve() if arguments.campaign_config else None,
        arguments.result_file.resolve() if arguments.result_file else None,
    )


def _run_cleanup(arguments: Namespace) -> int:
    scratch_root = arguments.scratch_root
    if scratch_root is not None and not scratch_root.is_absolute():
        raise ValueError("invalid scratch root")
    previous_scratch_root = os.environ.get("EC_ARTIFACT_SCRATCH_ROOT")
    try:
        if scratch_root is not None:
            os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = str(scratch_root)
        return (
            0
            if ArtifactEnvironment.recover(arguments.run_id).cleanup().completed
            else 2
        )
    finally:
        if previous_scratch_root is None:
            os.environ.pop("EC_ARTIFACT_SCRATCH_ROOT", None)
        else:
            os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = previous_scratch_root


def _runner_command(repetition: Repetition, environment: _RunEnvironment) -> list[str]:
    return [
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
        repetition.cell.workload,
        "--ablation",
        repetition.cell.ablation,
        "--timeout-seconds",
        str(repetition.cell.timeout_seconds),
    ]


def _runner_environment(environment: _RunEnvironment) -> dict[str, str]:
    return {"PATH": os.environ["PATH"], **environment.compose_env}


def _run_w7(repetition: Repetition, environment: _RunEnvironment) -> str:
    process = subprocess.Popen(
        _runner_command(repetition, environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_runner_environment(environment),
    )
    try:
        wait_for_restart_request(environment.control_dir, 10)
        environment.restart_coordinator()
        acknowledge_restart(environment.control_dir)
        stdout, stderr = process.communicate(
            timeout=repetition.cell.timeout_seconds + 10
        )
    except BaseException:
        process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
        raise
    if process.returncode != 0:
        raise subprocess.CalledProcessError(
            process.returncode or 2,
            process.args,
            output=stdout,
            stderr=stderr,
        )
    return stdout


def _runner_payload(stdout: str, repetition: Repetition) -> Mapping[str, object]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise ValueError("invalid runner output") from error
    if (
        not isinstance(payload, dict)
        or payload.get("workload") != repetition.cell.workload
        or not isinstance(payload.get("events"), list)
        or not all(isinstance(event, dict) for event in payload["events"])
        or not isinstance(payload.get("observation"), dict)
    ):
        raise ValueError("invalid runner output")
    return payload


def _monotonic_latency_ns(observation: Mapping[str, object]) -> int:
    started = observation.get("started_monotonic_ns")
    ended = observation.get("ended_monotonic_ns")
    if (
        type(started) is not int
        or type(ended) is not int
        or started < 0
        or ended < started
    ):
        raise ValueError("invalid in-container monotonic trial interval")
    return ended - started


def _application_bytes(events: list[object]) -> int:
    total = 0
    for event in events:
        if not isinstance(event, Mapping) or event.get("event") != "transport.publication_accepted":
            continue
        data = event.get("data")
        receipt = data.get("receipt") if isinstance(data, Mapping) else None
        value = receipt.get("application_bytes") if isinstance(receipt, Mapping) else None
        if type(value) is not int or value < 0:
            raise ValueError("invalid publication application-byte receipt")
        total += value
    return total


def _transport_resource_deltas(observation: Mapping[str, object]) -> dict[str, int]:
    initial = observation.get("initial_transport")
    final = observation.get("final_transport")
    if initial is None or final is None:
        raise ValueError("missing paired transport snapshots")
    if not isinstance(initial, Mapping) or not isinstance(final, Mapping):
        raise TypeError("invalid paired transport snapshots")
    mode = initial.get("mode")
    if mode != final.get("mode") or type(mode) is not str:
        raise ValueError("transport mode changed during trial")
    initial_connections = initial.get("connection_bytes")
    final_connections = final.get("connection_bytes")
    if initial_connections is None or final_connections is None:
        raise ValueError("missing transport connection counters")
    if not isinstance(initial_connections, Mapping) or not isinstance(final_connections, Mapping):
        raise TypeError("invalid transport connection counters")
    if set(initial_connections) != set(final_connections):
        raise ValueError("transport connection membership changed during trial")

    def delta(before: object, after: object) -> int:
        if (
            type(before) is not int
            or type(after) is not int
            or before < 0
            or after < before
        ):
            raise ValueError("transport counter regressed during trial")
        return after - before

    connection_delta = sum(
        delta(initial_connections[name], final_connections[name])
        for name in initial_connections
    )
    storage_delta = delta(initial.get("storage_bytes"), final.get("storage_bytes"))
    message_delta = delta(initial.get("message_count"), final.get("message_count"))
    return {
        "nats_connection_bytes": 0 if mode == "central-relay" else connection_delta,
        "http_bytes": connection_delta if mode == "central-relay" else 0,
        "storage_bytes": storage_delta,
        "message_count_delta": message_delta,
    }


def _run_with_host_resource_sampling(
    repetition: Repetition, environment: _RunEnvironment
) -> tuple[str, dict[str, object]]:
    """Collect explicitly partial container counters around a real runner invocation."""
    reader = build_docker_component_reader(
        project=environment.project,
        compose_file=environment.compose_file,
        environment=_runner_environment(environment),
    )
    clock = SystemClock()
    sampler = ResourceSampler(
        reader,
        clock,
        metric_coverage=_HOST_METRIC_COVERAGE,
    )
    idle = sampler.idle_baseline(_RESOURCE_COMPONENTS)

    def invoke_runner() -> str:
        if repetition.cell.workload == "W7":
            return _run_w7(repetition, environment)
        completed = subprocess.run(
            _runner_command(repetition, environment),
            check=True,
            capture_output=True,
            text=True,
            env=_runner_environment(environment),
        )
        return completed.stdout

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(invoke_runner)
        active = None
        window = None
        while not future.done():
            if active is None and _trial_window_ready(environment.control_dir, "start"):
                active = sampler.start_active_window(_RESOURCE_COMPONENTS)
                _acknowledge_trial_window(environment.control_dir, "start")
            if active is not None and _trial_window_ready(environment.control_dir, "end"):
                window = active.finish(outcome="runner-complete")
                _acknowledge_trial_window(environment.control_dir, "end")
                break
            clock.sleep_ns(100_000_000)
            if active is not None:
                active.sample_due()
        stdout = future.result()
    if window is None:
        raise ValueError("runner did not complete the resource timing handshake")
    return stdout, {
        "status": "partial",
        "scope": "host-trial-window",
        "idle_baseline": asdict(idle),
        "active_window": asdict(window),
    }


def _trial_window_ready(control_dir: Path, phase: str) -> bool:
    ready = control_dir / f"trial-window.{phase}.ready"
    try:
        contents = ready.read_bytes()
    except FileNotFoundError:
        return False
    if contents != b"ready\n":
        raise ValueError("invalid runner resource timing signal")
    return True


def _acknowledge_trial_window(control_dir: Path, phase: str) -> None:
    acknowledgement = control_dir / f"trial-window.{phase}.ack"
    try:
        descriptor = os.open(
            acknowledgement,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
    except FileExistsError:
        return
    try:
        os.write(descriptor, b"ack\n")
    finally:
        os.close(descriptor)


def run_repetition(
    repetition: Repetition,
    environment: _RunEnvironment,
    _: SourceProvenance,
) -> None:
    """Run one cell in its owned topology and persist its raw runner evidence."""
    prestart = asyncio.run(
        run_prestart_preflight(
            PreflightRequest(
                run_id=environment.run_id,
                mode=environment.mode,
                expected_agents=("worker-1", "observer-1"),
                resolved_config=environment.resolved_config,
                credential_file=environment.credential_file,
            )
        )
    )
    write_json(environment.output_dir / "preflight.json", prestart.to_dict())
    prestart.require_valid()
    environment.start()
    resources: dict[str, object] | None = None
    if isinstance(environment, ArtifactEnvironment):
        stdout, resources = _run_with_host_resource_sampling(repetition, environment)
    elif repetition.cell.workload == "W7":
        stdout = _run_w7(repetition, environment)
    else:
        completed = subprocess.run(
            _runner_command(repetition, environment),
            check=True,
            capture_output=True,
            text=True,
            env=_runner_environment(environment),
        )
        stdout = completed.stdout
    payload = _runner_payload(stdout, repetition)
    events = cast(list[object], payload["events"])
    observation = cast(Mapping[str, object], payload["observation"])
    outcome = classify_outcome(repetition.cell, observation)
    recorded_observation = {
        **dict(observation),
        "outcome": outcome,
        "latency_ns": (
            _monotonic_latency_ns(observation) if outcome == "completed" else None
        ),
    }
    if resources is not None:
        active_window = resources["active_window"]
        if not isinstance(active_window, Mapping):
            raise ValueError("invalid host resource window")
        resource_window = dict(active_window)
        resource_window["application_bytes"] = _application_bytes(events)
        resource_window.update(_transport_resource_deltas(observation))
        resources["active_window"] = resource_window
        recorded_observation["resources"] = resource_window
    write_jsonl(environment.output_dir / "events.jsonl", events)
    write_json(
        environment.output_dir / "resources.json",
        resources or {"status": "not_collected"},
    )
    write_jsonl(
        environment.output_dir / "trials.jsonl",
        (
            {
                "schema_version": "research-trial.v1",
                "block": repetition.block,
                "cell": asdict(repetition.cell),
                "events_artifact": "events.jsonl",
                "invariant_results": {"outcome_consistent": True},
                "measured": repetition.measured,
                "observation": recorded_observation,
                "resource_artifact": "resources.json",
                "run_id": repetition.run_id,
                "timing": {
                    "started_monotonic_ns": observation["started_monotonic_ns"],
                    "ended_monotonic_ns": observation["ended_monotonic_ns"],
                },
                "trial_id": repetition.run_id,
            },
        ),
    )


def _compose_config_sha256(environment: _RunEnvironment) -> str:
    compose_file = getattr(environment, "compose_file", None)
    if isinstance(compose_file, Path) and compose_file.is_file():
        return cast(str, file_sha256(compose_file))
    return "0" * 64


def _bundle_manifest(
    repetition: Repetition,
    environment: _RunEnvironment,
    source: SourceProvenance,
    profile: str,
    cleanup: _CleanupReport,
    campaign_id: str,
    campaign_contract: Mapping[str, object],
) -> dict[str, object]:
    metric_contract: dict[str, object] = {"status": "not_collected"}
    if isinstance(environment, ArtifactEnvironment):
        metric_contract = {
            "status": "partial",
            "components": list(_RESOURCE_COMPONENTS),
            "sampler_interval_ms": 100,
            "idle_baseline_seconds": 2,
        }
    return {
        "schema_version": "research-manifest.v1",
        "evidence_kind": "benchmark",
        "status": "PENDING",
        "run_id": repetition.run_id,
        "campaign_id": campaign_id,
        "profile": profile,
        "source": source.to_dict(),
        "command": [
            "scripts/research/run_artifact.py",
            "run",
            "--profile",
            profile,
        ],
        "timing": {},
        "host": _host_facts(),
        "dependencies": {},
        "images": {},
        "compose_config_sha256": _compose_config_sha256(environment),
        "schemas": {
            "event": "research-event.v1",
            "manifest": "research-manifest.v1",
            "trial": "research-trial.v1",
        },
        "cleanup": {"completed": cleanup.completed},
        "artifacts": {},
        "transport_config": {"mode": repetition.cell.mode},
        "workload_config": {
            "ablation": repetition.cell.ablation,
            "timeout_seconds": repetition.cell.timeout_seconds,
            "variant": repetition.cell.variant,
            "workload": repetition.cell.workload,
        },
        "metric_contract": metric_contract,
        "campaign_contract": dict(campaign_contract),
    }


def _schedule_row(repetition: Repetition) -> dict[str, object]:
    return {
        "run_id": repetition.run_id,
        "block": repetition.block,
        "measured": repetition.measured,
        "cell": asdict(repetition.cell),
    }


def _resolved_campaign_config(
    profile: str,
    schedule: Schedule,
    campaign_config: Path | None,
) -> dict[str, object]:
    if profile == "paper":
        if campaign_config is None:
            raise ValueError("paper profile requires campaign config")
        return _read_campaign_config(campaign_config)
    return {
        "schema_version": "research-development-campaign.v1",
        "campaign_id": f"{profile}-{schedule.seed}",
        "profile": profile,
        "seed": schedule.seed,
        "warmup_repetitions": schedule.warmup_count,
        "measured_repetitions": schedule.measured_count,
        "bootstrap_seed": schedule.seed,
        "bootstrap_samples": 10_000,
    }


def _campaign_directory_name(
    profile: str,
    schedule: Schedule,
    config: Mapping[str, object],
) -> str:
    campaign_id = config.get("campaign_id")
    if type(campaign_id) is not str or not campaign_id:
        raise ValueError("invalid campaign ID")
    expected = campaign_id if profile == "paper" else f"{profile}-{schedule.seed}"
    if profile != "paper" and campaign_id != expected:
        raise ValueError("invalid development campaign ID")
    return expected


def _paper_host_error() -> str | None:
    if platform.system() != "Linux":
        return "paper profile requires Linux"
    if platform.machine() != "x86_64":
        return "paper profile requires x86_64"
    values = _os_release()
    if values is None:
        return "paper profile requires readable /etc/os-release"
    if values.get("ID") != "ubuntu" or values.get("VERSION_ID") != "24.04":
        return "paper profile requires Ubuntu 24.04"
    return None


def _os_release() -> dict[str, str] | None:
    try:
        return {
            key: value.strip().strip('"')
            for line in Path("/etc/os-release").read_text().splitlines()
            if "=" in line
            for key, value in (line.split("=", 1),)
        }
    except OSError:
        return None


def _host_facts() -> dict[str, str]:
    os_release = _os_release() or {}
    return {
        "platform": sys.platform,
        "system": platform.system(),
        "architecture": platform.machine(),
        "release": platform.release(),
        "os_id": os_release.get("ID", "unknown"),
        "os_version": os_release.get("VERSION_ID", "unknown"),
    }


def main(
    argv: list[str] | None = None,
    environment_factory: Callable[[str, str, Path], _RunEnvironment] | None = None,
    repetition_runner: Callable[[Repetition, _RunEnvironment, SourceProvenance], None]
    | None = None,
) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    previous_scratch_root: str | None = None
    scratch_root_was_overridden = False
    try:
        parsed = _argument_parser().parse_args(arguments)
        if parsed.command == "cleanup":
            return _run_cleanup(parsed)
        if parsed.scratch_root is not None:
            if not parsed.scratch_root.is_absolute():
                raise ValueError("invalid scratch root")
            previous_scratch_root = os.environ.get("EC_ARTIFACT_SCRATCH_ROOT")
            os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = str(parsed.scratch_root)
            scratch_root_was_overridden = True
        profile, source_root, output_root, campaign_config, result_file = (
            _parse_run_args(parsed)
        )
        source = capture_source_provenance(source_root)
        if profile == "paper" and source.git_dirty:
            return 2
        if profile == "paper" and _paper_host_error() is not None:
            return 2
        schedule = build_schedule(
            profile=profile,
            campaign_config=campaign_config,
        )
        resolved_campaign_config = _resolved_campaign_config(
            profile,
            schedule,
            campaign_config,
        )
        campaign_path = output_root / _campaign_directory_name(
            profile,
            schedule,
            resolved_campaign_config,
        )
        campaign_path.mkdir(mode=0o700, parents=True, exist_ok=False)
        write_json(campaign_path / "campaign-config.json", resolved_campaign_config)
        write_jsonl(
            campaign_path / "schedule.jsonl",
            (_schedule_row(repetition) for repetition in schedule.repetitions),
        )
        bundle_paths = [
            str((campaign_path / "bundles" / repetition.run_id).resolve())
            for repetition in schedule.repetitions
        ]
        result = {
            "schema_version": "research-campaign.v1",
            "campaign_id": resolved_campaign_config["campaign_id"],
            "campaign_path": str(campaign_path),
            "bundle_paths": bundle_paths,
            "profile": profile,
            "source": source.to_dict(),
            "config_sha256": file_sha256(campaign_path / "campaign-config.json"),
            "schedule_sha256": file_sha256(campaign_path / "schedule.jsonl"),
        }
        write_json(campaign_path / "campaign.json", result)
        campaign_sha256 = file_sha256(campaign_path / "campaign.json")
        factory: Callable[[str, str, Path], _RunEnvironment] = (
            environment_factory or ArtifactEnvironment.create
        )
        runner: Callable[
            [Repetition, _RunEnvironment, SourceProvenance], None
        ] = repetition_runner or run_repetition
        for repetition in schedule.repetitions:
            environment = factory(
                repetition.run_id, repetition.cell.mode, campaign_path / "bundles"
            )
            cleanup: _CleanupReport
            try:
                runner(repetition, environment, source)
            finally:
                cleanup = environment.cleanup()
            if cleanup.completed is not True:
                return 2
            if not verify_source_provenance(source_root, source):
                return 2
            if (
                finalize_bundle(
                    environment.output_dir,
                    _bundle_manifest(
                        repetition,
                        environment,
                        source,
                        profile,
                        cleanup,
                        str(result["campaign_id"]),
                        {
                            "block": repetition.block,
                            "measured": repetition.measured,
                            "config_sha256": result["config_sha256"],
                            "schedule_sha256": result["schedule_sha256"],
                            "campaign_sha256": campaign_sha256,
                        },
                    ),
                    _MANIFEST_SCHEMA,
                )
                != "PASS"
            ):
                return 2
        if result_file is not None:
            write_json(result_file, result)
        return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        return 2
    finally:
        if scratch_root_was_overridden:
            if previous_scratch_root is None:
                os.environ.pop("EC_ARTIFACT_SCRATCH_ROOT", None)
            else:
                os.environ["EC_ARTIFACT_SCRATCH_ROOT"] = previous_scratch_root


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Repetition", "Schedule", "build_schedule", "main", "run_repetition"]
