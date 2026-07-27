"""Fixed research campaign schedules and artifact lifecycle entry point."""

from __future__ import annotations

import asyncio
import json
import os
import random
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import yaml  # type: ignore[import-untyped]

from scripts.research.artifact_env import ArtifactEnvironment
from scripts.research.coordinator_restart import (
    acknowledge_restart,
    wait_for_restart_request,
)
from scripts.research.evidence import (
    SourceProvenance,
    capture_source_provenance,
    verify_source_provenance,
    write_json,
    write_jsonl,
)
from scripts.research.preflight import PreflightRequest, run_prestart_preflight
from scripts.research.workload_matrix import MatrixCell, required_matrix_cells


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
    actual_seed = cast(int, configured_seed)
    warmup_blocks = cast(int, configured_warmup_blocks)
    measured_blocks = cast(int, configured_measured_blocks)
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
    output_root = (output_option or source_root / "docs/research/results/raw").resolve()
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
    if repetition.cell.workload == "W7":
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
    write_jsonl(environment.output_dir / "events.jsonl", events)
    write_jsonl(
        environment.output_dir / "trials.jsonl",
        (
            {
                "block": repetition.block,
                "measured": repetition.measured,
                "observation": observation,
                "run_id": repetition.run_id,
            },
        ),
    )


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
        schedule = build_schedule(
            profile=profile,
            campaign_config=campaign_config,
        )
        campaign_path = output_root / f"{profile}-{schedule.seed}"
        campaign_path.mkdir(mode=0o700, parents=True, exist_ok=False)
        write_json(
            campaign_path / "schedule.json",
            {"repetitions": [rep.run_id for rep in schedule.repetitions]},
        )
        factory = cast(
            Callable[[str, str, Path], _RunEnvironment],
            environment_factory or ArtifactEnvironment.create,
        )
        runner = cast(
            Callable[[Repetition, _RunEnvironment, SourceProvenance], None],
            repetition_runner or run_repetition,
        )
        bundle_paths: list[str] = []
        for repetition in schedule.repetitions:
            environment = factory(
                repetition.run_id, repetition.cell.mode, campaign_path / "bundles"
            )
            cleanup_failed = False
            try:
                runner(repetition, environment, source)
                bundle_paths.append(str(environment.output_dir))
            finally:
                cleanup_failed = environment.cleanup().completed is not True
            if cleanup_failed:
                return 2
            if not verify_source_provenance(source_root, source):
                return 2
        result = {
            "campaign_path": str(campaign_path),
            "bundle_paths": bundle_paths,
            "profile": profile,
            "source": source.to_dict(),
        }
        write_json(campaign_path / "campaign.json", result)
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
