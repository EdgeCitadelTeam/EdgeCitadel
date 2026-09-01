"""Generate deterministic publication tables from one checked paper campaign."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.research.check_artifact import ArtifactInvalid, check_campaign
from scripts.research.evidence import file_sha256, write_json
from scripts.research.statistics import (
    nearest_rank_percentile,
    newcombe_risk_difference_interval,
    paired_median_change,
    sample_median,
    summarize_measurements,
    wilson_interval,
)

_COST_METRICS = (
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
_CELL_FIELDS = (
    "workload",
    "mode",
    "variant",
    "ablation",
    "hardware_profile",
    "network_profile",
)


def _read_json(path: Path) -> Mapping[str, object]:
    value = json.loads(path.read_text())
    if not isinstance(value, Mapping):
        raise ArtifactInvalid(f"invalid checked JSON: {path}")
    return value


def _read_jsonl(path: Path) -> tuple[Mapping[str, object], ...]:
    values = tuple(json.loads(line) for line in path.read_text().splitlines())
    if any(not isinstance(value, Mapping) for value in values):
        raise ArtifactInvalid(f"invalid checked JSONL: {path}")
    return tuple(value for value in values if isinstance(value, Mapping))


def _input_sha256(campaign: Path, schedule: Sequence[Mapping[str, object]]) -> str:
    paths = [
        campaign / "campaign.json",
        campaign / "campaign-config.json",
        campaign / "schedule.jsonl",
    ]
    paths.extend(
        path
        for row in schedule
        for path in (
            campaign / "bundles" / str(row["run_id"]) / "manifest.json",
            campaign / "bundles" / str(row["run_id"]) / "trials.jsonl",
        )
    )
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(campaign).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_sha256(path).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def _cell_key(
    cell: Mapping[str, object],
    config: Mapping[str, object],
) -> tuple[str, str, str, str, str, str]:
    values = (
        cell.get("workload"),
        cell.get("mode"),
        cell.get("variant"),
        cell.get("ablation"),
        config.get("hardware_profile"),
        config.get("network_profile"),
    )
    if any(type(value) is not str or not value for value in values):
        raise ArtifactInvalid("checked campaign contains an invalid matrix cell")
    return tuple(str(value) for value in values)  # type: ignore[return-value]


def _cell_identity(key: Sequence[str]) -> dict[str, str]:
    return dict(zip(_CELL_FIELDS, key, strict=True))


def _measured_rows(
    campaign: Path,
    schedule: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
) -> dict[tuple[str, str, str, str, str, str], list[dict[str, object]]]:
    grouped: dict[
        tuple[str, str, str, str, str, str],
        list[dict[str, object]],
    ] = defaultdict(list)
    for scheduled in schedule:
        if scheduled.get("measured") is not True:
            continue
        trial = _read_jsonl(
            campaign / "bundles" / str(scheduled["run_id"]) / "trials.jsonl"
        )[0]
        cell = trial.get("cell")
        observation = trial.get("observation")
        block = trial.get("block")
        if (
            not isinstance(cell, Mapping)
            or not isinstance(observation, Mapping)
            or type(block) is not int
        ):
            raise ArtifactInvalid("checked campaign contains an invalid trial")
        grouped[_cell_key(cell, config)].append(
            {"block": block, "observation": dict(observation)}
        )
    return {
        key: sorted(
            rows,
            key=lambda row: row["block"] if isinstance(row["block"], int) else -1,
        )
        for key, rows in grouped.items()
    }


def _correctness_rows(
    grouped: Mapping[
        tuple[str, str, str, str, str, str],
        Sequence[Mapping[str, object]],
    ],
    *,
    confidence: float,
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for key in sorted(grouped):
        observations = [row["observation"] for row in grouped[key]]
        if any(not isinstance(value, Mapping) for value in observations):
            raise ArtifactInvalid("checked campaign contains an invalid observation")
        typed = [value for value in observations if isinstance(value, Mapping)]
        outcomes = tuple(str(value["outcome"]) for value in typed)
        latencies = tuple(
            float(value["latency_ns"])
            for value in typed
            if value["outcome"] == "completed"
        )
        measurements = summarize_measurements(
            outcomes=outcomes,
            completed_values=latencies,
        )
        completed_value = measurements["completed"]
        initiated_value = measurements["initiated"]
        if type(completed_value) is not int or type(initiated_value) is not int:
            raise ArtifactInvalid("invalid completed measurement counts")
        completed = completed_value
        initiated = initiated_value
        interval = wilson_interval(completed, initiated, confidence=confidence)
        row: dict[str, object] = {
            **_cell_identity(key),
            "initiated": initiated,
            "completed": completed,
            "failures": measurements["failures"],
            "timeouts": measurements["timeouts"],
            "completion_rate": completed / initiated,
            "completion_ci_low": interval.low,
            "completion_ci_high": interval.high,
            "median_latency_ns": measurements["median"],
            "p95_latency_ns": measurements["p95"],
        }
        if "p99" in measurements:
            row["p99_latency_ns"] = measurements["p99"]
        output.append(row)
    return output


def _reference_key(
    key: tuple[str, str, str, str, str, str],
) -> tuple[str, str, str, str, str, str]:
    return (
        key[0],
        "edgecitadel",
        "primary",
        "full-contract",
        key[4],
        key[5],
    )


def _correctness_comparisons(
    grouped: Mapping[
        tuple[str, str, str, str, str, str],
        Sequence[Mapping[str, object]],
    ],
    *,
    confidence: float,
    bootstrap_samples: int,
    seed: int,
) -> list[dict[str, object]]:
    comparisons: list[dict[str, object]] = []
    for index, key in enumerate(sorted(grouped)):
        reference_key = _reference_key(key)
        if key == reference_key or reference_key not in grouped:
            continue
        candidate = grouped[key]
        reference = grouped[reference_key]
        candidate_outcomes = [
            str(row["observation"]["outcome"])  # type: ignore[index]
            for row in candidate
        ]
        reference_outcomes = [
            str(row["observation"]["outcome"])  # type: ignore[index]
            for row in reference
        ]
        risk = newcombe_risk_difference_interval(
            candidate_outcomes.count("completed"),
            len(candidate_outcomes),
            reference_outcomes.count("completed"),
            len(reference_outcomes),
            confidence=confidence,
        )
        reference_by_block = {
            block: row for row in reference if type(block := row["block"]) is int
        }
        latency_pairs: list[tuple[float, float]] = []
        for row in candidate:
            block_value = row["block"]
            if type(block_value) is not int:
                raise ArtifactInvalid("checked campaign contains an invalid block")
            block = block_value
            reference_row = reference_by_block.get(block)
            candidate_observation = row["observation"]
            reference_observation = (
                reference_row.get("observation") if reference_row else None
            )
            if (
                isinstance(candidate_observation, Mapping)
                and isinstance(reference_observation, Mapping)
                and candidate_observation.get("outcome") == "completed"
                and reference_observation.get("outcome") == "completed"
            ):
                latency_pairs.append(
                    (
                        float(reference_observation["latency_ns"]),
                        float(candidate_observation["latency_ns"]),
                    )
                )
        comparison: dict[str, object] = {
            **_cell_identity(key),
            "baseline_mode": "edgecitadel",
            "baseline_variant": "primary",
            "baseline_ablation": "full-contract",
            "completion_risk_difference": risk.estimate,
            "completion_risk_difference_ci_low": risk.low,
            "completion_risk_difference_ci_high": risk.high,
        }
        if latency_pairs:
            comparison["latency"] = paired_median_change(
                latency_pairs,
                seed=seed + index,
                samples=bootstrap_samples,
                confidence=confidence,
            )
        comparisons.append(comparison)
    return comparisons


def _cost_rows(
    grouped: Mapping[
        tuple[str, str, str, str, str, str],
        Sequence[Mapping[str, object]],
    ],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for key in sorted(grouped):
        for metric in _COST_METRICS:
            values: list[float] = []
            outcomes: list[str] = []
            for row in grouped[key]:
                observation = row["observation"]
                if not isinstance(observation, Mapping):
                    raise ArtifactInvalid(
                        "checked campaign contains an invalid observation"
                    )
                resources = observation.get("resources")
                if not isinstance(resources, Mapping):
                    raise ArtifactInvalid("checked campaign contains invalid resources")
                value = resources.get(metric)
                if not isinstance(value, (int, float)) or isinstance(value, bool):
                    raise ArtifactInvalid(
                        f"checked campaign lacks cost metric {metric}"
                    )
                values.append(float(value))
                outcomes.append(str(observation["outcome"]))
            output_row: dict[str, object] = {
                **_cell_identity(key),
                "metric": metric,
                "initiated": len(values),
                "completed": outcomes.count("completed"),
                "failures": outcomes.count("failed"),
                "timeouts": outcomes.count("timeout"),
                "median": sample_median(values),
                "p95": nearest_rank_percentile(values, 0.95),
            }
            if len(values) >= 1000:
                output_row["p99"] = nearest_rank_percentile(values, 0.99)
            output.append(output_row)
    return output


def _csv_bytes(rows: Sequence[Mapping[str, object]]) -> bytes:
    if not rows:
        return b"\n"
    fieldnames = tuple(dict.fromkeys(key for row in rows for key in row))
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, value)
    finally:
        os.close(descriptor)


def _report_markdown(summary: Mapping[str, object]) -> bytes:
    correctness = summary["correctness"]
    comparisons = summary["comparisons"]
    cost = summary["cost"]
    if not isinstance(correctness, Sequence) or not isinstance(comparisons, Sequence):
        raise ArtifactInvalid("invalid analysis summary")
    if not isinstance(cost, Sequence):
        raise ArtifactInvalid("invalid analysis summary")
    lines = [
        "# Checked Campaign Analysis",
        "",
        f"Campaign: `{summary['campaign_id']}`",
        f"Input SHA-256: `{summary['input_sha256']}`",
        "",
        "## Contents",
        "",
        f"- Correctness and latency cells: {len(correctness)}",
        f"- Paired comparisons: {len(comparisons)}",
        f"- Cost rows: {len(cost)}",
        "",
        "All latency summaries contain completed observations only. Failure and ",
        "timeout counts remain adjacent in `summary.json` and the CSV tables.",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def analyze_campaign(
    campaign: Path,
    output: Path,
    *,
    confidence: float = 0.95,
    bootstrap_samples: int | None = None,
) -> dict[str, Path]:
    """Validate and render one campaign, refusing pre-existing output."""
    campaign = campaign.resolve()
    output = output.resolve()
    if output.exists():
        raise FileExistsError(output)
    report = check_campaign(campaign, require_publication=True)
    report.require_valid()
    config = _read_json(campaign / "campaign-config.json")
    metadata = _read_json(campaign / "campaign.json")
    schedule = _read_jsonl(campaign / "schedule.jsonl")
    configured_samples = config.get("bootstrap_samples")
    samples = configured_samples if bootstrap_samples is None else bootstrap_samples
    if type(samples) is not int or samples < 10_000:
        raise ValueError("bootstrap samples must be at least 10000")
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be between zero and one")
    seed = config.get("bootstrap_seed")
    if type(seed) is not int or seed < 0:
        raise ArtifactInvalid("checked campaign has an invalid bootstrap seed")
    grouped = _measured_rows(campaign, schedule, config)
    correctness = _correctness_rows(grouped, confidence=confidence)
    comparisons = _correctness_comparisons(
        grouped,
        confidence=confidence,
        bootstrap_samples=samples,
        seed=seed,
    )
    cost = _cost_rows(grouped)
    summary: dict[str, Any] = {
        "schema_version": "research-analysis.v1",
        "campaign_id": metadata["campaign_id"],
        "input_sha256": _input_sha256(campaign, schedule),
        "parameters": {
            "bootstrap_samples": samples,
            "confidence": confidence,
            "seed": seed,
        },
        "correctness": correctness,
        "comparisons": comparisons,
        "cost": cost,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{output.name}.tmp-",
            dir=output.parent,
        )
    )
    paths = {
        "summary.json": staging / "summary.json",
        "report.md": staging / "report.md",
        "tables/correctness.csv": staging / "tables/correctness.csv",
        "tables/cost.csv": staging / "tables/cost.csv",
        "figures/correctness.json": staging / "figures/correctness.json",
        "figures/cost.json": staging / "figures/cost.json",
    }
    try:
        write_json(paths["summary.json"], summary)
        _write_bytes(paths["report.md"], _report_markdown(summary))
        _write_bytes(paths["tables/correctness.csv"], _csv_bytes(correctness))
        _write_bytes(paths["tables/cost.csv"], _csv_bytes(cost))
        write_json(
            paths["figures/correctness.json"],
            {
                "schema_version": "research-figure-data.v1",
                "input_sha256": summary["input_sha256"],
                "cells": correctness,
                "comparisons": comparisons,
            },
        )
        write_json(
            paths["figures/cost.json"],
            {
                "schema_version": "research-figure-data.v1",
                "input_sha256": summary["input_sha256"],
                "cells": cost,
            },
        )
        if output.exists():
            raise FileExistsError(output)
        os.rename(staging, output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {name: output / Path(name) for name in paths}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--confidence", type=float, default=0.95)
    parser.add_argument("--bootstrap-samples", type=int)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    try:
        if (
            not arguments.input_root.is_absolute()
            or not arguments.output_root.is_absolute()
        ):
            raise ValueError("analysis roots must be absolute")
        analyze_campaign(
            arguments.input_root / arguments.campaign,
            arguments.output_root / arguments.campaign,
            confidence=arguments.confidence,
            bootstrap_samples=arguments.bootstrap_samples,
        )
    except (ArtifactInvalid, FileExistsError, OSError, ValueError) as error:
        print(f"analysis: INVALID: {error}", file=sys.stderr)
        return 2
    print("analysis: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["analyze_campaign", "main"]
