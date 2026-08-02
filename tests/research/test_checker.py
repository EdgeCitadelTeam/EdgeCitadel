"""Campaign-level artifact validation contracts."""

from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

import scripts.research.check_artifact as checker_module
from scripts.research.check_artifact import (
    ArtifactInvalid,
    CheckReport,
    check_bundle,
    check_campaign,
)
from scripts.research.evidence import (
    file_sha256,
    finalize_bundle,
    manifest_sha256,
    write_json,
    write_jsonl,
)
from scripts.research.workload_matrix import MatrixCell

MANIFEST_SCHEMA = Path("schemas/research-manifest.v1.json")
SOURCE = {
    "commit": "a" * 40,
    "git_dirty": False,
    "source_sha256": "b" * 64,
    "paths": ["scripts/research/run_artifact.py"],
}
CELL = {
    "workload": "W1",
    "mode": "edgecitadel",
    "variant": "primary",
    "ablation": "full-contract",
    "timeout_seconds": 30,
}
SECOND_CELL = {**CELL, "mode": "core-only"}
COMPONENTS = ["controller", "broker", "worker", "observer"]


def _config(
    *,
    warmup_blocks: int = 5,
    measured_blocks: int = 30,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "campaign_id": "tiny-paper",
        "seed": 7,
        "warmup_blocks": warmup_blocks,
        "measured_blocks": measured_blocks,
        "bootstrap_seed": 11,
        "bootstrap_samples": 10_000,
        "hardware_profile": "x86_64-controller",
        "network_profile": "lan",
        "sampler_interval_ms": 100,
        "idle_baseline_seconds": 2,
        "workload_timeouts": {
            workload: 330 if workload == "W6b" else 35 if workload == "W7" else 30
            for workload in (
                "W1",
                "W2",
                "W3",
                "W4",
                "W5",
                "W6a",
                "W6b",
                "W6c",
                "W7",
                "W8",
            )
        },
        "resource_components": COMPONENTS,
    }


def _observation(
    *,
    outcome: str = "completed",
    components: list[str] | None = None,
) -> dict[str, object]:
    return {
        "outcome": outcome,
        "initiated": 1,
        "accepted": 1,
        "delivered": 1 if outcome == "completed" else 0,
        "handler_attempts": 1,
        "executions": 1,
        "side_effects": 0,
        "prepared_outcomes": 1,
        "logical_terminals": 1 if outcome == "completed" else 0,
        "distinct_terminal_ids": 1 if outcome == "completed" else 0,
        "publication_attempts": 1,
        "wire_deliveries": 1,
        "progress_generated": None,
        "progress_live_delivered": None,
        "progress_replay_delivered": None,
        "progress_missing": None,
        "poison": 0,
        "inapplicable_crash_points": [],
        "timed_out": outcome == "timeout",
        "final_transport": {},
        "started_monotonic_ns": 10_000,
        "ended_monotonic_ns": 1_010_000,
        "latency_ns": 1_000_000 if outcome == "completed" else None,
        "resources": {
            "components": components or COMPONENTS,
            "cpu_seconds": 0.25,
            "peak_rss_bytes": 1024,
            "rss_seconds": 100.0,
            "rx_bytes": 10,
            "tx_bytes": 20,
            "application_bytes": 5,
            "nats_connection_bytes": 30,
            "http_bytes": 0,
            "storage_bytes": 40,
            "message_count_delta": 1,
            "sampler_cpu_seconds": 0.001,
            "cost_claims_valid": True,
        },
    }


def _write_campaign(
    root: Path,
    *,
    outcome: str = "completed",
    components: list[str] | None = None,
    cells: tuple[dict[str, object], ...] = (CELL,),
    seeded_schedule: bool = True,
    warmup_blocks: int = 5,
    measured_blocks: int = 30,
    images: dict[str, object] | None = None,
    source: dict[str, object] = SOURCE,
    observation_overrides: dict[str, object] | None = None,
) -> Path:
    campaign = root / "tiny-paper"
    campaign.mkdir(parents=True)
    config = _config(
        warmup_blocks=warmup_blocks,
        measured_blocks=measured_blocks,
    )
    write_json(campaign / "campaign-config.json", config)
    schedule_rows: list[dict[str, object]] = []
    for block in range(warmup_blocks + measured_blocks):
        ordered = list(cells)
        if seeded_schedule:
            random.Random(7 + block).shuffle(ordered)
        for cell in ordered:
            schedule_rows.append(
                {
                    "run_id": f"ec-7-{len(schedule_rows):05d}",
                    "block": block,
                    "measured": block >= warmup_blocks,
                    "cell": cell,
                }
            )
    schedule = tuple(schedule_rows)
    write_jsonl(campaign / "schedule.jsonl", schedule)
    bundle_paths = [str((campaign / "bundles" / row["run_id"]).resolve()) for row in schedule]
    metadata = {
        "schema_version": "research-campaign.v1",
        "campaign_id": "tiny-paper",
        "campaign_path": str(campaign.resolve()),
        "profile": "paper",
        "source": source,
        "config_sha256": file_sha256(campaign / "campaign-config.json"),
        "schedule_sha256": file_sha256(campaign / "schedule.jsonl"),
        "bundle_paths": bundle_paths,
    }
    write_json(campaign / "campaign.json", metadata)
    campaign_hash = file_sha256(campaign / "campaign.json")
    for row in schedule:
        bundle = campaign / "bundles" / str(row["run_id"])
        bundle.mkdir(parents=True)
        row_cell = row["cell"]
        assert isinstance(row_cell, dict)
        observation = {
            **_observation(
                outcome=outcome if row["measured"] else "completed",
                components=components,
            ),
            **(observation_overrides or {}),
        }
        write_jsonl(bundle / "events.jsonl", ())
        write_json(bundle / "resources.json", observation["resources"])
        write_jsonl(
            bundle / "trials.jsonl",
            (
                {
                    "schema_version": "research-trial.v1",
                    "run_id": row["run_id"],
                    "trial_id": row["run_id"],
                    "block": row["block"],
                    "measured": row["measured"],
                    "cell": row_cell,
                    "observation": observation,
                    "timing": {
                        "started_monotonic_ns": observation["started_monotonic_ns"],
                        "ended_monotonic_ns": observation["ended_monotonic_ns"],
                    },
                    "events_artifact": "events.jsonl",
                    "resource_artifact": "resources.json",
                    "invariant_results": {"outcome_consistent": True},
                },
            ),
        )
        manifest = {
            "schema_version": "research-manifest.v1",
            "evidence_kind": "benchmark",
            "status": "PENDING",
            "run_id": row["run_id"],
            "campaign_id": "tiny-paper",
            "profile": "paper",
            "source": source,
            "command": ["scripts/research/run_artifact.py", "run", "--profile", "paper"],
            "timing": {},
            "host": {
                "system": "Linux",
                "architecture": "x86_64",
                "os_id": "ubuntu",
                "os_version": "24.04",
            },
            "dependencies": {},
            "images": images
            if images is not None
            else {
                "artifact": "sha256:" + "d" * 64,
                "nats": "nats@sha256:" + "e" * 64,
            },
            "compose_config_sha256": "c" * 64,
            "schemas": {"manifest": "research-manifest.v1"},
            "cleanup": {"completed": True},
            "artifacts": {},
            "transport_config": {"mode": row_cell["mode"]},
            "workload_config": row_cell,
            "metric_contract": {
                "status": "collected",
                "components": components or COMPONENTS,
                "sampler_interval_ms": 100,
                "idle_baseline_seconds": 2,
            },
            "campaign_contract": {
                "block": row["block"],
                "measured": row["measured"],
                "config_sha256": metadata["config_sha256"],
                "schedule_sha256": metadata["schedule_sha256"],
                "campaign_sha256": campaign_hash,
            },
        }
        assert finalize_bundle(bundle, manifest, MANIFEST_SCHEMA) == "PASS"
    return campaign


@pytest.fixture(autouse=True)
def _tiny_fixed_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        checker_module,
        "required_matrix_cells",
        lambda: (MatrixCell(**CELL),),
    )


def test_bundle_api_reports_stable_kind_mismatch_and_typed_failure(
    tmp_path: Path,
) -> None:
    campaign = _write_campaign(tmp_path)
    bundle = next((campaign / "bundles").iterdir())

    report = check_bundle(bundle)
    mismatch = check_bundle(bundle, expected_kind="operator")

    assert isinstance(report, CheckReport)
    assert report.valid
    assert report.issues == ()
    report.require_valid()
    assert mismatch.issues[0].code == "ARTIFACT_KIND_MISMATCH"
    with pytest.raises(ArtifactInvalid, match="ARTIFACT_KIND_MISMATCH"):
        mismatch.require_valid()


def test_complete_campaign_passes_base_validation(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)

    report = check_campaign(campaign)

    assert report.valid
    assert report.issues == ()


def test_campaign_rejects_a_missing_scheduled_bundle(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)
    missing = campaign / "bundles" / "ec-7-00002"
    for path in sorted(missing.rglob("*"), reverse=True):
        path.unlink()
    missing.rmdir()

    report = check_campaign(campaign)

    assert "CAMPAIGN_BUNDLE_MISSING" in {issue.code for issue in report.issues}


def test_campaign_rejects_schedule_mutation_after_capture(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)
    schedule = campaign / "schedule.jsonl"
    schedule.write_text(schedule.read_text().replace('"block":2', '"block":9'))

    report = check_campaign(campaign)

    assert "CAMPAIGN_SCHEDULE_HASH_MISMATCH" in {
        issue.code for issue in report.issues
    }


def test_campaign_rejects_trial_schema_violation(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)
    trial_path = campaign / "bundles" / "ec-7-00000" / "trials.jsonl"
    trial = json.loads(trial_path.read_text())
    del trial["trial_id"]
    trial_path.write_text(json.dumps(trial, sort_keys=True, separators=(",", ":")) + "\n")

    report = check_campaign(campaign)

    assert "CAMPAIGN_TRIAL_SCHEMA_INVALID" in {
        issue.code for issue in report.issues
    }


def test_publication_rejects_w5_without_raw_crash_evidence(tmp_path: Path) -> None:
    w5 = {**CELL, "workload": "W5"}

    report = check_campaign(_write_campaign(tmp_path, cells=(w5,)), require_publication=True)

    assert "CAMPAIGN_WORKLOAD_EVIDENCE_INVALID" in {
        issue.code for issue in report.issues
    }


def test_publication_rejects_ineligible_manifest_host(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)
    manifest_path = campaign / "bundles" / "ec-7-00000" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["host"] = {"system": "Darwin", "architecture": "arm64"}
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_HOST_INELIGIBLE" in {issue.code for issue in report.issues}


@pytest.mark.parametrize(
    ("cell", "evidence"),
    (
        (
            {**CELL, "workload": "W6a"},
            {
                "wire_retry": {
                    "envelope_ids": ["first", "second"],
                    "accepted": [True, True],
                    "stream_sequences": [1, 1],
                    "duplicate_flags": [False, True],
                }
            },
        ),
        (
            {**CELL, "workload": "W6b"},
            {
                "semantic_retry": {
                    "first_envelope_id": "same",
                    "second_envelope_id": "same",
                    "task_id": "task-1",
                    "retry_window": {
                        "broker_duplicate_window_seconds": 1,
                        "retry_elapsed_seconds": 2,
                        "ledger_retention_seconds": 3,
                    },
                }
            },
        ),
        (
            {**CELL, "workload": "W6c"},
            {
                "collision": {
                    "rejections": 1,
                    "executions": 0,
                    "cached_output_exposure": 0,
                }
            },
        ),
    ),
)
def test_publication_rejects_raw_workload_evidence_that_contradicts_trial(
    tmp_path: Path,
    cell: dict[str, object],
    evidence: dict[str, object],
) -> None:
    report = check_campaign(
        _write_campaign(
            tmp_path,
            cells=(cell,),
            observation_overrides={"workload_evidence": evidence},
        ),
        require_publication=True,
    )

    assert "CAMPAIGN_WORKLOAD_EVIDENCE_INVALID" in {
        issue.code for issue in report.issues
    }


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    (
        ("timing", {"started_monotonic_ns": 1, "ended_monotonic_ns": 2}, "CAMPAIGN_TRIAL_TIMING_MISMATCH"),
        ("resource_artifact", "../resources.json", "CAMPAIGN_TRIAL_REFERENCE_INVALID"),
        ("invariant_results", {"outcome_consistent": False}, "CAMPAIGN_TRIAL_INVARIANT_FAILED"),
    ),
)
def test_campaign_rejects_unbound_trial_envelope_fields(
    tmp_path: Path,
    field: str,
    value: object,
    expected_code: str,
) -> None:
    campaign = _write_campaign(tmp_path)
    trial_path = campaign / "bundles" / "ec-7-00000" / "trials.jsonl"
    trial = json.loads(trial_path.read_text())
    trial[field] = value
    trial_path.write_text(json.dumps(trial, sort_keys=True, separators=(",", ":")) + "\n")

    report = check_campaign(campaign)

    assert expected_code in {issue.code for issue in report.issues}


def test_publication_campaign_rejects_harness_invalid_repetition(
    tmp_path: Path,
) -> None:
    campaign = _write_campaign(tmp_path, outcome="harness-invalid")

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_HARNESS_INVALID" in {issue.code for issue in report.issues}


def test_publication_campaign_rejects_component_membership_drift(
    tmp_path: Path,
) -> None:
    campaign = _write_campaign(
        tmp_path,
        components=["controller", "broker", "worker"],
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_COMPONENTS_MISMATCH" in {
        issue.code for issue in report.issues
    }


@pytest.mark.parametrize(
    "observation_overrides",
    (
        {"logical_terminals": -1},
        {"wire_deliveries": 1.5},
        {"initiated": True},
    ),
)
def test_campaign_rejects_invalid_observation_count_types(
    tmp_path: Path,
    observation_overrides: dict[str, object],
) -> None:
    campaign = _write_campaign(
        tmp_path,
        observation_overrides=observation_overrides,
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_TRIAL_SCHEMA_INVALID" in {
        issue.code for issue in report.issues
    }


@pytest.mark.parametrize(
    "observation_overrides",
    (
        {"logical_terminals": 2, "distinct_terminal_ids": 2},
        {"logical_terminals": 1, "distinct_terminal_ids": 2},
        {"wire_deliveries": 0},
        {"executions": 2},
        {"executions": None},
    ),
)
def test_publication_rejects_completed_w1_invariant_failures(
    tmp_path: Path,
    observation_overrides: dict[str, object],
) -> None:
    campaign = _write_campaign(
        tmp_path,
        observation_overrides=observation_overrides,
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_TRIAL_INVARIANT_FAILED" in {
        issue.code for issue in report.issues
    }


def test_publication_accepts_w2_parent_and_child_execution_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cell = {**CELL, "workload": "W2"}
    monkeypatch.setattr(
        checker_module,
        "required_matrix_cells",
        lambda: (MatrixCell(**cell),),
    )
    campaign = _write_campaign(
        tmp_path,
        cells=(cell,),
        observation_overrides={"executions": 2},
    )

    report = check_campaign(campaign, require_publication=True)

    assert report.valid


@pytest.mark.parametrize(
    "observation_overrides",
    (
        {"outcome": "failed"},
        {"outcome": "timeout", "timed_out": False, "latency_ns": None},
    ),
)
def test_publication_rejects_outcome_labels_that_contradict_observations(
    tmp_path: Path,
    observation_overrides: dict[str, object],
) -> None:
    campaign = _write_campaign(
        tmp_path,
        observation_overrides=observation_overrides,
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_OUTCOME_MISMATCH" in {issue.code for issue in report.issues}


def test_publication_rejects_image_identity_drift_between_bundles(
    tmp_path: Path,
) -> None:
    campaign = _write_campaign(tmp_path)
    bundle = campaign / "bundles" / "ec-7-00001"
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["images"] = {
        "artifact": "sha256:" + "1" * 64,
        "nats": "nats@sha256:" + "2" * 64,
    }
    manifest["manifest_sha256"] = manifest_sha256(manifest)
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n"
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_IMAGES_MISMATCH" in {issue.code for issue in report.issues}


@pytest.mark.parametrize("invalid_cost", (-1,))
def test_publication_rejects_invalid_cost_measurements(
    tmp_path: Path,
    invalid_cost: float,
) -> None:
    resources = _observation()["resources"]
    assert isinstance(resources, dict)
    campaign = _write_campaign(
        tmp_path,
        observation_overrides={
            "resources": {**resources, "cpu_seconds": invalid_cost},
        },
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_COST_INVALID" in {issue.code for issue in report.issues}


def test_publication_rejects_latency_that_disagrees_with_trial_clock(
    tmp_path: Path,
) -> None:
    campaign = _write_campaign(
        tmp_path,
        observation_overrides={"latency_ns": 999_999},
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_LATENCY_INVALID" in {issue.code for issue in report.issues}


def test_campaign_issues_are_sorted_and_stable(tmp_path: Path) -> None:
    report = check_campaign(tmp_path / "absent", require_publication=True)

    assert report.valid is False
    assert report.issues == tuple(
        sorted(report.issues, key=lambda issue: (issue.code, issue.path, issue.message))
    )


def test_publication_campaign_requires_exact_block_counts(tmp_path: Path) -> None:
    campaign = _write_campaign(
        tmp_path,
        warmup_blocks=4,
        measured_blocks=31,
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_BLOCK_CONTRACT_MISMATCH" in {
        issue.code for issue in report.issues
    }


def test_publication_campaign_reconstructs_seeded_schedule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cells = (CELL, SECOND_CELL)
    monkeypatch.setattr(
        checker_module,
        "required_matrix_cells",
        lambda: tuple(MatrixCell(**cell) for cell in cells),
    )
    campaign = _write_campaign(
        tmp_path,
        cells=cells,
        seeded_schedule=False,
    )

    report = check_campaign(campaign, require_publication=True)

    assert "CAMPAIGN_SCHEDULE_ORDER_MISMATCH" in {
        issue.code for issue in report.issues
    }


@pytest.mark.parametrize(
    ("source", "images", "expected_code"),
    (
        ({**SOURCE, "commit": "not-a-commit"}, None, "CAMPAIGN_SOURCE_INELIGIBLE"),
        (SOURCE, {}, "CAMPAIGN_IMAGES_INELIGIBLE"),
        (SOURCE, {"artifact": "artifact:latest"}, "CAMPAIGN_IMAGES_INELIGIBLE"),
    ),
)
def test_publication_requires_committed_source_and_immutable_images(
    tmp_path: Path,
    source: dict[str, object],
    images: dict[str, object] | None,
    expected_code: str,
) -> None:
    campaign = _write_campaign(tmp_path, source=source, images=images)

    report = check_campaign(campaign, require_publication=True)

    assert expected_code in {issue.code for issue in report.issues}


def test_campaign_metadata_is_canonical_json(tmp_path: Path) -> None:
    campaign = _write_campaign(tmp_path)
    metadata = json.loads((campaign / "campaign.json").read_text())

    assert metadata["schema_version"] == "research-campaign.v1"
    assert metadata["bundle_paths"] == sorted(metadata["bundle_paths"])
