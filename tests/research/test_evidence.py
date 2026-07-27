"""Contracts for immutable experiment evidence bundles."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

from scripts.research.evidence import (
    capture_source_provenance,
    finalize_bundle,
    verify_source_provenance,
    write_json,
)

_SCHEMA_PATH = Path("schemas/research-manifest.v1.json")
_EVENT_SCHEMA_PATH = Path("schemas/research-event.v1.json")
_TRIAL_SCHEMA_PATH = Path("schemas/research-trial.v1.json")


def _git_source(path: Path) -> Path:
    path.mkdir()
    subprocess.run(["git", "init", "--quiet"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "tests@example.invalid"], cwd=path, check=True
    )
    subprocess.run(
        ["git", "config", "user.name", "research tests"], cwd=path, check=True
    )
    (path / "scripts" / "research").mkdir(parents=True)
    (path / "scripts" / "research" / "workload_matrix.py").write_text("VALUE = 1\n")
    subprocess.run(["git", "add", "."], cwd=path, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "initial"], cwd=path, check=True)
    return path


def _manifest(source: object) -> dict[str, object]:
    return {
        "schema_version": "research-manifest.v1",
        "evidence_kind": "benchmark",
        "status": "PENDING",
        "run_id": "run-1",
        "campaign_id": "campaign-1",
        "profile": "matrix-smoke",
        "source": source.to_dict(),
        "command": ["scripts/research/run_artifact.py"],
        "timing": {"started_epoch": "2026-07-26T00:00:00Z"},
        "host": {"platform": "test"},
        "dependencies": {},
        "images": {},
        "compose_config_sha256": "0" * 64,
        "schemas": {},
        "cleanup": {"completed": True},
        "artifacts": {},
        "transport_config": {},
        "workload_config": {},
        "metric_contract": {},
    }


def test_write_json_is_canonical_and_finalization_hashes_raw_bundle(
    tmp_path: Path,
) -> None:
    source_root = _git_source(tmp_path / "source")
    source = capture_source_provenance(source_root)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    write_json(bundle / "preflight.json", {"z": 1, "a": 2})

    assert (bundle / "preflight.json").read_bytes() == b'{"a":2,"z":1}\n'
    assert finalize_bundle(bundle, _manifest(source), _SCHEMA_PATH) == "PASS"

    manifest = json.loads((bundle / "manifest.json").read_text())
    assert manifest["status"] == "PASS"
    assert manifest["artifacts"]["preflight.json"]
    assert manifest["manifest_sha256"]


def test_finalization_refuses_to_overwrite_a_final_bundle_or_raw_evidence(
    tmp_path: Path,
) -> None:
    source_root = _git_source(tmp_path / "source")
    source = capture_source_provenance(source_root)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    write_json(bundle / "events.jsonl", [{"sequence": 1}])
    assert finalize_bundle(bundle, _manifest(source), _SCHEMA_PATH) == "PASS"
    original_raw = (bundle / "events.jsonl").read_bytes()
    original_manifest = (bundle / "manifest.json").read_bytes()

    assert finalize_bundle(bundle, _manifest(source), _SCHEMA_PATH) == "INVALID"
    assert (bundle / "events.jsonl").read_bytes() == original_raw
    assert (bundle / "manifest.json").read_bytes() == original_manifest


@pytest.mark.parametrize(
    "raw",
    (
        {"token": "a" * 64},
        {"authorization": "Bearer top-secret"},
        {"key": "-----BEGIN PRIVATE KEY-----"},
    ),
)
def test_finalization_rejects_secret_bearing_raw_evidence(
    tmp_path: Path,
    raw: dict[str, str],
) -> None:
    source_root = _git_source(tmp_path / "source")
    source = capture_source_provenance(source_root)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    write_json(bundle / "preflight.json", raw)

    assert finalize_bundle(bundle, _manifest(source), _SCHEMA_PATH) == "INVALID"
    assert not (bundle / "manifest.json").exists()


def test_source_provenance_is_captured_before_output_and_detects_source_changes(
    tmp_path: Path,
) -> None:
    source_root = _git_source(tmp_path / "source")
    source = capture_source_provenance(source_root)
    external_output = tmp_path / "outside"
    external_output.mkdir()
    write_json(external_output / "campaign.json", {"source": source.to_dict()})

    assert verify_source_provenance(source_root, source)
    generated = source_root / "docs" / "research" / "results" / "raw" / "run-1"
    generated.mkdir(parents=True)
    write_json(generated / "campaign.json", {"generated": True})
    assert verify_source_provenance(source_root, source)
    (source_root / "scripts" / "research" / "workload_matrix.py").write_text(
        "VALUE = 2\n"
    )
    assert not verify_source_provenance(source_root, source)


def test_event_and_trial_schemas_require_raw_references_and_w5_crash_records() -> None:
    event_schema = Draft202012Validator(json.loads(_EVENT_SCHEMA_PATH.read_text()))
    event_schema.validate(
        {
            "run_id": "run-1",
            "trial_id": "trial-1",
            "sequence": 0,
            "monotonic_ns": 1,
            "epoch_time": "2026-07-26T00:00:00Z",
            "component": "worker",
            "event": "terminal",
            "data": {},
        }
    )
    trial_schema = Draft202012Validator(json.loads(_TRIAL_SCHEMA_PATH.read_text()))
    trial = {
        "run_id": "run-1",
        "trial_id": "trial-1",
        "matrix_cell": {
            "workload": "W5",
            "mode": "edgecitadel",
            "variant": "primary",
            "ablation": "full-contract",
            "timeout_seconds": 30,
        },
        "outcome": "completed",
        "initiated": 6,
        "accepted": 6,
        "delivered": 6,
        "handler_attempts": None,
        "executions": 6,
        "side_effects": 6,
        "prepared_outcomes": None,
        "logical_terminals": 6,
        "distinct_terminal_ids": 6,
        "publication_attempts": 6,
        "wire_deliveries": 6,
        "poison": 0,
        "timing": {"started_monotonic_ns": 1, "ended_monotonic_ns": 2},
        "events_artifact": "events.jsonl",
        "resource_artifact": "resources.json",
        "invariant_results": {},
        "crash_subtrials": [
            {
                "crash_point": "after-receive-before-handler",
                "ledger_state": "prepared",
                "inbound_deliveries": 1,
                "executions": 1,
                "side_effects": 1,
                "publication_attempts": 1,
                "logical_terminals": 1,
                "wire_deliveries": 1,
                "final_consumer_state": {},
            }
        ],
    }
    trial_schema.validate(trial)
    trial.pop("crash_subtrials")
    with pytest.raises(ValidationError):
        trial_schema.validate(trial)
