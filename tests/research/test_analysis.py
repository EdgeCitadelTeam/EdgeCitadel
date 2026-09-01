"""Deterministic publication-analysis contracts."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import scripts.research.analyze_artifact as analyzer_module
import scripts.research.check_artifact as checker_module
from scripts.research.analyze_artifact import analyze_campaign, main
from scripts.research.workload_matrix import MatrixCell
from tests.research.test_checker import CELL, _write_campaign

EXPECTED_OUTPUTS = (
    "figures/correctness.json",
    "figures/cost.json",
    "report.md",
    "summary.json",
    "tables/correctness.csv",
    "tables/cost.csv",
)


@pytest.fixture
def tiny_publication_campaign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    cell = MatrixCell(**CELL)
    monkeypatch.setattr(checker_module, "required_matrix_cells", lambda: (cell,))
    return _write_campaign(tmp_path / "input")


def test_analysis_is_byte_identical_and_writes_only_declared_outputs(
    tiny_publication_campaign: Path,
    tmp_path: Path,
) -> None:
    first = tmp_path / "derived-a"
    second = tmp_path / "derived-b"

    analyze_campaign(
        tiny_publication_campaign,
        first,
        confidence=0.95,
        bootstrap_samples=10_000,
    )
    analyze_campaign(
        tiny_publication_campaign,
        second,
        confidence=0.95,
        bootstrap_samples=10_000,
    )

    assert (
        tuple(
            path.relative_to(first).as_posix()
            for path in sorted(first.rglob("*"))
            if path.is_file()
        )
        == EXPECTED_OUTPUTS
    )
    for relative in EXPECTED_OUTPUTS:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_summary_retains_outcome_counts_and_guards_p99(
    tiny_publication_campaign: Path,
    tmp_path: Path,
) -> None:
    output = tmp_path / "derived"

    analyze_campaign(tiny_publication_campaign, output)

    summary = json.loads((output / "summary.json").read_text())
    assert summary["schema_version"] == "research-analysis.v1"
    assert summary["campaign_id"] == "tiny-paper"
    assert summary["parameters"] == {
        "bootstrap_samples": 10_000,
        "confidence": 0.95,
        "seed": 11,
    }
    assert len(summary["correctness"]) == 1
    row = summary["correctness"][0]
    assert row["initiated"] == row["completed"] == 30
    assert row["failures"] == row["timeouts"] == 0
    assert row["median_latency_ns"] == 1_000_000.0
    assert row["p95_latency_ns"] == 1_000_000.0
    assert "p99_latency_ns" not in row


def test_analysis_refuses_invalid_campaign_before_creating_output(
    tiny_publication_campaign: Path,
    tmp_path: Path,
) -> None:
    missing = tiny_publication_campaign / "bundles" / "ec-7-00002"
    for path in sorted(missing.rglob("*"), reverse=True):
        path.unlink()
    missing.rmdir()
    output = tmp_path / "derived"

    assert (
        main(
            [
                "--campaign",
                tiny_publication_campaign.name,
                "--input-root",
                str(tiny_publication_campaign.parent),
                "--output-root",
                str(output),
            ]
        )
        == 2
    )
    assert not output.exists()


def test_edgecitadel_ablation_is_compared_with_full_contract_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ablation = {
        **CELL,
        "variant": "ablation",
        "ablation": "none",
    }
    cells = (CELL, ablation)
    monkeypatch.setattr(
        checker_module,
        "required_matrix_cells",
        lambda: tuple(MatrixCell(**cell) for cell in cells),
    )
    campaign = _write_campaign(tmp_path / "input", cells=cells)
    output = tmp_path / "derived"

    analyze_campaign(campaign, output)

    summary = json.loads((output / "summary.json").read_text())
    assert len(summary["comparisons"]) == 1
    comparison = summary["comparisons"][0]
    assert comparison["mode"] == "edgecitadel"
    assert comparison["variant"] == "ablation"
    assert comparison["ablation"] == "none"
    assert comparison["baseline_mode"] == "edgecitadel"
    assert comparison["baseline_variant"] == "primary"
    assert comparison["baseline_ablation"] == "full-contract"


def test_analysis_refuses_nonpublication_profile_before_writing(
    tiny_publication_campaign: Path,
    tmp_path: Path,
) -> None:
    metadata_path = tiny_publication_campaign / "campaign.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["profile"] = "quick"
    metadata_path.write_text(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n"
    )
    output = tmp_path / "derived"

    assert (
        main(
            [
                "--campaign",
                tiny_publication_campaign.name,
                "--input-root",
                str(tiny_publication_campaign.parent),
                "--output-root",
                str(output),
            ]
        )
        == 2
    )
    assert not output.exists()


def test_analysis_rejects_existing_output_without_mutating_it(
    tiny_publication_campaign: Path,
    tmp_path: Path,
) -> None:
    output = tmp_path / "derived"
    output.mkdir()
    sentinel = output / "sentinel"
    sentinel.write_text("owned by caller\n")

    with pytest.raises(FileExistsError):
        analyze_campaign(tiny_publication_campaign, output)

    assert sentinel.read_text() == "owned by caller\n"


def test_late_render_failure_leaves_no_partial_publication_directory(
    tiny_publication_campaign: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "derived"

    def fail_write(_: Path, __: bytes) -> None:
        raise OSError("injected render failure")

    monkeypatch.setattr(analyzer_module, "_write_bytes", fail_write)

    with pytest.raises(OSError, match="injected render failure"):
        analyze_campaign(tiny_publication_campaign, output)

    assert not output.exists()
    assert not tuple(tmp_path.glob(".derived.tmp-*"))


def test_direct_script_help_does_not_shadow_the_standard_library() -> None:
    completed = subprocess.run(
        [
            "scripts/research/run-python",
            "scripts/research/analyze_artifact.py",
            "--help",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert all(
        option in completed.stdout
        for option in (
            "--campaign",
            "--confidence",
            "--bootstrap-samples",
            "--input-root",
            "--output-root",
        )
    )
