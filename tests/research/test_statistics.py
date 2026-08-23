"""Reference contracts for deterministic research estimators."""

from __future__ import annotations

import pytest

from scripts.research.statistics import (
    newcombe_risk_difference_interval,
    paired_median_change,
    summarize_measurements,
    wilson_interval,
)


@pytest.mark.parametrize(
    ("successes", "total", "expected"),
    (
        (0, 10, (0.0, 0.277533)),
        (5, 10, (0.236593, 0.763407)),
        (10, 10, (0.722467, 1.0)),
    ),
)
def test_wilson_95_percent_reference_vectors(
    successes: int,
    total: int,
    expected: tuple[float, float],
) -> None:
    interval = wilson_interval(successes, total)

    assert interval.low == pytest.approx(expected[0], abs=1e-6)
    assert interval.high == pytest.approx(expected[1], abs=1e-6)


def test_newcombe_risk_difference_uses_hybrid_score_bounds() -> None:
    difference = newcombe_risk_difference_interval(8, 10, 5, 10)

    assert difference.estimate == pytest.approx(0.3)
    assert difference.low == pytest.approx(-0.106672, abs=1e-6)
    assert difference.high == pytest.approx(0.599872, abs=1e-6)


def test_measurement_summary_uses_completed_values_without_imputation() -> None:
    summary = summarize_measurements(
        outcomes=("completed", "completed", "timeout", "failed"),
        completed_values=(1.0, 9.0),
    )

    assert summary == {
        "initiated": 4,
        "completed": 2,
        "failures": 1,
        "timeouts": 1,
        "median": 5.0,
        "p95": 9.0,
    }


def test_p99_is_guarded_until_one_thousand_completed_measurements() -> None:
    below = summarize_measurements(
        outcomes=("completed",) * 999,
        completed_values=tuple(float(value) for value in range(1, 1000)),
    )
    at_threshold = summarize_measurements(
        outcomes=("completed",) * 1000,
        completed_values=tuple(float(value) for value in range(1, 1001)),
    )

    assert "p99" not in below
    assert at_threshold["p99"] == 990.0


def test_seeded_paired_bootstrap_is_repeatable_and_preserves_block_pairs() -> None:
    pairs = ((1.0, 3.0), (2.0, 4.0), (10.0, 12.0), (20.0, 22.0))

    first = paired_median_change(pairs, seed=20260725, samples=10_000)
    second = paired_median_change(pairs, seed=20260725, samples=10_000)

    assert first == second
    assert first["paired_blocks"] == 4
    assert first["median_difference"] == 2.0
    assert first["relative_change"] == pytest.approx(0.6)
    assert first["bootstrap_interval"] == {"low": 2.0, "high": 2.0}


def test_paired_estimator_uses_the_median_of_within_block_differences() -> None:
    result = paired_median_change(
        ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0)),
        seed=9,
        samples=10_000,
    )

    assert result["median_difference"] == 0.0
    assert result["relative_change"] is None


@pytest.mark.parametrize(
    ("successes", "total"),
    ((-1, 10), (11, 10), (0, 0)),
)
def test_proportion_estimators_reject_invalid_counts(
    successes: int,
    total: int,
) -> None:
    with pytest.raises(ValueError, match="counts"):
        wilson_interval(successes, total)
