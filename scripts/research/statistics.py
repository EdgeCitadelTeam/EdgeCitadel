"""Deterministic estimators for checked research campaigns."""

from __future__ import annotations

import math
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Interval:
    low: float
    high: float


@dataclass(frozen=True)
class RiskDifferenceInterval:
    estimate: float
    low: float
    high: float


def _z_score(confidence: float) -> float:
    if not 0.0 < confidence < 1.0:
        raise ValueError("confidence must be between zero and one")
    return _normal_quantile(0.5 + confidence / 2.0)


def _normal_quantile(probability: float) -> float:
    """Acklam's deterministic inverse standard-normal approximation."""
    if not 0.0 < probability < 1.0:
        raise ValueError("probability must be between zero and one")
    a = (
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239,
    )
    b = (
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    )
    c = (
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838,
        -2.549732539343734,
        4.374664141464968,
        2.938163982698783,
    )
    d = (
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996,
        3.754408661907416,
    )
    lower = 0.02425
    upper = 1.0 - lower
    if probability < lower:
        q = math.sqrt(-2.0 * math.log(probability))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            (((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0
        )
    if probability > upper:
        q = math.sqrt(-2.0 * math.log(1.0 - probability))
        return -(
            (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
            / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        )
    q = probability - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
        * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    )


def sample_median(values: Iterable[float]) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("measurements must be nonempty")
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def wilson_interval(
    successes: int,
    total: int,
    *,
    confidence: float = 0.95,
) -> Interval:
    """Return the two-sided Wilson score interval for a binomial proportion."""
    if type(successes) is not int or type(total) is not int:
        raise ValueError("invalid counts")
    if total <= 0 or successes < 0 or successes > total:
        raise ValueError("invalid counts")
    z = _z_score(confidence)
    proportion = successes / total
    z_squared = z * z
    denominator = 1.0 + z_squared / total
    center = (proportion + z_squared / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / total + z_squared / (4.0 * total * total)
        )
        / denominator
    )
    return Interval(max(0.0, center - margin), min(1.0, center + margin))


def newcombe_risk_difference_interval(
    first_successes: int,
    first_total: int,
    second_successes: int,
    second_total: int,
    *,
    confidence: float = 0.95,
) -> RiskDifferenceInterval:
    """Return Newcombe's unpaired score interval for a risk difference."""
    first = wilson_interval(
        first_successes,
        first_total,
        confidence=confidence,
    )
    second = wilson_interval(
        second_successes,
        second_total,
        confidence=confidence,
    )
    first_rate = first_successes / first_total
    second_rate = second_successes / second_total
    estimate = first_rate - second_rate
    return RiskDifferenceInterval(
        estimate=estimate,
        low=estimate
        - math.sqrt((first_rate - first.low) ** 2 + (second.high - second_rate) ** 2),
        high=estimate
        + math.sqrt((first.high - first_rate) ** 2 + (second_rate - second.low) ** 2),
    )


def nearest_rank_percentile(values: Sequence[float], percentile: float) -> float:
    if not values:
        raise ValueError("measurements must be nonempty")
    if not 0.0 < percentile <= 1.0:
        raise ValueError("percentile must be between zero and one")
    ordered = sorted(float(value) for value in values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def summarize_measurements(
    *,
    outcomes: Iterable[str],
    completed_values: Sequence[float],
) -> dict[str, int | float | None]:
    """Summarize completed values while retaining every initiated outcome."""
    outcome_rows = tuple(outcomes)
    allowed = {"completed", "failed", "timeout"}
    if any(outcome not in allowed for outcome in outcome_rows):
        raise ValueError("invalid outcome")
    completed = outcome_rows.count("completed")
    values = tuple(float(value) for value in completed_values)
    if len(values) != completed or any(not math.isfinite(value) for value in values):
        raise ValueError("completed measurements do not match outcomes")
    result: dict[str, int | float | None] = {
        "initiated": len(outcome_rows),
        "completed": completed,
        "failures": outcome_rows.count("failed"),
        "timeouts": outcome_rows.count("timeout"),
        "median": sample_median(values) if values else None,
        "p95": nearest_rank_percentile(values, 0.95) if values else None,
    }
    if len(values) >= 1000:
        result["p99"] = nearest_rank_percentile(values, 0.99)
    return result


def paired_median_change(
    pairs: Sequence[tuple[float, float]],
    *,
    seed: int,
    samples: int = 10_000,
    confidence: float = 0.95,
) -> dict[str, object]:
    """Compare candidate values with a baseline using paired block resampling."""
    if not pairs:
        raise ValueError("paired measurements must be nonempty")
    if type(seed) is not int or type(samples) is not int or samples < 1:
        raise ValueError("invalid bootstrap configuration")
    _z_score(confidence)
    normalized = tuple((float(base), float(candidate)) for base, candidate in pairs)
    if any(not math.isfinite(value) for pair in normalized for value in pair):
        raise ValueError("paired measurements must be finite")
    baseline_median = sample_median(base for base, _ in normalized)
    candidate_median = sample_median(candidate for _, candidate in normalized)
    differences = tuple(candidate - base for base, candidate in normalized)
    difference = sample_median(differences)
    relative = (
        None
        if any(base == 0.0 for base, _ in normalized)
        else sample_median((candidate - base) / base for base, candidate in normalized)
    )
    rng = random.Random(seed)
    bootstrapped: list[float] = []
    for _ in range(samples):
        resample = tuple(rng.choice(normalized) for _ in normalized)
        bootstrapped.append(
            sample_median(candidate - base for base, candidate in resample)
        )
    tail = (1.0 - confidence) / 2.0
    return {
        "paired_blocks": len(normalized),
        "baseline_median": baseline_median,
        "candidate_median": candidate_median,
        "median_difference": difference,
        "relative_change": relative,
        "bootstrap_interval": {
            "low": nearest_rank_percentile(bootstrapped, tail),
            "high": nearest_rank_percentile(bootstrapped, 1.0 - tail),
        },
    }


__all__ = [
    "Interval",
    "RiskDifferenceInterval",
    "nearest_rank_percentile",
    "newcombe_risk_difference_interval",
    "paired_median_change",
    "sample_median",
    "summarize_measurements",
    "wilson_interval",
]
