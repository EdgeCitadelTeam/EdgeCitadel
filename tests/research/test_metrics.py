"""Contracts for fixed-window comparable resource accounting."""

from __future__ import annotations

from collections import deque

import pytest

from scripts.research.metrics import ComponentCounters, ResourceSampler


class _Clock:
    def __init__(self, *, sampler_cpu_per_sleep: float = 0.0) -> None:
        self.now = 0
        self.cpu = 0.0
        self.sampler_cpu_per_sleep = sampler_cpu_per_sleep

    def monotonic_ns(self) -> int:
        return self.now

    def process_cpu_seconds(self) -> float:
        return self.cpu

    def sleep_ns(self, duration_ns: int) -> None:
        self.now += duration_ns
        self.cpu += self.sampler_cpu_per_sleep


class _Reader:
    def __init__(self, snapshots: list[dict[str, ComponentCounters]]) -> None:
        self.snapshots = deque(snapshots)

    def read(self, components: tuple[str, ...]) -> dict[str, ComponentCounters]:
        return self.snapshots.popleft()


def _counter(
    *,
    cpu: float = 0.0,
    rss: int = 100,
    rx: int = 0,
    tx: int = 0,
    application: int = 0,
    nats: int = 0,
    http: int = 0,
    storage: int = 0,
    messages: int = 0,
) -> ComponentCounters:
    return ComponentCounters(
        cpu, rss, rx, tx, application, nats, http, storage, messages
    )


def test_active_window_samples_every_100ms_and_integrates_resource_deltas() -> None:
    components = ("controller", "broker")
    start = {
        "controller": _counter(cpu=1.0, rss=100, rx=10, tx=20),
        "broker": _counter(rss=50, application=10, nats=100, storage=1000, messages=3),
    }
    middle = {
        "controller": _counter(cpu=1.1, rss=200, rx=30, tx=50),
        "broker": _counter(rss=100, application=20, nats=130, storage=1010, messages=4),
    }
    end = {
        "controller": _counter(cpu=1.3, rss=300, rx=40, tx=70),
        "broker": _counter(rss=150, application=35, nats=160, storage=1025, messages=5),
    }
    sampler = ResourceSampler(_Reader([start, middle, end]), _Clock())

    window = sampler.sample_window(components, duration_ns=200_000_000)

    assert window.sample_monotonic_ns == (0, 100_000_000, 200_000_000)
    assert window.cpu_seconds == pytest.approx(0.3)
    assert window.peak_rss_bytes == 450
    assert window.rss_seconds == pytest.approx(60.0)
    assert window.rx_bytes == 30
    assert window.tx_bytes == 50
    assert window.application_bytes == 25
    assert window.nats_connection_bytes == 60
    assert window.storage_bytes == 25
    assert window.message_count_delta == 2


def test_idle_calibration_is_exactly_two_seconds_with_the_same_sampling_path() -> None:
    components = ("controller",)
    snapshots = [{"controller": _counter(rss=100)} for _ in range(21)]
    sampler = ResourceSampler(_Reader(snapshots), _Clock())

    window = sampler.idle_baseline(components)

    assert window.end_monotonic_ns - window.start_monotonic_ns == 2_000_000_000
    assert len(window.sample_monotonic_ns) == 21


def test_active_session_finishes_at_an_arbitrary_terminal_observation() -> None:
    components = ("controller",)
    sampler = ResourceSampler(
        _Reader(
            [
                {"controller": _counter(cpu=1.0, rss=100, rx=10)},
                {"controller": _counter(cpu=1.1, rss=200, rx=20)},
                {"controller": _counter(cpu=1.2, rss=300, rx=40)},
            ]
        ),
        clock := _Clock(),
    )

    session = sampler.start_active_window(components)
    clock.sleep_ns(100_000_000)
    session.sample_due()
    clock.sleep_ns(30_000_000)
    window = session.finish(outcome="completed")

    assert window.start_monotonic_ns == 0
    assert window.end_monotonic_ns == 130_000_000
    assert window.sample_monotonic_ns == (0, 100_000_000, 130_000_000)
    assert [sample["controller"].rss_bytes for sample in window.component_samples] == [
        100,
        200,
        300,
    ]
    assert window.cpu_seconds == pytest.approx(0.2)
    assert window.peak_rss_bytes == 300
    assert window.rx_bytes == 30


def test_component_membership_mismatch_invalidates_comparison() -> None:
    clock = _Clock()
    sampler = ResourceSampler(
        _Reader([{"controller": _counter()}, {"controller": _counter()}]),
        clock,
    )

    with pytest.raises(ValueError, match="component membership"):
        sampler.sample_window(("controller", "worker"), duration_ns=100_000_000)


def test_counter_regression_invalidates_a_resource_window() -> None:
    sampler = ResourceSampler(
        _Reader(
            [
                {"controller": _counter(cpu=1.0, rx=10)},
                {"controller": _counter(cpu=0.9, rx=9)},
            ]
        ),
        _Clock(),
    )

    with pytest.raises(ValueError, match="counters regressed"):
        sampler.sample_window(("controller",), duration_ns=100_000_000)


def test_partial_measurement_coverage_never_validates_cost_claims() -> None:
    sampler = ResourceSampler(
        _Reader(
            [
                {"controller": _counter(cpu=1.0)},
                {"controller": _counter(cpu=1.1)},
            ]
        ),
        _Clock(),
        metric_coverage=("cpu_seconds", "peak_rss_bytes"),
    )

    window = sampler.sample_window(("controller",), duration_ns=100_000_000)

    assert window.metric_coverage == ("cpu_seconds", "peak_rss_bytes")
    assert window.cost_claims_valid is False


def test_failed_window_retains_costs_and_sampler_overhead_invalidates_claims() -> None:
    components = ("controller",)
    snapshots = [
        {"controller": _counter(cpu=1.0, rss=100)},
        {"controller": _counter(cpu=1.2, rss=100)},
        {"controller": _counter(cpu=1.4, rss=100)},
    ]
    sampler = ResourceSampler(
        _Reader(snapshots),
        _Clock(sampler_cpu_per_sleep=0.003),
    )

    window = sampler.sample_window(
        components,
        duration_ns=200_000_000,
        outcome="timeout",
    )

    assert window.outcome == "timeout"
    assert window.cpu_seconds == pytest.approx(0.4)
    assert window.sampler_cpu_seconds == pytest.approx(0.006)
    assert window.cost_claims_valid is False
