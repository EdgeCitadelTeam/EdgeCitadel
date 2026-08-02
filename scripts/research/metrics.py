"""Monotonic fixed-window resource accounting for research repetitions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from time import perf_counter_ns, process_time, sleep
from typing import Protocol, cast

_INTERVAL_NS = 100_000_000
_IDLE_DURATION_NS = 2_000_000_000
_MAX_SAMPLER_CPU_PER_WALL_SECOND = 0.02
_RESOURCE_METRICS = (
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
class ComponentCounters:
    cpu_seconds: float
    rss_bytes: int
    rx_bytes: int
    tx_bytes: int
    application_bytes: int
    nats_connection_bytes: int
    http_bytes: int
    storage_bytes: int
    message_count: int


@dataclass(frozen=True)
class ResourceWindow:
    components: tuple[str, ...]
    metric_coverage: tuple[str, ...]
    start_monotonic_ns: int
    end_monotonic_ns: int
    sample_monotonic_ns: tuple[int, ...]
    component_samples: tuple[Mapping[str, ComponentCounters], ...]
    outcome: str
    cpu_seconds: float
    peak_rss_bytes: int
    rss_seconds: float
    rx_bytes: int
    tx_bytes: int
    application_bytes: int
    nats_connection_bytes: int
    http_bytes: int
    storage_bytes: int
    message_count_delta: int
    sampler_cpu_seconds: float
    cost_claims_valid: bool


@dataclass(frozen=True)
class _ComputedTotals:
    cpu_seconds: float
    peak_rss_bytes: int
    rss_seconds: float
    rx_bytes: int
    tx_bytes: int
    application_bytes: int
    nats_connection_bytes: int
    http_bytes: int
    storage_bytes: int
    message_count_delta: int


class _Clock(Protocol):
    def monotonic_ns(self) -> int: ...

    def process_cpu_seconds(self) -> float: ...

    def sleep_ns(self, duration_ns: int) -> None: ...


class _CounterReader(Protocol):
    def read(self, components: tuple[str, ...]) -> Mapping[str, ComponentCounters]: ...


class SystemClock:
    """Host monotonic clock used only by the resource sampler."""

    def monotonic_ns(self) -> int:
        return perf_counter_ns()

    def process_cpu_seconds(self) -> float:
        return process_time()

    def sleep_ns(self, duration_ns: int) -> None:
        sleep(duration_ns / 1_000_000_000)


class _ActiveResourceWindow:
    def __init__(
        self,
        sampler: ResourceSampler,
        components: tuple[str, ...],
        start_ns: int,
        sampler_cpu_start: float,
        initial_snapshot: dict[str, ComponentCounters],
    ) -> None:
        self._sampler = sampler
        self._components = components
        self._start_ns = start_ns
        self._sampler_cpu_start = sampler_cpu_start
        self._snapshots = [initial_snapshot]
        self._sample_times = [start_ns]
        self._finished = False

    def sample_due(self) -> bool:
        """Record one snapshot once the declared 100ms interval has elapsed."""
        if self._finished:
            raise ValueError("resource window is already finished")
        now = self._sampler._clock.monotonic_ns()
        if now - self._sample_times[-1] < _INTERVAL_NS:
            return False
        self._append_snapshot(now)
        return True

    def finish(self, *, outcome: str) -> ResourceWindow:
        """Close a variable-duration window with a final terminal snapshot."""
        if self._finished:
            raise ValueError("resource window is already finished")
        self._finished = True
        end_ns = self._sampler._clock.monotonic_ns()
        if end_ns < self._start_ns:
            raise ValueError("monotonic clock moved backwards")
        if end_ns > self._sample_times[-1]:
            self._append_snapshot(end_ns)
        return self._sampler._resource_window(
            self._components,
            self._start_ns,
            end_ns,
            self._sample_times,
            self._snapshots,
            outcome,
            self._sampler._clock.process_cpu_seconds() - self._sampler_cpu_start,
        )

    def _append_snapshot(self, sample_time: int) -> None:
        snapshot = _validate_snapshot(
            self._components, self._sampler._reader.read(self._components)
        )
        _validate_counter_progress(self._snapshots[-1], snapshot)
        self._sample_times.append(sample_time)
        self._snapshots.append(snapshot)


def _validate_snapshot(
    components: tuple[str, ...],
    snapshot: Mapping[str, ComponentCounters],
) -> dict[str, ComponentCounters]:
    if tuple(snapshot) != components:
        raise ValueError("component membership changed during sampling")
    values = dict(snapshot)
    for value in values.values():
        if value.cpu_seconds < 0 or any(
            counter < 0
            for counter in (
                value.rss_bytes,
                value.rx_bytes,
                value.tx_bytes,
                value.application_bytes,
                value.nats_connection_bytes,
                value.http_bytes,
                value.storage_bytes,
                value.message_count,
            )
        ):
            raise ValueError("counter values must be nonnegative")
    return values


def _validate_counter_progress(
    before: Mapping[str, ComponentCounters],
    after: Mapping[str, ComponentCounters],
) -> None:
    cumulative_fields = (
        "cpu_seconds",
        "rx_bytes",
        "tx_bytes",
        "application_bytes",
        "nats_connection_bytes",
        "http_bytes",
        "storage_bytes",
        "message_count",
    )
    if any(
        getattr(after[component], field) < getattr(before[component], field)
        for component in before
        for field in cumulative_fields
    ):
        raise ValueError("counters regressed during sampling")


class ResourceSampler:
    """Collect component-stable counter snapshots at the declared interval."""

    def __init__(
        self,
        reader: _CounterReader,
        clock: _Clock,
        *,
        metric_coverage: tuple[str, ...] = _RESOURCE_METRICS,
    ) -> None:
        if (
            len(set(metric_coverage)) != len(metric_coverage)
            or any(metric not in _RESOURCE_METRICS for metric in metric_coverage)
        ):
            raise ValueError("invalid resource metric coverage")
        self._reader = reader
        self._clock = clock
        self._metric_coverage = metric_coverage

    def idle_baseline(self, components: tuple[str, ...]) -> ResourceWindow:
        return self.sample_window(
            components, duration_ns=_IDLE_DURATION_NS, outcome="idle"
        )

    def start_active_window(
        self, components: tuple[str, ...]
    ) -> _ActiveResourceWindow:
        if not components or len(set(components)) != len(components):
            raise ValueError("components must be nonempty and unique")
        start_ns = self._clock.monotonic_ns()
        sampler_cpu_start = self._clock.process_cpu_seconds()
        initial_snapshot = _validate_snapshot(components, self._reader.read(components))
        return _ActiveResourceWindow(
            self,
            components,
            start_ns,
            sampler_cpu_start,
            initial_snapshot,
        )

    def sample_window(
        self,
        components: tuple[str, ...],
        *,
        duration_ns: int,
        outcome: str = "completed",
    ) -> ResourceWindow:
        if duration_ns <= 0 or duration_ns % _INTERVAL_NS:
            raise ValueError("duration must be a positive multiple of 100ms")
        session = self.start_active_window(components)
        while self._clock.monotonic_ns() < session._start_ns + duration_ns:
            self._clock.sleep_ns(_INTERVAL_NS)
            session.sample_due()
        return session.finish(outcome=outcome)

    def _resource_window(
        self,
        components: tuple[str, ...],
        start_ns: int,
        end_ns: int,
        sample_times: list[int],
        snapshots: list[dict[str, ComponentCounters]],
        outcome: str,
        sampler_cpu_seconds: float,
    ) -> ResourceWindow:
        totals = self._totals(snapshots, sample_times)
        wall_seconds = (end_ns - start_ns) / 1_000_000_000
        return ResourceWindow(
            components=components,
            metric_coverage=self._metric_coverage,
            start_monotonic_ns=start_ns,
            end_monotonic_ns=end_ns,
            sample_monotonic_ns=tuple(sample_times),
            component_samples=tuple(dict(snapshot) for snapshot in snapshots),
            outcome=outcome,
            sampler_cpu_seconds=sampler_cpu_seconds,
            cost_claims_valid=(
                wall_seconds > 0
                and set(self._metric_coverage) == set(_RESOURCE_METRICS)
                and sampler_cpu_seconds / wall_seconds
                <= _MAX_SAMPLER_CPU_PER_WALL_SECOND
            ),
            cpu_seconds=totals.cpu_seconds,
            peak_rss_bytes=totals.peak_rss_bytes,
            rss_seconds=totals.rss_seconds,
            rx_bytes=totals.rx_bytes,
            tx_bytes=totals.tx_bytes,
            application_bytes=totals.application_bytes,
            nats_connection_bytes=totals.nats_connection_bytes,
            http_bytes=totals.http_bytes,
            storage_bytes=totals.storage_bytes,
            message_count_delta=totals.message_count_delta,
        )

    @staticmethod
    def _totals(
        snapshots: list[dict[str, ComponentCounters]],
        sample_times: list[int],
    ) -> _ComputedTotals:
        first, last = snapshots[0], snapshots[-1]
        peak_rss = max(
            sum(value.rss_bytes for value in sample.values()) for sample in snapshots
        )
        rss_seconds = sum(
            (
                sum(value.rss_bytes for value in before.values())
                + sum(value.rss_bytes for value in after.values())
            )
            / 2
            * (after_time - before_time)
            / 1_000_000_000
            for before, after, before_time, after_time in zip(
                snapshots,
                snapshots[1:],
                sample_times,
                sample_times[1:],
            )
        )

        def delta(name: str) -> float | int:
            return sum(
                cast(float | int, getattr(last[component], name))
                - cast(float | int, getattr(first[component], name))
                for component in first
            )

        return _ComputedTotals(
            cpu_seconds=float(delta("cpu_seconds")),
            peak_rss_bytes=peak_rss,
            rss_seconds=rss_seconds,
            rx_bytes=int(delta("rx_bytes")),
            tx_bytes=int(delta("tx_bytes")),
            application_bytes=int(delta("application_bytes")),
            nats_connection_bytes=int(delta("nats_connection_bytes")),
            http_bytes=int(delta("http_bytes")),
            storage_bytes=int(delta("storage_bytes")),
            message_count_delta=int(delta("message_count")),
        )


__all__ = ["ComponentCounters", "ResourceSampler", "ResourceWindow", "SystemClock"]
