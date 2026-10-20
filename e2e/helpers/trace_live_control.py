"""Shared observations for matched live commit-observer controls on jim-eq."""

import os
import time
from pathlib import Path

if __package__:
    from .trace_latency_workload import summarize_latencies
else:
    from trace_latency_workload import summarize_latencies


def source_display_summary(declared, render, starts):
    expected, eligible, acks = render["expected"], render["eligible"], render["acks"]
    if (
        render["valid"] is not True
        or render["failure"] is not None
        or len(expected) != declared["samples"]
        or len(set(expected)) != len(expected)
        or len(eligible) != declared["measured_samples"]
        or len(set(eligible)) != len(eligible)
        or not set(eligible) <= set(expected)
        or set(starts) != set(expected)
        or set(acks) != set(expected)
    ):
        raise ValueError("incomplete source/display cohort")
    for identity in expected:
        if (
            type(starts[identity]) is not int
            or type(acks[identity]) is not int
            or not 0 <= starts[identity] <= acks[identity]
        ):
            raise ValueError("invalid source/display clock order")
    values = [(acks[identity] - starts[identity]) / 1_000_000 for identity in eligible]
    return dict(
        boundary="before_source_append_to_host_render_ack",
        values_ms=values,
        measured_samples=len(eligible),
        all_samples=len(expected),
        **summarize_latencies(values, len(eligible)),
    )


def process_cpu(pid, *, proc_root=Path("/proc")):
    # comm may contain spaces and parentheses; fields after the last ')' are fixed.
    fields = (proc_root / str(pid) / "stat").read_text().rpartition(")")[2].split()
    return dict(
        pid=pid,
        start_ticks=int(fields[19]),
        cpu_ticks=int(fields[11]) + int(fields[12]),
        ticks_per_second=os.sysconf("SC_CLK_TCK"),
        monotonic_ns=time.monotonic_ns(),
    )


def cpu_summary(before, after):
    for snapshot in (before, after):
        if (
            any(
                type(snapshot[key]) is not int or snapshot[key] < 0
                for key in (
                    "pid",
                    "start_ticks",
                    "ticks_per_second",
                    "cpu_ticks",
                    "monotonic_ns",
                )
            )
            or snapshot["pid"] == 0
        ):
            raise ValueError("invalid Core CPU counters")
    if any(
        before[key] != after[key] for key in ("pid", "start_ticks", "ticks_per_second")
    ):
        raise ValueError("Core process changed during measurement")
    if (
        before["ticks_per_second"] <= 0
        or before["cpu_ticks"] < 0
        or after["cpu_ticks"] < before["cpu_ticks"]
        or after["monotonic_ns"] <= before["monotonic_ns"]
    ):
        raise ValueError("invalid Core CPU counters")
    return dict(
        cpu_seconds=(after["cpu_ticks"] - before["cpu_ticks"])
        / before["ticks_per_second"],
        window_seconds=(after["monotonic_ns"] - before["monotonic_ns"]) / 1_000_000_000,
        ticks_per_second=before["ticks_per_second"],
        scope="Core process including collector, projection and reads; may include unrelated fleet work",
    )
