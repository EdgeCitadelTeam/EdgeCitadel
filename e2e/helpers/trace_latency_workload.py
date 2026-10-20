"""Declared diagnostic workload and complete-cohort latency statistics."""

import math
import statistics


def workload(open_loop_samples=None, event_rate=25 / 3):
    if not math.isfinite(event_rate) or not 0.1 <= event_rate <= 100:
        raise ValueError("event rate must be finite and between 0.1 and 100")
    if open_loop_samples is not None and (
        type(open_loop_samples) is not int or not 1 <= open_loop_samples <= 1500
    ):
        raise ValueError("open-loop sample count must be between 1 and 1500")
    operations = open_loop_samples if open_loop_samples is not None else 5
    return {
        "mode": "open_loop_terminal"
        if open_loop_samples is not None
        else "closed_loop",
        "operations": operations,
        "samples": operations if open_loop_samples is not None else operations * 2,
        "event_rate": event_rate if open_loop_samples is not None else None,
        "expected_events": operations * 2 + 2,
        "eligible_phases": ["finished"]
        if open_loop_samples is not None
        else ["started", "finished"],
        "browser_timeout_s": operations * 2 / event_rate + 180
        if open_loop_samples is not None
        else 180,
    }


def summarize_latencies(values, expected):
    if (
        len(values) != expected
        or not values
        or any(not math.isfinite(v) or v < 0 for v in values)
    ):
        raise ValueError("latency cohort is incomplete or invalid")
    ordered = sorted(values)
    return {
        "median_upper_bound_ms": statistics.median(ordered),
        "maximum_upper_bound_ms": ordered[-1],
        "p95_upper_bound_ms": ordered[math.ceil(len(ordered) * 0.95) - 1]
        if expected >= 1000
        else None,
        "p95_sample_sufficient": expected >= 1000,
    }


def baseline_workload(*, preflight=False):
    warmup_cycles, measured_cycles = (1, 1) if preflight else (5, 30)
    cycles = warmup_cycles + measured_cycles
    return {
        "mode": "baseline",
        "profile": "preflight" if preflight else "full",
        "agents": 10,
        "sampled_agents": [0, 1],
        "events_per_run": 50,
        "operations_per_run": 24,
        "cycles": cycles,
        "warmup_cycles": warmup_cycles,
        "measured_cycles": measured_cycles,
        "duration_s": cycles * 60,
        "event_rate": 25 / 3,
        "expected_events": cycles * 500,
        "expected_runs": cycles * 10,
        "samples": cycles * 48,
        "measured_samples": measured_cycles * 48,
        "eligible_phases": ["finished"],
        "browser_timeout_s": cycles * 60 + 180,
    }


def baseline_slot(index):
    """Uniform 120 ms slots: ten agents, fifty events per run, one run/minute."""
    cycle, offset = divmod(index, 500)
    step, agent = divmod(offset, 10)
    return cycle, agent, step
