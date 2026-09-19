import math

import pytest

from e2e.helpers.trace_latency_workload import (
    baseline_slot,
    baseline_workload,
    summarize_latencies,
    workload,
)


def test_open_loop_declares_terminal_cohort_and_all_emitted_events():
    declared = workload(1000)
    assert declared["mode"] == "open_loop_terminal"
    assert declared["samples"] == declared["operations"] == 1000
    assert declared["expected_events"] == 2002
    assert declared["eligible_phases"] == ["finished"]
    assert declared["browser_timeout_s"] == 420
    assert workload()["eligible_phases"] == ["started", "finished"]
    assert workload()["expected_events"] == 12


@pytest.mark.parametrize(
    "count,rate", [(0, 1), (1501, 1), (True, 1), (1, math.nan), (1, math.inf), (1, 0)]
)
def test_invalid_workloads_fail_before_emission(count, rate):
    with pytest.raises(ValueError):
        workload(count, rate)


def test_p95_requires_complete_cohort_and_uses_nearest_rank():
    values = list(range(1000))
    report = summarize_latencies(values[::-1], 1000)
    assert report["p95_upper_bound_ms"] == 949
    assert report["p95_sample_sufficient"]
    assert summarize_latencies(values[:999], 999)["p95_upper_bound_ms"] is None
    with pytest.raises(ValueError, match="incomplete"):
        summarize_latencies(values[:999], 1000)
    with pytest.raises(ValueError, match="incomplete"):
        summarize_latencies(values + [1000], 1000)


@pytest.mark.parametrize("values", [[], [-1], [math.nan], [math.inf]])
def test_invalid_latency_data_cannot_produce_a_statistic(values):
    with pytest.raises(ValueError):
        summarize_latencies(values, len(values))


def test_baseline_schedule_has_ten_agents_fifty_events_and_disjoint_warmup():
    declared = baseline_workload()
    assert declared["duration_s"] == 5 * 60 + 30 * 60
    assert declared["expected_events"] == 17500
    assert declared["samples"] == 1680 and declared["measured_samples"] == 1440
    observed = {}
    for index in range(declared["expected_events"]):
        cycle, agent, step = baseline_slot(index)
        observed.setdefault((cycle, agent), []).append(step)
    assert len(observed) == declared["expected_runs"] == 350
    assert all(steps == list(range(50)) for steps in observed.values())
    assert baseline_slot(499) == (0, 9, 49)
    assert baseline_slot(500) == (1, 0, 0)
    preflight = baseline_workload(preflight=True)
    assert preflight["expected_events"] == 1000 and preflight["samples"] == 96
    assert preflight["measured_samples"] == 48
