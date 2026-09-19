from copy import deepcopy

import pytest

from e2e.helpers.trace_baseline_audit import audit_cohort
from e2e.helpers.trace_latency_workload import (
    baseline_slot,
    baseline_workload,
    summarize_latencies,
)


@pytest.fixture
def evidence():
    declared = baseline_workload(preflight=True)
    scope = {
        "node_id": "node",
        "source_epoch": "epoch",
        "agent_ids": [f"agent-{i}" for i in range(10)],
    }
    events, records, expected, eligible = [], [], [], []
    positions, acks = {}, {}
    for index in range(declared["expected_events"]):
        cycle, actor, step = baseline_slot(index)
        identity = f"event-{index}"
        event = {
            "node_id": "node",
            "source_epoch": "epoch",
            "source_seq": 100 + index * 2,
            "event_id": identity,
            "agent_id": scope["agent_ids"][actor],
            "trace_id": f"run-{cycle}-{actor}",
            "kind": "run" if step in {0, 49} else "tool",
            "phase": "completed"
            if step == 49
            else "started"
            if step == 0 or step % 2
            else "finished",
            "span_id": f"span-{cycle}-{actor}-{(step - 1) // 2}"
            if step not in {0, 49}
            else None,
        }
        events.append(event)
        before = 1_000_000_000 + index * 120_000_000
        record = {
            key: event[key]
            for key in ("node_id", "source_epoch", "event_id", "agent_id", "trace_id")
        }
        record.update(
            collector_epoch="collector",
            ingest_seq=index + 1,
            before_ns=before,
            after_ns=before + 2_000_000,
        )
        records.append(record)
        positions[identity] = ("collector", index + 1)
        if event["kind"] == "tool" and event["phase"] == "finished" and actor in {0, 1}:
            expected.append(identity)
            acks[identity] = before + (500 + index) * 1_000_000
            if cycle >= declared["warmup_cycles"]:
                eligible.append(identity)
    commits = {"valid": True, "failure": None, "records": records}
    render = {
        "valid": True,
        "failure": None,
        "expected": expected,
        "eligible": eligible,
        "acks": acks,
    }
    browser = {
        "samples": 96,
        "lanes": [{"samples": 48}, {"samples": 48}],
        "page_errors": [],
        "execution_writes": 0,
    }
    markers = {r["event_id"]: r for r in records}
    values = [(acks[k] - markers[k]["before_ns"]) / 1_000_000 for k in eligible]
    claimed = {
        "samples": 48,
        "warmup_and_measured_samples": 96,
        "latency_upper_bounds_ms": values,
        "commit_bracket_max_ms": 2.0,
        "emission": {
            "events": 1000,
            "seconds": 120,
            "waited_for_render_during_emission": False,
        },
        **summarize_latencies(values, 48),
    }
    return declared, scope, events, positions, commits, render, browser, claimed


def test_reconstructs_cohort_from_source_with_unrelated_source_sequence_gaps(evidence):
    report = audit_cohort(*evidence)
    assert report["source_events"] == 1000 and report["runs"] == 20
    assert report["tool_pairs"] == 480
    assert report["warmup_samples"] == report["measured_samples"] == 48
    assert report["p95_numerical_target_met"] is None


@pytest.mark.parametrize(
    "mutation",
    [
        "source_missing",
        "actor_changed",
        "pair_changed",
        "phase_changed",
        "commit_duplicate",
        "commit_position",
        "warmup_substituted",
        "ack_missing",
        "ack_before_commit",
        "lane_population",
        "timing_changed",
        "invented_p95",
        "short_duration",
        "denominator_reordered",
    ],
)
def test_rejects_incomplete_or_substituted_evidence_even_when_counts_match(
    evidence, mutation
):
    values = deepcopy(evidence)
    declared, scope, events, positions, commits, render, browser, claimed = values
    if mutation == "source_missing":
        events.pop()
    elif mutation == "actor_changed":
        events[0]["agent_id"] = scope["agent_ids"][1]
    elif mutation == "pair_changed":
        events[20]["span_id"] = "unrelated-span"
    elif mutation == "phase_changed":
        events[10]["phase"] = "finished"
    elif mutation == "commit_duplicate":
        commits["records"][-1] = commits["records"][0]
    elif mutation == "commit_position":
        commits["records"][0]["ingest_seq"] += 1
    elif mutation == "warmup_substituted":
        render["eligible"][0] = render["expected"][0]
    elif mutation == "ack_missing":
        render["acks"].pop(render["expected"][0])
    elif mutation == "ack_before_commit":
        render["acks"][render["expected"][0]] = 0
    elif mutation == "lane_population":
        browser["lanes"] = [{"samples": 47}, {"samples": 49}]
    elif mutation == "timing_changed":
        claimed["latency_upper_bounds_ms"][0] += 1
    elif mutation == "invented_p95":
        claimed["p95_upper_bound_ms"] = 123
    elif mutation == "short_duration":
        claimed["emission"]["seconds"] = declared["duration_s"] - 1
    elif mutation == "denominator_reordered":
        render["expected"][0], render["expected"][1] = (
            render["expected"][1],
            render["expected"][0],
        )
    with pytest.raises(ValueError):
        audit_cohort(*values)
