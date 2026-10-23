"""The pre-registered service-continuity scenario (harness.svc)."""

from __future__ import annotations

import json
import math
from pathlib import Path

from harness import deploy, svc
from harness import spec as S

WORKLOAD = Path(__file__).resolve().parents[1] / "workloads" / "svc.json"


def test_the_committed_workload_is_the_generators_output():
    """Pre-registered: the workload is exactly what the fixed parameters give."""
    assert S.to_dict(S.from_dict(svc.document())) == json.loads(WORKLOAD.read_text())


def test_the_scenario_has_its_roles_and_runtime_classes():
    spec = S.load(WORKLOAD)
    assert spec.shared_memory_agents == ("assistant",)
    requests = spec.service["requests"]
    assert requests and all(spec.task(r).agent == "assistant" for r in requests)
    # Every request replies (unprotected); some also command the lamp.
    replies = {e.issuer for e in spec.effects if e.cls == "reply"}
    assert replies == set(requests)
    assert {c.id: c.protected for c in spec.effect_classes} == {
        "reply": False,
        "dev_lamp": True,
        "dev_auto_1": True,
        "dev_auto_2": True,
    }
    records = deploy.agent_records(spec)
    [assistant] = records["core"]
    assert {r["effect_class"] for r in assistant["effects"]} == {
        "device.command",
        "network.http",
    }
    config = deploy.monitor_config(
        spec, strategy="firebreak", control_latency_ms={"core": 4.0}
    )
    assert config["protected_effect_classes"] == ["device.command"]
    assert config["shared_memory_agents"] == ["assistant"]
    # External requests of every agent, submitted on its node at its release.
    assert set(deploy.external_recipients(spec)) == {"core", "edge-1", "edge-2"}


def test_a_stopped_service_costs_its_expected_future_requests():
    spec = S.load(WORKLOAD)
    early = svc.monitor_settings(spec, "early", 1.0)
    late = svc.monitor_settings(spec, "late", 1.0)
    assert early["boundary"] == late["boundary"] == "defer"
    # rate x (end of requests - nominal alert time)
    # The early alert is the schedule's produce event itself (P.sched ends at
    # 20.5 s; amended 2026-10-21 from 0.3 s after it).
    assert math.isclose(early["future_requests"]["assistant"], 0.5 * (85.0 - 20.5))
    assert late["future_requests"]["assistant"] < early["future_requests"]["assistant"]
    assert svc.monitor_settings(spec, "early", None).keys() == {
        "boundary",
        "future_requests",
    }
    other = S.from_dict(json.loads((WORKLOAD.parent / "c2.json").read_text()))
    assert svc.monitor_settings(other, "late", 1.0) == {}


LABEL = ["fault:wrong_value:sched@v1"]


def effect(effect_id, issuer, cls, phase, t, labels=()):
    return {
        "effect_id": effect_id,
        "issuer": issuer,
        "class": cls,
        "declared": True,
        "origin": "declared",
        "labels": list(labels),
        "value": "v",
        "phase": phase,
        "t_ms": t,
    }


def invocation(task, rt, t, labels=()):
    """The runtime events of one invocation of ``task``; ``labels`` at its
    compute start make it (and its commits, by lineage) tainted."""
    return [
        {"kind": kind, "task": task, "rt": rt, "inv_kind": "task", "t_ms": t + dt}
        | ({"labels": list(labels)} if kind == "compute_start" and labels else {})
        for kind, dt in (("start", 0), ("compute_start", 10))
    ]


def test_service_metrics_count_from_the_alert_on():
    spec = S.load(WORKLOAD)
    first, second, third = spec.service["requests"][:3]

    ledger = [
        effect(f"{first}.reply", first, "reply", "committed", 900),  # before
        effect(f"{second}.reply", second, "reply", "committed", 2600, LABEL),
        effect(f"{second}.lamp", second, "dev_lamp", "committed", 2700, LABEL),
        effect(f"{third}.reply", third, "reply", "held", 3100),
        effect("M05.cmd", "M05", "dev_auto_1", "committed", 3200),
    ]
    marks = [
        {"name": "submitted", "task": first, "t_ms": 500},
        {"name": "submitted", "task": second, "t_ms": 2100},
        {"name": "submitted", "task": third, "t_ms": 3000},
        {"name": "submitted", "task": "M05", "t_ms": 3150},
        {"name": "refused", "task": spec.service["requests"][3], "t_ms": 3300},
    ]
    # The second request's invocation computed on the corrupted schedule (the
    # assistant's shared memory): its commits are tainted by lineage.
    events = invocation(second, "rt-2", 2190, LABEL)
    row = svc.metrics(spec, {"t_alert_ms": 2000}, ledger, marks, events)
    assert (row["svc_requests"], row["svc_answered"], row["svc_refused"]) == (3, 1, 1)
    assert row["svc_tainted_replies"] == row["svc_tainted_device_commands"] == 1
    assert (row["svc_automation_committed"], row["svc_automation_due"]) == (1, 1)
    assert json.loads(row["svc_latency_by_request"]) == {second: 0.5}
    rows = [
        {
            "strategy_code": "none",
            "rep": 0,
            "svc_latency_by_request": json.dumps({second: 0.25}),
        },
        {"strategy_code": "firebreak", "rep": 0, **row},
    ]
    svc.added_latency(rows, ["rep"])
    assert rows[1]["svc_added_latency_s"] == 0.25
    assert rows[1]["svc_added_latency_n"] == 1


def test_a_tainted_reply_answers_a_request_but_not_correctly():
    spec = S.load(WORKLOAD)
    before, clean, tainted, unanswered = spec.service["requests"][:4]
    ledger = [
        # Asked before the alert, tainted reply after it: a tainted reply, but
        # not a request of the window.
        effect(f"{before}.reply", before, "reply", "committed", 2300, LABEL),
        effect(f"{clean}.reply", clean, "reply", "committed", 2600),
        effect(f"{tainted}.reply", tainted, "reply", "committed", 2900, LABEL),
    ]
    marks = [
        {"name": "submitted", "task": before, "t_ms": 1800},
        {"name": "submitted", "task": clean, "t_ms": 2100},
        {"name": "submitted", "task": tainted, "t_ms": 2400},
        {"name": "submitted", "task": unanswered, "t_ms": 3000},
    ]
    events = [
        *invocation(before, "rt-0", 1810, LABEL),
        *invocation(clean, "rt-1", 2110),
        *invocation(tainted, "rt-2", 2410, LABEL),
    ]
    row = svc.metrics(spec, {"t_alert_ms": 2000}, ledger, marks, events)
    # The registered columns count the tainted reply as an answer.
    assert (row["svc_requests"], row["svc_answered"]) == (3, 2)
    assert row["svc_answered_frac"] == 2 / 3
    assert row["svc_tainted_replies"] == 2
    # Only the clean reply is a correct answer; the wrong answers are the
    # window's own tainted replies.
    assert row["svc_answered_correct"] == 1
    assert row["svc_answered_correct_frac"] == 1 / 3
    assert row["svc_wrong_answers"] == 1
    empty = svc.metrics(spec, {"t_alert_ms": 10**9}, ledger, marks, events)
    assert (empty["svc_answered_correct"], empty["svc_wrong_answers"]) == (0, 0)
    assert math.isnan(empty["svc_answered_correct_frac"])


def test_the_svc_grids_have_their_planned_sizes():
    from harness import runner

    root = WORKLOAD.parents[1] / "experiments"
    sizes = {
        name: len(runner.grid(runner.Experiment.load(root / f"{name}.json")))
        for name in ("svc-smoke", "svc-pilot")
    }
    # 11 strategies (none included) x 2 alerts, plus the no-fault reference.
    assert sizes == {"svc-smoke": 23, "svc-pilot": 3 * 23}


def test_the_early_alert_is_the_produce_event_posted_on_receipt():
    spec = S.load(WORKLOAD)
    early, late = spec.alerts["early"], spec.alerts["late"]
    assert (early.event, early.item, early.delay_s) == ("produce", "sched@v1", 0.0)
    assert early.posted_on_receipt
    # The late alert is unchanged.
    assert (late.event, late.item, late.count, late.delay_s) == (
        "consume",
        "sched@v1",
        1,
        15.0,
    )
    assert not late.posted_on_receipt
