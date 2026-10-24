"""The pre-registered service-continuity scenario ``svc``.

Pre-registered (M6, 2026-10-21): the parameters in ``PARAMS``, the generated
workload ``workloads/svc.json``, the metrics in ``metrics`` and the cost
setting (``future_requests`` from the request rate, ``request_value`` in the
experiment) were fixed before any run. Do not tune them after seeing results.

Roles (``service`` in the spec):

- ``assistant`` (site K): a long-lived assistant, one det-worker with a shared
  memory (``shared_memory_agents``): every invocation continues the previous
  ones. It serves a Poisson stream of user requests (external requests the
  harness submits at their times), each answered with a reply to the user
  (class ``reply``, unprotected: a ``network.http`` effect to the run's
  ledger); a declared share are actuation requests that also command the desk
  lamp (class ``dev_lamp``, protected: a ``device.command``).
- ``automation`` (site A): commands other devices on a fixed schedule
  (``dev_auto_1``/``dev_auto_2``, protected), untouched by the fault.
- ``planner`` (site P, the root): hands the assistant tasks (each commands the
  lamp) and, once, a device schedule.

Fault: the schedule artifact is corrupted (``wrong_value`` on ``sched@v1``);
the assistant reads it (``A.sched``) and, its memory being shared, every later
invocation of the assistant is tainted. Alerts: ``early`` just after the
schedule is produced (on the produce event itself, posted by the run's
services on receipt: amended 2026-10-21, see below), ``late`` well after the
assistant took it in.

Metrics, from the alert to the end of the run (``metrics``): tainted device
commands committed; user requests answered; legitimate automation commands
committed; tainted replies delivered; per answered request, the latency from
its submission to its reply, and the added latency against the uncontained
run (strategy ``none``) of the same cell (``report``).

Cost (the same for every strategy): an action that stops the assistant's
service (its credential or its admission revoked) forfeits its expected
future requests, the request rate times the time left after the alert's
nominal time, each worth the experiment's ``request_value``; pausing one of its
invocations or revoking its egress forfeits that request. G_C defers the
horizon (``boundary: defer``): a future invocation of the assistant (an external
request) is handled when it materializes, by the receding re-solve, instead of
being a horizon sink only its admission could stop.

Amendment (2026-10-21, before any lab run of svc): the early alert fired
0.3 s after the controller saw the schedule produced, and the controller
polls the services' feed and resolves the faulty invocation through Core
first. With fast hand-offs the assistant could read the schedule before that
alert, which is the late case. The early alert now fires on the produce
event itself (offset 0), posted by the services when they receive it. Its
registered meaning, an alert before the assistant consumes the schedule,
is unchanged and no longer depends on hand-off speed; ``consumed_at_alert``
checks it in every run (the pipeline's gate reports it). The late alert is
unchanged. ``future_requests`` follows the earlier nominal alert time
(0.15 more expected requests).

Added (2026-10-23, before svc-main is scored; reported columns only): of the
requests from the alert on, those answered with a clean reply
(``svc_answered_correct``, and its share ``svc_answered_correct_frac``) and
those answered with a tainted or unresolved one (``svc_wrong_answers``), so
that a wrong answer is not counted as service. The registered columns are
unchanged.

Generate (deterministic)::

  python -m harness.svc --out workloads/svc.json
"""

from __future__ import annotations

import argparse
import json
import math
import random
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .spec import Spec, from_dict, nominal_schedule, save

PARAMS: dict[str, Any] = {
    "seed": 20261021,
    # User requests: Poisson arrivals in the window; each one computes for
    # request_compute_s and replies; actuation_share of them also command
    # the lamp.
    "request_rate_per_s": 0.5,
    "request_window_s": [5.0, 85.0],
    "actuation_share": 0.3,
    "request_compute_s": 1.0,
    "reply_delay_s": 0.05,
    "command_delay_s": 0.2,
    # Automation: one command every period in the window, alternating devices.
    "automation_period_s": 5.0,
    "automation_window_s": [5.0, 85.0],
    "automation_compute_s": 0.2,
    # Planner: hands the assistant a lamp task at these times; the schedule at
    # schedule_s.
    "planner_hand_s": [10.0, 30.0, 50.0, 70.0],
    "planner_compute_s": 0.5,
    "schedule_s": 20.0,
    # Amended 2026-10-21 (was 0.3): the early alert is the produce event
    # itself, posted on receipt by the run's services.
    "alert_early_delay_s": 0.0,
    "alert_late_delay_s": 15.0,
    # Spare slots of each worker beyond the schedule's own concurrency.
    "spare_slots": 2,
}
# Future invocations of the assistant are external requests: handled when they
# materialize (receding re-solve), not horizon sinks.
MONITOR_BOUNDARY = "defer"


def arrivals(rng: random.Random, rate: float, start: float, end: float) -> list[float]:
    times, t = [], start
    while True:
        t += rng.expovariate(rate)
        if t >= end:
            return times
        times.append(round(t, 3))


def document(params: Mapping[str, Any] = PARAMS) -> dict[str, Any]:
    """The svc workload, deterministic in ``params``."""
    rng = random.Random(params["seed"])
    start, end = params["request_window_s"]
    requests = arrivals(rng, params["request_rate_per_s"], start, end)
    actuation = [rng.random() < params["actuation_share"] for _ in requests]
    tasks: list[dict[str, Any]] = []
    effects: list[dict[str, Any]] = []
    messages: list[dict[str, Any]] = []
    request_ids = []
    for i, (t, acts) in enumerate(zip(requests, actuation, strict=True), 1):
        task_id = f"Q{i:03d}"
        request_ids.append(task_id)
        tasks.append(
            {
                "id": task_id,
                "agent": "assistant",
                "session": f"s{task_id}",
                "compute": params["request_compute_s"],
                "release": t,
                "step_len": params["request_compute_s"],
            }
        )
        effects.append(
            {
                "id": f"{task_id}.reply",
                "issuer": task_id,
                "class": "reply",
                "delay": params["reply_delay_s"],
            }
        )
        if acts:
            effects.append(
                {
                    "id": f"{task_id}.lamp",
                    "issuer": task_id,
                    "class": "dev_lamp",
                    "delay": params["command_delay_s"],
                }
            )
    period = params["automation_period_s"]
    a_start, a_end = params["automation_window_s"]
    for k in range(int(math.floor((a_end - a_start) / period)) + 1):
        task_id = f"M{k + 1:02d}"
        tasks.append(
            {
                "id": task_id,
                "agent": "automation",
                "session": f"s{task_id}",
                "compute": params["automation_compute_s"],
                "release": a_start + k * period,
                "step_len": params["automation_compute_s"],
            }
        )
        effects.append(
            {
                "id": f"{task_id}.cmd",
                "issuer": task_id,
                "class": f"dev_auto_{1 + k % 2}",
                "delay": params["command_delay_s"],
            }
        )
    handed = [
        (f"P{k + 1}", f"A.plan{k + 1}", t)
        for k, t in enumerate(params["planner_hand_s"])
    ]
    handed.append(("P.sched", "A.sched", params["schedule_s"]))
    for planner, assistant, t in handed:
        tasks.append(
            {
                "id": planner,
                "agent": "planner",
                "session": f"s{planner}",
                "compute": params["planner_compute_s"],
                "release": t,
                "step_len": params["planner_compute_s"],
            }
        )
        task: dict[str, Any] = {
            "id": assistant,
            "agent": "assistant",
            "session": f"s{assistant}",
            "compute": params["request_compute_s"],
            "step_len": params["request_compute_s"],
        }
        if planner == "P.sched":
            task["reads"] = ["sched@v1"]
        tasks.append(task)
        messages.append(
            {"id": f"h.{assistant}", "sender": planner, "receiver": assistant}
        )
        effects.append(
            {
                "id": f"{assistant}.lamp",
                "issuer": assistant,
                "class": "dev_lamp",
                "delay": params["command_delay_s"],
            }
        )
    doc: dict[str, Any] = {
        "spec_version": 1,
        "name": "svc",
        "agents": [
            {"id": "planner", "parent": None, "site": "P", "label": "PLANNER"},
            {"id": "assistant", "parent": "planner", "site": "K", "label": "ASSISTANT"},
            {
                "id": "automation",
                "parent": "planner",
                "site": "A",
                "label": "AUTOMATION",
            },
        ],
        "placement": {"K": "core", "A": "edge-1", "P": "edge-2"},
        "effect_classes": [
            {"id": "reply", "site": "K", "min_delay": 0.0, "protected": False},
            {"id": "dev_lamp", "site": "K", "min_delay": params["command_delay_s"]},
            {"id": "dev_auto_1", "site": "A", "min_delay": params["command_delay_s"]},
            {"id": "dev_auto_2", "site": "A", "min_delay": params["command_delay_s"]},
        ],
        "tasks": tasks,
        "external_inputs": [],
        "artifacts": [{"id": "sched@v1", "producer": "P.sched"}],
        "messages": messages,
        "effects": effects,
        "fault": {"class": "wrong_value", "item": "sched@v1"},
        "alerts": {
            "early": {
                "event": "produce",
                "item": "sched@v1",
                "delay_s": params["alert_early_delay_s"],
                "posted_on_receipt": True,
            },
            "late": {
                "event": "consume",
                "item": "sched@v1",
                "count": 1,
                "delay_s": params["alert_late_delay_s"],
            },
        },
        "rogue": {"rate": 0.0, "seed": 0},
        "shared_memory_agents": ["assistant"],
        "service": {
            "assistant": "assistant",
            "automation": "automation",
            "reply_class": "reply",
            "requests": request_ids,
            "request_rate_per_s": params["request_rate_per_s"],
            "requests_until_s": end,
        },
        "source": {"generator": "harness.svc", "params": dict(params)},
    }
    spec = from_dict(doc)
    doc["slots"] = {
        agent: concurrency(spec, agent) + params["spare_slots"]
        for agent in ("planner", "assistant", "automation")
    }
    return doc


def concurrency(spec: Spec, agent: str) -> int:
    """The most invocations of ``agent`` the nominal schedule runs at once,
    counting a created task's pending creation from its creator's end."""
    start, end = nominal_schedule(spec)
    spans = [(start[t.id], end[t.id]) for t in spec.tasks if t.agent == agent]
    points = sorted([(s, 1) for s, _ in spans] + [(e, -1) for _, e in spans])
    best = current = 0
    for _, step in points:
        current += step
        best = max(best, current)
    return best


def future_requests(spec: Spec, alert: str | None) -> float:
    """The assistant's expected requests after the alert's nominal time."""
    service = spec.service
    trigger = spec.alerts.get(alert) if alert else None
    if not service or trigger is None:
        return 0.0
    start, end = nominal_schedule(spec)
    producers = {a.id: a.producer for a in spec.artifacts}
    readers = [t.id for t in spec.tasks if trigger.item in t.reads]
    if trigger.event == "produce":
        at = end[producers[trigger.item]]
    elif trigger.event == "consume":
        at = min(start[t] for t in readers)
    else:
        raise ValueError(
            f"svc alerts trigger on produce or consume, not {trigger.event}"
        )
    at += trigger.delay_s
    remaining = max(0.0, float(service["requests_until_s"]) - at)
    return float(service["request_rate_per_s"]) * remaining


def monitor_settings(
    spec: Spec, alert: str | None, request_value: float | None
) -> dict[str, Any]:
    """The svc monitor configuration of one run (empty for other scenarios)."""
    if not spec.service:
        return {}
    settings: dict[str, Any] = {
        "boundary": MONITOR_BOUNDARY,
        "future_requests": {spec.service["assistant"]: future_requests(spec, alert)},
    }
    if request_value is not None:
        settings["future_request_value"] = float(request_value)
    return settings


def metrics(
    spec: Spec,
    meta: Mapping[str, Any],
    ledger: list[Mapping[str, Any]],
    marks: list[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]] = (),
    gateway: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Service continuity from the alert to the end of the run (services clock).

    A request counts from the alert on when it was submitted (or refused) at or
    after the alert; it is answered when its reply committed. Latency: from its
    submission to its reply's commit. Tainted commits are classified by their
    lineage (``groundtruth.commit_lineage``). An answered request is answered
    correctly when its reply's lineage is clean and a wrong answer otherwise
    (``svc_answered = svc_answered_correct + svc_wrong_answers``).
    """
    from .groundtruth import (
        commit_lineage,
        ledger_effects,
        protected_classes,
        read_records,
        runtime_tasks,
    )

    service = spec.service
    t_alert = float(meta["t_alert_ms"])
    records = read_records(spec, events, ledger)
    effects = ledger_effects(
        records,
        commit_lineage(spec, records, gateway, runtime_tasks(records.events, marks)),
    )
    submitted = {m["task"]: m["t_ms"] for m in marks if m.get("name") == "submitted"}
    refused = {m["task"]: m["t_ms"] for m in marks if m.get("name") == "refused"}
    asked = {**refused, **submitted}
    requests = [r for r in service["requests"] if asked.get(r, -math.inf) >= t_alert]
    reply = {
        e["issuer"]: e for e in effects.values() if e["class"] == service["reply_class"]
    }
    latency = {
        r: (reply[r]["t_committed"] - submitted[r]) / 1000
        for r in requests
        if r in submitted and r in reply and reply[r]["state"] == "committed"
    }
    committed_after = [
        e
        for e in effects.values()
        if e["state"] == "committed" and e["t_committed"] > t_alert
    ]
    automation = {t.id for t in spec.tasks if t.agent == service["automation"]}
    automation_due = [
        e_id
        for e_id, e in effects.items()
        if e["issuer"] in automation and asked.get(e["issuer"], -math.inf) >= t_alert
    ]
    protected = protected_classes(spec)
    ordered = sorted(latency.values())
    # Answered requests whose reply's lineage is clean. Tainted or unresolved
    # (None) is not clean, the convention of svc_tainted_replies; the rest of
    # the answered requests are wrong answers.
    correct = sum(1 for r in latency if reply[r]["tainted"] is False)
    return {
        "svc_requests": len(requests),
        "svc_refused": sum(1 for r in requests if r in refused and r not in submitted),
        "svc_answered": len(latency),
        "svc_answered_frac": len(latency) / len(requests) if requests else math.nan,
        "svc_answered_correct": correct,
        "svc_answered_correct_frac": correct / len(requests) if requests else math.nan,
        "svc_wrong_answers": len(latency) - correct,
        "svc_tainted_device_commands": sum(
            1
            for e in committed_after
            if e["tainted"] is not False and e["class"] in protected
        ),
        "svc_tainted_replies": sum(
            1
            for e in committed_after
            if e["tainted"] is not False and e["class"] == service["reply_class"]
        ),
        "svc_automation_committed": sum(
            1 for e in committed_after if e["issuer"] in automation
        ),
        "svc_automation_due": len(automation_due),
        "svc_latency_mean_s": sum(ordered) / len(ordered) if ordered else math.nan,
        "svc_latency_median_s": ordered[len(ordered) // 2] if ordered else math.nan,
        # Per answered request, for the added latency against the run without
        # containment (``added_latency``).
        "svc_latency_by_request": json.dumps(latency, sort_keys=True),
    }


def added_latency(rows: list[dict[str, Any]], pair: list[str]) -> None:
    """Per svc run, the mean added latency over the requests it and the
    uncontained run (``none``) of its cell both answered (``svc_added_latency_s``)."""
    baseline = {
        tuple(row.get(k) for k in pair): json.loads(row["svc_latency_by_request"])
        for row in rows
        if row.get("strategy_code") == "none" and row.get("svc_latency_by_request")
    }
    for row in rows:
        if not row.get("svc_latency_by_request"):
            continue
        base = baseline.get(tuple(row.get(k) for k in pair))
        own = json.loads(row["svc_latency_by_request"])
        both = sorted(set(own) & set(base or {}))
        row["svc_added_latency_s"] = (
            sum(own[r] - base[r] for r in both) / len(both) if both else math.nan
        )
        row["svc_added_latency_n"] = len(both)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.svc")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    save(from_dict(document()), args.out)
    print(json.dumps({"out": str(args.out)}))


if __name__ == "__main__":
    main()
