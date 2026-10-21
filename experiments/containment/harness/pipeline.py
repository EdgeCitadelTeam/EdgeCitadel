"""The lab's chained grids with a gate before the pilots.

``python -m harness pipeline --stack <stack file> --out <root> [--a2a-stack
<A2A-SQLite stack file>]`` runs, from ``experiments/containment`` and each
into its own directory under ``<root>``:

1. ``regression`` (``experiments/regression.json``: the lab-smoke cells of C2,
   N2 and rand-n7-s1, 69 runs), then scores it;
2. ``svc-smoke`` (23 runs), then scores it;
3. the gate on both (``gate.json``); on a failure it writes the reasons to
   ``GATE-FAILED`` and stops;
4. ``lab-pilot`` (370 runs) and ``svc-pilot`` (69 runs), each scored.

The gate (``GATE``) asks of every run of steps 1 and 2: it ended ``complete``
or ``quiescent``; the scorer's integrity counts (``commits_unlabelled``,
``commits_unresolved``, ``commits_label_mismatch``) are 0; and of all of them
together: Firebreak was clean after the alert in every faulted run and at most
3 worker slots were lost. Hand-offs (approved 2026-10-21): the per-hop median
(every hop of every run, ``metrics.handoff_latencies``) passes at 100 ms or
less. Otherwise, when everything else passed, the six-hop chain between
jim-eq and Endurance (``experiments/handoff-chain.json``, 10 repetitions)
runs on this stack (``handoff-ec``) and on the durability-matched A2A stack,
A2A with its SQLite task store (``--a2a-stack``, e.g.
``configs/lab-a2a-sqlite-gate.json``; ``handoff-a2a-sqlite``), and the gate
passes if EdgeCitadel's per-hop median on the chain is at most 3 times
A2A-SQLite's (H11's pre-registered criterion). ``gate.json`` records the
criterion used and every median; an A2A measurement that cannot run fails
the gate with that reason. Control latency (approved 2026-10-21): the
monitor plans with L = 150 ms at every site, which theory needs to bound a
control action's decide-to-enforced time; from the regression stage's
decision logs the gate records every site's sent-to-confirmed p50, p95, p99
and maximum (``control_sent_to_confirmed_ms``) and fails when a site's p99 is
above L (or when no command was confirmed, so the bound is unchecked).

A rerun resumes: finished runs are skipped, and every finished part is scored
again. Progress lines go to standard output (``PIPELINE ...``); run it as the
runner unit (``python -m harness unit --log <root>/pipeline.log -- pipeline
...``) so it outlives the login session.

A hand-off is several durable SQLite commits on the nodes, so its latency
follows their storage: ``gate.json`` also records the commit cost under
``<root>`` (``commit_cost_ms``), and ``python -m harness commit-cost DIR...``
measures it in each node's work root beforehand.
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
import tempfile
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

GATED = (
    ("regression", "experiments/regression.json"),
    ("svc-smoke", "experiments/svc-smoke.json"),
)
PILOTS = (
    ("lab-pilot", "experiments/lab-pilot.json"),
    ("svc-pilot", "experiments/svc-pilot.json"),
)
GATE: dict[str, Any] = {
    "statuses": ("complete", "quiescent"),
    "integrity": ("commits_unlabelled", "commits_unresolved", "commits_label_mismatch"),
    "max_slot_restarts": 3,
    "max_handoff_ms_median": 100.0,
    # The fallback: EdgeCitadel's chain hop median over A2A-SQLite's (H11).
    "max_handoff_ratio": 3.0,
    # The native planning L (runner.MEASURED_CONTROL_MS): every site's p99
    # sent-to-confirmed on the regression stage must be at most this.
    "max_control_p99_ms": 150.0,
}
CONTROL_STAGE = "regression"
HANDOFF_CHAIN = "experiments/handoff-chain.json"
HANDOFF_REASON = "median hand-off latency"
CONTROL_REASON = "control latency"


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if number != number else number  # NaN counts as 0


def quantile(values: Iterable[float], q: float) -> float:
    """Nearest-rank quantile (the maximum for q = 1)."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def control_summary(per_site: Mapping[str, Iterable[float]]) -> dict[str, Any]:
    """Per site: the count, p50, p95, p99 and maximum (ms)."""
    out = {}
    for site in sorted(per_site):
        times = list(per_site[site])
        if times:
            out[site] = {
                "n": len(times),
                **{
                    name: round(quantile(times, q), 1)
                    for name, q in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
                },
                "max": round(max(times), 1),
            }
    return out


def gate(
    rows: Iterable[Mapping[str, Any]],
    control: Mapping[str, Iterable[float]] | None = None,
) -> dict[str, Any]:
    """The gate's verdict on scored rows (and, when given, the per-site
    sent-to-confirmed times of the regression stage): ``passed`` and its
    ``reasons``."""
    rows = list(rows)
    reasons: list[str] = []
    bad_status = [
        f"{r['run_id']}: {r.get('status')}"
        for r in rows
        if r.get("status") not in GATE["statuses"]
    ]
    if bad_status:
        reasons.append(f"runs not complete or quiescent: {bad_status}")
    for column in GATE["integrity"]:
        bad = [r["run_id"] for r in rows if _number(r.get(column)) != 0]
        if bad:
            reasons.append(f"{column} is not 0 in {bad}")
    leaked = [
        r["run_id"]
        for r in rows
        if r.get("strategy_code") == "firebreak"
        and r.get("fault") not in (None, "", "none")
        and _number(r.get("clean_after_alert")) != 1
    ]
    if leaked:
        reasons.append(f"Firebreak not clean after the alert in {leaked}")
    restarts = int(sum(_number(r.get("slot_restarts")) for r in rows))
    if restarts > GATE["max_slot_restarts"]:
        reasons.append(
            f"{restarts} worker slot restarts (at most {GATE['max_slot_restarts']})"
        )
    handoffs = hops(rows)
    median = statistics.median(handoffs) if handoffs else None
    if median is None or median > GATE["max_handoff_ms_median"]:
        reasons.append(
            f"{HANDOFF_REASON} {median} ms over {len(handoffs)} hand-offs "
            f"(at most {GATE['max_handoff_ms_median']:.0f} ms needed)"
        )
    bound = GATE["max_control_p99_ms"]
    summary = control_summary(control) if control is not None else None
    if summary is not None:
        over = {s: v["p99"] for s, v in summary.items() if v["p99"] > bound}
        if not summary:
            reasons.append(
                f"{CONTROL_REASON}: no control command was confirmed in the "
                f"{CONTROL_STAGE} stage, so L = {bound:.0f} ms is unchecked"
            )
        elif over:
            reasons.append(
                f"{CONTROL_REASON}: sent-to-confirmed p99 above L = {bound:.0f} ms "
                f"at {over} (ms), so L is no longer an upper bound"
            )
    return {
        "passed": not reasons,
        "reasons": reasons,
        "runs": len(rows),
        "slot_restarts": restarts,
        "handoffs": len(handoffs),
        "handoff_ms_median": median,
        "handoff_criterion": "absolute",
        "control_latency_ms": bound,
        "control_sent_to_confirmed_ms": summary,
        "runs_with_errors": [r["run_id"] for r in rows if _number(r.get("n_errors"))],
        # Not a condition: whether svc's early alert still came before the
        # assistant read the corrupted schedule (1: the early/late distinction
        # has collapsed in that run; it moves with hand-off speed).
        "svc_early_consumed_at_alert": {
            r["run_id"]: r.get("consumed_at_alert")
            for r in rows
            if r.get("instance") == "svc" and r.get("alert") == "early"
        },
    }


def hops(rows: Iterable[Mapping[str, Any]]) -> list[float]:
    """Every per-hop hand-off of the scored runs (ms)."""
    return [float(h) for r in rows for h in json.loads(r.get("handoff_ms") or "[]")]


def handoff_ratio(
    ec_rows: Iterable[Mapping[str, Any]], a2a_rows: Iterable[Mapping[str, Any]]
) -> dict[str, Any]:
    """The fallback criterion on the chain sample of both systems."""
    ec, a2a = hops(ec_rows), hops(a2a_rows)
    if not ec or not a2a:
        raise ValueError(
            f"no hand-offs measured on the chain ({len(ec)} EdgeCitadel, "
            f"{len(a2a)} A2A-SQLite)"
        )
    ec_median, a2a_median = statistics.median(ec), statistics.median(a2a)
    ratio = ec_median / a2a_median if a2a_median > 0 else float("inf")
    return {
        "passed": ratio <= GATE["max_handoff_ratio"],
        "chain_handoffs": {"edgecitadel": len(ec), "a2a_sqlite": len(a2a)},
        "chain_handoff_ms_median": {"edgecitadel": ec_median, "a2a_sqlite": a2a_median},
        "chain_handoff_ratio": ratio,
    }


def _a2a_sqlite_stack(path: Path) -> None:
    """Refuse a stack that is not the durability-matched A2A one."""
    from .stack import StackConfig

    config = StackConfig.load(path)
    if config.system != "a2a" or config.a2a_task_store != "sqlite":
        raise ValueError(
            f"{path} is not an A2A stack with the SQLite task store "
            f"(system {config.system}, task store {config.a2a_task_store})"
        )


def commit_cost_ms(directory: Path, commits: int = 30) -> dict[str, float]:
    """Median durable commit in ``directory``, as agentd commits (rollback
    journal, ``synchronous=EXTRA``): to one database, and to an attached pair
    in one transaction (agentd's trace and task members)."""
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=directory) as scratch:
        db = sqlite3.connect(os.path.join(scratch, "main.db"), isolation_level=None)
        try:
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("PRAGMA synchronous=EXTRA")
            db.execute("ATTACH DATABASE ? AS pair", (os.path.join(scratch, "pair.db"),))
            db.execute("PRAGMA pair.synchronous=EXTRA")
            db.execute("CREATE TABLE one(x)")
            db.execute("CREATE TABLE pair.two(x)")
            out = {}
            for name, both in (("one_db", False), ("attached_pair", True)):
                times = []
                for i in range(commits):
                    started = time.perf_counter()
                    db.execute("BEGIN IMMEDIATE")
                    db.execute("INSERT INTO one VALUES (?)", (i,))
                    if both:
                        db.execute("INSERT INTO pair.two VALUES (?)", (i,))
                    db.execute("COMMIT")
                    times.append((time.perf_counter() - started) * 1000)
                out[name] = round(statistics.median(times), 2)
            return out
        finally:
            db.close()


def _say(message: str) -> None:
    print(
        f"PIPELINE {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {message}",
        flush=True,
    )


def _stage(
    stack_path: Path, name: str, experiment: str, out: Path
) -> list[dict[str, Any]]:
    from .report import score
    from .runner import Experiment, run_experiment
    from .stack import StackConfig

    _say(f"{name}: running {experiment} into {out / name}")
    run_experiment(
        StackConfig.load(stack_path), Experiment.load(experiment), out / name
    )
    rows = score(out / name)
    _say(f"{name}: scored {len(rows)} runs")
    return rows


def _control_latency(out: Path) -> dict[str, list[float]]:
    """Per site, the regression stage's sent-to-confirmed times (ms)."""
    from .report import control_latency

    return control_latency(out / CONTROL_STAGE)


def _handoff_fallback(
    verdict: dict[str, Any], stack_path: Path, a2a_stack: Path | None, out: Path
) -> None:
    """Replace a failed absolute hand-off check by the A2A-SQLite ratio."""
    verdict["handoff_criterion"] = "a2a_sqlite_ratio"
    reasons = [r for r in verdict["reasons"] if not r.startswith(HANDOFF_REASON)]
    try:
        if a2a_stack is None:
            raise ValueError("no A2A-SQLite stack given (--a2a-stack)")
        _a2a_sqlite_stack(a2a_stack)
        ec_rows = _stage(stack_path, "handoff-ec", HANDOFF_CHAIN, out)
        a2a_rows = _stage(a2a_stack, "handoff-a2a-sqlite", HANDOFF_CHAIN, out)
        ratio = handoff_ratio(ec_rows, a2a_rows)
    except Exception as error:  # noqa: BLE001 - the gate fails with the cause
        reasons.append(
            f"the per-hop median is above "
            f"{GATE['max_handoff_ms_median']:.0f} ms and the A2A-SQLite "
            f"measurement could not run: {type(error).__name__}: {error}"[:2000]
        )
    else:
        verdict.update({k: v for k, v in ratio.items() if k != "passed"})
        if not ratio["passed"]:
            medians = ratio["chain_handoff_ms_median"]
            reasons.append(
                f"chain hand-off median {medians['edgecitadel']:.1f} ms is "
                f"{ratio['chain_handoff_ratio']:.2f} times A2A-SQLite's "
                f"{medians['a2a_sqlite']:.1f} ms (at most "
                f"{GATE['max_handoff_ratio']:g} times needed)"
            )
    verdict["reasons"] = reasons
    verdict["passed"] = not reasons


def run_pipeline(stack_path: Path, out: Path, a2a_stack: Path | None = None) -> int:
    out.mkdir(parents=True, exist_ok=True)
    (out / "GATE-FAILED").unlink(missing_ok=True)
    gated: list[dict[str, Any]] = []
    for name, experiment in GATED:
        gated += _stage(stack_path, name, experiment, out)
    verdict = gate(gated, _control_latency(out))
    others = [r for r in verdict["reasons"] if not r.startswith(HANDOFF_REASON)]
    if not verdict["passed"] and not others:
        # Only the hand-off check failed: the approved fallback decides it.
        _handoff_fallback(verdict, stack_path, a2a_stack, out)
    verdict["commit_cost_ms"] = commit_cost_ms(out)
    (out / "gate.json").write_text(json.dumps(verdict, indent=1))
    if not verdict["passed"]:
        (out / "GATE-FAILED").write_text("\n".join(verdict["reasons"]) + "\n")
        _say("gate FAILED: " + "; ".join(verdict["reasons"]))
        return 1
    p99 = max(v["p99"] for v in verdict["control_sent_to_confirmed_ms"].values())
    _say(
        f"gate passed: {verdict['runs']} runs, {verdict['slot_restarts']} slot "
        f"restarts, median hand-off {verdict['handoff_ms_median']:.0f} ms "
        f"({verdict['handoff_criterion']} criterion), control p99 at most "
        f"{p99:.0f} ms"
    )
    for name, experiment in PILOTS:
        _stage(stack_path, name, experiment, out)
    _say("done")
    return 0
