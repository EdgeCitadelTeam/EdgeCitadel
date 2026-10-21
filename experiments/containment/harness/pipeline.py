"""The lab's chained grids with a gate before the pilots.

``python -m harness pipeline --stack <stack file> --out <root>`` runs, from
``experiments/containment`` and each into its own directory under ``<root>``:

1. ``regression`` (``experiments/regression.json``: the lab-smoke cells of C2,
   N2 and rand-n7-s1, 69 runs), then scores it;
2. ``svc-smoke`` (23 runs), then scores it;
3. the gate on both (``gate.json``); on a failure it writes the reasons to
   ``GATE-FAILED`` and stops;
4. ``lab-pilot`` (370 runs) and ``svc-pilot`` (69 runs), each scored.

The gate (``GATE``) asks of every run of steps 1 and 2: it ended ``complete``
or ``quiescent``; the scorer's integrity counts (``commits_unlabelled``,
``commits_unresolved``, ``commits_label_mismatch``) are 0; and of all of them
together: Firebreak was clean after the alert in every faulted run, at most 3
worker slots were lost, and the median hand-off latency (every hand-off of
every run, ``metrics.handoff_latencies``) is below 100 ms.

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
}


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if number != number else number  # NaN counts as 0


def gate(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """The gate's verdict on scored rows: ``passed`` and its ``reasons``."""
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
    handoffs = [float(h) for r in rows for h in json.loads(r.get("handoff_ms") or "[]")]
    median = statistics.median(handoffs) if handoffs else None
    if median is None or median >= GATE["max_handoff_ms_median"]:
        reasons.append(
            f"median hand-off latency {median} ms over {len(handoffs)} hand-offs "
            f"(below {GATE['max_handoff_ms_median']:.0f} ms needed)"
        )
    return {
        "passed": not reasons,
        "reasons": reasons,
        "runs": len(rows),
        "slot_restarts": restarts,
        "handoffs": len(handoffs),
        "handoff_ms_median": median,
        "runs_with_errors": [r["run_id"] for r in rows if _number(r.get("n_errors"))],
    }


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


def run_pipeline(stack_path: Path, out: Path) -> int:
    out.mkdir(parents=True, exist_ok=True)
    (out / "GATE-FAILED").unlink(missing_ok=True)
    gated: list[dict[str, Any]] = []
    for name, experiment in GATED:
        gated += _stage(stack_path, name, experiment, out)
    verdict = gate(gated)
    verdict["commit_cost_ms"] = commit_cost_ms(out)
    (out / "gate.json").write_text(json.dumps(verdict, indent=1))
    if not verdict["passed"]:
        (out / "GATE-FAILED").write_text("\n".join(verdict["reasons"]) + "\n")
        _say("gate FAILED: " + "; ".join(verdict["reasons"]))
        return 1
    _say(
        f"gate passed: {verdict['runs']} runs, {verdict['slot_restarts']} slot "
        f"restarts, median hand-off {verdict['handoff_ms_median']:.0f} ms"
    )
    for name, experiment in PILOTS:
        _stage(stack_path, name, experiment, out)
    _say("done")
    return 0
