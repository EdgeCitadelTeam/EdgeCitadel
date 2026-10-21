"""Overhead of each system on fault-free runs: completion time, handoff latency,
throughput, CPU and memory (``python -m harness.overhead <out>... --to <dir>``).

Everything comes from the run's own records, computed the same way for every
system (the services clock, the plan, the process samples):

- ``makespan_s``: the last task's end after the run's start;
- handoff latency (one row per created task in ``overhead_hops.csv``): the
  child's first start minus its dispatcher's produced event, i.e. the creating
  message to the child's agent plus that agent's dispatch of the task to
  itself, with the nodes on both ends;
- ``throughput_tps`` for fan-outs: created tasks ended per second between the
  root's produced event and the last child's end;
- CPU-seconds and peak resident memory per process role over the run, from
  ``procstat__<host>.jsonl``: ``runtime`` (agentd, NATS), ``control`` (Core
  aggregator with the monitor), ``agents`` (det-workers or A2A servers);
  the shared ground-truth services are reported apart and left out of totals.
"""

from __future__ import annotations

import csv
import json
import statistics
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

ROLES = (
    ("services", "services"),
    ("aggregator", "control"),
    ("agentd-", "runtime"),
    ("nats", "runtime"),
    ("worker-", "agents"),
    ("a2a-", "agents"),
)


def role_of(group: str) -> str | None:
    for prefix, role in ROLES:
        if group.startswith(prefix):
            return role
    return None  # the sampler itself


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def task_times(events: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, int]]:
    times: dict[str, dict[str, int]] = {}
    for e in events:
        if e.get("inv_kind") != "task":
            continue
        entry = times.setdefault(e["task"], {})
        kind = e["kind"]
        if kind in {"start", "produced", "end"} and kind not in entry:
            entry[kind] = int(e["t_ms"])
    return times


def hops(
    plan: Mapping[str, Any], times: Mapping[str, Mapping[str, int]]
) -> list[dict[str, Any]]:
    rows = []
    for task, spec in plan["tasks"].items():
        dispatcher = spec["creation"]["dispatcher"]
        if dispatcher is None:
            continue
        start = times.get(task, {}).get("start")
        produced = times.get(dispatcher, {}).get("produced")
        if start is None or produced is None:
            continue
        rows.append(
            {
                "task": task,
                "via": spec["creation"]["via"],
                "from_node": plan["agents"][plan["tasks"][dispatcher]["agent"]]["node"],
                "to_node": plan["agents"][spec["agent"]]["node"],
                "handoff_ms": start - produced,
            }
        )
    return rows


def resources(
    samples: list[dict[str, Any]], t0: int, t1: int
) -> dict[str, dict[str, float]]:
    """CPU-seconds used and peak RSS per role between t0 and t1 on one host."""
    window = [s for s in samples if t0 <= s["t_ms"] <= t1] or samples
    out: dict[str, dict[str, float]] = {}
    if not window:
        return out
    first, last = window[0]["groups"], window[-1]["groups"]
    for group, sample in last.items():
        role = role_of(group)
        if role is None:
            continue
        entry = out.setdefault(role, {"cpu_s": 0.0, "peak_rss_mb": 0.0})
        before = first.get(group, {}).get("cpu_s", 0.0)
        entry["cpu_s"] += max(0.0, sample["cpu_s"] - before)
    for role, entry in out.items():
        peak = max(
            sum(v["rss_kb"] for g, v in s["groups"].items() if role_of(g) == role)
            for s in window
        )
        entry["peak_rss_mb"] = peak / 1024
    return out


def run_row(run_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    meta = json.loads((run_dir / "run.json").read_text())
    if meta.get("strategy") != "reference" or meta.get("t0_ms") is None:
        return None
    plan = json.loads((run_dir / "plan.json").read_text())
    times = task_times(_jsonl(run_dir / "events.jsonl"))
    t0 = int(meta["t0_ms"])
    ends = [t["end"] for t in times.values() if "end" in t]
    t1 = max(ends) if ends else t0
    hop_rows = hops(plan, times)
    identity = {
        "run_id": meta["run_id"],
        "system": meta.get("system", "edgecitadel"),
        "mode": meta.get("mode"),
        "instance": meta.get("instance"),
        "rep": meta.get("rep"),
        "status": meta.get("status"),
        "attempts": meta.get("attempts", 1),
    }
    handoffs = [h["handoff_ms"] for h in hop_rows]
    row: dict[str, Any] = {
        **identity,
        "n_tasks": len(plan["tasks"]),
        "n_ended": len(ends),
        "makespan_s": (t1 - t0) / 1000,
        "handoff_p50_ms": statistics.median(handoffs) if handoffs else None,
        "throughput_tps": None,
    }
    roots = [t for t, s in plan["tasks"].items() if s["creation"]["dispatcher"] is None]
    children = [t for t in plan["tasks"] if t not in roots]
    if (
        len(roots) == 1
        and children
        and all(
            plan["tasks"][c]["creation"]["dispatcher"] == roots[0] for c in children
        )
    ):
        start = times.get(roots[0], {}).get("produced")
        last = max((times.get(c, {}).get("end", 0) for c in children), default=0)
        if start and last > start:
            row["throughput_tps"] = len(children) / ((last - start) / 1000)
    totals = {"cpu_s": 0.0, "peak_rss_mb": 0.0}
    for path in sorted(run_dir.glob("procstat__*.jsonl")):
        host = path.stem.removeprefix("procstat__")
        for role, value in resources(_jsonl(path), t0, t1).items():
            row[f"{host}_{role}_cpu_s"] = round(value["cpu_s"], 3)
            row[f"{host}_{role}_peak_rss_mb"] = round(value["peak_rss_mb"], 1)
            if role != "services":
                totals["cpu_s"] += value["cpu_s"]
                totals["peak_rss_mb"] += value["peak_rss_mb"]
    row["total_cpu_s"] = round(totals["cpu_s"], 3)
    row["total_peak_rss_mb"] = round(totals["peak_rss_mb"], 1)
    return row, [{**identity, **h} for h in hop_rows]


def collect(outs: Iterable[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows, hop_rows = [], []
    for out in outs:
        for meta in sorted((out / "runs").glob("*/run.json")):
            if ".infra" in meta.parent.name:
                continue
            result = run_row(meta.parent)
            if result is not None:
                rows.append(result[0])
                hop_rows += result[1]
    return rows, hop_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns: list[str] = []
    for row in rows:
        columns += [c for c in row if c not in columns]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, restval="")
        writer.writeheader()
        writer.writerows(rows)


def summarize(
    rows: list[dict[str, Any]], hop_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Median and spread per system and instance (and per node pair for hops)."""
    out = []
    keys = sorted({(r["system"], r["instance"]) for r in rows})
    for system, instance in keys:
        group = [r for r in rows if r["system"] == system and r["instance"] == instance]
        hop_group = [
            h for h in hop_rows if h["system"] == system and h["instance"] == instance
        ]
        entry: dict[str, Any] = {
            "system": system,
            "instance": instance,
            "runs": len(group),
        }
        for column in (
            "makespan_s",
            "throughput_tps",
            "total_cpu_s",
            "total_peak_rss_mb",
        ):
            values = [
                float(r[column]) for r in group if r.get(column) not in (None, "")
            ]
            entry[f"{column}_median"] = statistics.median(values) if values else None
        for pair in sorted({(h["from_node"], h["to_node"]) for h in hop_group}):
            values = sorted(
                h["handoff_ms"]
                for h in hop_group
                if (h["from_node"], h["to_node"]) == pair
            )
            label = f"handoff_{pair[0]}_to_{pair[1]}"
            entry[f"{label}_p50_ms"] = values[len(values) // 2]
            entry[f"{label}_p90_ms"] = values[
                min(len(values) - 1, int(len(values) * 0.9))
            ]
            entry[f"{label}_n"] = len(values)
        out.append(entry)
    return out


def main(argv: list[str] | None = None) -> None:
    import argparse

    parser = argparse.ArgumentParser(prog="harness.overhead")
    parser.add_argument("outs", nargs="+", type=Path, help="result directories")
    parser.add_argument("--to", type=Path, required=True, help="where the CSVs go")
    args = parser.parse_args(argv)
    rows, hop_rows = collect(args.outs)
    args.to.mkdir(parents=True, exist_ok=True)
    write_csv(args.to / "overhead_runs.csv", rows)
    write_csv(args.to / "overhead_hops.csv", hop_rows)
    write_csv(args.to / "overhead_summary.csv", summarize(rows, hop_rows))
    print(f"{len(rows)} reference runs, {len(hop_rows)} handoffs -> {args.to}")


if __name__ == "__main__":
    main()
