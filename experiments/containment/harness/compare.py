"""The system comparison across result directories (H10): one table of runs from
EdgeCitadel (main runs and compare-ec), A2A and LangGraph, scored by the same
scorer, then the pre-registered tests.

``python -m harness.compare <out>... --to <dir>`` writes:

- ``compare_runs.csv``: every run's ``runs.csv`` row plus two columns the
  baselines need: ``repeated_actuations`` (an effect that reached the device
  again, ``sink_repeat`` events) and ``reexecuted_tasks`` (planned tasks that
  started more than once);
- ``compare_summary.csv``: ``report.summarize`` with ``system`` in the cell;
- ``compare_paired.csv``: ``report.paired`` (Firebreak against every other
  strategy on matched runs, whatever system ran them);
- ``h10.csv``: per strategy, the content-fault part (rogue rate 0) and the
  rogue part (rate 0.3): runs, clean-after-alert rate, median and lower
  quartile of kept work, and the one-sided tests of H10 (b): exact McNemar on
  paired content runs and Fisher exact at rate 0.3, Firebreak against each
  native control. Saturated rogue (rate 1.0) is summarized, never tested.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .report import PAIR, paired, score, summarize

FIREBREAK = "firebreak"


def _events(path: Path) -> Iterable[dict[str, Any]]:
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip():
                yield json.loads(line)


def extra_columns(run_dir: Path) -> dict[str, int]:
    starts: Counter[str] = Counter()
    repeats = 0
    for event in _events(run_dir / "events.jsonl"):
        if event.get("kind") == "sink_repeat":
            repeats += 1
        elif event.get("inv_kind") == "task" and event.get("kind") == "start":
            starts[event["task"]] += 1
    return {
        "repeated_actuations": repeats,
        "reexecuted_tasks": sum(1 for n in starts.values() if n > 1),
    }


def load(outs: Iterable[Path]) -> Any:
    import pandas as pd

    frames = []
    for out in outs:
        if not (out / "runs.csv").exists():
            score(out)
        runs = pd.read_csv(out / "runs.csv")
        extra = [extra_columns(out / "runs" / str(r)) for r in runs["run_id"]]
        runs = pd.concat([runs, pd.DataFrame(extra, index=runs.index)], axis=1)
        runs["source"] = str(out)
        frames.append(runs)
    runs = pd.concat(frames, ignore_index=True)
    if "system" not in runs:
        runs["system"] = "edgecitadel"
    runs["system"] = runs["system"].fillna("edgecitadel")
    return runs


def mcnemar_one_sided(b: int, c: int) -> float:
    """P(X >= b) for X ~ Bin(b + c, 1/2): Firebreak clean where the other is
    not (b) against the reverse (c)."""
    n = b + c
    if n == 0:
        return 1.0
    return sum(math.comb(n, i) for i in range(b, n + 1)) / 2**n


def fisher_one_sided(a: int, n1: int, c: int, n2: int) -> float:
    """P(first group has >= a successes) with the margins fixed (hypergeometric):
    a of n1 Firebreak runs clean against c of n2 runs of the other control."""
    total, clean = n1 + n2, a + c
    denominator = math.comb(total, clean)
    return (
        sum(
            math.comb(n1, k) * math.comb(n2, clean - k)
            for k in range(a, min(n1, clean) + 1)
        )
        / denominator
    )


def _part(runs: Any, rate: float) -> Any:
    rogue = runs["rogue_rate"].fillna(0.0).astype(float)
    return runs[(rogue == rate) & (runs.strategy_code != "reference")]


def h10_table(runs: Any) -> Any:
    import pandas as pd

    rows = []
    for rate, label in ((0.0, "content"), (0.3, "rogue 0.3"), (1.0, "saturated rogue")):
        part = _part(runs, rate)
        fb = part[part.strategy_code == FIREBREAK]
        for (system, code), g in part.groupby(["system", "strategy_code"]):
            kept = g["kept_cfrac"].astype(float)
            row: dict[str, Any] = {
                "part": label,
                "system": system,
                "strategy_code": code,
                "runs": len(g),
                "clean_rate": g["clean_after_alert"].astype(float).mean(),
                "kept_median": kept.median(),
                "kept_q1": kept.quantile(0.25),
                "timeouts": int(g["timeout"].fillna(0).astype(int).sum()),
            }
            if "repeated_actuations" in g:
                row["repeated_actuations"] = int(g["repeated_actuations"].sum())
            if code != FIREBREAK and len(fb) and label == "content":
                joined = fb.set_index(PAIR).join(
                    g.set_index(PAIR), lsuffix="_fb", rsuffix="_o", how="inner"
                )
                a = joined.clean_after_alert_fb.astype(bool)
                o = joined.clean_after_alert_o.astype(bool)
                b, c = int((a & ~o).sum()), int((~a & o).sum())
                row.update(
                    pairs=len(joined), b=b, c=c, p_one_sided=mcnemar_one_sided(b, c)
                )
            elif code != FIREBREAK and len(fb) and label == "rogue 0.3":
                a = int(fb["clean_after_alert"].astype(bool).sum())
                c = int(g["clean_after_alert"].astype(bool).sum())
                row.update(
                    fb_clean=a,
                    fb_runs=len(fb),
                    p_one_sided=fisher_one_sided(a, len(fb), c, len(g)),
                )
            rows.append(row)
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.compare")
    parser.add_argument("outs", nargs="+", type=Path, help="result directories")
    parser.add_argument("--to", type=Path, required=True)
    args = parser.parse_args(argv)
    runs = load(args.outs)
    args.to.mkdir(parents=True, exist_ok=True)
    runs.to_csv(args.to / "compare_runs.csv", index=False)
    summarize(runs).to_csv(args.to / "compare_summary.csv", index=False)
    paired(runs).to_csv(args.to / "compare_paired.csv", index=False)
    h10_table(runs).to_csv(args.to / "h10.csv", index=False)
    print(f"{len(runs)} runs from {len(args.outs)} directories -> {args.to}")


if __name__ == "__main__":
    main()
