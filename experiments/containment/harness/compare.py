"""The system comparison across result directories (H10): one table of runs from
EdgeCitadel (main runs and compare-ec), A2A and LangGraph, scored by the same
scorer, then the pre-registered tests.

``python -m harness.compare <out>... --to <dir> [--ec [PART:]KEY=VALUE ...]``
writes:

- ``compare_runs.csv``: every run's ``runs.csv`` row plus columns the
  baselines need: ``repeated_actuations`` (an effect that reached the device
  again, ``sink_repeat`` events), ``reexecuted_tasks`` (planned tasks that
  started more than once) and each leak (a violation committed after the
  alert, ``metrics.evaluate``'s ``violations``) in exactly one class:

  - ``leak_after_stop``: its issuing invocation had been cancelled (or paused
    and not resumed) before the effect landed: the stop did not reach work
    already under way;
  - ``leak_polluted_use``: issued by another task that used polluted data
    (consumed the bad artifact or what was built from it, or received a rogue
    message), never stopped before the effect landed;
  - ``leak_at_source``: issued by the faulty task itself, never stopped;

  plus ``repeat_leaks``: tainted repeated actuations after the alert (a
  re-executed LangGraph node; the ledger keeps one record per effect, so
  ``violations`` misses them), classified the same way and counted against
  ``clean`` (``clean_after_alert`` and no repeat leak), the rate H10 uses;
  and ``reexecuted_relabelled``: re-executed tasks whose last execution
  produced other labels than the first, which the scorer reads;

- ``compare_summary.csv``: ``report.summarize`` with ``system`` in the cell;
- ``compare_paired.csv``: ``report.paired`` (Firebreak against every other
  strategy on matched runs of the same system and mechanism settings);
- ``h10.csv``: per strategy, the content-fault part (rogue rate 0), the rogue
  part (rate 0.3) and saturated rogue (1.0): runs, clean-after-alert rate,
  median and lower quartile of kept work, makespan, timeouts, consumed at
  alert, leaks by class, and the one-sided tests of H10 (b): exact McNemar on
  content runs paired with Firebreak's, with the paired kept-work difference
  of (c), and Fisher exact at rate 0.3 against Firebreak's runs of the same
  cells. Saturated rogue is summarized, never tested.

Cross-system pairs match on the workload cell (``CROSS``), not on the
monitor's mechanism settings, which the baselines do not have: the
EdgeCitadel rows must hold one mechanism configuration per cell (choose it
with ``--ec phase1_mode=adaptive``, or per part with ``--ec
content:phase1_mode=...`` and ``--ec rogue:...`` when the main runs and
compare-ec differ), and every cell on either side must be unique, or the
comparison stops and says which. A leak's invocation is the one that
committed it (the sink's ``task_id``); ``committed_by_later_attempt`` counts
effects committed by another invocation than the first attempt, whose labels
the ledger keeps.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .groundtruth import fault_seeds, ledger_effects, protected_classes, read_records
from .report import paired, score, sign_flip_p, summarize
from .spec import from_dict

FIREBREAK = "firebreak"
EDGECITADEL = "edgecitadel"
# A workload cell across systems: what the alert met, not how a monitor answered.
CROSS = ["instance", "rogue_rate", "alert", "root_mode", "latency", "time_scale", "rep"]
CELL_OF_RUN = [c for c in CROSS if c != "rep"]
MECHANISM = ["profile", "phase1_mode", "phase1_labels", "receding_during_waits"]
LEAK_CLASSES = ("leak_after_stop", "leak_polluted_use", "leak_at_source")
EXTRA = (
    "repeated_actuations",
    "reexecuted_tasks",
    "reexecuted_relabelled",
    "committed_by_later_attempt",
)
PARTS = {"content": lambda rate: rate == 0.0, "rogue": lambda rate: rate > 0.0}
STOPS = {"cancelled", "paused"}
RUNS = {"start", "resumed"}


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _actions(decisions: Iterable[Mapping[str, Any]]) -> Iterable[Mapping[str, Any]]:
    for d in decisions:
        for key in ("phase1", "phase2", "actions"):
            for record in d.get(key) or ():
                if isinstance(record, Mapping) and "action" in record:
                    yield record


def leak_classes(run_dir: Path) -> dict[str, int]:
    """Each violation committed after the alert, by why it got through."""
    out = dict.fromkeys(
        (*LEAK_CLASSES, "repeat_leaks", "committed_by_later_attempt"), 0
    )
    meta = json.loads((run_dir / "run.json").read_text())
    t_alert = meta.get("t_alert_ms")
    if meta.get("t0_ms") is None or t_alert is None:
        return out
    spec = from_dict(json.loads((run_dir / "spec.json").read_text()))
    events = _jsonl(run_dir / "events.jsonl")
    ledger = _jsonl(run_dir / "ledger.jsonl")
    records = read_records(spec, events, ledger)
    # When each invocation (runtime task) stopped or ran: the worker's events,
    # and the native controllers' confirmed cancels, which name invocations.
    marks: dict[str, list[tuple[int, bool]]] = defaultdict(list)
    for e in records.events:
        if e.get("rt") and e["kind"] in STOPS | RUNS:
            marks[str(e["rt"])].append((int(e["t_ms"]), e["kind"] in STOPS))
    for record in _actions(_jsonl(run_dir / "decisions.jsonl")):
        action = record["action"]
        if (
            action.get("op") == "cancel"
            and record.get("state") == "confirmed"
            and record.get("t_confirmed") is not None
        ):
            marks[str(action.get("object_id"))].append(
                (int(record["t_confirmed"]), True)
            )
    # The invocation that committed the effect (the sink's record), not the
    # first attempt's, which the ledger's committed record copies.
    issued_by = {
        str(r["effect_id"]): str(
            (r.get("gateway") or {}).get("task_id") or r.get("rt_task")
        )
        for r in records.ledger
        if r["phase"] == "committed"
    }
    out["committed_by_later_attempt"] = sum(
        1
        for r in records.ledger
        if r["phase"] == "committed"
        and (r.get("gateway") or {}).get("task_id") not in {None, r.get("rt_task")}
    )
    classes = {c.id for c in spec.effect_classes}
    protected = protected_classes(spec)
    seeds = fault_seeds(spec)
    effects = ledger_effects(records)

    def counted(cls: str) -> bool:
        return cls in protected or cls not in classes

    for effect_id, e in effects.items():
        if not (
            e["state"] == "committed"
            and e["labels"]
            and counted(e["class"])
            and e["t_committed"] > t_alert
        ):
            continue
        rt = issued_by.get(effect_id, "")
        out[leak_class(e["t_committed"], marks.get(rt, []), e["issuer"], seeds)] += 1
    for event in records.events:
        effect = effects.get(str(event.get("effect_id")))
        if (
            event["kind"] == "sink_repeat"
            and effect is not None
            and event.get("labels")
            and counted(effect["class"])
            and int(event["t_ms"]) > t_alert
        ):
            rt = str(event.get("rt"))
            t = int(event["t_ms"])
            out[leak_class(t, marks.get(rt, []), effect["issuer"], seeds)] += 1
            out["repeat_leaks"] += 1
    return out


def leak_class(
    t_committed: int,
    marks: Iterable[tuple[int, bool]],
    issuer: str,
    seeds: set[str],
) -> str:
    """``marks``: (time, stopped) of the issuing invocation; the last one before
    the commit says whether it was stopped then."""
    before = sorted(m for m in marks if m[0] < t_committed)
    if before and before[-1][1]:
        return "leak_after_stop"
    return "leak_at_source" if issuer in seeds else "leak_polluted_use"


def extra_columns(run_dir: Path) -> dict[str, int]:
    starts: Counter[str] = Counter()
    produced: dict[str, dict[str, set[str]]] = defaultdict(dict)  # task -> rt -> labels
    repeats = 0
    for event in _jsonl(run_dir / "events.jsonl"):
        if event.get("kind") == "sink_repeat":
            repeats += 1
        elif event.get("inv_kind") == "task" and event.get("kind") == "start":
            starts[event["task"]] += 1
        elif event.get("inv_kind") == "task" and event.get("kind") == "produce":
            labels = produced[event["task"]].setdefault(str(event["rt"]), set())
            labels.update(event.get("labels") or ())
    runs_of = [list(by_rt.values()) for by_rt in produced.values()]
    relabelled = sum(1 for seen in runs_of if len(seen) > 1 and seen[0] != seen[-1])
    return {
        "repeated_actuations": repeats,
        "reexecuted_tasks": sum(1 for n in starts.values() if n > 1),
        "reexecuted_relabelled": relabelled,
        **leak_classes(run_dir),
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
        runs["system"] = EDGECITADEL
    runs["system"] = runs["system"].fillna(EDGECITADEL)
    leaks = runs[list(LEAK_CLASSES)].sum(axis=1)
    scored = runs["violations"].fillna(0).astype(int) + runs["repeat_leaks"]
    if (leaks != scored).any():
        bad = runs.loc[leaks != scored, "run_id"].tolist()
        raise ValueError(f"leak classes do not add up to violations in {bad[:5]}")
    runs["clean"] = (
        runs["clean_after_alert"].fillna(0).astype(bool) & (runs["repeat_leaks"] == 0)
    ).astype(int)
    return runs


def select_edgecitadel(runs: Any, where: Iterable[tuple[str | None, str, str]]) -> Any:
    """Keep the EdgeCitadel rows of one mechanism configuration (``--ec``):
    (part or None for both, column, value)."""
    rate = runs["rogue_rate"].fillna(0.0).astype(float)
    keep = runs.system == EDGECITADEL
    for part, key, value in where:
        if key not in runs:
            raise ValueError(f"--ec {key}: no such column")
        if part is not None and part not in PARTS:
            raise ValueError(f"--ec {part}: the parts are {sorted(PARTS)}")
        scope = rate.map(PARTS[part]) if part else rate.notna()
        keep = keep & (~scope | (runs[key].astype(str) == value))
    return runs[(runs.system != EDGECITADEL) | keep]


def check_cells(runs: Any) -> None:
    """Each (system, strategy) holds one run per cross-system cell; otherwise a
    pairing would join an arbitrary one of them."""
    faulted = runs[runs.strategy_code != "reference"]
    for (system, code), g in faulted.groupby(["system", "strategy_code"]):
        counts = g.groupby(CROSS, dropna=False).size()
        if (counts > 1).any():
            settings = (
                g[MECHANISM].drop_duplicates().to_dict("records")
                if set(MECHANISM) <= set(g)
                else []
            )
            raise ValueError(
                f"{system}/{code}: {int((counts > 1).sum())} cells hold more than "
                f"one run (mechanism settings {settings}); choose one "
                "configuration with --ec KEY=VALUE or pass fewer result directories"
            )


def clean_column(g: Any) -> str:
    """``clean`` (no leak, repeats included) when loaded from result directories."""
    return "clean" if "clean" in g else "clean_after_alert"


def _part(runs: Any, rate: float) -> Any:
    rogue = runs["rogue_rate"].fillna(0.0).astype(float)
    return runs[(rogue == rate) & (runs.strategy_code != "reference")]


def _median(g: Any, column: str) -> float:
    return float(g[column].astype(float).median()) if column in g else math.nan


def _cells(g: Any) -> list[tuple]:
    return list(map(tuple, g[CELL_OF_RUN].astype(str).itertuples(index=False)))


def h10_table(runs: Any) -> Any:
    import pandas as pd

    rows = []
    for rate, label in ((0.0, "content"), (0.3, "rogue 0.3"), (1.0, "saturated rogue")):
        part = _part(runs, rate)
        fb = part[(part.strategy_code == FIREBREAK) & (part.system == EDGECITADEL)]
        for (system, code), g in part.groupby(["system", "strategy_code"]):
            kept = g["kept_cfrac"].astype(float)
            row: dict[str, Any] = {
                "part": label,
                "system": system,
                "strategy_code": code,
                "runs": len(g),
                "clean_rate": g[clean_column(g)].astype(float).mean(),
                "kept_median": kept.median(),
                "kept_q1": kept.quantile(0.25),
                "makespan_median_s": _median(g, "makespan_s"),
                "consumed_at_alert_median": _median(g, "consumed_at_alert"),
                "timeouts": int(g["timeout"].fillna(0).astype(int).sum()),
            }
            for column in (*EXTRA, *LEAK_CLASSES, "repeat_leaks"):
                if column in g:
                    row[column] = int(g[column].sum())
            if code == FIREBREAK and system == EDGECITADEL:
                rows.append(row)
                continue
            if label == "content" and len(fb):
                joined = fb.set_index(CROSS).join(
                    g.set_index(CROSS), lsuffix="_fb", rsuffix="_o", how="inner"
                )
                col = clean_column(g)
                a = joined[f"{col}_fb"].astype(bool)
                o = joined[f"{col}_o"].astype(bool)
                b, c = int((a & ~o).sum()), int((~a & o).sum())
                diff = joined.kept_cfrac_fb.astype(float) - joined.kept_cfrac_o.astype(
                    float
                )
                row.update(
                    pairs=len(joined),
                    b=b,
                    c=c,
                    p_one_sided=mcnemar_one_sided(b, c) if len(joined) else math.nan,
                    kept_diff_mean=diff.mean(),
                    kept_diff_median=diff.median(),
                    p_kept_sign_flip=sign_flip_p(diff) if len(joined) else math.nan,
                )
                if not len(joined):
                    print(
                        f"warning: {system}/{code} has no content run paired with "
                        "Firebreak's (check the cells and --ec)",
                        file=sys.stderr,
                    )
            elif label == "rogue 0.3" and len(fb):
                # Firebreak's runs of the same cells (C2 in compare-ec), not every
                # rate-0.3 run the main grid may hold.
                cells = set(_cells(g))
                mine = fb[[cell in cells for cell in _cells(fb)]]
                col = clean_column(g)
                a = int(mine[col].astype(bool).sum())
                c = int(g[col].astype(bool).sum())
                row.update(
                    fb_clean=a,
                    fb_runs=len(mine),
                    p_one_sided=fisher_one_sided(a, len(mine), c, len(g))
                    if len(mine)
                    else math.nan,
                )
            rows.append(row)
    return pd.DataFrame(rows)


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


def _setting(text: str) -> tuple[str | None, str, str]:
    key, sep, value = text.partition("=")
    if not sep:
        raise argparse.ArgumentTypeError(f"expected [PART:]KEY=VALUE, got {text!r}")
    part, colon, column = key.rpartition(":")
    return (part if colon else None), column, value


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.compare")
    parser.add_argument("outs", nargs="+", type=Path, help="result directories")
    parser.add_argument("--to", type=Path, required=True)
    parser.add_argument(
        "--ec",
        type=_setting,
        action="append",
        default=[],
        metavar="[PART:]KEY=VALUE",
        help="keep the EdgeCitadel rows with this setting, in both parts or in one "
        "(content, rogue), e.g. phase1_mode=adaptive or content:phase1_mode=freeze",
    )
    args = parser.parse_args(argv)
    runs = select_edgecitadel(load(args.outs), args.ec)
    check_cells(runs)
    args.to.mkdir(parents=True, exist_ok=True)
    runs.to_csv(args.to / "compare_runs.csv", index=False)
    summarize(runs).to_csv(args.to / "compare_summary.csv", index=False)
    paired(runs).to_csv(args.to / "compare_paired.csv", index=False)
    h10_table(runs).to_csv(args.to / "h10.csv", index=False)
    print(f"{len(runs)} runs from {len(args.outs)} directories -> {args.to}")


if __name__ == "__main__":
    main()
