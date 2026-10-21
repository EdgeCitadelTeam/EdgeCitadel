"""Scoring and reports: CSVs, bootstrap CIs, paired comparisons, paper figures.

``score`` (stdlib only) turns every collected run into one row of
``runs.csv`` (``metrics.evaluate`` plus the run's identity) and its control
actions into ``actions.csv``. ``report`` (pandas, numpy, matplotlib) writes:

- ``summary.csv``: per cell (mode, rogue rate, alert, root mode, latency and
  its regime, strategy) the split outcomes with 95 % percentile-bootstrap CIs
  over runs: (a) runs with no tainted effect after the alert, (b) tainted
  effects after containment was established, (c) runs where it was
  established and the alert-to-established time, (d) unconfirmed, control
  failure, infeasible and timeout counts; normal work kept (% compute);
  control accounting (logical actions, control messages, confirmations);
  root-cause accuracy in ranked mode;
- ``paired.csv``: Firebreak against every other strategy on matched runs (same
  instance, rogue rate, alert, root mode, latency, repetition): mean
  differences with bootstrap CIs, exact McNemar tests on (a) and (c) and a
  paired sign-flip permutation test on kept work;
- ``sim_vs_runtime.csv`` when ``sim_runs.csv`` exists: the simulator's
  prediction next to the runtime's mean for each cell;
- figures in the simulator's v2 style: the Pareto 2x2 (rogue rate 0 / > 0 by
  the lowest / highest latency setting; (a) against kept work) and (a), (c)
  and kept work against control latency, with stress settings marked.

Timeouts, unconfirmed and failed control actions and infeasible decisions stay
in every denominator (``score`` keeps those runs; they count against (a)-(c)).
"""

from __future__ import annotations

import csv
import json
import math
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .metrics import evaluate
from .names import CODE_NAME, COLOR, FAMILY, MARKER, PLOTTED_NAMES
from .netem import MEASURED_RTT_MS
from .runner import infrastructure_failure, testbed_counts
from .spec import from_dict

IDENTITY = (
    "run_id",
    "experiment",
    "mode",
    "instance",
    "spec_name",
    "fault",
    "rogue_rate",
    "strategy_code",
    "strategy",
    "family",
    "profile",
    "phase1_include_cut",
    "alert",
    "root_mode",
    "latency",
    "latency_regime",
    "control_latency_ms",
    "rep",
    "time_scale",
    "n_errors",
    "attempts",
)
BOOTSTRAP = 2000


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _optional_json(path: Path) -> Any:
    return (
        json.loads(path.read_text()) if path.exists() and path.stat().st_size else None
    )


def _regime(meta: Mapping[str, Any]) -> str:
    """The latency regime of a run recorded before runs carried it."""
    values = [
        float(v)
        for key in ("control_latency_ms", "control_delay_ms")
        for v in (meta.get(key) or {}).values()
    ]
    return "stress" if max(values, default=0.0) > MEASURED_RTT_MS else "measured"


def score_run(run_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    meta = json.loads((run_dir / "run.json").read_text())
    code = meta["strategy"]
    latency = meta.get("control_latency_ms") or {}
    row: dict[str, Any] = {
        "run_id": meta["run_id"],
        "experiment": meta.get("experiment"),
        "mode": meta.get("mode"),
        "instance": meta.get("instance"),
        "spec_name": meta.get("spec_name"),
        "fault": meta.get("fault"),
        "rogue_rate": meta.get("rogue_rate"),
        "strategy_code": code,
        "strategy": CODE_NAME.get(code, code),
        "family": FAMILY.get(CODE_NAME.get(code, ""), ""),
        "alert": meta.get("alert"),
        "root_mode": meta.get("root_mode") or ("given" if meta.get("alert") else None),
        "latency": meta.get("latency"),
        "latency_regime": meta.get("latency_regime") or _regime(meta),
        "effect_sink": meta.get("effect_sink", "ledger"),
        # The monitor's mechanism profile and option of the run (main design:
        # deployed, no at-alert cut).
        "profile": meta.get("profile", "deployed"),
        "phase1_include_cut": int(bool(meta.get("phase1_include_cut", False))),
        "control_latency_ms": sum(latency.values()) / len(latency)
        if latency
        else math.nan,
        "rep": meta.get("rep"),
        "time_scale": meta.get("time_scale"),
        "n_errors": len(meta.get("errors", [])),
        "attempts": meta.get("attempts", 1),
        # A run whose last attempt still failed on the testbed stays in every
        # denominator; this column lets a reader see (or exclude) it. The
        # counts are also re-read from the collected records.
        "testbed_failure": infrastructure_failure(
            {
                **meta,
                **{
                    key: max(int(meta.get(key) or 0), count)
                    for key, count in testbed_counts(run_dir).items()
                },
            }
        )
        or "",
    }
    actions: list[dict[str, Any]] = []
    if meta.get("t0_ms") is None or (
        meta.get("fault") != "none" and meta.get("t_alert_ms") is None
    ):
        # Failed to start, or its alert never fired: kept as a failed run.
        row["status"] = meta.get("status", "failed_to_start")
        row["timeout"] = int(meta.get("status") == "timeout")
        row["clean_after_alert"] = 0
        row["established"] = 0
        return row, actions
    spec = from_dict(json.loads((run_dir / "spec.json").read_text()))
    decisions = _jsonl(run_dir / "decisions.jsonl")
    status = _optional_json(run_dir / "status_at_decision.json") or _optional_json(
        run_dir / "status.json"
    )
    row.update(
        evaluate(
            spec,
            meta,
            _jsonl(run_dir / "events.jsonl"),
            _jsonl(run_dir / "ledger.jsonl"),
            decisions,
            status,
        )
    )
    t_alert = meta.get("t_alert_ms")
    for d in decisions:
        for record in [*d.get("phase1", ()), *d.get("phase2", ())]:
            a = record["action"]

            def rel(key: str) -> float:
                value = record.get(key)
                return (
                    math.nan
                    if value is None or t_alert is None
                    else (value - t_alert) / 1000
                )

            actions.append(
                {
                    "run_id": meta["run_id"],
                    "strategy": row["strategy"],
                    "decision": d.get("type"),
                    "incident": record["incident_id"],
                    "phase": record["phase"],
                    "op": a["op"],
                    "kind": a["kind"],
                    "node": a["node_id"],
                    "state": record["state"],
                    "error": record.get("error"),
                    "t_decision_s": rel("t_decision"),
                    "t_sent_s": rel("t_sent"),
                    "t_applied_s": rel("t_applied"),
                    "t_confirmed_s": rel("t_confirmed"),
                    "t_released_s": rel("t_released"),
                }
            )
    return row, actions


def _write_csv(
    path: Path, rows: list[dict[str, Any]], first: Iterable[str] = ()
) -> None:
    columns = list(first)
    for row in rows:
        columns += [c for c in row if c not in columns]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, restval="")
        writer.writeheader()
        writer.writerows(rows)


def score(out: Path) -> list[dict[str, Any]]:
    rows, actions = [], []
    # Failed testbed attempts (<run_id>.infraN) are kept for inspection only.
    for run_dir in sorted((out / "runs").glob("*/run.json")):
        if ".infra" in run_dir.parent.name:
            continue
        row, acts = score_run(run_dir.parent)
        rows.append(row)
        actions += acts
    _write_csv(out / "runs.csv", rows, IDENTITY)
    _write_csv(out / "actions.csv", actions)
    return rows


# ---------------------------------------------------------------------------
# Summaries, paired comparisons and figures (pandas / matplotlib)
# ---------------------------------------------------------------------------
CELL = [
    "mode",
    "profile",
    "phase1_include_cut",
    "rogue_rate",
    "alert",
    "root_mode",
    "latency",
    "latency_regime",
    "control_latency_ms",
]
PAIR = [
    "instance",
    "profile",
    "phase1_include_cut",
    "rogue_rate",
    "alert",
    "root_mode",
    "latency",
    "rep",
]


def bootstrap_ci(values: Any, *, seed: int = 0) -> tuple[float, float]:
    import numpy as np

    x = np.asarray([v for v in values if v == v], dtype=float)
    if len(x) < 2:
        return math.nan, math.nan
    rng = np.random.default_rng(seed)
    means = x[rng.integers(0, len(x), size=(BOOTSTRAP, len(x)))].mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def mcnemar_exact(b: int, c: int) -> float:
    """Two-sided exact p-value on b vs c discordant pairs."""
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    tail = sum(math.comb(n, i) for i in range(k + 1)) / 2**n
    return min(1.0, 2 * tail)


def sign_flip_p(differences: Any, *, seed: int = 0, draws: int = 20000) -> float:
    import numpy as np

    d = np.asarray([v for v in differences if v == v], dtype=float)
    if len(d) == 0 or not np.any(d):
        return 1.0
    observed = abs(d.mean())
    if len(d) <= 16:
        signs = np.array(np.meshgrid(*[[-1, 1]] * len(d))).reshape(len(d), -1).T
    else:
        signs = np.random.default_rng(seed).choice([-1, 1], size=(draws, len(d)))
    return float((np.abs((signs * d).mean(axis=1)) >= observed - 1e-12).mean())


def _column(g: Any, name: str) -> Any:
    """A numeric column, all NaN when this kind of row does not have it
    (simulator rows carry no monitor timing, runtime rows no simulator codes)."""
    import pandas as pd

    if name not in g:
        return pd.Series(math.nan, index=g.index)
    return pd.to_numeric(g[name], errors="coerce")


def _flags(g: Any, name: str) -> int:
    return int(_column(g, name).fillna(0).astype(bool).sum())


def summarize(runs: Any) -> Any:
    import pandas as pd

    rows = []
    faulted = runs[runs.strategy_code != "reference"]
    for column in CELL:
        if column not in faulted:
            faulted = faulted.assign(**{column: math.nan})
    for keys, g in faulted.groupby(CELL + ["strategy_code", "strategy"], dropna=False):
        clean = _column(g, "clean_after_alert") * 100
        established = _column(g, "established") * 100
        kept = _column(g, "kept_cfrac") * 100
        rows.append(
            {
                **dict(zip(CELL + ["strategy_code", "strategy"], keys)),
                "runs": len(g),
                # (a) no tainted effect committed after the alert
                "clean_after_alert": clean.mean(),
                "clean_after_alert_lo": bootstrap_ci(clean)[0],
                "clean_after_alert_hi": bootstrap_ci(clean)[1],
                # (b) tainted effects after containment was established
                "tainted_after_established": _column(
                    g, "tainted_after_established"
                ).mean(),
                "tainted_after_established_runs": int(
                    (_column(g, "tainted_after_established") > 0).sum()
                ),
                # (c) containment established, and how fast
                "established": established.mean(),
                "established_lo": bootstrap_ci(established)[0],
                "established_hi": bootstrap_ci(established)[1],
                "time_to_established_s_median": _column(
                    g, "time_to_established_s"
                ).median(),
                # (d) never excluded, only counted
                "unconfirmed_runs": _flags(g, "unconfirmed"),
                "control_failed_runs": _flags(g, "control_failed"),
                "infeasible_runs": _flags(g, "infeasible"),
                "timeouts": _flags(g, "timeout"),
                "kept": kept.mean(),
                "kept_lo": bootstrap_ci(kept)[0],
                "kept_hi": bootstrap_ci(kept)[1],
                "kept_runs": int(kept.notna().sum()),
                "violations": _column(g, "violations").mean(),
                "decision_s_median": _column(g, "decision_s").median(),
                "solve_ms_median": _column(g, "solve_ms").median(),
                "logical_actions": _column(g, "n_logical_actions").mean(),
                "control_messages": _column(g, "n_control_messages").mean(),
                "confirmations": _column(g, "n_confirmations").mean(),
                "root_correct": _column(g, "root_correct").mean() * 100,
                "root_rank_median": _column(g, "root_rank").median(),
                "collateral": _column(g, "n_collateral").mean(),
                "interrupted": _column(g, "n_interrupted").mean(),
                "kept_gpu_s": _column(g, "kept_gpu_s").mean(),
                "kept_tokens": _column(g, "kept_tokens").mean(),
                "slot_restarts": int(_column(g, "slot_restarts").fillna(0).sum()),
                "testbed_failures": int(
                    (g["testbed_failure"].fillna("") != "").sum()
                    if "testbed_failure" in g
                    else 0
                ),
            }
        )
    return pd.DataFrame(rows)


def _mcnemar(j: Any, column: str) -> dict[str, Any]:
    ours, other = j[f"{column}_fb"].astype(float), j[f"{column}_other"].astype(float)
    b = int(((ours == 1) & (other == 0)).sum())
    c = int(((ours == 0) & (other == 1)).sum())
    d = (ours - other) * 100
    return {
        f"d_{column}": d.mean(),
        f"d_{column}_lo": bootstrap_ci(d)[0],
        f"d_{column}_hi": bootstrap_ci(d)[1],
        f"only_firebreak_{column}": b,
        f"only_other_{column}": c,
        f"mcnemar_p_{column}": mcnemar_exact(b, c),
    }


def paired(runs: Any, reference: str = "firebreak") -> Any:
    import pandas as pd

    rows = []
    runs = runs.copy()
    for column in PAIR:
        if column not in runs:
            runs[column] = math.nan
    base = runs[runs.strategy_code == reference].set_index(PAIR)
    for code, g in runs[~runs.strategy_code.isin([reference, "reference"])].groupby(
        "strategy_code"
    ):
        other = g.set_index(PAIR)
        joined = base.join(other, lsuffix="_fb", rsuffix="_other", how="inner")
        if joined.empty:
            continue
        cells = [
            "profile",
            "phase1_include_cut",
            "rogue_rate",
            "alert",
            "root_mode",
            "latency",
        ]
        for cell, j in joined.groupby(cells, dropna=False):
            dk = (
                j.kept_cfrac_fb.astype(float) - j.kept_cfrac_other.astype(float)
            ) * 100
            rows.append(
                {
                    **dict(zip(cells, cell)),
                    "against": CODE_NAME.get(code, code),
                    "pairs": len(j),
                    **_mcnemar(j, "clean_after_alert"),
                    **_mcnemar(j, "established"),
                    "d_kept": dk.mean(),
                    "d_kept_lo": bootstrap_ci(dk)[0],
                    "d_kept_hi": bootstrap_ci(dk)[1],
                    "kept_sign_flip_p": sign_flip_p(dk),
                }
            )
    return pd.DataFrame(rows)


def sim_vs_runtime(runs: Any, sim: Any) -> Any:
    keys = ["instance", "rogue_rate", "alert", "latency", "strategy"]
    rt = (
        runs[runs.strategy_code != "reference"]
        .groupby(keys, dropna=False)
        .agg(
            runtime_runs=("clean_after_alert", "size"),
            runtime_clean_after_alert=("clean_after_alert", "mean"),
            runtime_kept=("kept_cfrac", "mean"),
        )
    )
    sm = sim.groupby(keys, dropna=False).agg(
        sim_clean_after_alert=("clean_after_alert", "mean"),
        sim_kept=("kept_cfrac", "mean"),
    )
    out = rt.join(sm, how="left").reset_index()
    out["d_clean_after_alert"] = (
        out.runtime_clean_after_alert - out.sim_clean_after_alert
    ) * 100
    out["d_kept"] = (out.runtime_kept - out.sim_kept) * 100
    return out


def _setup_style():  # the simulator's v2 publication style
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.labelsize": 9.5,
            "axes.titlesize": 9.5,
            "axes.titlelocation": "left",
            "axes.edgecolor": "#9a9a9a",
            "axes.grid": True,
            "grid.color": "#e4e4e4",
            "grid.linewidth": 0.7,
            "axes.axisbelow": True,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
            "legend.fontsize": 8.5,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    return plt


def _marker(name: str, ms: float | None = None) -> dict[str, Any]:
    big = name == "Firebreak (ours)"
    return dict(
        color=COLOR[name],
        marker=MARKER[name],
        ms=ms or (13 if big else 7.5),
        mec="#222222",
        mew=0.6,
        # Smaller markers on top of the large star, so coinciding points stay
        # visible (as in the simulator's figures).
        zorder=3 if big else 6,
    )


def _panel_point(
    g: Any,
) -> tuple[float, float, tuple[float, float], tuple[float, float]]:
    """Mean kept / clean-after-alert (%) over a panel's runs, with bootstrap CIs."""
    kept = g.kept_cfrac.astype(float) * 100
    clean = g.clean_after_alert.astype(float) * 100
    return kept.mean(), clean.mean(), bootstrap_ci(kept), bootstrap_ci(clean)


def _no_runs(ax: Any) -> None:
    ax.text(
        0.5,
        0.5,
        "no runs in this cell",
        transform=ax.transAxes,
        ha="center",
        va="center",
        color="#7a7a7a",
        fontsize=9,
    )


def _save(fig: Any, path: Path) -> None:
    import matplotlib.pyplot as plt

    fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(path.with_suffix(".png"), dpi=200, bbox_inches="tight")
    plt.close(fig)


METRICS = {
    "clean": ("clean_after_alert", "runs with no tainted effect\nafter the alert (%)"),
    "established": ("established", "runs with containment\nestablished (%)"),
    "kept": ("kept_cfrac", "normal work kept (%)"),
}
STRESS_NOTE = "stress settings, above the measured LAN round trip (1.5-4 ms)"


def _main_runs(runs: Any, mode: str) -> Any:
    """Runs of one mode in the main design: plotted strategies, root given, the
    deployed profile without the at-alert cut."""
    r = runs[(runs["mode"] == mode) & runs.strategy.isin(PLOTTED_NAMES)]
    if "profile" in r:
        r = r[r.profile.isna() | (r.profile == "deployed")]
    if "phase1_include_cut" in r:
        r = r[r.phase1_include_cut.isna() | (r.phase1_include_cut == 0)]
    if "root_mode" in r:
        r = r[r.root_mode.isna() | (r.root_mode == "given")]
    return r


def _is_stress(part: Any) -> bool:
    return "latency_regime" in part and (part.latency_regime == "stress").any()


def fig_pareto(runs: Any, path: Path, mode: str) -> None:
    """(a) runs with no tainted effect after the alert vs normal work kept,
    pooled over each panel's runs: rogue rate 0 / > 0 (rows) by the lowest /
    highest latency setting (columns); stress settings say so in the title."""
    plt = _setup_style()
    r = _main_runs(runs, mode)
    if r.empty:
        return
    by_latency = r.groupby("latency").control_latency_ms.mean().sort_values()
    columns = list(dict.fromkeys([by_latency.index[0], by_latency.index[-1]]))
    rows = [(False, "Well-behaved (rogue rate 0)"), (True, "Rogue rate > 0")]
    fig, axes = plt.subplots(
        2,
        len(columns),
        figsize=(3.7 * len(columns), 6.8),
        sharex=True,
        sharey=True,
        squeeze=False,
    )
    for i, (rogue, label) in enumerate(rows):
        for j, latency in enumerate(columns):
            ax = axes[i, j]
            part = r[
                (r.latency == latency) & ((r.rogue_rate.astype(float) > 0) == rogue)
            ]
            if part.empty:
                _no_runs(ax)
            for name in PLOTTED_NAMES:
                g = part[part.strategy == name]
                if g.empty:
                    continue
                x, y, (xlo, xhi), (ylo, yhi) = _panel_point(g)
                if not any(math.isnan(v) for v in (xlo, xhi, ylo, yhi)):
                    ax.errorbar(
                        x,
                        y,
                        xerr=[[x - xlo], [xhi - x]],
                        yerr=[[y - ylo], [yhi - y]],
                        fmt="none",
                        ecolor=COLOR[name],
                        elinewidth=0.8,
                    )
                ax.plot(x, y, ls="", **_marker(name))
            ax.plot(
                [100],
                [100],
                marker=(5, 1, 0),
                ms=7,
                color="none",
                mec="#7a7a7a",
                mew=0.9,
            )
            stress = " (stress)" if _is_stress(r[r.latency == latency]) else ""
            ax.set_title(f"{label}\nlatency {latency}{stress}", fontsize=9)
            ax.set_xlim(-3, 106)
            ax.set_ylim(-3, 106)
            if i == 1:
                ax.set_xlabel("normal work kept (%)")
            if j == 0:
                ax.set_ylabel(METRICS["clean"][1])
    present = [n for n in PLOTTED_NAMES if (r.strategy == n).any()]
    handles = [
        plt.Line2D(
            [], [], ls="", label=n, **_marker(n, 11 if n == "Firebreak (ours)" else 7)
        )
        for n in present
    ]
    fig.legend(handles=handles, loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.08))
    fig.tight_layout()
    _save(fig, path)


def fig_latency(runs: Any, path: Path, mode: str, metric: str = "clean") -> None:
    """A split outcome (or kept work) vs the control latency, one line per
    strategy (rogue 0 / > 0); settings above the measured LAN round trip are
    shaded and labelled stress."""
    plt = _setup_style()
    r = _main_runs(runs, mode)
    column, ylabel = METRICS[metric]
    if r.empty or column not in r or r[column].isna().all():
        return
    fig, axes = plt.subplots(1, 2, figsize=(7.4, 3.3), sharey=True)
    panels = [(False, "Well-behaved (rogue rate 0)"), (True, "Rogue rate > 0")]
    stress = r[r.latency_regime == "stress"] if "latency_regime" in r else r.iloc[:0]
    for ax, (rogue, label) in zip(axes, panels):
        part = r[(r.rogue_rate.astype(float) > 0) == rogue]
        if part.empty:
            _no_runs(ax)
        if not stress.empty:
            lo = (
                r[r.latency_regime != "stress"].control_latency_ms.max()
                if (r.latency_regime != "stress").any()
                else 0.0
            )
            hi = stress.control_latency_ms.max()
            ax.axvspan(lo / 1000, hi / 1000 * 1.05, color="#f2f2f2", zorder=0, lw=0)
        for name in PLOTTED_NAMES:
            g = part[part.strategy == name]
            if g.empty:
                continue
            line = g.groupby("control_latency_ms")[column].apply(
                lambda x: x.astype(float).mean() * 100
            )
            big = name == "Firebreak (ours)"
            ax.plot(
                line.index / 1000,
                line.values,
                color=COLOR[name],
                lw=2.6 if big else 1.4,
                ls=(0, (4, 2)) if FAMILY[name] == "no lineage" else "-",
                marker=MARKER[name],
                ms=10 if big else 5.5,
                mec="#222222",
                mew=0.5,
                label=name,
            )
        ax.set_title(label, fontsize=9)
        ax.set_xlabel("control latency (s)")
        ax.set_ylim(-3, 104)
    axes[0].set_ylabel(ylabel)
    legend: dict[str, Any] = {}
    for ax in axes:  # either panel may be empty
        for handle, name in zip(*ax.get_legend_handles_labels()):
            legend.setdefault(name, handle)
    names = [n for n in PLOTTED_NAMES if n in legend]
    fig.legend(
        [legend[n] for n in names],
        names,
        loc="lower center",
        ncol=4,
        bbox_to_anchor=(0.5, -0.17),
    )
    fig.tight_layout()
    if not stress.empty:
        fig.text(
            0.5,
            -0.2,
            f"Shaded: {STRESS_NOTE}.",
            ha="center",
            va="top",
            fontsize=8,
            color="#7a7a7a",
        )
    _save(fig, path)


def report(out: Path, *, figures: bool = True) -> None:
    import pandas as pd

    if not (out / "runs.csv").exists():
        score(out)
    runs = pd.read_csv(out / "runs.csv")
    summary = summarize(runs)
    paired(runs).to_csv(out / "paired.csv", index=False)
    if (out / "sim_runs.csv").exists():
        sim = pd.read_csv(out / "sim_runs.csv")
        sim_vs_runtime(runs, sim).to_csv(out / "sim_vs_runtime.csv", index=False)
        summary = pd.concat([summary, summarize(sim)], ignore_index=True)
        runs = pd.concat([runs, sim], ignore_index=True)
    summary.to_csv(out / "summary.csv", index=False)
    if figures:
        figures_dir = out / "figures"
        figures_dir.mkdir(exist_ok=True)
        for mode in runs["mode"].dropna().unique():
            fig_pareto(runs, figures_dir / f"fig_pareto_{mode}", mode)
            fig_latency(runs, figures_dir / f"fig_clean_vs_latency_{mode}", mode)
            fig_latency(
                runs,
                figures_dir / f"fig_established_vs_latency_{mode}",
                mode,
                "established",
            )
            fig_latency(
                runs, figures_dir / f"figA_kept_vs_latency_{mode}", mode, "kept"
            )
