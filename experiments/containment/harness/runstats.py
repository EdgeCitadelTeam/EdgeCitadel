"""Control-plane and resource columns of one run (the scale testbed's metrics).

Beside the scorer's containment, kept-work and decision columns (``metrics``:
``t_received_s``, ``decision_s``, ``solve_ms``, ``tau1_s``, command and
confirmation counts, G_C and G_P sizes), per run:

- control latency: every control command's sent-to-confirmed time (monitor
  clock at both ends) per site and overall (``confirm_ms_*``), and the fence
  lag, from a site's confirmation to the monitor having ingested that site's
  journal through the confirmed position (``fence_lag_ms_*``);
- Core's ingest lag (``ingest_lag_ms_*``), from ``core_ingest.json`` (the
  runner's ``coreq ingest`` at collection): Core's receive time minus each
  event's occurrence on its node, two clocks assumed synchronized;
- CPU-seconds and peak resident memory over the run, from the process samples
  (``procstat__<host>.jsonl``, when the stack samples): Core with the monitor
  (one process, ``core_*``), agentd (all nodes, and the busiest node),
  NATS and the det-workers;
- ``n_sites``: the agentd nodes of the run.

Distributions are nearest-rank (``distribution``); a column is NaN when its
records are missing.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

NAN = float("nan")
# Seconds after the alert counted as the response window for CPU.
RESPONSE_WINDOW_S = 10.0


def quantile(values: Iterable[float], q: float) -> float:
    """Nearest-rank quantile (the maximum for q = 1)."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def distribution(values: Iterable[float]) -> dict[str, float]:
    """``n``, p50, p95, p99 and the maximum (empty: ``n`` 0)."""
    values = [float(v) for v in values]
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        **{
            name: round(quantile(values, q), 1)
            for name, q in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99))
        },
        "max": round(max(values), 1),
    }


def _columns(prefix: str, summary: Mapping[str, float]) -> dict[str, float]:
    return {f"{prefix}_{key}": summary.get(key, NAN) for key in ("p50", "p95", "max")}


def fence_lags(decisions: Iterable[Mapping[str, Any]]) -> list[float]:
    """Per confirmed and fenced command (each once): its site's ingest of the
    confirmed journal position after the confirmation (ms, monitor clock)."""
    seen: dict[tuple[str, str], float] = {}
    for d in decisions:
        for record in [
            *d.get("phase1", ()),
            *d.get("phase2", ()),
            *d.get("released", ()),
        ]:
            for node, outcome in (record.get("sites") or {}).items():
                confirmed, ingested = (
                    outcome.get("t_confirmed"),
                    outcome.get("t_ingested"),
                )
                if confirmed is None or ingested is None:
                    continue
                key = (node, str(outcome.get("command_id") or confirmed))
                seen.setdefault(key, max(0.0, float(ingested) - float(confirmed)))
    return list(seen.values())


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def resource_columns(
    run_dir: Path, t0: float | None, t_end: float | None, t_alert: float | None
) -> dict[str, float]:
    """CPU-seconds and peak RSS (MB) per role over the run (and Core's and
    agentd's CPU in the response window after the alert)."""
    groups_cpu: dict[str, float] = {}
    groups_rss: dict[str, float] = {}
    response_cpu: dict[str, float] = {}
    found = False
    for path in sorted(run_dir.glob("procstat__*.jsonl")):
        samples = _jsonl(path)
        window = [
            s
            for s in samples
            if (t0 is None or s["t_ms"] >= t0) and (t_end is None or s["t_ms"] <= t_end)
        ] or samples
        if not window:
            continue
        found = True
        first, last = window[0]["groups"], window[-1]["groups"]
        for group, sample in last.items():
            before = first.get(group, {}).get("cpu_s", 0.0)
            groups_cpu[group] = max(0.0, sample["cpu_s"] - before)
            groups_rss[group] = (
                max(s["groups"].get(group, {}).get("rss_kb", 0) for s in window) / 1024
            )
        if t_alert is not None:
            after = [
                s
                for s in samples
                if t_alert <= s["t_ms"] <= t_alert + RESPONSE_WINDOW_S * 1000
            ]
            if len(after) >= 2:
                for group, sample in after[-1]["groups"].items():
                    before = after[0]["groups"].get(group, {}).get("cpu_s", 0.0)
                    response_cpu[group] = max(0.0, sample["cpu_s"] - before)
    if not found:
        return {
            key: NAN
            for key in (
                "core_cpu_s",
                "core_rss_peak_mb",
                "core_cpu_s_response",
                "agentd_cpu_s",
                "agentd_cpu_s_max",
                "agentd_cpu_s_response",
                "agentd_rss_peak_mb",
                "nats_cpu_s",
                "workers_cpu_s",
                "workers_rss_peak_mb",
            )
        }

    def total(values: Mapping[str, float], prefix: str) -> float:
        return round(sum(v for g, v in values.items() if g.startswith(prefix)), 3)

    agentd = [v for g, v in groups_cpu.items() if g.startswith("agentd-")]
    return {
        "core_cpu_s": total(groups_cpu, "aggregator"),
        "core_rss_peak_mb": round(groups_rss.get("aggregator", NAN), 1),
        "core_cpu_s_response": total(response_cpu, "aggregator")
        if response_cpu
        else NAN,
        "agentd_cpu_s": total(groups_cpu, "agentd-"),
        "agentd_cpu_s_max": round(max(agentd), 3) if agentd else NAN,
        "agentd_cpu_s_response": total(response_cpu, "agentd-")
        if response_cpu
        else NAN,
        # The sum of each agentd's peak (an upper bound of the joint peak).
        "agentd_rss_peak_mb": total(groups_rss, "agentd-"),
        "nats_cpu_s": total(groups_cpu, "nats"),
        "workers_cpu_s": total(groups_cpu, "worker-"),
        "workers_rss_peak_mb": total(groups_rss, "worker-"),
    }


def columns(
    run_dir: Path, meta: Mapping[str, Any], decisions: list[Mapping[str, Any]]
) -> dict[str, Any]:
    """Every column of this module for one scored run."""
    from .metrics import sent_to_confirmed

    per_site = sent_to_confirmed(decisions)
    confirm = distribution(v for values in per_site.values() for v in values)
    row: dict[str, Any] = {
        "n_sites": len(meta.get("control_latency_ms") or {}) or NAN,
        "n_confirm": confirm["n"],
        **_columns("confirm_ms", confirm),
        "confirm_ms_by_site": json.dumps(
            {site: distribution(v) for site, v in sorted(per_site.items())},
            sort_keys=True,
        ),
        **_columns("fence_lag_ms", distribution(fence_lags(decisions))),
    }
    ingest_path = run_dir / "core_ingest.json"
    ingest = json.loads(ingest_path.read_text()) if ingest_path.exists() else {}
    overall = ingest.get("all", {})
    row.update(
        {
            "ingest_lag_ms_p50": overall.get("p50", NAN),
            "ingest_lag_ms_p95": overall.get("p95", NAN),
            "ingest_lag_ms_p99": overall.get("p99", NAN),
            "ingest_lag_ms_max": overall.get("max", NAN),
            "ingest_lag_ms_by_node": json.dumps(
                ingest.get("by_node", {}), sort_keys=True
            ),
        }
    )
    row.update(
        resource_columns(
            run_dir, meta.get("t0_ms"), meta.get("t_end_ms"), meta.get("t_alert_ms")
        )
    )
    return row
