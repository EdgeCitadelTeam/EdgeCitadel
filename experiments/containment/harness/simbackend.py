"""The experiment grid in the offline simulator, for sim-to-real comparison.

Each runtime cell (instance, rogue rate, alert, latency, strategy) is run once
in the simulator (it is deterministic, so repetitions add nothing) with the
simulator's pipeline of the same strategy (``names.SIM_CODE``). Runtime
seconds are divided by the experiment's time scale, so a runtime control
latency of 2 s at time scale 2 is 1 s in the simulator. The simulator models
content faults only (wrong value, stale version); other fault classes are
skipped. Rows use ``runs.csv``'s column names with ``mode = sim``.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from .deploy import nodes
from .names import CODE_NAME, FAMILY, SIM_CODE
from .runner import Experiment, cell_spec, grid
from .simexport import import_contain, to_sim

SIM_FAULTS = frozenset({"wrong_value", "stale_version"})
TAU_S = 0.05

COLUMNS = {
    # The simulator has no confirmations: its "contained" is outcome (a).
    "clean_after_alert": "contained",
    "kept_cfrac": "retained_cfrac",
    "kept_frac": "retained_frac",
    "violations": "violations",
    "viol_declared": "viol_declared",
    "viol_undeclared": "viol_undeclared",
    "n_normal": "n_normal",
    "n_at_risk": "n_at_risk",
    "n_collateral": "collateral",
    "n_contaminated": "contaminated",
    "lost_compute_s": "lost_compute",
    "n_actions": "n_actions",
    "solve_ms": "solve_ms_first",
    "gc_nodes": "gc_nodes",
    "gc_edges": "gc_edges",
    "gp_nodes": "gp_nodes",
    "gp_edges": "gp_edges",
    "n_spawned": "n_spawned",
    "unavoidable": "unavoidable",
}


def run_grid(
    experiment: Experiment, out: Path, *, sim_root: str | None = None
) -> list[dict[str, Any]]:
    import_contain(sim_root)
    from contain.receding import registry, run_case2

    pipelines = registry()
    blocks: dict[tuple, list] = defaultdict(list)
    for cell in grid(experiment):
        if cell.strategy == "reference" or cell.rep != 0:
            continue
        if cell.strategy not in SIM_CODE:
            continue  # a runtime-only strategy (names.EXTRA)
        if cell.root_mode != "given":
            continue  # the simulator always starts from the known source
        blocks[(cell.instance, cell.rogue_rate, cell.alert, cell.latency.name)].append(
            cell
        )
    rows = []
    for cells in blocks.values():
        spec = cell_spec(cells[0])
        if spec.fault.cls not in SIM_FAULTS:
            continue
        latency_ms = cells[0].latency.per_node(nodes(spec))
        mean_ms = sum(latency_ms.values()) / len(latency_ms)
        wf, fault, over = to_sim(
            spec,
            cells[0].alert,
            control_latency=mean_ms / 1000 / experiment.time_scale,
        )
        chosen = [pipelines[SIM_CODE[c.strategy]] for c in cells]
        for cell, result in zip(cells, run_case2(wf, fault, chosen, TAU_S, over)):
            name = CODE_NAME[cell.strategy]
            row = {
                "run_id": f"sim__{cell.run_id}",
                "experiment": experiment.name,
                "mode": "sim",
                "instance": cell.instance.stem,
                "spec_name": spec.name,
                "fault": spec.fault.cls,
                "rogue_rate": spec.rogue.rate,
                "strategy_code": cell.strategy,
                "strategy": name,
                "family": FAMILY.get(name, ""),
                "alert": cell.alert,
                "root_mode": cell.root_mode,
                "latency": cell.latency.name,
                "latency_regime": cell.latency.regime(experiment.measured_rtt_ms),
                "control_latency_ms": mean_ms,
                "rep": 0,
                "time_scale": experiment.time_scale,
                "status": "simulated",
                "timeout": 0,
                "t_alert_s": fault.t_alert,
                "sim_code": result["code"],
            }
            row.update({ours: result.get(theirs) for ours, theirs in COLUMNS.items()})
            rows.append(row)
    if rows:
        with (out / "sim_runs.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    return rows
