"""The scaled testbed: one workflow on S agentd sites (``python -m harness.scale``).

Few physical hosts, many software sites: the same real code (agentd, the
monitor, the det-workers) with more agentd nodes. Core, NATS, the services and
the monitor run on jim-eq with the root's site (node ``core``); every other
site is an agentd node on NetSys-Endurance (``edge-1`` .. ``edge-<S-1>``). The
stack starts only the nodes a workload's placement uses, so one stack file
(``configs/lab-scale.json``, up to 32 sites) serves every S, and S is the
workload's.

Workloads (``workloads``): random workflows from the simulator's generator
(``random_workflow`` builds complete binary trees, so ``n_agents`` is 2^d-1:
63 is the size nearest 50-60), compiled like rand-n7-sY (p_share 0.5, sigma
0.5, consumed 0.5, the simulator's fault and the exported early/late
alerts), one file per seed and S (``rand-n63-s<seed>-S<S>.json``): identical
tasks, fault and alerts, only the placement differs. Agents are placed by team:
the agent tree is cut into S connected groups of near-equal size, each a
subtree minus the subtrees split off below it (``team_groups``), so a site
hosts a realistic group of a team lead and its reports; the root's group is on
``core``.

Stack (``stack``): the lab file with the scale testbed's own ports, unit
prefix (``firebreak-exp-s-``) and work roots, so it never collides with the
evaluation stack, and process sampling on (``procstat_interval_ms``).

Grids: ``experiments/scale.json`` (72 runs, about 5.7 h: the S = 32
comparison and rogue cell on rand-n63-s0, and the scale curve at S = 3, 8, 16)
and the optional ``experiments/scale-extra.json`` (27 runs, about 2.3 h: the
S = 32 comparison on rand-n63-s2), both into one output root. Run them on
jim-eq after the evaluation pipeline, never beside it (shared CPUs and disks
would confound both), as their own runner unit::

  python -m harness unit --name firebreak-exp-s-runner --log <root>/scale.log \
      -- run --stack <lab-scale copy> --experiment experiments/scale.json \
      --out <root>/scale
  python -m harness score --out <root>/scale

Simulator hook (``sim-export``): per workload, its site assignment (agent and
simulator site -> node -> host) and a manifest naming the spec, for the
simulator, which rebuilds the same instance with ``harness.simexport.to_sim``
(``workloads/scale/sim/``).
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from .spec import Spec, derive, load, makespan, save, validate

SITES = (3, 8, 16, 32)
SEEDS = (0, 2, 3)
N_AGENTS = 63
# random_workflow / random_fault parameters of every rand-nN-sY workload.
GENERATOR = {"p_share": 0.5, "sigma": 0.5, "consumed": 0.5}
# Seeds kept: in increasing order, those whose late alert comes at least this
# long after the early one, so the early/late distinction survives fast
# hand-offs (stated before generating; seeds 0..11 were looked at).
MIN_ALERT_GAP_S = 3.0
ROOT_NODE = "core"
ROOT_HOST = "jim-eq"
EDGE_HOST = "netsys-endurance"
UNIT_PREFIX = "firebreak-exp-s-"
# Ports of the scale stack: off production (4222, 8000, 8123), the evaluation
# stack (14322, 18322, 18900, 18800), A2A (19300-19599) and LangGraph (19500).
PORTS = {"nats": 24322, "nats_monitor": 28322, "services": 28900, "aggregator": 28800}
AVOID_PORTS = frozenset({4222, 8000, 8123, 14322, 18322, 18900, 18800, 19500})
AVOID_RANGES = ((19300, 19599),)


def node_names(sites: int) -> list[str]:
    """``core`` and ``edge-1`` .. ``edge-<sites-1>``."""
    if sites < 1:
        raise ValueError("at least one site")
    return [ROOT_NODE, *(f"edge-{i}" for i in range(1, sites))]


def team_groups(spec: Spec, sites: int) -> list[list[str]]:
    """The agent tree cut into ``sites`` connected groups of near-equal size.

    Start from one group (the whole tree); while there are fewer groups than
    sites, split the largest group that can be split: cut off the subtree
    (within that group) whose size is closest to half the group's. Each group
    is then a subtree minus the subtrees split off below it, rooted at its own
    team lead. The root's group comes first, the others in the order of their
    leads in a breadth-first walk of the tree. Deterministic."""
    children: dict[str, list[str]] = {a.id: [] for a in spec.agents}
    root = None
    for agent in spec.agents:
        if agent.parent is None:
            root = agent.id
        else:
            children[agent.parent].append(agent.id)
    assert root is not None
    for kids in children.values():
        kids.sort()
    if sites > len(children):
        raise ValueError(f"{sites} sites for {len(children)} agents")
    group_of = {agent: root for agent in children}  # agent -> its group's lead

    def members(lead: str) -> list[str]:
        out, stack = [], [lead]
        while stack:
            node = stack.pop()
            out.append(node)
            stack.extend(c for c in children[node] if group_of[c] == lead)
        return out

    leads = [root]
    while len(leads) < sites:
        best = None
        for lead in leads:
            group = members(lead)
            if len(group) < 2:
                continue
            size: dict[str, int] = {}
            for node in reversed(group):  # children before parents
                size[node] = 1 + sum(
                    size[c] for c in children[node] if group_of[c] == lead
                )
            half = len(group) / 2
            cut = min(
                (n for n in group if n != lead),
                key=lambda n: (abs(size[n] - half), n),
            )
            key = (len(group), -abs(size[cut] - half))
            if best is None or key > best[0]:
                best = (key, lead, cut)
        if best is None:
            break
        _, lead, cut = best
        moved, stack = [], [cut]
        while stack:
            node = stack.pop()
            moved.append(node)
            stack.extend(c for c in children[node] if group_of[c] == lead)
        for node in moved:
            group_of[node] = cut
        leads.append(cut)
    order, queue = [], [root]
    while queue:
        node = queue.pop(0)
        order.append(node)
        queue.extend(children[node])
    rank = {agent: i for i, agent in enumerate(order)}
    leads.sort(key=lambda lead: (lead != root, rank[lead]))
    return [sorted(members(lead), key=rank.__getitem__) for lead in leads]


def placed(spec: Spec, sites: int, name: str | None = None) -> Spec:
    """``spec`` with its agents placed on ``sites`` nodes by team."""
    nodes = node_names(sites)
    groups = team_groups(spec, sites)
    site_of = {a.id: a.site for a in spec.agents}
    placement: dict[str, str] = {}
    for node, group in zip(nodes, groups):
        for agent in group:
            placement[site_of[agent]] = node
    for cls in spec.effect_classes:
        placement.setdefault(cls.site, ROOT_NODE)
    out = replace(spec, placement=placement, name=name or spec.name)
    validate(out)
    return out


def generate(
    sim_root: str | Path | None,
    *,
    seeds: Iterable[int] = SEEDS,
    sites: Iterable[int] = SITES,
    n_agents: int = N_AGENTS,
) -> dict[str, Spec]:
    """Every workload: ``rand-n<n>-s<seed>-S<S>`` -> spec."""
    from . import simexport

    out = {}
    for seed in seeds:
        base = simexport.scenario(
            "random", sim_root=sim_root, n_agents=n_agents, seed=seed, **GENERATOR
        )
        for s in sites:
            name = f"rand-n{n_agents}-s{seed}-S{s}"
            spec = placed(base, s, name=name)
            out[name] = replace(
                spec,
                source={
                    **dict(spec.source or {}),
                    "scale": {"sites": s, "groups": "team_groups", "base": base.name},
                },
            )
    return out


def alert_gap(spec: Spec) -> float:
    from . import simexport

    return simexport.alert_time(spec, spec.alerts["late"]) - simexport.alert_time(
        spec, spec.alerts["early"]
    )


def stack_document(
    sites: int = 32, *, dryrun: bool = False, procstat_ms: int = 1000
) -> dict[str, Any]:
    """The scale stack file: ``core`` on jim-eq, the edges on Endurance (a dry
    run: everything on this machine)."""
    nodes = node_names(sites)
    runtime_env = {
        "EDGECITADEL_TRANSPORT_POLL_MS": "1000",
        "EDGECITADEL_CLAIM_POLL_MS": "1000",
    }
    if dryrun:
        return {
            "mode": "dryrun",
            "controller": "local",
            "_note": f"scale testbed dry run: {sites} agentd sites on this machine",
            "hosts": {
                "local": {
                    "address": "127.0.0.1",
                    "python": "${HARNESS_PYTHON}",
                    "repo": "",
                    "runtime_root": "${HARNESS_RUNTIME_ROOT}",
                    "work_root": "${HARNESS_WORK_ROOT}",
                    "nats_server": "${HARNESS_NATS_SERVER}",
                }
            },
            "node_hosts": {node: "local" for node in nodes},
            "tau_ms": 50,
            "control_transport": "nats",
            "effect_sink": {"kind": "ledger"},
            "controller_lease_ms": 30000,
            "procstat_interval_ms": procstat_ms,
        }
    return {
        "mode": "lab",
        "controller": ROOT_HOST,
        "control_transport": "nats",
        "_note": (
            f"scale testbed: up to {sites} agentd sites; core (the root's team) "
            "with Core, NATS, the services and the monitor on jim-eq, every "
            "other site on NetSys-Endurance. Own ports, unit prefix and work "
            "roots, so it never collides with the evaluation stack; do not run "
            "both at once (shared CPUs would confound both measurements)."
        ),
        "hosts": {
            ROOT_HOST: {
                "address": "${HARNESS_JIM_ADDRESS}",
                "listen": "${HARNESS_CONTROLLER_LISTEN}",
                "python": "${HARNESS_PYTHON}",
                "repo": "${HARNESS_REPO}",
                "runtime_root": "${HARNESS_RUNTIME_ROOT}",
                "work_root": "${HARNESS_WORK_ROOT}",
                "nats_server": "${HARNESS_NATS_SERVER}",
            },
            EDGE_HOST: {
                "ssh": "${HARNESS_SSH_ENDURANCE}",
                "units": "user",
                "unit_prefix": UNIT_PREFIX,
                "address": "${HARNESS_ENDURANCE_ADDRESS}",
                "python": "${HARNESS_REMOTE_PYTHON}",
                "repo": "${HARNESS_REMOTE_REPO}",
                "runtime_root": "${HARNESS_REMOTE_RUNTIME_ROOT}",
                "work_root": "${HARNESS_REMOTE_WORK_ROOT}",
            },
        },
        "node_hosts": {
            node: ROOT_HOST if node == ROOT_NODE else EDGE_HOST for node in nodes
        },
        "ports": dict(PORTS),
        "tau_ms": 50,
        "effect_sink": {"kind": "ledger"},
        "controller_lease_ms": 30000,
        "runtime_env": runtime_env,
        "procstat_interval_ms": procstat_ms,
    }


def check_ports(ports: Mapping[str, int]) -> None:
    for name, port in ports.items():
        if port in AVOID_PORTS or any(lo <= port <= hi for lo, hi in AVOID_RANGES):
            raise ValueError(f"port {name}={port} is reserved")
    if len(set(ports.values())) != len(ports):
        raise ValueError("ports must differ")


def site_assignment(spec: Spec, stack: Mapping[str, Any]) -> dict[str, Any]:
    """Where every agent runs: its simulator site and label, node and host."""
    node_hosts = stack["node_hosts"]
    agents = {}
    for agent in spec.agents:
        node = spec.placement[agent.site]
        agents[agent.id] = {
            "sim_id": agent.label,
            "sim_site": agent.site,
            "node": node,
            "host": node_hosts.get(node),
        }
    nodes: dict[str, list[str]] = {}
    for agent_id, entry in agents.items():
        nodes.setdefault(entry["node"], []).append(agent_id)
    return {
        "sites": len(nodes),
        "agents": agents,
        "nodes": {
            node: {"host": node_hosts.get(node), "agents": sorted(ids)}
            for node, ids in sorted(nodes.items())
        },
    }


def sim_export(
    paths: Sequence[Path], stack: Mapping[str, Any], out: Path
) -> dict[str, Any]:
    """Write, per workload file, its site assignment for the simulator and a
    manifest naming the spec (relative to ``out``); returns the manifest."""
    import os

    from .runner import MEASURED_CONTROL_MS

    latency = int(MEASURED_CONTROL_MS)
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "note": (
            "Identical instances for the simulator: rebuild each with "
            "harness.simexport.to_sim(harness.spec.load(spec)) (control latency "
            f"L = {latency} ms at every site, as the native lab regime) and "
            "place its agents as in its sites file (agent and simulator site -> "
            "node -> host). The generator's parameters are in each spec's source."
        ),
        "control_latency_ms": latency,
        "instances": {},
    }
    for path in sorted(paths):
        spec = load(path)
        name = path.stem
        assignment = site_assignment(spec, stack)
        (out / f"{name}.sites.json").write_text(json.dumps(assignment, indent=1) + "\n")
        manifest["instances"][name] = {
            "spec": os.path.relpath(path.resolve(), out.resolve()),
            "sites_file": f"{name}.sites.json",
            "sites": assignment["sites"],
            "source": spec.source,
            "tasks": len(spec.tasks),
            "agents": len(spec.agents),
            "nominal_makespan_s": round(makespan(spec), 3),
        }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def summary(spec: Spec) -> dict[str, Any]:
    groups: dict[str, int] = {}
    for agent in spec.agents:
        node = spec.placement[agent.site]
        groups[node] = groups.get(node, 0) + 1
    derived = derive(spec)
    return {
        "name": spec.name,
        "agents": len(spec.agents),
        "tasks": len(spec.tasks),
        "effects": len(spec.effects),
        "deliveries": len(derived.deliveries),
        "sites": len(groups),
        "agents_per_site": sorted(groups.values(), reverse=True),
        "nominal_makespan_s": round(makespan(spec), 2),
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.scale", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("workloads")
    p.add_argument("--sim-root")
    p.add_argument("--out", type=Path, default=Path("workloads/scale"))
    p.add_argument("--seeds", type=int, nargs="+", default=list(SEEDS))
    p.add_argument("--sites", type=int, nargs="+", default=list(SITES))
    p.add_argument("--n-agents", type=int, default=N_AGENTS)
    p = sub.add_parser("stack")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--sites", type=int, default=32)
    p.add_argument("--dryrun", action="store_true")
    p = sub.add_parser("sim-export")
    p.add_argument("--stack", type=Path, default=Path("configs/lab-scale.json"))
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("specs", type=Path, nargs="+")
    args = parser.parse_args(argv)
    if args.command == "workloads":
        specs = generate(
            args.sim_root, seeds=args.seeds, sites=args.sites, n_agents=args.n_agents
        )
        args.out.mkdir(parents=True, exist_ok=True)
        for name, spec in specs.items():
            save(spec, args.out / f"{name}.json")
            print(
                json.dumps({**summary(spec), "alert_gap_s": round(alert_gap(spec), 2)})
            )
    elif args.command == "stack":
        document = stack_document(args.sites, dryrun=args.dryrun)
        if "ports" in document:
            check_ports(document["ports"])
        args.out.write_text(json.dumps(document, indent=2) + "\n")
        print(json.dumps({"out": str(args.out), "nodes": len(document["node_hosts"])}))
    else:
        stack = json.loads(args.stack.read_text())
        manifest = sim_export(args.specs, stack, args.out)
        print(
            json.dumps({"out": str(args.out), "instances": len(manifest["instances"])})
        )


if __name__ == "__main__":
    main()
