"""Command line of the containment experiment harness.

Run from ``experiments/containment`` (``python -m harness <command>``):

  export    simulator scenario -> workload spec (needs CONTAIN_SIM_ROOT)
  validate  check specs and print their derived plan, nominal makespan and alerts
  run       execute an experiment grid on a stack (dry run or lab)
  score     per-run metrics -> runs.csv and actions.csv
  sim       the same grid in the simulator -> sim_runs.csv (sim-to-real)
  report    summaries with bootstrap CIs, paired comparisons and figures
  netem     print (or apply/clear) a lab latency profile's tc commands
  unit      run one of these commands as a transient systemd unit (a runner
            that outlives the login session; its processes stop with it)
  pipeline  the lab chain: regression, svc-smoke, gate, lab-pilot, svc-pilot
  commit-cost  median durable SQLite commit (as agentd's) in each directory
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _export(args: argparse.Namespace) -> None:
    from . import simexport
    from .spec import Rogue, save

    params: dict = {}
    if args.scenario == "random":
        params = dict(
            n_agents=args.n_agents,
            seed=args.seed,
            p_share=args.p_share,
            sigma=args.sigma,
            consumed=args.consumed,
        )
    rogue = Rogue(rate=args.rogue_rate, seed=args.rogue_seed, p_spawn=args.p_spawn)
    spec = simexport.scenario(
        args.scenario,
        sim_root=args.sim_root,
        rogue=rogue,
        nodes=args.nodes.split(","),
        **params,
    )
    save(spec, args.out)
    print(
        f"{args.out}: {spec.name}, {len(spec.tasks)} tasks, alerts {sorted(spec.alerts)}"
    )


def _validate(args: argparse.Namespace) -> None:
    from . import simexport
    from .spec import derive, load, makespan

    for path in args.specs:
        spec = load(path)
        derived = derive(spec)
        alerts = {}
        for name, trigger in spec.alerts.items():
            try:
                alerts[name] = round(simexport.alert_time(spec, trigger), 3)
            except Exception as error:  # noqa: BLE001 - report, keep validating
                alerts[name] = f"n/a ({error})"
        print(
            json.dumps(
                {
                    "spec": str(path),
                    "name": spec.name,
                    "tasks": len(spec.tasks),
                    "agents": len(spec.agents),
                    "deliveries": len(derived.deliveries),
                    "roots": list(derived.roots),
                    "fault": spec.fault.cls,
                    "nominal_makespan_s": round(makespan(spec), 3),
                    "alert_times_s": alerts,
                }
            )
        )


def _run(args: argparse.Namespace) -> None:
    from .runner import Experiment, run_experiment
    from .stack import StackConfig

    run_experiment(
        StackConfig.load(args.stack),
        Experiment.load(args.experiment),
        args.out,
        limit=args.limit,
    )


def _score(args: argparse.Namespace) -> None:
    from .report import score

    rows = score(args.out)
    print(f"{len(rows)} runs scored -> {args.out / 'runs.csv'}")


def _sim(args: argparse.Namespace) -> None:
    from .runner import Experiment
    from .simbackend import run_grid

    rows = run_grid(Experiment.load(args.experiment), args.out, sim_root=args.sim_root)
    print(f"{len(rows)} simulator runs -> {args.out / 'sim_runs.csv'}")


def _report(args: argparse.Namespace) -> None:
    from .report import report

    report(args.out, figures=not args.no_figures)


def _netem(args: argparse.Namespace) -> None:
    from .netem import commands, load_profiles

    profiles = load_profiles(args.stack)
    for host, lines in commands(profiles, args.profile, clear=args.clear).items():
        print(f"# {host}")
        for line in lines:
            print(line)


def unit_command(
    name: str, log: Path, harness_args: list[str], *, user: bool = False
) -> list[str]:
    """``systemd-run`` for ``python -m harness <args>`` from this directory, with
    the HARNESS_* and PYTHONPATH environment, output appended to ``log``."""
    import os

    if not harness_args or harness_args[0] == "unit":
        raise SystemExit("unit: give the harness command to run")
    env = [
        f"--setenv={key}={value}"
        for key, value in sorted(os.environ.items())
        if key.startswith("HARNESS_") or key in {"PYTHONPATH", "PATH"}
    ]
    return [
        "systemd-run",
        *(["--user"] if user else []),
        f"--unit={name}",
        "--collect",
        "--property=KillMode=control-group",
        f"--property=WorkingDirectory={Path.cwd()}",
        f"--property=StandardOutput=append:{log.resolve()}",
        f"--property=StandardError=append:{log.resolve()}",
        *env,
        sys.executable,
        "-m",
        "harness",
        *harness_args,
    ]


def _pipeline(args: argparse.Namespace) -> None:
    from .pipeline import run_pipeline

    sys.exit(run_pipeline(args.stack, args.out))


def _commit_cost(args: argparse.Namespace) -> None:
    from .pipeline import commit_cost_ms

    for directory in args.dirs:
        print(json.dumps({"dir": str(directory), **commit_cost_ms(directory)}))


def _unit(args: argparse.Namespace) -> None:
    import subprocess

    args.log.parent.mkdir(parents=True, exist_ok=True)
    command = unit_command(args.name, args.log, args.harness, user=args.user)
    subprocess.run(command, check=True)
    print(f"{args.name}: started; output in {args.log}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="harness",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("export")
    p.add_argument(
        "--scenario", required=True, choices=["c2", "c3", "n2", "n3", "random"]
    )
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--sim-root")
    p.add_argument("--nodes", default="core,edge-1,edge-2")
    p.add_argument("--n-agents", type=int, default=15)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--p-share", type=float, default=0.25)
    p.add_argument("--sigma", type=float, default=0.5)
    p.add_argument("--consumed", type=float, default=0.5)
    p.add_argument("--rogue-rate", type=float, default=0.0)
    p.add_argument("--rogue-seed", type=int, default=0)
    p.add_argument("--p-spawn", type=float, default=1.0)
    p.set_defaults(handler=_export)
    p = sub.add_parser("validate")
    p.add_argument("specs", nargs="+", type=Path)
    p.set_defaults(handler=_validate)
    p = sub.add_parser("run")
    p.add_argument("--stack", required=True, type=Path)
    p.add_argument("--experiment", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--limit", type=int)
    p.set_defaults(handler=_run)
    p = sub.add_parser("score")
    p.add_argument("--out", required=True, type=Path)
    p.set_defaults(handler=_score)
    p = sub.add_parser("sim")
    p.add_argument("--experiment", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--sim-root")
    p.set_defaults(handler=_sim)
    p = sub.add_parser("report")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--no-figures", action="store_true")
    p.set_defaults(handler=_report)
    p = sub.add_parser("netem")
    p.add_argument("--stack", required=True, type=Path)
    p.add_argument("--profile", required=True)
    p.add_argument("--clear", action="store_true")
    p.set_defaults(handler=_netem)
    p = sub.add_parser("pipeline")
    p.add_argument("--stack", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.set_defaults(handler=_pipeline)
    p = sub.add_parser("commit-cost")
    p.add_argument("dirs", nargs="+", type=Path)
    p.set_defaults(handler=_commit_cost)
    p = sub.add_parser("unit")
    p.add_argument("--name", default="firebreak-exp-h-runner")
    p.add_argument("--log", required=True, type=Path)
    p.add_argument("--user", action="store_true", help="a user unit")
    p.add_argument("harness", nargs=argparse.REMAINDER, help="-- <command> ...")
    p.set_defaults(handler=_unit)
    args = parser.parse_args(argv)
    if getattr(args, "harness", None) and args.harness[0] == "--":
        args.harness = args.harness[1:]
    args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
