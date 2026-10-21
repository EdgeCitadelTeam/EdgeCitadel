"""Run one det-worker agent: ``slots`` concurrent claim loops of the managed runtime.

The workload model runs an agent's sessions in parallel, so one agent needs
several concurrent invocations. Each slot is the production
``managed_runtime.run`` loop with its own agentd session; all slots share one
``DetWorker`` (the agent's memory). Configuration comes from arguments or the
``DET_WORKER_*`` environment variables named in ``plugin.yaml``; the services
token is read from ``DET_WORKER_SERVICES_TOKEN`` only (never an argument). The
managed runtime itself reads ``EDGECITADEL_STATE_DIR`` and
``EDGECITADEL_CONNECTOR_ID``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from edgecitadel_agentd import managed_runtime
from edgecitadel_agentd.client import AgentdClientError

from .services import ServicesClient
from .worker import SKILL_ID, DetWorker

RESTART_DELAY_S = 1.0
CARD = """agent_id: {agent}
name: {agent}
description: Deterministic workload worker for containment experiments.
version: 0.1.0
runtime:
  kind: native
  roles: [worker]
  conformance: L1
  heartbeat_interval_sec: 30
skills:
  - id: {skill}
    name: det-task
    description: Execute one planned deterministic workload task.
"""


def parse(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ
    parser = argparse.ArgumentParser(prog="det_worker")
    parser.add_argument("--plan", default=env.get("DET_WORKER_PLAN"))
    parser.add_argument("--agent", default=env.get("DET_WORKER_AGENT"))
    parser.add_argument("--services", default=env.get("DET_WORKER_SERVICES"))
    parser.add_argument(
        "--slots", type=int, default=int(env.get("DET_WORKER_SLOTS", "4"))
    )
    parser.add_argument("--work-dir", default=env.get("DET_WORKER_DIR", "."))
    args = parser.parse_args(argv)
    for name in ("plan", "agent", "services"):
        if not getattr(args, name):
            parser.error(f"--{name} (or DET_WORKER_{name.upper()}) is required")
    if not env.get("DET_WORKER_SERVICES_TOKEN"):
        parser.error("DET_WORKER_SERVICES_TOKEN is required")
    if args.slots < 1:
        parser.error("--slots must be positive")
    return args


async def serve(args: argparse.Namespace) -> None:
    plan = json.loads(Path(args.plan).read_text())
    if args.agent not in plan["agents"]:
        raise SystemExit(f"agent {args.agent} is not in the plan")
    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)
    card = work / f"{args.agent}.card.yaml"
    card.write_text(CARD.format(agent=args.agent, skill=SKILL_ID))
    loop = asyncio.get_running_loop()
    # Every slot polls its task and calls agentd through threads.
    loop.set_default_executor(ThreadPoolExecutor(max_workers=8 + 4 * args.slots))
    worker = DetWorker(
        plan,
        args.agent,
        ServicesClient(args.services, os.environ["DET_WORKER_SERVICES_TOKEN"]),
        node=plan["agents"][args.agent]["node"],
        event_log=work / f"{args.agent}.events.jsonl",
    )
    stop = asyncio.Event()
    opened: set[int] = set()

    def stop_all() -> None:
        stop.set()

    def install_stop() -> None:
        # Each run() installs a handler for its own loop; one handler stops all.
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, stop_all)

    async def slot(index: int) -> None:
        async def on_start(_client: object) -> None:
            install_stop()
            opened.add(index)
            if len(opened) == args.slots:
                (work / "ready").write_text(str(args.slots))

        while not stop.is_set():
            try:
                await managed_runtime.run(card, worker.handle, on_start=on_start)
                return
            except AgentdClientError as error:
                # A transient local RPC failure ends the runtime loop; open a new
                # session. A task in hand at that moment is recovered by agentd.
                await worker.note("slot_restart", slot=index, error=str(error)[:200])
                await asyncio.sleep(RESTART_DELAY_S)

    slots = [asyncio.create_task(slot(i)) for i in range(args.slots)]
    stopping = asyncio.create_task(stop.wait())
    done, _ = await asyncio.wait(
        [stopping, *slots], return_when=asyncio.FIRST_COMPLETED
    )
    for task in [*slots, stopping]:
        task.cancel()
    results = await asyncio.gather(*slots, return_exceptions=True)
    failed = [
        r
        for r in results
        if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError)
    ]
    if stopping not in done or failed:
        raise SystemExit(f"a det-worker slot exited: {failed[:1]!r}")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    asyncio.run(serve(parse(argv)))


if __name__ == "__main__":
    main()
