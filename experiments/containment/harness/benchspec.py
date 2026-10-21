"""Workloads for the system comparison's overhead runs (no fault).

They run through the same plan, det-worker, services and scorer as every other
workload, so each system pays exactly its own messaging and bookkeeping:

- ``chain``: tasks alternate between two agents, each created by a message
  from the previous one, with near-zero compute. The handoff latency of a hop
  is the child's start minus the parent's produced event (services clock): the
  creating message to the child's agent plus that agent's dispatch of the task
  to itself, as every created task in the workloads pays. Placing the two
  agents on ``core``/``edge-1`` gives jim-eq <-> Endurance hops, ``edge-1``/
  ``edge-2`` Endurance-local hops, ``core``/``core`` jim-eq-local hops.
- ``fanout``: the root task creates ``width`` independent near-zero tasks on
  one agent at once; their completion rate is the throughput at that
  concurrency.

Each task has its own session (so creation is by message, with no pushed
input to wait for), which also sets the agent's slot count to its tasks + 2
(``deploy.slots``): chains stay short and are repeated across runs, and the
fan-out spreads over four agents of three tasks each.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

COMPUTE_S = 0.001
# Six hops keep each agent at 4 sessions (6 slots), within the workloads' range:
# more slots per agentd node overflow its socket backlog (EAGAIN), which the
# smoke workloads never reach. Samples come from repetitions instead.
HOPS = 6


def chain(name: str, hops: int, nodes: tuple[str, str]) -> dict[str, Any]:
    agents = [
        {"id": "r", "parent": None, "site": "s-r"},
        {"id": "b", "parent": "r", "site": "s-b"},
    ]
    tasks, messages = [], []
    for i in range(hops + 1):
        agent = "r" if i % 2 == 0 else "b"
        task = {"id": f"t{i:03d}", "agent": agent, "session": f"s{i:03d}"}
        tasks.append({**task, "compute": COMPUTE_S, "step_len": 1.0})
        if i:
            messages.append(
                {"id": f"m{i:03d}", "sender": f"t{i - 1:03d}", "receiver": f"t{i:03d}"}
            )
    tasks[0]["reads"] = ["task"]
    return _spec(name, agents, {"s-r": nodes[0], "s-b": nodes[1]}, tasks, messages)


def fanout(
    name: str, agents_n: int, width: int, nodes: tuple[str, str]
) -> dict[str, Any]:
    """The root creates ``width`` tasks on each of ``agents_n`` agents at once."""
    agents = [{"id": "r", "parent": None, "site": "s-r"}]
    agents += [{"id": f"b{j}", "parent": "r", "site": "s-b"} for j in range(agents_n)]
    tasks = [
        {
            "id": "root",
            "agent": "r",
            "session": "sr",
            "compute": COMPUTE_S,
            "reads": ["task"],
        }
    ]
    messages = []
    for j in range(agents_n):
        for i in range(width):
            task = f"w{j}.{i}"
            tasks.append(
                {
                    "id": task,
                    "agent": f"b{j}",
                    "session": f"s{j}.{i}",
                    "compute": COMPUTE_S,
                }
            )
            messages.append({"id": f"m{j}.{i}", "sender": "root", "receiver": task})
    return _spec(name, agents, {"s-r": nodes[0], "s-b": nodes[1]}, tasks, messages)


def _spec(
    name: str,
    agents: list[dict[str, Any]],
    placement: dict[str, str],
    tasks: list[dict[str, Any]],
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "name": name,
        "agents": agents,
        "placement": placement,
        "effect_classes": [],
        "external_inputs": ["task"],
        "tasks": tasks,
        "artifacts": [],
        "messages": messages,
        "effects": [],
        "fault": {"class": "none"},
        "alerts": {},
    }


SHAPES = {
    "chain-k2e": lambda: chain("chain-k2e", HOPS, ("core", "edge-1")),
    "chain-e2e": lambda: chain("chain-e2e", HOPS, ("edge-1", "edge-2")),
    "chain-k2k": lambda: chain("chain-k2k", HOPS, ("core", "core")),
    "fanout-k2e-12": lambda: fanout("fanout-k2e-12", 4, 3, ("core", "edge-1")),
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.benchspec")
    parser.add_argument("--out", type=Path, required=True, help="directory")
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    for name, make in SHAPES.items():
        (args.out / f"{name}.json").write_text(json.dumps(make(), indent=1) + "\n")
        print(args.out / f"{name}.json")


if __name__ == "__main__":
    main()
