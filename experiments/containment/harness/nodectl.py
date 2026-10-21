"""Node-side control of one experiment agentd node.

Runs on the node's host: in-process or as a subprocess in the local dry run,
over SSH in the lab. It never touches a production agentd: every experiment
node has its own state directory, created fresh for each run.

- ``serve``: an agentd daemon on an ordinary SQLite store. Production startup
  requires the provisioned quota layout; experiment nodes use the explicit
  store injection that agentd's component fixtures use
  (``agent-runtime/tests/agentd/service_test_support.py``). Containment, the
  effect gateway, transport and trace sync are the production code paths.
- ``configure``: write ``node.json`` (node id and NATS endpoint).
- ``install``: register each workload agent as a Managed Agent whose grant is
  derived from the workload (recipients and effect rules) and write its token.
- ``operator``: register the harness's submitter on this node (a native
  connector allowed to create tasks for the node's agents with external
  requests only).
- ``schedule``: submit external requests through that connector, each at its
  time, and mark each with the run's services (``submitted`` with its runtime
  task, or ``refused`` with agentd's code) on the services' clock.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

OPERATOR = "harness-op"
# agentd accepts native connectors of known harness kinds only; the submitter
# acts like a native root harness and gets only task creation and status.
OPERATOR_HOST_TYPE = "pi"
OPERATOR_CAPABILITIES = ["edgecitadel_delegate", "edgecitadel_task_status"]


def agentd_dir(state: Path) -> Path:
    return state / "agentd"


def admin_client(state: Path) -> Any:
    from edgecitadel_agentd.client import AgentdClient
    from edgecitadel_agentd.service import socket_path_for

    directory = agentd_dir(state)
    return AgentdClient(
        socket_path_for(directory),
        admin_token=(directory / "admin.token").read_text().strip(),
    )


def configure(
    state: Path, node_id: str, nats_url: str | None, nats_token: str | None
) -> None:
    state.mkdir(parents=True, exist_ok=True)
    node: dict[str, Any] = {"agent_id": node_id, "version": 2, "mode": "edge"}
    if nats_url is not None:
        node.update(
            messaging_mode="single-client",
            nats_url=nats_url,
            nats_token=nats_token,
            plugin_nats_url=nats_url,
            plugin_nats_token=nats_token,
        )
    path = state / "node.json"
    path.write_text(json.dumps(node))
    path.chmod(0o600)


def serve(state: Path, stop: threading.Event | None = None) -> None:
    from edgecitadel_agentd.service import serve as production_serve
    from edgecitadel_agentd.store import AgentdStore

    directory = agentd_dir(state)
    stop = stop or threading.Event()
    production_serve(
        directory,
        stop,
        open_store=lambda: AgentdStore(directory / "agentd.sqlite3"),
    )


def install(state: Path, agents: Sequence[Mapping[str, Any]]) -> list[str]:
    """Register agents: ``[{"id", "recipients": [...], "effects": [{effect_class, target}]}]``."""
    admin = admin_client(state)
    for agent in agents:
        connector = f"managed-{agent['id']}"
        registration = admin.call(
            "connector.register",
            connector_id=connector,
            host_type="managed-agent",
            agent_id=agent["id"],
            capabilities=["det.task"],
        )
        token = state / "connectors" / f"{connector}.token"
        token.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        token.write_text(str(registration["token"]) + "\n")
        token.chmod(0o600)
    admin.call(
        "managed.reconcile",
        records=[
            {
                "package_id": f"edgecitadel.det-worker.{agent['id']}",
                "desired_state": "running",
                "agent_ids": [agent["id"]],
                "outbound_agents": list(agent["recipients"]),
                "effect_grant": list(agent["effects"]),
            }
            for agent in agents
        ],
    )
    return [agent["id"] for agent in agents]


def operator(state: Path, recipients: Sequence[str]) -> None:
    admin = admin_client(state)
    registration = admin.call(
        "connector.register",
        connector_id=OPERATOR,
        host_type=OPERATOR_HOST_TYPE,
        agent_id=OPERATOR,
        capabilities=OPERATOR_CAPABILITIES,
    )
    admin.call(
        "connector.grant",
        connector_id=OPERATOR,
        outbound_agents=list(recipients),
        effects=[],
    )
    path = state / "connectors" / f"{OPERATOR}.token"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(str(registration["token"]) + "\n")
    path.chmod(0o600)


# agentd's transport failures (busy or restarting), retried in place.
TRANSPORT_ERRORS = ("agentd is unavailable", "agentd closed the connection")
SUBMIT_ATTEMPTS = 4


def schedule(
    state: Path,
    entries: Sequence[Mapping[str, Any]],
    services: Any,
    stop: threading.Event | None = None,
) -> None:
    """Submit ``[{"at_ms", "recipient", "task", "body"}]`` at their times
    (wall clock) through the operator connector; mark each with the services."""
    from edgecitadel_agentd.client import AgentdClient, AgentdClientError
    from edgecitadel_agentd.service import socket_path_for

    stop = stop or threading.Event()
    client = AgentdClient(
        socket_path_for(agentd_dir(state)),
        connector_id=OPERATOR,
        token=(state / "connectors" / f"{OPERATOR}.token").read_text().strip(),
    )
    for entry in sorted(entries, key=lambda e: (e["at_ms"], e["task"])):
        if stop.wait(max(0.0, entry["at_ms"] / 1000 - time.time())):
            return
        mark: dict[str, Any] = {"task": entry["task"], "at_ms": entry["at_ms"]}
        for attempt in range(SUBMIT_ATTEMPTS):
            try:
                task = client.call(
                    "task.create",
                    recipient_id=entry["recipient"],
                    payload={
                        "body": json.dumps(dict(entry["body"]), separators=(",", ":"))
                    },
                    skill_id="det.task",
                )
            except AgentdClientError as error:
                if str(error).startswith(TRANSPORT_ERRORS) and (
                    attempt + 1 < SUBMIT_ATTEMPTS
                ):
                    time.sleep(0.2 * (attempt + 1))
                    continue
                mark.update(name="refused", code=str(error)[:200])
            else:
                mark.update(name="submitted", rt=str(task["task_id"]))
            break
        services.mark(mark)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.nodectl")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("serve", "configure", "install", "operator", "schedule"):
        command = sub.add_parser(name)
        command.add_argument("--state", required=True, type=Path)
    sub.choices["configure"].add_argument("--node", required=True)
    sub.choices["configure"].add_argument("--nats-url")
    sub.choices["install"].add_argument("--agents", required=True, type=Path)
    sub.choices["operator"].add_argument("--recipient", action="append", required=True)
    sub.choices["schedule"].add_argument("--entries", required=True, type=Path)
    sub.choices["schedule"].add_argument("--services", required=True)
    args = parser.parse_args(argv)
    if args.command == "serve":
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        serve(args.state, stop)
    elif args.command == "configure":
        # The broker token comes from the environment, never the command line.
        configure(
            args.state, args.node, args.nats_url, os.environ.get("HARNESS_NATS_TOKEN")
        )
    elif args.command == "install":
        print(json.dumps(install(args.state, json.loads(args.agents.read_text()))))
    elif args.command == "operator":
        operator(args.state, args.recipient)
    elif args.command == "schedule":
        from ._worker import services as det_services

        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        # The services token comes from the environment, never the command line.
        services = det_services.ServicesClient(
            args.services, os.environ["HARNESS_SERVICES_TOKEN"]
        )
        schedule(args.state, json.loads(args.entries.read_text()), services, stop)
        stop.wait()  # a run's process lives until the stack stops it
    sys.stdout.flush()


if __name__ == "__main__":
    main()
