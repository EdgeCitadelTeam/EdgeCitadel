"""The per-run stack of the Google A2A baseline: services, one A2A server per agent.

It answers the runner exactly as ``stack.RunStack`` does, so a run's control
loop, alert trigger, collected files and scoring are the EdgeCitadel ones:

- ``up`` starts the run's services (the same ledger, events, readiness and
  sink) and one ``harness.a2a_agent`` per workload agent on its node's host,
  each with the compiled plan and the addresses of all agents;
- ``submit`` sends a root task to the root agent's server; the A2A task id is
  the invocation id the workers' events carry, so ``attempt_of`` is identity;
- ``post_alert`` hands the operator alert to the native controller
  (``harness.a2a_control``), which writes the monitor-schema decision log
  where the runner collects it; ``monitor_status`` reports that controller.

No NATS, agentd or Core runs here. Ports: ``ports.a2a_base`` + agent index on
each host (lab), or free local ports (dry run).
"""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import deploy
from .a2a_common import LoopThread, Peers
from .a2a_control import NativeController
from .spec import Spec, fault_origin
from .stack import RunStack, StackError, free_port, wait_until

log = logging.getLogger(__name__)
DECISIONS = "core/containment-decisions.jsonl"


class A2ARunStack(RunStack):
    def __init__(self, config: Any, run_id: str, run_dir: Path) -> None:
        super().__init__(config, run_id, run_dir)
        self.operator_token = secrets.token_urlsafe(24)
        self.loop = LoopThread(f"a2a-{run_id[:24]}")
        self.peers: Peers | None = None
        self.controller: NativeController | None = None

    def up(
        self,
        spec: Spec,
        plan: Mapping[str, Any],
        *,
        strategy: str,
        enforce: bool = True,
        **_edgecitadel_options: Any,
    ) -> None:
        """Start everything. The EdgeCitadel monitor's options (latency model,
        profile, phase-1 mode) have no meaning here and are ignored."""
        if plan["effect_sink"].get("adapter") != "http":
            raise StackError("the A2A baseline runs on the ledger sink only")
        controller = self.config.controller
        for host in {controller, *self.config.node_hosts.values()}:
            self.hosts[host].write(f"{self.host_dir(host)}/.keep", b"")
        self._start_services(controller, spec)
        for node in deploy.nodes(spec):
            if node not in self.config.node_hosts:
                raise StackError(
                    f"workload node '{node}' has no host in the stack config"
                )
        base = int(self.config.ports.get("a2a_base", 0))
        urls, ports = {}, {}
        for index, agent in enumerate(a.id for a in spec.agents):
            host = self.config.node_hosts[plan["agents"][agent]["node"]]
            ports[agent] = base + index if base else free_port()
            urls[agent] = f"http://{self.config.hosts[host].address}:{ports[agent]}/"
        peers_json = json.dumps(urls).encode()
        plan_bytes = json.dumps(plan).encode()
        sink_url = f"{self.services_url}/sink/{self.sink_token}".encode()
        for agent in urls:
            node = plan["agents"][agent]["node"]
            host = self.config.node_hosts[node]
            directory = f"{self.host_dir(host)}/workers/{agent}"
            self.hosts[host].write(f"{directory}/plan.json", plan_bytes, mode=0o644)
            self.hosts[host].write(f"{directory}/peers.json", peers_json, mode=0o644)
            self.hosts[host].write(f"{directory}/sink.url", sink_url)
            self.hosts[host].write(
                f"{directory}/operator.token", self.operator_token.encode()
            )
            self._start(
                host,
                f"a2a-{agent}",
                [
                    self._python(host),
                    "-m",
                    "harness.a2a_agent",
                    "--plan",
                    f"{directory}/plan.json",
                    "--agent",
                    agent,
                    "--services",
                    self.services_url,
                    "--peers",
                    f"{directory}/peers.json",
                    "--bind",
                    # Lab servers listen on the host's own (tailnet) address only.
                    "127.0.0.1"
                    if self.config.mode == "dryrun"
                    else self.config.hosts[host].address,
                    "--port",
                    str(ports[agent]),
                    "--slots",
                    str(deploy.slots(spec, agent)),
                    "--work-dir",
                    directory,
                    "--task-store",
                    self.config.a2a_task_store,
                ],
                cwd=f"{self.config.hosts[host].repo}/experiments/containment",
                env={"DET_WORKER_SERVICES_TOKEN": self.services_token},
            )
        for agent in urls:
            host = self.config.node_hosts[plan["agents"][agent]["node"]]
            ready = f"{self.host_dir(host)}/workers/{agent}/ready"
            wait_until(
                lambda h=host, r=ready: self.hosts[h].exists(r), f"A2A agent {agent}"
            )
        self.start_procstat()
        self.peers = Peers(urls)
        origin = fault_origin(spec)
        self.controller = NativeController(
            self.loop,
            self.peers,
            strategy=strategy if enforce else None,
            agent_nodes={a: plan["agents"][a]["node"] for a in urls},
            faulty_agent=spec.task(origin).agent if origin else None,
            bad_item=spec.fault.item,
            decisions_path=Path(self.host_dir(controller)) / DECISIONS,
            operator_token=self.operator_token,
            incident_id=f"native:{self.run_id}",
        )

    def submit(self, node: str, recipient: str, body: Mapping[str, Any]) -> str:
        assert self.peers is not None
        task = self.loop.run(
            self.peers.send(
                recipient,
                body,
                message_id=f"root:{secrets.token_hex(8)}",
                context_id=f"root:{body.get('task')}",
            )
        )
        return task.id

    def attempt_of(self, task_id: str) -> str | None:
        return task_id

    def post_alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        assert self.controller is not None
        return self.controller.alert(alert)

    def monitor_status(self) -> dict[str, Any]:
        return self.controller.status() if self.controller else {}

    def down(self) -> list[str]:
        """Stop the operator first (its sweeps would otherwise see servers go
        away), then every process."""
        if self.controller is not None:
            self.controller.stop()
        if self.peers is not None:
            try:
                self.loop.run(self.peers.close(), timeout=10)
            except Exception as error:  # noqa: BLE001 - clients of stopped servers
                log.debug("closing A2A clients: %r", error)
        died = super().down()
        self.loop.close()
        return died
