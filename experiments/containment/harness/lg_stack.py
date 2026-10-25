"""The per-run stack of the LangGraph baseline: services and one ``langgraph dev``.

It answers the runner as ``stack.RunStack`` does. The workload runs as one
LangGraph thread whose run is the whole workflow (``harness.lg_graph``), on a
single host: every workload node maps to the controller host, and the
overhead numbers are labelled single-host. ``langgraph dev`` is LangGraph's
in-memory server (no licence, no Postgres or Redis); it is the core runtime
the baseline compares, not the hosted product.

- ``up`` starts the run's services and the server, with the compiled plan,
  the agents' slot counts and the sink's URL in the run's LangGraph folder;
- ``submit`` starts the run with every root task (the first call starts them
  all with the same ``t0``; later calls return the ids already assigned);
  ``schedule`` (the runner's per-node request schedule, which on EdgeCitadel
  is ``nodectl schedule`` through agentd) submits every entry at once and
  marks each ``submitted`` or ``refused`` with the services: the workflow is
  one LangGraph run, and each root's det-worker still waits for its release;
- ``post_alert`` hands the alert to ``harness.lg_control``. The controller
  runs in the runner, so it writes its decision log on the runner's machine:
  in the run's directory on the LangGraph host when that is this machine (a
  dry run), else under the runner's run directory (``controller/``), which
  ``fetch`` serves where the runner collects the log from the controller
  host (``core/containment-decisions.jsonl``).
"""

from __future__ import annotations

import json
import logging
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import deploy
from .lg_control import LangGraphController
from .native import LoopThread
from .spec import Spec, fault_origin
from .stack import (
    LocalHost,
    RunStack,
    StackError,
    free_port,
    http_json,
    wait_until,
)

log = logging.getLogger(__name__)
DECISIONS = "core/containment-decisions.jsonl"
# One super-step per supervisor round and per worker round; generous.
RECURSION_LIMIT = 10_000
ENTRY = {
    "langgraph": "from harness.lg_graph import graph  # noqa: F401\n",
    # timing only (reference and uncontained runs): the functional API
    "langgraph_functional": "from harness.lg_func import graph  # noqa: F401\n",
}


class LangGraphRunStack(RunStack):
    def __init__(self, config: Any, run_id: str, run_dir: Path) -> None:
        super().__init__(config, run_id, run_dir)
        self.loop = LoopThread(f"lg-{run_id[:24]}")
        self.controller: LangGraphController | None = None
        self.plan: Mapping[str, Any] = {}
        self.root_ids: dict[str, str] = {}
        self.url = ""
        self.decisions_file: Path | None = None

    def up(
        self,
        spec: Spec,
        plan: Mapping[str, Any],
        *,
        strategy: str,
        enforce: bool = True,
        **_edgecitadel_options: Any,
    ) -> None:
        if plan["effect_sink"].get("adapter") != "http":
            raise StackError("the LangGraph baseline runs on the ledger sink only")
        host = self.config.controller
        if set(self.config.node_hosts.values()) != {host}:
            raise StackError("the LangGraph baseline runs on one host (the controller)")
        self.plan = plan
        self.hosts[host].write(f"{self.host_dir(host)}/.keep", b"")
        self._start_services(host, spec)
        work = f"{self.host_dir(host)}/langgraph"
        slots = {a.id: deploy.slots(spec, a.id) for a in spec.agents}
        files = {
            "plan.json": json.dumps(plan).encode(),
            "slots.json": json.dumps(slots).encode(),
            "graph_entry.py": ENTRY[self.config.system].encode(),
            "langgraph.json": json.dumps(
                {
                    "dependencies": ["."],
                    "graphs": {"workload": "./graph_entry.py:graph"},
                    "env": {},
                }
            ).encode(),
        }
        for name, data in files.items():
            self.hosts[host].write(f"{work}/{name}", data, mode=0o644)
        self.hosts[host].write(
            f"{work}/sink.url", f"{self.services_url}/sink/{self.sink_token}".encode()
        )
        port = int(self.config.ports.get("langgraph", 0)) or free_port()
        bind = (
            "127.0.0.1"
            if self.config.mode == "dryrun"
            else self.config.hosts[host].address
        )
        langgraph = str(Path(self._python(host)).parent / "langgraph")
        self._start(
            host,
            "langgraph",
            [
                langgraph,
                "dev",
                "--no-browser",
                "--no-reload",
                "--allow-blocking",
                "--host",
                bind,
                "--port",
                str(port),
                "--config",
                f"{work}/langgraph.json",
            ],
            cwd=work,
            env={
                "LG_WORK_DIR": work,
                "DET_WORKER_SERVICES": self.services_url,
                "DET_WORKER_SERVICES_TOKEN": self.services_token,
                "LANGGRAPH_CLI_NO_ANALYTICS": "1",
                "LANGSMITH_TRACING": "false",
                "LANGCHAIN_TRACING_V2": "false",
            },
        )
        self.url = f"http://{self.config.hosts[host].address}:{port}"
        wait_until(lambda: http_json(f"{self.url}/ok"), "langgraph dev", timeout=120)
        origin = fault_origin(spec)
        self.controller = LangGraphController(
            self.loop,
            self.url,
            strategy=strategy if enforce else None,
            node=host,
            faulty_task=origin,
            faulty_agent=spec.task(origin).agent if origin else None,
            bad_item=spec.fault.item,
            decisions_path=self._decisions_file(host),
            incident_id=f"native:{self.run_id}",
            run_config={"recursion_limit": RECURSION_LIMIT},
        )
        self.start_procstat()

    def _decisions_file(self, host: str) -> Path:
        """Where the controller (in the runner's process) writes its decision log:
        the LangGraph host's run directory only when that host is this machine."""
        if isinstance(self.hosts[host], LocalHost):
            self.decisions_file = Path(self.host_dir(host)) / DECISIONS
        else:
            self.decisions_file = self.run_dir / "controller" / Path(DECISIONS).name
        return self.decisions_file

    def fetch(self, host: str, path: str) -> bytes | None:
        """The runner collects the decision log from the controller host's run
        directory; it is on this machine (``_decisions_file``)."""
        local = self.decisions_file
        if (
            local is not None
            and host == self.config.controller
            and path == f"{self.host_dir(host)}/{DECISIONS}"
        ):
            return local.read_bytes() if local.exists() else None
        return super().fetch(host, path)

    def submit(self, node: str, recipient: str, body: Mapping[str, Any]) -> str:
        """The first root starts the run with every root; later calls return
        the request ids already assigned."""
        assert self.controller is not None
        if not self.root_ids:
            requests = []
            for root in derive_roots(self.plan):
                request_id = f"root:{root}:{secrets.token_hex(4)}"
                agent = self.plan["tasks"][root]["agent"]
                requests.append(
                    {
                        "id": request_id,
                        "recipient": agent,
                        "body": {**body, "task": root},
                    }
                )
                self.root_ids[root] = request_id
            self.loop.run(self.controller.start(requests))
        return self.root_ids.get(str(body.get("task")), "")

    def schedule(self, node: str, entries: list[dict[str, Any]]) -> None:
        for entry in sorted(entries, key=lambda e: (e["at_ms"], e["task"])):
            mark: dict[str, Any] = {"task": entry["task"], "at_ms": entry["at_ms"]}
            try:
                rt = self.submit(node, entry["recipient"], entry["body"])
            except Exception as error:  # noqa: BLE001 - recorded as refused
                mark.update(name="refused", code=f"langgraph: {error}"[:200])
            else:
                mark.update(name="submitted", rt=rt)
            self.services("POST", "/mark", mark)

    def attempt_of(self, task_id: str) -> str | None:
        return task_id

    def post_alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        assert self.controller is not None
        return self.controller.alert(alert)

    def alert_endpoint(self) -> dict[str, Any] | None:
        return None  # the operator runs in the controller: it posts the alert

    def busy(self) -> bool:
        """The workflow's current run is still pending or running."""
        if self.controller is None:
            return False
        return bool(self.loop.run(self.controller.running(), timeout=10))

    def monitor_status(self) -> dict[str, Any]:
        return self.controller.status() if self.controller else {}

    def down(self) -> list[str]:
        if self.controller is not None:
            self.controller.stop()
        died = super().down()
        self.loop.close()
        return died


def derive_roots(plan: Mapping[str, Any]) -> list[str]:
    return [str(r) for r in plan["roots"]]
