"""C2 containment scenario on a local multi-process deployment.

Topology (all on localhost, every component its own process):

- ``nats-server`` with JetStream: Core NATS on jim-eq;
- Core: ``uvicorn aggregator.main:app`` with the trace collector and the
  containment monitor, which sends signed control commands over NATS;
- three agentd nodes: ``jim-eq`` (root ``kipp``), ``cluster`` (team A:
  ``tars`` and leaves ``a1``-``a3``) and ``endurance`` (team B: ``case`` and
  leaves ``b1``, ``b2``), each exporting its trace to Core; cluster and endurance
  reach the broker through a relay that adds a one-way delay;
- one Managed Agent process per agent (``e2e.containment.agents``).

Workflow: kipp asks tars for an analysis and case for independent work. tars
starts the healthy ``a3``, has ``a1`` produce a shared artifact, passes it to
the sibling ``a2`` and returns a2's analysis; kipp fans it out to case, whose
``b1`` schedules an actuator command (``effect.execute`` with
``scheduled_at_ms``): endurance's effect gateway keeps it and, when it is due,
re-checks it and performs it by posting to the actuator ledger. ``a3`` and ``b2``
never touch the artifact. A validator then reports the artifact's version as failed:

- ``early``: while a2 holds the artifact and nothing has left team A;
- ``late``: after b1 scheduled its actuator command.

The run writes a JSON summary: the monitor's decision (root cause, actions with
t_alert, t_decision, t_sent, t_applied, t_confirmed and releases), what each
node enforced (its own ``containment.list``), whether the actuator ran (its own
ledger, written by an HTTP endpoint the harness runs, never by an agent), which
healthy tasks completed and the controller leases. The opt-in
``aggregator/tests/test_containment_e2e.py`` checks Firebreak's properties on it.
Every agentd node enforces the fail-closed controller lease, renewed by the
monitor. agentd runs with the ordinary-store test entrypoint, not the
provisioned storage layout.

    PATH=<dir with nats-server>:$PATH python -m e2e.containment.c2 \\
        --strategy firebreak --variant late --out results/firebreak-late.json
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.service import socket_path_for

REPO = Path(__file__).resolve().parents[2]
OWNER = "core-monitor"
NODES = {
    "jim-eq": ("kipp",),
    "cluster": ("tars", "a1", "a2", "a3"),
    "endurance": ("case", "b1", "b2"),
}
# A child's recipients are its grant intersected with its parent's scope, so the
# root's grant must cover every agent below it.
OUTBOUND = {
    "kipp": ["tars", "case", "a1", "a2", "a3", "b1", "b2"],
    "tars": ["a1", "a2", "a3"],
    "case": ["b1", "b2"],
}
ACTUATOR = [{"effect_class": "device.command", "target": "actuator/*"}]
# Effects are delegated: a child never authorizes more than its parent could.
EFFECTS = {"kipp": ACTUATOR, "case": ACTUATOR, "b1": ACTUATOR}
ORG_PARENT = {
    "tars": "kipp",
    "case": "kipp",
    "a1": "tars",
    "a2": "tars",
    "a3": "tars",
    "b1": "case",
    "b2": "case",
}
HEALTHY = ("a3", "b2")
REMOTE_DELAY_MS = 25.0
# The lab runs agentd's controller lease fail-closed; the monitor renews it.
CONTROLLER_LEASE_MS = 30_000
TERMINAL = {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait(predicate, timeout: float, what: str, interval: float = 0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {what}")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class Node:
    node_id: str
    state: Path

    @property
    def socket(self) -> Path:
        return socket_path_for(self.state / "agentd")

    def admin(self) -> AgentdClient:
        token = (self.state / "agentd" / "admin.token").read_text().strip()
        return AgentdClient(self.socket, admin_token=token, timeout=10)

    def db(self) -> sqlite3.Connection:
        """agentd's trace database with its task database attached, read-only."""
        directory = self.state / "agentd"
        db = sqlite3.connect(
            (directory / "agentd.sqlite3").as_uri() + "?mode=ro", uri=True, timeout=5
        )
        db.execute(
            "ATTACH DATABASE ? AS task_state",
            ((directory / "agentd-tasks.sqlite3").as_uri() + "?mode=ro",),
        )
        return db


class ActuatorLedger:
    """The scenario's independent evidence of what the actuator received: an
    HTTP endpoint (the gateway's ``http`` adapter target) appending each POST."""

    def __init__(self, path: Path):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        lock = threading.Lock()

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 - http.server API
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with lock, path.open("a") as ledger:
                    ledger.write(
                        json.dumps({**body, "received_at_ms": now_ms()}) + "\n"
                    )
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args) -> None:
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/actuator"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@dataclass
class Deployment:
    run_dir: Path
    strategy: str
    profile: str = "deployed"
    phase1_include_cut: bool = False
    processes: list[tuple[str, subprocess.Popen]] = field(default_factory=list)
    ledger: ActuatorLedger | None = None
    nodes: dict[str, Node] = field(default_factory=dict)
    admin_token: str = field(default_factory=lambda: secrets.token_hex(24))
    nats_token: str = field(default_factory=lambda: secrets.token_urlsafe(24))
    control_key: bytes = field(default_factory=lambda: secrets.token_hex(32).encode())
    core_port: int = field(default_factory=free_port)
    nats_port: int = field(default_factory=free_port)

    # -- processes -----------------------------------------------------------------
    def spawn(self, name: str, argv: list[str], env: dict | None = None) -> None:
        logs = self.run_dir / "logs"
        logs.mkdir(exist_ok=True)
        environment = {
            **os.environ,
            "PYTHONPATH": f"{REPO}:{REPO / 'agent-runtime/src'}",
            **(env or {}),
        }
        with (logs / f"{name}.log").open("w") as log:
            process = subprocess.Popen(
                argv, cwd=REPO, env=environment, stdout=log, stderr=subprocess.STDOUT
            )
        self.processes.append((name, process))

    def alive(self) -> None:
        for name, process in self.processes:
            if process.poll() is not None:
                raise RuntimeError(f"{name} exited with {process.returncode}")

    def stop(self) -> None:
        if self.ledger is not None:
            self.ledger.close()
        for _, process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 10
        for _, process in reversed(self.processes):
            try:
                process.wait(max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(5)

    # -- bring-up ------------------------------------------------------------------
    def start(self) -> None:
        self.ledger = ActuatorLedger(self.run_dir / "actuator-ledger.jsonl")
        adapters = self.run_dir / "effect-adapters.json"
        adapters.write_text(json.dumps({"http": {"ledger_url": self.ledger.url}}))
        self.start_nats()
        self.start_core()
        # One node at a time: each agentd adds its agents' subjects to the shared
        # AGENT_INBOX stream by read-modify-write, so concurrent first starts on
        # one broker can drop another node's subjects (single-client mode).
        for node_id in NODES:
            self.start_node(node_id)
            self.provision(node_id)
            node = self.nodes[node_id]
            wait(lambda node=node: self.inboxes_ready(node), 60, f"{node_id} inboxes")
        for node_id, agents in NODES.items():
            for agent in agents:
                self.spawn(
                    f"agent-{agent}",
                    [
                        sys.executable,
                        "-m",
                        "e2e.containment.agents",
                        "--agent",
                        agent,
                        "--run-dir",
                        str(self.run_dir),
                    ],
                    {
                        "EDGECITADEL_STATE_DIR": str(self.nodes[node_id].state),
                        "EDGECITADEL_CONNECTOR_ID": f"managed-{agent}",
                    },
                )
            node = self.nodes[node_id]
            wait(lambda node=node: self.ready(node), 60, f"{node_id} ready")
        missing = self.inbox_subjects_missing()
        if missing:
            raise RuntimeError(f"AGENT_INBOX lost subjects: {sorted(missing)}")

    def inbox_subjects_missing(self) -> set[str]:
        import asyncio

        from nats.aio.client import Client as NATS

        async def subjects() -> set[str]:
            nc = NATS()
            await nc.connect(
                servers=[f"nats://127.0.0.1:{self.nats_port}"], token=self.nats_token
            )
            try:
                info = await nc.jetstream().stream_info("AGENT_INBOX")
                return set(info.config.subjects or ())
            finally:
                await nc.close()

        expected = {
            f"agents.{agent}.inbox"
            for agents in NODES.values()
            for agent in (*agents, "operator")
        }
        return expected - asyncio.run(subjects())

    def start_nats(self) -> None:
        directory = self.run_dir / "nats"
        directory.mkdir()
        config = directory / "nats.conf"
        config.write_text(
            f'host: "127.0.0.1"\nport: {self.nats_port}\n'
            f"authorization {{ token: {json.dumps(self.nats_token)} }}\n"
            f"jetstream {{ store_dir: {json.dumps(str(directory / 'js'))}, "
            "max_file_store: 2GB }\n"
        )
        config.chmod(0o600)
        binary = shutil.which("nats-server")
        if binary is None:
            raise RuntimeError("nats-server is not on PATH")
        self.spawn("nats", [binary, "-c", str(config)])
        wait(lambda: self.connectable(self.nats_port), 20, "nats-server")

    @staticmethod
    def connectable(port: int) -> bool:
        with socket.socket() as probe:
            return probe.connect_ex(("127.0.0.1", port)) == 0

    def start_core(self) -> None:
        core = self.run_dir / "core"
        core.mkdir()
        key = core / f"{OWNER}.key"
        key.write_bytes(self.control_key + b"\n")
        key.chmod(0o600)
        config = core / "containment.json"
        config.write_text(
            json.dumps(
                {
                    "monitor": {
                        "owner": OWNER,
                        "protected_effect_classes": ["device.command"],
                        "session_isolated_agents": sorted(
                            a for agents in NODES.values() for a in agents
                        ),
                        "agent_nodes": {
                            agent: node
                            for node, agents in NODES.items()
                            for agent in agents
                        },
                        "org_parent": ORG_PARENT,
                        "stop_team_level": 1,
                        "control_latency_ms": {
                            "jim-eq": 2,
                            "cluster": 2 * REMOTE_DELAY_MS,
                            "endurance": 2 * REMOTE_DELAY_MS,
                        },
                        "strategy": self.strategy,
                        "profile": self.profile,
                        "phase1_include_cut": self.phase1_include_cut,
                    },
                    "control": {
                        "nats": {
                            "key_file": str(key),
                            "confirmation_timeout_ms": 5000,
                            "command_lifetime_ms": 30000,
                        }
                    },
                },
                indent=2,
            )
        )
        self.spawn(
            "core",
            [
                sys.executable,
                "-m",
                "uvicorn",
                "aggregator.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(self.core_port),
            ],
            {
                "DB_PATH": str(core / "openclaw.db"),
                "NATS_URL": f"nats://127.0.0.1:{self.nats_port}",
                "NATS_TOKEN": self.nats_token,
                "EDGECITADEL_TRACE_COLLECTOR": "1",
                "EDGECITADEL_CONTAINMENT_MONITOR": "1",
                "EDGECITADEL_CONTAINMENT_CONFIG": str(config),
                "EDGECITADEL_ADMIN_TOKEN": self.admin_token,
            },
        )
        wait(
            lambda: self.core_status().get("state") == "running",
            60,
            "Core and the containment monitor",
        )

    def start_node(self, node_id: str) -> None:
        state = self.run_dir / "nodes" / node_id
        (state / "agentd" / "control-keys").mkdir(parents=True, mode=0o700)
        key = state / "agentd" / "control-keys" / f"{OWNER}.key"
        key.write_bytes(self.control_key + b"\n")
        key.chmod(0o600)
        port = self.nats_port
        if node_id != "jim-eq":
            # Remote sites reach Core NATS over a delayed link.
            port = free_port()
            self.spawn(
                f"relay-{node_id}",
                [
                    sys.executable,
                    "-m",
                    "e2e.containment.relay",
                    "--listen",
                    str(port),
                    "--upstream",
                    str(self.nats_port),
                    "--delay-ms",
                    str(REMOTE_DELAY_MS),
                ],
            )
            wait(lambda: self.connectable(port), 10, f"relay for {node_id}")
        url = f"nats://127.0.0.1:{port}"
        (state / "node.json").write_text(
            json.dumps(
                {
                    "version": 2,
                    "mode": "edge",
                    "agent_id": node_id,
                    "messaging_mode": "single-client",
                    "plugin_nats_url": url,
                    "plugin_nats_token": self.nats_token,
                }
            )
        )
        (state / "node.json").chmod(0o600)
        node = Node(node_id, state)
        self.nodes[node_id] = node
        self.spawn(
            f"agentd-{node_id}",
            [
                sys.executable,
                str(REPO / "agent-runtime/tests/agentd/service_test_support.py"),
                str(state / "agentd"),
            ],
            {
                "EDGECITADEL_TRACE_SYNC": "1",
                "EDGECITADEL_CONTROLLER_LEASE_MS": str(CONTROLLER_LEASE_MS),
                "EDGECITADEL_EFFECT_ADAPTERS": str(
                    self.run_dir / "effect-adapters.json"
                ),
            },
        )
        wait(lambda: node.socket.exists(), 30, f"agentd on {node_id}")
        wait(lambda: self.started(node), 30, f"agentd services on {node_id}")

    @staticmethod
    def admin_call(admin: AgentdClient, operation: str, **params):
        """An admin write, retried on agentd's transient SQLite contention.

        Some agentd admin writes (connector.register) read before writing in a
        deferred transaction while its telemetry thread writes the same database
        through its own connection; the lock upgrade can then fail at once with
        "database is locked" (reported as "agentd operation failed"), rolled back.
        """
        for attempt in range(5):
            try:
                return admin.call(operation, **params)
            except AgentdClientError as error:
                if str(error) != "agentd operation failed" or attempt == 4:
                    raise
                time.sleep(0.2)
        raise AssertionError("unreachable")

    def provision(self, node_id: str) -> None:
        node = self.nodes[node_id]
        admin = node.admin()
        connectors = node.state / "connectors"
        connectors.mkdir(mode=0o700, exist_ok=True)
        records = []
        for agent in NODES[node_id]:
            token = self.admin_call(
                admin,
                "connector.register",
                connector_id=f"managed-{agent}",
                host_type="managed-agent",
                agent_id=agent,
                capabilities=["work"],
            )["token"]
            path = connectors / f"managed-{agent}.token"
            path.write_text(token + "\n")
            path.chmod(0o600)
            records.append(
                {
                    "package_id": f"c2.{agent}",
                    "desired_state": "running",
                    "agent_ids": [agent],
                    "outbound_agents": OUTBOUND.get(agent, []),
                    "effect_grant": EFFECTS.get(agent, []),
                }
            )
        self.admin_call(admin, "managed.reconcile", records=records)
        if node_id == "jim-eq":
            token = self.admin_call(
                admin,
                "connector.register",
                connector_id="operator",
                host_type="codex",
                agent_id="operator",
                capabilities=["edgecitadel_delegate", "edgecitadel_task_status"],
            )["token"]
            self.admin_call(
                admin,
                "connector.grant",
                connector_id="operator",
                outbound_agents=["kipp"],
            )
            (connectors / "operator.token").write_text(token + "\n")

    def started(self, node: Node) -> bool:
        self.alive()
        try:
            health = node.admin().call("health")
        except AgentdClientError:
            return False
        return bool(health["transport"].get("connected")) and (
            health["telemetry"].get("state") == "running"
        )

    def inboxes_ready(self, node: Node) -> bool:
        self.alive()
        transport = node.admin().call("health")["transport"]
        expected = len(NODES[node.node_id]) + (node.node_id == "jim-eq")
        return bool(transport.get("connected")) and (
            transport.get("ready_inbox_count") == expected
        )

    def ready(self, node: Node) -> bool:
        if not self.inboxes_ready(node):
            return False
        sessions = {
            c["agent_id"]
            for c in node.admin().call("connector.list")
            if c["session_active"]
        }
        return set(NODES[node.node_id]) <= sessions

    # -- observation ---------------------------------------------------------------
    def core_status(self) -> dict:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.core_port}/api/containment/status",
            headers={"X-EdgeCitadel-Admin-Token": self.admin_token},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return json.loads(response.read())
        except OSError:
            return {}

    def alert(self, body: dict) -> dict:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.core_port}/api/containment/alerts",
            data=json.dumps(body).encode(),
            headers={
                "X-EdgeCitadel-Admin-Token": self.admin_token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.loads(response.read())

    def monitor(self) -> sqlite3.Connection:
        path = self.run_dir / "core" / "containment.sqlite3"
        return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)

    def decisions(self) -> list[dict]:
        path = self.run_dir / "core" / "containment-decisions.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    def tasks(self) -> dict[str, list[dict]]:
        """Each agent's tasks as its own node records them."""
        out: dict[str, list[dict]] = {}
        for node_id, node in self.nodes.items():
            with closing(node.db()) as db:
                for task_id, recipient, state in db.execute(
                    "SELECT task_id,recipient_id,state FROM tasks"
                ):
                    if recipient in NODES[node_id]:
                        out.setdefault(recipient, []).append(
                            {"task_id": task_id, "state": state}
                        )
        return out

    def gate(self, name: str) -> None:
        (self.run_dir / "gates").mkdir(exist_ok=True)
        (self.run_dir / "gates" / name).touch()

    def marked(self, name: str) -> dict | None:
        path = self.run_dir / "marks" / name
        return json.loads(path.read_text()) if path.exists() else None


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------
def submit_root(deployment: Deployment) -> str:
    node = deployment.nodes["jim-eq"]
    token = (node.state / "connectors" / "operator.token").read_text().strip()
    operator = AgentdClient(node.socket, connector_id="operator", token=token)
    operator.call("session.open")
    task = operator.call(
        "task.create", recipient_id="kipp", payload={"body": "run the C2 workflow"}
    )
    return task["task_id"]


def artifact_version(deployment: Deployment) -> str | None:
    with closing(deployment.monitor()) as db:
        row = db.execute(
            "SELECT v.version_id FROM containment_versions v JOIN containment_invocations i "
            "ON i.attempt_id=v.producer_attempt WHERE i.agent_id='a1' AND v.entity='result'"
        ).fetchone()
    return row[0] if row else None


def consumed_by(deployment: Deployment, version: str, agent: str) -> bool:
    with closing(deployment.monitor()) as db:
        return (
            db.execute(
                "SELECT 1 FROM containment_edges e JOIN containment_invocations i "
                "ON e.dst='inv:'||i.attempt_id WHERE e.src=? AND e.etype='data' "
                "AND i.agent_id=?",
                (f"ver:{version}", agent),
            ).fetchone()
            is not None
        )


def agent_running(deployment: Deployment, agent: str) -> bool:
    with closing(deployment.monitor()) as db:
        return (
            db.execute(
                "SELECT 1 FROM containment_invocations WHERE agent_id=? AND ended_ms IS NULL",
                (agent,),
            ).fetchone()
            is not None
        )


def pending_effects(deployment: Deployment) -> int:
    return deployment.core_status().get("live", {}).get("pending_effects", 0)


WORKFLOW_KINDS = ("run", "task", "dispatch", "lineage", "effect")


def evidence_caught_up(deployment: Deployment) -> bool:
    """Core holds every workflow event the nodes recorded, and the monitor applied
    it, so every strategy decides on the same, complete G_P."""
    recorded: set[tuple] = set()
    marks = ",".join("?" * len(WORKFLOW_KINDS))
    for node in deployment.nodes.values():
        with closing(node.db()) as db:
            recorded.update(
                db.execute(
                    "SELECT node_id,source_epoch,event_id FROM trace_journal_all "
                    f"WHERE json_extract(event_json,'$.kind') IN ({marks})",
                    WORKFLOW_KINDS,
                )
            )
    core = deployment.run_dir / "core" / "openclaw.db"
    with closing(sqlite3.connect(core.as_uri() + "?mode=ro", uri=True)) as db:
        ingested = set(
            db.execute("SELECT node_id,source_epoch,event_id FROM trace_raw_events")
        )
    cursor = deployment.core_status().get("cursor", {})
    return recorded <= ingested and cursor.get("lag") == 0


def scheduled_effect(deployment: Deployment) -> dict:
    node = deployment.nodes["endurance"]
    with closing(node.db()) as db:
        rows = db.execute("SELECT effect_id,state FROM scheduled_effects").fetchall()
        events = [
            json.loads(row[0])
            for row in db.execute(
                "SELECT event_json FROM trace_journal_all "
                "WHERE json_extract(event_json,'$.kind')='effect' ORDER BY source_seq"
            )
        ]
    ledger = deployment.run_dir / "actuator-ledger.jsonl"
    return {
        "agentd_state": {effect_id: state for effect_id, state in rows},
        "events": [
            {
                "phase": e["phase"],
                "effect_id": e["attributes"]["effect_id"],
                "reason": e["attributes"]["reason"],
                "occurred_at": e["occurred_at"],
            }
            for e in events
        ],
        "actuator_ledger": [
            json.loads(line) for line in ledger.read_text().splitlines()
        ]
        if ledger.exists()
        else [],
    }


def enforced(deployment: Deployment) -> dict[str, dict]:
    """What each node holds, revoked or filters, from its own containment.list."""
    out = {}
    for node_id, node in deployment.nodes.items():
        listing = node.admin().call("containment.list")
        out[node_id] = {
            "holds": [
                {"kind": h["kind"], "object_id": h["object_id"], "owner": h["owner"]}
                for h in listing["holds"]
            ],
            "revocations": [
                {"kind": r["kind"], "object_id": r["object_id"]}
                for r in listing["revocations"]
            ],
            "ingress_filters": [
                {"authority": f["authority"], "target": f["target"]}
                for f in listing["ingress_filters"]
            ],
        }
    return out


def run_scenario(deployment: Deployment, variant: str, effect_margin_s: float) -> dict:
    timeline: dict[str, int] = {}
    root_task = submit_root(deployment)
    timeline["root_submitted"] = now_ms()
    if variant == "early":
        version = wait(lambda: artifact_version(deployment), 60, "the artifact version")
        wait(
            lambda: deployment.marked("a2-started")
            and agent_running(deployment, "a2")
            and consumed_by(deployment, version, "tars"),
            60,
            "a2 holding the artifact",
        )
    else:
        deployment.gate("a2-finish")
        version = wait(lambda: artifact_version(deployment), 60, "the artifact version")
        wait(
            lambda: deployment.marked("b1-scheduled")
            and pending_effects(deployment) == 1,
            90,
            "b1's scheduled actuator command in G_P",
        )
    timeline["condition_met"] = now_ms()
    wait(lambda: evidence_caught_up(deployment), 60, "Core and the monitor to catch up")
    timeline["alert_injected"] = now_ms()
    accepted = deployment.alert(
        {
            "kind": "validator_failure",
            "version_id": version,
            "strategy": deployment.strategy,
        }
    )
    decision = wait(
        lambda: next(
            (
                d
                for d in deployment.decisions()
                if d["type"] == "decision"
                and d["alert"]["alert_id"] == accepted["alert_id"]
            ),
            None,
        ),
        60,
        "the monitor's decision",
    )
    timeline["decision_logged"] = now_ms()
    # Let the contained workflow try to continue, and the healthy work finish.
    deployment.gate("a2-finish")
    deployment.gate("healthy-finish")
    scheduled = deployment.marked("b1-scheduled")
    settle_until = time.monotonic() + 20
    if scheduled:
        settle_until = max(
            settle_until,
            time.monotonic() + (scheduled["due"] - now_ms()) / 1000 + effect_margin_s,
        )

    def healthy_done() -> bool:
        tasks = deployment.tasks()
        return all(
            tasks.get(agent) and all(t["state"] in TERMINAL for t in tasks[agent])
            for agent in HEALTHY
        )

    # With a scheduled effect, observe past its due time whatever else happens.
    while time.monotonic() < settle_until and (scheduled or not healthy_done()):
        deployment.alive()
        time.sleep(0.5)
    # Receding-horizon re-solves that the continuation may have triggered.
    time.sleep(2)
    timeline["observed"] = now_ms()
    return {
        "root_task": root_task,
        "artifact_version": version,
        "alert": accepted,
        "timeline": timeline,
        "decision": decision,
        "follow_ups": [
            d
            for d in deployment.decisions()
            if d["type"] == "resolve" and d["incident_id"] == decision["incident_id"]
        ],
    }


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
def producer_attempt(deployment: Deployment) -> str:
    with closing(deployment.monitor()) as db:
        return db.execute(
            "SELECT attempt_id FROM containment_invocations WHERE agent_id='a1'"
        ).fetchone()[0]


def releases_follow_confirmations(decision: dict) -> bool:
    """No release was sent before every action of the chosen cut was confirmed."""
    chosen = {
        json.dumps(a, sort_keys=True) for a in decision["solve"].get("actions", [])
    }
    confirmed = [
        r["t_confirmed"]
        for r in decision["phase1"] + decision["phase2"]
        if json.dumps(r["action"], sort_keys=True) in chosen
    ]
    if any(t is None for t in confirmed):
        return not decision["released"]
    latest = max(confirmed, default=0)
    return all(r["t_release_sent"] >= latest for r in decision["released"])


def summarize(deployment: Deployment, outcome: dict) -> dict:
    decision = outcome["decision"]
    decisions = [decision, *outcome["follow_ups"]]
    status = deployment.core_status()
    tasks = deployment.tasks()
    owner_of = {t["task_id"]: agent for agent, ts in tasks.items() for t in ts}
    effect = scheduled_effect(deployment)

    def label(action: dict) -> str:
        subject = action["object_id"]
        if action["kind"] in {"task", "egress", "authority"}:
            subject = owner_of.get(subject, subject)
        elif action["kind"] == "effect":
            subject = "b1 actuator command"
        return f"{action['op']}:{action['kind']}({subject})@{action['node_id']}"

    actions = [
        {
            "action": label(r["action"]),
            "object_id": r["action"]["object_id"],
            "decision": index,
            "phase": r["phase"],
            "state": r["state"],
            "error": r["error"],
            "t_alert": r["t_alert"],
            "t_decision": r["t_decision"],
            "t_sent": r["t_sent"],
            "t_applied": r["t_applied"],
            "t_confirmed": r["t_confirmed"],
            "t_release_sent": r["t_release_sent"],
            "t_released": r["t_released"],
            "ms_after_alert": {
                key: None if r[key] is None else r[key] - r["t_alert"]
                for key in (
                    "t_decision",
                    "t_sent",
                    "t_applied",
                    "t_confirmed",
                    "t_release_sent",
                    "t_released",
                )
            },
            "sites": {
                site: {
                    "state": o["state"],
                    "gates": (o.get("enforcement") or {}).get("gates"),
                    "task": (o.get("enforcement") or {}).get("task"),
                    "effect_state": (o.get("enforcement") or {}).get("effect_state"),
                }
                for site, o in r["sites"].items()
            },
        }
        for index, d in enumerate(decisions)
        for r in d["phase1"] + d["phase2"]
    ]
    producer = producer_attempt(deployment)
    roots = decision["roots"]
    healthy = {agent: [t["state"] for t in tasks.get(agent, [])] for agent in HEALTHY}
    return {
        "strategy": deployment.strategy,
        "profile": decision["profile"],
        "mechanisms": decision["mechanisms"],
        "phase1_include_cut": decision["phase1_include_cut"],
        "status": decision["status"],
        "cut_valid": decision["cut_valid"],
        "follow_up_statuses": [d["status"] for d in outcome["follow_ups"]],
        "root_cause": {
            "roots": roots,
            "basis": decision["root_basis"],
            "producer_attempt": producer,
            "ranking": decision["ranking"][:3],
            "names_producer": any(
                root in {f"inv:{producer}", f"ver:{outcome['artifact_version']}"}
                for root in roots
            ),
        },
        "contamination": decision["contamination"],
        "graph": {"g_p": status.get("g_p"), "g_c": decision.get("graph")},
        "solve": decision.get("solve"),
        "actions": actions,
        "releases_after_confirmations": all(
            releases_follow_confirmations(d) for d in decisions
        ),
        "released": [label(r["action"]) for d in decisions for r in d["released"]],
        "in_force": [label(r["action"]) for r in status.get("in_force", [])],
        "unconfirmed": [label(r["action"]) for r in status.get("unconfirmed", [])],
        "controller_leases": status.get("control", {}).get("controller_leases"),
        "enforced_at_nodes": enforced(deployment),
        "effect": effect,
        "effect_performed": bool(effect["actuator_ledger"]),
        "healthy_tasks": healthy,
        "healthy_completed": sum(
            1 for states in healthy.values() if states and set(states) == {"completed"}
        ),
        "tasks": {agent: [t["state"] for t in ts] for agent, ts in tasks.items()},
        "timeline": outcome["timeline"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--strategy",
        default="firebreak",
        choices=[
            "firebreak",
            "trace_and_stop",
            "trace_and_quarantine",
            "sink_guard",
            "stop_team",
            "greedy_cut",
            "reject_source",
            "stop_downstream",
            "cut_without_deadlines",
        ],
    )
    parser.add_argument("--variant", choices=["early", "late"], default="late")
    parser.add_argument("--profile", choices=["deployed", "shared"], default="deployed")
    parser.add_argument("--phase1-include-cut", action="store_true")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--root", type=Path, default=None, help="directory for run directories"
    )
    parser.add_argument("--keep", action="store_true", help="keep the run directory")
    parser.add_argument("--effect-margin-s", type=float, default=5.0)
    args = parser.parse_args()
    if args.root is not None:
        args.root.mkdir(parents=True, exist_ok=True)
    run_dir = Path(
        tempfile.mkdtemp(prefix=f"c2-{args.strategy}-{args.variant}-", dir=args.root)
    )
    deployment = Deployment(
        run_dir,
        args.strategy,
        profile=args.profile,
        phase1_include_cut=args.phase1_include_cut,
    )
    try:
        deployment.start()
        outcome = run_scenario(deployment, args.variant, args.effect_margin_s)
        summary = {"variant": args.variant, **summarize(deployment, outcome)}
        summary["run_dir"] = str(run_dir)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(summary, indent=2, sort_keys=True))
        print(
            json.dumps(
                {
                    k: summary[k]
                    for k in (
                        "variant",
                        "strategy",
                        "profile",
                        "phase1_include_cut",
                        "status",
                        "cut_valid",
                        "effect_performed",
                        "healthy_completed",
                        "released",
                        "in_force",
                    )
                },
                indent=2,
            )
        )
        return 0
    finally:
        deployment.stop()
        if not args.keep:
            shutil.rmtree(run_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
