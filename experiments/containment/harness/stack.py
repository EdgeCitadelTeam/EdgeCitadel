"""The per-run experiment stack: services, NATS, agentd nodes, Core monitor, workers.

Every run gets a fresh stack (fresh state directories, broker, monitor database
and worker memory), so no hold, revocation or provenance outlives its run. The
same code drives both modes through a ``Host``:

- dry run: every process on this machine (``LocalHost``), one agentd node per
  workload node, all on one local NATS server;
- lab: the controller host (jim-eq) runs the services, NATS and the Core
  aggregator; each workload node's agentd and workers run on its host over SSH
  (``SSHHost``). Experiment processes use their own ports and directories and
  never touch the hosts' resident EdgeCitadel services.

Processes run from ``runtime_root``, an EdgeCitadel checkout that has the
integrated agentd and monitor (m3/integration), with this repository's harness
and det-worker on ``PYTHONPATH``.
"""

from __future__ import annotations

import json
import os
import secrets
import shlex
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import deploy
from .spec import Spec, derive

HARNESS_ROOT = Path(__file__).resolve().parents[1]  # experiments/containment
REPO_ROOT = HARNESS_ROOT.parents[1]
STARTUP_TIMEOUT_S = 60.0


class StackError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Hosts
# ---------------------------------------------------------------------------
@dataclass
class HostConfig:
    name: str
    address: str  # how the other hosts reach this one
    python: str
    repo: str  # checkout of this repository (harness + det-worker)
    runtime_root: str  # EdgeCitadel checkout with the integrated runtime
    work_root: str  # where run state lives on this host
    ssh: str | None = None  # "user@host"; None runs locally
    nats_server: str | None = None

    def pythonpath(self) -> str:
        return ":".join(
            [
                self.runtime_root,
                f"{self.runtime_root}/agent-runtime/src",
                f"{self.repo}/experiments/containment",
                f"{self.repo}/agent-packages/det-worker",
            ]
        )

    def base_env(self) -> dict[str, str]:
        return {
            "PYTHONPATH": self.pythonpath(),
            # Never write bytecode into the runtime checkout we do not own.
            "PYTHONDONTWRITEBYTECODE": "1",
            "NO_PROXY": "*",
            "no_proxy": "*",
        }


@dataclass
class Proc:
    name: str
    host: str
    pid: int
    log: str
    popen: subprocess.Popen[bytes] | None = None


class LocalHost:
    def __init__(self, config: HostConfig) -> None:
        self.config = config

    def start(
        self,
        name: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        cwd: str,
        log: str,
    ) -> Proc:
        Path(log).parent.mkdir(parents=True, exist_ok=True)
        with open(log, "ab") as handle:
            popen = subprocess.Popen(
                list(argv),
                cwd=cwd,
                env={**os.environ, **self.config.base_env(), **env},
                stdin=subprocess.DEVNULL,
                stdout=handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        return Proc(name, self.config.name, popen.pid, log, popen)

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        timeout: float = 60,
    ) -> str:
        result = subprocess.run(
            list(argv),
            cwd=cwd,
            env={**os.environ, **self.config.base_env(), **(env or {})},
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            raise StackError(
                f"{argv[1:4]} failed on {self.config.name}: {result.stderr.strip()[-2000:]}"
            )
        return result.stdout

    def alive(self, proc: Proc) -> bool:
        return proc.popen is not None and proc.popen.poll() is None

    def stop(self, proc: Proc, timeout: float = 10) -> None:
        if proc.popen is None or proc.popen.poll() is not None:
            return
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.popen.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.popen.wait(5)

    def write(self, path: str, data: bytes, mode: int = 0o600) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(mode)

    def read(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def exists(self, path: str) -> bool:
        return Path(path).exists()


class SSHHost:
    """Processes on a lab host; secrets travel in mode-600 files, never argv."""

    def __init__(self, config: HostConfig) -> None:
        assert config.ssh is not None
        self.config = config

    def _ssh(
        self, command: str, *, data: bytes | None = None, timeout: float = 60
    ) -> str:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", self.config.ssh, command],
            input=data,
            capture_output=True,
            timeout=timeout,
        )
        if result.returncode != 0:
            raise StackError(
                f"ssh {self.config.name}: {result.stderr.decode()[-2000:]}"
            )
        return result.stdout.decode()

    def _env_file(self, name: str, env: Mapping[str, str], directory: str) -> str:
        path = f"{directory}/.env.{name}"
        body = "".join(
            f"export {k}={shlex.quote(v)}\n"
            for k, v in {**self.config.base_env(), **env}.items()
        )
        self.write(path, body.encode())
        return path

    def start(
        self,
        name: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        cwd: str,
        log: str,
    ) -> Proc:
        env_file = self._env_file(name, env, str(Path(log).parent))
        command = (
            f"mkdir -p {shlex.quote(str(Path(log).parent))} && cd {shlex.quote(cwd)} && "
            f". {shlex.quote(env_file)} && setsid nohup {shlex.join(argv)} "
            f">> {shlex.quote(log)} 2>&1 < /dev/null & echo $!"
        )
        return Proc(name, self.config.name, int(self._ssh(command).strip()), log)

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        timeout: float = 60,
    ) -> str:
        directory = cwd or self.config.work_root
        env_file = self._env_file("run", env or {}, self.config.work_root)
        command = f"cd {shlex.quote(directory)} && . {shlex.quote(env_file)} && {shlex.join(argv)}"
        return self._ssh(command, timeout=timeout)

    def alive(self, proc: Proc) -> bool:
        return (
            self._ssh(f"kill -0 {proc.pid} 2>/dev/null && echo yes || echo no").strip()
            == "yes"
        )

    def stop(self, proc: Proc, timeout: float = 10) -> None:
        self._ssh(
            f"kill -TERM -- -{proc.pid} 2>/dev/null; for i in $(seq {int(timeout * 5)}); do "
            f"kill -0 {proc.pid} 2>/dev/null || exit 0; sleep 0.2; done; kill -KILL -- -{proc.pid} 2>/dev/null; true"
        )

    def write(self, path: str, data: bytes, mode: int = 0o600) -> None:
        directory = shlex.quote(str(Path(path).parent))
        self._ssh(
            f"umask 077 && mkdir -p {directory} && cat > {shlex.quote(path)} && chmod {mode:o} {shlex.quote(path)}",
            data=data,
        )

    def read(self, path: str) -> bytes:
        return self._ssh(f"cat {shlex.quote(path)}").encode()

    def exists(self, path: str) -> bool:
        return (
            self._ssh(f"test -e {shlex.quote(path)} && echo yes || echo no").strip()
            == "yes"
        )


def make_host(config: HostConfig) -> LocalHost | SSHHost:
    return SSHHost(config) if config.ssh else LocalHost(config)


# ---------------------------------------------------------------------------
# Stack configuration
# ---------------------------------------------------------------------------
@dataclass
class StackConfig:
    mode: str  # dryrun | lab
    hosts: dict[str, HostConfig]
    controller: str  # host running services, NATS and the Core aggregator
    node_hosts: dict[str, str]  # workload node id -> host
    ports: dict[str, int] = field(
        default_factory=dict
    )  # 0 or absent: pick free (local)
    tau_ms: float = 50.0
    monitor_overrides: dict[str, Any] = field(default_factory=dict)
    # How the monitor reaches agentd on other hosts. The integrated monitor's
    # control client only speaks to local admin sockets, so a lab run forwards
    # each remote node's socket over SSH ("ssh_forward") until the monitor
    # has its NATS control client; "local" requires every node on the controller.
    control_transport: str = "local"
    netem: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | Path) -> StackConfig:
        """Read a stack file; ``${VAR}`` in host fields expands from the environment
        and an empty ``repo`` means this checkout."""
        document = json.loads(Path(path).read_text())
        hosts = {}
        for name, value in document["hosts"].items():
            fields = {
                k: os.path.expandvars(v) if isinstance(v, str) else v
                for k, v in value.items()
            }
            fields["repo"] = fields.get("repo") or str(REPO_ROOT)
            unresolved = [
                k for k, v in fields.items() if isinstance(v, str) and "${" in v
            ]
            if unresolved:
                raise StackError(
                    f"host {name}: unset environment variables in {unresolved}"
                )
            hosts[name] = HostConfig(name=name, **fields)
        return cls(
            mode=document["mode"],
            hosts=hosts,
            controller=document["controller"],
            node_hosts=dict(document["node_hosts"]),
            ports=dict(document.get("ports", {})),
            tau_ms=float(document.get("tau_ms", 50.0)),
            monitor_overrides=dict(document.get("monitor_overrides", {})),
            control_transport=document.get("control_transport", "local"),
            netem=dict(document.get("netem", {})),
        )


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def http_json(
    url: str,
    *,
    data: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = 5,
) -> Any:
    request = urllib.request.Request(
        url,
        data=None if data is None else json.dumps(data).encode(),
        method="GET" if data is None else "POST",
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read() or b"null")


def wait_until(
    probe, what: str, timeout: float = STARTUP_TIMEOUT_S, interval: float = 0.2
) -> Any:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = probe()
            if value:
                return value
        except (OSError, StackError, urllib.error.URLError, ValueError) as error:
            last = error
        time.sleep(interval)
    raise StackError(f"{what} did not become ready ({last!r})")


# ---------------------------------------------------------------------------
# One run's stack
# ---------------------------------------------------------------------------
class RunStack:
    """Start, address and stop every process of one run."""

    def __init__(self, config: StackConfig, run_id: str, run_dir: Path) -> None:
        self.config = config
        self.run_id = run_id
        self.run_dir = run_dir  # on the controller host
        self.hosts = {name: make_host(h) for name, h in config.hosts.items()}
        self.procs: list[Proc] = []
        self.services_token = secrets.token_urlsafe(24)
        self.admin_token = secrets.token_urlsafe(24)
        self.nats_token = secrets.token_urlsafe(24)
        self.services_url = ""
        self.aggregator_url = ""

    # -- paths ------------------------------------------------------------------
    def host_dir(self, host: str) -> str:
        return f"{self.config.hosts[host].work_root}/{self.run_id}"

    def node_state(self, node: str) -> str:
        return f"{self.host_dir(self.config.node_hosts[node])}/nodes/{node}"

    def _port(self, name: str) -> int:
        configured = int(self.config.ports.get(name, 0))
        return configured or free_port()

    def _start(
        self,
        host: str,
        name: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
    ) -> Proc:
        log = f"{self.host_dir(host)}/logs/{name}.log"
        proc = self.hosts[host].start(
            name, argv, env=env or {}, cwd=cwd or self.host_dir(host), log=log
        )
        self.procs.append(proc)
        return proc

    def _python(self, host: str) -> str:
        return self.config.hosts[host].python

    # -- lifecycle --------------------------------------------------------------
    def up(
        self,
        spec: Spec,
        plan: Mapping[str, Any],
        *,
        strategy: str,
        control_latency_ms: Mapping[str, float],
        control_delay_ms: Mapping[str, float],
        enforce: bool = True,
    ) -> None:
        """Start everything; with ``enforce`` False the monitor gets no control
        endpoints (it decides and logs, nothing is applied: baseline runs)."""
        controller = self.config.controller
        for host in {controller, *self.config.node_hosts.values()}:
            self.hosts[host].write(f"{self.host_dir(host)}/.keep", b"")
        self._start_services(controller, spec)
        nats_url = self._start_nats(controller)
        for node in deploy.nodes(spec):
            if node not in self.config.node_hosts:
                raise StackError(
                    f"workload node '{node}' has no host in the stack config"
                )
        records = deploy.agent_records(spec)
        for node in deploy.nodes(spec):
            self._start_node(node, nats_url)
        for node in deploy.nodes(spec):
            self._install(node, records.get(node, []))
        root_node = plan["agents"][plan["root_agent"]]["node"]
        host = self.config.node_hosts[root_node]
        self.hosts[host].run(
            [
                self._python(host),
                "-m",
                "harness.nodectl",
                "operator",
                "--state",
                self.node_state(root_node),
                "--recipient",
                plan["root_agent"],
            ]
        )
        self._start_aggregator(
            controller,
            spec,
            strategy,
            control_latency_ms,
            control_delay_ms,
            nats_url,
            enforce,
        )
        plan_bytes = json.dumps(plan).encode()
        for agent in spec.agents:
            self._start_worker(
                agent.id,
                plan["agents"][agent.id]["node"],
                plan_bytes,
                deploy.slots(spec, agent.id),
            )
        for agent in spec.agents:
            host = self.config.node_hosts[plan["agents"][agent.id]["node"]]
            ready = f"{self.host_dir(host)}/workers/{agent.id}/ready"
            wait_until(lambda: self.hosts[host].exists(ready), f"det-worker {agent.id}")

    def _start_services(self, host: str, spec: Spec) -> None:
        directory = f"{self.host_dir(host)}/services"
        self.hosts[host].write(
            f"{directory}/services.token", self.services_token.encode()
        )
        port = self._port("services")
        bind = "127.0.0.1" if self.config.mode == "dryrun" else "0.0.0.0"
        self._start(
            host,
            "services",
            [
                self._python(host),
                "-m",
                "harness.services",
                "--dir",
                directory,
                "--host",
                bind,
                "--port",
                str(port),
                "--max-spawn",
                str(spec.rogue.max_dyn),
            ],
        )
        ready = f"{directory}/services.ready"
        wait_until(lambda: self.hosts[host].exists(ready), "services")
        port = json.loads(self.hosts[host].read(ready))["port"]
        self.services_url = f"http://{self.config.hosts[host].address}:{port}"
        wait_until(lambda: self.services("GET", "/health"), "services health")

    def _start_nats(self, host: str) -> str:
        port, monitor = self._port("nats"), self._port("nats_monitor")
        directory = f"{self.host_dir(host)}/nats"
        config = (
            f"port: {port}\nhttp_port: {monitor}\n"
            f"authorization {{ token: {json.dumps(self.nats_token)} }}\n"
            f"jetstream {{ store_dir: {json.dumps(directory + '/js')} }}\n"
        )
        self.hosts[host].write(f"{directory}/nats.conf", config.encode())
        binary = self.config.hosts[host].nats_server or "nats-server"
        self._start(host, "nats", [binary, "-c", f"{directory}/nats.conf"])
        url = f"nats://{self.config.hosts[host].address}:{port}"
        wait_until(
            lambda: http_json(f"http://127.0.0.1:{monitor}/healthz")
            if self.config.hosts[host].ssh is None
            else True,
            "NATS",
        )
        return url

    def _start_node(self, node: str, nats_url: str) -> None:
        host = self.config.node_hosts[node]
        state = self.node_state(node)
        python = self._python(host)
        self.hosts[host].run(
            [
                python,
                "-m",
                "harness.nodectl",
                "configure",
                "--state",
                state,
                "--node",
                node,
                "--nats-url",
                nats_url,
            ],
            env={"HARNESS_NATS_TOKEN": self.nats_token},
        )
        self._start(
            host,
            f"agentd-{node}",
            [python, "-m", "harness.nodectl", "serve", "--state", state],
            env={"EDGECITADEL_TRACE_SYNC": "1"},
        )
        wait_until(
            lambda: self.hosts[host].exists(f"{state}/agentd/admin.token"),
            f"agentd {node}",
        )

    def _install(self, node: str, records: list[dict[str, Any]]) -> None:
        host = self.config.node_hosts[node]
        state = self.node_state(node)
        path = f"{state}/agents.json"
        self.hosts[host].write(path, json.dumps(records).encode())
        wait_until(
            lambda: self.hosts[host].run(
                [
                    self._python(host),
                    "-m",
                    "harness.nodectl",
                    "install",
                    "--state",
                    state,
                    "--agents",
                    path,
                ]
            ),
            f"agent registration on {node}",
            timeout=20,
            interval=0.5,
        )

    def _start_aggregator(
        self,
        host: str,
        spec: Spec,
        strategy: str,
        latency: Mapping[str, float],
        delay: Mapping[str, float],
        nats_url: str,
        enforce: bool,
    ) -> None:
        directory = f"{self.host_dir(host)}/core"
        python = self._python(host)
        db = f"{directory}/openclaw.db"
        self.hosts[host].write(f"{directory}/.keep", b"")
        # Create Core's trace store before the monitor thread starts: the
        # monitor otherwise races the collector's own initialization.
        self.hosts[host].run(
            [
                python,
                "-c",
                "import sqlite3,sys\nfrom aggregator import trace_store\nc=sqlite3.connect(sys.argv[1]); trace_store.initialize(c); c.commit(); c.close()",
                db,
            ],
            cwd=self.config.hosts[host].runtime_root,
        )
        control = {}
        for node in deploy.nodes(spec) if enforce else ():
            if self.config.node_hosts[node] == host:
                control[node] = {
                    "socket": self._socket(node),
                    "admin_token_file": f"{self.node_state(node)}/agentd/admin.token",
                }
            elif self.config.control_transport == "ssh_forward":
                control[node] = self._forward_control(node, host)
        unreachable = [n for n in deploy.nodes(spec) if enforce and n not in control]
        if unreachable:
            raise StackError(
                f"the monitor cannot reach agentd on {unreachable}; set control_transport"
            )
        document = {
            "monitor": deploy.monitor_config(
                spec,
                strategy=strategy,
                control_latency_ms=latency,
                tau_ms=self.config.tau_ms,
                overrides=self.config.monitor_overrides,
            ),
            "control": {"agentd": control},
        }
        self.hosts[host].write(
            f"{directory}/containment.json", json.dumps(document, indent=1).encode()
        )
        port = self._port("aggregator")
        bind = "127.0.0.1" if self.config.mode == "dryrun" else "0.0.0.0"
        self._start(
            host,
            "aggregator",
            [
                python,
                "-m",
                "uvicorn",
                "harness.aggregator_shim:app",
                "--host",
                bind,
                "--port",
                str(port),
                "--log-level",
                "warning",
            ],
            cwd=self.config.hosts[host].runtime_root,
            env={
                "NATS_URL": nats_url,
                "NATS_TOKEN": self.nats_token,
                "DB_PATH": db,
                "EDGECITADEL_TRACE_COLLECTOR": "1",
                "EDGECITADEL_CONTAINMENT_MONITOR": "1",
                "EDGECITADEL_CONTAINMENT_CONFIG": f"{directory}/containment.json",
                "EDGECITADEL_ADMIN_TOKEN": self.admin_token,
                "HARNESS_CONTROL_DELAY_MS": json.dumps(dict(delay)),
            },
        )
        self.aggregator_url = f"http://{self.config.hosts[host].address}:{port}"
        wait_until(
            lambda: self.monitor_status().get("state") == "running", "Core monitor"
        )

    def _forward_control(self, node: str, controller: str) -> dict[str, str]:
        """Forward a remote node's admin socket to the controller over SSH."""
        remote = self.config.hosts[self.config.node_hosts[node]]
        directory = Path(self.host_dir(controller)) / "control"
        token = directory / f"{node}.admin.token"
        self.hosts[controller].write(
            str(token),
            self.hosts[remote.name].read(f"{self.node_state(node)}/agentd/admin.token"),
        )
        # Unix socket paths are short (108 bytes); keep the forward under /tmp.
        local = f"/tmp/ec-{secrets.token_hex(6)}-{node}.sock"
        self._start(
            controller,
            f"control-forward-{node}",
            [
                "ssh",
                "-N",
                "-o",
                "BatchMode=yes",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "StreamLocalBindUnlink=yes",
                "-L",
                f"{local}:{self._socket(node)}",
                str(remote.ssh),
            ],
        )
        wait_until(lambda: Path(local).exists(), f"control forward to {node}")
        return {"socket": local, "admin_token_file": str(token)}

    # -- lab network profiles ------------------------------------------------------
    def apply_netem(self, profile: str | None) -> bool:
        """Install a latency profile on every host when the stack file allows it."""
        if not profile or not self.config.netem.get("apply"):
            return False
        from .netem import commands

        for host, lines in commands(self.config.netem, profile).items():
            for line in lines:
                self.hosts[host].run(["sh", "-c", line])
        return True

    def clear_netem(self) -> None:
        from .netem import commands

        for host, lines in commands(self.config.netem, "", clear=True).items():
            for line in lines:
                self.hosts[host].run(["sh", "-c", line])

    def _socket(self, node: str) -> str:
        host = self.config.node_hosts[node]
        out = self.hosts[host].run(
            [
                self._python(host),
                "-c",
                "import sys\nfrom pathlib import Path\nfrom edgecitadel_agentd.service import socket_path_for\nprint(socket_path_for(Path(sys.argv[1])))",
                f"{self.node_state(node)}/agentd",
            ]
        )
        return out.strip()

    def _start_worker(self, agent: str, node: str, plan: bytes, slots: int) -> None:
        host = self.config.node_hosts[node]
        directory = f"{self.host_dir(host)}/workers/{agent}"
        self.hosts[host].write(f"{directory}/plan.json", plan, mode=0o644)
        self._start(
            host,
            f"worker-{agent}",
            [
                self._python(host),
                "-m",
                "det_worker",
                "--plan",
                f"{directory}/plan.json",
                "--agent",
                agent,
                "--services",
                self.services_url,
                "--slots",
                str(slots),
                "--work-dir",
                directory,
            ],
            cwd=f"{self.config.hosts[host].repo}/agent-packages/det-worker",
            env={
                "EDGECITADEL_STATE_DIR": self.node_state(node),
                "EDGECITADEL_CONNECTOR_ID": f"managed-{agent}",
                "DET_WORKER_SERVICES_TOKEN": self.services_token,
            },
        )

    def down(self) -> list[str]:
        """Stop every process; returns the ones that had died early.

        The Core aggregator goes first: stopping workers ends their sessions,
        which agentd records as interrupted invocations, and a running monitor
        would answer those with lease-violation decisions after the run ended.
        """
        died = []
        first = [p for p in self.procs if p.name == "aggregator"]
        rest = [p for p in reversed(self.procs) if p.name != "aggregator"]
        for proc in first + rest:
            host = self.hosts[proc.host]
            try:
                if not host.alive(proc):
                    died.append(proc.name)
                host.stop(proc)
            except StackError:
                died.append(proc.name)
        self.procs.clear()
        return died

    # -- endpoints ----------------------------------------------------------------
    def services(self, method: str, path: str, data: Any = None) -> Any:
        return http_json(
            self.services_url + path,
            data=data if method == "POST" else None,
            headers={"X-Harness-Token": self.services_token},
        )

    def monitor_status(self) -> dict[str, Any]:
        return http_json(
            self.aggregator_url + "/api/containment/status",
            headers={"x-edgecitadel-admin-token": self.admin_token},
        )

    def post_alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        return http_json(
            self.aggregator_url + "/api/containment/alerts",
            data=dict(alert),
            headers={"x-edgecitadel-admin-token": self.admin_token},
        )

    def submit(self, node: str, recipient: str, body: Mapping[str, Any]) -> str:
        host = self.config.node_hosts[node]
        path = f"{self.node_state(node)}/submit-{secrets.token_hex(4)}.json"
        self.hosts[host].write(path, json.dumps(dict(body)).encode())
        out = self.hosts[host].run(
            [
                self._python(host),
                "-m",
                "harness.nodectl",
                "submit",
                "--state",
                self.node_state(node),
                "--recipient",
                recipient,
                "--body",
                path,
            ]
        )
        return json.loads(out)["task_id"]

    def attempt_of(self, task_id: str) -> str | None:
        """The execution attempt Core recorded for a task (None until ingested)."""
        host = self.config.controller
        out = self.hosts[host].run(
            [
                self._python(host),
                "-m",
                "harness.coreq",
                "attempt",
                "--db",
                f"{self.host_dir(host)}/core/openclaw.db",
                "--task",
                task_id,
            ],
            cwd=self.config.hosts[host].runtime_root,
        )
        return json.loads(out)["attempt_id"]

    def fetch(self, host: str, path: str) -> bytes | None:
        try:
            return (
                self.hosts[host].read(path) if self.hosts[host].exists(path) else None
            )
        except StackError:
            return None

    def roots(self, spec: Spec) -> tuple[str, ...]:
        return derive(spec).roots
