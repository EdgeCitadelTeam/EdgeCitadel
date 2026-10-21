"""The per-run experiment stack: services, NATS, agentd nodes, Core monitor, workers.

Every run gets a fresh stack (fresh state directories, broker, monitor database
and worker memory), so no hold, revocation or provenance outlives its run. The
same code drives both modes through a ``Host``:

- dry run: every process on this machine (``LocalHost``), one agentd node per
  workload node, all on one local NATS server;
- lab: the controller host (jim-eq) runs the services, NATS and the Core
  aggregator; each workload node's agentd and workers run on its host over SSH
  (``SSHHost``) or, for a host listed with ``helper``, through that host's
  ``harness.labhelper`` over the lab's NATS (``HelperHost``: no SSH; start,
  stop and collect harness processes and run directories only, under the
  host's work root). Experiment processes use their own ports and directories
  and never touch the hosts' resident EdgeCitadel services.

Processes run from ``runtime_root``, an EdgeCitadel checkout that has the
agentd and monitor of this revision (usually this checkout), with this
repository's harness and det-worker on ``PYTHONPATH``.

Every agentd node enforces the fail-closed controller lease
(``EDGECITADEL_CONTROLLER_LEASE_MS``, ``controller_lease_ms``, 30 s by default)
and the monitor renews it from its start in every run, including runs that
must not contain (``enforce: false``: the monitor decides and logs but sends
no decision). Effects are performed by agentd's effect gateway: through the
run's ledger (``http``, every node), and with an ``effect_sink`` of kind
``homeassistant`` also through Home Assistant (fronted by the run's services)
for the effect classes its ``targets`` map, on the ``nodes`` it names (every
node by default).
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import threading
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
    # The local address this host's cross-host services (the run's NATS and
    # services) listen on, and the other hosts reach them at: the tailnet
    # address in the lab. Defaults to ``address``.
    listen: str | None = None
    # Drive this host through its labhelper instead of SSH: {"nats_url": the
    # lab broker, "token_file": the broker token's file on the runner's host,
    # "subject": the helper's subject (default harness.lab.<name>)}.
    helper: dict[str, str] | None = None
    # An SSH host's processes as transient systemd units (``user`` or
    # ``system``) named <unit_prefix><run>-<process>, checked and stopped
    # through systemd; None: background process groups.
    units: str | None = None
    # A stack's own unit namespace: an attempt stops only the leftover units
    # of its prefix, so two stacks on one host never stop each other's (the
    # scale testbed uses firebreak-exp-s- beside the evaluation's -h-).
    unit_prefix: str = "firebreak-exp-h-"

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
    unit: str | None = None  # the labhelper's unit (HelperHost)


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

    def glob(self, directory: str, patterns: Sequence[str]) -> list[str]:
        return [
            str(p)
            for pattern in patterns
            for p in sorted(Path(directory).glob(pattern))
        ]

    def remove_tree(self, path: str) -> None:
        shutil.rmtree(path, ignore_errors=True)


UNIT_PREFIX = "firebreak-exp-h-"
# A runner started with ``python -m harness unit`` (never stopped by cleanup).
RUNNER_UNIT = "firebreak-exp-h-runner"


def unit_name(log: str, name: str, prefix: str = UNIT_PREFIX) -> str:
    """The unit of a run's process (its log is <work_root>/<run>/logs/<name>.log)."""
    run_id = Path(log).parent.parent.name
    return prefix + re.sub(r"[^A-Za-z0-9_.:-]+", "-", f"{run_id}-{name}")[:200]


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
        q = shlex.quote
        if self.config.units:
            unit = unit_name(log, name, self.config.unit_prefix)
            source = '. "$0" && exec "$@"'
            command = (
                f"mkdir -p {q(str(Path(log).parent))} && "
                f"systemd-run {self._scope()}--quiet --collect --unit={q(unit)} "
                f"-p Type=exec -p TimeoutStopSec=10 -p WorkingDirectory={q(cwd)} "
                f"-p StandardOutput=append:{q(log)} -p StandardError=append:{q(log)} "
                f"/bin/bash -c {q(source)} {q(env_file)} {shlex.join(argv)} "
                f"&& systemctl {self._scope()}show -p MainPID --value {q(unit)}"
            )
            pid = int(self._ssh(command).strip().splitlines()[-1])
            return Proc(name, self.config.name, pid, log, unit=unit)
        command = (
            f"mkdir -p {q(str(Path(log).parent))} && cd {q(cwd)} && "
            f". {q(env_file)} && setsid nohup {shlex.join(argv)} "
            f">> {q(log)} 2>&1 < /dev/null & echo $!"
        )
        return Proc(name, self.config.name, int(self._ssh(command).strip()), log)

    def _scope(self) -> str:
        return "--user " if self.config.units == "user" else ""

    def stop_leftover_units(self) -> list[str]:
        """Stop the harness units an interrupted run left (never a runner)."""
        prefix = self.config.unit_prefix
        listing = self._ssh(
            f"systemctl {self._scope()}list-units --all --no-legend --plain "
            f"'{prefix}*' | awk '{{print $1}}'"
        )
        left = [
            unit
            for unit in listing.split()
            if unit.startswith(prefix)
            and not unit.startswith((RUNNER_UNIT, f"{prefix}runner"))
        ]
        if left:
            units = " ".join(shlex.quote(u) for u in left)
            self._ssh(
                f"systemctl {self._scope()}stop {units}; "
                f"systemctl {self._scope()}reset-failed {units} 2>/dev/null; true",
                timeout=90,
            )
        return left

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
        if proc.unit:
            check = (
                f"systemctl {self._scope()}is-active --quiet {shlex.quote(proc.unit)}"
            )
            return self._ssh(f"{check} && echo yes || echo no").strip() == "yes"
        return (
            self._ssh(f"kill -0 {proc.pid} 2>/dev/null && echo yes || echo no").strip()
            == "yes"
        )

    def stop(self, proc: Proc, timeout: float = 10) -> None:
        if proc.unit:
            self._ssh(
                f"systemctl {self._scope()}stop {shlex.quote(proc.unit)} 2>/dev/null; true",
                timeout=60,
            )
            return
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

    def glob(self, directory: str, patterns: Sequence[str]) -> list[str]:
        paths = " ".join(f"{shlex.quote(directory)}/{p}" for p in patterns)
        return self._ssh(f"ls -d {paths} 2>/dev/null || true").split()

    def remove_tree(self, path: str) -> None:
        self._ssh(f"rm -rf -- {shlex.quote(path)}")


class HelperClient:
    """A NATS connection to the lab broker on its own event-loop thread, shared
    by every run that drives the same helper subject."""

    _shared: dict[tuple[str, str], HelperClient] = {}

    def __init__(self, url: str, token: str) -> None:
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        self.nc = self._wait(self._connect(url, token), 20)

    @classmethod
    def for_helper(cls, url: str, token_file: str, subject: str) -> HelperClient:
        key = (url, subject)
        if key not in cls._shared:
            path = Path(token_file)
            if path.is_symlink() or not path.is_file():
                raise StackError(f"labhelper {subject}: no broker token file")
            cls._shared[key] = cls(url, path.read_text().strip())
        return cls._shared[key]

    @staticmethod
    async def _connect(url: str, token: str) -> Any:
        from nats.aio.client import Client as NATS

        nc = NATS()
        await nc.connect(
            servers=[url],
            token=token,
            connect_timeout=3,
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
        )
        return nc

    def _wait(self, coroutine: Any, timeout: float) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout)

    def close(self) -> None:
        for key, client in list(self._shared.items()):
            if client is self:
                del self._shared[key]
        self._wait(self.nc.close(), 10)
        self.loop.call_soon_threadsafe(self.loop.stop)

    def request(self, subject: str, payload: Mapping[str, Any], timeout: float) -> Any:
        async def call() -> Any:
            message = await self.nc.request(
                subject, json.dumps(payload).encode(), timeout=timeout
            )
            return json.loads(message.data)

        try:
            return self._wait(call(), timeout + 5)
        except Exception as error:  # noqa: BLE001 - a transport failure of the lab path
            raise StackError(f"labhelper {subject}: {type(error).__name__}") from None


class HelperHost:
    """A lab host driven through its ``harness.labhelper`` over NATS (no SSH).

    The helper starts harness processes as ``firebreak-exp-h-*`` units, runs
    short ``nodectl`` commands and reads and writes files, all under the host's
    work root; secrets travel in the request and land in mode-600 files there.
    """

    def __init__(self, config: HostConfig) -> None:
        assert config.helper is not None
        from .labhelper import subject_for

        self.config = config
        self.subject = config.helper.get("subject") or subject_for(config.name)
        self.client = HelperClient.for_helper(
            config.helper["nats_url"], config.helper["token_file"], self.subject
        )

    def _call(self, op: str, args: Mapping[str, Any], wait: float = 60) -> Any:
        reply = self.client.request(self.subject, {"op": op, "args": dict(args)}, wait)
        if not reply.get("ok"):
            raise StackError(f"labhelper {self.config.name} {op}: {reply.get('error')}")
        return reply["value"]

    def start(
        self,
        name: str,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        cwd: str,
        log: str,
    ) -> Proc:
        value = self._call(
            "start",
            {
                "name": name,
                "argv": list(argv),
                "env": {**self.config.base_env(), **env},
                "cwd": cwd,
                "log": log,
            },
        )
        return Proc(name, self.config.name, int(value["pid"]), log, unit=value["unit"])

    def run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
        timeout: float = 60,
    ) -> str:
        return str(
            self._call(
                "run",
                {
                    "argv": list(argv),
                    "env": {**self.config.base_env(), **(env or {})},
                    "cwd": cwd,
                    "timeout": timeout,
                },
                wait=timeout + 10,
            )
        )

    def alive(self, proc: Proc) -> bool:
        return bool(self._call("alive", {"unit": proc.unit}))

    def stop(self, proc: Proc, timeout: float = 10) -> None:
        self._call("stop", {"unit": proc.unit, "timeout": timeout}, wait=timeout + 30)

    def write(self, path: str, data: bytes, mode: int = 0o600) -> None:
        payload = base64.b64encode(data).decode()
        self._call("write", {"path": path, "data": payload, "mode": mode})

    def read(self, path: str) -> bytes:
        chunks, offset = [], 0
        while True:
            value = self._call("read", {"path": path, "offset": offset})
            chunk = base64.b64decode(value["data"])
            chunks.append(chunk)
            offset += len(chunk)
            if value["eof"] or not chunk:
                return b"".join(chunks)

    def exists(self, path: str) -> bool:
        return bool(self._call("exists", {"path": path}))

    def glob(self, directory: str, patterns: Sequence[str]) -> list[str]:
        return list(
            self._call("glob", {"directory": directory, "patterns": list(patterns)})
        )

    def remove_tree(self, path: str) -> None:
        self._call("remove", {"path": path})


Host = LocalHost | SSHHost | HelperHost


def make_host(config: HostConfig) -> Host:
    if config.helper:
        return HelperHost(config)
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
    # How the monitor reaches agentd: "local" admin sockets (every node on the
    # controller), "nats" (owner-signed commands on the run's NATS, agentd's
    # edgecitadel.control.v1.<node> subject; needs a monitor with
    # NatsControlClient, m3/integration ab174ed or later), or "ssh_forward"
    # (remote admin sockets forwarded over SSH, for older monitors).
    control_transport: str = "local"
    netem: dict[str, Any] = field(default_factory=dict)
    # Where agentd's effect gateway performs effects: {"kind": "ledger"} (the
    # run's services, through the gateway's http adapter) or {"kind":
    # "homeassistant", "targets": {"<class>" | "*": "<domain>.<service>/<entity>"},
    # "upstream": <Home Assistant URL> | null, "token_file": <path on every node
    # host> | null}. Without an upstream (dry run) the services answer as Home
    # Assistant for a token the run generates; with one they forward the call.
    effect_sink: dict[str, Any] = field(default_factory=lambda: {"kind": "ledger"})
    # agentd's fail-closed controller lease on every node, renewed by the monitor.
    controller_lease_ms: int = 30_000
    # Which system runs the workload (SYSTEMS): EdgeCitadel with provenance and
    # the monitor; EdgeCitadel with trace export, controller lease and monitor
    # off (the transport and runtime alone, for overhead runs); the Google
    # A2A baseline (harness.a2a_stack); or the LangGraph baseline
    # (harness.lg_stack, one host).
    system: str = "edgecitadel"
    # Sample every run process's CPU and memory at this period (harness.procstat);
    # None leaves sampling off (the containment runs' default).
    procstat_interval_ms: int | None = None
    # Where the A2A baseline keeps its tasks: a2a-sdk's in-memory store, or
    # SQLite (durable, like EdgeCitadel's journal) for overhead comparisons.
    a2a_task_store: str = "memory"
    # Extra EDGECITADEL_* settings for agentd and the Managed Agent runtime on
    # every node (the lab stacks: the M7 fallback re-checks at 1 s; agentd's
    # own default is 50 ms). The harness's own settings cannot be overridden.
    runtime_env: dict[str, str] = field(default_factory=dict)

    def plan_sink(self) -> dict[str, Any]:
        """The workers' view of the sink (det_worker.effects)."""
        if self.effect_sink["kind"] == "homeassistant":
            return {"adapter": "homeassistant", "targets": self.effect_sink["targets"]}
        return {"adapter": "http"}

    def netem_rtt_ms(self, profile: str | None) -> float:
        from .netem import added_rtt_ms

        return added_rtt_ms(self.netem, profile)

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
            helper = fields.get("helper")
            if helper is not None:
                helper = {k: os.path.expandvars(v) for k, v in dict(helper).items()}
                if set(helper) - {"nats_url", "token_file", "subject"} or not (
                    helper.get("nats_url") and helper.get("token_file")
                ):
                    raise StackError(
                        f"host {name}: helper takes nats_url, token_file and subject"
                    )
                if any("${" in v for v in helper.values()):
                    raise StackError(
                        f"host {name}: unset environment variables in helper"
                    )
                fields["helper"] = helper
            if fields.get("units") not in (None, "user", "system"):
                raise StackError(f"host {name}: units is user or system")
            if "unit_prefix" in fields and not re.fullmatch(
                r"firebreak-exp-[a-z0-9]+-", str(fields["unit_prefix"])
            ):
                raise StackError(
                    f"host {name}: unit_prefix is firebreak-exp-<name>- (lower case)"
                )
            hosts[name] = HostConfig(name=name, **fields)
        transport = _transport(document.get("control_transport", "local"))
        if transport != "nats" and any(h.helper for h in hosts.values()):
            raise StackError("hosts driven by a labhelper need control_transport nats")
        return cls(
            mode=document["mode"],
            hosts=hosts,
            controller=document["controller"],
            node_hosts=dict(document["node_hosts"]),
            ports=dict(document.get("ports", {})),
            tau_ms=float(document.get("tau_ms", 50.0)),
            monitor_overrides=dict(document.get("monitor_overrides", {})),
            control_transport=transport,
            netem=_netem(document.get("netem", {})),
            effect_sink=_effect_sink(document.get("effect_sink", {"kind": "ledger"})),
            controller_lease_ms=_lease(document.get("controller_lease_ms", 30_000)),
            system=_system(document.get("system", "edgecitadel")),
            procstat_interval_ms=_interval(document.get("procstat_interval_ms")),
            a2a_task_store=_task_store(document.get("a2a_task_store", "memory")),
            runtime_env=_runtime_env(document.get("runtime_env", {})),
        )


SYSTEMS = (
    "edgecitadel",
    "edgecitadel_bare",
    "a2a",
    "langgraph",
    # timing only: the same workload in the functional API (harness.lg_func)
    "langgraph_functional",
)
CONTROL_TRANSPORTS = ("local", "nats", "ssh_forward")


def _interval(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 50 <= value <= 10_000:
        raise StackError("procstat_interval_ms must be an integer from 50 to 10000")
    return value


# Set by the harness per node or run; a stack file cannot override them.
HARNESS_RUNTIME_ENV = frozenset(
    {
        "EDGECITADEL_TRACE_SYNC",
        "EDGECITADEL_CONTROLLER_LEASE_MS",
        "EDGECITADEL_EFFECT_ADAPTERS",
        "EDGECITADEL_STATE_DIR",
        "EDGECITADEL_CONNECTOR_ID",
    }
)
RUNTIME_ENV_NAME = re.compile(r"^EDGECITADEL_[A-Z0-9_]+$")


def _runtime_env(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or not all(
        isinstance(k, str)
        and RUNTIME_ENV_NAME.fullmatch(k)
        and k not in HARNESS_RUNTIME_ENV
        and isinstance(v, str)
        for k, v in value.items()
    ):
        raise StackError(
            "runtime_env maps EDGECITADEL_* names (not the harness's own) to strings"
        )
    return dict(value)


def _task_store(name: str) -> str:
    if name not in ("memory", "sqlite"):
        raise StackError("a2a_task_store must be memory or sqlite")
    return name


def _system(name: str) -> str:
    if name not in SYSTEMS:
        raise StackError(f"system must be one of {SYSTEMS}")
    return name


# The NATS control client retries one command until this timeout, then records
# it as unconfirmed (neither in force nor absent).
NATS_CONFIRMATION_TIMEOUT_MS = 5000


def _netem(document: Mapping[str, Any]) -> dict[str, Any]:
    from .netem import check_profiles

    check_profiles(document)
    return dict(document)


def _effect_sink(document: Mapping[str, Any]) -> dict[str, Any]:
    from ._worker import effects

    def expand(value: Any) -> Any:
        if isinstance(value, str):
            return os.path.expandvars(value)
        if isinstance(value, Mapping):
            return {k: expand(v) for k, v in value.items()}
        return value

    sink = expand(dict(document))
    kind = sink.get("kind")
    if kind == "ledger" and set(sink) == {"kind"}:
        return sink
    if kind != "homeassistant" or set(sink) - {
        "kind",
        "targets",
        "upstream",
        "token_file",
        "nodes",
    }:
        raise StackError("effect_sink: kind ledger, or homeassistant with targets")
    nodes = sink.get("nodes")
    if nodes is not None and (
        not isinstance(nodes, list) or not all(isinstance(n, str) for n in nodes)
    ):
        raise StackError("effect_sink: nodes lists the workload nodes with the adapter")
    unresolved = [k for k, v in sink.items() if "${" in json.dumps(v)]
    if unresolved:
        raise StackError(f"effect_sink: unset environment variables in {unresolved}")
    if bool(sink.get("upstream")) != bool(sink.get("token_file")):
        raise StackError(
            "effect_sink: a Home Assistant upstream needs the token_file its "
            "nodes read, and only an upstream does"
        )
    try:
        effects.check_sink({"adapter": "homeassistant", "targets": sink.get("targets")})
    except ValueError as error:
        raise StackError(f"effect_sink: {error}") from None
    return {
        "kind": kind,
        "targets": dict(sink["targets"]),
        "upstream": sink.get("upstream") or None,
        "token_file": sink.get("token_file") or None,
        "nodes": nodes,
    }


def _lease(value: object) -> int:
    if type(value) is not int or value < 1000:
        raise StackError("controller_lease_ms must be an integer of at least 1000")
    return value


def _transport(name: str) -> str:
    if name not in CONTROL_TRANSPORTS:
        raise StackError(f"control_transport must be one of {CONTROL_TRANSPORTS}")
    return name


PORT_WAIT_S = 30.0


def port_free(address: str, port: int) -> bool:
    """Whether a server could bind ``address:port`` now (as ours do, with
    address reuse, so connections in TIME_WAIT do not count)."""
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((address, port))
        except OSError:
            return False
    return True


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
        # The gateway's adapters authenticate to the sink with this path token.
        self.sink_token = secrets.token_urlsafe(24)
        # Dry-run Home Assistant: the services accept this token in its place.
        self.ha_token = secrets.token_urlsafe(32)
        self.admin_token = secrets.token_urlsafe(24)
        self.nats_token = secrets.token_urlsafe(24)
        self.services_url = ""
        self.aggregator_url = ""
        self.bare = config.system == "edgecitadel_bare"

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

    def _listen(self, host: str) -> str:
        """Where a service the other hosts use listens (and is reached): the
        loopback in a dry run, the host's tailnet address in the lab."""
        if self.config.mode == "dryrun":
            return "127.0.0.1"
        return self.config.hosts[host].listen or self.config.hosts[host].address

    def _runner_listen(self, host: str) -> str:
        """Where a service only the runner uses listens: the loopback when the
        runner runs on that host, else where the other hosts reach it."""
        if self.config.mode == "dryrun" or isinstance(self.hosts[host], LocalHost):
            return "127.0.0.1"
        return self._listen(host)

    # -- lifecycle --------------------------------------------------------------
    def prepare(self) -> list[str]:
        """Before an attempt, clear what an interrupted one left: harness units
        on unit-driven hosts, this run's directories, and the stack's fixed
        ports on the controller (waited for, never taken from another
        process). Returns what it did."""
        notes = []
        for name, host in self.hosts.items():
            if isinstance(host, SSHHost) and host.config.units:
                left = host.stop_leftover_units()
                if left:
                    notes.append(f"{name}: stopped leftover units {left[:4]}")
            directory = self.host_dir(name)
            if host.exists(directory):
                host.remove_tree(directory)
                notes.append(f"{name}: removed {directory}")
        controller = self.config.controller
        if isinstance(self.hosts[controller], LocalHost):
            where = {
                "nats": self._listen(controller),
                "services": self._listen(controller),
                "nats_monitor": "127.0.0.1",
                "aggregator": self._runner_listen(controller),
            }
            wanted = {
                (where.get(k, "127.0.0.1"), int(v))
                for k, v in self.config.ports.items()
                if v
            }
            deadline = time.monotonic() + PORT_WAIT_S
            while busy := sorted(a for a in wanted if not port_free(*a)):
                if time.monotonic() > deadline:
                    raise StackError(
                        f"ports still in use after {PORT_WAIT_S:.0f} s: {busy}"
                    )
                time.sleep(1)
        return notes

    def up(
        self,
        spec: Spec,
        plan: Mapping[str, Any],
        *,
        strategy: str,
        control_latency_ms: Mapping[str, float],
        control_delay_ms: Mapping[str, float],
        enforce: bool = True,
        profile: str = "deployed",
        phase1_mode: str | None = None,
        phase1_labels: str = "frontier",
        receding_during_waits: bool = False,
        monitor: Mapping[str, Any] | None = None,
    ) -> None:
        """Start everything. With ``enforce`` False the monitor decides and logs
        but sends no decision (baseline runs); it still renews every node's
        controller lease, so effects pass in those runs too. ``monitor`` adds
        the scenario's own monitor settings (``svc.monitor_settings``)."""
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
        records = deploy.agent_records(spec, plan["effect_sink"])
        for node in deploy.nodes(spec):
            self._start_node(node, nats_url)
        for node in deploy.nodes(spec):
            self._install(node, records.get(node, []))
        for node, agents in deploy.external_recipients(spec).items():
            host = self.config.node_hosts[node]
            self.hosts[host].run(
                [
                    self._python(host),
                    "-m",
                    "harness.nodectl",
                    "operator",
                    "--state",
                    self.node_state(node),
                    *[arg for agent in agents for arg in ("--recipient", agent)],
                ]
            )
        if self.bare:
            if enforce:
                raise StackError(
                    "edgecitadel_bare has no monitor: run none or reference"
                )
        else:
            self._start_aggregator(
                controller,
                spec,
                strategy,
                control_latency_ms,
                control_delay_ms,
                nats_url,
                enforce,
                {
                    "profile": profile,
                    # None: the profile's (MonitorConfig.phase1_mode_for).
                    **({"phase1_mode": phase1_mode} if phase1_mode else {}),
                    "phase1_labels": phase1_labels,
                    "receding_during_waits": receding_during_waits,
                    **(monitor or {}),
                },
            )
            self._await_leases(spec)
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
        self.start_procstat()

    def start_procstat(self) -> None:
        """Sample this run's processes on each host (``procstat_interval_ms``),
        once all of them run; each is the leader of its own process group."""
        interval = self.config.procstat_interval_ms
        if not interval:
            return
        groups: dict[str, dict[str, int]] = {}
        for proc in self.procs:
            groups.setdefault(proc.host, {})[proc.name] = proc.pid
        for host, members in groups.items():
            directory = self.host_dir(host)
            path = f"{directory}/procstat.groups.json"
            self.hosts[host].write(path, json.dumps(members).encode(), mode=0o644)
            self._start(
                host,
                "procstat",
                [
                    self._python(host),
                    "-m",
                    "harness.procstat",
                    "--groups",
                    path,
                    "--out",
                    f"{directory}/procstat.jsonl",
                    "--interval",
                    str(interval / 1000),
                ],
            )

    def _start_services(self, host: str, spec: Spec) -> None:
        directory = f"{self.host_dir(host)}/services"
        self.hosts[host].write(
            f"{directory}/services.token", self.services_token.encode()
        )
        self.hosts[host].write(f"{directory}/sink.token", self.sink_token.encode())
        sink = self.config.effect_sink
        upstream = []
        if sink["kind"] == "homeassistant":
            if sink["upstream"]:
                upstream = ["--ha-upstream", sink["upstream"]]
            else:
                self.hosts[host].write(f"{directory}/ha.token", self.ha_token.encode())
        port = self._port("services")
        # Workers and gateways on every host post here.
        bind = self._listen(host)
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
                *upstream,
            ],
        )
        ready = f"{directory}/services.ready"
        wait_until(lambda: self.hosts[host].exists(ready), "services")
        port = json.loads(self.hosts[host].read(ready))["port"]
        self.services_url = f"http://{bind}:{port}"
        wait_until(lambda: self.services("GET", "/health"), "services health")

    def _start_nats(self, host: str) -> str:
        port, monitor = self._port("nats"), self._port("nats_monitor")
        directory = f"{self.host_dir(host)}/nats"
        # Nodes on every host connect; the monitoring port only for this host.
        listen = self._listen(host)
        config = (
            f"host: {json.dumps(listen)}\nport: {port}\n"
            f"http: {json.dumps(f'127.0.0.1:{monitor}')}\n"
            f"authorization {{ token: {json.dumps(self.nats_token)} }}\n"
            f"jetstream {{ store_dir: {json.dumps(directory + '/js')} }}\n"
        )
        self.hosts[host].write(f"{directory}/nats.conf", config.encode())
        binary = self.config.hosts[host].nats_server or "nats-server"
        self._start(host, "nats", [binary, "-c", f"{directory}/nats.conf"])
        url = f"nats://{listen}:{port}"
        wait_until(
            lambda: http_json(f"http://127.0.0.1:{monitor}/healthz")
            if isinstance(self.hosts[host], LocalHost)
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
            env={
                **self.config.runtime_env,
                # Bare: no export to Core and no controller lease (agentd's
                # defaults); local journaling stays, it is the dispatch path.
                "EDGECITADEL_TRACE_SYNC": "0" if self.bare else "1",
                "EDGECITADEL_CONTROLLER_LEASE_MS": "0"
                if self.bare
                else str(self.config.controller_lease_ms),
                "EDGECITADEL_EFFECT_ADAPTERS": self._effect_adapters(node),
            },
        )
        wait_until(
            lambda: self.hosts[host].exists(f"{state}/agentd/admin.token"),
            f"agentd {node}",
        )
        # The admin token is written before the socket listens.
        socket = self._socket(node)
        wait_until(lambda: self.hosts[host].exists(socket), f"agentd {node} socket")

    def _effect_adapters(self, node: str) -> str:
        """Write the node's gateway adapter file (no secret in it); its path.

        Every node has the run's ledger; Home Assistant only where the sink
        names the node (every node without ``nodes``)."""
        host = self.config.node_hosts[node]
        state = self.node_state(node)
        sink = self.config.effect_sink
        adapters: dict[str, Any] = {
            "http": {"ledger_url": f"{self.services_url}/sink/{self.sink_token}"}
        }
        if sink["kind"] == "homeassistant" and (
            sink.get("nodes") is None or node in sink["nodes"]
        ):
            token_file = sink["token_file"]
            if token_file is None:  # dry run: the services stand in for HA
                token_file = f"{state}/agentd/credentials/homeassistant.token"
                self.hosts[host].write(token_file, self.ha_token.encode())
            adapters["homeassistant"] = {
                "base_url": f"{self.services_url}/ha/{self.sink_token}",
                "token_file": token_file,
            }
        path = f"{state}/effect-adapters.json"
        # The URL carries the sink token: the file is mode 600 like the others.
        self.hosts[host].write(path, json.dumps(adapters).encode())
        return path

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
        settings: Mapping[str, Any],
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
        monitor = deploy.monitor_config(
            spec,
            strategy=strategy,
            control_latency_ms=latency,
            tau_ms=self.config.tau_ms,
            enforce=enforce,
            overrides={**settings, **self.config.monitor_overrides},
        )
        # Control is configured in every run: the monitor renews the nodes'
        # leases even when it must not contain (enforce false).
        control: dict[str, Any] = {}
        if self.config.control_transport == "nats":
            owner = str(monitor.get("owner", deploy.MONITOR_OWNER))
            control["nats"] = {
                "key_file": self._control_key(spec, host, owner),
                "confirmation_timeout_ms": NATS_CONFIRMATION_TIMEOUT_MS,
            }
        else:
            sockets = {}
            for node in deploy.nodes(spec):
                if self.config.node_hosts[node] == host:
                    sockets[node] = {
                        "socket": self._socket(node),
                        "admin_token_file": f"{self.node_state(node)}/agentd/admin.token",
                    }
                elif self.config.control_transport == "ssh_forward":
                    sockets[node] = self._forward_control(node, host)
            unreachable = [n for n in deploy.nodes(spec) if n not in sockets]
            if unreachable:
                raise StackError(
                    f"the monitor cannot reach agentd on {unreachable}; "
                    "set control_transport to nats or ssh_forward"
                )
            control["agentd"] = sockets
        document = {"monitor": monitor, "control": control}
        self.hosts[host].write(
            f"{directory}/containment.json", json.dumps(document, indent=1).encode()
        )
        port = self._port("aggregator")
        # Only the runner calls Core's API (nodes use NATS).
        bind = self._runner_listen(host)
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
        self.aggregator_url = f"http://{bind}:{port}"
        wait_until(
            lambda: self.monitor_status().get("state") == "running", "Core monitor"
        )

    def _await_leases(self, spec: Spec) -> None:
        """Every node enforces the controller lease: work starts only once the
        monitor's renewal reached each of them (else every effect is denied)."""

        def renewed() -> bool:
            leases = (
                self.monitor_status().get("control", {}).get("controller_leases", {})
            )
            now_ms = int(self.services("GET", "/clock")["t_ms"])
            return all(
                (leases.get(node) or {}).get("until_ms", 0) > now_ms
                for node in deploy.nodes(spec)
            )

        wait_until(renewed, "controller leases on every node")

    def _control_key(self, spec: Spec, controller: str, owner: str) -> str:
        """A fresh owner key for this run, given to the monitor and every node.

        agentd authenticates a NATS control command by the HMAC of its owner's
        key in ``<agentd state>/control-keys/<owner>.key``; the key never leaves
        mode-600 files and lives only as long as the run's state.
        """
        key = secrets.token_hex(32).encode()
        path = f"{self.host_dir(controller)}/control/{owner}.key"
        self.hosts[controller].write(path, key)
        for node in deploy.nodes(spec):
            self.hosts[self.config.node_hosts[node]].write(
                f"{self.node_state(node)}/agentd/control-keys/{owner}.key", key
            )
        return path

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
        if any(isinstance(h, HelperHost) for h in self.hosts.values()):
            raise StackError("netem profiles are installed over SSH, not a labhelper")
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
                "-m",
                "harness.nodectl",
                "socket",
                "--state",
                self.node_state(node),
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
                **self.config.runtime_env,
                "EDGECITADEL_STATE_DIR": self.node_state(node),
                "EDGECITADEL_CONNECTOR_ID": f"managed-{agent}",
                "DET_WORKER_SERVICES_TOKEN": self.services_token,
            },
        )

    def down(self) -> list[str]:
        """Stop every process; returns the ones that had died early.

        Workers and nodes go first and the Core aggregator after them (before
        NATS and the services it reads from), so Core records whatever the
        nodes journal until they stop. Stopping workers ends their sessions,
        which agentd records as interrupted invocations; the monitor may answer
        those with lease-violation decisions, which the scorer ignores after
        the run's end.
        """
        died = []
        core = [p for p in self.procs if p.name == "aggregator"]
        rest = [p for p in reversed(self.procs) if p.name != "aggregator"]
        last = [p for p in rest if p.name in {"nats", "services"}]
        order = [p for p in rest if p not in last] + core + last
        for proc in order:
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

    def busy(self) -> bool:
        """Whether the system is still executing work although no worker event
        arrived (the runner's quiescence rule asks). Only the LangGraph
        baseline says so, while a forked run redoes work; a run whose remaining
        invocations wait on inputs that will never come is not busy."""
        return False

    def monitor_status(self) -> dict[str, Any]:
        if self.bare:
            return {}
        return http_json(
            self.aggregator_url + "/api/containment/status",
            headers={"x-edgecitadel-admin-token": self.admin_token},
        )

    def post_alert(self, alert: Mapping[str, Any]) -> dict[str, Any]:
        endpoint = self.alert_endpoint()
        assert endpoint is not None
        return http_json(endpoint["url"], data=dict(alert), headers=endpoint["headers"])

    def alert_endpoint(self) -> dict[str, Any] | None:
        """Where an alert is posted over HTTP, so the run's services can post
        one on receipt (None: the stack has no alert intake to hand over)."""
        if self.bare:
            return None
        return {
            "url": self.aggregator_url + "/api/containment/alerts",
            "headers": {"x-edgecitadel-admin-token": self.admin_token},
        }

    def schedule(self, node: str, entries: list[dict[str, Any]]) -> None:
        """Submit the node's external requests at their times (``nodectl
        schedule``); each is marked ``submitted`` or ``refused`` in the run's
        marks."""
        host = self.config.node_hosts[node]
        path = f"{self.node_state(node)}/requests.json"
        self.hosts[host].write(path, json.dumps(entries).encode())
        self._start(
            host,
            f"requests-{node}",
            [
                self._python(host),
                "-m",
                "harness.nodectl",
                "schedule",
                "--state",
                self.node_state(node),
                "--entries",
                path,
                "--services",
                self.services_url,
            ],
            env={"HARNESS_SERVICES_TOKEN": self.services_token},
        )

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


def make_stack(config: StackConfig, run_id: str, run_dir: Path) -> RunStack:
    """The run's stack for the configured system (the A2A baseline imports
    a2a-sdk, so EdgeCitadel runs never need it)."""
    if config.system == "a2a":
        from .a2a_stack import A2ARunStack

        return A2ARunStack(config, run_id, run_dir)
    if config.system in {"langgraph", "langgraph_functional"}:
        from .lg_stack import LangGraphRunStack

        return LangGraphRunStack(config, run_id, run_dir)
    return RunStack(config, run_id, run_dir)
