"""Process supervision for Agents installed from EdgeCitadel packages."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from .store import AgentdStore, StoreError

MAX_LAUNCH_BYTES = 1024 * 1024
MAX_RESTART_DELAY_SECONDS = 30.0
MAX_CONSECUTIVE_RESTARTS = 8
STABLE_RUNTIME_SECONDS = 60.0


def _pid_running(pid: object) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _process_identity(pid: int) -> str | None:
    if not _pid_running(pid):
        return None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        if proc_stat.is_file():
            fields = proc_stat.read_text().rpartition(") ")[2].split()
            if len(fields) > 19:
                return hashlib.sha256(f"linux:{pid}:{fields[19]}".encode()).hexdigest()
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-o", "command=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            env={**os.environ, "LC_ALL": "C"},
        )
    except OSError:
        return None
    description = result.stdout.strip()
    if result.returncode != 0 or not description:
        return None
    return hashlib.sha256(f"ps:{pid}:{description}".encode()).hexdigest()


def _private_write(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(dict(value), indent=2, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)
    path.chmod(0o600)


class ManagedAgentSupervisor:
    """Reconcile durable Managed Agent intent with owned process groups."""

    def __init__(self, state_dir: Path, store: AgentdStore) -> None:
        self.state_dir = state_dir
        self.store = store
        self._state_path = state_dir / "agentd" / "managed-processes.json"
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._children: dict[str, subprocess.Popen[bytes]] = {}
        self._runtime = self._load_runtime_state()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=15)
            if self._thread.is_alive():
                raise StoreError("Managed Agent supervisor did not stop")

    def wake(self) -> None:
        self._wake.set()

    def status(self) -> list[dict[str, object]]:
        desired = {
            str(record["package_id"]): record
            for record in self.store.list_managed_agents()
        }
        result: list[dict[str, object]] = []
        with self._lock:
            package_ids = desired.keys() | self._runtime.keys()
        for package_id in sorted(package_ids):
            record = desired.get(package_id, {})
            with self._lock:
                actual = self._runtime.get(package_id)
                running = actual is not None and self._runtime_is_owned(
                    package_id, actual
                )
                if (
                    actual is not None
                    and not running
                    and actual.get("runtime_state") == "running"
                ):
                    returncode = actual.get("returncode")
                    actual.update(
                        runtime_state="exited",
                        pid=None,
                        process_identity=None,
                        detail=f"process exited with status {returncode}",
                    )
                    self._persist_runtime_state()
                observed = dict(actual or {})
            pid = observed.get("pid")
            runtime_state = str(observed.get("runtime_state", "stopped"))
            result.append(
                {
                    **record,
                    "package_id": package_id,
                    "runtime_state": "running" if running else runtime_state,
                    "pid": pid if running else None,
                    "detail": observed.get("detail", "stopped"),
                }
            )
        return result

    def _load_runtime_state(self) -> dict[str, dict[str, object]]:
        if not self._state_path.is_file() or self._state_path.is_symlink():
            return {}
        try:
            document = json.loads(self._state_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        processes = document.get("processes") if isinstance(document, dict) else None
        if not isinstance(processes, dict):
            return {}
        return {
            str(key): cast(dict[str, object], value)
            for key, value in processes.items()
            if isinstance(value, dict)
        }

    def _persist_runtime_state(self) -> None:
        _private_write(
            self._state_path,
            {"version": 1, "processes": self._runtime},
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._reconcile_once()
            except Exception as error:  # Keep one bad record from killing supervision.
                print(
                    f"Managed Agent reconciliation failed: {type(error).__name__}",
                    file=sys.stderr,
                )
            self._wake.wait(0.5)
            self._wake.clear()
        for package_id in list(self._runtime):
            self._stop_process(package_id, final_state="stopped")

    def _reconcile_once(self) -> None:
        desired = {
            str(record["package_id"]): record
            for record in self.store.list_managed_agents()
        }
        for package_id in list(self._runtime):
            record = desired.get(package_id)
            if record is None or record.get("desired_state") != "running":
                self._stop_process(package_id, final_state="stopped")
        for package_id, record in desired.items():
            if record.get("desired_state") != "running" or not record.get(
                "launch_path"
            ):
                continue
            observed = self._runtime.get(package_id)
            if observed is not None and self._runtime_is_owned(package_id, observed):
                continue
            try:
                launch = self._launch_document(package_id, record)
                restart_policy = launch.get("restart_policy")
                if restart_policy not in {"always", "on-failure", "never"}:
                    raise StoreError("Managed Agent restart policy is invalid")
            except (OSError, StoreError, ValueError) as error:
                self._record_start_failure(package_id, error)
                continue
            if observed is not None and _pid_running(observed.get("pid")):
                observed.update(
                    runtime_state="failed",
                    detail="unverified live process; refusing duplicate start",
                    restart_blocked=True,
                )
                self._persist_runtime_state()
                continue
            if observed is not None and observed.get("restart_blocked"):
                continue
            now = time.monotonic()
            if observed is not None and observed.get("runtime_state") in {
                "running",
                "exited",
            }:
                returncode = observed.get("returncode")
                should_restart = restart_policy == "always" or (
                    restart_policy == "on-failure" and returncode != 0
                )
                if not should_restart:
                    observed.update(
                        runtime_state="stopped" if returncode == 0 else "failed",
                        pid=None,
                        process_identity=None,
                        detail=f"process exited with status {returncode}",
                        restart_blocked=True,
                    )
                    self._persist_runtime_state()
                    continue
                self._schedule_restart(package_id, observed, now)
                continue
            next_start_at = (
                observed.get("next_start_at") if observed is not None else None
            )
            if isinstance(next_start_at, (int, float)):
                if now < next_start_at:
                    continue
            try:
                self._start_process(package_id, record, launch)
            except (OSError, StoreError, ValueError) as error:
                self._record_start_failure(package_id, error)

    def _schedule_restart(
        self,
        package_id: str,
        observed: dict[str, object],
        now: float,
        *,
        detail: str | None = None,
    ) -> None:
        restart_value = observed.get("restart_count", 0)
        previous_count = (
            restart_value
            if isinstance(restart_value, int)
            and not isinstance(restart_value, bool)
            and restart_value >= 0
            else 0
        )
        started_at = observed.get("started_at")
        if isinstance(started_at, (int, float)) and now - started_at >= (
            STABLE_RUNTIME_SECONDS
        ):
            previous_count = 0
        restart_count = previous_count + 1
        if restart_count > MAX_CONSECUTIVE_RESTARTS:
            observed.update(
                runtime_state="failed",
                pid=None,
                process_identity=None,
                restart_count=restart_count,
                detail="restart limit reached; stop and start the Managed Agent after correction",
                restart_blocked=True,
            )
            self._persist_runtime_state()
            return
        delay = min(2 ** min(restart_count - 1, 5), MAX_RESTART_DELAY_SECONDS)
        observed.update(
            runtime_state="restarting",
            pid=None,
            process_identity=None,
            restart_count=restart_count,
            next_start_at=now + delay,
            detail=detail or f"restart {restart_count} scheduled in {delay:g}s",
        )
        self._persist_runtime_state()

    def _record_start_failure(self, package_id: str, error: Exception) -> None:
        with self._lock:
            observed = self._runtime.get(package_id, {})
            self._runtime[package_id] = observed
            self._schedule_restart(
                package_id,
                observed,
                time.monotonic(),
                detail=f"start failed: {type(error).__name__}",
            )

    def _runtime_is_owned(
        self, package_id: str, observed: Mapping[str, object]
    ) -> bool:
        pid = observed.get("pid")
        identity = observed.get("process_identity")
        with self._lock:
            child = self._children.get(package_id)
            if child is not None and (returncode := child.poll()) is not None:
                self._children.pop(package_id, None)
                if isinstance(observed, dict):
                    observed["returncode"] = returncode
                return False
        return (
            isinstance(pid, int)
            and isinstance(identity, str)
            and identity == _process_identity(pid)
        )

    def _launch_document(
        self, package_id: str, record: Mapping[str, object]
    ) -> dict[str, Any]:
        launch_root = (self.state_dir / "managed-launch").resolve()
        raw_path = record.get("launch_path")
        if not isinstance(raw_path, str):
            raise StoreError("Managed Agent launch path is missing")
        path = Path(raw_path)
        if path.is_symlink() or not path.is_file():
            raise StoreError("Managed Agent launch file must be a regular file")
        resolved = path.resolve()
        if not resolved.is_relative_to(launch_root):
            raise StoreError("Managed Agent launch file is outside private state")
        encoded = resolved.read_bytes()
        if len(encoded) > MAX_LAUNCH_BYTES:
            raise StoreError("Managed Agent launch file exceeds 1 MiB")
        try:
            document = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StoreError("Managed Agent launch file is invalid") from error
        if not isinstance(document, dict) or document.get("package_id") != package_id:
            raise StoreError("Managed Agent launch identity does not match")
        return cast(dict[str, Any], document)

    def _start_process(
        self,
        package_id: str,
        record: Mapping[str, object],
        launch: Mapping[str, Any] | None = None,
    ) -> None:
        launch = launch or self._launch_document(package_id, record)
        argv = launch.get("argv")
        environment = launch.get("environment")
        if (
            not isinstance(argv, list)
            or not argv
            or len(argv) > 128
            or not all(isinstance(item, str) and item for item in argv)
        ):
            raise StoreError("Managed Agent argv is invalid")
        if not isinstance(environment, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in environment.items()
        ):
            raise StoreError("Managed Agent environment is invalid")
        cwd = Path(str(launch.get("cwd", ""))).resolve()
        log_path = Path(str(launch.get("log_path", ""))).resolve()
        if not cwd.is_relative_to((self.state_dir / "plugins").resolve()):
            raise StoreError("Managed Agent working directory is outside private state")
        if not log_path.is_relative_to((self.state_dir / "logs").resolve()):
            raise StoreError("Managed Agent log path is outside private state")
        if log_path.exists() and (log_path.is_symlink() or not log_path.is_file()):
            raise StoreError("Managed Agent log must be a regular file")
        log_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        log_path.parent.chmod(0o700)
        with log_path.open("ab") as log_file:
            log_path.chmod(0o600)
            process = subprocess.Popen(
                cast(list[str], argv),
                cwd=cwd,
                env=cast(dict[str, str], environment),
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        identity = _process_identity(process.pid)
        if identity is None:
            process.terminate()
            raise StoreError("Managed Agent process identity could not be verified")
        with self._lock:
            previous = self._runtime.get(package_id, {})
            restart_value = previous.get("restart_count", 0)
            restart_count = (
                restart_value
                if isinstance(restart_value, int)
                and not isinstance(restart_value, bool)
                and restart_value >= 0
                else 0
            )
            self._children[package_id] = process
            self._runtime[package_id] = {
                "runtime_state": "running",
                "pid": process.pid,
                "process_identity": identity,
                "detail": f"pid {process.pid}",
                "restart_count": restart_count,
                "started_at": time.monotonic(),
            }
            self._persist_runtime_state()

    def _stop_process(self, package_id: str, *, final_state: str) -> None:
        with self._lock:
            observed = self._runtime.get(package_id)
        if observed is None:
            return
        pid = observed.get("pid")
        child = self._children.get(package_id)
        process_alive = not (child is not None and child.poll() is not None) and (
            _pid_running(pid)
        )
        if process_alive:
            if not self._runtime_is_owned(package_id, observed) or not isinstance(
                pid, int
            ):
                observed.update(
                    runtime_state="failed",
                    detail="unverified live process; refusing signal",
                )
                self._persist_runtime_state()
                return
            try:
                if os.getpgid(pid) != pid:
                    raise StoreError("Managed Agent process group is not owned")
                os.killpg(pid, signal.SIGTERM)
                deadline = time.monotonic() + 5
                while _pid_running(pid) and time.monotonic() < deadline:
                    time.sleep(0.05)
                if _pid_running(pid):
                    os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        child = self._children.pop(package_id, None)
        if child is not None:
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        with self._lock:
            self._runtime[package_id] = {
                "runtime_state": final_state,
                "pid": None,
                "detail": final_state,
            }
            self._persist_runtime_state()
