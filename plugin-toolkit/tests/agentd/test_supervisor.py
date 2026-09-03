from __future__ import annotations

import json
import os
import signal
import stat
import sys
import time
from pathlib import Path

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.supervisor import ManagedAgentSupervisor


def _wait_for(predicate, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition did not become ready")


def _configure(
    tmp_path: Path, store: AgentdStore
) -> tuple[ManagedAgentSupervisor, Path]:
    package_root = tmp_path / "plugins" / "local.test" / "1.0.0"
    package_root.mkdir(parents=True)
    logs = tmp_path / "logs"
    logs.mkdir()
    launch_root = tmp_path / "managed-launch"
    launch_root.mkdir()
    launch = launch_root / "local-test.json"
    launch.write_text(
        json.dumps(
            {
                "version": 1,
                "package_id": "local.test",
                "argv": [sys.executable, "-c", "import time; time.sleep(60)"],
                "cwd": str(package_root),
                "environment": {"PATH": os.environ.get("PATH", "")},
                "log_path": str(logs / "local.test.log"),
                "restart_policy": "on-failure",
            }
        )
    )
    launch.chmod(0o600)
    store.reconcile_managed_agents(
        [
            {
                "package_id": "local.test",
                "desired_state": "running",
                "launch_path": str(launch),
            }
        ]
    )
    return ManagedAgentSupervisor(tmp_path, store), launch


def test_supervisor_starts_recovers_and_stops_owned_process(tmp_path: Path) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    supervisor, _launch = _configure(tmp_path, store)
    supervisor.start()
    try:
        _wait_for(lambda: supervisor.status()[0]["runtime_state"] == "running")
        first_pid = supervisor.status()[0]["pid"]
        assert isinstance(first_pid, int)
        os.killpg(first_pid, signal.SIGTERM)

        def restarted() -> bool:
            status = supervisor.status()[0]
            return (
                status["runtime_state"] == "running"
                and isinstance(status["pid"], int)
                and status["pid"] != first_pid
            )

        _wait_for(restarted)
        second_pid = supervisor.status()[0]["pid"]
        assert isinstance(second_pid, int)

        store.reconcile_managed_agents(
            [{"package_id": "local.test", "desired_state": "stopped"}]
        )
        supervisor.wake()
        _wait_for(lambda: supervisor.status()[0]["runtime_state"] == "stopped")
        assert (
            stat.S_IMODE(
                (tmp_path / "agentd" / "managed-processes.json").stat().st_mode
            )
            == 0o600
        )
        assert not _process_exists(second_pid)
    finally:
        supervisor.stop()
        store.close()


def test_supervisor_respects_never_restart_policy(tmp_path: Path) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    supervisor, launch = _configure(tmp_path, store)
    document = json.loads(launch.read_text())
    document["restart_policy"] = "never"
    document["argv"] = [sys.executable, "-c", "raise SystemExit(3)"]
    launch.write_text(json.dumps(document))
    launch.chmod(0o600)
    supervisor.start()
    try:
        _wait_for(lambda: supervisor.status()[0]["runtime_state"] == "failed")
        first = supervisor.status()[0]
        assert first["pid"] is None
        time.sleep(1)
        second = supervisor.status()[0]
        assert second["runtime_state"] == "failed"
        assert second["pid"] is None
    finally:
        supervisor.stop()
        store.close()


def test_status_persists_a_child_exit_instead_of_reporting_stale_running(
    tmp_path: Path,
) -> None:
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    supervisor, launch = _configure(tmp_path, store)
    document = json.loads(launch.read_text())
    document["restart_policy"] = "never"
    document["argv"] = [sys.executable, "-c", "raise SystemExit(0)"]
    launch.write_text(json.dumps(document))
    launch.chmod(0o600)
    try:
        supervisor._reconcile_once()
        child = supervisor._children["local.test"]
        child.wait(timeout=5)

        status = supervisor.status()[0]
        persisted = json.loads(
            (tmp_path / "agentd" / "managed-processes.json").read_text()
        )["processes"]["local.test"]

        assert status["runtime_state"] == "exited"
        assert status["pid"] is None
        assert persisted["runtime_state"] == "exited"
        assert persisted["returncode"] == 0
    finally:
        supervisor.stop()
        store.close()


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
