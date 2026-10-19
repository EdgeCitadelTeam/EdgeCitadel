import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.service import PROCESS_STATE_NAME, socket_path_for
from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer


def command(state):
    return [
        sys.executable,
        str(Path(__file__).with_name("service_test_support.py")),
        str(state),
    ]


def environment():
    return {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def start(state):
    process = subprocess.Popen(
        command(state),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment(),
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert process.poll() is None, process.stderr.read()
            try:
                AgentdClient(socket_path_for(state), timeout=0.2).call("health")
                # On replacement the old socket can briefly still exist; its
                # owner must match this newly started process.
                if (
                    json.loads((state / PROCESS_STATE_NAME).read_text())["pid"]
                    == process.pid
                ):
                    return process
            except (AgentdClientError, FileNotFoundError):
                pass
            time.sleep(0.01)
        raise AssertionError("owned daemon did not become ready")
    except BaseException:
        stop(process)
        raise


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    process.stdout.close()
    process.stderr.close()


def test_second_daemon_and_maintenance_cannot_replace_live_writer(tmp_path):
    state = tmp_path / "state/agentd"
    process = start(state)
    try:
        socket_inode = socket_path_for(state).stat().st_ino
        record = (state / PROCESS_STATE_NAME).read_bytes()
        rejected = subprocess.run(
            command(state),
            env=environment(),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert rejected.returncode == 1
        assert "active writer" in rejected.stderr
        assert socket_path_for(state).stat().st_ino == socket_inode
        assert (state / PROCESS_STATE_NAME).read_bytes() == record
        assert AgentdClient(socket_path_for(state)).call("health")["status"] == "ready"
        with (
            pytest.raises(WriterActiveError, match="active writer"),
            exclusive_writer(state),
        ):
            pytest.fail("maintenance obtained live daemon ownership")
    finally:
        stop(process)
    assert json.loads((state / PROCESS_STATE_NAME).read_text())["pid"] is None
    assert not socket_path_for(state).exists()
    assert (state / "writer.lock").exists()


def test_sigkill_releases_lock_and_replacement_reclaims_stale_socket(tmp_path):
    state = tmp_path / "state/agentd"
    first = start(state)
    try:
        first.kill()
        assert first.wait(timeout=10) == -signal.SIGKILL
        replacement = start(state)
        try:
            assert (
                json.loads((state / PROCESS_STATE_NAME).read_text())["pid"]
                == replacement.pid
            )
            assert replacement.pid != first.pid
            assert (
                AgentdClient(socket_path_for(state)).call("health")["status"] == "ready"
            )
        finally:
            stop(replacement)
    finally:
        stop(first)
    with exclusive_writer(state):
        assert (state / "writer.lock").stat().st_mode & 0o777 == 0o600


def test_maintenance_owner_prevents_daemon_start(tmp_path):
    state = tmp_path / "state/agentd"
    with exclusive_writer(state):
        rejected = subprocess.run(
            command(state),
            env=environment(),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert rejected.returncode == 1
        assert "active writer" in rejected.stderr
        assert not (state / PROCESS_STATE_NAME).exists()
        assert not socket_path_for(state).exists()
        assert not (state / "agentd.sqlite3").exists()
