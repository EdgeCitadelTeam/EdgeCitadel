from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import Mock

from scripts import plugin_runner


RUNNER = Path(plugin_runner.__file__).resolve()


def test_restart_policy_semantics():
    assert plugin_runner.should_restart("always", 0) is True
    assert plugin_runner.should_restart("always", 9) is True
    assert plugin_runner.should_restart("on-failure", 9) is True
    assert plugin_runner.should_restart("on-failure", 0) is False
    assert plugin_runner.should_restart("never", 9) is False


def test_runner_restarts_failed_child_then_returns_success(monkeypatch):
    failed = Mock()
    failed.poll.return_value = 1
    failed.returncode = 1
    succeeded = Mock()
    succeeded.poll.return_value = 0
    succeeded.returncode = 0
    children = iter((failed, succeeded))
    monkeypatch.setattr(
        plugin_runner.subprocess, "Popen", lambda *_args: next(children)
    )
    monkeypatch.setattr(plugin_runner.time, "sleep", lambda *_args: None)

    assert plugin_runner.run(["plugin"], "on-failure") == 0


def test_runner_forwards_direct_termination_to_child(monkeypatch):
    child = Mock()
    child.poll.side_effect = [None, None, 0]
    child.returncode = -signal.SIGTERM
    handlers = {}
    monkeypatch.setattr(plugin_runner.subprocess, "Popen", lambda *_args: child)
    monkeypatch.setattr(
        plugin_runner.signal,
        "signal",
        lambda signum, handler: handlers.setdefault(signum, handler),
    )

    def stop_on_sleep(_seconds):
        handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(plugin_runner.time, "sleep", stop_on_sleep)

    assert plugin_runner.run(["plugin"], "always") == -signal.SIGTERM
    child.terminate.assert_called_once_with()


def test_runner_restarts_real_failed_child_and_cleans_up_group(tmp_path):
    counter = tmp_path / "starts"
    child_code = (
        "from pathlib import Path; import sys; "
        "p=Path(sys.argv[1]); "
        "p.write_text(str(int(p.read_text())+1) if p.exists() else '1'); "
        "raise SystemExit(3)"
    )
    runner = subprocess.Popen(
        [
            sys.executable,
            str(RUNNER),
            "--restart-policy",
            "on-failure",
            "--",
            sys.executable,
            "-c",
            child_code,
            str(counter),
        ],
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if counter.exists() and int(counter.read_text()) >= 2:
                break
            time.sleep(0.1)
        assert counter.exists() and int(counter.read_text()) >= 2
    finally:
        if runner.poll() is None:
            os.killpg(runner.pid, signal.SIGTERM)
        try:
            runner.wait(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(runner.pid, signal.SIGKILL)
            runner.wait(timeout=3)
