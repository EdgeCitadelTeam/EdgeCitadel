"""Stack configuration and the SSH host's commands (no lab access here)."""

from __future__ import annotations

import json
import subprocess

import pytest

from harness import stack


def host_config(**overrides) -> stack.HostConfig:
    values = dict(
        name="edge",
        address="NetSys-Endurance",
        python="/opt/venv/bin/python",
        repo="/srv/ec",
        runtime_root="/srv/ec-int",
        work_root="/var/tmp/x",
        ssh="lab@NetSys-Endurance",
    )
    values.update(overrides)
    return stack.HostConfig(**values)


@pytest.fixture
def captured(monkeypatch):
    calls = []

    def fake_run(argv, input=None, capture_output=True, timeout=None, **_):
        calls.append({"argv": argv, "input": input})
        return subprocess.CompletedProcess(argv, 0, stdout=b"4242\n", stderr=b"")

    monkeypatch.setattr(stack.subprocess, "run", fake_run)
    return calls


def test_ssh_start_sources_secrets_from_a_private_file(captured):
    host = stack.SSHHost(host_config())
    proc = host.start(
        "worker-a",
        ["/opt/venv/bin/python", "-m", "det_worker", "--agent", "a"],
        env={"DET_WORKER_SERVICES_TOKEN": "s3cret"},
        cwd="/srv/ec/agent-packages/det-worker",
        log="/var/tmp/x/run/logs/worker-a.log",
    )
    assert proc.pid == 4242
    write, start = captured
    assert write["argv"][-1].startswith("umask 077 && mkdir -p")
    assert b"export DET_WORKER_SERVICES_TOKEN=s3cret" in write["input"]
    assert b"PYTHONPATH=/srv/ec-int:/srv/ec-int/agent-runtime/src" in write["input"]
    command = start["argv"][-1]
    assert "s3cret" not in " ".join(start["argv"])
    assert ". /var/tmp/x/run/logs/.env.worker-a" in command
    assert "setsid nohup /opt/venv/bin/python -m det_worker --agent a" in command
    assert start["argv"][:4] == ["ssh", "-o", "BatchMode=yes", "lab@NetSys-Endurance"]


def test_ssh_stop_signals_the_whole_process_group(captured):
    host = stack.SSHHost(host_config())
    host.stop(stack.Proc("worker-a", "edge", 4242, "/var/tmp/x/l.log"), timeout=1)
    [call] = captured
    assert "kill -TERM -- -4242" in call["argv"][-1]
    assert "kill -KILL -- -4242" in call["argv"][-1]


def test_stack_file_expands_the_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("HARNESS_PYTHON", "/usr/bin/python3")
    document = {
        "mode": "dryrun",
        "controller": "local",
        "hosts": {
            "local": {
                "address": "127.0.0.1",
                "python": "${HARNESS_PYTHON}",
                "repo": "",
                "runtime_root": "/rt",
                "work_root": "/w",
            }
        },
        "node_hosts": {"core": "local"},
    }
    path = tmp_path / "stack.json"
    path.write_text(json.dumps(document))
    config = stack.StackConfig.load(path)
    assert config.hosts["local"].python == "/usr/bin/python3"
    assert config.hosts["local"].repo == str(stack.REPO_ROOT)
    document["hosts"]["local"]["python"] = "${HARNESS_UNSET_FOR_TEST}"
    path.write_text(json.dumps(document))
    with pytest.raises(stack.StackError, match="unset environment"):
        stack.StackConfig.load(path)


def test_shipped_stack_files_name_hosts_only():
    for name in ("dryrun.json", "lab.json"):
        text = (stack.HARNESS_ROOT / "configs" / name).read_text()
        document = json.loads(text)
        assert "token" not in text.lower() and "password" not in text.lower()
        assert set(document["node_hosts"].values()) <= set(document["hosts"])
