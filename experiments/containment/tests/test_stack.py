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
    for path in sorted((stack.HARNESS_ROOT / "configs").glob("*.json")):
        text = path.read_text()
        document = json.loads(text)
        # A Home Assistant token is only ever referenced by its file's path.
        without_reference = text.replace('"token_file": "${HARNESS_HA_TOKEN_FILE}"', "")
        assert "token" not in without_reference.lower(), path.name
        assert "password" not in text.lower(), path.name
        assert set(document["node_hosts"].values()) <= set(document["hosts"])
        assert document["controller_lease_ms"] == 30000, path.name


def test_nats_control_gives_the_monitor_and_every_node_one_private_key(
    tmp_path, mini_spec
):
    hosts = {
        name: host_config(name=name, ssh=None, work_root=str(tmp_path / name))
        for name in ("ctl", "edge")
    }
    config = stack.StackConfig(
        mode="dryrun",
        hosts=hosts,
        controller="ctl",
        node_hosts={"core": "ctl", "edge-1": "edge", "edge-2": "edge"},
        control_transport="nats",
    )
    run = stack.RunStack(config, "r1", tmp_path / "out")
    path = run._control_key(mini_spec, "ctl", "core-monitor")
    key = (tmp_path / "ctl" / "r1" / "control" / "core-monitor.key").read_bytes()
    assert path.endswith("control/core-monitor.key") and len(key) == 64
    for node in ("core", "edge-1", "edge-2"):
        copy = f"{run.node_state(node)}/agentd/control-keys/core-monitor.key"
        assert run.hosts[config.node_hosts[node]].read(copy) == key
        assert stack.Path(copy).stat().st_mode & 0o777 == 0o600


def test_unknown_control_transports_are_rejected(tmp_path):
    with pytest.raises(stack.StackError, match="control_transport"):
        stack._transport("carrier-pigeon")


def test_effect_sinks_are_validated_when_the_stack_is_loaded(monkeypatch):
    assert stack._effect_sink({"kind": "ledger"}) == {"kind": "ledger"}
    monkeypatch.setenv("HA_TARGET", "light.turn_on/light.lab_lamp")
    sink = stack._effect_sink(
        {"kind": "homeassistant", "targets": {"*": "${HA_TARGET}"}}
    )
    assert sink["targets"] == {"*": "light.turn_on/light.lab_lamp"}
    assert sink["upstream"] is None and sink["token_file"] is None
    for bad, match in (
        ({"kind": "authorize_perform"}, "kind"),
        ({"kind": "homeassistant"}, "targets"),
        (
            {"kind": "homeassistant", "targets": {"*": "x"}, "upstream": "http://ha"},
            "token_file",
        ),
        ({"kind": "homeassistant", "targets": {"*": "${NOT_SET_ANYWHERE}"}}, "unset"),
        ({"kind": "ledger", "upstream": "http://ha"}, "kind"),
    ):
        with pytest.raises(stack.StackError, match=match):
            stack._effect_sink(bad)
    with pytest.raises(stack.StackError, match="controller_lease_ms"):
        stack._lease(500)


def local_stack(tmp_path, **config) -> stack.RunStack:
    hosts = {"ctl": host_config(name="ctl", ssh=None, work_root=str(tmp_path / "ctl"))}
    run = stack.RunStack(
        stack.StackConfig(
            mode="dryrun",
            hosts=hosts,
            controller="ctl",
            node_hosts={"core": "ctl"},
            **config,
        ),
        "r1",
        tmp_path / "out",
    )
    run.services_url = "http://127.0.0.1:9"
    return run


def test_nodes_perform_effects_on_the_runs_sink(tmp_path):
    ledger = local_stack(tmp_path / "ledger")
    path = ledger._effect_adapters("core")
    assert stack.Path(path).stat().st_mode & 0o777 == 0o600
    assert json.loads(stack.Path(path).read_text()) == {
        "http": {"ledger_url": f"http://127.0.0.1:9/sink/{ledger.sink_token}"}
    }
    assert ledger.config.plan_sink() == {"adapter": "http"}
    targets = {"*": "input_boolean.turn_on/input_boolean.lamp"}
    physical = local_stack(
        tmp_path / "ha",
        effect_sink={
            "kind": "homeassistant",
            "targets": targets,
            "upstream": None,
            "token_file": None,
        },
    )
    adapters = json.loads(stack.Path(physical._effect_adapters("core")).read_text())
    ha = adapters["homeassistant"]
    assert ha["base_url"] == f"http://127.0.0.1:9/ha/{physical.sink_token}"
    # Dry run: the run's own token stands in for Home Assistant's, in a private file.
    token = stack.Path(ha["token_file"])
    assert token.read_text() == physical.ha_token
    assert token.stat().st_mode & 0o777 == 0o600
    assert physical.config.plan_sink() == {
        "adapter": "homeassistant",
        "targets": targets,
    }
