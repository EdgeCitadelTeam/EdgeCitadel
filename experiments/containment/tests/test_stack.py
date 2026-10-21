"""Stack configuration and the SSH host's commands (no lab access here)."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

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
        without_reference = text.replace(
            '"token_file": "${HARNESS_HA_TOKEN_FILE}"', ""
        ).replace('"token_file": "${HARNESS_HELPER_TOKEN_FILE}"', "")
        assert "token" not in without_reference.lower(), path.name
        assert "password" not in text.lower(), path.name
        assert set(document["node_hosts"].values()) <= set(document["hosts"])
        # The fail-closed lease belongs to EdgeCitadel with its monitor; the
        # bare runtime turns it off and the A2A baseline has none.
        if document.get("system", "edgecitadel") == "edgecitadel":
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
        ({"kind": "homeassistant", "targets": {"*": "x"}, "nodes": "core"}, "nodes"),
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
    # Every node keeps the ledger for the classes Home Assistant does not map.
    assert adapters["http"] == {
        "ledger_url": f"http://127.0.0.1:9/sink/{physical.sink_token}"
    }


def test_home_assistant_is_configured_only_on_the_nodes_it_names(tmp_path):
    """The svc physical case: only site K's node reaches the desk lamp."""
    run = local_stack(
        tmp_path,
        effect_sink=stack._effect_sink(
            {
                "kind": "homeassistant",
                "targets": {"dev_lamp": "light.toggle/light.desk_lamp"},
                "nodes": ["core"],
            }
        ),
    )
    run.config.node_hosts["edge-1"] = "ctl"
    core = json.loads(stack.Path(run._effect_adapters("core")).read_text())
    edge = json.loads(stack.Path(run._effect_adapters("edge-1")).read_text())
    assert set(core) == {"http", "homeassistant"} and set(edge) == {"http"}


def test_lab_services_listen_on_the_tailnet_and_core_api_on_the_loopback(
    tmp_path, monkeypatch
):
    """NATS and the services take connections from every host, so they bind
    the controller's tailnet address (its monitoring port the loopback);
    Core's API serves only the runner, on the controller: the loopback."""
    hosts = {
        "ctl": host_config(
            name="ctl", ssh=None, listen="100.64.0.1", work_root=str(tmp_path)
        ),
        "edge": host_config(),
    }
    config = stack.StackConfig(
        mode="lab", hosts=hosts, controller="ctl", node_hosts={"core": "ctl"}
    )
    run = stack.RunStack(config, "r1", tmp_path / "out")
    assert run._listen("ctl") == "100.64.0.1"
    assert run._runner_listen("ctl") == "127.0.0.1"
    # A runner elsewhere reaches Core's API at the controller's address.
    assert run._runner_listen("edge") == "NetSys-Endurance"
    started = []
    monkeypatch.setattr(run, "_start", lambda *args, **kw: started.append(args))
    monkeypatch.setattr(stack, "wait_until", lambda *args, **kw: None)
    url = run._start_nats("ctl")
    conf = (tmp_path / "r1" / "nats" / "nats.conf").read_text()
    assert url.startswith("nats://100.64.0.1:")
    assert 'host: "100.64.0.1"' in conf and 'http: "127.0.0.1:' in conf
    assert "0.0.0.0" not in conf


def test_helper_hosts_need_the_nats_control_path_and_no_netem(tmp_path, monkeypatch):
    for name in (
        "HARNESS_PYTHON",
        "HARNESS_REPO",
        "HARNESS_RUNTIME_ROOT",
        "HARNESS_NATS_SERVER",
        "HARNESS_CONTROLLER_LISTEN",
        "HARNESS_REMOTE_PYTHON",
        "HARNESS_REMOTE_REPO",
        "HARNESS_REMOTE_RUNTIME_ROOT",
        "HARNESS_REMOTE_WORK_ROOT",
        "HARNESS_HELPER_NATS_URL",
        "HARNESS_HELPER_TOKEN_FILE",
    ):
        monkeypatch.setenv(name, f"/x/{name.lower()}")
    shipped = stack.HARNESS_ROOT / "configs" / "lab-helper.json"
    config = stack.StackConfig.load(shipped)
    edge = config.hosts["netsys-endurance"]
    assert edge.ssh is None and edge.helper == {
        "nats_url": "/x/harness_helper_nats_url",
        "token_file": "/x/harness_helper_token_file",
        "subject": "harness.lab.netsys-endurance",
    }
    document = json.loads(shipped.read_text())
    document["control_transport"] = "ssh_forward"
    path = tmp_path / "s.json"
    path.write_text(json.dumps(document))
    with pytest.raises(stack.StackError, match="control_transport nats"):
        stack.StackConfig.load(path)


def test_teardown_stops_core_after_the_nodes_and_before_its_broker(tmp_path):
    run = local_stack(tmp_path)
    stopped = []

    class Host:
        def alive(self, proc):
            return True

        def stop(self, proc):
            stopped.append(proc.name)

    run.hosts["ctl"] = Host()
    for name in ("services", "nats", "agentd-core", "aggregator", "worker-a"):
        run.procs.append(stack.Proc(name, "ctl", 1, "log"))
    assert run.down() == []
    assert stopped == ["worker-a", "agentd-core", "aggregator", "nats", "services"]


def test_an_ssh_host_with_units_runs_each_process_as_a_named_unit(captured):
    host = stack.SSHHost(host_config(units="user"))
    proc = host.start(
        "worker-a",
        ["/opt/venv/bin/python", "-m", "det_worker", "--agent", "a"],
        env={"DET_WORKER_SERVICES_TOKEN": "s3cret"},
        cwd="/srv/ec/agent-packages/det-worker",
        log="/var/tmp/x/run-7/logs/worker-a.log",
    )
    assert proc.unit == "firebreak-exp-h-run-7-worker-a" and proc.pid == 4242
    command = captured[-1]["argv"][-1]
    assert (
        "systemd-run --user --quiet --collect --unit=firebreak-exp-h-run-7-worker-a"
        in command
    )
    assert "s3cret" not in command
    host.alive(proc)
    host.stop(proc)
    assert (
        "systemctl --user is-active --quiet firebreak-exp-h-run-7-worker-a"
        in (captured[-2]["argv"][-1])
    )
    assert (
        "systemctl --user stop firebreak-exp-h-run-7-worker-a"
        in captured[-1]["argv"][-1]
    )


def test_leftover_units_are_stopped_but_never_the_runner(monkeypatch):
    host = stack.SSHHost(host_config(units="system"))
    calls = []

    def ssh(command, *, data=None, timeout=60):
        calls.append(command)
        if "list-units" in command:
            return (
                "firebreak-exp-h-r1-worker-a.service\nfirebreak-exp-h-runner.service\n"
            )
        return ""

    monkeypatch.setattr(host, "_ssh", ssh)
    assert host.stop_leftover_units() == ["firebreak-exp-h-r1-worker-a.service"]
    assert "systemctl stop firebreak-exp-h-r1-worker-a.service" in calls[-1]
    assert "runner" not in calls[-1]


def test_an_attempt_waits_for_its_fixed_ports_and_clears_stale_state(
    tmp_path, monkeypatch
):
    import socket

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen()
    port = held.getsockname()[1]
    run = local_stack(tmp_path, ports={"aggregator": port})
    stale = tmp_path / "ctl" / "r1" / "nodes"
    stale.mkdir(parents=True)
    monkeypatch.setattr(stack, "PORT_WAIT_S", 0.5)
    try:
        with pytest.raises(stack.StackError, match="ports still in use"):
            run.prepare()
        assert not stale.exists()
    finally:
        held.close()
    assert run.prepare() == []


def test_the_runner_unit_carries_the_harness_environment(monkeypatch, tmp_path):
    from harness.__main__ import unit_command

    monkeypatch.setenv("HARNESS_RUNTIME_ROOT", "/srv/ec")
    monkeypatch.setenv("HARNESS_HA_TOKEN_FILE", "/etc/ha.token")
    command = unit_command(
        "firebreak-exp-h-runner", tmp_path / "runner.log", ["run", "--out", "o"]
    )
    assert command[:3] == ["systemd-run", "--unit=firebreak-exp-h-runner", "--collect"]
    assert "--setenv=HARNESS_RUNTIME_ROOT=/srv/ec" in command
    assert command[-5:] == ["-m", "harness", "run", "--out", "o"][-5:]
    with pytest.raises(SystemExit):
        unit_command("u", tmp_path / "l", [])


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs Linux /proc")
def test_procstat_sums_cpu_and_memory_per_process_group():
    import sys

    from harness import procstat

    child = subprocess.Popen(
        [sys.executable, "-c", "import time; sum(range(3_000_000)); time.sleep(5)"],
        start_new_session=True,
    )
    try:
        time.sleep(0.5)
        groups = procstat.sample({"child": child.pid, "gone": 2**22 + 7})
    finally:
        child.kill()
        child.wait()
    assert groups["child"]["n"] == 1
    assert groups["child"]["rss_kb"] > 1000
    assert groups["child"]["cpu_s"] > 0
    assert groups["gone"] == {"cpu_s": 0.0, "rss_kb": 0, "n": 0}


LAB_FALLBACKS = {
    "EDGECITADEL_TRANSPORT_POLL_MS": "1000",
    "EDGECITADEL_CLAIM_POLL_MS": "1000",
}


def test_lab_stacks_set_the_m7_fallback_re_checks_on_full_and_bare(monkeypatch):
    """Every EdgeCitadel lab stack, full or bare, runs agentd's fallback
    re-checks at 1 s (code default 50 ms), so H11 compares like with like;
    dry runs and the A2A stacks keep the default."""
    import re

    configs = stack.HARNESS_ROOT / "configs"
    for path in sorted(configs.glob("*.json")):
        for name in re.findall(r"\$\{(\w+)\}", path.read_text()):
            monkeypatch.setenv(name, f"/x/{name.lower()}")
        config = stack.StackConfig.load(path)
        if config.mode == "lab" and config.system != "a2a":
            assert config.runtime_env == LAB_FALLBACKS, path.name
        else:
            assert config.runtime_env == {}, path.name


def test_runtime_env_reaches_agentd_and_the_workers_but_not_the_harness_settings(
    tmp_path, monkeypatch
):
    run = local_stack(tmp_path, runtime_env=dict(LAB_FALLBACKS))
    started = {}
    monkeypatch.setattr(
        run, "_start", lambda host, name, argv, **kw: started.update({name: kw["env"]})
    )
    monkeypatch.setattr(stack, "wait_until", lambda *args, **kw: None)
    monkeypatch.setattr(run.hosts["ctl"], "run", lambda *args, **kw: "")
    run._start_node("core", "nats://127.0.0.1:1")
    run._start_worker("a", "core", b"{}", 2)
    for name in ("agentd-core", "worker-a"):
        assert LAB_FALLBACKS.items() <= started[name].items()
    for bad in (
        {"EDGECITADEL_TRACE_SYNC": "0"},
        {"PATH": "/tmp"},
        {"EDGECITADEL_CLAIM_POLL_MS": 1000},
    ):
        with pytest.raises(stack.StackError, match="runtime_env"):
            stack._runtime_env(bad)
