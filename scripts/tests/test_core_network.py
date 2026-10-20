"""Contract checks for the creation guide's owned runtime and listener table."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import core_network as network


def policy(mode="local", *, mqtt=False, bind=None):
    return {
        "version": 1,
        "mode": mode,
        "bind_address": bind,
        "generation": 1,
        "mqtt": mqtt,
    }


def descriptor(tmp_path):
    return {
        "version": 1,
        "owner": str(tmp_path / "state/node.json"),
        "runtime": str(tmp_path / "runtime"),
        "project": "test-core",
        "compose_file": str(tmp_path / "source/docker-compose.yml"),
        "generation": 1,
        "phase": "configured",
        "docker": None,
    }


@pytest.mark.parametrize(
    "mode,bind",
    [
        ("local", None),
        ("tailscale", "100.80.0.1"),
        ("custom", "192.168.1.10"),
        ("custom", "2001:db8::1"),
    ],
)
@pytest.mark.parametrize("mqtt", [False, True])
def test_publications_match_complete_listener_contract(mode, bind, mqtt):
    result = network.publications(policy(mode, mqtt=mqtt, bind=bind))
    tuples = {
        (service, port["host_ip"], int(port["published"]), port["target"])
        for service, ports in result.items()
        for port in ports
    }
    expected = {("nginx", "127.0.0.1", 80, 80)}
    expected |= {
        ("nats", "127.0.0.1", port, port)
        for port in [4222, 7422, 8222] + ([1883] if mqtt else [])
    }
    if bind:
        expected.add(("nginx", bind, 80, 80))
        expected |= {
            ("nats", bind, port, port)
            for port in [4222, 7422] + ([1883] if mqtt else [])
        }
    assert tuples == expected


@pytest.mark.parametrize(
    "bind",
    [
        "0.0.0.0",
        "::",
        "::ffff:127.0.0.1",
        "127.0.0.1",
        "224.0.0.1",
        "fe80::1%eth0",
        "localhost",
        "bad host",
    ],
)
def test_remote_policy_never_accepts_wildcard_or_local_bind(bind):
    with pytest.raises(network.NetworkError):
        network.publications(policy("custom", bind=bind))


def test_atomic_write_is_private_and_leaves_no_staging_files(tmp_path):
    target = tmp_path / "private/node.json"
    network.write_json(target, {"generation": 1})
    network.write_json(target, {"generation": 2})
    assert json.loads(target.read_text()) == {"generation": 2}
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert list(target.parent.iterdir()) == [target]


def test_failed_atomic_replace_preserves_previous_state(tmp_path, monkeypatch):
    target = tmp_path / "node.json"
    network.atomic_write(target, "previous")

    def failure(*args):
        raise OSError("fault injection before node commit")

    monkeypatch.setattr(Path, "replace", failure)
    with pytest.raises(OSError, match="fault injection"):
        network.atomic_write(target, "candidate")
    assert target.read_text() == "previous"
    assert list(tmp_path.iterdir()) == [target]


def test_runtime_lock_rejects_concurrent_process_without_deleting_lock(tmp_path):
    path = tmp_path / "runtime.lock"
    code = "from pathlib import Path; from scripts.core_network import lock; "
    code += f"\nwith lock(Path({str(path)!r})): pass"
    with network.lock(path):
        inode = path.stat().st_ino
        child = subprocess.run(
            [sys.executable, "-c", code],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
        )
        assert child.returncode != 0
        assert "Core runtime is busy" in child.stderr
    with network.lock(path):
        assert path.stat().st_ino == inode


def test_descriptor_rejects_other_owner_and_newer_format(tmp_path):
    runtime = tmp_path / "runtime"
    value = descriptor(tmp_path)
    network.write_json(runtime / network.DESCRIPTOR, value)
    assert network.read_descriptor(runtime, tmp_path / "state") == value
    with pytest.raises(network.NetworkError, match="different --state-dir"):
        network.read_descriptor(runtime, tmp_path / "other")
    value["version"] = 99
    network.write_json(runtime / network.DESCRIPTOR, value)
    with pytest.raises(network.NetworkError, match="unsupported"):
        network.read_descriptor(runtime, tmp_path / "state")


def model(selected):
    ports = network.publications(selected)
    return {
        "services": {
            name: {"ports": ports.get(name, []), "networks": {"default": None}}
            for name in ("nats", "aggregator", "dashboard", "nginx")
        },
        "networks": {"default": {}},
    }


@pytest.mark.parametrize(
    "change",
    ["wildcard", "extra-service", "host-network", "extra-network", "routed", "ipv6"],
)
def test_effective_model_fails_closed_on_extra_exposure(change):
    selected = policy()
    value = model(selected)
    network.verify_model(value, selected)
    if change == "wildcard":
        value["services"]["nats"]["ports"].append({"published": "4222", "target": 4222})
    elif change == "extra-service":
        value["services"]["proxy"] = {"ports": []}
    elif change == "host-network":
        value["services"]["aggregator"]["network_mode"] = "host"
    elif change == "extra-network":
        value["services"]["nats"]["networks"]["routed"] = None
    elif change == "routed":
        value["networks"]["default"]["driver_opts"] = {
            "com.docker.network.bridge.gateway_mode_ipv4": "routed"
        }
    else:
        value["networks"]["default"]["enable_ipv6"] = True
    with pytest.raises(network.NetworkError):
        network.verify_model(value, selected)


def test_context_or_daemon_change_is_not_a_new_empty_core():
    identity = {
        "context": "default",
        "endpoint": "unix:///var/run/docker.sock",
        "daemon_id": "one",
    }
    network.assert_identity(identity, identity)
    for field in identity:
        with pytest.raises(network.NetworkError, match="changed"):
            network.assert_identity(identity, {**identity, field: "different"})


def test_remote_docker_endpoint_rejected_before_daemon_inspection(monkeypatch):
    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    monkeypatch.setattr(network, "_read_command", lambda command: "remote")
    monkeypatch.setattr(
        network,
        "_json_command",
        lambda command: [{"Endpoints": {"docker": {"Host": "ssh://remote"}}}],
    )
    with pytest.raises(network.NetworkError, match="local Docker"):
        network.docker_identity()


@pytest.mark.parametrize(
    "override", ["COMPOSE_FILE", "COMPOSE_PROFILES", "COMPOSE_PROJECT_NAME"]
)
def test_managed_compose_rejects_ambient_overrides(tmp_path, monkeypatch, override):
    value = descriptor(tmp_path)
    value["docker"] = {"context": "default"}
    monkeypatch.setenv(override, "unexpected")
    with pytest.raises(network.NetworkError):
        network.compose_command(tmp_path, value, "up")


def test_managed_compose_pins_every_input(tmp_path, monkeypatch):
    for variable in ("COMPOSE_FILE", "COMPOSE_PROFILES", "COMPOSE_PROJECT_NAME"):
        monkeypatch.delenv(variable, raising=False)
    value = descriptor(tmp_path)
    value["docker"] = {"context": "desktop-linux"}
    command = network.compose_command(tmp_path, value, "config", "--format", "json")
    assert command[:6] == [
        "docker",
        "--context",
        "desktop-linux",
        "compose",
        "--project-name",
        "test-core",
    ]
    assert command[6:] == [
        "--env-file",
        str(tmp_path / ".env"),
        "-f",
        value["compose_file"],
        "-f",
        str(tmp_path / "docker-compose.managed.yml"),
        "config",
        "--format",
        "json",
    ]


def test_preflight_stages_private_inputs_without_touching_runtime(
    tmp_path, monkeypatch
):
    runtime = tmp_path / "runtime"
    value = descriptor(tmp_path)
    value["docker"] = {"context": "test"}
    observed = []

    def inspect(command):
        env_file = Path(command[command.index("--env-file") + 1])
        observed.append(env_file)
        assert "real-test-credential" not in env_file.read_text()
        assert 'CUSTOM_VALUE="preserve-for-interpolation"' in env_file.read_text()
        assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
        return model(policy())

    monkeypatch.setattr(network, "_json_command", inspect)
    network.preflight_model(
        runtime,
        value,
        policy(),
        {
            "NATS_TOKEN": "real-test-credential",
            "CUSTOM_VALUE": "preserve-for-interpolation",
        },
    )
    assert not runtime.exists()
    assert observed and not observed[0].parent.exists()


@pytest.mark.parametrize(
    "change", [None, "extra-network", "routed", "ipv6", "privileged"]
)
def test_running_topology_is_qualified_beyond_ports(monkeypatch, change):
    selected = policy()
    containers = []
    for name in ("nats", "aggregator", "dashboard", "nginx"):
        ports = {}
        for publication in network.publications(selected).get(name, []):
            ports.setdefault(f"{publication['target']}/tcp", []).append(
                {"HostIp": publication["host_ip"], "HostPort": publication["published"]}
            )
        containers.append(
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.service": name,
                        "com.docker.compose.project": "owned",
                    }
                },
                "State": {"Running": True},
                "HostConfig": {"NetworkMode": "owned_default"},
                "NetworkSettings": {
                    "Ports": ports,
                    "Networks": {"owned_default": {"NetworkID": "owned-network-id"}},
                },
            }
        )
    bridge = {"Driver": "bridge", "EnableIPv6": False, "Internal": False, "Options": {}}
    if change == "extra-network":
        containers[0]["NetworkSettings"]["Networks"]["extra"] = {"NetworkID": "other"}
    elif change == "routed":
        bridge["Options"] = {"com.docker.network.bridge.gateway_mode_ipv4": "routed"}
    elif change == "ipv6":
        bridge["EnableIPv6"] = True
    elif change == "privileged":
        containers[0]["HostConfig"]["Privileged"] = True

    def inspect(command):
        assert command == [
            "docker",
            "--context",
            "owned-context",
            "network",
            "inspect",
            "owned-network-id",
        ]
        return [bridge]

    monkeypatch.setattr(network, "_json_command", inspect)
    if change:
        with pytest.raises(network.NetworkError):
            network.verify_bindings(containers, selected, context="owned-context")
    else:
        network.verify_bindings(containers, selected, context="owned-context")


@pytest.mark.parametrize(
    "mode,bind",
    [("local", None), ("tailscale", "100.80.0.1"), ("custom", "192.168.1.10")],
)
@pytest.mark.parametrize("mqtt", [False, True])
def test_actual_compose_merge_replaces_wildcards(tmp_path, mode, bind, mqtt):
    # Pure effective-model proof: no daemon calls, image pulls, or service starts.
    if not os.environ.get("RUN_CORE_COMPOSE_MODEL"):
        pytest.skip("set RUN_CORE_COMPOSE_MODEL=1 with Compose 2.24.4+ installed")
    source = Path(__file__).resolve().parents[2] / "docker-compose.yml"
    override = tmp_path / "managed.yml"
    selected = policy(mode, mqtt=mqtt, bind=bind)
    override.write_text(
        network.render_override(tmp_path, str(tmp_path / "state/node.json"), selected)
    )
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            "ec-guide-model-test",
            "-f",
            str(source),
            "-f",
            str(override),
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    )
    value = json.loads(result.stdout)
    network.verify_model(value, selected)
    assert value["services"]["aggregator"]["volumes"][0]["source"] == str(
        tmp_path / "data"
    )
    for service in value["services"].values():
        assert service["labels"][network.OWNER_LABEL] == str(
            tmp_path / "state/node.json"
        )


@pytest.mark.parametrize(
    "projects,requested,expected",
    [
        (["custom-project"], None, "custom-project"),
        (["custom-project", "custom-project"], "custom-project", "custom-project"),
        ([], "explicit-project", "explicit-project"),
        ([], None, "checkout"),
        (["one", "two"], None, "Multiple Compose projects"),
        (["one"], "two", "conflicts"),
    ],
)
def test_source_project_preserves_labels_and_rejects_ambiguity(
    tmp_path, monkeypatch, projects, requested, expected
):
    source = tmp_path / "docker-compose.yml"
    inspected = [
        {
            "Config": {
                "Labels": {
                    "com.docker.compose.project": project,
                    "com.docker.compose.project.config_files": str(source),
                }
            }
        }
        for project in projects
    ]
    inspected.append(
        {
            "Config": {
                "Labels": {
                    "com.docker.compose.project": "unrelated",
                    "com.docker.compose.project.config_files": "/unrelated/docker-compose.yml",
                }
            }
        }
    )
    monkeypatch.setattr(network, "_read_command", lambda command: "test-container")
    monkeypatch.setattr(network, "_json_command", lambda command: inspected)
    if requested:
        monkeypatch.setenv("COMPOSE_PROJECT_NAME", requested)
    else:
        monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    if expected in {"Multiple Compose projects", "conflicts"}:
        with pytest.raises(network.NetworkError, match=expected):
            network.source_project({"context": "test"}, source, "checkout")
    else:
        assert (
            network.source_project({"context": "test"}, source, "checkout") == expected
        )


def test_project_lock_serializes_different_runtime_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    identity = {"daemon_id": "owned-test-daemon"}
    child = """
from pathlib import Path
from scripts.core_network import lock, project_lock
import sys
Path.home = lambda: Path(sys.argv[1])
with lock(Path(sys.argv[1]) / 'other-runtime/runtime.lock'):
    with project_lock({'daemon_id': 'owned-test-daemon'}, 'same-project'):
        pass
"""
    with network.lock(tmp_path / "first-runtime/runtime.lock"):
        with network.project_lock(identity, "same-project"):
            result = subprocess.run(
                [sys.executable, "-c", child, str(tmp_path)],
                cwd=Path(__file__).resolve().parents[2],
                capture_output=True,
                text=True,
            )
            assert result.returncode != 0 and "Core runtime is busy" in result.stderr
    result = subprocess.run(
        [sys.executable, "-c", child, str(tmp_path)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
