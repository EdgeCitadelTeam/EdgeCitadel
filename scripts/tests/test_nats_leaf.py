from __future__ import annotations

import hashlib
import io
import json
import stat
import tarfile
from pathlib import Path

import pytest

from scripts import nats_leaf


def _release_archive(asset_name: str, binary: bytes) -> bytes:
    output = io.BytesIO()
    root_name = asset_name.removesuffix(".tar.gz")
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        member = tarfile.TarInfo(f"{root_name}/nats-server")
        member.mode = 0o755
        member.size = len(binary)
        archive.addfile(member, io.BytesIO(binary))
    return output.getvalue()


def _render(state_dir: Path) -> str:
    return nats_leaf.render_config(
        state_dir=state_dir,
        node_id="studio-macmini",
        upstream_nats_url="nats://core.test:4222",
        local_token="local-only-token",
        leaf_username="leaf-user",
        leaf_password="leaf-password",
    )


def test_render_config_is_deterministic_loopback_only_and_domain_scoped(tmp_path):
    first = _render(tmp_path)
    second = _render(tmp_path)

    assert first == second
    assert "listen: 127.0.0.1:4223" in first
    assert "http: 127.0.0.1:8223" in first
    assert "nats-leaf://leaf-user:leaf-password@core.test:7422" in first
    assert f'domain: "{nats_leaf.domain_for("studio-macmini")}"' in first
    assert str(tmp_path / "nats_leaf" / "data") in first
    assert "local-only-token" in first
    assert "change-me" not in first


def test_domain_is_stable_and_node_specific():
    assert nats_leaf.domain_for("edge-one") == nats_leaf.domain_for("edge-one")
    assert nats_leaf.domain_for("edge-one") != nats_leaf.domain_for("edge-two")


def test_leaf_endpoint_rejects_invalid_upstream():
    with pytest.raises(nats_leaf.NatsLeafError, match="upstream"):
        nats_leaf.leaf_endpoint("http://core.test")


@pytest.mark.parametrize(
    ("system", "machine", "expected"),
    (
        ("darwin", "x86_64", "darwin-amd64"),
        ("darwin", "arm64", "darwin-arm64"),
        ("linux", "amd64", "linux-amd64"),
        ("linux", "aarch64", "linux-arm64"),
    ),
)
def test_platform_asset_maps_supported_systems(system, machine, expected, monkeypatch):
    monkeypatch.setattr(nats_leaf.sys, "platform", system)
    monkeypatch.setattr(nats_leaf.platform, "machine", lambda: machine)

    _, _, asset_name, checksum = nats_leaf._platform_asset()

    assert expected in asset_name
    assert len(checksum) == 64


def test_platform_asset_explains_external_binary_fallback(monkeypatch):
    monkeypatch.setattr(nats_leaf.sys, "platform", "freebsd")
    monkeypatch.setattr(nats_leaf.platform, "machine", lambda: "riscv64")

    with pytest.raises(nats_leaf.NatsLeafError, match="EDGECITADEL_NATS_SERVER"):
        nats_leaf._platform_asset()


def test_explicit_binary_override_takes_precedence(tmp_path, monkeypatch):
    binary = tmp_path / "custom-nats-server"
    binary.write_text("#!/bin/sh\n", encoding="utf-8")
    binary.chmod(0o700)
    monkeypatch.setenv("EDGECITADEL_NATS_SERVER", str(binary))
    monkeypatch.setattr(
        nats_leaf.shutil,
        "which",
        lambda _name: (_ for _ in ()).throw(AssertionError("PATH was consulted")),
    )

    assert nats_leaf._binary(tmp_path) == str(binary.resolve())


def test_missing_nats_server_is_provisioned_once_with_verified_release(
    tmp_path, monkeypatch, capsys
):
    asset_name = "nats-server-v2.14.6-darwin-arm64.tar.gz"
    archive = _release_archive(asset_name, b"#!/bin/sh\necho 'nats-server: v2.14.6'\n")
    downloads: list[str] = []
    monkeypatch.delenv("EDGECITADEL_NATS_SERVER", raising=False)
    monkeypatch.setattr(nats_leaf.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        nats_leaf,
        "_platform_asset",
        lambda: ("darwin", "arm64", asset_name, hashlib.sha256(archive).hexdigest()),
    )
    monkeypatch.setattr(
        nats_leaf,
        "_download_release_asset",
        lambda name: downloads.append(name) or archive,
    )

    first = Path(nats_leaf._binary(tmp_path))
    second = Path(nats_leaf._binary(tmp_path))

    assert first == nats_leaf._managed_binary(tmp_path).resolve()
    assert second == first
    assert downloads == [asset_name]
    assert stat.S_IMODE(first.stat().st_mode) == 0o700
    assert "checksum-verified NATS Server v2.14.6" in capsys.readouterr().err


def test_provision_rejects_release_with_wrong_checksum(tmp_path, monkeypatch):
    asset_name = "nats-server-v2.14.6-linux-amd64.tar.gz"
    archive = _release_archive(asset_name, b"#!/bin/sh\necho 'nats-server: v2.14.6'\n")
    monkeypatch.setattr(
        nats_leaf,
        "_platform_asset",
        lambda: ("linux", "amd64", asset_name, "0" * 64),
    )
    monkeypatch.setattr(nats_leaf, "_download_release_asset", lambda _name: archive)

    with pytest.raises(nats_leaf.NatsLeafError, match="SHA-256"):
        nats_leaf._provision_binary(tmp_path)

    assert not nats_leaf._managed_binary(tmp_path).exists()


def test_provision_rejects_archive_without_expected_regular_file(tmp_path, monkeypatch):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        link = tarfile.TarInfo("nats-server-v2.14.6-linux-amd64/nats-server")
        link.type = tarfile.SYMTYPE
        link.linkname = "/tmp/not-nats"
        archive.addfile(link)
    content = output.getvalue()
    asset_name = "nats-server-v2.14.6-linux-amd64.tar.gz"
    monkeypatch.setattr(
        nats_leaf,
        "_platform_asset",
        lambda: ("linux", "amd64", asset_name, hashlib.sha256(content).hexdigest()),
    )
    monkeypatch.setattr(nats_leaf, "_download_release_asset", lambda _name: content)

    with pytest.raises(nats_leaf.NatsLeafError, match="invalid layout"):
        nats_leaf._provision_binary(tmp_path)


def test_configure_writes_private_state_and_separates_credentials(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(nats_leaf, "_validate_config", lambda *args: None)
    monkeypatch.setattr(nats_leaf, "_service_mode", lambda *args: "process")
    monkeypatch.setattr(nats_leaf, "_start_process", lambda *args: None)
    monkeypatch.setattr(
        nats_leaf,
        "wait_ready",
        lambda *args, **kwargs: {
            "state": "leaf_connected",
            "local_ready": True,
            "leaf_connected": True,
        },
    )

    nats_leaf.configure_and_start(
        state_dir=tmp_path,
        node_id="edge-one",
        upstream_nats_url="nats://core.test:4222",
        local_token="local-only-token",
        leaf_username="leaf-user",
        leaf_password="leaf-password",
        binary="/test/nats-server",
    )

    value_paths = nats_leaf.paths(tmp_path)
    credentials = json.loads(value_paths["credentials"].read_text())
    assert credentials["leaf_password"] == "leaf-password"
    assert "leaf-password" in value_paths["config"].read_text()
    assert stat.S_IMODE(value_paths["root"].stat().st_mode) == 0o700
    assert stat.S_IMODE(value_paths["config"].stat().st_mode) == 0o600
    assert stat.S_IMODE(value_paths["credentials"].stat().st_mode) == 0o600


def test_observe_distinguishes_local_ready_from_leaf_connected(tmp_path, monkeypatch):
    value_paths = nats_leaf.paths(tmp_path)
    value_paths["config"].parent.mkdir(parents=True)
    value_paths["config"].write_text("test")
    value_paths["pid"].write_text("123")
    monkeypatch.setattr(nats_leaf, "_pid_running", lambda pid: True)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    monkeypatch.setattr(
        nats_leaf.socket, "create_connection", lambda *args, **kwargs: Connection()
    )
    responses = {
        "/healthz": {"status": "ok"},
        "/jsz": {"streams": 1},
        "/leafz": {"leafnodes": 0},
    }
    monkeypatch.setattr(nats_leaf, "_http_json", lambda path: responses[path])

    degraded = nats_leaf.observe(tmp_path)
    assert degraded["state"] == "degraded"
    assert degraded["local_ready"] is True
    assert degraded["leaf_connected"] is False

    responses["/leafz"] = {"leafnodes": 1}
    assert nats_leaf.observe(tmp_path)["state"] == "leaf_connected"


def test_cleanup_failed_join_removes_secret_material(tmp_path, monkeypatch):
    value_paths = nats_leaf.paths(tmp_path)
    value_paths["data"].mkdir(parents=True)
    for key in ("config", "credentials", "pid", "plist"):
        value_paths[key].write_text("secret")
    monkeypatch.setattr(nats_leaf, "stop", lambda *args: None)

    nats_leaf.cleanup_failed_join(tmp_path)

    for key in ("config", "credentials", "pid", "plist"):
        assert not value_paths[key].exists()
    assert not value_paths["data"].exists()
    assert json.loads(value_paths["lifecycle"].read_text())["state"] == "failed"


def test_launchd_start_reloads_job_to_reconcile_upgraded_binary(tmp_path, monkeypatch):
    calls: list[list[str]] = []

    class Result:
        returncode = 0

    monkeypatch.setattr(nats_leaf, "_launchd_loaded", lambda: True)
    monkeypatch.setattr(
        nats_leaf.subprocess,
        "run",
        lambda command, **kwargs: calls.append(command) or Result(),
    )

    nats_leaf._start_launchd("/new-cellar/bin/nats-server", tmp_path)

    assert calls[0] == ["launchctl", "bootout", nats_leaf._launchd_target()]
    assert calls[1][:3] == ["launchctl", "bootstrap", f"gui/{nats_leaf.os.getuid()}"]
    plist = nats_leaf.plistlib.loads(nats_leaf.paths(tmp_path)["plist"].read_bytes())
    assert plist["ProgramArguments"][0] == "/new-cellar/bin/nats-server"
