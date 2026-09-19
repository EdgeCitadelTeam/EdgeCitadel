"""Administrator preflight must never overwrite existing identities or storage."""

import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def provisioner(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "trace-storage/provision.py"
    spec = importlib.util.spec_from_file_location("trace_provision", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.os, "geteuid", lambda: 0)
    monkeypatch.setattr(module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(module.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(module, "_STORAGE", tmp_path / "images")
    monkeypatch.setattr(module, "_UNITS", tmp_path / "units")
    module._STORAGE.mkdir(mode=0o700)
    module._UNITS.mkdir()
    monkeypatch.setattr(
        module,
        "_mount_unit",
        lambda path: "trace.mount" if "agentd" in path.parts else "volume.mount",
    )
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/" + name)

    def missing(name):
        raise KeyError(name)

    def mutation(*args):
        pytest.fail("refused provisioning attempted a system mutation")

    monkeypatch.setattr(module.pwd, "getpwnam", missing)
    monkeypatch.setattr(module, "_run", mutation)
    return module


@pytest.mark.parametrize("destination", ["image", "unit", "dangling_image"])
def test_existing_destination_is_preserved(provisioner, destination):
    path = provisioner._STORAGE / "ownedtest.ext4"
    if destination == "unit":
        path = provisioner._UNITS / "volume.mount"
    if destination == "dangling_image":
        path.symlink_to(path.parent / "absent")
    else:
        path.write_bytes(b"existing administrator resource")
    with pytest.raises(RuntimeError, match="destination already exists"):
        provisioner.provision("ownedtest")
    if destination == "dangling_image":
        assert path.is_symlink()
    else:
        assert path.read_bytes() == b"existing administrator resource"


def test_existing_account_is_never_reused(provisioner, monkeypatch):
    monkeypatch.setattr(provisioner.pwd, "getpwnam", lambda name: object())
    with pytest.raises(RuntimeError, match="account already exists"):
        provisioner.provision("ownedtest")
    assert list(provisioner._STORAGE.iterdir()) == []


def test_writable_image_parent_refuses_before_account_creation(provisioner):
    provisioner._STORAGE.chmod(0o777)
    with pytest.raises(RuntimeError, match="private and root-owned"):
        provisioner.provision("ownedtest")
    assert provisioner._STORAGE.stat().st_mode & 0o777 == 0o777


def test_unsupported_syscall_architecture_refuses_before_commands(
    provisioner, monkeypatch
):
    monkeypatch.setattr(provisioner.platform, "machine", lambda: "unqualified")
    monkeypatch.setattr(
        provisioner, "_mount_unit", lambda path: pytest.fail("unexpected command")
    )
    with pytest.raises(RuntimeError, match="64-bit Linux"):
        provisioner.provision("ownedtest")


def test_unprivileged_invocation_refuses_before_commands(provisioner, monkeypatch):
    monkeypatch.setattr(provisioner.os, "geteuid", lambda: 1001)
    monkeypatch.setattr(
        provisioner, "_mount_unit", lambda path: pytest.fail("unexpected command")
    )
    with pytest.raises(RuntimeError, match="Linux administrator"):
        provisioner.provision("ownedtest")
