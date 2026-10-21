"""The per-host lab helper: its scope, and a host driven through it over NATS."""

from __future__ import annotations

import base64
import os
import shutil
import socket
import subprocess
import sys
import threading
import time

import pytest

from harness import labhelper, stack
from harness.labhelper import Helper, HelperError


@pytest.fixture
def helper(tmp_path) -> Helper:
    # timeit stands in for a harness module: a harmless process to start.
    return Helper(tmp_path / "root", sys.executable, units="none", modules=("timeit",))


def sleeper(seconds: float) -> list[str]:
    return [
        sys.executable,
        "-m",
        "timeit",
        "-n",
        "1",
        f"__import__('time').sleep({seconds})",
    ]


def test_files_stay_under_the_experiment_root(helper, tmp_path):
    run = helper.root / "r1"
    secret = base64.b64encode(b"token").decode()
    helper.write(str(run / "nodes" / "x.token"), secret)
    assert (run / "nodes" / "x.token").stat().st_mode & 0o777 == 0o600
    assert (
        base64.b64decode(helper.read(str(run / "nodes" / "x.token"))["data"])
        == b"token"
    )
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (helper.root / "escape").symlink_to(outside)
    for path in (
        "/etc/passwd",
        str(helper.root / ".." / "elsewhere"),
        str(helper.root / "escape" / "f"),
    ):
        with pytest.raises(HelperError, match="outside the experiment root"):
            helper.write(path, secret)
        with pytest.raises(HelperError):
            helper.read(path)
    with pytest.raises(HelperError):
        helper.write(str(run / "x"), secret, mode=0o755)
    with pytest.raises(HelperError, match="invalid pattern"):
        helper.glob(str(run), ["../*"])
    # Only a whole run directory is removed.
    with pytest.raises(HelperError):
        helper.remove(str(run / "nodes"))
    with pytest.raises(HelperError):
        helper.remove(str(helper.root))
    assert helper.remove(str(run)) and not run.exists()


def test_only_harness_processes_start_and_run(helper):
    log = str(helper.root / "r1" / "logs" / "p.log")
    for argv in (
        ["/bin/sh", "-c", "true"],
        [sys.executable, "-c", "print(1)"],
        [sys.executable, "-m", "http.server"],
    ):
        with pytest.raises(HelperError):
            helper.start("p", argv, {}, str(helper.root), log)
    with pytest.raises(HelperError, match="not a short command"):
        helper.run(
            [sys.executable, "-m", "harness.nodectl", "serve", "--state", "x"], {}
        )
    with pytest.raises(HelperError):
        helper.run(sleeper(0), {})
    with pytest.raises(HelperError, match="not a harness unit"):
        helper.stop("sshd.service")


def test_a_started_process_is_a_named_unit_with_a_private_environment(helper):
    log = helper.root / "r1" / "logs" / "worker.log"
    started = helper.start(
        "worker",
        sleeper(30),
        {"HARNESS_SECRET": "s3cret"},
        str(helper.root / "r1"),
        str(log),
    )
    try:
        assert started["unit"] == "firebreak-exp-h-r1-worker"
        assert helper.alive(started["unit"])
        env_file = log.parent / ".env.worker"
        assert env_file.stat().st_mode & 0o777 == 0o600
        assert "s3cret" in env_file.read_text()
    finally:
        helper.stop(started["unit"])
    assert not helper.alive(started["unit"])


def nats_binary() -> str | None:
    return os.environ.get("HARNESS_NATS_SERVER") or shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.mark.skipif(nats_binary() is None, reason="needs nats-server")
def test_a_host_is_driven_through_its_helper_without_ssh(tmp_path):
    import asyncio

    token_file = tmp_path / "broker.token"
    token_file.write_text("lab-broker-token")
    port = free_port()
    conf = tmp_path / "nats.conf"
    conf.write_text(
        f'host: "127.0.0.1"\nport: {port}\nauthorization {{ token: "lab-broker-token" }}\n'
    )
    broker = subprocess.Popen(
        [nats_binary(), "-c", str(conf)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    root = tmp_path / "work"
    url = f"nats://127.0.0.1:{port}"
    served = Helper(root, sys.executable, units="none")
    loop = asyncio.new_event_loop()
    stop = asyncio.Event()
    thread = threading.Thread(
        target=loop.run_until_complete,
        args=(
            labhelper.serve(served, url, "lab-broker-token", "harness.lab.edge", stop),
        ),
        daemon=True,
    )
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                socket.create_connection(("127.0.0.1", port), 0.2).close()
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        thread.start()
        time.sleep(1.0)
        host = stack.make_host(
            stack.HostConfig(
                name="edge",
                address="127.0.0.1",
                python=sys.executable,
                repo=str(stack.REPO_ROOT),
                runtime_root=str(stack.REPO_ROOT),
                work_root=str(root),
                helper={"nats_url": url, "token_file": str(token_file)},
            )
        )
        assert isinstance(host, stack.HelperHost)
        run = f"{root}/r1"
        big = os.urandom(700_000)  # read back in several chunks
        host.write(f"{run}/logs/big.log", big)
        assert host.read(f"{run}/logs/big.log") == big
        assert host.exists(f"{run}/logs/big.log") and not host.exists(f"{run}/none")
        assert host.glob(run, ["logs/*.log"]) == [f"{run}/logs/big.log"]
        state = f"{run}/nodes/core"
        out = host.run(
            [sys.executable, "-m", "harness.nodectl", "socket", "--state", state]
        )
        assert out.strip().endswith(".sock")
        with pytest.raises(stack.StackError, match="outside the experiment root"):
            host.read("/etc/hostname")
        host.remove_tree(run)
        assert not host.exists(run)
        host.client.close()
    finally:
        if thread.is_alive():
            loop.call_soon_threadsafe(stop.set)
            thread.join(10)
        broker.terminate()
        broker.wait(10)
