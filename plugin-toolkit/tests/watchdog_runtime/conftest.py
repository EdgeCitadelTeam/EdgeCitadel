import sys
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest


PLUGIN_ROOT = Path(__file__).parents[3] / "plugins" / "watchdog"
if str(PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT))


@pytest.fixture
def nats_url(tmp_path: Path, free_tcp_port: int) -> str:
    """Run an isolated JetStream broker and remove only its temporary state."""
    executable = shutil.which("nats-server")
    if executable is None:
        pytest.skip("nats-server is not installed")
    process = subprocess.Popen(
        [
            executable,
            "-js",
            "-p",
            str(free_tcp_port),
            "-m",
            "-1",
            "-sd",
            str(tmp_path / "jetstream"),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", free_tcp_port), timeout=0.1):
                break
        except OSError:
            if process.poll() is not None:
                pytest.fail("isolated nats-server exited during startup")
            time.sleep(0.05)
    else:
        process.terminate()
        process.wait(timeout=2)
        pytest.fail("isolated nats-server did not become ready")

    yield f"nats://127.0.0.1:{free_tcp_port}"

    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)
