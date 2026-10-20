"""Run as edgecitadel-leaf on jim-eq; secrets never enter command-line arguments."""

import os
import subprocess
from pathlib import Path

for line in (
    Path("/var/lib/edgecitadel-leaf/state/credentials/jev.env").read_text().splitlines()
):
    if line.startswith("TYPESAFE_API_KEY="):
        os.environ["TYPESAFE_API_KEY"] = (
            line.split("=", 1)[1].strip().strip('"').strip("'")
        )
assert os.environ.get("TYPESAFE_API_KEY")
os.environ.update(
    XDG_RUNTIME_DIR="/run/user/993",
    DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/993/bus",
    EDGECITADEL_SUPERVISOR_PYTHON="/opt/edgecitadel/jev-20260926/venv/bin/python",
    PI_ROLE="root",
    PI_NODE="/opt/edgecitadel/pi-20260927/runtime/node-v24.21.0-linux-x64/bin/node",
    PI_SDK_DIR="/opt/edgecitadel/pi-20260927/runtime/sdk",
    PI_WORKSPACE="/var/lib/edgecitadel-leaf/pi-workspace",
)
subprocess.run(
    [
        "/opt/edgecitadel/jev-20260926/scripts/edgecitadel",
        "agent",
        "install",
        "/opt/edgecitadel/pi-20260927/pi-root",
        "--state-dir",
        "/var/lib/edgecitadel-leaf/state",
        "--yes",
    ],
    check=True,
)
