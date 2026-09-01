from __future__ import annotations

import grp
import os
import subprocess
import sys
from pathlib import Path


RECONCILE = Path(__file__).parents[1] / "lib" / "reconcile-env.py"
PHASE_ONE = Path(__file__).parents[1] / "lib" / "_phase_1_install_deps.sh"
UPDATE = Path(__file__).parents[1] / "lib" / "_update.sh"


def _run(path: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RECONCILE),
            "--env-file",
            str(path),
            "--group",
            grp.getgrgid(os.getgid()).gr_name,
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _values(path: Path) -> dict[str, str]:
    return dict(
        line.split("=", 1) for line in path.read_text().splitlines() if "=" in line
    )


def test_reconcile_preserves_existing_values_and_generates_missing_secrets(
    tmp_path,
):
    env = tmp_path / "env"
    env.write_text("NATS_TOKEN=existing-token\nUNRELATED=value\nNATS_LEAF_USERNAME=\n")

    result = _run(env)

    assert result.returncode == 0, result.stdout + result.stderr
    values = _values(env)
    assert values["NATS_TOKEN"] == "existing-token"
    assert values["UNRELATED"] == "value"
    generated = {
        "NATS_LEAF_USERNAME",
        "NATS_LEAF_PASSWORD",
        "EDGECITADEL_ADMIN_TOKEN",
    }
    assert generated.issubset(values)
    assert all(values[key] and "change-me" not in values[key] for key in generated)
    assert all(values[key] not in result.stdout for key in generated)
    assert env.stat().st_mode & 0o777 == 0o640

    first = env.read_bytes()
    repeated = _run(env)
    assert repeated.returncode == 0
    assert env.read_bytes() == first
    assert "already reconciled" in repeated.stdout


def test_check_reports_names_without_mutating_or_exposing_values(tmp_path):
    env = tmp_path / "env"
    env.write_text("NATS_TOKEN=change-me\n")
    original = env.read_bytes()

    result = _run(env, "--check")

    assert result.returncode == 1
    assert env.read_bytes() == original
    assert "NATS_TOKEN" in result.stdout
    assert "NATS_LEAF_PASSWORD" in result.stdout


def test_reconcile_rejects_missing_file(tmp_path):
    result = _run(tmp_path / "missing")

    assert result.returncode == 1
    assert "missing or unsafe" in result.stdout


def test_install_and_update_reconcile_before_service_affecting_changes():
    phase_one = PHASE_ONE.read_text()
    update = UPDATE.read_text()

    assert phase_one.index("reconcile-env.py") < phase_one.index("# Sync /opt")
    assert update.index("reconcile-env.py") < update.index("run rsync")
