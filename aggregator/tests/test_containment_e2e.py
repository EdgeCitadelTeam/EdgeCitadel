"""The C2 scenario with real processes: NATS, Core with the monitor, three agentd
nodes and one Managed Agent process per agent (``e2e/containment/c2.py``).

Opt-in (about a minute per case): RUN_CONTAINMENT_E2E=1 with nats-server on PATH.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_CONTAINMENT_E2E") != "1" or shutil.which("nats-server") is None,
    reason="set RUN_CONTAINMENT_E2E=1 with nats-server on PATH",
)


def run_c2(tmp_path: Path, strategy: str, variant: str, *options: str) -> dict:
    out = tmp_path / f"{strategy}-{variant}.json"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "e2e.containment.c2",
            "--strategy",
            strategy,
            "--variant",
            variant,
            "--root",
            str(tmp_path),
            "--out",
            str(out),
            *options,
        ],
        cwd=REPO,
        env={**os.environ, "PYTHONPATH": f"{REPO}:{REPO / 'agent-runtime/src'}"},
        check=True,
        timeout=400,
    )
    return json.loads(out.read_text())


@pytest.mark.parametrize("variant", ["early", "late"])
def test_firebreak_contains_the_shared_artifact_and_keeps_healthy_work(
    tmp_path, variant
):
    result = run_c2(tmp_path, "firebreak", variant)
    assert result["root_cause"]["names_producer"]
    assert (result["status"], result["cut_valid"]) == ("contained", True)
    assert result["releases_after_confirmations"] and not result["unconfirmed"]
    # The scheduled actuator command never ran (independent ledger) and agentd
    # never committed it.
    assert not result["effect_performed"]
    assert "committed" not in result["effect"]["agentd_state"].values()
    assert result["healthy_completed"] == 2
    # What the monitor holds in force is exactly what the nodes enforce.
    held = {
        (node, hold["object_id"])
        for node, listing in result["enforced_at_nodes"].items()
        for hold in listing["holds"]
    }
    in_force = {
        (a["action"].rsplit("@", 1)[1], a["object_id"])
        for a in result["actions"]
        if a["state"] == "in_force" and a["action"].startswith("hold:")
    }
    assert held == in_force
    for action in result["actions"]:
        if action["state"] in {"in_force", "released"}:
            times = action["ms_after_alert"]
            assert 0 <= times["t_decision"] <= times["t_sent"] <= times["t_confirmed"]


def test_phase1_with_the_at_alert_cut_blocks_the_late_effect_from_the_first_round(
    tmp_path,
):
    """phase1_include_cut: the first round issues Firebreak's at-alert cut with
    the freeze; the actuator command is held from that round on, and every
    release still follows the confirmed phase-2 cut."""
    result = run_c2(tmp_path, "firebreak", "late", "--phase1-include-cut")
    assert result["phase1_include_cut"] and result["profile"] == "deployed"
    assert (result["status"], result["cut_valid"]) == ("contained", True)
    effect_holds = [
        a for a in result["actions"] if a["action"].startswith("hold:effect")
    ]
    assert effect_holds and {a["phase"] for a in effect_holds} == {1}
    assert all(a["state"] == "in_force" for a in effect_holds)
    assert not result["effect_performed"]
    assert "committed" not in result["effect"]["agentd_state"].values()
    assert result["releases_after_confirmations"] and not result["unconfirmed"]
