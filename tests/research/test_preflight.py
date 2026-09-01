"""Two-phase benchmark preflight contract tests."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from scripts.research.artifact_env import ArtifactEnvironment
from scripts.research.preflight import (
    PreflightRequest,
    run_preflight,
    run_prestart_preflight,
)


@pytest.mark.asyncio
async def test_prestart_preflight_rejects_a_malformed_credential(
    tmp_path,
) -> None:
    credential = tmp_path / "credential"
    credential.write_text("placeholder\n")
    credential.chmod(0o600)
    request = PreflightRequest(
        run_id="ec-20260725-test-a",
        mode="edgecitadel",
        expected_agents=("worker-1", "observer-1"),
        resolved_config={"freshness_attestation": {}},
        credential_file=credential,
    )

    report = await run_prestart_preflight(request)

    assert report.valid is False
    assert report.errors
    assert all(
        set(check) == {"name", "passed", "observed", "expected"}
        for check in report.checks
    )
    with pytest.raises(RuntimeError):
        report.require_valid()


@pytest.mark.asyncio
async def test_prestart_preflight_requires_the_exact_empty_state_attestation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )
    request = PreflightRequest(
        run_id=environment.run_id,
        mode=environment.mode,
        expected_agents=("worker-1", "observer-1"),
        resolved_config=environment.resolved_config,
        credential_file=environment.credential_file,
    )

    assert (await run_prestart_preflight(request)).valid is True

    (environment.state_dir / "stale.json").write_text("{}\n")
    stale_report = await run_prestart_preflight(request)
    assert stale_report.valid is False
    assert (
        next(
            check
            for check in stale_report.checks
            if check["name"] == "freshness_attestation"
        )["passed"]
        is False
    )

    changed_attestation = dict(environment.resolved_config["freshness_attestation"])
    changed_attestation["state_dir"] = str(tmp_path / "other")
    altered_request = replace(
        request,
        resolved_config={"freshness_attestation": changed_attestation},
    )
    assert (await run_prestart_preflight(altered_request)).valid is False


@pytest.mark.asyncio
async def test_poststart_preflight_uses_runtime_attestation_not_empty_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_ARTIFACT_SCRATCH_ROOT", str(tmp_path / "scratch"))
    environment = ArtifactEnvironment.create(
        "ec-20260725-test-a",
        "edgecitadel",
        tmp_path / "raw",
    )
    config = dict(environment.resolved_config)
    config["poststart_attestation"] = {
        "authenticated": True,
        "network": {
            "endpoint_qdiscs": {"observer-1": "", "worker-1": ""},
            "profile": "lan",
            "probe_seconds": 5,
        },
        "ready_agents": ["observer-1", "worker-1"],
        "topology": {"mode": "edgecitadel"},
    }
    request = PreflightRequest(
        run_id=environment.run_id,
        mode=environment.mode,
        expected_agents=("worker-1", "observer-1"),
        resolved_config=config,
        credential_file=environment.credential_file,
    )
    (environment.state_dir / "runtime.sqlite").write_text("populated\n")

    report = await run_preflight(request)

    assert report.valid is True
    assert {check["name"] for check in report.checks} == {
        "credential",
        "mode",
        "agents",
        "resolved_config_mode",
        "authentication",
        "ready_agents",
        "topology_mode",
        "network_profile",
    }

    invalid_config = dict(config)
    invalid_config["poststart_attestation"] = {
        **config["poststart_attestation"],
        "authenticated": False,
    }
    invalid_report = await run_preflight(
        replace(request, resolved_config=invalid_config)
    )
    assert invalid_report.valid is False
    assert "authentication failed" in invalid_report.errors


@pytest.mark.asyncio
async def test_poststart_preflight_accepts_structural_lab_controller_snapshot(
    tmp_path: Path,
) -> None:
    credential = tmp_path / "credential"
    credential.write_text("a" * 64 + "\n")
    credential.chmod(0o600)
    request = PreflightRequest(
        run_id="ec-lab-01",
        mode="edgecitadel",
        expected_agents=(),
        resolved_config={
            "run_id": "ec-lab-01",
            "lab_variant": "lifecycle",
            "app_url": "http://127.0.0.1:18080",
            "agg_url": "http://127.0.0.1:18080",
            "nats_url": "nats://127.0.0.1:14222",
            "monitor_url": "http://127.0.0.1:18222",
            "inventory_url": "http://127.0.0.1:18080/api/lab/status",
        },
        credential_file=credential,
    )

    report = await run_preflight(request)

    assert report.valid is True
    assert (
        next(check for check in report.checks if check["name"] == "agents")["passed"]
        is True
    )
