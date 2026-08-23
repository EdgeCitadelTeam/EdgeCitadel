"""Shared preflight adapter for the authenticated multi-agent lab."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Callable

from scripts.research.lab_config import ControllerConfig, LabConfigError, credential_token
from scripts.research.preflight import PreflightReport, PreflightRequest, run_preflight

_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")


def _request_json(
    url: str,
    *,
    token: str | None = None,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> object | None:
    headers = {"Accept": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with opener(request, timeout=2) as response:  # type: ignore[attr-defined]
            if response.status != 200:  # type: ignore[attr-defined]
                return None
            return json.loads(response.read())  # type: ignore[attr-defined]
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def _check(name: str, passed: bool, observed: object, expected: object) -> dict[str, object]:
    return {
        "name": name,
        "passed": passed,
        "observed": observed,
        "expected": expected,
    }


async def run_controller_preflight(
    controller_config: ControllerConfig,
    credential_file: Path,
    expected_agents: tuple[str, ...] = (),
    *,
    opener: Callable[..., object] = urllib.request.urlopen,
) -> PreflightReport:
    """Run the shared preflight and append lab-specific live attestations."""
    request = PreflightRequest(
        run_id=controller_config.run_id,
        mode="edgecitadel",
        expected_agents=tuple(expected_agents),
        resolved_config=controller_config.to_dict(),
        credential_file=credential_file,
    )
    shared = await run_preflight(request)
    try:
        token = credential_token(credential_file)
    except LabConfigError:
        token = None
    system_status = _request_json(
        f"{controller_config.agg_url}/api/system/status", opener=opener
    )
    inventory = (
        _request_json(controller_config.inventory_url, token=token, opener=opener)
        if token is not None
        else None
    )
    registry = _request_json(
        f"{controller_config.agg_url}/api/registry", opener=opener
    )
    monitor = _request_json(
        f"{controller_config.monitor_url.removesuffix('/')}/varz", opener=opener
    )

    system_valid = (
        isinstance(system_status, Mapping)
        and system_status.get("nats_connected") is True
        and system_status.get("jetstream_stream_ok") is True
    )
    inventory_valid = (
        isinstance(inventory, Mapping)
        and inventory.get("run_id") == controller_config.run_id
        and isinstance(inventory.get("reservations"), list)
        and isinstance(inventory.get("reservation_events"), list)
        and isinstance(inventory.get("node_reports"), list)
    )
    registry_shape_valid = isinstance(registry, list) and all(
        isinstance(item, Mapping)
        and isinstance(item.get("agent_id"), str)
        and bool(item.get("agent_id"))
        and isinstance(item.get("agent_state"), str)
        for item in registry
    )
    online_agents = {
        item.get("agent_id")
        for item in registry
        if (
            isinstance(item, Mapping)
            and item.get("agent_state") == "online"
            and isinstance(item.get("agent_id"), str)
            and bool(item.get("agent_id"))
        )
    } if isinstance(registry, list) else set()
    registry_valid = registry_shape_valid and set(expected_agents).issubset(online_agents)
    mqtt_absent = isinstance(monitor, Mapping) and monitor.get("mqtt") in (None, False, {})
    fixture_immutable = _IMAGE_ID.fullmatch(controller_config.fixture_image_id) is not None

    checks = shared.checks + (
        _check(
            "system_status_semantic",
            system_valid,
            "healthy" if system_valid else "invalid",
            "NATS and JetStream ready",
        ),
        _check(
            "lab_inventory_authenticated",
            inventory_valid,
            "authenticated" if inventory_valid else "invalid",
            "run-scoped authenticated inventory",
        ),
        _check(
            "registry_ready",
            registry_valid,
            sorted(online_agents),
            sorted(expected_agents),
        ),
        _check(
            "mqtt_not_listening",
            mqtt_absent,
            "absent" if mqtt_absent else "configured",
            "absent",
        ),
        _check(
            "fixture_image_immutable",
            fixture_immutable,
            controller_config.fixture_image_id if fixture_immutable else "invalid",
            "sha256 image ID",
        ),
    )
    errors = shared.errors + tuple(
        f"{check['name']} failed" for check in checks[len(shared.checks):]
        if check["passed"] is False
    )
    return PreflightReport(
        valid=not errors,
        checked_at=shared.checked_at,
        checks=checks,
        errors=errors,
        config_snapshot=_portable_config_snapshot(shared.config_snapshot),
    )


def _portable_config_snapshot(snapshot: Mapping[str, object]) -> dict[str, object]:
    root = Path(__file__).resolve().parents[2]
    value = dict(snapshot)
    value["credential_file"] = "<credential-file>"
    value["state_dir"] = "<run-state>"
    evidence = value.get("evidence_dir")
    if isinstance(evidence, str):
        try:
            relative = Path(evidence).resolve().relative_to(root)
        except ValueError:
            value["evidence_dir"] = "<evidence-dir>"
        else:
            value["evidence_dir"] = f"$SOURCE_ROOT/{relative.as_posix()}"
    return value


__all__ = ["run_controller_preflight"]
