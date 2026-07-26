"""Two-phase validation for hermetic benchmark artifact runs."""

from __future__ import annotations

import hashlib
import json
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from scripts.research.modes.base import Mode

_TOKEN_PATTERN = re.compile(rb"[0-9a-f]{64}\n\Z")


def _directory_inventory(path: Path) -> str | None:
    if not path.is_dir() or path.is_symlink():
        return None
    entries = sorted(entry.relative_to(path).as_posix() for entry in path.rglob("*"))
    encoded = json.dumps(entries, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


_EMPTY_DIRECTORY_INVENTORY = hashlib.sha256(b"[]").hexdigest()


@dataclass(frozen=True)
class PreflightRequest:
    run_id: str
    mode: str
    expected_agents: tuple[str, ...]
    resolved_config: Mapping[str, object]
    credential_file: Path


@dataclass(frozen=True)
class PreflightReport:
    valid: bool
    checked_at: str
    checks: tuple[Mapping[str, object], ...]
    errors: tuple[str, ...]
    config_snapshot: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "valid": self.valid,
            "checked_at": self.checked_at,
            "checks": [dict(check) for check in self.checks],
            "errors": list(self.errors),
            "config_snapshot": dict(self.config_snapshot),
        }

    def require_valid(self) -> None:
        if not self.valid:
            raise RuntimeError("benchmark preflight failed")


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _check(
    name: str,
    passed: bool,
    observed: object,
    expected: object,
) -> dict[str, object]:
    return {
        "name": name,
        "passed": passed,
        "observed": observed,
        "expected": expected,
    }


def _credential_check(path: Path) -> tuple[bool, str]:
    try:
        metadata = path.stat()
        contents = path.read_bytes()
    except OSError:
        return (False, "unreadable")
    if not stat.S_ISREG(metadata.st_mode):
        return (False, "not_regular")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        return (False, "invalid_permissions")
    if _TOKEN_PATTERN.fullmatch(contents) is None:
        return (False, "invalid_contents")
    return (True, "valid")


def _freshness_check(config: Mapping[str, object]) -> tuple[bool, object]:
    attestation = config.get("freshness_attestation")
    if not isinstance(attestation, Mapping):
        return (False, "missing")
    state_dir = attestation.get("state_dir")
    expected_inventory = attestation.get("inventory_sha256")
    if type(state_dir) is not str or type(expected_inventory) is not str:
        return (False, "invalid")
    observed_inventory = _directory_inventory(Path(state_dir))
    observed = {
        "inventory_sha256": observed_inventory,
        "state_dir": state_dir,
    }
    return (
        observed_inventory == expected_inventory == _EMPTY_DIRECTORY_INVENTORY,
        observed,
    )


async def run_prestart_preflight(request: PreflightRequest) -> PreflightReport:
    credential_valid, credential_observed = _credential_check(request.credential_file)
    mode_valid = request.mode in {mode.value for mode in Mode}
    agents_valid = bool(request.expected_agents) and all(request.expected_agents)
    freshness_valid, freshness_observed = _freshness_check(request.resolved_config)
    config_mode_valid = request.resolved_config.get("mode") == request.mode
    checks = (
        _check("credential", credential_valid, credential_observed, "0600 hex token"),
        _check(
            "mode",
            mode_valid,
            request.mode if mode_valid else "invalid",
            "declared mode",
        ),
        _check("agents", agents_valid, len(request.expected_agents), "nonempty"),
        _check(
            "freshness_attestation",
            freshness_valid,
            freshness_observed,
            {"inventory_sha256": _EMPTY_DIRECTORY_INVENTORY},
        ),
        _check(
            "resolved_config_mode",
            config_mode_valid,
            request.resolved_config.get("mode"),
            request.mode,
        ),
    )
    errors = tuple(
        f"{check['name']} failed" for check in checks if check["passed"] is False
    )
    return PreflightReport(
        valid=not errors,
        checked_at=_now_iso(),
        checks=checks,
        errors=errors,
        config_snapshot=dict(request.resolved_config),
    )


async def run_preflight(request: PreflightRequest) -> PreflightReport:
    credential_valid, credential_observed = _credential_check(request.credential_file)
    mode_valid = request.mode in {mode.value for mode in Mode}
    agents_valid = bool(request.expected_agents) and all(request.expected_agents)
    config_mode_valid = request.resolved_config.get("mode") == request.mode
    attestation = request.resolved_config.get("poststart_attestation")
    if isinstance(attestation, Mapping):
        authenticated = attestation.get("authenticated") is True
        ready_agents = attestation.get("ready_agents")
        ready_agents_valid = (
            isinstance(ready_agents, list)
            and all(type(agent) is str for agent in ready_agents)
            and sorted(ready_agents) == sorted(request.expected_agents)
        )
        topology = attestation.get("topology")
        topology_mode_valid = (
            isinstance(topology, Mapping) and topology.get("mode") == request.mode
        )
        network = attestation.get("network")
        if isinstance(network, Mapping):
            qdiscs = network.get("endpoint_qdiscs")
            network_valid = (
                network.get("profile") in {"lan", "50ms-rtt", "1pct-loss"}
                and network.get("probe_seconds") == 5
                and isinstance(qdiscs, Mapping)
                and set(qdiscs) == set(request.expected_agents)
                and all(type(value) is str for value in qdiscs.values())
            )
        else:
            network_valid = False
    else:
        authenticated = False
        ready_agents = "missing"
        ready_agents_valid = False
        topology = "missing"
        topology_mode_valid = False
        network = "missing"
        network_valid = False
    checks = (
        _check("credential", credential_valid, credential_observed, "0600 hex token"),
        _check(
            "mode",
            mode_valid,
            request.mode if mode_valid else "invalid",
            "declared mode",
        ),
        _check("agents", agents_valid, len(request.expected_agents), "nonempty"),
        _check(
            "resolved_config_mode",
            config_mode_valid,
            request.resolved_config.get("mode"),
            request.mode,
        ),
        _check("authentication", authenticated, authenticated, True),
        _check(
            "ready_agents",
            ready_agents_valid,
            ready_agents,
            sorted(request.expected_agents),
        ),
        _check("topology_mode", topology_mode_valid, topology, {"mode": request.mode}),
        _check(
            "network_profile",
            network_valid,
            network,
            "validated five-second endpoint profile",
        ),
    )
    errors = tuple(
        f"{check['name']} failed" for check in checks if check["passed"] is False
    )
    return PreflightReport(
        valid=not errors,
        checked_at=_now_iso(),
        checks=checks,
        errors=errors,
        config_snapshot=dict(request.resolved_config),
    )


__all__ = [
    "PreflightReport",
    "PreflightRequest",
    "run_preflight",
    "run_prestart_preflight",
]
