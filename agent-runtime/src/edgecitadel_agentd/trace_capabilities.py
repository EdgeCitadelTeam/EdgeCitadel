"""Optional telemetry negotiation; never a task-delivery or authority decision."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .trace_contract import TraceContractError, validate_trace_capability

TELEMETRY_EXTENSION_URI = "https://edgecitadel.dev/extensions/execution-trace"
TRACE_FAMILIES = frozenset(
    {
        "run",
        "task",
        "dispatch",
        "permission",
        "model",
        "tool",
        "coverage",
        "source",
        "link",
        "security",
    }
)


@dataclass(frozen=True)
class TelemetryCapability:
    schema_version: int | None
    status: str
    live_families: frozenset[str] = frozenset()
    historical_families: frozenset[str] = frozenset()
    unsupported_families: frozenset[str] = frozenset()


def negotiate_telemetry(card: dict[str, Any]) -> TelemetryCapability:
    """Parse only the optional advertisement, after normal card validation.

    Unknown coverage is not the same as an explicit unsupported declaration.
    A declaration advertises ability; it is not evidence a particular run was
    observed, and does not grant permission to append/import events.
    """
    capabilities = card.get("capabilities", {})
    if type(capabilities) is not dict:
        return TelemetryCapability(None, "invalid_advertisement")
    extensions = capabilities.get("extensions", [])
    if type(extensions) is not list or len(extensions) > 64:
        return TelemetryCapability(None, "invalid_advertisement")
    matches = [
        item
        for item in extensions
        if isinstance(item, dict) and item.get("uri") == TELEMETRY_EXTENSION_URI
    ]
    if not matches:
        return TelemetryCapability(None, "not_advertised")
    if len(matches) != 1:
        return TelemetryCapability(None, "invalid_advertisement")
    extension = matches[0]
    try:
        validate_trace_capability(extension)
    except TraceContractError:
        return TelemetryCapability(None, "invalid_advertisement")
    params = extension["params"]
    versions, families = params["schema_versions"], params["families"]
    if 1 not in versions:
        return TelemetryCapability(None, "unsupported_version")
    return TelemetryCapability(
        1,
        "negotiated",
        frozenset(name for name, mode in families.items() if mode == "live"),
        frozenset(name for name, mode in families.items() if mode == "historical"),
        TRACE_FAMILIES
        - frozenset(name for name, mode in families.items() if mode != "unsupported"),
    )
