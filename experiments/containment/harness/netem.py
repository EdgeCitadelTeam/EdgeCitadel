"""Lab latency profiles as ``tc netem`` commands, scoped to experiment traffic.

Profiles live in the stack file under ``netem``::

  "netem": {
    "apply": false,
    "interface": {"jim-eq": "tailscale0", ...},
    "ports": [14222, 18900],
    "profiles": {"stress-wan-50": {"delay_ms": 50, "jitter_ms": 10, "distribution": "normal"}}
  }

A profile delays only packets to or from the experiment's ports (a ``prio``
qdisc with a ``u32`` filter per port), so the hosts' resident EdgeCitadel
services and SSH are not slowed. ``delay_ms: null`` marks a placeholder for a
measured distribution; such a profile refuses to render. With ``apply`` true
the runner installs a cell's profile on every host before the run and removes
it after (root on each host); otherwise ``python -m harness netem`` prints the
commands for the operator.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

HANDLE = "1:"
BAND = "1:3"
PRIOMAP = " ".join(["1"] * 16)
# Upper end of the measured LAN round trip between the testbed's sites
# (1.5-4 ms, jim-eq / NetSys-Endurance / NetSys-Cluster). Every host delays
# its own egress, so a profile adds twice its delay to a round trip; one that
# adds more than this is a stress profile and must be named as one.
MEASURED_RTT_MS = 4.0


def added_rtt_ms(netem: Mapping[str, Any], profile: str | None) -> float:
    """The round trip a profile adds (0 for none or a placeholder)."""
    spec = netem.get("profiles", {}).get(profile) if profile else None
    if not spec or spec.get("delay_ms") is None:
        return 0.0
    return 2 * float(spec["delay_ms"])


def check_profiles(
    netem: Mapping[str, Any], measured_rtt_ms: float = MEASURED_RTT_MS
) -> None:
    for name in netem.get("profiles", {}):
        if added_rtt_ms(netem, name) > measured_rtt_ms and "stress" not in name.lower():
            raise ValueError(
                f"netem profile '{name}' adds more than the measured LAN round trip "
                f"({measured_rtt_ms:g} ms): label it as a stress profile"
            )


def load_profiles(stack_file: str | Path) -> dict[str, Any]:
    document = json.loads(Path(stack_file).read_text())
    return dict(document.get("netem", {}))


def commands(
    netem: Mapping[str, Any], profile: str, *, clear: bool = False
) -> dict[str, list[str]]:
    """Per host, the ``tc`` commands that install (or remove) a profile."""
    interfaces: Mapping[str, str] = netem.get("interface", {})
    if clear:
        return {
            host: [f"tc qdisc del dev {dev} root 2>/dev/null || true"]
            for host, dev in interfaces.items()
        }
    spec = netem.get("profiles", {}).get(profile)
    if spec is None:
        raise ValueError(f"unknown netem profile '{profile}'")
    if spec.get("delay_ms") is None:
        raise ValueError(
            f"netem profile '{profile}' is a placeholder: fill in its measured delay"
        )
    delay = f"delay {float(spec['delay_ms']):g}ms"
    if spec.get("jitter_ms"):
        delay += f" {float(spec['jitter_ms']):g}ms"
        if spec.get("distribution"):
            delay += f" distribution {spec['distribution']}"
    extra = ""
    if spec.get("loss_pct"):
        extra += f" loss {float(spec['loss_pct']):g}%"
    ports = [int(p) for p in netem.get("ports", [])]
    if not ports:
        raise ValueError("netem.ports must name the experiment's ports")
    out: dict[str, list[str]] = {}
    for host, dev in interfaces.items():
        lines = [
            f"tc qdisc del dev {dev} root 2>/dev/null || true",
            # Every priority maps to band 2, so only the filtered ports reach band 3.
            f"tc qdisc add dev {dev} root handle {HANDLE} prio bands 3 priomap {PRIOMAP}",
            f"tc qdisc add dev {dev} parent {BAND} handle 30: netem {delay}{extra}",
        ]
        for port in ports:
            for direction in ("dport", "sport"):
                lines.append(
                    f"tc filter add dev {dev} protocol ip parent {HANDLE}0 prio 3 u32 "
                    f"match ip {direction} {port} 0xffff flowid {BAND}"
                )
        out[host] = lines
    return out
