"""Monitor configuration: authority fallback, timing model and cost weights.

agentd exports each invocation's effective authority (recipients and effect
rules) on ``run/started``, and the monitor uses it. ``grants`` and
``effect_scopes`` are only the fallback where no export exists (a task that has
not started, a new invocation beyond the horizon, or an export of null because
it exceeded 8 KiB): the monitor then mirrors agentd's rule, the configured grant
intersected with the dispatcher's scope. An agent without a configured grant is
treated as able to reach every known agent and to perform every protected effect
class, which over-approximates G_C soundly. ``protected_effect_classes`` names
the effect classes whose commits are sinks.

The timing model gives lower bounds only. Deadlines are earliest possible times,
so over-estimating a latency or a minimum delay would make G_C unsound.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .model import AGENT_PATTERN, NODE_PATTERN, STRATEGY_NAMES

LINK_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}>[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
)


class ConfigError(ValueError):
    pass


def _agents(value: object, what: str) -> frozenset[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and AGENT_PATTERN.fullmatch(item) for item in value
    ):
        raise ConfigError(f"invalid_{what}")
    return frozenset(value)


def _names(value: object, what: str) -> frozenset[str]:
    if not isinstance(value, list) or not all(
        isinstance(item, str) and NODE_PATTERN.fullmatch(item) for item in value
    ):
        raise ConfigError(f"invalid_{what}")
    return frozenset(value)


def _number_map(value: object, what: str, key_pattern=NODE_PATTERN) -> dict[str, float]:
    if not isinstance(value, dict):
        raise ConfigError(f"invalid_{what}")
    out = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key_pattern.fullmatch(key):
            raise ConfigError(f"invalid_{what}")
        if isinstance(item, bool) or not isinstance(item, (int, float)) or item < 0:
            raise ConfigError(f"invalid_{what}")
        out[key] = float(item)
    return out


@dataclass(frozen=True)
class MonitorConfig:
    owner: str = "core-monitor"
    # Authority mirror.
    grants: Mapping[str, frozenset[str]] = field(default_factory=dict)
    effect_scopes: Mapping[str, frozenset[str]] = field(default_factory=dict)
    protected_effect_classes: frozenset[str] = frozenset()
    agent_nodes: Mapping[str, str] = field(default_factory=dict)
    org_parent: Mapping[str, str] = field(default_factory=dict)
    untrusted_agents: frozenset[str] = frozenset()
    external_requests_untrusted: bool = False
    # Agents whose invocations keep no conversation state across tasks.
    session_isolated_agents: frozenset[str] = frozenset()
    # Timing model (milliseconds, lower bounds).
    tau_ms: float = 50.0
    control_latency_ms: Mapping[str, float] = field(default_factory=dict)
    default_control_latency_ms: float = 50.0
    delivery_ms: Mapping[str, float] = field(default_factory=dict)  # "<src>><dst>"
    default_delivery_ms: float = 0.0
    min_start_ms: float = 0.0
    min_emission_ms: Mapping[str, float] = field(default_factory=dict)
    default_min_emission_ms: float = 0.0
    # Cost model.
    value_weights: Mapping[str, float] = field(default_factory=dict)
    c0: float = 0.01
    w_tainted: float = 0.0
    revoke_weight: float = 0.5
    # Root cause and strategy.
    root_cause_window_ms: int = 600_000
    top_k: int = 5
    untrusted_prior: float = 0.5
    type_prior: float = 0.25
    strategy: str = "firebreak"
    # Stop team: how many org levels above the root's agent its team starts.
    stop_team_level: int = 0
    enum_limit: int = 12
    max_rounds: int = 3
    # G_C horizon: new invocations predicted per level; 1 is the main pipeline.
    horizon: int = 1
    # "sink": a new invocation at the horizon is a sink; "defer": a dead end that
    # the receding re-solve handles once it materializes.
    boundary: str = "sink"
    # After each cut, block new dispatches (egress) of every source-side invocation.
    taint_aware_admission: bool = False
    # Run every strategy through the two-phase protocol: phase-1 freeze first,
    # its holds released only after the strategy's actions are confirmed,
    # fenced and still a cut. False applies each strategy's actions directly.
    # Either way it is the same for all strategies.
    phase1_freeze: bool = True
    # Also issue c0-cost actions on the last hop into sinks judged unavoidable
    # (deadlines are lower bounds, so some may still land in time). On by team
    # decision (2026-10-20).
    best_effort_guard: bool = True
    # Renew agentd's fail-closed controller lease (EDGECITADEL_CONTROLLER_LEASE_MS
    # on the nodes) this often, on every known node that describes
    # renew:controller; 0 disables. Keep it well under the lease and the command
    # lifetime: a renewal never outlives the command that carried it.
    controller_lease_renew_ms: int = 10_000
    # How long a decision waits for Core to ingest each confirming node's events
    # up to the journal position it confirmed before it gives up releasing.
    ingest_timeout_ms: int = 5_000

    # -- lookups ------------------------------------------------------------
    def control_latency(self, node_id: str) -> float:
        return self.control_latency_ms.get(node_id, self.default_control_latency_ms)

    def delivery(self, source: str | None, target: str | None) -> float:
        if source is not None and source == target:
            return 0.0
        if source is None or target is None:
            return min([self.default_delivery_ms, *self.delivery_ms.values()])
        return self.delivery_ms.get(f"{source}>{target}", self.default_delivery_ms)

    def min_emission(self, agent_id: str) -> float:
        return self.min_emission_ms.get(agent_id, self.default_min_emission_ms)

    def value(self, agent_id: str) -> float:
        return self.value_weights.get(agent_id, 1.0)

    # -- loading ------------------------------------------------------------
    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> MonitorConfig:
        if not isinstance(value, Mapping):
            raise ConfigError("invalid_config")
        known = {f for f in cls.__dataclass_fields__}
        unknown = set(value) - known
        if unknown:
            raise ConfigError("unknown_config_field")
        kwargs: dict[str, Any] = {}
        for key, item in value.items():
            if key == "owner":
                if not isinstance(item, str) or not NODE_PATTERN.fullmatch(item):
                    raise ConfigError("invalid_owner")
                kwargs[key] = item
            elif key in {"grants", "effect_scopes"}:
                if not isinstance(item, dict):
                    raise ConfigError(f"invalid_{key}")
                parse = _agents if key == "grants" else _names
                kwargs[key] = {
                    agent: parse(targets, key) for agent, targets in item.items()
                }
                if not all(AGENT_PATTERN.fullmatch(agent) for agent in item):
                    raise ConfigError(f"invalid_{key}")
            elif key in {"agent_nodes", "org_parent"}:
                if not isinstance(item, dict) or not all(
                    isinstance(k, str)
                    and isinstance(v, str)
                    and NODE_PATTERN.fullmatch(v)
                    for k, v in item.items()
                ):
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = dict(item)
            elif key in {"untrusted_agents", "session_isolated_agents"}:
                kwargs[key] = _agents(item, key)
            elif key == "protected_effect_classes":
                kwargs[key] = _names(item, key)
            elif key in {
                "control_latency_ms",
                "min_emission_ms",
                "value_weights",
            }:
                kwargs[key] = _number_map(item, key)
            elif key == "delivery_ms":
                kwargs[key] = _number_map(item, key, LINK_PATTERN)
            elif key == "strategy":
                if item not in STRATEGY_NAMES:
                    raise ConfigError("invalid_strategy")
                kwargs[key] = item
            elif key in {
                "external_requests_untrusted",
                "taint_aware_admission",
                "best_effort_guard",
                "phase1_freeze",
            }:
                if not isinstance(item, bool):
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = item
            elif key in {"stop_team_level", "controller_lease_renew_ms"}:
                if type(item) is not int or item < 0:
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = item
            elif key == "boundary":
                if item not in {"sink", "defer"}:
                    raise ConfigError("invalid_boundary")
                kwargs[key] = item
            elif key in {
                "root_cause_window_ms",
                "top_k",
                "enum_limit",
                "max_rounds",
                "horizon",
                "ingest_timeout_ms",
            }:
                if type(item) is not int or item < 1:
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = item
            else:
                if (
                    isinstance(item, bool)
                    or not isinstance(item, (int, float))
                    or item < 0
                ):
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = float(item)
        return cls(**kwargs)

    @classmethod
    def load(cls, path: Path) -> MonitorConfig:
        try:
            document = json.loads(path.read_text())
        except (OSError, ValueError) as error:
            raise ConfigError("containment_config_unreadable") from error
        return cls.from_json(document)
