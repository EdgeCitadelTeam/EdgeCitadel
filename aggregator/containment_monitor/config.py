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

Mechanisms around a strategy's own rule are set per strategy (``Mechanisms``):
the two-phase protocol (``phase1_freeze``), the best-effort guard and
taint-aware admission. ``profile`` gives every strategy its defaults and
``mechanisms`` overrides them per strategy (``"*"`` for all):

- ``deployed`` (default): each strategy as it would be deployed. The rule-based
  baselines apply their rule directly (no freeze, no guard, no taint-aware
  admission; nothing lineage-based is added to the no-lineage ones); the three
  cut strategies run the two-phase protocol with the adaptive phase 1, the
  guard and taint-aware admission, which Theorem 5 needs.
- ``shared``: every mechanism on for every strategy with the frontier-freeze
  phase 1, an ablation in which strategies differ only in their phase-2 rule.

Taint-aware admission places labels: reversible egress holds under which agentd
holds each new event of the unit and the monitor admits it per event
(``admission``).

Confirmation, fencing and agentd's gates apply to every strategy either way.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

from .model import AGENT_PATTERN, NODE_PATTERN, STRATEGY_NAMES

LINK_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}>[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$"
)


class ConfigError(ValueError):
    pass


PROFILES = ("deployed", "shared")
PHASE1_MODES = ("freeze", "freeze_plus_cut", "adaptive")
PHASE1_LABELS = ("frontier", "forward_closure")
# The phase-1 mode of each profile when ``phase1_mode`` is not set.
PROFILE_PHASE1_MODES = {"deployed": "adaptive", "shared": "freeze"}
# The strategies that solve a cut on G_C (the others apply a rule).
CUT_STRATEGIES = frozenset({"greedy_cut", "cut_without_deadlines", "firebreak"})


@dataclass(frozen=True)
class Mechanisms:
    """What runs around one strategy's own rule."""

    # The two-phase protocol: phase-1 freeze (frontier holds and admission
    # freeze) first, released only after the strategy's actions are confirmed,
    # fenced and still a cut. False applies the strategy's actions directly.
    phase1_freeze: bool
    # Also issue c0-cost actions on the last hop into sinks judged unavoidable
    # (deadlines are lower bounds, so some may still land in time).
    best_effort_guard: bool
    # After each cut, block new dispatches and effects (egress) of every
    # invocation still on the source side: the label set of theory A.6.
    taint_aware_admission: bool

    def to_json(self) -> dict[str, bool]:
        return asdict(self)


def profile_mechanisms(profile: str, strategy: str) -> Mechanisms:
    if profile == "shared":
        return Mechanisms(True, True, True)
    cut = strategy in CUT_STRATEGIES
    return Mechanisms(cut, cut, cut)


def _mechanism_overrides(value: object) -> dict[str, dict[str, bool]]:
    fields = set(Mechanisms.__dataclass_fields__)
    if not isinstance(value, dict):
        raise ConfigError("invalid_mechanisms")
    out = {}
    for strategy, flags in value.items():
        if (
            strategy not in {*STRATEGY_NAMES, "*"}
            or not isinstance(flags, dict)
            or set(flags) - fields
            or not all(isinstance(flag, bool) for flag in flags.values())
        ):
            raise ConfigError("invalid_mechanisms")
        out[strategy] = dict(flags)
    return out


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
    # Agents whose memory is shared by all their invocations, whatever the
    # conversation (an inbox of pushed inputs, a shared store): each invocation
    # continues the previous one of the agent, across contexts.
    shared_memory_agents: frozenset[str] = frozenset()
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
    # Service agents: the expected number of future unprotected requests per
    # agent (e.g. replies to users), each worth future_request_value, which an
    # action that stops the agent's service forfeits (costs).
    future_requests: Mapping[str, float] = field(default_factory=dict)
    future_request_value: float = 1.0
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
    # Mechanisms per strategy (see the module docstring): a profile, then
    # overrides {"<strategy>" | "*": {"<mechanism>": bool}}.
    profile: str = "deployed"
    mechanisms: Mapping[str, Mapping[str, bool]] = field(default_factory=dict)
    # What phase 1 issues (enforcement.TwoPhaseProtocol) for a strategy that
    # runs the two-phase protocol: "adaptive" (B_a, the strategy's own cut
    # solved at the alert, with the taint-aware labels of its source side; the
    # freeze only when B_a leaves a protected sink untimely, when the earliest
    # tainted-effect deadline is within phase1_margin_rounds control rounds of
    # B_a's landing, or when B_a in force is not a cut once reconciled),
    # "freeze" (the frontier freeze B_1) or "freeze_plus_cut" (B_1 and B_a at
    # once). None takes the profile's (``phase1_mode_for``): adaptive for the
    # cut strategies of the deployed profile, freeze in the shared ablation,
    # whose strategies share one phase 1. Phase 2 and the guarded release are
    # the same in every mode.
    phase1_mode: str | None = None
    phase1_margin_rounds: int = 2
    # Phase-1 labels (admission holds): "frontier" covers U (freeze modes) or
    # S(B_a) (adaptive); "forward_closure" covers the sources' forward closure.
    phase1_labels: str = "frontier"
    # Let the receding trigger (a taint advance) add actions while the
    # protocol waits for ingest; additions only, never releases.
    receding_during_waits: bool = False
    # Renew agentd's fail-closed controller lease (EDGECITADEL_CONTROLLER_LEASE_MS
    # on the nodes) this often, on every known node that describes
    # renew:controller; 0 disables. Keep it well under the lease and the command
    # lifetime: a renewal never outlives the command that carried it.
    controller_lease_renew_ms: int = 10_000
    # False: decide and log every response but send no decision (actions are
    # recorded failed, not_enforced); describe and lease renewals still go out.
    enforce: bool = True
    # How long a decision waits for Core to ingest each confirming node's events
    # up to the journal position it confirmed before it gives up releasing.
    ingest_timeout_ms: int = 5_000

    # -- lookups ------------------------------------------------------------
    def mechanisms_for(self, strategy: str) -> Mechanisms:
        defaults = profile_mechanisms(self.profile, strategy)
        overrides = {
            **self.mechanisms.get("*", {}),
            **self.mechanisms.get(strategy, {}),
        }
        return replace(defaults, **overrides)

    def phase1_mode_for(self, strategy: str) -> str | None:
        """The strategy's phase-1 mode; None when it has no phase 1 (it
        applies its rule directly)."""
        if not self.mechanisms_for(strategy).phase1_freeze:
            return None
        if self.phase1_mode is not None:
            return self.phase1_mode
        return PROFILE_PHASE1_MODES[self.profile]

    def session_of(self, agent_id: str, context_id: str | None) -> str | None:
        """The memory an invocation continues: its conversation, ``*`` (all of
        a shared-memory agent's invocations), or None (no carried state)."""
        if agent_id in self.session_isolated_agents:
            return None
        if agent_id in self.shared_memory_agents:
            return "*"
        return context_id

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
            elif key in {
                "untrusted_agents",
                "session_isolated_agents",
                "shared_memory_agents",
            }:
                kwargs[key] = _agents(item, key)
            elif key == "protected_effect_classes":
                kwargs[key] = _names(item, key)
            elif key in {
                "control_latency_ms",
                "min_emission_ms",
                "value_weights",
            }:
                kwargs[key] = _number_map(item, key)
            elif key == "future_requests":
                kwargs[key] = _number_map(item, key, AGENT_PATTERN)
            elif key == "delivery_ms":
                kwargs[key] = _number_map(item, key, LINK_PATTERN)
            elif key == "strategy":
                if item not in STRATEGY_NAMES:
                    raise ConfigError("invalid_strategy")
                kwargs[key] = item
            elif key == "profile":
                if item not in PROFILES:
                    raise ConfigError("invalid_profile")
                kwargs[key] = item
            elif key == "phase1_mode":
                if item not in PHASE1_MODES:
                    raise ConfigError("invalid_phase1_mode")
                kwargs[key] = item
            elif key == "phase1_labels":
                if item not in PHASE1_LABELS:
                    raise ConfigError("invalid_phase1_labels")
                kwargs[key] = item
            elif key == "mechanisms":
                kwargs[key] = _mechanism_overrides(item)
            elif key in {
                "external_requests_untrusted",
                "receding_during_waits",
                "enforce",
            }:
                if not isinstance(item, bool):
                    raise ConfigError(f"invalid_{key}")
                kwargs[key] = item
            elif key in {
                "stop_team_level",
                "controller_lease_renew_ms",
                "phase1_margin_rounds",
            }:
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
