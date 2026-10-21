"""Runtime configuration derived from a workload: agent grants, slots, monitor config.

The workload's declared flows are the single source: each agent's grant and
effect rules are its derived scope (``spec.derive``), and the monitor gets the
same authority mirror plus the run's timing model. Strategy and latency vary
per run; everything else is fixed per instance, so paired runs share it.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from ._worker import effects
from .names import STRATEGY_CODES
from .spec import RUNTIME_EFFECT_CLASS, Spec, derive, node_of_agent

# The monitor's strategy codes (display names in names.CODE_NAME).
STRATEGIES = STRATEGY_CODES
# The monitor's default control owner (MonitorConfig.owner); agentd fences
# holds by owner and authenticates NATS commands with this owner's key.
MONITOR_OWNER = "core-monitor"


def effect_rule(cls: str, sink: Mapping[str, Any] | None = None) -> dict[str, str]:
    """The grant rule of one workload effect class on the run's sink: the
    ledger's ``actuator://<class>/*``, or the Home Assistant service the
    class maps to (exactly that target)."""
    if sink is not None and sink.get("adapter") == "homeassistant":
        target = effects.runtime_target(sink, cls, "")
        return {"effect_class": RUNTIME_EFFECT_CLASS, "target": target}
    return {"effect_class": RUNTIME_EFFECT_CLASS, "target": f"actuator://{cls}/*"}


def nodes(spec: Spec) -> list[str]:
    return sorted({node_of_agent(spec, a.id) for a in spec.agents})


def agent_records(
    spec: Spec, sink: Mapping[str, Any] | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Per node: the Managed Agent registrations of the agents placed there."""
    derived = derive(spec)
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for a in spec.agents:
        out[node_of_agent(spec, a.id)].append(
            {
                "id": a.id,
                "recipients": list(derived.grants[a.id]),
                # Classes mapped to one Home Assistant target share its rule.
                "effects": list(
                    {
                        rule["target"]: rule
                        for rule in (
                            effect_rule(c, sink) for c in derived.effect_grant[a.id]
                        )
                    }.values()
                ),
            }
        )
    return dict(out)


def slots(spec: Spec, agent: str) -> int:
    """Concurrent invocations of one agent.

    Only a pending creation waits on other work (its task's pushed inputs),
    and each session has at most one waiting at a time; every other
    invocation finishes on its own. So sessions + 1 slots can never deadlock;
    one more spare (two more with undeclared invocations) keeps deliveries
    from queueing behind running turns. Each idle slot polls agentd, so more
    slots cost the node real load.
    """
    sessions = {t.session for t in spec.tasks if t.agent == agent}
    spare = 3 if spec.rogue.rate > 0 else 2
    return len(sessions) + spare


def monitor_config(
    spec: Spec,
    *,
    strategy: str,
    control_latency_ms: Mapping[str, float],
    tau_ms: float = 50.0,
    enforce: bool = True,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The monitor's ``MonitorConfig`` JSON for one run.

    Timing values are lower bounds (the monitor's deadlines must stay sound):
    control latency is the run's latency setting per node; delivery, start and
    emission bounds stay 0 unless overridden with measured minimums. The run's
    mechanism profile, ``phase1_mode`` and ``receding_during_waits`` come in
    ``overrides``.
    """
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy '{strategy}'")
    derived = derive(spec)
    config: dict[str, Any] = {
        "grants": {a.id: list(derived.grants[a.id]) for a in spec.agents},
        "effect_scopes": {
            a.id: [RUNTIME_EFFECT_CLASS]
            for a in spec.agents
            if derived.effect_grant[a.id]
        },
        "protected_effect_classes": [RUNTIME_EFFECT_CLASS],
        "agent_nodes": {a.id: node_of_agent(spec, a.id) for a in spec.agents},
        "org_parent": {a.id: a.parent for a in spec.agents if a.parent is not None},
        "control_latency_ms": {
            node: float(ms) for node, ms in control_latency_ms.items()
        },
        "tau_ms": float(tau_ms),
        "strategy": strategy,
        "enforce": enforce,
    }
    if control_latency_ms:
        config["default_control_latency_ms"] = float(min(control_latency_ms.values()))
    config.update(overrides or {})
    return config
