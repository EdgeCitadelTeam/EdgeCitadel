"""How a det-worker carries out one effect: agentd's effect gateway performs it.

The worker asks the gateway to execute the effect (``effect.execute``). The
gateway re-checks holds, revocations, the effect scope and the leases under
its control lock, commits, and performs the sink call through a configured
adapter, so nothing reaches the sink past a hold or revocation even if the
worker misbehaves. An effect due later is scheduled in the gateway, which
performs it when due unless its re-check then refuses it (an effect hold
cancels it there).

The sink (``plan["effect_sink"]``) is the experiment's actuator:

- ``http``: the gateway's ledger adapter posts each committed effect to the
  run's services, which record it as ``committed``;
- ``homeassistant``: the gateway calls a Home Assistant service for every
  effect class mapped to a service target ``<domain>.<service>/<entity_id>``
  (``targets``, ``*`` for every class), and the run's services sit in front of
  Home Assistant and record each call that reached it as ``committed``; an
  effect class it does not map goes to the run's ledger (``http``).

The runtime effect class is the workload class's (``runtime_class`` in the
plan: ``device.command`` for a protected class, ``network.http`` for an
unprotected one, such as a reply to a user). The worker writes the other
ledger phases: ``attempted`` before the request, then ``scheduled`` (the
gateway keeps it until due), ``held`` (its invocation carries an admission
label: the gateway holds it until the label's owner admits it), ``denied``
(refused, with agentd's code) or ``sink_failed`` (committed but the adapter
could not perform it). ``committed`` comes only from the sink. The worker passes its identity of
the effect as ``data`` (``ec_run``, ``ec_effect``, ``ec_issuer``) so the sink
can attribute the call; the Home Assistant front strips those keys.

This module does not import agentd, so the harness can validate a setting
without the runtime installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

RUNTIME_EFFECT_CLASS = "device.command"
# An effect due sooner than this is executed at once instead of scheduled.
IMMEDIATE_EFFECT_MS = 20
ADAPTERS = ("http", "homeassistant")


class EffectPort(Protocol):
    """What the effect path needs from the worker."""

    async def call(
        self, context: Any, operation: str, **params: Any
    ) -> dict[str, Any]: ...

    async def ledger(self, record: Mapping[str, Any]) -> None: ...

    def now_ms(self) -> int: ...


def check_sink(sink: Mapping[str, Any]) -> dict[str, Any]:
    """The plan's ``effect_sink``: ``{"adapter": "http"}`` or
    ``{"adapter": "homeassistant", "targets": {"<class>" | "*": "<target>"}}``."""
    adapter = sink.get("adapter")
    if adapter not in ADAPTERS:
        raise ValueError(f"effect sink adapter must be one of {ADAPTERS}")
    if adapter == "homeassistant":
        targets = sink.get("targets")
        if not isinstance(targets, Mapping) or not targets:
            raise ValueError("a homeassistant sink needs targets per effect class")
        return {"adapter": adapter, "targets": dict(targets)}
    return {"adapter": adapter}


def adapter_for(sink: Mapping[str, Any], cls: str) -> str:
    """The gateway adapter that performs an effect of workload class ``cls``."""
    if sink["adapter"] == "homeassistant" and (
        cls in sink["targets"] or "*" in sink["targets"]
    ):
        return "homeassistant"
    return "http"


def runtime_target(sink: Mapping[str, Any], cls: str, target: str) -> str:
    """What the gateway is asked to act on for an effect of class ``cls``."""
    if adapter_for(sink, cls) == "homeassistant":
        targets = sink["targets"]
        return str(targets.get(cls, targets.get("*", "")))
    return target


def sink_data(record: Mapping[str, Any]) -> dict[str, str]:
    """The worker's identity of the effect, carried to the sink."""
    return {
        "ec_run": str(record["run"]),
        "ec_effect": str(record["effect_id"]),
        "ec_issuer": str(record["issuer"]),
    }


class GatewayExecute:
    """One effect through ``effect.execute``; ``op.params`` keeps its progress.

    ``classes`` is the plan's ``effect_classes`` (each with its runtime class).
    """

    def __init__(
        self, sink: Mapping[str, Any], classes: Mapping[str, Mapping[str, Any]]
    ) -> None:
        self.sink = check_sink(sink)
        self.classes = classes

    def runtime_class(self, cls: str) -> str:
        known = self.classes.get(cls)
        return str(known["runtime_class"]) if known else RUNTIME_EFFECT_CLASS

    async def run(
        self,
        port: EffectPort,
        context: Any,
        op: Any,
        record: Mapping[str, Any],
        *,
        request_id: str,
    ) -> None:
        p = op.params
        if "scheduled" not in p:
            await port.ledger({**record, "phase": "attempted"})
            # Decided once: a resumed invocation repeats the identical request.
            p["scheduled"] = p["due_ms"] - port.now_ms() > IMMEDIATE_EFFECT_MS
        scheduled = p["scheduled"]
        # A repeated request id (a resumed invocation, a retried transport
        # failure) returns the gateway's stored receipt: it never acts twice.
        reply = await port.call(
            context,
            "effect.execute",
            schema_version=1,
            request_id=request_id,
            binding_id=context.trace.binding_id,
            adapter=adapter_for(self.sink, record["class"]),
            effect_class=self.runtime_class(record["class"]),
            target=runtime_target(self.sink, record["class"], p["target"]),
            data=sink_data(record),
            tool_call_id=None,
            scheduled_at_ms=int(p["due_ms"]) if scheduled else None,
        )
        if reply.get("status") != "ok":
            op.state = "denied"
            await port.ledger({**record, "phase": "denied", "code": reply.get("code")})
            return
        result = reply["result"]
        op.state = "done"
        if result.get("state") in {"scheduled", "held"}:
            await port.ledger({**record, "phase": result["state"]})
            return
        outcome = result.get("outcome") or {}
        if not outcome.get("performed"):
            await port.ledger(
                {**record, "phase": "sink_failed", "code": outcome.get("code")}
            )
