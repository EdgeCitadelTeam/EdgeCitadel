"""How a det-worker carries out one effect: a pluggable path to the sink.

The sink is the harness's effect ledger (in the lab possibly a Home Assistant
service or another HTTP endpoint). Every path writes the same ledger phases,
which are the experiment's ground truth: ``attempted`` (the worker decided to
act), then ``committed`` (the sink was reached), ``held`` (agentd held it; the
path retries the same request until released) or ``denied`` (refused, with
agentd's code).

- ``authorize_perform`` (today): agentd's effect gateway authorizes the effect,
  immediately or scheduled for its due time; a scheduled one is committed when
  due (``effect_held`` while a hold covers it); then the worker performs the
  sink call itself.
- ``gateway_execute`` (m4/design): the worker asks agentd's gateway to execute
  the effect and the gateway performs the sink call, so nothing reaches the
  sink past a hold or revocation even if the worker misbehaves. Not available
  until ``effect.execute`` lands; selecting it is refused up front
  (``available``) rather than producing runs whose effects all fail.

This module does not import agentd, so the harness can validate a setting
without the runtime installed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, Protocol

RUNTIME_EFFECT_CLASS = "device.command"
HELD_RETRY_SECONDS = 0.1
# A scheduled effect due sooner than this is authorized as an immediate one.
IMMEDIATE_EFFECT_MS = 20


class EffectPort(Protocol):
    """What a path needs from the worker."""

    async def call(
        self, context: Any, operation: str, **params: Any
    ) -> dict[str, Any]: ...

    async def ledger(self, record: Mapping[str, Any]) -> None: ...

    async def sleep_until(self, t_ms: float) -> None: ...

    def now_ms(self) -> int: ...


class EffectPath:
    name = ""
    available = True

    async def run(
        self,
        port: EffectPort,
        context: Any,
        op: Any,
        record: Mapping[str, Any],
        *,
        request_id: str,
        commit_request_id: str,
    ) -> None:
        """Carry out ``op`` (an effect op; ``op.params`` keeps the path's progress
        across a pause, ``op.state`` ends ``done`` or ``denied``)."""
        raise NotImplementedError


class AuthorizeThenPerform(EffectPath):
    name = "authorize_perform"

    async def run(self, port, context, op, record, *, request_id, commit_request_id):
        p = op.params
        if "requested" not in p:
            await port.ledger({**record, "phase": "attempted"})
            scheduled = p["due_ms"] - port.now_ms() > IMMEDIATE_EFFECT_MS
            reply = await port.call(
                context,
                "effect.authorize",
                schema_version=1,
                request_id=request_id,
                binding_id=context.trace.binding_id,
                effect_class=RUNTIME_EFFECT_CLASS,
                target=p["target"],
                tool_call_id=None,
                scheduled_at_ms=int(p["due_ms"]) if scheduled else None,
            )
            if reply.get("status") != "ok":
                op.state = "denied"
                await port.ledger(
                    {**record, "phase": "denied", "code": reply.get("code")}
                )
                return
            p["requested"] = reply["result"]["state"]
        if p["requested"] == "committed":
            await self.perform(port, record)
            op.state = "done"
            return
        await port.sleep_until(p["due_ms"])
        held = False
        while True:
            reply = await port.call(
                context,
                "effect.commit",
                schema_version=1,
                request_id=commit_request_id,
                binding_id=context.trace.binding_id,
                effect_id=request_id,
            )
            if reply.get("status") == "ok":
                await self.perform(port, record)
                op.state = "done"
                return
            if reply.get("code") != "effect_held":
                op.state = "denied"
                await port.ledger(
                    {**record, "phase": "denied", "code": reply.get("code")}
                )
                return
            if not held:
                held = True
                await port.ledger({**record, "phase": "held", "code": "effect_held"})
            await asyncio.sleep(HELD_RETRY_SECONDS)

    async def perform(self, port: EffectPort, record: Mapping[str, Any]) -> None:
        """The sink call: the ledger records the effect as having happened."""
        await port.ledger({**record, "phase": "committed"})


class GatewayExecute(EffectPath):
    """agentd's gateway performs the sink call (``effect.execute``, m4/design).

    To implement when the contract lands: record ``attempted``, send
    ``effect.execute`` with the request id, binding, class, target, due time
    and the sink request (``sink_request``), retry the same request id while
    ``effect_held``, and record ``denied`` with agentd's code otherwise. The
    sink, not the worker, then records ``committed``.
    """

    name = "gateway_execute"
    available = False

    async def run(self, port, context, op, record, *, request_id, commit_request_id):
        raise NotImplementedError("effect.execute is not available before m4/design")


def sink_request(sink_url: str, record: Mapping[str, Any]) -> dict[str, Any]:
    """The HTTP call a sink receives for one effect, on either path."""
    return {"method": "POST", "url": sink_url, "json": {**record, "phase": "committed"}}


EFFECT_PATHS: dict[str, type[EffectPath]] = {
    AuthorizeThenPerform.name: AuthorizeThenPerform,
    GatewayExecute.name: GatewayExecute,
}


def make_effect_path(name: str) -> EffectPath:
    cls = EFFECT_PATHS.get(name)
    if cls is None:
        raise ValueError(f"unknown effect path '{name}'")
    if not cls.available:
        raise ValueError(f"effect path '{name}' is not available in this runtime yet")
    return cls()
