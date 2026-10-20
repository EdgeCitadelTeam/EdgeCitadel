"""The Core aggregator with an injected control-plane delay (experiment instrument).

Latency settings beyond what the testbed itself has are emulated by delaying
every control command the monitor sends to a node by ``HARNESS_CONTROL_DELAY_MS``
(JSON ``{"<node>": ms}``; ``"*"`` for every node) before it leaves the monitor.
The monitor is told the same latency in its configuration, as the simulator
assumes, so its deadlines account for it. With no delay configured this module
serves the unmodified application. Run it as ``uvicorn harness.aggregator_shim:app``.

Monitors from m3/integration ab174ed on send each command through a client's
``send(node_id, action, target, policy_version=...)`` (local sockets or NATS);
earlier ones call ``LocalAgentdControlClient._call(node_id, operation, params)``.
The delay wraps whichever the installed monitor has, before the command is
built, so a signed command's issue and expiry times start after the delay.
"""

from __future__ import annotations

import functools
import json
import os
import time
from typing import Any

from aggregator.containment_monitor import control

_DELAYS: dict[str, float] = json.loads(
    os.environ.get("HARNESS_CONTROL_DELAY_MS") or "{}"
)


def delay_for(node_id: str) -> float:
    return float(_DELAYS.get(node_id, _DELAYS.get("*", 0.0)))


def _delayed(method: Any) -> Any:
    @functools.wraps(method)
    def call(self: Any, node_id: str, *args: Any, **kwargs: Any) -> Any:
        delay = delay_for(node_id)
        if delay > 0:
            time.sleep(delay / 1000)  # enforcement runs in the monitor's thread pool
        return method(self, node_id, *args, **kwargs)

    return call


def install() -> list[str]:
    """Wrap the control clients' per-node entry point; returns what was wrapped."""
    wrapped = []
    for name in ("LocalAgentdControlClient", "NatsControlClient"):
        cls = getattr(control, name, None)
        if cls is None:
            continue
        attribute = "send" if "send" in vars(cls) else "_call"
        if attribute in vars(cls):
            setattr(cls, attribute, _delayed(vars(cls)[attribute]))
            wrapped.append(f"{name}.{attribute}")
    if not wrapped:
        raise RuntimeError("no monitor control client to delay")
    return wrapped


if any(_DELAYS.values()):
    install()

from aggregator.main import app  # noqa: E402  (built after the patch, by design)

__all__ = ["app"]
