"""The Core aggregator with an injected control-plane delay (experiment instrument).

Latency settings beyond what the testbed itself has are emulated by delaying
every control call the monitor sends to a node by ``HARNESS_CONTROL_DELAY_MS``
(JSON ``{"<node>": ms}``; ``"*"`` for every node) before it reaches agentd.
The monitor is told the same latency in its configuration, as the simulator
assumes, so its deadlines account for it. With no delay configured this module
serves the unmodified application. Run it as ``uvicorn harness.aggregator_shim:app``.
"""

from __future__ import annotations

import json
import os
import time

from aggregator.containment_monitor import control

_DELAYS: dict[str, float] = json.loads(
    os.environ.get("HARNESS_CONTROL_DELAY_MS") or "{}"
)

if any(_DELAYS.values()):
    _call = control.LocalAgentdControlClient._call

    def _delayed_call(self, node_id, operation, params):  # type: ignore[no-untyped-def]
        delay = _DELAYS.get(node_id, _DELAYS.get("*", 0.0))
        if delay > 0:
            time.sleep(delay / 1000)  # enforcement runs in the monitor's thread pool
        return _call(self, node_id, operation, params)

    control.LocalAgentdControlClient._call = _delayed_call  # type: ignore[method-assign]

from aggregator.main import app  # noqa: E402  (built after the patch, by design)

__all__ = ["app"]
