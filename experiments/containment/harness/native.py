"""What the system baselines' native controllers share (A2A, LangGraph).

A native controller answers the harness's operator alert with a system's own
controls and logs every action in the EdgeCitadel monitor's decision-log
schema (``core/containment-decisions.jsonl``), so ``metrics.decision_metrics``
scores it with the same code: the first response is the ``decision``, later
ones that act are ``resolve`` records, and an action is established once its
``t_confirmed`` is set and its state is not a control failure.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Awaitable, Mapping
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")
CONFIRMED = "confirmed"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class LoopThread:
    """An asyncio loop on its own daemon thread, for synchronous callers."""

    def __init__(self, name: str) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self.loop.run_forever, name=name, daemon=True
        )
        self._thread.start()

    def run(self, coro: Awaitable[T], timeout: float | None = 60.0) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)  # type: ignore[arg-type]

    def submit(self, coro: Awaitable[T]) -> asyncio.Future[T]:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)  # type: ignore[arg-type,return-value]

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(5)


def action_record(
    incident_id: str, op: str, kind: str, object_id: str, node: str, agent: str
) -> dict[str, Any]:
    """One planned action in the monitor's decision-log shape."""
    return {
        "incident_id": incident_id,
        "phase": 1,
        "action": {
            "op": op,
            "kind": kind,
            "object_id": object_id,
            "node_id": node,
            "agent_id": agent,
        },
        "state": "planned",
        "t_decision": now_ms(),
        "t_sent": None,
        "t_confirmed": None,
    }


def append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    with path.open("a") as handle:
        handle.write(json.dumps(record, separators=(",", ":")) + "\n")
