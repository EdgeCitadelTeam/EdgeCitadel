"""Fixed-size, service-instance telemetry observations; never durable evidence."""

from __future__ import annotations

import threading
import time

COUNTERS = (
    "publish_attempts",
    "publish_failures",
    "broker_acknowledgments",
    "invalid_broker_acknowledgments",
    "broker_ack_checkpoint_failures",
    "settlement_requests",
    "settlement_request_failures",
    "settlement_page_observations",
)


class SourceMetrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts = dict.fromkeys(COUNTERS, 0)
        self._last_at: dict[str, int | None] = dict.fromkeys(COUNTERS)

    def note(self, key: str) -> None:
        with self._lock:
            if key not in self._counts:
                return
            self._counts[key] = min(2**53 - 1, self._counts[key] + 1)
            self._last_at[key] = time.time_ns() // 1_000_000

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "lifetime": "service_instance",
                "counts": dict(self._counts),
                "last_observed_at_ms": dict(self._last_at),
            }
