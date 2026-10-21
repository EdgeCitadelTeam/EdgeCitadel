"""Wake agentd's waiting loops when work may have become available.

The store calls ``notify`` where a task can become claimable (created,
requeued, its hold or a filter released, its held dispatch admitted) or an
outbox message publishable (enqueued, or its hold, label or held event
released). The transport's outbound loop and the task-claim long-poll wait on
the signal instead of sleeping a fixed interval, and still re-check at a
fallback interval, so a change without a notification is picked up late, not
never. A notification only wakes waiters: they re-read the store under its
lock, so one sent inside a transaction is seen after its commit, and one for a
transaction that rolled back costs a re-check.

Fallback intervals (milliseconds): ``EDGECITADEL_TRANSPORT_POLL_MS`` for the
outbound loop and ``EDGECITADEL_CLAIM_POLL_MS`` for a waiting claim, both 50 by
default: a missed notification then still meets the 100 ms hand-off budget,
while an idle node re-reads its outbox and each waiting slot its claimable
tasks at most 20 times a second with one indexed read each.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable

DEFAULT_POLL_MS = 50


def interval_ms(name: str, default: int = DEFAULT_POLL_MS) -> int:
    """A positive interval from the environment (1 ms to 60 s)."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if not 1 <= value <= 60_000:
        raise ValueError(f"{name} must be between 1 and 60000 milliseconds")
    return value


class WorkSignal:
    """A generation counter with waiting threads and asyncio wakers."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._generation = 0
        self._wakers: list[Callable[[], None]] = []

    @property
    def generation(self) -> int:
        with self._condition:
            return self._generation

    def notify(self) -> None:
        with self._condition:
            self._generation += 1
            wakers = list(self._wakers)
            self._condition.notify_all()
        for waker in wakers:
            try:
                waker()
            except RuntimeError:  # its event loop has closed
                pass

    def wait(self, generation: int, timeout: float) -> int:
        """Block until the generation moves past ``generation`` or the timeout."""
        with self._condition:
            self._condition.wait_for(
                lambda: self._generation != generation, max(0.0, timeout)
            )
            return self._generation

    def add_waker(self, waker: Callable[[], None]) -> Callable[[], None]:
        """Call ``waker`` (thread-safe, non-blocking) on every notification;
        returns its removal."""
        with self._condition:
            self._wakers.append(waker)

        def remove() -> None:
            with self._condition:
                if waker in self._wakers:
                    self._wakers.remove(waker)

        return remove
