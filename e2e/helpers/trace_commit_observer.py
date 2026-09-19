"""Bounded, opt-in commit instrumentation for the jim-eq qualification launcher.

Not imported by production. Stop the collector before leaving instrument_collector;
the context owns module wiring, not service or connection lifetime.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CommitBracket:
    before_ns: int
    after_ns: int


class TimedConnection(sqlite3.Connection):
    last_commit: CommitBracket | None = None

    def __enter__(self):
        self.last_commit = None
        self._initial_changes = self.total_changes
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.last_commit = None
        changed = self.total_changes > self._initial_changes
        committing = exc_type is None and self.in_transaction and changed
        before = time.monotonic_ns()
        result = super().__exit__(exc_type, exc_value, traceback)
        after = time.monotonic_ns()
        if committing:
            self.last_commit = CommitBracket(before, after)
        return result


class CommitObserver:
    """One owned source/run; bounded memory, first identity wins, no payload output."""

    def __init__(self, *, node_id, source_epoch, trace_id, capacity=4096):
        if not 1 <= capacity <= 100_000:
            raise ValueError("invalid observer capacity")
        self.scope = (node_id, source_epoch, trace_id)
        self.capacity = capacity
        self._lock = threading.Lock()
        self._records = {}
        self._failure = None
        self._max_callback_ns = 0

    def invalidate(self, reason):
        with self._lock:
            self._failure = self._failure or reason

    def observe(self, connection, message, result):
        started = time.monotonic_ns()
        try:
            # Replays of accepted positions return outcome=accepted, but their
            # read-only transaction has no commit marker. Never retime them.
            bracket = connection.last_commit
            if result.outcome != "accepted" or bracket is None:
                return
            event = json.loads(message.data)["event"]
            if (
                event["node_id"],
                event["source_epoch"],
                event["trace_id"],
            ) != self.scope:
                return
            identity = (event["node_id"], event["source_epoch"], event["event_id"])
            with self._lock:
                if identity in self._records:
                    return
                if len(self._records) == self.capacity:
                    self._failure = self._failure or "capacity_exceeded"
                    return
                self._records[identity] = {
                    "node_id": identity[0],
                    "source_epoch": identity[1],
                    "event_id": identity[2],
                    "collector_epoch": result.collector_epoch,
                    "ingest_seq": result.ingest_seq,
                    **asdict(bracket),
                }
        except Exception:  # noqa: BLE001 - invalidate measurement, preserve ingestion
            self.invalidate("observer_error")
        finally:
            elapsed = time.monotonic_ns() - started
            with self._lock:
                self._max_callback_ns = max(self._max_callback_ns, elapsed)

    def report(self):
        with self._lock:
            return {
                "valid": self._failure is None,
                "failure": self._failure,
                "records": [dict(record) for record in self._records.values()],
                "max_callback_ns": self._max_callback_ns,
                "scope": "Commit brackets only; no rendered acknowledgments or latency claim",
            }


class _CollectorSQLite:
    def __getattr__(self, name):
        return getattr(sqlite3, name)

    def connect(self, *args, **kwargs):
        return sqlite3.connect(*args, **kwargs, factory=TimedConnection)


@contextmanager
def instrument_collector(module, observer):
    """Wire only this collector module; never mutate the global sqlite3 module."""
    original_sqlite = module.sqlite3
    original_delivery = module.ingest_delivery
    if original_sqlite is not sqlite3:
        raise RuntimeError("collector already instrumented")

    async def delivery(connection, message, *, on_commit=None):
        connection.last_commit = None

        def committed(result):
            try:
                observer.observe(connection, message, result)
            except Exception:  # noqa: BLE001 - an observer must not skip collector bookkeeping
                observer.invalidate("observer_error")
            finally:
                if on_commit is not None:
                    on_commit(result)

        return await original_delivery(connection, message, on_commit=committed)

    module.sqlite3 = _CollectorSQLite()
    module.ingest_delivery = delivery
    try:
        yield
    finally:
        module.ingest_delivery = original_delivery
        module.sqlite3 = original_sqlite
