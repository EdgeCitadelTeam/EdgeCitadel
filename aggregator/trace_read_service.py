"""Bounded read-only query workers and dashboard access scope."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable
from contextlib import closing
from pathlib import Path

from edgecitadel_agentd.trace_cursor import cursor_scope_hash

from . import (
    trace_change_pages,
    trace_event_pages,
    trace_graph_pages,
    trace_list_pages,
    trace_history_pages,
)
from .trace_event_pages import TraceReadError
from .trace_read_key import load_or_create_key
from .trace_projection_store import _state
from .trace_projection_tables import select_tables

QUERY_TIMEOUT_SECONDS = 5.0
MAX_READERS = 4


class TraceReadService:
    def __init__(
        self,
        db_path: Path,
        key_path: Path,
        *,
        collector_status: Callable[[], dict] | None = None,
    ):
        self._collector_status = collector_status
        self.db_path = db_path.resolve()
        self._key = load_or_create_key(key_path)
        self._policy = {
            "mode": "trusted_fleet",
            "policy_version": 2,
        }
        self._scope = cursor_scope_hash({}, self._policy)
        self._slots = threading.BoundedSemaphore(MAX_READERS)
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._pool = ThreadPoolExecutor(
            max_workers=MAX_READERS, thread_name_prefix="trace-read"
        )

    async def query(self, kind: str, **parameters) -> dict:
        canceled = threading.Event()
        with self._lock:
            if self._closed.is_set() or not self._slots.acquire(blocking=False):
                raise TraceReadError("unavailable")
            try:
                future = self._pool.submit(self._read, kind, parameters, canceled)
            except RuntimeError:
                self._slots.release()
                raise TraceReadError("unavailable") from None
            # Release only after the actual worker terminates, not when its
            # awaiting request is canceled while SQLite still owns a connection.
            future.add_done_callback(lambda _: self._slots.release())
        try:
            result = await asyncio.wrap_future(future)
            if "freshness" in result and self._collector_status is not None:
                # Runtime availability is sampled after the retained database read;
                # it is not historical state or proof that all sources are caught up.
                status = self._collector_status()
                result["freshness"]["collector_state"] = (
                    "collecting"
                    if status.get("state") == "running"
                    and status.get("connected") is True
                    else "unavailable"
                )
            return result
        except asyncio.CancelledError:
            canceled.set()
            raise

    def _read(self, kind: str, parameters: dict, canceled: threading.Event) -> dict:
        deadline = time.monotonic() + QUERY_TIMEOUT_SECONDS
        try:
            with closing(
                sqlite3.connect(
                    self.db_path.as_uri() + "?mode=ro", uri=True, timeout=0.05
                )
            ) as connection:
                connection.set_progress_handler(
                    lambda: int(
                        canceled.is_set()
                        or self._closed.is_set()
                        or time.monotonic() >= deadline
                    ),
                    1000,
                )
                if canceled.is_set() or self._closed.is_set():
                    raise TraceReadError("unavailable")
                if kind == "status":
                    with connection:
                        connection.execute("BEGIN")
                        state = _state(select_tables(connection))
                        high = connection.execute(
                            "SELECT ingest_seq FROM trace_collector"
                        ).fetchone()[0]
                        return {
                            "generation": state.generation,
                            "freshness": {
                                "ingest_cursor": high,
                                "projection_cursor": state.change_cursor,
                                "oldest_unsettled_age_ms": None,
                                "collector_state": "unknown",
                            },
                        }
                if kind == "list":
                    return trace_list_pages.read_list(
                        connection,
                        signing_key=self._key,
                        access_policy=self._policy,
                        **parameters,
                    )
                readers = {
                    "graph": trace_graph_pages.read_graph,
                    "events": trace_event_pages.read_events,
                    "changes": trace_change_pages.read_changes,
                    "history": trace_history_pages.read_history,
                }
                return readers[kind](
                    connection,
                    signing_key=self._key,
                    scope_hash=self._scope,
                    **parameters,
                )
        except TraceReadError:
            raise
        except (sqlite3.Error, ValueError):
            raise TraceReadError("unavailable") from None

    def close(self) -> None:
        with self._lock:
            self._closed.set()
        self._pool.shutdown(wait=True, cancel_futures=True)
