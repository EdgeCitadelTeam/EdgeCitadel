"""Bounded rotating scope admission on a dedicated telemetry event loop."""

from __future__ import annotations

import asyncio
import sqlite3

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from .store import AgentdStore
from .trace_contract import TraceContractError
from .trace_exporter import ExportScope, TraceExporter
from .trace_metrics import SourceMetrics
from .trace_sync import TraceSyncWorker
from .writer_lock import WriterActiveError, exclusive_writer

MAX_ACTIVE_SCOPES = 8
SLICE_SECONDS = 30.0
DISCOVERY_SECONDS = 1.0
ScopeKey = tuple[str, str, str]


def scope_page(
    store: AgentdStore, after: ScopeKey | None, ceiling: ScopeKey | None
) -> tuple[list[tuple[ExportScope, bool]], ScopeKey | None]:
    """Read at most eight generation rows using the primary-key index.

    Include empty, retired and paused generations in the scan cursor, so filtering
    cannot cause unbounded scans or pin discovery behind a poison generation.
    Capture an upper boundary per pass; newer scopes are picked up on a later pass.
    """
    with store._lock:
        db = store._connection
        if db.in_transaction:
            raise TraceContractError("sync_requires_committed_store")
        if ceiling is None:
            row = db.execute(
                "SELECT node_id,source_epoch,export_generation FROM trace_export_generations "
                "ORDER BY node_id DESC,source_epoch DESC,export_generation DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return [], None
            ceiling = tuple(row)
        rows = db.execute(
            "SELECT node_id,source_epoch,export_generation,next_export_seq,sync_fault "
            "FROM trace_export_generations WHERE (node_id,source_epoch,export_generation)> "
            "(?,?,?) AND (node_id,source_epoch,export_generation)<=(?,?,?) "
            "ORDER BY node_id,source_epoch,export_generation LIMIT ?",
            (*(after or ("", "", "")), *ceiling, MAX_ACTIVE_SCOPES),
        ).fetchall()
    return [
        (ExportScope(*row[:3]), row[3] > 1 and row[4] is None) for row in rows
    ], ceiling


def clear_scope_fault(store: AgentdStore, scope: ExportScope) -> None:
    """Explicit local retry after diagnosis; scheduling never clears faults."""
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("sync_requires_committed_store")
        with store._connection:
            store._connection.execute(
                "UPDATE trace_export_generations SET sync_fault=NULL "
                "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                scope.values(),
            )


class TraceSyncManager:
    """At most eight admitted workers and one owner per state directory.

    A 30-second window preserves the normal polling floor between admissions.
    Work beyond the window stays durable and is revisited in key order. Memory and
    task count are bounded independently of generation count; catch-up latency is
    proportional to eligible pages and remains a separate measured acceptance gate.
    """

    def __init__(self, store: AgentdStore, js: JetStreamContext, nc: NATS) -> None:
        self.store, self.js, self.nc = store, js, nc
        self.metrics = SourceMetrics()
        self.state = "idle"
        self.fault: str | None = None
        self.active: dict[ExportScope, asyncio.Task[None]] = {}
        self._running = False

    async def _serve(self, scope: ExportScope, stop: asyncio.Event) -> None:
        worker = TraceSyncWorker(self.store, self.js, self.nc, scope)
        worker.metrics = self.metrics
        try:
            await worker.run(stop)
        except Exception:  # noqa: BLE001 - isolate the scope without persisting exception text
            # Do not retain exception text or arbitrary remote data in metadata.
            worker.state, worker.fault = "paused", "unexpected_sync_fault"
        if worker.state == "paused":
            with self.store._lock:
                if self.store._connection.in_transaction:
                    raise TraceContractError("sync_requires_committed_store")
                with self.store._connection:
                    self.store._connection.execute(
                        "UPDATE trace_export_generations SET sync_fault=? "
                        "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                        (worker.fault or "unexpected_sync_fault", *scope.values()),
                    )

    async def _release(self) -> None:
        tasks = list(self.active.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.active.clear()
        for result in results:
            if isinstance(result, Exception):
                raise result

    async def run(self, stop: asyncio.Event) -> None:
        if self._running:
            raise RuntimeError("trace_sync_manager_already_running")
        self._running = True
        try:
            # Separate from agentd's writer.lock; retain the inode after release.
            with exclusive_writer(self.store.path.parent / "telemetry-sync"):
                self.state, self.fault = "running", None
                after: ScopeKey | None = None
                ceiling: ScopeKey | None = None
                try:
                    while not stop.is_set():
                        page, ceiling = scope_page(self.store, after, ceiling)
                        if not page:
                            after, ceiling = None, None
                            await TraceExporter._wait(stop, DISCOVERY_SECONDS)
                            continue
                        after = page[-1][0].values()
                        for scope, eligible in page:
                            if eligible:
                                self.active[scope] = asyncio.create_task(
                                    self._serve(scope, stop)
                                )
                        await TraceExporter._wait(
                            stop, SLICE_SECONDS if self.active else DISCOVERY_SECONDS
                        )
                        await self._release()
                finally:
                    await self._release()
        except WriterActiveError:
            self.state, self.fault = "paused", "sync_owner_active"
        except (TraceContractError, sqlite3.DatabaseError):
            self.state, self.fault = "paused", "local_scheduler_fault"
        finally:
            self._running = False
            if self.state != "paused":
                self.state = "stopped"
