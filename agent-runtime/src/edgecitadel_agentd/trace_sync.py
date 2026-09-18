"""Coordinate one source generation; the lifecycle owner admits unique scopes.

Run on the telemetry loop with its dedicated store connection, never the command
loop. Discovery, admission limits and daemon startup are separate responsibilities.
"""

from __future__ import annotations

import asyncio
import sqlite3

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from .store import AgentdStore
from .trace_collector_recovery import begin_recovery, recover_batch
from .trace_contract import TraceContractError
from .trace_exporter import ExportScope, TraceExporter
from .trace_metrics import SourceMetrics
from .trace_settlement_poll import SettlementPoller


class TraceSyncWorker:
    """Publishing stays live during control backoff; recovery fences both paths."""

    def __init__(
        self, store: AgentdStore, js: JetStreamContext, nc: NATS, scope: ExportScope
    ) -> None:
        self.store, self.js, self.nc, self.scope = store, js, nc, scope
        self.metrics = SourceMetrics()
        self.state = "idle"
        self.fault: str | None = None
        self.exporter = TraceExporter(store, js, scope)
        self.poller = SettlementPoller(store, nc, scope.values())
        self._replay = asyncio.Event()
        self._publisher: asyncio.Task[None] | None = None
        self._running = False

    async def _cancel_publisher(self) -> None:
        if self._publisher is not None:
            self._publisher.cancel()
            await asyncio.gather(self._publisher, return_exceptions=True)
            self._publisher = None

    async def _recover(self, stop: asyncio.Event, *, changed: bool) -> None:
        # Cancellation completes before any spool reset. A cancelled publish can
        # have reached the broker; its immutable message identity makes replay safe.
        await self._cancel_publisher()
        await self.poller.close()
        self.state = "recovering"
        delay = 1.0
        while not stop.is_set():
            try:
                with self.store._lock:
                    if self.store._connection.in_transaction:
                        raise TraceContractError("recovery_requires_committed_store")
                    if changed:
                        row = self.store._connection.execute(
                            "SELECT collector_epoch FROM trace_source_settlements "
                            "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                            self.scope.values(),
                        ).fetchone()
                        if row is None:
                            raise TraceContractError("recovery_epoch_missing")
                        begin_recovery(
                            self.store, self.scope.values(), expected_epoch=row[0]
                        )
                        changed = False
                    row = self.store._connection.execute(
                        "SELECT phase FROM trace_collector_recovery "
                        "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                        self.scope.values(),
                    ).fetchone()
                if row is None or row[0] != "scanning":
                    break
                if recover_batch(self.store, self.scope.values()):
                    break
                delay = 1.0
                await asyncio.sleep(0)
            except sqlite3.OperationalError:
                self.fault = "local_storage_unavailable"
                await TraceExporter._wait(stop, delay)
                delay = min(30.0, delay * 2)
        self.poller = SettlementPoller(self.store, self.nc, self.scope.values())
        self.poller.metrics = self.metrics
        self._replay.clear()
        self.state, self.fault = "running", None

    def _start_publisher(self, stop: asyncio.Event) -> None:
        self.exporter = TraceExporter(self.store, self.js, self.scope)
        self.exporter.metrics = self.metrics
        self._publisher = asyncio.create_task(
            self.exporter.run(stop, replay_requested=self._replay)
        )

    async def run(self, stop: asyncio.Event) -> None:
        if self._running:
            raise RuntimeError("trace_scope_worker_already_running")
        self._running = True
        stopped = asyncio.create_task(stop.wait())
        poll: asyncio.Task[str] | None = None
        try:
            # Resume a durable recovery before either network path is started.
            await self._recover(stop, changed=False)
            if stop.is_set():
                return
            self._start_publisher(stop)
            while not stop.is_set():
                publisher = self._publisher
                assert publisher is not None
                poll = asyncio.create_task(self.poller.poll())
                done, _ = await asyncio.wait(
                    (poll, publisher, stopped),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stopped in done:
                    break
                if publisher in done:
                    await publisher
                    self.state, self.fault = "paused", self.exporter.fault
                    break
                outcome = await poll
                if outcome == "collector_changed":
                    await self._recover(stop, changed=True)
                    if not stop.is_set():
                        self._start_publisher(stop)
                    continue
                if self.poller.state == "paused":
                    self.state, self.fault = "paused", self.poller.fault
                    break
                if self.poller.replay_required:
                    self._replay.set()
        except (TraceContractError, sqlite3.DatabaseError):
            self.state, self.fault = "paused", "local_sync_fault"
        finally:
            await self._cancel_publisher()
            await self.poller.close()
            for task in (poll, stopped):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (poll, stopped) if task is not None),
                return_exceptions=True,
            )
            self._running = False
            if self.state != "paused":
                self.state = "stopped"
