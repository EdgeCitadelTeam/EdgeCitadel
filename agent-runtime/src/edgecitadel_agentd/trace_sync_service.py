"""Opt-in telemetry lifecycle isolated from the command transport loop."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from pathlib import Path

from nats import errors as nats_errors
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.errors import APIError, NotFoundError

from edgecitadel_plugin_runtime.telemetry_stream import (
    TelemetryConfigurationError,
    ensure_telemetry_stream,
)

from .node_state import read_node
from .store import AgentdStore, StoreError
from .trace_exporter import ExportScope
from .trace_metrics import SourceMetrics
from .trace_sync_manager import TraceSyncManager, clear_scope_fault


class TraceSyncService:
    def __init__(
        self,
        state_dir: Path,
        open_store: Callable[[], AgentdStore],
        *,
        enabled: bool = False,
    ) -> None:
        self.state_dir, self.enabled = state_dir, enabled
        self._open_store = open_store
        self._metrics = SourceMetrics()
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._closed = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._status: dict[str, object] = {
            "enabled": enabled,
            "state": "idle" if enabled else "disabled",
            "connected": False,
            "active_scopes": 0,
            "fault": None,
        }

    def status(self) -> dict[str, object]:
        with self._lock:
            return {**self._status, "metrics": self._metrics.snapshot()}

    def _set_status(self, **values: object) -> None:
        with self._lock:
            self._status.update(values)

    def start(self) -> None:
        with self._lifecycle_lock:
            self._start()

    def _start(self) -> None:
        with self._lock:
            if (
                self._closed
                or not self.enabled
                or (self._thread is not None and self._thread.is_alive())
            ):
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._thread_main, name="trace-sync", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop_service()

    def close(self) -> None:
        """Terminal daemon shutdown; late control requests cannot restart work."""
        with self._lifecycle_lock:
            self._closed = True
            self.stop()

    def _stop_service(self) -> None:
        with self._lock:
            already_stopping = self._stop.is_set()
            self._stop.set()
            thread, loop, task = self._thread, self._loop, self._task
            if not already_stopping and loop is not None and task is not None:
                loop.call_soon_threadsafe(task.cancel)
        if thread is not None:
            thread.join(timeout=10)
            if thread.is_alive():
                raise StoreError(
                    "telemetry synchronization did not stop within 10 seconds"
                )

    def control(
        self, store: AgentdStore, params: dict[str, object]
    ) -> dict[str, object]:
        """Administrator-authorized caller; only retry clears one durable fault."""
        action = params.get("action")
        scope = params.get("scope")
        if (
            action not in ("stop", "start", "retry")
            or set(params) - {"action", "scope"}
            or (action != "retry" and "scope" in params)
            or (
                action == "retry"
                and (
                    not isinstance(scope, list)
                    or len(scope) != 3
                    or any(
                        not isinstance(value, str) or not 1 <= len(value) <= 128
                        for value in scope
                    )
                )
            )
        ):
            raise StoreError("invalid telemetry control request")
        with self._lifecycle_lock:
            if self._closed:
                raise StoreError("telemetry lifecycle is closed")
            if action != "stop" and not self.enabled:
                raise StoreError("telemetry is disabled by process configuration")
            if action == "retry":
                assert isinstance(scope, list)
                with store._lock:
                    if (
                        store._connection.execute(
                            "SELECT 1 FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                            scope,
                        ).fetchone()
                        is None
                    ):
                        raise StoreError("unknown telemetry export generation")
                # Join every publisher before clearing a fault; late worker exit
                # must not race with an explicit administrator retry.
                self.stop()
                clear_scope_fault(store, ExportScope(*scope))
                self.start()
            elif action == "stop":
                self.stop()
            else:
                self.start()
            return self.status()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._entry())
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - status must never contain credential/exception text
            self._set_status(state="paused", fault="telemetry_lifecycle_fault")
        finally:
            with self._lock:
                self._loop, self._task = None, None
                self._status.update(connected=False, active_scopes=0)
                if self._stop.is_set():
                    self._status["state"] = "stopped"

    async def _entry(self) -> None:
        try:
            await self._run()
        finally:
            # Clear thread-visible handles before asyncio.run closes its loop.
            with self._lock:
                self._loop, self._task = None, None

    async def _run(self) -> None:
        with self._lock:
            self._loop, self._task = asyncio.get_running_loop(), asyncio.current_task()
        if self._stop.is_set():
            return
        self._set_status(state="starting", fault=None)
        # Construct and close the second SQLite handle on the telemetry thread.
        store = self._open_store()
        nc: NATS | None = None
        delay = 1.0
        try:
            while not self._stop.is_set():
                node = read_node(self.state_dir)
                if node is None:
                    self._set_status(
                        state="unconfigured", fault="node_state_unavailable"
                    )
                    await asyncio.sleep(1)
                    continue
                nc = NATS()
                try:
                    await nc.connect(
                        servers=[node["plugin_nats_url"]],
                        token=node["plugin_nats_token"],
                        connect_timeout=2,
                        reconnect_time_wait=1,
                        max_reconnect_attempts=-1,
                        pending_size=256 * 1024,
                    )
                    # Core is domainless, including when connecting through a Leaf.
                    # Command transport separately targets the Leaf's local domain.
                    js = nc.jetstream()
                    # Leaf domainless management APIs resolve locally; only Core
                    # provisions/verifies its stream. Publish/control subjects route
                    # across the Leaf without a JetStream management lookup.
                    if node.get("messaging_mode") != "nats_leaf":
                        await ensure_telemetry_stream(js, create=False)
                    await self._manage(store, nc, js)
                    return  # A stopped/paused manager requires an explicit restart.
                except (
                    TelemetryConfigurationError,
                    nats_errors.AuthorizationError,
                    nats_errors.BadSubjectError,
                ):
                    self._set_status(
                        state="paused", fault="telemetry_configuration_error"
                    )
                    return
                except (nats_errors.Error, APIError, OSError) as error:
                    if (
                        isinstance(error, APIError)
                        and not isinstance(error, NotFoundError)
                        and error.code is not None
                        and 400 <= error.code < 500
                        and error.code not in (408, 429)
                    ):
                        self._set_status(
                            state="paused", fault="telemetry_configuration_error"
                        )
                        return
                    self._set_status(
                        state="retrying", fault="telemetry_unavailable", connected=False
                    )
                finally:
                    if not nc.is_closed:
                        await nc.close()
                    nc = None
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
        finally:
            try:
                if nc is not None and not nc.is_closed:
                    await nc.close()
            finally:
                store.close()

    async def _manage(self, store: AgentdStore, nc: NATS, js: JetStreamContext) -> None:
        manager = TraceSyncManager(store, js, nc)
        manager.metrics = self._metrics
        stop = asyncio.Event()
        task = asyncio.create_task(manager.run(stop))
        try:
            while not task.done():
                self._set_status(
                    state=manager.state,
                    fault=manager.fault,
                    connected=nc.is_connected,
                    active_scopes=len(manager.active),
                )
                await asyncio.sleep(0.1)
            await task
            self._set_status(state=manager.state, fault=manager.fault)
        finally:
            stop.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
