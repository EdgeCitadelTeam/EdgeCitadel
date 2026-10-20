"""Owned projector thread; explicit construction only, no startup integration.

The collector owns raw schema/payload migration. This worker opens an existing
Core database, initializes only derived state, and runs bounded maintenance
cycles. Health exposes fixed diagnostics and observed cursors, never exceptions
or event payloads. Call close off the application event loop during shutdown.
"""

from __future__ import annotations

import copy
import sqlite3
import threading
import time
from pathlib import Path

from edgecitadel_agentd.writer_lock import WriterActiveError, exclusive_writer

from . import trace_projection_history as history
from . import trace_projection_retention as retention
from . import trace_projection_store as projection
from .trace_projection_maintenance import run_cycle
from .trace_projection_tables import select_tables

POLL_SECONDS = 0.1
SHUTDOWN_SECONDS = 10.0


class TraceProjectorService:
    def __init__(self, db_path: Path):
        self.db_path = db_path.resolve()
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._stop = threading.Event()
        self._closed = False
        self._thread: threading.Thread | None = None
        self._connection: sqlite3.Connection | None = None
        self._status = {
            "state": "idle",
            "fault": None,
            "checkpoint": None,
            "cycles": 0,
            "last_success_at_ms": None,
            "last_cycle_ms": None,
            "phase": None,
        }

    def status(self) -> dict:
        with self._lock:
            result = copy.deepcopy(self._status)
            result["worker_alive"] = bool(self._thread and self._thread.is_alive())
            result["closed"] = self._closed
            return result

    def _set(self, **values) -> None:
        with self._lock:
            self._status.update(values)

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("projector_lifecycle_closed")
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            with self._lock:
                self._status.update(
                    state="starting",
                    fault=None,
                    checkpoint=None,
                    last_success_at_ms=None,
                    last_cycle_ms=None,
                    phase=None,
                )
                self._thread = threading.Thread(
                    target=self._main, name="trace-projector", daemon=True
                )
                thread = self._thread
            thread.start()

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop.set()
            with self._lock:
                thread = self._thread
                if thread and thread.is_alive():
                    self._status["state"] = "stopping"
                # interrupt is SQLite's cross-thread cancellation API. Only the
                # owning worker closes the connection, under this same lock.
                if self._connection is not None:
                    self._connection.interrupt()
            if thread is not None:
                thread.join(SHUTDOWN_SECONDS)
                if thread.is_alive():
                    raise RuntimeError("trace_projector_shutdown_timeout")
            self._set(state="stopped")

    def close(self) -> None:
        with self._lifecycle_lock:
            with self._lock:
                self._closed = True
            self.stop()

    def _main(self) -> None:
        try:
            while not self._stop.is_set() and not self.db_path.is_file():
                self._set(state="waiting", fault="projector_collector_unavailable")
                self._stop.wait(POLL_SECONDS)
            if self._stop.is_set():
                return
            with exclusive_writer(
                self.db_path.parent / (self.db_path.name + ".trace-projector")
            ):
                delay = POLL_SECONDS
                while not self._stop.is_set():
                    with self._lock:
                        previous_cycles = self._status["cycles"]
                    try:
                        self._run_connection()
                        if self._stop.is_set():
                            return
                    except sqlite3.Error as error:
                        if self._stop.is_set():
                            return
                        code = getattr(error, "sqlite_errorcode", 0) & 0xFF
                        if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                            raise
                        self._set(state="retrying", fault="projector_database_busy")
                    with self._lock:
                        if self._status["cycles"] != previous_cycles:
                            delay = POLL_SECONDS
                    self._stop.wait(delay)
                    delay = min(2.0, delay * 2)
        except WriterActiveError:
            self._set(state="paused", fault="projector_writer_active")
        except ValueError as error:
            code = str(error)
            fault = (
                "projector_rebuild_required"
                if code
                in {"projection_version_unavailable", "projection_rebuild_required"}
                else "projector_wal_required"
                if code == "projector_wal_required"
                else "projector_state_unavailable"
            )
            self._set(state="paused", fault=fault)
        except sqlite3.Error:
            self._set(state="paused", fault="projector_storage_unavailable")
        except OSError:
            self._set(state="paused", fault="projector_storage_unavailable")
        except Exception:  # noqa: BLE001 - thread boundary reports failure, never payload-bearing exception text
            self._set(state="failed", fault="projector_lifecycle_fault")
        finally:
            if self._stop.is_set():
                self._set(state="stopping")

    def _run_connection(self) -> None:
        # mode=rw prevents accidental creation of an empty parallel Core store.
        db = sqlite3.connect(self.db_path.as_uri() + "?mode=rw", uri=True, timeout=0.05)
        with self._lock:
            self._connection = db
        try:
            db.set_progress_handler(lambda: int(self._stop.is_set()), 1000)
            if self._stop.is_set():
                return
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trace_collector'"
            ).fetchone()
            if (
                not exists
                or db.execute(
                    "SELECT 1 FROM trace_collector WHERE singleton=1"
                ).fetchone()
                is None
            ):
                self._set(state="waiting", fault="projector_collector_unavailable")
                return
            if db.execute("PRAGMA journal_mode").fetchone()[0] != "wal":
                raise ValueError("projector_wal_required")
            db.execute("PRAGMA cache_spill=OFF")
            projection.initialize(db)
            while not self._stop.is_set():
                started = time.monotonic_ns()
                result = run_cycle(db, now_ms=time.time_ns() // 1_000_000)
                if self._stop.is_set():
                    return
                checkpoint = self._checkpoint(db)
                with self._lock:
                    if self._stop.is_set():
                        return
                    self._status.update(
                        state="running",
                        fault=None,
                        checkpoint=checkpoint,
                        phase=result["phase"],
                        last_success_at_ms=time.time_ns() // 1_000_000,
                        last_cycle_ms=(time.monotonic_ns() - started) // 1_000_000,
                        cycles=min(2**53 - 1, self._status["cycles"] + 1),
                    )
                self._stop.wait(POLL_SECONDS)
        finally:
            with self._lock:
                try:
                    db.set_progress_handler(None, 0)
                    db.close()
                finally:
                    self._connection = None

    @staticmethod
    def _checkpoint(db: sqlite3.Connection) -> dict:
        with db:
            db.execute("BEGIN")
            tables = select_tables(db)
            state = projection._state(tables)
            high = db.execute(
                "SELECT ingest_seq FROM trace_collector WHERE singleton=1"
            ).fetchone()[0]
            return {
                "generation": state.generation,
                "collector_epoch": state.collector_epoch,
                "ingest_cursor": state.ingest_cursor,
                "change_cursor": state.change_cursor,
                "collector_ingest_cursor": high,
                "lag_ingest_commits": high - state.ingest_cursor,
                "history_from_cursor": history.floor(tables),
                "retirement_pending": retention.pending(tables) is not None,
            }
