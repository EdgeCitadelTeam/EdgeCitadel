"""Independent opt-in Core telemetry thread, durable consumer and control responder."""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path

from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_settlement_pages import SETTLEMENT_PAGE_SUBJECT
from edgecitadel_agentd.writer_lock import exclusive_writer
from edgecitadel_plugin_runtime.telemetry_stream import (
    CONSUMER_NAME,
    EVENT_SUBJECT,
    STREAM_NAME,
    TelemetryConfigurationError,
    ensure_telemetry_consumer,
    ensure_telemetry_stream,
)
from nats.aio.client import Client as NATS

from nats import errors as nats_errors

from . import trace_capacity, trace_payloads
from .trace_capacity import snapshot
from .trace_control import SettlementResponder
from .trace_ingest import ingest_delivery
from .trace_retention import expire_payloads
from .trace_store import MAX_REJECTED_POSITIONS, initialize


class TraceCollectorService:
    def __init__(self, db_path: Path, url: str, token: str):
        self.db_path, self.url, self.token = db_path, url, token
        self._lock = threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._closed = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop = None
        self._task = None
        self._status = {
            "enabled": True,
            "state": "idle",
            "connected": False,
            "fault": None,
            "broker_backlog": {"state": "unavailable"},
            "metrics": {
                "lifetime": "service_instance",
                "commit_observations": {
                    key: 0
                    for key in (
                        "accepted",
                        "duplicate",
                        "conflict",
                        "rejected",
                        "quarantined",
                    )
                },
                "ack_successes": 0,
                "ack_failures": 0,
                "persistence_failures": 0,
                "last_commit_observed_at_ms": None,
                "last_delivery_age_ms": None,
                "delivery_clock_skew": False,
                "last_event_collection_age_ms": None,
                "event_collection_age_state": "unavailable",
            },
        }

    def status(self):
        with self._lock:
            return copy.deepcopy(self._status)

    def _set(self, **values):
        with self._lock:
            self._status.update(values)

    def _increment(self, key):
        with self._lock:
            metrics = self._status["metrics"]
            metrics[key] = min(2**53 - 1, metrics[key] + 1)

    def _observe_commit(self, message, result):
        now = time.time_ns() // 1_000_000
        age = None
        try:
            age = now - int(message.metadata.timestamp.timestamp() * 1000)
        except (AttributeError, ValueError, OverflowError, nats_errors.Error):
            pass  # Non-JetStream test/adaptor messages have no broker timestamp.
        event_age = None
        event_age_state = "unavailable"
        # Only validated, committed accepted/duplicate events supply event time.
        # Replays retain occurrence time, so this is apparent age, not network latency.
        if result.outcome in {"accepted", "duplicate"}:
            try:
                occurred = datetime.fromisoformat(
                    json.loads(message.data)["event"]["occurred_at"]
                )
                if occurred.tzinfo is not None:
                    delta = now - round(occurred.timestamp() * 1000)
                    if delta < 0:
                        event_age_state = "clock_skew"
                    elif delta <= 2**53 - 1:
                        event_age, event_age_state = delta, "observed"
            except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
                pass
        with self._lock:
            metrics = self._status["metrics"]
            metrics["last_event_collection_age_ms"] = event_age
            metrics["event_collection_age_state"] = event_age_state
            counts = metrics["commit_observations"]
            if result.outcome in counts:
                counts[result.outcome] = min(2**53 - 1, counts[result.outcome] + 1)
            metrics["last_commit_observed_at_ms"] = now
            metrics["last_delivery_age_ms"] = (
                age if age is not None and age >= 0 else None
            )
            metrics["delivery_clock_skew"] = age is not None and age < 0

    async def _sample_backlog(self, consumer):
        try:
            info = await asyncio.wait_for(consumer.consumer_info(), timeout=2)
            counts = (info.num_pending, info.num_ack_pending)
            if any(
                type(value) is not int or not 0 <= value <= 2**53 - 1
                for value in counts
            ):
                raise ValueError("invalid_broker_counts")
            self._set(
                broker_backlog={
                    "state": "available",
                    "sampled_at_ms": time.time_ns() // 1_000_000,
                    "pending_delivery": counts[0],
                    "awaiting_ack": counts[1],
                }
            )
        except Exception:  # noqa: BLE001 - status sampling must never stop ingestion
            self._set(broker_backlog={"state": "unavailable"})

    async def _watch_backlog(self, consumer):
        try:
            while not self._stop.is_set():
                await self._sample_backlog(consumer)
                await asyncio.sleep(5)
        finally:
            self._set(broker_backlog={"state": "unavailable"})

    def start(self):
        with self._lifecycle_lock:
            self._start()

    def _start(self):
        with self._lock:
            if self._closed or (self._thread is not None and self._thread.is_alive()):
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._main, name="trace-collector", daemon=True
            )
            self._thread.start()

    def stop(self):
        with self._lifecycle_lock:
            self._stop_service()

    def _stop_service(self):
        with self._lock:
            stopping = self._stop.is_set()
            self._stop.set()
            if not stopping and self._loop is not None and self._task is not None:
                self._loop.call_soon_threadsafe(self._task.cancel)
            thread = self._thread
        if thread is not None:
            thread.join(10)
            if thread.is_alive():
                raise RuntimeError("trace_collector_shutdown_timeout")

    def close(self):
        with self._lifecycle_lock:
            self._closed = True
            self.stop()

    def control(self, action: str):
        if action not in ("stop", "start", "retry"):
            raise ValueError("invalid_collector_control")
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("collector_lifecycle_closed")
            if action in ("stop", "retry"):
                self.stop()
            if action in ("start", "retry"):
                self.start()
            return self.status()

    def _main(self):
        try:
            with exclusive_writer(self.db_path.parent / "trace-collector"):
                asyncio.run(self._entry())
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - publish a fixed code, never exception text
            self._set(state="paused", fault="collector_lifecycle_fault")
        finally:
            self._set(connected=False, broker_backlog={"state": "unavailable"})
            if self._stop.is_set():
                self._set(state="stopped")

    async def _entry(self):
        with self._lock:
            self._loop, self._task = asyncio.get_running_loop(), asyncio.current_task()
        try:
            delay = 1.0
            while not self._stop.is_set():
                try:
                    await self._run()
                    return
                except TraceContractError as error:
                    if error.code == "core_payload_layout_unavailable":
                        self._set(
                            state="paused",
                            connected=False,
                            fault="collector_payload_layout_unavailable",
                        )
                        return
                    self._set(
                        state="retrying",
                        connected=False,
                        fault="collector_physical_pressure"
                        if error.code == "core_physical_pressure"
                        else "collector_storage_unavailable",
                    )
                    await asyncio.sleep(delay)
                    delay = min(30.0, delay * 2)
                except sqlite3.OperationalError:
                    self._set(
                        state="retrying",
                        connected=False,
                        fault="collector_storage_unavailable",
                    )
                    await asyncio.sleep(delay)
                    delay = min(30.0, delay * 2)
        finally:
            with self._lock:
                self._loop, self._task = None, None

    async def _run(self):
        db = sqlite3.connect(self.db_path)
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA busy_timeout=50")
            db.execute("PRAGMA cache_spill=OFF")
            initialize(db, backfill=False)
            while not self._stop.is_set():
                if trace_capacity.backfill_step(db):
                    break
                await asyncio.sleep(0)
            if self._stop.is_set():
                return
            trace_payloads.prepare(db)
            self._set(state="migrating", connected=False, fault=None)
            while not self._stop.is_set():
                if trace_payloads.migrate_batch(db):
                    break
                await asyncio.sleep(0)
            if self._stop.is_set():
                return
            epoch = db.execute(
                "SELECT collector_epoch FROM trace_collector WHERE singleton=1"
            ).fetchone()[0]
            self._set(
                state="starting",
                collector_epoch=epoch,
                fault=None,
                **snapshot(db, rejection_limit=MAX_REJECTED_POSITIONS),
            )
            delay = 1.0
            while not self._stop.is_set():
                nc = NATS()
                try:
                    await nc.connect(
                        servers=[self.url],
                        token=self.token,
                        connect_timeout=2,
                        reconnect_time_wait=1,
                        max_reconnect_attempts=-1,
                        pending_size=256 * 1024,
                    )
                    js = nc.jetstream()
                    await ensure_telemetry_stream(js)
                    await ensure_telemetry_consumer(js)
                    consumer = await js.pull_subscribe(
                        EVENT_SUBJECT, durable=CONSUMER_NAME, stream=STREAM_NAME
                    )
                    control = await nc.subscribe(
                        SETTLEMENT_PAGE_SUBJECT,
                        cb=SettlementResponder(db).__call__,
                        pending_msgs_limit=32,
                        pending_bytes_limit=32 * 1024,
                    )
                    self._set(state="running", connected=True, fault=None)
                    delay = 1.0
                    backlog = asyncio.create_task(self._watch_backlog(consumer))
                    try:
                        await self._ingest(db, nc, consumer)
                    finally:
                        backlog.cancel()
                        await asyncio.gather(backlog, return_exceptions=True)
                        await control.unsubscribe()
                        await consumer.unsubscribe()
                except (
                    TelemetryConfigurationError,
                    nats_errors.AuthorizationError,
                    nats_errors.BadSubjectError,
                ):
                    self._set(state="paused", fault="collector_configuration_error")
                    return
                except (nats_errors.Error, OSError):
                    self._set(
                        state="retrying", connected=False, fault="collector_unavailable"
                    )
                finally:
                    if not nc.is_closed:
                        await nc.close()
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
        finally:
            db.close()

    async def _ingest(self, db, nc, consumer):
        next_retention = 0.0
        while not self._stop.is_set():
            self._set(connected=nc.is_connected)
            if time.monotonic() >= next_retention:
                try:
                    retained = expire_payloads(db, now_ms=time.time_ns() // 1_000_000)
                    storage = snapshot(db, rejection_limit=MAX_REJECTED_POSITIONS)
                except (sqlite3.DatabaseError, TraceContractError):
                    self._set(retention={"state": "unavailable"})
                else:
                    self._set(retention={"state": "observed", **retained}, **storage)
                next_retention = time.monotonic() + 1.0
            try:
                messages = await consumer.fetch(batch=32, timeout=1)
            except nats_errors.TimeoutError:
                continue
            for message in messages:
                committed = False

                def observed(result, delivered_message=message):
                    nonlocal committed
                    committed = True
                    self._observe_commit(delivered_message, result)

                try:
                    await ingest_delivery(db, message, on_commit=observed)
                except (nats_errors.Error, OSError):
                    if committed:
                        self._increment("ack_failures")
                    raise
                except (sqlite3.DatabaseError, TraceContractError) as error:
                    self._increment("persistence_failures")
                    fault = "collector_storage_unavailable"
                    if isinstance(error, TraceContractError):
                        fault = {
                            "core_capacity_exceeded": "collector_capacity_exceeded",
                            "core_physical_pressure": "collector_physical_pressure",
                        }.get(error.code, fault)
                    self._set(
                        fault=fault,
                        **snapshot(db, rejection_limit=MAX_REJECTED_POSITIONS),
                    )
                    # No ACK without a durable disposition. Delay this record while
                    # proceeding to the next fetched record, including valid input.
                    await message.nak(delay=1)
                else:
                    self._increment("ack_successes")
                    self._set(
                        fault=None,
                        **snapshot(db, rejection_limit=MAX_REJECTED_POSITIONS),
                    )
