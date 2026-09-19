"""Selected-spool publisher; broker acknowledgments never retire local evidence.

Startup wiring and settlement-driven replay scheduling are deliberately separate.
The caller supplies the configured Core JetStream context, including in Leaf mode.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import sqlite3
from dataclasses import dataclass

from nats import errors as nats_errors
from nats.js import JetStreamContext
from nats.js.errors import APIError

from edgecitadel_plugin_runtime.telemetry_stream import STREAM_NAME

from .store import AgentdStore
from .trace_completed import export_page
from .trace_contract import TraceContractError, canonical_bytes, validate_export
from .trace_metrics import SourceMetrics

BATCH_SIZE = 32


@dataclass(frozen=True)
class ExportScope:
    node_id: str
    source_epoch: str
    export_generation: str

    def values(self) -> tuple[str, str, str]:
        return self.node_id, self.source_epoch, self.export_generation


@dataclass(frozen=True)
class ExportRecord:
    scope: ExportScope
    export_seq: int
    event_sha256: str
    payload: bytes
    recovery_epoch: str | None = None

    @property
    def subject(self) -> str:
        return f"edgecitadel.telemetry.v1.{self.scope.node_id}"

    @property
    def message_id(self) -> str:
        parts = [*self.scope.values(), self.export_seq]
        if self.recovery_epoch is not None:
            # A restored Core needs a new delivery even while the broker remembers
            # the original ACKed publication. Retries in one recovery stay stable.
            parts.extend(["collector-recovery", self.recovery_epoch])
        identity = canonical_bytes(parts)
        return "trace-v1-" + hashlib.sha256(identity).hexdigest()


def selected_batch(
    store: AgentdStore,
    scope: ExportScope,
    *,
    replay: bool = False,
    after: int = 0,
) -> list[ExportRecord]:
    """Read committed rows only; explicit replay includes retained broker ACKs.

    The bounded keyset cursor is an export position, never a row count. Retired
    sources remain eligible. Missing payloads/positions do not imply settlement.
    """
    if type(after) is not int or after < 0:
        raise ValueError("invalid_export_cursor")
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("export_requires_committed_spool")
        recovery = store._connection.execute(
            "SELECT blocked_epochs_json FROM trace_collector_recovery "
            "WHERE node_id=? AND source_epoch=? AND export_generation=?",
            scope.values(),
        ).fetchone()
        blocked = json.loads(recovery[0]) if recovery else []
        recovery_epoch = blocked[-1] if blocked else None
        rows = export_page(
            store._connection,
            scope.values(),
            after=after,
            limit=BATCH_SIZE,
            states=("pending", "broker_acked") if replay else ("pending",),
            payload_required=True,
        )

    return [
        ExportRecord(
            scope,
            row["export_seq"],
            row["event_sha256"],
            validate_export(
                {
                    "schema_version": 1,
                    "node_id": scope.node_id,
                    "source_epoch": scope.source_epoch,
                    "export_generation": scope.export_generation,
                    "export_seq": row["export_seq"],
                    "event_sha256": row["event_sha256"],
                    "event": json.loads(row["event_json"]),
                }
            ),
            recovery_epoch=recovery_epoch,
        )
        for row in rows
    ]


def checkpoint_broker_ack(store: AgentdStore, record: ExportRecord) -> None:
    """A late ACK cannot resurrect a pruned row or overwrite settlement/loss."""
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("export_requires_committed_spool")
        with store._connection:
            store._connection.execute(
                "UPDATE trace_spool_all SET state='broker_acked' "
                "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=? "
                "AND event_sha256=? AND state='pending' AND journal_event_id IS NOT NULL",
                (*record.scope.values(), record.export_seq, record.event_sha256),
            )


class TraceExporter:
    """One source/generation worker; independent from task execution and retry."""

    def __init__(
        self, store: AgentdStore, js: JetStreamContext, scope: ExportScope
    ) -> None:
        self.store, self.js, self.scope = store, js, scope
        self.metrics = SourceMetrics()
        self.state = "idle"
        self.fault: str | None = None

    async def publish_batch(
        self,
        *,
        replay: bool = False,
        after: int = 0,
        stop: asyncio.Event | None = None,
    ) -> int:
        """Return last broker-acknowledged position; no rows returns the cursor."""
        batch = selected_batch(self.store, self.scope, replay=replay, after=after)
        last = after
        for record in batch:
            if stop is not None and stop.is_set():
                break
            self.metrics.note("publish_attempts")
            try:
                ack = await self.js.publish(
                    record.subject,
                    record.payload,
                    timeout=5,
                    headers={
                        "Nats-Msg-Id": record.message_id,
                        "Nats-Expected-Stream": STREAM_NAME,
                    },
                )
            except (nats_errors.Error, APIError, OSError, TimeoutError):
                self.metrics.note("publish_failures")
                raise
            if ack.stream != STREAM_NAME:
                self.metrics.note("invalid_broker_acknowledgments")
                raise TraceContractError("telemetry_ack_stream_mismatch")
            self.metrics.note("broker_acknowledgments")
            try:
                checkpoint_broker_ack(self.store, record)
            except (sqlite3.DatabaseError, TraceContractError):
                self.metrics.note("broker_ack_checkpoint_failures")
                raise
            last = record.export_seq
        return last

    async def run(
        self, stop: asyncio.Event, *, replay_requested: asyncio.Event | None = None
    ) -> None:
        """Retry transient failures at 1–30 seconds plus bounded jitter.

        Fixed local faults pause this scope until an explicit restart. This loop
        drains new pending rows; settlement owns scheduling of retained replay.
        """
        delay = 1.0
        replay_after: int | None = None
        try:
            while not stop.is_set():
                self.state, self.fault = "publishing", None
                if (
                    replay_after is None
                    and replay_requested is not None
                    and replay_requested.is_set()
                ):
                    replay_requested.clear()
                    replay_after = 0
                after = replay_after if replay_after is not None else 0
                try:
                    last = await self.publish_batch(
                        stop=stop, replay=replay_after is not None, after=after
                    )
                except (TraceContractError, json.JSONDecodeError, UnicodeError):
                    self.state, self.fault = "paused", "invalid_export_record"
                    return
                except (
                    nats_errors.AuthorizationError,
                    nats_errors.BadSubjectError,
                    nats_errors.MaxPayloadError,
                ):
                    self.state, self.fault = "paused", "telemetry_configuration_error"
                    return
                except (
                    nats_errors.Error,
                    APIError,
                    OSError,
                    sqlite3.OperationalError,
                ) as error:
                    if (
                        isinstance(error, APIError)
                        and error.code is not None
                        and 400 <= error.code < 500
                        and error.code not in (408, 429)
                    ):
                        self.state, self.fault = (
                            "paused",
                            "telemetry_configuration_error",
                        )
                        return
                    self.state, self.fault = "retrying", "telemetry_unavailable"
                    await self._wait(stop, delay + random.uniform(0, 0.25))
                    delay = min(delay * 2, 30.0)
                else:
                    delay = 1.0
                    if replay_after is not None:
                        replay_after = last if last != after else None
                    if last == after:
                        self.state = "idle"
                        await self._wait(stop, 1.0)
                    else:
                        await asyncio.sleep(0)
        finally:
            if self.state != "paused":
                self.state = "stopped"

    @staticmethod
    async def _wait(stop: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
