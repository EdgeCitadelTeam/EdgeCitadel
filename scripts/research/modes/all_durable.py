"""All-durable benchmark transport."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone

from nats.aio.client import Client as NATS

import nats
from scripts.research.modes.base import EventSink, Mode
from scripts.research.modes.edgecitadel import (
    AsyncSleep,
    CoordinatorRestart,
    WorkerOperation,
    _JetStreamTransport,
)


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _uuid4() -> str:
    return str(uuid.uuid4()).lower()


class AllDurableTransport(_JetStreamTransport):
    def __init__(
        self,
        *,
        nats_url: str,
        run_id: str,
        token: str,
        event_sink: EventSink,
        agent_card: Mapping[str, object] | None = None,
        observer_agent_id: str | None = None,
        coordinator_restart: CoordinatorRestart | None = None,
        worker_stop: WorkerOperation | None = None,
        worker_start: WorkerOperation | None = None,
        connection_factory: Callable[..., Awaitable[NATS]] = nats.connect,
        evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
        epoch_now: Callable[[], str] = _now_iso,
        uuid4: Callable[[], str] = _uuid4,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        super().__init__(
            nats_url=nats_url,
            run_id=run_id,
            token=token,
            event_sink=event_sink,
            agent_card=agent_card,
            observer_agent_id=observer_agent_id,
            mode=Mode.ALL_DURABLE,
            ablation="full-contract",
            nats_msg_id=True,
            outcome_ledger=True,
            durable_transients=True,
            coordinator_restart=coordinator_restart,
            worker_stop=worker_stop,
            worker_start=worker_start,
            connection_factory=connection_factory,
            evidence_clock_ns=evidence_clock_ns,
            epoch_now=epoch_now,
            uuid4=uuid4,
            sleep=sleep,
        )


__all__ = ["AllDurableTransport"]
