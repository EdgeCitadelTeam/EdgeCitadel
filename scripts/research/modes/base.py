"""Transport-neutral contract for benchmark modes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from edgecitadel_plugin_runtime.task_publisher import EventSink
from edgecitadel_plugin_runtime.task_types import PublicationReceipt

if TYPE_CHECKING:
    from edgecitadel_plugin_runtime.task_executor import TaskExecutor


class Mode(str, Enum):
    CENTRAL_RELAY = "central-relay"
    CORE_ONLY = "core-only"
    EDGECITADEL = "edgecitadel"
    ALL_DURABLE = "all-durable"


class ObserverDelivery(Protocol):
    async def ack(self) -> None: ...


@dataclass(frozen=True)
class ObservedEnvelope:
    envelope: Mapping[str, object]
    observed_ns: int
    observation_index: int
    stream_sequence: int | None
    delivery_count: int
    replayed: bool
    delivery: ObserverDelivery | None


@dataclass(frozen=True)
class TransportSnapshot:
    mode: Mode
    streams: Mapping[str, Mapping[str, object]]
    consumers: Mapping[str, Mapping[str, object]]
    pending: int | None
    ack_pending: int | None
    connection_bytes: Mapping[str, int]
    storage_bytes: int
    message_count: int


class FaultController(Protocol):
    async def disconnect_progress_observer(self) -> None: ...

    async def reconnect_progress_observer(self) -> None: ...

    async def stop_worker(self, agent_id: str) -> None: ...

    async def start_worker(self, agent_id: str) -> None: ...

    async def restart_coordinator(self) -> None: ...


class TaskTransport(Protocol):
    @property
    def faults(self) -> FaultController: ...

    @property
    def mode(self) -> Mode: ...

    @property
    def outcome_ledger_enabled(self) -> bool: ...

    async def start_terminal_observer(self) -> None: ...

    async def start_progress_observer(self) -> None: ...

    async def start_receiver(
        self,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None: ...

    async def wait_receiver_ready(
        self,
        agent_id: str,
        timeout_s: float,
    ) -> None: ...

    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...

    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...

    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...

    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None: ...

    async def inspect_state(self) -> TransportSnapshot: ...

    async def close(self) -> None: ...


__all__ = [
    "EventSink",
    "FaultController",
    "Mode",
    "ObservedEnvelope",
    "ObserverDelivery",
    "PublicationReceipt",
    "TaskTransport",
    "TransportSnapshot",
]
