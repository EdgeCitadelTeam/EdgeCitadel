"""Transport-neutral message values and delivery contract."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ._values import _freeze_mapping


@dataclass(frozen=True)
class TransportMessage:
    """A portable message exchanged by plugin agents."""

    v: int
    id: str
    type: str
    sender_id: str
    timestamp: str
    payload: Mapping[str, object]
    recipient_id: str | None = None
    task_id: str | None = None
    context_id: str | None = None
    task_state: str | None = None
    agent_state: str | None = None
    hop_count: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", _freeze_mapping(self.payload))


@runtime_checkable
class Transport(Protocol):
    """Registration and message delivery without a concrete transport."""

    async def register(self, agent_id: str) -> None: ...

    def receive(self, agent_id: str) -> AsyncIterator[TransportMessage]: ...

    async def publish(self, message: TransportMessage) -> None: ...

    async def drain(self) -> None: ...
