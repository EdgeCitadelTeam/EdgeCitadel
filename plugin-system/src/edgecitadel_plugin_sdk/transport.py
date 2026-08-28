"""Transport-neutral message values and delivery contract."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class TransportMessage:
    """A portable message exchanged by plugin agents."""

    sender_id: str
    recipient_id: str | None
    message_type: str
    payload: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@runtime_checkable
class Transport(Protocol):
    """Registration and message delivery without a concrete transport."""

    async def register(self, agent_id: str) -> None: ...

    def receive(self, agent_id: str) -> AsyncIterator[TransportMessage]: ...

    async def publish(self, message: TransportMessage) -> None: ...

    async def drain(self) -> None: ...
