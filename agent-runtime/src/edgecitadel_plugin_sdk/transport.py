"""Transport-neutral message values and delivery contract."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ._values import _freeze_mapping, _thaw_value


@dataclass(frozen=True)
class TransportMessage:
    """A portable canonical envelope exchanged by plugin agents.

    Payload values are expected to be JSON-shaped. Mapping/list/tuple trees are
    recursively snapshotted; arbitrary object graphs are outside the portable
    contract.
    """

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

    def to_mapping(self) -> dict[str, object]:
        """Return an independent canonical wire mapping without validation."""
        message: dict[str, object] = {
            "v": self.v,
            "id": self.id,
            "type": self.type,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "payload": _thaw_value(self.payload),
        }
        optional_values: tuple[tuple[str, object | None], ...] = (
            ("recipient_id", self.recipient_id),
            ("task_id", self.task_id),
            ("context_id", self.context_id),
            ("task_state", self.task_state),
            ("agent_state", self.agent_state),
            ("hop_count", self.hop_count),
        )
        for name, value in optional_values:
            if value is not None:
                message[name] = value
        return message


@runtime_checkable
class Transport(Protocol):
    """Registration and message delivery without a concrete transport."""

    async def register(self, agent_id: str) -> None: ...

    def receive(self, agent_id: str) -> AsyncIterator[TransportMessage]: ...

    async def publish(self, message: TransportMessage) -> None: ...

    async def drain(self) -> None: ...
