"""Framework-neutral agent runtime contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class RuntimeContext:
    """Portable initialization values supplied to an agent runtime."""

    plugin_id: str
    agent_id: str
    configuration: Mapping[str, object]
    metadata: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "configuration", MappingProxyType(dict(self.configuration))
        )
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))


@runtime_checkable
class AgentRuntime(Protocol):
    """Lifecycle and message-handling surface implemented by an agent runtime."""

    async def initialize(self, context: RuntimeContext) -> None: ...

    async def handle(self, message: Mapping[str, object]) -> Mapping[str, object]: ...

    async def drain(self) -> None: ...

    async def shutdown(self) -> None: ...
