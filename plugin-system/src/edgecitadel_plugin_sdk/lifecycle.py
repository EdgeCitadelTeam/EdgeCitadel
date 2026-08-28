"""Portable plugin lifecycle values and hooks."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Protocol, runtime_checkable


class LifecycleState(str, Enum):
    """Reserved supervisor lifecycle vocabulary."""

    DISCOVERED = "discovered"
    VALIDATED = "validated"
    INSTALLED = "installed"
    STARTING = "starting"
    READY = "ready"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True)
class LifecycleTransition:
    """A requested or completed plugin lifecycle transition."""

    plugin_id: str
    previous_state: LifecycleState
    next_state: LifecycleState
    detail: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if self.detail is not None:
            object.__setattr__(self, "detail", MappingProxyType(dict(self.detail)))


@runtime_checkable
class LifecycleHooks(Protocol):
    """Callbacks surrounding a plugin lifecycle transition."""

    async def before_transition(self, transition: LifecycleTransition) -> None: ...

    async def after_transition(self, transition: LifecycleTransition) -> None: ...
