"""Injected transport publishers for task execution."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from edgecitadel_plugin_runtime.task_types import PublicationReceipt


class TerminalPublisher(Protocol):
    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...


class ProgressPublisher(Protocol):
    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt: ...


class EventSink(Protocol):
    def emit(self, event: Mapping[str, object]) -> None: ...


__all__ = ["EventSink", "ProgressPublisher", "TerminalPublisher"]
