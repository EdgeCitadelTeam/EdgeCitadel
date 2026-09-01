"""Shared fixtures for Hermes Plugin tests."""

from __future__ import annotations
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


PLUGIN_ROOT = Path(__file__).parents[3] / "plugins" / "hermes"
if str(PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(PLUGIN_ROOT))


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def make_command(
    *,
    body: str = "hello",
    task_id: str | None = None,
    context_id: str | None = None,
    sender_id: str = "test-sender",
) -> dict:
    return {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "command",
        "sender_id": sender_id,
        "task_id": task_id or str(uuid.uuid4()),
        "context_id": context_id or str(uuid.uuid4()),
        "timestamp": _now_iso(),
        "payload": {"body": body},
    }


class FakeContext:
    """Stand-in for edgecitadel_plugin_runtime.pull_consumer.Context."""

    def __init__(self, agent_id: str = "us-mac-hermes"):
        self.agent_id = agent_id
        self.nc = MagicMock()
        self.nc.publish = AsyncMock()
        # Make request a proper AsyncMock returning a Msg-shaped reply,
        # so any handler that uses request-reply against memory.turns.* gets
        # tracked by the regression guard.
        reply_msg = MagicMock()
        reply_msg.data = b"{}"
        self.nc.request = AsyncMock(return_value=reply_msg)
        self.js = MagicMock()
        self.msg = MagicMock()
        self.msg.in_progress = AsyncMock()
        self.progress_calls: list[dict[str, Any]] = []

    async def in_progress(self) -> None:
        await self.msg.in_progress()

    async def publish_progress(
        self,
        task_id: str,
        *,
        body: str = "",
        progress: int | None = None,
        extra: dict | None = None,
    ) -> None:
        self.progress_calls.append(
            {
                "task_id": task_id,
                "body": body,
                "progress": progress,
                "extra": extra or {},
            }
        )


@pytest.fixture
def fake_ctx():
    return FakeContext()


@pytest.fixture
def cmd():
    return make_command
