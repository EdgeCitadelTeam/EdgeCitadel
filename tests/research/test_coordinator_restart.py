"""Run-owned coordinator restart handshake contracts."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from scripts.research.coordinator_restart import (
    acknowledge_restart,
    request_restart,
    wait_for_restart_request,
)


def test_runner_request_waits_for_host_acknowledgement(tmp_path: Path) -> None:
    async def exercise() -> None:
        requested = asyncio.create_task(request_restart(tmp_path, timeout_seconds=1))

        await asyncio.to_thread(wait_for_restart_request, tmp_path, 1)
        acknowledge_restart(tmp_path)

        await requested

    asyncio.run(exercise())


def test_wait_rejects_a_malformed_request(tmp_path: Path) -> None:
    (tmp_path / "coordinator-restart.request").write_text("unexpected\n")

    with pytest.raises(RuntimeError, match="invalid coordinator restart request"):
        wait_for_restart_request(tmp_path, 0.1)
