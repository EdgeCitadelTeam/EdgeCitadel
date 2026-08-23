"""Bidirectional runner/host trial-window timing handshake contracts."""

from __future__ import annotations

import asyncio
from pathlib import Path

from scripts.research.trial_timing import TrialWindowSignal


def test_runner_waits_for_host_acknowledgements_at_both_window_boundaries(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        signal = TrialWindowSignal(tmp_path)
        start = asyncio.create_task(signal.await_start_acknowledgement())
        for _ in range(10):
            if (tmp_path / "trial-window.start.ready").is_file():
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("runner did not declare start readiness")
        assert not start.done()
        (tmp_path / "trial-window.start.ack").write_text("ack\n")
        await start

        end = asyncio.create_task(signal.await_end_acknowledgement())
        for _ in range(10):
            if (tmp_path / "trial-window.end.ready").is_file():
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("runner did not declare end readiness")
        assert not end.done()
        (tmp_path / "trial-window.end.ack").write_text("ack\n")
        await end

    asyncio.run(exercise())
