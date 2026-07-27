"""Own one in-container fixture and transport for a workload repetition."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Protocol

from adapters._common.task_publisher import EventSink
from scripts.research.fixtures.native_control import (
    NativeControlConfig,
    TransportFactory,
    build_transport,
    run_fixture,
)
from scripts.research.modes.base import TaskTransport
from scripts.research.workload_matrix import MatrixCell, TrialObservation, run_cell


class _FixtureRunner(Protocol):
    async def __call__(
        self,
        config: NativeControlConfig,
        transport: TaskTransport,
        event_sink: EventSink,
    ) -> None: ...


class _EventBuffer(EventSink, Protocol):
    events: list[Mapping[str, object]]


def _fixture_ready(event_sink: _EventBuffer, agent_id: str) -> bool:
    for event in event_sink.events:
        data = event.get("data")
        if (
            event.get("event") == "fixture.ready"
            and isinstance(data, Mapping)
            and data.get("agent_id") == agent_id
        ):
            return True
    return False


async def _wait_fixture_ready(event_sink: _EventBuffer, agent_id: str) -> None:
    for _ in range(100):
        if _fixture_ready(event_sink, agent_id):
            return
        await asyncio.sleep(0.01)
    raise RuntimeError("fixture readiness timed out")


async def execute_cell(
    cell: MatrixCell,
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
    observers: object,
    event_sink: _EventBuffer,
    *,
    transport_factory: TransportFactory = build_transport,
    fixture_runner: _FixtureRunner = run_fixture,
) -> TrialObservation:
    """Run one declared cell with a ready native fixture, then close everything."""
    if cell.mode != config.mode:
        raise ValueError("cell mode does not match fixture")
    transport = transport_factory(config, endpoints, token, event_sink)
    fixture_task = asyncio.create_task(fixture_runner(config, transport, event_sink))
    try:
        await _wait_fixture_ready(event_sink, config.agent_id)
        return await run_cell(
            cell,
            transport,
            {"sender_id": "requester-1", "worker_id": config.agent_id},
            observers,
            event_sink,
        )
    finally:
        fixture_task.cancel()
        try:
            await fixture_task
        except asyncio.CancelledError:
            pass
        await transport.close()


__all__ = ["execute_cell"]
