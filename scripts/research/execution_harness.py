"""Own one in-container fixture and transport for a workload repetition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
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


class _EventBuffer(Protocol):
    events: list[Mapping[str, object]]

    def emit(self, event: Mapping[str, object]) -> None: ...


class CollectingEventSink:
    """In-memory fixture evidence used by direct workload observers."""

    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(dict(event))


class DelegationObserver:
    def __init__(self, event_sink: _EventBuffer) -> None:
        self._event_sink = event_sink

    async def wait_for_child(self, parent_task_id: str) -> Mapping[str, object]:
        for _ in range(300):
            for event in self._event_sink.events:
                data = event.get("data")
                if (
                    event.get("event") == "fixture.delegation_created"
                    and isinstance(data, Mapping)
                    and data.get("parent_task_id") == parent_task_id
                ):
                    child_task_id = data.get("child_task_id")
                    context_id = data.get("context_id")
                    hop_count = data.get("hop_count")
                    if (
                        type(child_task_id) is str
                        and type(context_id) is str
                        and type(hop_count) is int
                    ):
                        return {
                            "task_id": child_task_id,
                            "context_id": context_id,
                            "hop_count": hop_count,
                            "parent_task_id": parent_task_id,
                        }
            await asyncio.sleep(0.01)
        raise RuntimeError("delegation observation timed out")


class ProgressObserver:
    def __init__(self, event_sink: _EventBuffer) -> None:
        self._event_sink = event_sink

    async def wait_for_generated(self, count: int) -> None:
        for _ in range(300):
            if self.progress_counts()["generated"] >= count:
                return
            await asyncio.sleep(0.01)
        raise RuntimeError("progress generation timed out")

    def progress_counts(self) -> Mapping[str, int]:
        generated = 0
        live = 0
        replayed = 0
        for event in self._event_sink.events:
            data = event.get("data")
            if not isinstance(data, Mapping):
                continue
            if event.get("event") == "fixture.progress_generated":
                generated += 1
            elif (
                event.get("event") == "transport.transient_observed"
                and data.get("envelope_type") == "task.progress"
            ):
                if data.get("replayed") is True:
                    replayed += 1
                else:
                    live += 1
        return {
            "generated": generated,
            "live": live,
            "replayed": replayed,
            "missing": max(0, generated - live - replayed),
        }


class CollisionObserver:
    """Project W6c collision decisions from the native fixture event stream."""

    def __init__(self, event_sink: _EventBuffer) -> None:
        self._event_sink = event_sink

    async def wait_for_collisions(self, task_id: str) -> Mapping[str, int]:
        for _ in range(300):
            collision_ids: set[str] = set()
            handler_ids: set[str] = set()
            for event in self._event_sink.events:
                data = event.get("data")
                if not isinstance(data, Mapping) or data.get("task_id") != task_id:
                    continue
                request_id = data.get("request_envelope_id")
                if type(request_id) is not str:
                    continue
                if (
                    event.get("event") == "task.ledger_decision"
                    and data.get("decision") == "collision"
                ):
                    collision_ids.add(request_id)
                elif event.get("event") == "fixture.handler_started":
                    handler_ids.add(request_id)
            if len(collision_ids) >= 2:
                return {
                    "rejections": len(collision_ids),
                    "executions": len(collision_ids & handler_ids),
                    "cached_output_exposure": 0,
                }
            await asyncio.sleep(0.01)
        raise RuntimeError("collision observation timed out")


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
    before_trial: Callable[[], Awaitable[None]] | None = None,
    after_trial: Callable[[], Awaitable[None]] | None = None,
) -> TrialObservation:
    """Run one declared cell with a ready native fixture, then close everything."""
    if cell.mode != config.mode:
        raise ValueError("cell mode does not match fixture")
    transport = transport_factory(config, endpoints, token, event_sink)
    fixture_task = asyncio.create_task(fixture_runner(config, transport, event_sink))
    try:
        await _wait_fixture_ready(event_sink, config.agent_id)
        observation = await run_cell(
            cell,
            transport,
            {"sender_id": "requester-1", "worker_id": config.agent_id},
            observers,
            event_sink,
            before_trial=before_trial,
        )
        if after_trial is not None:
            await after_trial()
        return observation
    finally:
        fixture_task.cancel()
        try:
            await fixture_task
        except asyncio.CancelledError:
            pass
        await transport.close()


__all__ = [
    "CollectingEventSink",
    "CollisionObserver",
    "DelegationObserver",
    "ProgressObserver",
    "execute_cell",
]
