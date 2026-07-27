"""Lifecycle contracts for the in-container workload execution harness."""

from __future__ import annotations

import asyncio
from time import perf_counter_ns

import pytest

from adapters._common.task_types import PublicationReceipt
from scripts.research.execution_harness import execute_cell
from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.modes.base import Mode, ObservedEnvelope
from scripts.research.workload_matrix import MatrixCell


class _Sink:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def emit(self, event: dict[str, object]) -> None:
        self.events.append(event)


class _Transport:
    def __init__(self) -> None:
        self.submissions: list[dict[str, object]] = []
        self.closed = False

    async def start_terminal_observer(self) -> None:
        return None

    async def submit_task(self, envelope: dict[str, object]) -> PublicationReceipt:
        self.submissions.append(envelope)
        return PublicationReceipt(
            envelope_id=str(envelope["id"]),
            accepted=True,
            transport="core-only",
            stream=None,
            stream_sequence=None,
            duplicate=False,
            accepted_ns=perf_counter_ns(),
            application_bytes=1,
            wire_bytes=1,
        )

    async def observe_terminal(
        self, task_id: str, timeout_s: float
    ) -> ObservedEnvelope:
        nonce = self.submissions[0]["payload"]["body"]
        return ObservedEnvelope(
            envelope={
                "id": "10000000-0000-4000-8000-000000000001",
                "task_id": task_id,
                "payload": {"body": f"edgecitadel:{nonce}"},
            },
            observed_ns=perf_counter_ns(),
            observation_index=1,
            stream_sequence=None,
            delivery_count=1,
            replayed=False,
            delivery=None,
        )

    async def inspect_state(self) -> dict[str, object]:
        return {"mode": "core-only"}

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_execute_cell_waits_for_fixture_readiness_and_always_closes_transport(
    tmp_path,
) -> None:
    transport = _Transport()
    sink = _Sink()
    config = NativeControlConfig(
        run_id="ec-test",
        agent_id="worker-1",
        mode="core-only",
        behavior="echo",
        delay_ms=0,
        crash_point=None,
        heartbeat_interval_ms=1000,
        outcome_db=str(tmp_path / "outcomes.sqlite"),
        side_effect_db=str(tmp_path / "effects.sqlite"),
    )

    async def fixture(_: object, __: object, event_sink: _Sink) -> None:
        event_sink.emit({"event": "fixture.ready", "data": {"agent_id": "worker-1"}})
        await asyncio.Event().wait()

    observation = await execute_cell(
        MatrixCell("W1", Mode.CORE_ONLY.value, "primary", "full-contract", 30),
        config,
        {"NATS_URL": "nats://nats:4222"},
        "a" * 64,
        {},
        sink,
        transport_factory=lambda *_: transport,
        fixture_runner=fixture,
    )

    assert observation.logical_terminals == 1
    assert transport.closed is True
    assert sink.events[0]["event"] == "fixture.ready"
