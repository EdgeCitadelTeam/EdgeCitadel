"""External worker control-file and durable evidence contracts."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from adapters._common.task_types import PublicationReceipt
from scripts.research.external_lifecycle import (
    ExternalActuatorObserver,
    ExternalCrashObserver,
    ExternalWorkerLifecycle,
)
from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.workload_matrix import CrashPoint


def _config(tmp_path: Path) -> NativeControlConfig:
    return NativeControlConfig(
        run_id="ec-test",
        agent_id="worker-1",
        mode="core-only",
        behavior="echo",
        delay_ms=0,
        crash_point=None,
        heartbeat_interval_ms=1000,
        outcome_db=str(tmp_path / "state/outcomes.sqlite"),
        side_effect_db=str(tmp_path / "state/effects.sqlite"),
    )


@pytest.mark.asyncio
async def test_activate_waits_for_the_exact_written_generation(tmp_path: Path) -> None:
    control = tmp_path / "control"
    state = tmp_path / "state"
    control.mkdir()
    state.mkdir()
    config = _config(tmp_path)
    expected = hashlib.sha256(
        json.dumps(asdict(config), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    async def mark_ready() -> None:
        active = control / "active-native-control.json"
        while not active.exists():
            await asyncio.sleep(0.01)
        (control / "worker-status.txt").write_text(
            f"status=running\npid=1\nexit_code=\ngeneration={expected}\n"
        )
        (state / "worker-events.jsonl").write_text(
            json.dumps({"event": "fixture.ready", "data": {"agent_id": "worker-1"}})
            + "\n"
        )

    marker = asyncio.create_task(mark_ready())
    lifecycle = ExternalWorkerLifecycle(control, state)
    assert await lifecycle.activate(config, 1) == expected
    await marker


@pytest.mark.asyncio
async def test_wait_for_exit_requires_matching_generation(tmp_path: Path) -> None:
    control = tmp_path / "control"
    state = tmp_path / "state"
    control.mkdir()
    state.mkdir()
    generation = "a" * 64
    (control / "worker-status.txt").write_text(
        f"status=exited\npid=1\nexit_code=86\ngeneration={generation}\n"
    )

    assert (
        await ExternalWorkerLifecycle(control, state).wait_for_exit(generation, 1) == 86
    )


def test_task_events_only_return_the_selected_task(tmp_path: Path) -> None:
    control = tmp_path / "control"
    state = tmp_path / "state"
    control.mkdir()
    state.mkdir()
    (state / "worker-events.jsonl").write_text(
        "\n".join(
            (
                json.dumps(
                    {"event": "fixture.handler_started", "data": {"task_id": "a"}}
                ),
                json.dumps(
                    {"event": "fixture.handler_started", "data": {"task_id": "b"}}
                ),
            )
        )
        + "\n"
    )

    events = ExternalWorkerLifecycle(control, state).task_events("b")
    assert len(events) == 1
    assert events[0]["data"] == {"task_id": "b"}


class _Lifecycle:
    async def activate(self, config: NativeControlConfig, timeout_s: float) -> str:
        assert config.behavior == "actuator"
        assert config.crash_point == CrashPoint.AFTER_SIDE_EFFECT.value
        assert timeout_s == 1
        return "a" * 64

    async def wait_for_exit(self, generation: str, timeout_s: float) -> int:
        assert generation == "a" * 64
        assert timeout_s == 1
        return 86

    def task_events(self, task_id: str) -> tuple[dict[str, object], ...]:
        assert task_id == "task-1"
        return (
            {"event": "fixture.handler_started", "data": {"task_id": task_id}},
            {"event": "fixture.side_effect_committed", "data": {"task_id": task_id}},
        )


class _Transport:
    def __init__(self) -> None:
        self.submissions: list[object] = []
        self.terminal: object | None = None

    async def submit_task(self, envelope: object) -> PublicationReceipt:
        assert isinstance(envelope, dict)
        self.submissions.append(envelope)
        return PublicationReceipt(
            envelope_id="wire-1",
            accepted=True,
            transport="test",
            stream=None,
            stream_sequence=None,
            duplicate=None,
            accepted_ns=1,
            application_bytes=1,
            wire_bytes=1,
        )

    async def observe_terminal(self, task_id: str, timeout_s: float) -> object | None:
        assert task_id == "task-1"
        assert timeout_s in (1, 30)
        return self.terminal


@pytest.mark.asyncio
async def test_crash_observer_records_external_crash_evidence(tmp_path: Path) -> None:
    transport = _Transport()
    observer = ExternalCrashObserver(
        cast(ExternalWorkerLifecycle, _Lifecycle()),
        _config(tmp_path),
        transport,
    )

    result = await observer.run_crash_subtrial(
        CrashPoint.AFTER_SIDE_EFFECT,
        {"task_id": "task-1"},
        1,
    )

    assert result == {
        "applicability": "applicable",
        "accepted": 1,
        "delivered": 1,
        "executions": 1,
        "side_effects": 1,
        "logical_terminals": 0,
        "distinct_terminal_ids": 0,
        "publication_attempts": 1,
        "wire_deliveries": 0,
        "poison": 0,
        "timed_out": False,
    }
    assert transport.submissions == [{"task_id": "task-1"}]


@pytest.mark.asyncio
async def test_crash_observer_counts_mapping_terminal_ids(tmp_path: Path) -> None:
    transport = _Transport()
    transport.terminal = SimpleNamespace(envelope={"id": "terminal-1"})
    observer = ExternalCrashObserver(
        cast(ExternalWorkerLifecycle, _Lifecycle()),
        _config(tmp_path),
        transport,
    )

    result = await observer.run_crash_subtrial(
        CrashPoint.AFTER_SIDE_EFFECT,
        {"task_id": "task-1"},
        1,
    )

    assert result["logical_terminals"] == 1
    assert result["distinct_terminal_ids"] == 1
    assert result["wire_deliveries"] == 1


class _ActuatorLifecycle:
    def __init__(self) -> None:
        self.configurations: list[NativeControlConfig] = []

    async def activate(self, config: NativeControlConfig, timeout_s: float) -> str:
        assert timeout_s == 30
        self.configurations.append(config)
        return "a" * 64 if len(self.configurations) == 1 else "b" * 64

    async def wait_for_exit(self, generation: str, timeout_s: float) -> int:
        assert generation == "a" * 64
        assert timeout_s == 30
        return 86

    def task_events(self, task_id: str) -> tuple[dict[str, object], ...]:
        assert task_id == "task-1"
        return (
            {"event": "fixture.handler_started", "data": {"task_id": task_id}},
            {"event": "fixture.side_effect_committed", "data": {"task_id": task_id}},
            {"event": "fixture.handler_started", "data": {"task_id": task_id}},
            {"event": "fixture.side_effect_committed", "data": {"task_id": task_id}},
            {"event": "task.ledger_decision", "data": {"task_id": task_id}},
        )


@pytest.mark.asyncio
async def test_actuator_observer_recovers_core_after_a_crashed_side_effect(
    tmp_path: Path,
) -> None:
    lifecycle = _ActuatorLifecycle()
    transport = _Transport()
    transport.terminal = SimpleNamespace(envelope={"id": "terminal-1"})
    observer = ExternalActuatorObserver(
        cast(ExternalWorkerLifecycle, lifecycle),
        _config(tmp_path),
        transport,
    )

    await observer.prepare(30)
    await observer.record_submission({"id": "wire-1", "task_id": "task-1"})
    result = await observer.wait_for_actuator_outcome("task-1")

    assert [config.crash_point for config in lifecycle.configurations] == [
        CrashPoint.AFTER_SIDE_EFFECT.value,
        None,
    ]
    assert all(config.behavior == "actuator" for config in lifecycle.configurations)
    assert transport.submissions == [{"id": "wire-1", "task_id": "task-1"}]
    assert result == {
        "handler_attempts": 2,
        "delivered": 2,
        "side_effects": 2,
        "prepared_outcomes": 0,
        "logical_terminals": 1,
        "distinct_terminal_ids": 1,
        "publication_attempts": 1,
        "wire_deliveries": 1,
        "poison": 0,
        "timed_out": False,
        "crash_point": CrashPoint.AFTER_SIDE_EFFECT.value,
    }
