"""Direct runner configuration contracts."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from scripts.research import in_container_runner
from scripts.research.execution_harness import CollectingEventSink
from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.in_container_runner import (
    _argument_parser,
    prepare_direct_execution,
)
from scripts.research.modes.base import Mode
from scripts.research.workload_matrix import MatrixCell, TrialObservation


def _config(tmp_path: Path) -> NativeControlConfig:
    return NativeControlConfig(
        run_id="ec-test",
        agent_id="worker-1",
        mode=Mode.CORE_ONLY.value,
        behavior="echo",
        delay_ms=0,
        crash_point=None,
        heartbeat_interval_ms=1000,
        outcome_db=str(tmp_path / "outcomes.sqlite"),
        side_effect_db=str(tmp_path / "effects.sqlite"),
    )


@pytest.mark.parametrize(
    ("workload", "behavior", "observer"),
    (
        ("W1", "echo", None),
        ("W2", "delegate", "delegation"),
        ("W3", "progress", "progress"),
        ("W4", "echo", None),
        ("W6a", "echo", None),
        ("W6c", "echo", "collision"),
        ("W7", "echo", None),
    ),
)
def test_direct_runner_configures_only_same_process_workloads(
    tmp_path: Path,
    workload: str,
    behavior: str,
    observer: str | None,
) -> None:
    cell = MatrixCell(workload, Mode.CORE_ONLY.value, "primary", "full-contract", 30)
    configured, observers = prepare_direct_execution(
        cell, _config(tmp_path), CollectingEventSink()
    )

    assert configured.behavior == behavior
    assert set(observers) == ({observer} if observer else set())


@pytest.mark.parametrize("workload", ("W5", "W6b", "W8"))
def test_direct_runner_refuses_workloads_requiring_external_lifecycle(
    tmp_path: Path,
    workload: str,
) -> None:
    cell = MatrixCell(workload, Mode.CORE_ONLY.value, "primary", "full-contract", 30)

    with pytest.raises(ValueError, match="external worker lifecycle"):
        prepare_direct_execution(cell, _config(tmp_path), CollectingEventSink())


@pytest.mark.parametrize("workload", ("W5", "W6b", "W8"))
def test_runner_accepts_supervised_external_fixture_workloads(workload: str) -> None:
    assert (
        _argument_parser()
        .parse_args(["--config", "/state/native-control.json", "--workload", workload])
        .workload
        == workload
    )


def test_runner_uses_the_workload_specific_semantic_retry_timeout() -> None:
    assert (
        _argument_parser()
        .parse_args(["--config", "/state/native-control.json", "--workload", "W6b"])
        .timeout_seconds
        is None
    )


def test_runner_reports_caught_execution_errors(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        asyncio.run(
            in_container_runner._main(
                ("--config", "/missing/native-control.json", "--workload", "W1"),
                {},
            )
        )
        == 2
    )
    assert capsys.readouterr().err == "runner failed: invalid config file\n"


@pytest.mark.parametrize("mode", (Mode.EDGECITADEL.value, Mode.ALL_DURABLE.value))
def test_direct_runner_assigns_the_durable_observer_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    seen: dict[str, object] = {}

    class Transport:
        pass

    def transport_factory(**kwargs: object) -> Transport:
        seen.update(kwargs)
        return Transport()

    if mode == Mode.EDGECITADEL.value:
        monkeypatch.setattr(
            "scripts.research.modes.edgecitadel.EdgeCitadelTransport",
            transport_factory,
        )
    else:
        monkeypatch.setattr(
            "scripts.research.modes.all_durable.AllDurableTransport",
            transport_factory,
        )
    config = _config(tmp_path)
    object.__setattr__(config, "mode", mode)

    transport = in_container_runner._build_direct_transport(
        config,
        {"NATS_URL": "nats://nats:4222"},
        "a" * 64,
        CollectingEventSink(),
    )

    assert isinstance(transport, Transport)
    assert seen["observer_agent_id"] == "requester-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workload", "handler_events", "reported_executions", "expected"),
    (
        ("W1", 1, None, 1),
        ("W2", 2, None, 2),
        ("W6c", 1, 0, 0),
    ),
)
async def test_direct_runner_projects_observed_handler_execution_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    workload: str,
    handler_events: int,
    reported_executions: int | None,
    expected: int,
) -> None:
    async def execute_cell(
        _cell: object,
        _config: object,
        _endpoints: object,
        _token: object,
        _observers: object,
        event_sink: CollectingEventSink,
        **_: object,
    ) -> TrialObservation:
        for index in range(handler_events):
            event_sink.emit(
                {
                    "event": "fixture.handler_started",
                    "data": {"request_envelope_id": f"request-{index}"},
                }
            )
        return TrialObservation(
            initiated=1,
            accepted=1,
            delivered=1,
            handler_attempts=None,
            executions=reported_executions,
            side_effects=None,
            prepared_outcomes=None,
            logical_terminals=1,
            distinct_terminal_ids=1,
            publication_attempts=1,
            wire_deliveries=1,
            progress_generated=None,
            progress_live_delivered=None,
            progress_replay_delivered=None,
            progress_missing=None,
            poison=None,
            inapplicable_crash_points=(),
            timed_out=False,
            final_transport={},
        )

    monkeypatch.setattr(in_container_runner, "execute_cell", execute_cell)
    cell = MatrixCell(
        workload,
        Mode.CORE_ONLY.value,
        "primary",
        "full-contract",
        30,
    )

    observation, _ = await in_container_runner.run_direct_cell(
        cell,
        _config(tmp_path),
        {"NATS_URL": "nats://nats:4222"},
        "a" * 64,
    )

    assert observation.executions == expected
