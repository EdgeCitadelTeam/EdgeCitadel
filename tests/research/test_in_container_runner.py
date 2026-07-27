"""Direct runner configuration contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.research.execution_harness import CollectingEventSink
from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.in_container_runner import prepare_direct_execution
from scripts.research.modes.base import Mode
from scripts.research.workload_matrix import MatrixCell


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


@pytest.mark.parametrize("workload", ("W5", "W6b", "W6c", "W7", "W8"))
def test_direct_runner_refuses_workloads_requiring_external_lifecycle(
    tmp_path: Path,
    workload: str,
) -> None:
    cell = MatrixCell(workload, Mode.CORE_ONLY.value, "primary", "full-contract", 30)

    with pytest.raises(ValueError, match="external worker lifecycle"):
        prepare_direct_execution(cell, _config(tmp_path), CollectingEventSink())
