"""Direct runner configuration contracts."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.research import in_container_runner
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
    assert seen["observer_agent_id"] == "observer-1"
