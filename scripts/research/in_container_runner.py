"""Execute directly controlled workload cells inside an artifact runner container."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, replace
from pathlib import Path
from typing import cast

from edgecitadel_plugin_runtime.task_publisher import EventSink
from scripts.research.coordinator_restart import request_restart
from scripts.research.execution_harness import (
    CollectingEventSink,
    CollisionObserver,
    DelegationObserver,
    ProgressObserver,
    execute_cell,
)
from scripts.research.external_lifecycle import (
    ExternalActuatorObserver,
    ExternalCrashObserver,
    ExternalSemanticRetryObserver,
    ExternalWorkerLifecycle,
)
from scripts.research.fixtures.native_control import (
    NativeControlConfig,
    build_agent_card,
    load_native_config,
    read_transport_token,
    runtime_endpoints,
)
from scripts.research.modes.base import Mode, TaskTransport
from scripts.research.trial_timing import TrialWindowSignal
from scripts.research.workload_matrix import (
    MatrixCell,
    TrialObservation,
    run_cell,
    workload_timeout_seconds,
)

_DIRECT_WORKLOADS = frozenset({"W1", "W2", "W3", "W4", "W6a", "W6c", "W7"})
_EXTERNAL_WORKLOADS = frozenset({"W5", "W6b", "W8"})
_RUNNER_WORKLOADS = _DIRECT_WORKLOADS | _EXTERNAL_WORKLOADS
_BEHAVIORS = {
    "W1": "echo",
    "W2": "delegate",
    "W3": "progress",
    "W4": "echo",
    "W6a": "echo",
    "W6c": "echo",
    "W7": "echo",
}
_REQUESTER_AGENT_ID = "requester-1"


def _coordinator_restart_callback(
    timeout_seconds: int,
) -> Callable[[], Awaitable[str | None]]:
    async def restart() -> str | None:
        await request_restart(Path("/control"), timeout_seconds)
        return None

    return restart


def _build_direct_transport(
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
    event_sink: EventSink,
    coordinator_restart: Callable[[], Awaitable[str | None]] | None = None,
) -> TaskTransport:
    if config.mode == Mode.EDGECITADEL.value:
        from scripts.research.modes.edgecitadel import EdgeCitadelTransport

        return EdgeCitadelTransport(
            nats_url=endpoints["NATS_URL"],
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            agent_card=build_agent_card(config),
            observer_agent_id=_REQUESTER_AGENT_ID,
            coordinator_restart=coordinator_restart,
        )
    if config.mode == Mode.ALL_DURABLE.value:
        from scripts.research.modes.all_durable import AllDurableTransport

        return AllDurableTransport(
            nats_url=endpoints["NATS_URL"],
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            agent_card=build_agent_card(config),
            observer_agent_id=_REQUESTER_AGENT_ID,
            coordinator_restart=coordinator_restart,
        )
    if config.mode == Mode.CENTRAL_RELAY.value:
        from scripts.research.modes.central_relay import CentralRelayTransport

        return CentralRelayTransport(
            relay_url=endpoints["RELAY_URL"],
            run_id=config.run_id,
            token=token,
            event_sink=event_sink,
            coordinator_restart=coordinator_restart,
        )
    from scripts.research.modes.core_nats import CoreNatsTransport

    return CoreNatsTransport(
        nats_url=endpoints["NATS_URL"],
        run_id=config.run_id,
        token=token,
        event_sink=event_sink,
        agent_card=build_agent_card(config),
        coordinator_restart=coordinator_restart,
    )


def prepare_direct_execution(
    cell: MatrixCell,
    config: NativeControlConfig,
    event_sink: CollectingEventSink,
) -> tuple[NativeControlConfig, Mapping[str, object]]:
    """Configure only workloads whose worker lifecycle stays in this process."""
    if cell.mode != config.mode:
        raise ValueError("cell mode does not match config")
    if cell.workload not in _DIRECT_WORKLOADS:
        raise ValueError("workload requires external worker lifecycle")
    configured = replace(config, behavior=_BEHAVIORS[cell.workload])
    observers: dict[str, object] = {}
    if cell.workload == "W2":
        observers["delegation"] = DelegationObserver(event_sink)
    elif cell.workload == "W3":
        observers["progress"] = ProgressObserver(event_sink)
    elif cell.workload == "W6c":
        observers["collision"] = CollisionObserver(event_sink)
    return configured, observers


async def run_direct_cell(
    cell: MatrixCell,
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
    trial_window: TrialWindowSignal | None = None,
) -> tuple[TrialObservation, tuple[Mapping[str, object], ...]]:
    event_sink = CollectingEventSink()
    configured, observers = prepare_direct_execution(cell, config, event_sink)
    coordinator_restart = (
        _coordinator_restart_callback(cell.timeout_seconds)
        if cell.workload == "W7"
        else None
    )
    observation = await execute_cell(
        cell,
        configured,
        endpoints,
        token,
        observers,
        event_sink,
        transport_factory=lambda configured,
        configured_endpoints,
        configured_token,
        sink: (
            _build_direct_transport(
                configured,
                configured_endpoints,
                configured_token,
                sink,
                coordinator_restart,
            )
        ),
        before_trial=(
            trial_window.await_start_acknowledgement
            if trial_window is not None
            else None
        ),
        after_trial=(
            trial_window.await_end_acknowledgement if trial_window is not None else None
        ),
    )
    events = tuple(event_sink.events)
    if cell.workload != "W6c":
        handler_attempts = sum(
            event.get("event") == "fixture.handler_started" for event in events
        )
        observation = replace(
            observation,
            executions=handler_attempts,
            handler_attempts=handler_attempts,
        )
    return observation, events


async def run_external_cell(
    cell: MatrixCell,
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
    trial_window: TrialWindowSignal | None = None,
) -> tuple[TrialObservation, tuple[Mapping[str, object], ...]]:
    """Run a workload whose fixture process is owned by the compose supervisor."""
    if cell.mode != config.mode:
        raise ValueError("cell mode does not match config")
    if cell.workload not in _EXTERNAL_WORKLOADS:
        raise ValueError("workload does not use an external worker lifecycle")
    event_sink = CollectingEventSink()
    transport = _build_direct_transport(config, endpoints, token, event_sink)
    try:
        lifecycle = ExternalWorkerLifecycle(Path("/control"), Path("/state"))
        if cell.workload == "W5":
            observers: Mapping[str, object] = {
                "crash": ExternalCrashObserver(lifecycle, config, transport)
            }
        elif cell.workload == "W6b":
            semantic_retry = ExternalSemanticRetryObserver(lifecycle, config, transport)
            await semantic_retry.prepare(float(cell.timeout_seconds))
            observers = {"semantic_retry": semantic_retry}
        else:
            actuator = ExternalActuatorObserver(lifecycle, config, transport)
            await actuator.prepare(float(cell.timeout_seconds))
            observers = {"actuator": actuator}
        observation = await run_cell(
            cell,
            transport,
            {"sender_id": "requester-1", "worker_id": config.agent_id},
            observers,
            event_sink,
            before_trial=(
                trial_window.await_start_acknowledgement
                if trial_window is not None
                else None
            ),
        )
        if trial_window is not None:
            await trial_window.await_end_acknowledgement()
    finally:
        await transport.close()
    return observation, tuple(event_sink.events)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one directly controlled workload cell."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--workload", choices=tuple(sorted(_RUNNER_WORKLOADS)), required=True
    )
    parser.add_argument("--ablation", default="full-contract")
    parser.add_argument("--timeout-seconds", type=int)
    return parser


async def _main(argv: Sequence[str], environ: Mapping[str, str]) -> int:
    arguments = _argument_parser().parse_args(argv)
    try:
        config = load_native_config(arguments.config)
        endpoints = runtime_endpoints(config, environ)
        token = read_transport_token(environ.get("EC_CREDENTIAL_FILE", ""))
        timeout_seconds = (
            workload_timeout_seconds(arguments.workload)
            if arguments.timeout_seconds is None
            else arguments.timeout_seconds
        )
        if timeout_seconds <= 0:
            raise ValueError("invalid timeout")
        cell = MatrixCell(
            workload=arguments.workload,
            mode=config.mode,
            variant="primary",
            ablation=arguments.ablation,
            timeout_seconds=timeout_seconds,
        )
        trial_window = TrialWindowSignal(Path("/control"))
        if cell.workload in _DIRECT_WORKLOADS:
            observation, events = await run_direct_cell(
                cell, config, endpoints, token, trial_window
            )
        else:
            observation, events = await run_external_cell(
                cell, config, endpoints, token, trial_window
            )
    except (OSError, ValueError, RuntimeError) as error:
        print(f"runner failed: {error}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "events": list(events),
                "observation": asdict(observation),
                "workload": cell.workload,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


def main(
    argv: Sequence[str] | None = None,
    environ: Mapping[str, str] = os.environ,
) -> int:
    return asyncio.run(_main(tuple(argv or ()), environ))


if __name__ == "__main__":
    raise SystemExit(main(cast(Sequence[str], sys.argv[1:])))


__all__ = ["prepare_direct_execution", "run_direct_cell", "run_external_cell"]
