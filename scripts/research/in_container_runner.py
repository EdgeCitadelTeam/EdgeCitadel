"""Execute directly controlled workload cells inside an artifact runner container."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, replace
from typing import cast

from scripts.research.execution_harness import (
    CollectingEventSink,
    DelegationObserver,
    ProgressObserver,
    execute_cell,
)
from scripts.research.fixtures.native_control import (
    NativeControlConfig,
    load_native_config,
    read_transport_token,
    runtime_endpoints,
)
from scripts.research.workload_matrix import MatrixCell, TrialObservation

_DIRECT_WORKLOADS = frozenset({"W1", "W2", "W3", "W4", "W6a"})
_BEHAVIORS = {
    "W1": "echo",
    "W2": "delegate",
    "W3": "progress",
    "W4": "echo",
    "W6a": "echo",
}


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
    return configured, observers


async def run_direct_cell(
    cell: MatrixCell,
    config: NativeControlConfig,
    endpoints: Mapping[str, str],
    token: str,
) -> tuple[TrialObservation, tuple[Mapping[str, object], ...]]:
    event_sink = CollectingEventSink()
    configured, observers = prepare_direct_execution(cell, config, event_sink)
    observation = await execute_cell(
        cell,
        configured,
        endpoints,
        token,
        observers,
        event_sink,
    )
    return observation, tuple(event_sink.events)


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one directly controlled workload cell."
    )
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--workload", choices=tuple(sorted(_DIRECT_WORKLOADS)), required=True
    )
    parser.add_argument("--ablation", default="full-contract")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    return parser


async def _main(argv: Sequence[str], environ: Mapping[str, str]) -> int:
    arguments = _argument_parser().parse_args(argv)
    try:
        config = load_native_config(arguments.config)
        endpoints = runtime_endpoints(config, environ)
        token = read_transport_token(environ.get("EC_CREDENTIAL_FILE", ""))
        if arguments.timeout_seconds <= 0:
            raise ValueError("invalid timeout")
        cell = MatrixCell(
            workload=arguments.workload,
            mode=config.mode,
            variant="primary",
            ablation=arguments.ablation,
            timeout_seconds=arguments.timeout_seconds,
        )
        observation, events = await run_direct_cell(cell, config, endpoints, token)
    except (OSError, ValueError, RuntimeError):
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


__all__ = ["prepare_direct_execution", "run_direct_cell"]
