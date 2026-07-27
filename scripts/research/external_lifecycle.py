"""Run-owned control and evidence access for the supervised native worker."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, replace
from pathlib import Path
from typing import Protocol

from adapters._common.task_types import PublicationReceipt
from scripts.research.fixtures.native_control import NativeControlConfig
from scripts.research.modes.base import Mode
from scripts.research.workload_matrix import CrashPoint

_POLL_SECONDS = 0.05


def _config_bytes(config: NativeControlConfig) -> bytes:
    return json.dumps(asdict(config), sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )


def _status(path: Path) -> Mapping[str, str]:
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError:
        return {}
    result: dict[str, str] = {}
    for line in lines:
        key, separator, value = line.partition("=")
        if not separator or key in result:
            return {}
        result[key] = value
    return result


def _event_at_offset(path: Path, offset: int) -> tuple[Mapping[str, object], ...]:
    try:
        with path.open("rb") as handle:
            handle.seek(offset)
            payload = handle.read()
    except FileNotFoundError:
        return ()
    except OSError:
        raise RuntimeError("external worker events are unavailable") from None
    events: list[Mapping[str, object]] = []
    for line in payload.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            raise RuntimeError("invalid external worker event") from None
        if not isinstance(event, dict):
            raise TypeError("invalid external worker event")
        events.append(event)
    return tuple(events)


class _CrashTransport(Protocol):
    async def submit_task(
        self, envelope: Mapping[str, object]
    ) -> PublicationReceipt: ...

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> object | None: ...


class ExternalWorkerLifecycle:
    """Activate a specific external worker generation and read its durable events."""

    def __init__(self, control_dir: Path, state_dir: Path) -> None:
        if not control_dir.is_absolute() or not state_dir.is_absolute():
            raise ValueError("invalid external worker paths")
        self._control_path = control_dir / "active-native-control.json"
        self._status_path = control_dir / "worker-status.txt"
        self._event_path = state_dir / "worker-events.jsonl"

    async def activate(self, config: NativeControlConfig, timeout_s: float) -> str:
        if timeout_s <= 0:
            raise ValueError("invalid activation timeout")
        try:
            event_offset = self._event_path.stat().st_size
        except FileNotFoundError:
            event_offset = 0
        except OSError:
            raise RuntimeError("external worker events are unavailable") from None
        encoded = _config_bytes(config)
        generation = hashlib.sha256(encoded).hexdigest()
        temporary = self._control_path.with_suffix(".tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(descriptor, encoded)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self._control_path)
        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            status = _status(self._status_path)
            if (
                status.get("generation") == generation
                and status.get("status") == "running"
                and any(
                    _is_ready_event(event, config.agent_id)
                    for event in _event_at_offset(self._event_path, event_offset)
                )
            ):
                return generation
            await asyncio.sleep(_POLL_SECONDS)
        raise RuntimeError("external worker activation timed out")

    async def wait_for_exit(self, generation: str, timeout_s: float) -> int:
        if len(generation) != 64 or timeout_s <= 0:
            raise ValueError("invalid worker generation")
        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            status = _status(self._status_path)
            if (
                status.get("generation") == generation
                and status.get("status") == "exited"
            ):
                try:
                    return int(status["exit_code"])
                except (KeyError, ValueError):
                    raise RuntimeError("invalid external worker status") from None
            await asyncio.sleep(_POLL_SECONDS)
        raise RuntimeError("external worker exit timed out")

    def task_events(self, task_id: str) -> tuple[Mapping[str, object], ...]:
        if not task_id:
            raise ValueError("invalid task id")
        events: list[Mapping[str, object]] = []
        for event in _event_at_offset(self._event_path, 0):
            data = event.get("data")
            if isinstance(data, dict) and data.get("task_id") == task_id:
                events.append(event)
        return tuple(events)


def _is_ready_event(event: Mapping[str, object], agent_id: str) -> bool:
    data = event.get("data")
    return (
        event.get("event") == "fixture.ready"
        and isinstance(data, Mapping)
        and data.get("agent_id") == agent_id
    )


class ExternalCrashObserver:
    """Execute one W5 crash point through the supervised native worker."""

    def __init__(
        self,
        lifecycle: ExternalWorkerLifecycle,
        config: NativeControlConfig,
        transport: _CrashTransport,
    ) -> None:
        self._lifecycle = lifecycle
        self._config = config
        self._transport = transport

    async def run_crash_subtrial(
        self,
        point: CrashPoint,
        envelope: Mapping[str, object],
        timeout_s: float,
    ) -> Mapping[str, object]:
        task_id = envelope.get("task_id")
        if type(task_id) is not str or not task_id or timeout_s <= 0:
            raise ValueError("invalid crash subtrial")
        if self._config.mode == Mode.CORE_ONLY.value and point is CrashPoint.AFTER_MARK:
            return _crash_metrics("transport-inapplicable")
        behavior = "actuator" if point is CrashPoint.AFTER_SIDE_EFFECT else "echo"
        generation = await self._lifecycle.activate(
            replace(self._config, behavior=behavior, crash_point=point.value),
            timeout_s,
        )
        submitted = dict(envelope)
        if point is CrashPoint.DURING_EXCEPTION:
            submitted["payload"] = {"body": None}
        receipt = await self._transport.submit_task(submitted)
        exit_code = await self._lifecycle.wait_for_exit(generation, timeout_s)
        terminal = await self._transport.observe_terminal(task_id, min(timeout_s, 1.0))
        events = self._lifecycle.task_events(task_id)
        event_names = tuple(
            event.get("event")
            for event in events
            if isinstance(event.get("event"), str)
        )
        terminal_envelope = getattr(terminal, "envelope", None)
        terminal_identifier = (
            terminal_envelope.get("id")
            if isinstance(terminal_envelope, Mapping)
            else None
        )
        metrics = _crash_metrics("applicable")
        metrics.update(
            accepted=int(receipt.accepted),
            delivered=event_names.count("fixture.handler_started"),
            executions=event_names.count("fixture.handler_started"),
            side_effects=event_names.count("fixture.side_effect_committed"),
            logical_terminals=int(terminal is not None),
            distinct_terminal_ids=int(isinstance(terminal_identifier, str)),
            publication_attempts=1,
            wire_deliveries=int(terminal is not None),
            timed_out=exit_code != 86,
        )
        return metrics


def _crash_metrics(applicability: str) -> dict[str, object]:
    return {
        "applicability": applicability,
        "accepted": 0,
        "delivered": 0,
        "executions": 0,
        "side_effects": 0,
        "logical_terminals": 0,
        "distinct_terminal_ids": 0,
        "publication_attempts": 0,
        "wire_deliveries": 0,
        "poison": 0,
        "timed_out": False,
    }


__all__ = ["ExternalCrashObserver", "ExternalWorkerLifecycle"]
