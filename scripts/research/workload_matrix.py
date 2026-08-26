"""Predeclared reliability workloads for the benchmark campaign."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from time import perf_counter_ns
from typing import Protocol, cast
from uuid import uuid4

from scripts.research.modes.base import Mode


class CrashPoint(StrEnum):
    AFTER_RECEIVE = "after-receive-before-handler"
    AFTER_SIDE_EFFECT = "after-side-effect-before-ledger-prepare"
    AFTER_PREPARE = "after-ledger-prepare-before-result-publish"
    AFTER_PUBLISH = "after-result-publish-before-publish-mark"
    AFTER_MARK = "after-publish-mark-before-inbound-commit"
    DURING_EXCEPTION = "during-handler-exception-conversion"


@dataclass(frozen=True)
class MatrixCell:
    workload: str
    mode: str
    variant: str
    ablation: str
    timeout_seconds: int


@dataclass(frozen=True)
class TrialObservation:
    initiated: int
    accepted: int
    delivered: int
    handler_attempts: int | None
    executions: int | None
    side_effects: int | None
    prepared_outcomes: int | None
    logical_terminals: int
    distinct_terminal_ids: int
    publication_attempts: int
    wire_deliveries: int
    progress_generated: int | None
    progress_live_delivered: int | None
    progress_replay_delivered: int | None
    progress_missing: int | None
    poison: int | None
    inapplicable_crash_points: tuple[str, ...]
    timed_out: bool
    final_transport: Mapping[str, object]
    started_monotonic_ns: int = 0
    ended_monotonic_ns: int = 0
    initial_transport: Mapping[str, object] = field(default_factory=dict)
    workload_evidence: Mapping[str, object] = field(default_factory=dict)


class _CellTransport(Protocol):
    async def start_terminal_observer(self) -> None: ...

    async def submit_task(self, envelope: Mapping[str, object]) -> object: ...

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> object | None: ...

    async def inspect_state(self) -> object: ...


class _ProgressObserver(Protocol):
    async def wait_for_generated(self, count: int) -> None: ...

    def progress_counts(self) -> Mapping[str, int]: ...


class _ProgressFaults(Protocol):
    async def disconnect_progress_observer(self) -> None: ...

    async def reconnect_progress_observer(self) -> None: ...


class _DelegationObserver(Protocol):
    async def wait_for_child(self, parent_task_id: str) -> Mapping[str, object]: ...


class _CollisionObserver(Protocol):
    async def wait_for_collisions(self, task_id: str) -> Mapping[str, int]: ...


class _SemanticRetryObserver(Protocol):
    async def wait_for_retry_window(self, task_id: str) -> Mapping[str, int]: ...


class _CrashObserver(Protocol):
    async def run_crash_subtrial(
        self,
        point: CrashPoint,
        envelope: Mapping[str, object],
        timeout_s: float,
    ) -> Mapping[str, object]: ...


class _ActuatorObserver(Protocol):
    async def record_submission(self, envelope: Mapping[str, object]) -> None: ...

    async def wait_for_actuator_outcome(self, task_id: str) -> Mapping[str, object]: ...


class _WorkerFaults(Protocol):
    async def stop_worker(self, agent_id: str) -> None: ...

    async def start_worker(self, agent_id: str) -> None: ...

    async def restart_coordinator(self) -> None: ...


_WORKLOADS = ("W1", "W2", "W3", "W4", "W5", "W6a", "W6b", "W6c", "W7", "W8")
_ABLATION_WORKLOADS = frozenset({"W6a", "W6b", "W8"})
_TIMEOUT_SECONDS = 30
_WORKLOAD_TIMEOUTS = {"W6b": 330, "W7": 35}


def workload_timeout_seconds(workload: str) -> int:
    if workload not in _WORKLOADS:
        raise ValueError("invalid workload")
    return _WORKLOAD_TIMEOUTS.get(workload, _TIMEOUT_SECONDS)


def required_matrix_cells() -> tuple[MatrixCell, ...]:
    primary = tuple(
        MatrixCell(
            workload=workload,
            mode=mode.value,
            variant="primary",
            ablation="full-contract",
            timeout_seconds=workload_timeout_seconds(workload),
        )
        for workload in _WORKLOADS
        for mode in Mode
    )
    ablations = tuple(
        MatrixCell(
            workload=workload,
            mode=Mode.EDGECITADEL.value,
            variant="ablation",
            ablation=ablation,
            timeout_seconds=workload_timeout_seconds(workload),
        )
        for workload in _WORKLOADS
        if workload in _ABLATION_WORKLOADS
        for ablation in ("none", "broker-only")
    )
    return primary + ablations


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _fixture_value(
    fixture: Mapping[str, object],
    name: str,
    default: str,
) -> str:
    value = fixture.get(name, default)
    if type(value) is not str or not value:
        raise ValueError(f"invalid fixture {name}")
    return value


def _snapshot_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _snapshot_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return tuple(_snapshot_value(item) for item in value)
    if isinstance(value, list):
        return [_snapshot_value(item) for item in value]
    return value


def _snapshot_mapping(snapshot: object) -> Mapping[str, object]:
    if isinstance(snapshot, Mapping):
        return _snapshot_value(snapshot)  # type: ignore[return-value]
    return _snapshot_value(
        {
            "mode": getattr(getattr(snapshot, "mode", None), "value", None),
            "streams": getattr(snapshot, "streams", {}),
            "consumers": getattr(snapshot, "consumers", {}),
            "pending": getattr(snapshot, "pending", None),
            "ack_pending": getattr(snapshot, "ack_pending", None),
            "connection_bytes": getattr(snapshot, "connection_bytes", {}),
            "storage_bytes": getattr(snapshot, "storage_bytes", None),
            "message_count": getattr(snapshot, "message_count", None),
        }
    )  # type: ignore[return-value]


def _require_nonnegative_metrics(
    metrics: Mapping[str, object],
    names: set[str],
) -> None:
    if set(metrics) != names:
        raise RuntimeError("invalid workload observation")
    for name, value in metrics.items():
        if name in {"applicability", "timed_out", "crash_point"}:
            continue
        if type(value) is not int or value < 0:
            raise RuntimeError("invalid workload observation")


def classify_outcome(
    cell: MatrixCell,
    observation: Mapping[str, object],
) -> str:
    """Classify a repetition from its task semantics rather than process exit."""
    timed_out = observation.get("timed_out") is True
    if cell.workload == "W6c":
        collision_passed = (
            timed_out
            and observation.get("accepted") == 3
            and observation.get("delivered") == 0
            and observation.get("executions") == 0
            and observation.get("logical_terminals") == 0
            and observation.get("distinct_terminal_ids") == 0
            and observation.get("publication_attempts") == 3
            and observation.get("wire_deliveries") == 0
            and observation.get("poison") == 2
        )
        return "completed" if collision_passed else "failed"
    if timed_out:
        return "timeout"

    initiated = observation.get("initiated")
    logical = observation.get("logical_terminals")
    executions = observation.get("executions")
    if cell.workload == "W2":
        execution_consistent = executions is None or executions == 2
    elif cell.workload == "W8":
        execution_consistent = type(executions) is int and executions >= 1
    else:
        execution_consistent = executions is None or executions == initiated
    completed = (
        type(initiated) is int
        and initiated > 0
        and type(observation.get("accepted")) is int
        and observation["accepted"] >= 1  # type: ignore[operator]
        and observation.get("delivered") == initiated
        and logical == initiated
        and observation.get("distinct_terminal_ids") == logical
        and observation.get("publication_attempts", 0) >= initiated  # type: ignore[operator]
        and type(observation.get("wire_deliveries")) is int
        and observation["wire_deliveries"] >= logical  # type: ignore[operator]
        and execution_consistent
    )
    if cell.workload == "W3":
        progress = tuple(
            observation.get(name)
            for name in (
                "progress_live_delivered",
                "progress_replay_delivered",
                "progress_missing",
            )
        )
        completed = (
            completed
            and observation.get("progress_generated") == 20
            and all(type(value) is int for value in progress)
            and sum(value for value in progress if type(value) is int) == 20
        )
    if cell.workload == "W8":
        completed = (
            completed
            and type(observation.get("side_effects")) is int
            and observation["side_effects"] >= 1  # type: ignore[operator]
            and type(observation.get("prepared_outcomes")) is int
            and observation["prepared_outcomes"] >= 1  # type: ignore[operator]
        )
    return "completed" if completed else "failed"


async def run_cell(
    cell: MatrixCell,
    transport: _CellTransport,
    fixture: Mapping[str, object],
    observers: object,
    event_sink: object | None,
    *,
    clock_ns: Callable[[], int] = perf_counter_ns,
    before_trial: Callable[[], Awaitable[None]] | None = None,
) -> TrialObservation:
    if cell.workload not in {
        "W1",
        "W2",
        "W3",
        "W4",
        "W5",
        "W6a",
        "W6b",
        "W6c",
        "W7",
        "W8",
    }:
        raise ValueError(f"unsupported workload: {cell.workload}")
    sender_id = _fixture_value(fixture, "sender_id", "requester-1")
    worker_id = _fixture_value(fixture, "worker_id", "worker-1")
    task_id = str(uuid4())
    nonce = str(uuid4())
    envelope: dict[str, object] = {
        "v": 1,
        "id": str(uuid4()),
        "type": "command",
        "sender_id": sender_id,
        "recipient_id": worker_id,
        "task_id": task_id,
        "context_id": task_id,
        "hop_count": 0,
        "timestamp": _now_iso(),
        "payload": {"body": nonce},
    }
    await transport.start_terminal_observer()
    if cell.workload == "W5":
        if before_trial is not None:
            await before_trial()
        initial_transport = _snapshot_mapping(await transport.inspect_state())
        started_ns = clock_ns()
        if not isinstance(observers, Mapping):
            raise ValueError("invalid crash observer")
        crash_observer = observers.get("crash")
        if crash_observer is None or not callable(
            getattr(crash_observer, "run_crash_subtrial", None)
        ):
            raise ValueError("invalid crash observer")
        expected_metrics = {
            "applicability",
            "accepted",
            "delivered",
            "executions",
            "side_effects",
            "logical_terminals",
            "distinct_terminal_ids",
            "publication_attempts",
            "wire_deliveries",
            "poison",
            "timed_out",
        }
        crash_results: list[Mapping[str, object]] = []
        for point in CrashPoint:
            subtrial = dict(envelope)
            subtrial_task_id = str(uuid4())
            subtrial["id"] = str(uuid4())
            subtrial["task_id"] = subtrial_task_id
            subtrial["context_id"] = subtrial_task_id
            subtrial["payload"] = {"body": str(uuid4())}
            result = await cast(_CrashObserver, crash_observer).run_crash_subtrial(
                point,
                subtrial,
                float(cell.timeout_seconds),
            )
            if not isinstance(result, Mapping):
                raise TypeError("invalid crash observation")
            _require_nonnegative_metrics(result, expected_metrics)
            applicability = result["applicability"]
            if applicability not in {"applicable", "transport-inapplicable"}:
                raise RuntimeError("invalid crash observation")
            if cell.mode == Mode.CORE_ONLY.value and point is CrashPoint.AFTER_MARK:
                if applicability != "transport-inapplicable":
                    raise RuntimeError("missing core-only inapplicability")
            elif applicability != "applicable":
                raise RuntimeError("unexpected crash inapplicability")
            if type(result["timed_out"]) is not bool:
                raise RuntimeError("invalid crash observation")
            crash_results.append(result)
        inapplicable = tuple(
            point.value
            for point, result in zip(CrashPoint, crash_results, strict=True)
            if result["applicability"] == "transport-inapplicable"
        )
        return TrialObservation(
            initiated=len(crash_results),
            accepted=sum(cast(int, result["accepted"]) for result in crash_results),
            delivered=sum(cast(int, result["delivered"]) for result in crash_results),
            handler_attempts=None,
            executions=sum(cast(int, result["executions"]) for result in crash_results),
            side_effects=sum(
                cast(int, result["side_effects"]) for result in crash_results
            ),
            prepared_outcomes=None,
            logical_terminals=sum(
                cast(int, result["logical_terminals"]) for result in crash_results
            ),
            distinct_terminal_ids=sum(
                cast(int, result["distinct_terminal_ids"]) for result in crash_results
            ),
            publication_attempts=sum(
                cast(int, result["publication_attempts"]) for result in crash_results
            ),
            wire_deliveries=sum(
                cast(int, result["wire_deliveries"]) for result in crash_results
            ),
            progress_generated=None,
            progress_live_delivered=None,
            progress_replay_delivered=None,
            progress_missing=None,
            poison=sum(cast(int, result["poison"]) for result in crash_results),
            inapplicable_crash_points=inapplicable,
            timed_out=any(cast(bool, result["timed_out"]) for result in crash_results),
            final_transport=_snapshot_mapping(await transport.inspect_state()),
            started_monotonic_ns=started_ns,
            ended_monotonic_ns=clock_ns(),
            initial_transport=initial_transport,
            workload_evidence={"crash_subtrials": tuple(crash_results)},
        )
    worker_faults: _WorkerFaults | None = None
    if cell.workload in {"W4", "W7"}:
        faults = getattr(transport, "faults", None)
        if (
            faults is None
            or not callable(getattr(faults, "stop_worker", None))
            or not callable(getattr(faults, "start_worker", None))
            or not callable(getattr(faults, "restart_coordinator", None))
        ):
            raise ValueError("invalid worker fault controller")
        worker_faults = cast(_WorkerFaults, faults)
        await worker_faults.stop_worker(worker_id)
    progress_counts: Mapping[str, int] | None = None
    if cell.workload == "W3":
        if not isinstance(observers, Mapping):
            raise ValueError("invalid progress observer")
        progress_observer = observers.get("progress")
        start_progress_observer = getattr(transport, "start_progress_observer", None)
        faults = getattr(transport, "faults", None)
        if (
            progress_observer is None
            or not callable(getattr(progress_observer, "wait_for_generated", None))
            or not callable(getattr(progress_observer, "progress_counts", None))
            or not callable(start_progress_observer)
            or faults is None
            or not callable(getattr(faults, "disconnect_progress_observer", None))
            or not callable(getattr(faults, "reconnect_progress_observer", None))
        ):
            raise ValueError("invalid progress observer")
        typed_progress_observer = cast(_ProgressObserver, progress_observer)
        typed_faults = cast(_ProgressFaults, faults)
        await start_progress_observer()
    if before_trial is not None:
        await before_trial()
    initial_transport = _snapshot_mapping(await transport.inspect_state())
    started_ns = clock_ns()
    receipts = [await transport.submit_task(envelope)]
    workload_evidence: dict[str, object] = {}
    if cell.workload == "W8":
        if not isinstance(observers, Mapping):
            raise ValueError("invalid actuator observer")
        actuator_observer = observers.get("actuator")
        if (
            actuator_observer is None
            or not callable(
                getattr(actuator_observer, "wait_for_actuator_outcome", None)
            )
            or not callable(getattr(actuator_observer, "record_submission", None))
        ):
            raise ValueError("invalid actuator observer")
        await cast(_ActuatorObserver, actuator_observer).record_submission(envelope)
        actuator = await cast(
            _ActuatorObserver, actuator_observer
        ).wait_for_actuator_outcome(task_id)
        expected_metrics = {
            "handler_attempts",
            "delivered",
            "side_effects",
            "prepared_outcomes",
            "logical_terminals",
            "distinct_terminal_ids",
            "publication_attempts",
            "wire_deliveries",
            "poison",
            "timed_out",
            "crash_point",
        }
        if not isinstance(actuator, Mapping):
            raise RuntimeError("invalid actuator observation")
        _require_nonnegative_metrics(actuator, expected_metrics)
        if (
            type(actuator["timed_out"]) is not bool
            or actuator["crash_point"] != CrashPoint.AFTER_SIDE_EFFECT.value
        ):
            raise RuntimeError("invalid actuator observation")
        handler_attempts = cast(int, actuator["handler_attempts"])
        side_effects = cast(int, actuator["side_effects"])
        if handler_attempts < 1 or side_effects < 1:
            raise RuntimeError("invalid actuator observation")
        return TrialObservation(
            initiated=1,
            accepted=sum(
                getattr(receipt, "accepted", False) is True for receipt in receipts
            ),
            delivered=cast(int, actuator["delivered"]),
            handler_attempts=handler_attempts,
            executions=handler_attempts,
            side_effects=side_effects,
            prepared_outcomes=cast(int, actuator["prepared_outcomes"]),
            logical_terminals=cast(int, actuator["logical_terminals"]),
            distinct_terminal_ids=cast(int, actuator["distinct_terminal_ids"]),
            publication_attempts=cast(int, actuator["publication_attempts"]),
            wire_deliveries=cast(int, actuator["wire_deliveries"]),
            progress_generated=None,
            progress_live_delivered=None,
            progress_replay_delivered=None,
            progress_missing=None,
            poison=cast(int, actuator["poison"]),
            inapplicable_crash_points=(),
            timed_out=actuator["timed_out"],
            final_transport=_snapshot_mapping(await transport.inspect_state()),
            started_monotonic_ns=started_ns,
            ended_monotonic_ns=clock_ns(),
            initial_transport=initial_transport,
            workload_evidence=workload_evidence,
        )
    if cell.workload == "W6a":
        receipts.append(await transport.submit_task(envelope))
        workload_evidence["wire_retry"] = {
            "envelope_ids": [getattr(receipt, "envelope_id", None) for receipt in receipts],
            "accepted": [getattr(receipt, "accepted", None) for receipt in receipts],
            "stream_sequences": [
                getattr(receipt, "stream_sequence", None) for receipt in receipts
            ],
            "duplicate_flags": [getattr(receipt, "duplicate", None) for receipt in receipts],
        }
    if cell.workload == "W6b":
        if not isinstance(observers, Mapping):
            raise ValueError("invalid semantic retry observer")
        retry_observer = observers.get("semantic_retry")
        if retry_observer is None or not callable(
            getattr(retry_observer, "wait_for_retry_window", None)
        ):
            raise ValueError("invalid semantic retry observer")
        retry_window = await cast(
            _SemanticRetryObserver, retry_observer
        ).wait_for_retry_window(task_id)
        if (
            not isinstance(retry_window, Mapping)
            or set(retry_window)
            != {
                "broker_duplicate_window_seconds",
                "retry_elapsed_seconds",
                "ledger_retention_seconds",
            }
            or any(
                type(value) is not int or value < 0 for value in retry_window.values()
            )
            or retry_window["retry_elapsed_seconds"]
            <= retry_window["broker_duplicate_window_seconds"]
            or retry_window["retry_elapsed_seconds"]
            >= retry_window["ledger_retention_seconds"]
        ):
            raise RuntimeError("invalid semantic retry window")
        semantic_retry = dict(envelope)
        semantic_retry["id"] = str(uuid4())
        semantic_retry["payload"] = dict(
            cast(Mapping[str, object], envelope["payload"])
        )
        receipts.append(await transport.submit_task(semantic_retry))
        workload_evidence["semantic_retry"] = {
            "first_envelope_id": envelope["id"],
            "second_envelope_id": semantic_retry["id"],
            "task_id": task_id,
            "retry_window": dict(retry_window),
        }
    if cell.workload == "W6c":
        sender_mutation = dict(envelope)
        sender_mutation["id"] = str(uuid4())
        sender_mutation["sender_id"] = f"{sender_id}-collision"
        sender_mutation["payload"] = dict(
            cast(Mapping[str, object], envelope["payload"])
        )
        payload_mutation = dict(envelope)
        payload_mutation["id"] = str(uuid4())
        payload_mutation["payload"] = {"body": f"{nonce}-collision"}
        receipts.extend(
            (
                await transport.submit_task(sender_mutation),
                await transport.submit_task(payload_mutation),
            )
        )
    accepted = sum(getattr(receipt, "accepted", False) is True for receipt in receipts)
    if cell.workload == "W6c":
        if not isinstance(observers, Mapping):
            raise ValueError("invalid collision observer")
        collision_observer = observers.get("collision")
        if collision_observer is None or not callable(
            getattr(collision_observer, "wait_for_collisions", None)
        ):
            raise ValueError("invalid collision observer")
        collision = await cast(
            _CollisionObserver, collision_observer
        ).wait_for_collisions(task_id)
        if (
            not isinstance(collision, Mapping)
            or set(collision) != {"rejections", "executions", "cached_output_exposure"}
            or any(type(value) is not int or value < 0 for value in collision.values())
            or collision["rejections"] != 2
            or collision["executions"] != 0
            or collision["cached_output_exposure"] != 0
        ):
            raise RuntimeError("invalid collision observation")
        return TrialObservation(
            initiated=1,
            accepted=accepted,
            delivered=0,
            handler_attempts=None,
            executions=collision["executions"],
            side_effects=None,
            prepared_outcomes=None,
            logical_terminals=0,
            distinct_terminal_ids=0,
            publication_attempts=len(receipts),
            wire_deliveries=0,
            progress_generated=None,
            progress_live_delivered=None,
            progress_replay_delivered=None,
            progress_missing=None,
            poison=collision["rejections"],
            inapplicable_crash_points=(),
            timed_out=True,
            final_transport=_snapshot_mapping(await transport.inspect_state()),
            started_monotonic_ns=started_ns,
            ended_monotonic_ns=clock_ns(),
            initial_transport=initial_transport,
            workload_evidence={"collision": dict(collision)},
        )
    if cell.workload == "W7":
        if worker_faults is None:
            raise AssertionError("missing worker fault controller")
        await worker_faults.restart_coordinator()
        await worker_faults.start_worker(worker_id)
    elif worker_faults is not None:
        await worker_faults.start_worker(worker_id)
    observed_task_id = task_id
    if cell.workload == "W2":
        if not isinstance(observers, Mapping):
            raise ValueError("invalid delegation observer")
        delegation_observer = observers.get("delegation")
        if delegation_observer is None or not callable(
            getattr(delegation_observer, "wait_for_child", None)
        ):
            raise ValueError("invalid delegation observer")
        child = await cast(_DelegationObserver, delegation_observer).wait_for_child(
            task_id
        )
        child_task_id = child.get("task_id")
        if (
            type(child_task_id) is not str
            or child_task_id == task_id
            or child.get("context_id") != task_id
            or child.get("hop_count") != 1
            or child.get("parent_task_id") != task_id
        ):
            raise RuntimeError("invalid delegation observation")
        observed_task_id = child_task_id
    if cell.workload == "W3":
        await typed_progress_observer.wait_for_generated(5)
        await typed_faults.disconnect_progress_observer()
        await typed_progress_observer.wait_for_generated(15)
        await typed_faults.reconnect_progress_observer()
        await typed_progress_observer.wait_for_generated(20)
        candidate_counts = typed_progress_observer.progress_counts()
        if not isinstance(candidate_counts, Mapping):
            raise RuntimeError("invalid progress observation")
        required_progress_keys = {"generated", "live", "replayed", "missing"}
        if set(candidate_counts) != required_progress_keys or any(
            type(value) is not int or value < 0 for value in candidate_counts.values()
        ):
            raise RuntimeError("invalid progress observation")
        if (
            candidate_counts["generated"] != 20
            or sum(candidate_counts[name] for name in ("live", "replayed", "missing"))
            != 20
        ):
            raise RuntimeError("invalid progress observation")
        progress_counts = candidate_counts
    terminal = await transport.observe_terminal(
        observed_task_id,
        float(cell.timeout_seconds),
    )
    final_transport = _snapshot_mapping(await transport.inspect_state())
    if terminal is None:
        return TrialObservation(
            initiated=1,
            accepted=accepted,
            delivered=0,
            handler_attempts=None,
            executions=None,
            side_effects=None,
            prepared_outcomes=None,
            logical_terminals=0,
            distinct_terminal_ids=0,
            publication_attempts=len(receipts),
            wire_deliveries=0,
            progress_generated=(
                progress_counts["generated"] if progress_counts is not None else None
            ),
            progress_live_delivered=(
                progress_counts["live"] if progress_counts is not None else None
            ),
            progress_replay_delivered=(
                progress_counts["replayed"] if progress_counts is not None else None
            ),
            progress_missing=(
                progress_counts["missing"] if progress_counts is not None else None
            ),
            poison=None,
            inapplicable_crash_points=(),
            timed_out=True,
            final_transport=final_transport,
            started_monotonic_ns=started_ns,
            ended_monotonic_ns=clock_ns(),
            initial_transport=initial_transport,
            workload_evidence=workload_evidence,
        )
    terminal_envelope = getattr(terminal, "envelope", None)
    if not isinstance(terminal_envelope, Mapping):
        raise RuntimeError("invalid terminal observation")  # noqa: TRY004
    payload = terminal_envelope.get("payload")
    if (
        terminal_envelope.get("task_id") != observed_task_id
        or not isinstance(payload, Mapping)
        or payload.get("body") != f"edgecitadel:{nonce}"
        or (cell.workload == "W2" and payload.get("parent_task_id") != task_id)
    ):
        raise RuntimeError("terminal nonce mismatch")
    if event_sink is not None and hasattr(event_sink, "emit"):
        event_sink.emit({"event": "matrix.w1.terminal", "task_id": task_id})
    terminal_id = terminal_envelope.get("id")
    observed_ns = getattr(terminal, "observed_ns", None)
    ended_ns = (
        observed_ns
        if type(observed_ns) is int and observed_ns >= started_ns
        else clock_ns()
    )
    return TrialObservation(
        initiated=1,
        accepted=accepted,
        delivered=1,
        handler_attempts=None,
        executions=None,
        side_effects=None,
        prepared_outcomes=None,
        logical_terminals=1,
        distinct_terminal_ids=int(type(terminal_id) is str),
        publication_attempts=len(receipts),
        wire_deliveries=1,
        progress_generated=(
            progress_counts["generated"] if progress_counts is not None else None
        ),
        progress_live_delivered=(
            progress_counts["live"] if progress_counts is not None else None
        ),
        progress_replay_delivered=(
            progress_counts["replayed"] if progress_counts is not None else None
        ),
        progress_missing=(
            progress_counts["missing"] if progress_counts is not None else None
        ),
        poison=None,
        inapplicable_crash_points=(),
        timed_out=False,
        final_transport=final_transport,
        started_monotonic_ns=started_ns,
        ended_monotonic_ns=ended_ns,
        initial_transport=initial_transport,
        workload_evidence=workload_evidence,
    )


__all__ = [
    "CrashPoint",
    "MatrixCell",
    "TrialObservation",
    "classify_outcome",
    "required_matrix_cells",
    "run_cell",
]
