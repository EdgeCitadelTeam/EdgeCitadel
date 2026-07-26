"""Transport-neutral benchmark contract tests."""

from __future__ import annotations

import ast
import inspect
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import FrozenInstanceError, dataclass, fields
from enum import Enum
from pathlib import Path
from typing import TypeVar, cast, get_type_hints

import pytest

import scripts.research.modes.base as base_module
from adapters._common.task_executor import (
    ExecutionResult,
    InboundDelivery,
    TaskExecutor,
)
from adapters._common.task_publisher import (
    EventSink as CanonicalEventSink,
)
from adapters._common.task_publisher import (
    ProgressPublisher,
    TerminalPublisher,
)
from adapters._common.task_types import (
    PublicationReceipt as CanonicalPublicationReceipt,
)
from scripts.research.modes.base import (
    FaultController,
    Mode,
    ObservedEnvelope,
    ObserverDelivery,
    PublicationReceipt,
    TaskTransport,
    TransportSnapshot,
)

_TestFunction = TypeVar("_TestFunction", bound=Callable[..., object])


def typed_decorator(
    decorator: object,
) -> Callable[[_TestFunction], _TestFunction]:
    return cast(Callable[[_TestFunction], _TestFunction], decorator)


async_test = typed_decorator(pytest.mark.asyncio)


EXPECTED_EXPORTS = {
    "EventSink",
    "FaultController",
    "Mode",
    "ObservedEnvelope",
    "ObserverDelivery",
    "PublicationReceipt",
    "TaskTransport",
    "TransportSnapshot",
}


def _public_protocol_members(protocol: type[object]) -> set[str]:
    return {name for name in vars(protocol) if not name.startswith("_")}


def _resolved_hints(member: object) -> dict[str, object]:
    return get_type_hints(
        member,
        globalns=vars(base_module),
        localns={"TaskExecutor": TaskExecutor},
    )


def _assert_async_signature(
    protocol: type[object],
    name: str,
    parameters: tuple[tuple[str, object], ...],
    return_type: object,
) -> None:
    member = getattr(protocol, name)
    assert inspect.iscoroutinefunction(member)
    signature = inspect.signature(member)
    assert list(signature.parameters) == [
        "self",
        *(parameter_name for parameter_name, _ in parameters),
    ]
    for parameter in signature.parameters.values():
        assert parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert parameter.default is inspect.Parameter.empty
    assert _resolved_hints(member) == {
        **dict(parameters),
        "return": return_type,
    }


def _assert_read_only_property(
    protocol: type[object],
    name: str,
    return_type: object,
) -> None:
    descriptor = vars(protocol)[name]
    assert isinstance(descriptor, property)
    assert descriptor.fget is not None
    assert descriptor.fset is None
    assert descriptor.fdel is None
    assert list(inspect.signature(descriptor.fget).parameters) == ["self"]
    assert _resolved_hints(descriptor.fget) == {"return": return_type}


def _requires_terminal_publisher(publisher: TerminalPublisher) -> None:
    del publisher


def _requires_progress_publisher(publisher: ProgressPublisher) -> None:
    del publisher


def _assert_publisher_compatibility(transport: TaskTransport) -> None:
    _requires_terminal_publisher(transport)
    _requires_progress_publisher(transport)


def _rebind(value: object, name: str, replacement: object) -> None:
    setattr(value, name, replacement)


def test_mode_values_and_json_representation_are_exact() -> None:
    assert Mode.__bases__ == (str, Enum)
    assert [(member.name, member.value) for member in Mode] == [
        ("CENTRAL_RELAY", "central-relay"),
        ("CORE_ONLY", "core-only"),
        ("EDGECITADEL", "edgecitadel"),
        ("ALL_DURABLE", "all-durable"),
    ]
    assert json.dumps({"mode": Mode.CORE_ONLY}) == '{"mode": "core-only"}'
    assert str(Mode.CORE_ONLY) == "Mode.CORE_ONLY"


def test_frozen_value_types_have_exact_shallow_fields() -> None:
    assert [field.name for field in fields(ObservedEnvelope)] == [
        "envelope",
        "observed_ns",
        "observation_index",
        "stream_sequence",
        "delivery_count",
        "replayed",
        "delivery",
    ]
    assert get_type_hints(ObservedEnvelope) == {
        "envelope": Mapping[str, object],
        "observed_ns": int,
        "observation_index": int,
        "stream_sequence": int | None,
        "delivery_count": int,
        "replayed": bool,
        "delivery": ObserverDelivery | None,
    }
    assert [field.name for field in fields(TransportSnapshot)] == [
        "mode",
        "streams",
        "consumers",
        "pending",
        "ack_pending",
        "connection_bytes",
        "storage_bytes",
        "message_count",
    ]
    assert get_type_hints(TransportSnapshot) == {
        "mode": Mode,
        "streams": Mapping[str, Mapping[str, object]],
        "consumers": Mapping[str, Mapping[str, object]],
        "pending": int | None,
        "ack_pending": int | None,
        "connection_bytes": Mapping[str, int],
        "storage_bytes": int,
        "message_count": int,
    }

    envelope: dict[str, object] = {"id": "terminal-1"}
    observed = ObservedEnvelope(
        envelope=envelope,
        observed_ns=1,
        observation_index=1,
        stream_sequence=None,
        delivery_count=1,
        replayed=False,
        delivery=None,
    )
    snapshot = TransportSnapshot(
        mode=Mode.CENTRAL_RELAY,
        streams={},
        consumers={},
        pending=None,
        ack_pending=None,
        connection_bytes={},
        storage_bytes=0,
        message_count=0,
    )

    with pytest.raises(FrozenInstanceError):
        _rebind(observed, "observed_ns", 2)
    with pytest.raises(FrozenInstanceError):
        _rebind(snapshot, "message_count", 1)

    envelope["nested-change"] = True
    assert observed.envelope["nested-change"] is True


def test_canonical_types_are_reexported_without_aliases() -> None:
    assert PublicationReceipt is CanonicalPublicationReceipt
    assert base_module.EventSink is CanonicalEventSink
    assert set(base_module.__all__) == EXPECTED_EXPORTS
    assert {
        name
        for name in vars(base_module)
        if name.endswith("Receipt") and not name.startswith("_")
    } == {"PublicationReceipt"}
    for forbidden in ("ExecutionResult", "InboundDelivery", "TaskExecutor"):
        assert not hasattr(base_module, forbidden)


def test_protocol_surfaces_and_signatures_are_exact() -> None:
    assert getattr(ObserverDelivery, "_is_protocol", False)
    assert getattr(FaultController, "_is_protocol", False)
    assert getattr(TaskTransport, "_is_protocol", False)

    assert _public_protocol_members(ObserverDelivery) == {"ack"}
    _assert_async_signature(ObserverDelivery, "ack", (), type(None))

    fault_methods: dict[str, tuple[tuple[str, object], ...]] = {
        "disconnect_progress_observer": (),
        "reconnect_progress_observer": (),
        "stop_worker": (("agent_id", str),),
        "start_worker": (("agent_id", str),),
        "restart_coordinator": (),
    }
    assert _public_protocol_members(FaultController) == set(fault_methods)
    for name, parameters in fault_methods.items():
        _assert_async_signature(
            FaultController,
            name,
            parameters,
            type(None),
        )

    properties = {
        "faults": FaultController,
        "mode": Mode,
        "outcome_ledger_enabled": bool,
    }
    methods: dict[
        str,
        tuple[tuple[tuple[str, object], ...], object],
    ] = {
        "start_terminal_observer": ((), type(None)),
        "start_progress_observer": ((), type(None)),
        "start_receiver": (
            (("agent_id", str), ("executor", TaskExecutor)),
            type(None),
        ),
        "wait_receiver_ready": (
            (("agent_id", str), ("timeout_s", float)),
            type(None),
        ),
        "submit_task": (
            (("envelope", Mapping[str, object]),),
            PublicationReceipt,
        ),
        "publish_progress": (
            (("envelope", Mapping[str, object]),),
            PublicationReceipt,
        ),
        "publish_terminal": (
            (("envelope", Mapping[str, object]),),
            PublicationReceipt,
        ),
        "publish_heartbeat": (
            (("envelope", Mapping[str, object]),),
            PublicationReceipt,
        ),
        "observe_terminal": (
            (("task_id", str), ("timeout_s", float)),
            ObservedEnvelope | None,
        ),
        "inspect_state": ((), TransportSnapshot),
        "close": ((), type(None)),
    }
    assert _public_protocol_members(TaskTransport) == {
        *properties,
        *methods,
    }
    for name, return_type in properties.items():
        _assert_read_only_property(TaskTransport, name, return_type)
    for name, (parameters, return_type) in methods.items():
        _assert_async_signature(
            TaskTransport,
            name,
            parameters,
            return_type,
        )


def test_modes_package_is_inert_and_base_has_no_runtime_executor_import() -> None:
    package_path = Path(__file__).parents[2] / "scripts" / "research" / "modes"
    package_tree = ast.parse((package_path / "__init__.py").read_text(encoding="utf-8"))
    assert len(package_tree.body) == 1
    only_statement = package_tree.body[0]
    assert isinstance(only_statement, ast.Expr)
    assert isinstance(only_statement.value, ast.Constant)
    assert isinstance(only_statement.value.value, str)

    base_tree = ast.parse((package_path / "base.py").read_text(encoding="utf-8"))
    top_level_imports = {
        node.module for node in base_tree.body if isinstance(node, ast.ImportFrom)
    }
    assert "adapters._common.task_executor" not in top_level_imports
    assert not any(
        module is not None
        and module.startswith(
            (
                "scripts.research.fixtures",
                "scripts.research.workloads",
                "scripts.research.modes.",
            )
        )
        for module in top_level_imports
    )
    assert "TaskExecutor" not in vars(base_module)


@dataclass
class _Counters:
    executor_calls: int = 0
    inbound_commits: int = 0
    observer_acks: int = 0


class _WorkerInboundDelivery:
    def __init__(
        self,
        envelope: Mapping[str, object],
        counters: _Counters,
        calls: list[str],
    ) -> None:
        self.worker_agent_id = "worker-1"
        self.raw = json.dumps(envelope, sort_keys=True).encode()
        self.delivery_count = 1
        self.stream_sequence: int | None = None
        self._counters = counters
        self._calls = calls

    async def in_progress(self) -> None:
        raise AssertionError("receiver must not extend this fake delivery")

    async def commit(self) -> None:
        self._counters.inbound_commits += 1
        self._calls.append("inbound.commit")

    async def retry(self) -> None:
        raise AssertionError("receiver must not retry this fake delivery")

    async def terminate(self) -> None:
        raise AssertionError("receiver must not terminate this fake delivery")


class _RecordingExecutor:
    def __init__(self, counters: _Counters, calls: list[str]) -> None:
        self._counters = counters
        self._calls = calls

    async def execute(self, delivery: InboundDelivery) -> ExecutionResult:
        self._counters.executor_calls += 1
        self._calls.append("executor.execute")
        await delivery.commit()
        return ExecutionResult("completed", None, None, "disabled")


class _ObserverAck:
    def __init__(self, counters: _Counters, calls: list[str]) -> None:
        self._counters = counters
        self._calls = calls

    async def ack(self) -> None:
        self._counters.observer_acks += 1
        self._calls.append("observer.ack")


class _RecordingFaults:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def disconnect_progress_observer(self) -> None:
        self._calls.append("faults.disconnect_progress_observer")

    async def reconnect_progress_observer(self) -> None:
        self._calls.append("faults.reconnect_progress_observer")

    async def stop_worker(self, agent_id: str) -> None:
        self._calls.append(f"faults.stop_worker:{agent_id}")

    async def start_worker(self, agent_id: str) -> None:
        self._calls.append(f"faults.start_worker:{agent_id}")

    async def restart_coordinator(self) -> None:
        self._calls.append("faults.restart_coordinator")


class _RecordingTransport:
    def __init__(self, counters: _Counters, calls: list[str]) -> None:
        self._counters = counters
        self._calls = calls
        self._faults = _RecordingFaults(calls)
        self._executor: TaskExecutor | None = None
        self._ready_agents: set[str] = set()
        self._terminals: dict[str, Mapping[str, object]] = {}
        self._observation_index = 0

    @property
    def faults(self) -> FaultController:
        return self._faults

    @property
    def mode(self) -> Mode:
        return Mode.CORE_ONLY

    @property
    def outcome_ledger_enabled(self) -> bool:
        return True

    async def start_terminal_observer(self) -> None:
        self._calls.append("transport.start_terminal_observer")

    async def start_progress_observer(self) -> None:
        self._calls.append("transport.start_progress_observer")

    async def start_receiver(
        self,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None:
        self._calls.append(f"transport.start_receiver:{agent_id}")
        self._executor = executor
        self._ready_agents.add(agent_id)

    async def wait_receiver_ready(
        self,
        agent_id: str,
        timeout_s: float,
    ) -> None:
        self._calls.append(f"transport.wait_receiver_ready:{agent_id}:{timeout_s}")
        if agent_id not in self._ready_agents or timeout_s <= 0:
            raise TimeoutError(agent_id)

    def _receipt(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        envelope_id = envelope.get("id")
        if not isinstance(envelope_id, str):
            raise TypeError("fake envelope requires a string id")
        return PublicationReceipt(
            envelope_id=envelope_id,
            accepted=True,
            transport=self.mode.value,
            stream=None,
            stream_sequence=None,
            duplicate=None,
            accepted_ns=time.perf_counter_ns(),
            application_bytes=len(json.dumps(envelope, sort_keys=True).encode()),
            wire_bytes=None,
        )

    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self._calls.append("transport.submit_task")
        receipt = self._receipt(envelope)
        if self._executor is None:
            raise RuntimeError("receiver is not started")
        delivery = _WorkerInboundDelivery(
            envelope,
            self._counters,
            self._calls,
        )
        await self._executor.execute(delivery)
        return receipt

    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self._calls.append("transport.publish_progress")
        return self._receipt(envelope)

    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self._calls.append("transport.publish_terminal")
        task_id = envelope.get("task_id")
        if not isinstance(task_id, str):
            raise TypeError("fake terminal requires a string task_id")
        self._terminals[task_id] = envelope
        return self._receipt(envelope)

    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self._calls.append("transport.publish_heartbeat")
        return self._receipt(envelope)

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        self._calls.append(f"transport.observe_terminal:{task_id}:{timeout_s}")
        envelope = self._terminals.get(task_id)
        if envelope is None:
            return None
        self._observation_index += 1
        return ObservedEnvelope(
            envelope=envelope,
            observed_ns=time.perf_counter_ns(),
            observation_index=self._observation_index,
            stream_sequence=None,
            delivery_count=1,
            replayed=False,
            delivery=_ObserverAck(self._counters, self._calls),
        )

    async def inspect_state(self) -> TransportSnapshot:
        self._calls.append("transport.inspect_state")
        return TransportSnapshot(
            mode=self.mode,
            streams={},
            consumers={},
            pending=None,
            ack_pending=None,
            connection_bytes={"client": 0},
            storage_bytes=0,
            message_count=0,
        )

    async def close(self) -> None:
        self._calls.append("transport.close")


@async_test
async def test_fake_transport_full_lifecycle_preserves_contract_boundaries() -> None:
    calls: list[str] = []
    counters = _Counters()
    executor = _RecordingExecutor(counters, calls)
    transport = _RecordingTransport(counters, calls)
    typed_transport: TaskTransport = transport
    _assert_publisher_compatibility(typed_transport)

    envelope: dict[str, object] = {
        "id": "request-1",
        "task_id": "task-1",
    }
    progress_envelope: dict[str, object] = {
        "id": "progress-1",
        "task_id": "task-1",
    }
    terminal_envelope: dict[str, object] = {
        "id": "terminal-1",
        "task_id": "task-1",
    }

    faults = typed_transport.faults
    await typed_transport.start_terminal_observer()
    await typed_transport.start_progress_observer()
    await typed_transport.start_receiver(
        "worker-1",
        cast(TaskExecutor, executor),
    )
    await typed_transport.wait_receiver_ready("worker-1", 5.0)
    accepted = await typed_transport.submit_task(envelope)
    progress = await typed_transport.publish_progress(progress_envelope)
    terminal = await typed_transport.publish_terminal(terminal_envelope)
    observed = await typed_transport.observe_terminal("task-1", 5.0)
    snapshot = await typed_transport.inspect_state()
    assert observed is not None
    if observed.delivery is not None:
        await observed.delivery.ack()
    await faults.disconnect_progress_observer()
    await faults.reconnect_progress_observer()
    await faults.stop_worker("worker-1")
    await faults.start_worker("worker-1")
    await faults.restart_coordinator()
    await typed_transport.close()

    assert accepted.envelope_id == envelope["id"]
    assert progress.envelope_id == progress_envelope["id"]
    assert terminal.envelope_id == terminal_envelope["id"]
    assert accepted.accepted is progress.accepted is terminal.accepted is True
    assert (
        0
        < accepted.accepted_ns
        <= progress.accepted_ns
        <= terminal.accepted_ns
        <= observed.observed_ns
    )
    assert observed.envelope["id"] == terminal.envelope_id
    assert observed.observation_index > 0
    assert observed.delivery_count > 0
    assert observed.replayed is False
    assert accepted.stream is None
    assert accepted.stream_sequence is None
    assert accepted.duplicate is None
    assert accepted.wire_bytes is None
    assert observed.stream_sequence is None
    assert snapshot.mode is Mode.CORE_ONLY
    assert snapshot.streams == {}
    assert snapshot.consumers == {}
    assert snapshot.pending is None
    assert snapshot.ack_pending is None
    assert snapshot.connection_bytes == {"client": 0}
    assert snapshot.storage_bytes == 0
    assert snapshot.message_count == 0
    assert counters == _Counters(
        executor_calls=1,
        inbound_commits=1,
        observer_acks=1,
    )
    assert calls == [
        "transport.start_terminal_observer",
        "transport.start_progress_observer",
        "transport.start_receiver:worker-1",
        "transport.wait_receiver_ready:worker-1:5.0",
        "transport.submit_task",
        "executor.execute",
        "inbound.commit",
        "transport.publish_progress",
        "transport.publish_terminal",
        "transport.observe_terminal:task-1:5.0",
        "transport.inspect_state",
        "observer.ack",
        "faults.disconnect_progress_observer",
        "faults.reconnect_progress_observer",
        "faults.stop_worker:worker-1",
        "faults.start_worker:worker-1",
        "faults.restart_coordinator",
        "transport.close",
    ]


@async_test
async def test_timeout_and_replay_signals_remain_distinct() -> None:
    calls: list[str] = []
    transport = _RecordingTransport(_Counters(), calls)

    with pytest.raises(TimeoutError):
        await transport.wait_receiver_ready("missing-worker", 0.0)
    assert await transport.observe_terminal("missing-task", 0.0) is None

    replayed = ObservedEnvelope(
        envelope={"id": "terminal-1"},
        observed_ns=time.perf_counter_ns(),
        observation_index=2,
        stream_sequence=None,
        delivery_count=2,
        replayed=True,
        delivery=None,
    )
    publication = PublicationReceipt(
        envelope_id="terminal-1",
        accepted=True,
        transport="all-durable",
        stream="TASK_RESULTS",
        stream_sequence=7,
        duplicate=False,
        accepted_ns=time.perf_counter_ns(),
        application_bytes=1,
        wire_bytes=None,
    )

    assert replayed.replayed is True
    assert replayed.envelope["id"] == publication.envelope_id
    assert publication.duplicate is False
