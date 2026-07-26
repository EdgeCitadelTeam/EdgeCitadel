"""Contract tests for the ephemeral Core NATS transport."""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
import logging
import subprocess
import time
import traceback
import typing
import warnings
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import TypeVar, cast

import pytest
from nats.aio.client import Client as NATS
from nats.errors import AuthorizationError
from nats.errors import Error as NATSError

import nats
import scripts.research.modes.core_nats as core_module
from adapters._common.task_executor import InjectedCrash, TaskExecutor
from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import canonical_json, default_validator
from scripts.research.modes.base import EventSink, Mode, TaskTransport
from scripts.research.modes.core_nats import CoreNatsTransport
from tests.research.nats_server import NatsServer

TOKEN = "b" * 64
NOW = "2026-07-25T12:00:00.000Z"
_OWNER_LABEL = "ai.edgecitadel.owner=test-nats"
_F = TypeVar("_F", bound=Callable[..., object])


def _typed_decorator(value: object) -> Callable[[_F], _F]:
    return cast(Callable[[_F], _F], value)


def _asyncio_test(function: _F) -> _F:
    return _typed_decorator(pytest.mark.asyncio)(function)


def _parametrize(*args: object, **kwargs: object) -> Callable[[_F], _F]:
    factory = cast(Callable[..., object], pytest.mark.parametrize)
    return _typed_decorator(factory(*args, **kwargs))


def docker_test(function: _F) -> _F:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", pytest.PytestUnknownMarkWarning)
        decorator = pytest.mark.docker
    return _typed_decorator(decorator)(function)


def _as_connection_factory(
    value: object,
) -> Callable[..., Awaitable[NATS]]:
    return cast(Callable[..., Awaitable[NATS]], value)


def _as_task_executor(value: object) -> TaskExecutor:
    return cast(TaskExecutor, value)


class _EventSink:
    def emit(self, event: Mapping[str, object]) -> None:
        del event


class _RecordingEventSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(event)


async def _unused_connection_factory(**kwargs: object) -> object:
    del kwargs
    raise AssertionError("constructor must connect lazily")


def _accept_task_transport(transport: TaskTransport) -> TaskTransport:
    return transport


def test_core_public_export_and_task_transport_contract() -> None:
    assert list(core_module.__all__) == ["CoreNatsTransport"]
    sink: EventSink = _EventSink()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    assert _accept_task_transport(transport) is transport
    assert transport.mode is Mode.CORE_ONLY
    assert transport.outcome_ledger_enabled is True
    assert transport.faults is transport.faults


def test_core_imports_event_sink_from_base_contract() -> None:
    tree = ast.parse(inspect.getsource(core_module))
    event_sink_imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and any(alias.name == "EventSink" for alias in node.names)
    }
    assert event_sink_imports == {"scripts.research.modes.base"}


def test_core_constructor_signature_annotations_and_defaults_are_exact() -> None:
    signature = inspect.signature(CoreNatsTransport)
    expected_names = [
        "nats_url",
        "run_id",
        "token",
        "event_sink",
        "agent_card",
        "coordinator_restart",
        "worker_stop",
        "worker_start",
        "connection_factory",
        "evidence_clock_ns",
        "epoch_now",
        "uuid4",
        "sleep",
    ]
    assert list(signature.parameters) == expected_names
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in signature.parameters.values()
    )
    parameters = signature.parameters
    assert [parameters[name].default for name in expected_names[:4]] == [
        inspect.Parameter.empty
    ] * 4
    assert parameters["agent_card"].default is None
    assert parameters["coordinator_restart"].default is None
    assert parameters["worker_stop"].default is None
    assert parameters["worker_start"].default is None
    assert parameters["connection_factory"].default is nats.connect
    assert parameters["evidence_clock_ns"].default is time.perf_counter_ns
    assert parameters["epoch_now"].default is core_module._now_iso
    assert parameters["uuid4"].default is core_module._uuid4
    assert parameters["sleep"].default is asyncio.sleep
    assert typing.get_type_hints(CoreNatsTransport.__init__) == {
        "nats_url": str,
        "run_id": str,
        "token": str,
        "event_sink": EventSink,
        "agent_card": Mapping[str, object] | None,
        "coordinator_restart": Callable[[], Awaitable[str | None]] | None,
        "worker_stop": Callable[[str], Awaitable[None]] | None,
        "worker_start": Callable[[str], Awaitable[None]] | None,
        "connection_factory": Callable[..., Awaitable[NATS]],
        "evidence_clock_ns": Callable[[], int],
        "epoch_now": Callable[[], str],
        "uuid4": Callable[[], str],
        "sleep": Callable[[float], Awaitable[None]],
        "return": type(None),
    }


def test_core_resolved_config_is_exact_fresh_and_read_only() -> None:
    endpoint_sentinel = "nats://endpoint-sentinel.invalid:4222"
    run_sentinel = "run-sentinel"
    token_sentinel = "b1" * 32
    card = {
        "name": "agent-sentinel",
        "metadata": {"credential": "credential-sentinel"},
    }

    async def callback_sentinel() -> str:
        return "callback-sentinel"

    transport = CoreNatsTransport(
        nats_url=endpoint_sentinel,
        run_id=run_sentinel,
        token=token_sentinel,
        event_sink=_EventSink(),
        agent_card=card,
        coordinator_restart=callback_sentinel,
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    card["name"] = "mutated-agent"
    cast_metadata = card["metadata"]
    assert isinstance(cast_metadata, dict)
    cast_metadata["credential"] = "mutated-credential"
    expected = {
        "mode": "core-only",
        "ablation": "full-contract",
        "nats_msg_id": False,
        "outcome_ledger": True,
    }
    first = transport.resolved_config
    second = transport.resolved_config
    assert type(first) is MappingProxyType
    assert first == expected
    assert first is not second
    assert {key: type(value) for key, value in first.items()} == {
        "mode": str,
        "ablation": str,
        "nats_msg_id": bool,
        "outcome_ledger": bool,
    }
    with pytest.raises(TypeError):
        first["mode"] = "changed"  # type: ignore[index]
    with pytest.raises((AttributeError, TypeError)):
        first.clear()  # type: ignore[attr-defined]
    mutable_copy = dict(first)
    mutable_copy.clear()
    assert dict(transport.resolved_config) == expected
    assert dict(second) == expected
    assert json.loads(json.dumps(dict(second), sort_keys=True)) == expected
    serialized = json.dumps(dict(second), sort_keys=True)
    for sentinel in (
        endpoint_sentinel,
        run_sentinel,
        token_sentinel,
        "agent-sentinel",
        "credential-sentinel",
        "mutated-agent",
        "mutated-credential",
        "callback-sentinel",
    ):
        assert sentinel not in serialized
    descriptor = inspect.getattr_static(type(transport), "resolved_config")
    assert isinstance(descriptor, property)
    assert descriptor.fset is None


def _agent_card(agent_id: str = "worker-1") -> dict[str, object]:
    return {
        "name": agent_id,
        "description": "Core-only test worker.",
        "version": "1.0.0",
        "url": f"nats://edgecitadel/agents.{agent_id}.inbox",
        "provider": {"organization": "EdgeCitadel"},
        "capabilities": {"streaming": True},
        "securitySchemes": {},
        "metadata": {
            "runtime.kind": "native",
            "runtime.roles": ["worker"],
            "runtime.conformance": "L1",
            "runtime.heartbeat_interval_sec": 10,
        },
    }


def _command(
    *,
    envelope_id: str = "10000000-0000-4000-8000-000000000001",
    task_id: str = "20000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "command",
        "sender_id": "requester-1",
        "recipient_id": "worker-1",
        "task_id": task_id,
        "timestamp": NOW,
        "payload": {"body": "nonce"},
    }


def _terminal(
    *,
    envelope_id: str = "30000000-0000-4000-8000-000000000001",
    task_id: str = "20000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "result",
        "sender_id": "worker-1",
        "recipient_id": "requester-1",
        "task_id": task_id,
        "context_id": task_id,
        "hop_count": 0,
        "task_state": "completed",
        "timestamp": NOW,
        "payload": {"body": "edgecitadel:nonce"},
    }


def _progress(
    *,
    envelope_id: str = "40000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    task_id = "20000000-0000-4000-8000-000000000001"
    return {
        "v": 1,
        "id": envelope_id,
        "type": "task.progress",
        "sender_id": "worker-1",
        "recipient_id": "requester-1",
        "task_id": task_id,
        "context_id": task_id,
        "hop_count": 0,
        "task_state": "working",
        "timestamp": NOW,
        "payload": {"message": "working", "progress": 5},
    }


def _heartbeat(
    *,
    envelope_id: str = "50000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "heartbeat",
        "sender_id": "worker-1",
        "timestamp": NOW,
        "payload": {},
    }


def _status(
    *,
    envelope_id: str = "60000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "status",
        "sender_id": "worker-1",
        "timestamp": NOW,
        "agent_state": "online",
        "payload": {},
    }


def _register(
    *,
    envelope_id: str = "70000000-0000-4000-8000-000000000001",
) -> dict[str, object]:
    return {
        "v": 1,
        "id": envelope_id,
        "type": "register",
        "sender_id": "worker-1",
        "timestamp": NOW,
        "payload": _agent_card(),
    }


def _subject_matches(pattern: str, subject: str) -> bool:
    pattern_parts = pattern.split(".")
    subject_parts = subject.split(".")
    return len(pattern_parts) == len(subject_parts) and all(
        expected == "*" or expected == actual
        for expected, actual in zip(pattern_parts, subject_parts, strict=True)
    )


class _FakeMessage:
    def __init__(self, subject: str, data: bytes) -> None:
        self.subject = subject
        self.data = data


class _FakeSubscription:
    def __init__(
        self,
        connection: _FakeNATS,
        subject: str,
        callback: Callable[[_FakeMessage], Awaitable[None]],
    ) -> None:
        self.connection = connection
        self.subject = subject
        self.callback = callback
        self.active = True
        self._wait_for_msgs_task: asyncio.Task[None] | None = None

    async def unsubscribe(self, limit: int = 0) -> None:
        assert limit == 0
        self.active = False
        self.connection.timeline.append(("unsubscribe", self.subject))


class _FakeNATS:
    def __init__(
        self,
        timeline: list[tuple[str, object]],
        *,
        in_bytes: int = 0,
        out_bytes: int = 0,
    ) -> None:
        self.timeline = timeline
        self.subscriptions: list[_FakeSubscription] = []
        self.stats = {"in_bytes": in_bytes, "out_bytes": out_bytes}
        self.is_closed = False

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        **kwargs: object,
    ) -> None:
        assert kwargs == {}
        self.timeline.append(("publish", (subject, payload)))

    async def flush(self, timeout: int = 10) -> None:
        assert timeout == 10
        self.timeline.append(("flush", None))

    async def subscribe(
        self,
        subject: str,
        *,
        cb: Callable[[_FakeMessage], Awaitable[None]],
        **kwargs: object,
    ) -> _FakeSubscription:
        assert kwargs == {}
        subscription = _FakeSubscription(self, subject, cb)
        self.subscriptions.append(subscription)
        self.timeline.append(("subscribe", subject))
        return subscription

    async def close(self) -> None:
        self.is_closed = True
        self.timeline.append(("close", None))

    def jetstream(self) -> object:
        raise AssertionError("Core-only transport must not access JetStream")

    async def deliver(self, subject: str, envelope: Mapping[str, object]) -> None:
        data = canonical_json(envelope)
        callbacks = [
            subscription.callback
            for subscription in self.subscriptions
            if subscription.active and _subject_matches(subscription.subject, subject)
        ]
        for callback in callbacks:
            await callback(_FakeMessage(subject, data))


class _FakeConnectionFactory:
    def __init__(
        self,
        *outcomes: _FakeNATS | BaseException,
    ) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[Mapping[str, object]] = []

    async def __call__(self, **kwargs: object) -> object:
        self.calls.append(dict(kwargs))
        if not self.outcomes:
            raise AssertionError("unexpected Core NATS connection")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _TimelineClock:
    def __init__(self, timeline: list[tuple[str, object]]) -> None:
        self.timeline = timeline
        self.value = 10_000

    def __call__(self) -> int:
        self.value += 1
        self.timeline.append(("clock", self.value))
        return self.value


class _UUIDs:
    def __init__(self, *values: str) -> None:
        self.values = list(values)

    def __call__(self) -> str:
        if not self.values:
            raise AssertionError("unexpected UUID allocation")
        return self.values.pop(0)


def _require_explicit_docker(request: pytest.FixtureRequest) -> None:
    if request.config.option.markexpr != "docker":
        pytest.skip("run explicitly with -m docker")
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        pytest.skip("Docker is unavailable")
    if result.returncode != 0:
        pytest.skip("Docker is unavailable")


def _owned_docker_resources(kind: str) -> set[str]:
    if kind == "container":
        command = [
            "docker",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            f"label={_OWNER_LABEL}",
        ]
    elif kind == "volume":
        command = [
            "docker",
            "volume",
            "ls",
            "--quiet",
            "--filter",
            f"label={_OWNER_LABEL}",
        ]
    else:
        raise AssertionError("unknown Docker resource kind")
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return {line for line in result.stdout.splitlines() if line}


def _assert_owned_docker_inventory_empty() -> None:
    assert not _owned_docker_resources("container")
    assert not _owned_docker_resources("volume")


async def _wait_for_event_count(
    sink: _RecordingEventSink,
    event_name: str,
    count: int,
) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        matches = [event for event in sink.events if event["event"] == event_name]
        if len(matches) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {event_name}")


@_parametrize(
    ("field", "value"),
    [
        ("nats_url", "http://nats.invalid:4222"),
        ("nats_url", "nats://user@nats.invalid:4222"),
        ("nats_url", "nats://nats.invalid:4222?token=secret"),
        ("nats_url", "nats://nats.invalid:4222?"),
        ("nats_url", "nats://nats.invalid:4222#"),
        ("run_id", "bad.run"),
        ("run_id", True),
        ("token", "B" * 64),
        ("token", "b" * 63),
    ],
)
def test_core_constructor_rejects_invalid_endpoint_identity_and_token(
    field: str,
    value: object,
) -> None:
    arguments: dict[str, typing.Any] = {
        "nats_url": "nats://127.0.0.1:4222",
        "run_id": "run-1",
        "token": TOKEN,
        "event_sink": _EventSink(),
        "connection_factory": _unused_connection_factory,
    }
    arguments[field] = value
    with pytest.raises(ValueError):
        CoreNatsTransport(**arguments)


@_asyncio_test
async def test_core_publications_use_exact_subjects_canonical_bytes_and_flush_order() -> (
    None
):
    timeline: list[tuple[str, object]] = []
    connection = _FakeNATS(timeline)
    factory = _FakeConnectionFactory(connection)
    sink = _RecordingEventSink()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        connection_factory=_as_connection_factory(factory),
        evidence_clock_ns=_TimelineClock(timeline),
        epoch_now=lambda: NOW,
    )
    envelopes = [_command(), _terminal(), _progress(), _heartbeat()]
    methods = [
        transport.submit_task,
        transport.publish_terminal,
        transport.publish_progress,
        transport.publish_heartbeat,
    ]
    expected_subjects = [
        "artifact.run-1.agents.worker-1.inbox",
        (
            "artifact.run-1.agents.requester-1.result."
            "20000000-0000-4000-8000-000000000001"
        ),
        (
            "artifact.run-1.agents.worker-1.task_progress."
            "20000000-0000-4000-8000-000000000001"
        ),
        "artifact.run-1.agents.worker-1.heartbeat",
    ]
    try:
        receipts = [
            await method(envelope)
            for method, envelope in zip(methods, envelopes, strict=True)
        ]
        publishes = [
            cast(tuple[str, bytes], value)
            for kind, value in timeline
            if kind == "publish"
        ]
        assert [subject for subject, _ in publishes] == expected_subjects
        assert [payload for _, payload in publishes] == [
            canonical_json(envelope) for envelope in envelopes
        ]
        for receipt, envelope in zip(receipts, envelopes, strict=True):
            assert receipt.envelope_id == envelope["id"]
            assert receipt.accepted is True
            assert receipt.transport == "core-only"
            assert receipt.stream is None
            assert receipt.stream_sequence is None
            assert receipt.duplicate is None
            assert receipt.wire_bytes is None
            assert receipt.application_bytes == len(canonical_json(envelope))
            assert receipt.accepted_ns > 0
        for index, (kind, _) in enumerate(timeline):
            if kind == "publish":
                assert timeline[index + 1][0] == "flush"
                assert timeline[index + 2][0] == "clock"
        assert (
            len(
                [
                    event
                    for event in sink.events
                    if event["event"] == "transport.publication_accepted"
                ]
            )
            == 4
        )
        assert factory.calls and set(factory.calls[0]) == {
            "allow_reconnect",
            "closed_cb",
            "connect_timeout",
            "disconnected_cb",
            "error_cb",
            "max_reconnect_attempts",
            "servers",
            "token",
        }
        assert factory.calls[0]["servers"] == ["nats://127.0.0.1:4222"]
        assert factory.calls[0]["token"] == TOKEN
        assert factory.calls[0]["allow_reconnect"] is False
        assert factory.calls[0]["max_reconnect_attempts"] == 0
        assert factory.calls[0]["connect_timeout"] == 2
    finally:
        await transport.close()
    assert connection.is_closed is True


class _RecordingExecutor:
    def __init__(self) -> None:
        self.deliveries: list[object] = []
        self.called = asyncio.Event()

    async def execute(self, delivery: object) -> None:
        self.deliveries.append(delivery)
        await typing.cast(typing.Any, delivery).in_progress()
        await typing.cast(typing.Any, delivery).commit()
        await typing.cast(typing.Any, delivery).retry()
        await typing.cast(typing.Any, delivery).terminate()
        self.called.set()


@_asyncio_test
async def test_core_receiver_subscribes_registers_then_reports_ready() -> None:
    timeline: list[tuple[str, object]] = []
    connection = _FakeNATS(timeline)
    factory = _FakeConnectionFactory(connection)
    sink = _RecordingEventSink()
    executor = _RecordingExecutor()
    register_id = "70000000-0000-4000-8000-000000000001"
    card = _agent_card()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        agent_card=card,
        connection_factory=_as_connection_factory(factory),
        uuid4=_UUIDs(register_id),
        epoch_now=lambda: NOW,
    )
    card["name"] = "mutated"
    try:
        await transport.start_receiver(
            "worker-1",
            _as_task_executor(executor),
        )
        await transport.wait_receiver_ready("worker-1", 1)
        operations = [kind for kind, _ in timeline]
        subscribe_index = operations.index("subscribe")
        register_index = next(
            index
            for index, (kind, value) in enumerate(timeline)
            if kind == "publish"
            and cast(tuple[str, bytes], value)[0].endswith(".register")
        )
        assert operations[subscribe_index + 1] == "flush"
        assert register_index > subscribe_index + 1
        assert operations[register_index + 1] == "flush"
        subject, payload = cast(tuple[str, bytes], timeline[register_index][1])
        assert subject == "artifact.run-1.agents.worker-1.register"
        registration = json.loads(payload)
        assert registration == _register(envelope_id=register_id)
        default_validator().validate_envelope(registration)

        request = _command()
        await connection.deliver(
            "artifact.run-1.agents.worker-1.inbox",
            request,
        )
        await asyncio.wait_for(executor.called.wait(), timeout=1)
        delivery = typing.cast(typing.Any, executor.deliveries[0])
        assert delivery.worker_agent_id == "worker-1"
        assert delivery.raw == canonical_json(request)
        assert delivery.delivery_count == 1
        assert delivery.stream_sequence is None
        ready = [
            event
            for event in sink.events
            if event["event"] == "transport.receiver_ready"
        ]
        assert ready[-1]["data"] == {
            "agent_id": "worker-1",
            "kind": "receiver",
        }
    finally:
        await transport.close()


@_asyncio_test
async def test_core_observers_separate_terminal_transient_and_registration() -> None:
    timeline: list[tuple[str, object]] = []
    connection = _FakeNATS(timeline)
    sink = _RecordingEventSink()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
        evidence_clock_ns=_TimelineClock(timeline),
        epoch_now=lambda: NOW,
    )
    try:
        await transport.start_terminal_observer()
        await transport.start_progress_observer()
        assert [subscription.subject for subscription in connection.subscriptions] == [
            "artifact.run-1.agents.*.result.*",
            "artifact.run-1.agents.*.task_progress.*",
            "artifact.run-1.agents.*.heartbeat",
            "artifact.run-1.agents.*.status",
            "artifact.run-1.agents.*.register",
        ]
        await connection.deliver(
            (
                "artifact.run-1.agents.requester-1.result."
                "20000000-0000-4000-8000-000000000001"
            ),
            _terminal(),
        )
        await connection.deliver(
            (
                "artifact.run-1.agents.worker-1.task_progress."
                "20000000-0000-4000-8000-000000000001"
            ),
            _progress(),
        )
        await connection.deliver(
            "artifact.run-1.agents.worker-1.heartbeat",
            _heartbeat(),
        )
        await connection.deliver(
            "artifact.run-1.agents.worker-1.status",
            _status(),
        )
        await connection.deliver(
            "artifact.run-1.agents.worker-1.register",
            _register(),
        )
        observed = await transport.observe_terminal(
            "20000000-0000-4000-8000-000000000001",
            0,
        )
        assert observed is not None
        assert observed.envelope == _terminal()
        assert observed.observation_index == 1
        assert observed.stream_sequence is None
        assert observed.delivery_count == 1
        assert observed.replayed is False
        assert observed.delivery is None

        transient = [
            event
            for event in sink.events
            if event["event"] == "transport.transient_observed"
        ]
        registration = [
            event
            for event in sink.events
            if event["event"] == "transport.registration_observed"
        ]
        assert [event["data"]["envelope_type"] for event in transient] == [  # type: ignore[index]
            "task.progress",
            "heartbeat",
            "status",
        ]
        assert len(registration) == 1
        assert "payload" not in cast(Mapping[str, object], registration[0]["data"])

        old_subscriptions = list(connection.subscriptions[1:])
        await transport.faults.disconnect_progress_observer()
        assert all(not subscription.active for subscription in old_subscriptions)
        await transport.faults.reconnect_progress_observer()
        assert len(connection.subscriptions) == 9
        second = _progress(envelope_id="40000000-0000-4000-8000-000000000002")
        await connection.deliver(
            (
                "artifact.run-1.agents.worker-1.task_progress."
                "20000000-0000-4000-8000-000000000001"
            ),
            second,
        )
        assert [
            event["data"]["observation_index"]  # type: ignore[index]
            for event in sink.events
            if event["event"] == "transport.transient_observed"
        ] == [1, 2, 3, 4]
    finally:
        await transport.close()


@_asyncio_test
async def test_core_rejects_registration_card_for_different_sender() -> None:
    connection = _FakeNATS([])
    sink = _RecordingEventSink()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=sink,
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    mismatched = _register()
    mismatched["payload"] = _agent_card("other-worker")
    try:
        await transport.start_progress_observer()
        with pytest.raises(
            RuntimeError,
            match=r"^invalid core nats message$",
        ):
            await connection.deliver(
                "artifact.run-1.agents.worker-1.register",
                mismatched,
            )
        assert transport._registration_observation_index == 0
        assert all(
            event["event"] != "transport.registration_observed" for event in sink.events
        )
    finally:
        await transport.close()


@_parametrize(
    "source_error",
    [
        AuthorizationError(),
        NATSError("nats: 'Authorization Violation' secret-source-text"),
    ],
)
@_asyncio_test
async def test_core_authentication_failures_are_exact_secret_free_permission_error(
    source_error: Exception,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "deadbeef" * 8
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=secret,
        event_sink=_RecordingEventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(source_error)),
    )
    caplog.set_level(logging.DEBUG)
    with pytest.raises(
        PermissionError,
        match=r"^transport authentication failed$",
    ) as raised:
        await transport.submit_task(_command())
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    rendered = str(raised.value) + repr(raised.value) + caplog.text
    assert secret not in rendered
    assert "secret-source-text" not in rendered
    await transport.close()


@_asyncio_test
async def test_core_cancellation_precedes_pre_establishment_auth_callback() -> None:
    flush_started = asyncio.Event()

    class PendingAuthNATS(_FakeNATS):
        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            flush_started.set()
            await asyncio.Event().wait()

    candidate = PendingAuthNATS([])

    async def factory(**kwargs: object) -> NATS:
        error_callback = cast(
            Callable[[Exception], Awaitable[None]],
            kwargs["error_cb"],
        )
        await error_callback(AuthorizationError())
        return cast(NATS, candidate)

    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=factory,
    )
    submission = asyncio.create_task(transport.submit_task(_command()))
    try:
        await flush_started.wait()
        submission.cancel()
        with pytest.raises(asyncio.CancelledError) as raised:
            await submission
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert candidate.is_closed is True
        assert transport._pending_candidates == []
    finally:
        if not submission.done():
            await asyncio.gather(submission, return_exceptions=True)
        await transport.close()


@_asyncio_test
async def test_core_base_exception_precedes_pre_establishment_auth_callback() -> None:
    source = InjectedCrash("private injected crash")

    class CrashingAuthNATS(_FakeNATS):
        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            raise source

    candidate = CrashingAuthNATS([])

    async def factory(**kwargs: object) -> NATS:
        error_callback = cast(
            Callable[[Exception], Awaitable[None]],
            kwargs["error_cb"],
        )
        await error_callback(AuthorizationError())
        return cast(NATS, candidate)

    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=factory,
    )
    try:
        with pytest.raises(
            InjectedCrash,
            match=r"^core nats connection failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert raised.value is not source
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert "private injected crash" not in str(raised.value)
        assert candidate.is_closed is True
        assert transport._pending_candidates == []
    finally:
        await transport.close()


@_asyncio_test
async def test_core_callback_authentication_failure_is_discarded_before_raise() -> None:
    source = NATSError("nats: 'Authorization Violation' private callback detail")
    candidate = _FakeNATS([])

    async def factory(**kwargs: object) -> object:
        callback = cast(
            Callable[[Exception], Awaitable[None]],
            kwargs["error_cb"],
        )
        await callback(source)
        return candidate

    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(factory),
    )
    try:
        with pytest.raises(
            PermissionError,
            match=r"^transport authentication failed$",
        ) as raised:
            await transport.submit_task(_command())
        production_frames = [
            frame
            for frame, _ in traceback.walk_tb(raised.value.__traceback__)
            if frame.f_code.co_filename.endswith("/core_nats.py")
        ]
        assert production_frames
        for frame in production_frames:
            assert source not in frame.f_locals.values()
            for value in frame.f_locals.values():
                if isinstance(value, (list, tuple, set)):
                    assert source not in value
                elif isinstance(value, dict):
                    assert source not in value.values()
        assert candidate.is_closed is True
    finally:
        await transport.close()


@_asyncio_test
async def test_core_non_authentication_connection_failure_preserves_type() -> None:
    endpoint = "nats://private-nats.invalid:4222"
    private_path = "/private/nats/credentials"
    source = RuntimeError(f"ordinary failure at {endpoint}{private_path}")
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(source)),
    )
    with pytest.raises(
        RuntimeError,
        match=r"^core nats connection failed$",
    ) as raised:
        await transport.submit_task(_command())
    assert type(raised.value) is type(source)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    rendered = str(raised.value) + repr(raised.value)
    assert endpoint not in rendered
    assert private_path not in rendered
    await transport.close()


@_asyncio_test
async def test_core_structured_connection_failure_drops_private_fields() -> None:
    private_bytes = b"private-token-and-endpoint"
    private_reason = "private /credential/path"
    source = UnicodeDecodeError(
        "utf-8",
        private_bytes,
        0,
        1,
        private_reason,
    )
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(source)),
    )
    try:
        with pytest.raises(UnicodeDecodeError) as raised:
            await transport.submit_task(_command())
        assert type(raised.value) is type(source)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        rendered = (
            str(raised.value)
            + repr(raised.value)
            + repr(raised.value.object)
            + raised.value.reason
        )
        assert private_bytes.decode() not in rendered
        assert private_reason not in rendered
        assert raised.value.object != private_bytes
    finally:
        await transport.close()


@_asyncio_test
@_parametrize("operation", ["publish", "flush"])
async def test_core_post_connect_failure_is_sanitized(
    operation: str,
) -> None:
    private_endpoint = "nats://private.invalid:4222/private/path"
    private_bytes = b"private-token-and-endpoint"
    source: Exception
    if operation == "publish":
        source = RuntimeError(f"publish failed at {private_endpoint}")
    else:
        source = UnicodeDecodeError(
            "utf-8",
            private_bytes,
            0,
            1,
            private_endpoint,
        )

    class OperationFailingNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.flush_calls = 0

        async def publish(
            self,
            subject: str,
            payload: bytes = b"",
            **kwargs: object,
        ) -> None:
            if operation == "publish":
                raise source
            await super().publish(subject, payload, **kwargs)

        async def flush(self, timeout: int = 10) -> None:
            self.flush_calls += 1
            if operation == "flush" and self.flush_calls == 2:
                raise source
            await super().flush(timeout)

    connection = OperationFailingNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    try:
        with pytest.raises(type(source)) as raised:
            await transport.submit_task(_command())
        assert raised.value is not source
        assert type(raised.value) is type(source)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        if type(raised.value) is RuntimeError:
            assert str(raised.value) == "core nats operation failed"
        if isinstance(raised.value, UnicodeDecodeError):
            assert raised.value.reason == "core nats operation failed"
        production_frames = [
            frame
            for frame, _ in traceback.walk_tb(raised.value.__traceback__)
            if frame.f_code.co_filename.endswith("/core_nats.py")
        ]
        assert production_frames
        assert all(source not in frame.f_locals.values() for frame in production_frames)
        rendered = str(raised.value) + repr(raised.value)
        if isinstance(raised.value, UnicodeDecodeError):
            rendered += repr(raised.value.object) + raised.value.reason
        assert private_endpoint not in rendered
        assert private_bytes.decode() not in rendered
        assert TOKEN not in rendered
    finally:
        await transport.close()


@_asyncio_test
async def test_core_observe_terminal_timeout_rechecks_background_failure() -> None:
    connection = _FakeNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    source = RuntimeError("retained background failure")
    try:
        await transport.start_terminal_observer()
        observation = asyncio.create_task(
            transport.observe_terminal(
                "20000000-0000-4000-8000-000000000001",
                0.01,
            )
        )
        await asyncio.sleep(0)
        transport._background_failure = source
        with pytest.raises(
            RuntimeError, match=r"^retained background failure$"
        ) as raised:
            await observation
        assert raised.value is source
    finally:
        await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
@_parametrize("role", ["terminal", "progress", "receiver"])
async def test_core_failed_subscription_setup_rolls_back_live_subscriptions(
    role: str,
) -> None:
    class SetupFailingNATS(_FakeNATS):
        def __init__(self, failure_role: str) -> None:
            super().__init__([])
            self.failure_role = failure_role
            self.flush_calls = 0
            self.subscribe_calls = 0
            self.failed = False

        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            self.flush_calls += 1
            if not self.failed and (
                (self.failure_role == "terminal" and self.flush_calls == 2)
                or (self.failure_role == "receiver" and self.flush_calls == 3)
            ):
                self.failed = True
                raise RuntimeError(f"{self.failure_role} setup failed")
            await super().flush(timeout)

        async def subscribe(
            self,
            subject: str,
            *,
            cb: Callable[[_FakeMessage], Awaitable[None]],
            **kwargs: object,
        ) -> _FakeSubscription:
            self.subscribe_calls += 1
            if (
                not self.failed
                and self.failure_role == "progress"
                and self.subscribe_calls == 3
            ):
                self.failed = True
                raise RuntimeError("progress setup failed")
            return await super().subscribe(subject, cb=cb, **kwargs)

    connection = SetupFailingNATS(role)
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        agent_card=_agent_card(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
        uuid4=_UUIDs(
            "70000000-0000-4000-8000-000000000001",
            "70000000-0000-4000-8000-000000000002",
        ),
        epoch_now=lambda: NOW,
    )
    executor = _RecordingExecutor()

    async def start() -> None:
        if role == "terminal":
            await transport.start_terminal_observer()
        elif role == "progress":
            await transport.start_progress_observer()
        else:
            await transport.start_receiver(
                "worker-1",
                _as_task_executor(executor),
            )

    try:
        with pytest.raises(
            RuntimeError,
            match=r"^core nats operation failed$",
        ) as raised:
            await start()
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert f"{role} setup failed" not in str(raised.value)
        assert connection.subscriptions
        assert all(not subscription.active for subscription in connection.subscriptions)
        assert transport._terminal_subscription is None
        assert transport._progress_subscriptions == []
        assert transport._receiver_subscription is None
        assert transport._receiver_ready is None

        await start()
        assert any(subscription.active for subscription in connection.subscriptions)
    finally:
        await transport.close()


@_asyncio_test
@_parametrize("role", ["terminal", "progress", "receiver"])
async def test_core_readiness_event_failure_rolls_back_subscriptions(role: str) -> None:
    class ReadinessFailingSink:
        def emit(self, event: Mapping[str, object]) -> None:
            data = cast(Mapping[str, object], event["data"])
            target_event = (
                "transport.receiver_ready"
                if role == "receiver"
                else "transport.observer_ready"
            )
            if event["event"] == target_event and data["kind"] == role:
                raise RuntimeError(f"{role} readiness evidence failed")

    connection = _FakeNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=ReadinessFailingSink(),
        agent_card=_agent_card(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
        uuid4=_UUIDs("70000000-0000-4000-8000-000000000001"),
        epoch_now=lambda: NOW,
    )
    executor = _RecordingExecutor()

    async def start() -> None:
        if role == "terminal":
            await transport.start_terminal_observer()
        elif role == "progress":
            await transport.start_progress_observer()
        else:
            await transport.start_receiver(
                "worker-1",
                _as_task_executor(executor),
            )

    try:
        with pytest.raises(
            RuntimeError,
            match=rf"^{role} readiness evidence failed$",
        ):
            await start()
        assert connection.subscriptions
        assert all(not subscription.active for subscription in connection.subscriptions)
        assert transport._terminal_subscription is None
        assert transport._progress_subscriptions == []
        assert transport._receiver_subscription is None
        assert transport._receiver_ready is None
    finally:
        await transport.close()


@_asyncio_test
async def test_core_restart_retries_after_transient_attempt_callback() -> None:
    first = _FakeNATS([])
    replacement = _FakeNATS([])
    calls: list[Mapping[str, object]] = []
    sleeps: list[float] = []

    async def factory(**kwargs: object) -> object:
        calls.append(dict(kwargs))
        if len(calls) == 1:
            return first
        if len(calls) == 2:
            callback = cast(
                Callable[[Exception], Awaitable[None]],
                kwargs["error_cb"],
            )
            await callback(RuntimeError("transient attempt callback"))
            raise ConnectionError("transient connect failure")
        if len(calls) == 3:
            return replacement
        raise AssertionError("unexpected Core NATS connection")

    async def bounded_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) > 1:
            raise AssertionError("transient callback poisoned restart")

    async def restart() -> None:
        return None

    transport = CoreNatsTransport(
        nats_url="nats://stable-nats:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(factory),
        sleep=bounded_sleep,
    )
    try:
        await transport.start_terminal_observer()
        await transport.faults.restart_coordinator()
        assert len(calls) == 3
        assert sleeps == [0.1]
        assert first.is_closed is True
        assert replacement.is_closed is False
        assert [subscription.subject for subscription in replacement.subscriptions] == [
            "artifact.run-1.agents.*.result.*"
        ]
        assert transport._background_failure is None
    finally:
        await transport.close()


@_asyncio_test
async def test_core_restart_serializes_publish_and_uses_replacement_url() -> None:
    first = _FakeNATS([])
    replacement = _FakeNATS([])
    factory = _FakeConnectionFactory(first, replacement)
    restart_entered = asyncio.Event()
    release_restart = asyncio.Event()

    async def restart() -> str:
        restart_entered.set()
        await release_restart.wait()
        return "nats://replacement.invalid:4333"

    transport = CoreNatsTransport(
        nats_url="nats://stable.invalid:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(factory),
    )
    restart_task: asyncio.Task[None] | None = None
    publish_task: asyncio.Task[PublicationReceipt] | None = None
    try:
        await transport.start_terminal_observer()
        restart_task = asyncio.create_task(transport.faults.restart_coordinator())
        await asyncio.wait_for(restart_entered.wait(), timeout=1)
        publish_task = asyncio.create_task(transport.submit_task(_command()))
        await asyncio.sleep(0)
        assert not publish_task.done()

        release_restart.set()
        await asyncio.wait_for(restart_task, timeout=1)
        await asyncio.wait_for(publish_task, timeout=1)
        assert factory.calls[1]["servers"] == ["nats://replacement.invalid:4333"]
        assert any(kind == "publish" for kind, _ in replacement.timeline)
        assert not any(kind == "publish" for kind, _ in first.timeline)
    finally:
        release_restart.set()
        pending = [task for task in (restart_task, publish_task) if task is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await transport.close()


@_parametrize(
    "replacement_url",
    ["nats://replacement.invalid:4333?", "nats://replacement.invalid:4333#"],
)
@_asyncio_test
async def test_core_restart_rejects_empty_query_or_fragment_delimiter(
    replacement_url: str,
) -> None:
    async def restart() -> str:
        return replacement_url

    transport = CoreNatsTransport(
        nats_url="nats://stable.invalid:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    try:
        with pytest.raises(ValueError, match=r"^invalid nats_url$"):
            await transport.faults.restart_coordinator()
        assert transport._nats_url == "nats://stable.invalid:4222"
    finally:
        await transport.close()


@_asyncio_test
async def test_core_restart_bounds_each_connect_attempt_by_shared_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connect_timeouts: list[float] = []
    moments = iter((0.0, 9.99, 10.0))

    def monotonic() -> float:
        return next(moments, 10.0)

    async def factory(**kwargs: object) -> object:
        connect_timeout = cast(float, kwargs["connect_timeout"])
        connect_timeouts.append(connect_timeout)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def bounded_sleep(delay: float) -> None:
        del delay

    async def restart() -> None:
        return None

    fake_time = SimpleNamespace(monotonic=monotonic)
    monkeypatch.setattr(core_module, "time", fake_time)
    transport = CoreNatsTransport(
        nats_url="nats://stable-nats:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(factory),
        sleep=bounded_sleep,
    )
    try:
        started = time.perf_counter()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                transport.faults.restart_coordinator(),
                timeout=0.5,
            )
        assert time.perf_counter() - started < 0.2
        assert connect_timeouts == [2]
    finally:
        await transport.close()


@_asyncio_test
async def test_core_restart_bounds_cancelled_candidate_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    moments = iter((0.0, 9.8, 9.9, 10.0))
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    def monotonic() -> float:
        return next(moments, 10.0)

    class HangingCandidate(_FakeNATS):
        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            await asyncio.Event().wait()

        async def close(self) -> None:
            close_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.is_closed = True
                close_finished.set()

    async def restart() -> None:
        return None

    fake_time = SimpleNamespace(monotonic=monotonic)
    monkeypatch.setattr(core_module, "time", fake_time)
    candidate = HangingCandidate([])
    transport = CoreNatsTransport(
        nats_url="nats://stable-nats:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    try:
        started = time.perf_counter()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                transport.faults.restart_coordinator(),
                timeout=0.6,
            )
        assert time.perf_counter() - started < 0.5
        assert close_started.is_set()
        assert close_finished.is_set()
    finally:
        await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_closes_candidate_when_initial_flush_fails() -> None:
    source = RuntimeError(
        "initial flush failed at nats://private.invalid:4222/private/path"
    )

    class FlushFailingNATS(_FakeNATS):
        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            raise source

    candidate = FlushFailingNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    try:
        with pytest.raises(
            RuntimeError,
            match=r"^core nats connection failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert type(raised.value) is type(source)
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert "private.invalid" not in str(raised.value)
        assert "/private/path" not in str(raised.value)
        assert candidate.is_closed is True
    finally:
        await transport.close()


@_asyncio_test
async def test_core_owns_candidate_before_initial_flush_completes() -> None:
    flush_started = asyncio.Event()
    release_flush = asyncio.Event()

    class PendingFlushNATS(_FakeNATS):
        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            flush_started.set()
            await release_flush.wait()

    candidate = PendingFlushNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    submission = asyncio.create_task(transport.submit_task(_command()))
    try:
        await flush_started.wait()
        assert len(transport._pending_candidates) == 1
        assert cast(object, transport._pending_candidates[0]) is candidate

        submission.cancel()
        with pytest.raises(asyncio.CancelledError):
            await submission
        assert candidate.is_closed is True
        assert transport._pending_candidates == []
    finally:
        release_flush.set()
        if not submission.done():
            await asyncio.gather(submission, return_exceptions=True)
        await transport.close()


@_asyncio_test
async def test_core_retries_failed_pre_establishment_candidate_close() -> None:
    flush_source = RuntimeError("flush failed at nats://private.invalid:4222")
    close_source = RuntimeError("close failed at /private/credential")

    class CleanupRetryNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            raise flush_source

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise close_source
            await super().close()

    candidate = CleanupRetryNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    with pytest.raises(
        RuntimeError,
        match=r"^core nats connection failed$",
    ) as raised:
        await transport.submit_task(_command())
    assert raised.value is not flush_source
    assert candidate.close_calls == 1

    await transport.close()
    assert candidate.close_calls == 2
    assert candidate.is_closed is True


@_asyncio_test
async def test_core_bounds_and_tracks_hanging_pre_establishment_cleanup() -> None:
    flush_source = RuntimeError("flush failed")
    close_cancelled = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class HangingCleanupNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            raise flush_source

        async def close(self) -> None:
            self.close_calls += 1
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await release_close.wait()
            finally:
                self.is_closed = True
                close_finished.set()

    candidate = HangingCleanupNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    try:
        with pytest.raises(
            RuntimeError,
            match=r"^core nats connection failed$",
        ):
            await asyncio.wait_for(transport.submit_task(_command()), timeout=0.5)
        assert close_cancelled.is_set()
        assert candidate.close_calls == 1
        assert len(transport._pending_candidates) == 1
        assert cast(object, transport._pending_candidates[0]) is candidate

        release_close.set()
        await asyncio.wait_for(close_finished.wait(), timeout=0.5)
        await transport.close()
        assert candidate.close_calls == 1
        assert transport._pending_candidates == []
    finally:
        release_close.set()
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_retains_candidate_when_cleanup_wait_is_cancelled() -> None:
    close_source = RuntimeError("cancel-resistant close failed")
    close_started = asyncio.Event()
    first_close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class CancelledCleanupNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                await super().close()
                return
            close_started.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                await release_close.wait()
            finally:
                first_close_finished.set()
            raise close_source

    candidate = CancelledCleanupNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    cleanup = asyncio.create_task(
        transport._release_candidate(cast(NATS, candidate), deadline=None)
    )
    try:
        await close_started.wait()
        cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup
        assert len(transport._pending_candidates) == 1
        assert cast(object, transport._pending_candidates[0]) is candidate
        assert candidate.close_calls == 1

        release_close.set()
        await asyncio.wait_for(first_close_finished.wait(), timeout=0.5)
        with pytest.raises(
            RuntimeError,
            match=r"^core nats operation failed$",
        ) as raised:
            await transport.close()
        assert raised.value is not close_source
        assert candidate.close_calls == 1
        assert cast(object, transport._pending_candidates[0]) is candidate

        await transport.close()
        assert candidate.close_calls == 2
        assert candidate.is_closed is True
        assert transport._pending_candidates == []
    finally:
        release_close.set()
        if not cleanup.done():
            await asyncio.gather(cleanup, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_cleanup_cancellation_supersedes_private_flush_failure() -> None:
    flush_source = RuntimeError("private flush failure")
    close_started = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class CancelDuringCleanupNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def flush(self, timeout: int = 10) -> None:
            assert timeout == 10
            raise flush_source

        async def close(self) -> None:
            self.close_calls += 1
            close_started.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                await release_close.wait()
            await super().close()
            close_finished.set()

    candidate = CancelDuringCleanupNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    submission = asyncio.create_task(transport.submit_task(_command()))
    try:
        await close_started.wait()
        submission.cancel()
        with pytest.raises(asyncio.CancelledError) as raised:
            await submission
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        assert len(transport._pending_candidates) == 1
        assert cast(object, transport._pending_candidates[0]) is candidate

        release_close.set()
        await asyncio.wait_for(close_finished.wait(), timeout=0.5)
        await transport.close()
        assert candidate.close_calls == 1
        assert transport._pending_candidates == []
    finally:
        release_close.set()
        if not submission.done():
            await asyncio.gather(submission, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_close_failure_retains_connection_for_retry() -> None:
    source = RuntimeError("close failed at nats://private.invalid:4222")

    class RetryCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([], in_bytes=7, out_bytes=9)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise source
            await super().close()

    connection = RetryCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    with pytest.raises(
        RuntimeError,
        match=r"^core nats operation failed$",
    ) as raised:
        await transport.close()
    assert raised.value is not source
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert transport._nc is cast(NATS, connection)
    assert transport._closed is False
    assert transport._accumulated_in_bytes == 0
    assert transport._accumulated_out_bytes == 0
    assert connection.close_calls == 1

    await transport.close()
    assert transport._nc is None
    assert transport._closed is True
    assert transport._accumulated_in_bytes == 7
    assert transport._accumulated_out_bytes == 9
    assert connection.close_calls == 2


@_asyncio_test
async def test_core_close_base_exception_is_type_preserved_and_secret_free() -> None:
    private_source = "private nats://secret.invalid/token/path"
    source = InjectedCrash(private_source)

    class RetryCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([], in_bytes=7, out_bytes=9)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise source
            await super().close()

    connection = RetryCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    with pytest.raises(
        InjectedCrash,
        match=r"^core nats operation failed$",
    ) as raised:
        await transport.close()
    assert raised.value is not source
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    production_frames = [
        frame
        for frame, _ in traceback.walk_tb(raised.value.__traceback__)
        if frame.f_code.co_filename.endswith("/core_nats.py")
    ]
    assert production_frames
    for frame in production_frames:
        assert source not in frame.f_locals.values()
        for value in frame.f_locals.values():
            if isinstance(value, (list, tuple, set)):
                assert source not in value
            elif isinstance(value, dict):
                assert source not in value.values()
    assert private_source not in str(raised.value) + repr(raised.value)
    assert transport._nc is cast(NATS, connection)
    assert transport._closed is False
    assert transport._accumulated_in_bytes == 0
    assert transport._accumulated_out_bytes == 0
    assert connection.close_calls == 1

    await transport.close()
    assert transport._nc is None
    assert transport._closed is True
    assert transport._accumulated_in_bytes == 7
    assert transport._accumulated_out_bytes == 9
    assert connection.close_calls == 2


@_asyncio_test
async def test_core_cancelled_close_retains_connection_and_tracked_close() -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class CancelResistantCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([], in_bytes=7, out_bytes=9)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            close_started.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                await release_close.wait()
            await super().close()
            close_finished.set()

    connection = CancelResistantCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    closing = asyncio.create_task(transport.close())
    try:
        await close_started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert transport._closed is False
        assert transport._nc is cast(NATS, connection)
        assert connection.close_calls == 1

        release_close.set()
        await asyncio.wait_for(close_finished.wait(), timeout=0.5)
        await transport.close()
        assert transport._closed is True
        assert transport._nc is None
        assert connection.close_calls == 1
        assert transport._accumulated_in_bytes == 7
        assert transport._accumulated_out_bytes == 9
    finally:
        release_close.set()
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_close_records_completion_before_propagating_cancellation() -> None:
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    class CompletingCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([], in_bytes=7, out_bytes=9)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            close_started.set()
            await release_close.wait()
            await super().close()

    connection = CompletingCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    closing = asyncio.create_task(transport.close())
    try:
        await close_started.wait()
        release_close.set()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert transport._closed is False
        assert transport._nc is None
        assert connection.close_calls == 1
        assert connection.is_closed is True
        assert transport._accumulated_in_bytes == 7
        assert transport._accumulated_out_bytes == 9

        await transport.close()
        assert transport._closed is True
        assert connection.close_calls == 1
    finally:
        release_close.set()
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_close_propagates_cancellation_suppressed_by_candidate() -> None:
    close_started = asyncio.Event()

    class SuppressingCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([], in_bytes=7, out_bytes=9)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            close_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await super().close()

    connection = SuppressingCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    closing = asyncio.create_task(transport.close())
    try:
        await close_started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert transport._closed is False
        assert transport._nc is None
        assert connection.close_calls == 1
        assert connection.is_closed is True
        assert transport._accumulated_in_bytes == 7
        assert transport._accumulated_out_bytes == 9

        await transport.close()
        assert transport._closed is True
        assert connection.close_calls == 1
    finally:
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_deadline_close_propagates_caller_cancellation() -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    class DeadlineCloseNATS(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls > 1:
                await super().close()
                return
            close_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                close_finished.set()

    connection = DeadlineCloseNATS()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    await transport.submit_task(_command())
    closing = asyncio.create_task(
        transport._close_connection(deadline=time.monotonic() + 10)
    )
    try:
        await close_started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert close_finished.is_set()
        assert transport._nc is cast(NATS, connection)
        assert connection.close_calls == 1

        await transport.close()
        assert connection.close_calls == 2
        assert connection.is_closed is True
    finally:
        if not closing.done():
            await asyncio.gather(closing, return_exceptions=True)
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_standalone_candidate_close_propagates_cancellation() -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    class CancelledCandidate(_FakeNATS):
        async def close(self) -> None:
            close_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                close_finished.set()

    candidate = CancelledCandidate([])
    closing = asyncio.create_task(
        CoreNatsTransport._close_candidate(
            cast(NATS, candidate),
            deadline=time.monotonic() + 10,
        )
    )
    await close_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert close_finished.is_set()


@_asyncio_test
async def test_core_close_candidate_is_invoked_at_deadline_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    class HangingCandidate(_FakeNATS):
        async def close(self) -> None:
            close_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                close_finished.set()

    candidate = HangingCandidate([])

    class FakeTime:
        @staticmethod
        def monotonic() -> float:
            return 10.0

    monkeypatch.setattr(core_module, "time", FakeTime())
    closed = await CoreNatsTransport._close_candidate(
        cast(NATS, candidate),
        deadline=10.0,
    )
    assert closed is False
    assert close_started.is_set()
    assert close_finished.is_set()


@_asyncio_test
async def test_core_close_candidate_deadline_bounds_cancel_resistant_close() -> None:
    close_cancelled = asyncio.Event()
    close_finished = asyncio.Event()
    release_close = asyncio.Event()

    class CancelResistantCandidate(_FakeNATS):
        async def close(self) -> None:
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await release_close.wait()
            finally:
                close_finished.set()

    candidate = CancelResistantCandidate([])
    close_attempt = asyncio.create_task(
        CoreNatsTransport._close_candidate(
            cast(NATS, candidate),
            deadline=time.monotonic() + 0.01,
        )
    )
    try:
        done, _ = await asyncio.wait({close_attempt}, timeout=0.05)
        assert close_attempt in done
        assert await close_attempt is False
        assert close_cancelled.is_set()
    finally:
        release_close.set()
        if not close_attempt.done():
            await close_attempt
        await asyncio.wait_for(close_finished.wait(), timeout=0.5)


@_asyncio_test
async def test_core_successful_candidate_release_clears_pending_ownership() -> None:
    close_source = RuntimeError("first close failed")

    class RetryCandidate(_FakeNATS):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise close_source
            await super().close()

    candidate = RetryCandidate()
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(candidate)),
    )
    try:
        await transport._release_candidate(cast(NATS, candidate), deadline=None)
        assert len(transport._pending_candidates) == 1
        assert cast(object, transport._pending_candidates[0]) is candidate

        await transport._release_candidate(cast(NATS, candidate), deadline=None)
        assert transport._pending_candidates == []

        await transport.close()
        assert candidate.close_calls == 2
    finally:
        if not transport._closed:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_surfaces_subscription_base_exception_on_operations_and_close() -> (
    None
):
    private_source = "nats://private.invalid:4222/private/crash"
    crash_gate = asyncio.Event()
    processing_tasks: list[asyncio.Task[None]] = []

    class CrashingSubscriptionNATS(_FakeNATS):
        async def subscribe(
            self,
            subject: str,
            *,
            cb: Callable[[_FakeMessage], Awaitable[None]],
            **kwargs: object,
        ) -> _FakeSubscription:
            subscription = await super().subscribe(subject, cb=cb, **kwargs)

            async def process() -> None:
                await crash_gate.wait()
                raise InjectedCrash(private_source)

            processing_task = asyncio.create_task(process())
            processing_tasks.append(processing_task)
            subscription._wait_for_msgs_task = processing_task
            return subscription

    connection = CrashingSubscriptionNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    close_failure: BaseException | None = None
    try:
        await transport.start_terminal_observer()
        crash_gate.set()
        for _ in range(10):
            await asyncio.sleep(0)
            if processing_tasks[0].done():
                break
        await asyncio.sleep(0)

        with pytest.raises(
            InjectedCrash,
            match=r"^core nats subscription failed$",
        ) as operation_raised:
            await transport.inspect_state()
        assert operation_raised.value.__cause__ is None
        assert operation_raised.value.__context__ is None
        assert private_source not in str(operation_raised.value)

        with pytest.raises(
            InjectedCrash,
            match=r"^core nats subscription failed$",
        ) as close_raised:
            await transport.close()
        close_failure = close_raised.value
        assert close_raised.value is operation_raised.value
    finally:
        if processing_tasks:
            await asyncio.gather(*processing_tasks, return_exceptions=True)
        if close_failure is None:
            await asyncio.gather(transport.close(), return_exceptions=True)


@_asyncio_test
async def test_core_restart_restores_owned_roles_and_accumulates_connection_bytes() -> (
    None
):
    first_timeline: list[tuple[str, object]] = []
    second_timeline: list[tuple[str, object]] = []
    first = _FakeNATS(first_timeline, in_bytes=11, out_bytes=22)
    second = _FakeNATS(second_timeline, in_bytes=5, out_bytes=7)
    factory = _FakeConnectionFactory(first, second)
    restarted = 0

    async def restart() -> str:
        nonlocal restarted
        restarted += 1
        return "nats://127.0.0.1:4333"

    transport = CoreNatsTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        agent_card=_agent_card(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(factory),
        uuid4=_UUIDs(
            "70000000-0000-4000-8000-000000000001",
            "70000000-0000-4000-8000-000000000002",
        ),
        epoch_now=lambda: NOW,
    )
    executor = _RecordingExecutor()
    try:
        await transport.start_terminal_observer()
        await transport.start_progress_observer()
        await transport.start_receiver(
            "worker-1",
            _as_task_executor(executor),
        )
        await transport.faults.restart_coordinator()
        assert restarted == 1
        assert first.is_closed is True
        assert factory.calls[1]["servers"] == ["nats://127.0.0.1:4333"]
        assert [subscription.subject for subscription in second.subscriptions] == [
            "artifact.run-1.agents.*.result.*",
            "artifact.run-1.agents.*.task_progress.*",
            "artifact.run-1.agents.*.heartbeat",
            "artifact.run-1.agents.*.status",
            "artifact.run-1.agents.*.register",
            "artifact.run-1.agents.worker-1.inbox",
        ]
        snapshot = await transport.inspect_state()
        assert snapshot.mode is Mode.CORE_ONLY
        assert snapshot.streams == {}
        assert snapshot.consumers == {}
        assert snapshot.pending is None
        assert snapshot.ack_pending is None
        assert snapshot.connection_bytes == {
            "in_bytes": 16,
            "out_bytes": 29,
        }
        assert snapshot.storage_bytes == 0
        assert snapshot.message_count == 0
    finally:
        await transport.close()
    assert second.is_closed is True


@_asyncio_test
async def test_core_restart_preserves_deliberately_stopped_local_receiver() -> None:
    first = _FakeNATS([])
    second = _FakeNATS([])
    factory = _FakeConnectionFactory(first, second)

    async def restart() -> None:
        return None

    transport = CoreNatsTransport(
        nats_url="nats://stable-nats:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        agent_card=_agent_card(),
        coordinator_restart=restart,
        connection_factory=_as_connection_factory(factory),
        uuid4=_UUIDs(
            "70000000-0000-4000-8000-000000000001",
            "70000000-0000-4000-8000-000000000002",
        ),
        epoch_now=lambda: NOW,
    )
    executor = _RecordingExecutor()
    try:
        await transport.start_receiver(
            "worker-1",
            _as_task_executor(executor),
        )
        await transport.faults.stop_worker("worker-1")
        await transport.faults.restart_coordinator()
        assert factory.calls[1]["servers"] == ["nats://stable-nats:4222"]
        assert second.subscriptions == []
        assert all(kind != "publish" for kind, _ in second.timeline)
        await transport.faults.start_worker("worker-1")
        assert [subscription.subject for subscription in second.subscriptions] == [
            "artifact.run-1.agents.worker-1.inbox"
        ]
        assert any(
            kind == "publish"
            and cast(tuple[str, bytes], value)[0]
            == "artifact.run-1.agents.worker-1.register"
            for kind, value in second.timeline
        )
    finally:
        await transport.close()


@_asyncio_test
async def test_core_restart_never_starts_external_worker() -> None:
    stopped: list[str] = []
    started: list[str] = []
    restart_calls = 0

    async def stop(agent_id: str) -> None:
        stopped.append(agent_id)

    async def start(agent_id: str) -> None:
        started.append(agent_id)

    async def restart() -> None:
        nonlocal restart_calls
        restart_calls += 1

    connection = _FakeNATS([])
    transport = CoreNatsTransport(
        nats_url="nats://stable-nats:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_RecordingEventSink(),
        coordinator_restart=restart,
        worker_stop=stop,
        worker_start=start,
        connection_factory=_as_connection_factory(_FakeConnectionFactory(connection)),
    )
    try:
        await transport.faults.stop_worker("worker-1")
        await transport.faults.restart_coordinator()
        assert stopped == ["worker-1"]
        assert started == []
        assert restart_calls == 1
        await transport.faults.start_worker("worker-1")
        assert started == ["worker-1"]
    finally:
        await transport.close()


@docker_test
@_asyncio_test
async def test_core_docker_round_trip_offline_loss_and_dynamic_restart(
    request: pytest.FixtureRequest,
) -> None:
    _require_explicit_docker(request)
    _assert_owned_docker_inventory_empty()
    server = NatsServer(token=TOKEN, jetstream=False)
    transport: CoreNatsTransport | None = None
    try:
        server.start()

        async def restart() -> str:
            server.restart(preserve_storage=False)
            replacement_url = server.url
            assert isinstance(replacement_url, str)
            return replacement_url

        sink = _RecordingEventSink()
        executor = _RecordingExecutor()
        transport = CoreNatsTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
            agent_card=_agent_card(),
            coordinator_restart=restart,
            uuid4=_UUIDs(
                "70000000-0000-4000-8000-000000000001",
                "70000000-0000-4000-8000-000000000002",
            ),
            epoch_now=lambda: NOW,
        )
        await transport.start_terminal_observer()
        await transport.start_progress_observer()
        await transport.start_receiver(
            "worker-1",
            _as_task_executor(executor),
        )
        await transport.wait_receiver_ready("worker-1", 2)
        await _wait_for_event_count(sink, "transport.registration_observed", 1)

        task_receipt = await transport.submit_task(_command())
        await asyncio.wait_for(executor.called.wait(), timeout=2)
        assert task_receipt.accepted is True
        assert task_receipt.transport == "core-only"
        first_delivery = typing.cast(typing.Any, executor.deliveries[0])
        assert first_delivery.raw == canonical_json(_command())
        assert first_delivery.delivery_count == 1
        assert first_delivery.stream_sequence is None

        await transport.publish_terminal(_terminal())
        observed = await transport.observe_terminal(
            "20000000-0000-4000-8000-000000000001",
            2,
        )
        assert observed is not None
        assert observed.envelope == _terminal()
        assert observed.replayed is False
        assert observed.delivery_count == 1
        await transport.publish_progress(_progress())
        await transport.publish_heartbeat(_heartbeat())
        await _wait_for_event_count(sink, "transport.transient_observed", 2)

        registration_events = [
            event
            for event in sink.events
            if event["event"] == "transport.registration_observed"
        ]
        assert registration_events[0]["data"] == {
            "agent_id": "worker-1",
            "delivery_count": 1,
            "envelope_id": "70000000-0000-4000-8000-000000000001",
            "observation_index": 1,
            "replayed": False,
            "stream_sequence": None,
        }

        await transport.faults.stop_worker("worker-1")
        executor.called.clear()
        lost = _command(
            envelope_id="10000000-0000-4000-8000-000000000002",
            task_id="20000000-0000-4000-8000-000000000002",
        )
        lost_receipt = await transport.submit_task(lost)
        assert lost_receipt.accepted is True

        await transport.faults.restart_coordinator()
        await transport.publish_progress(
            _progress(envelope_id="40000000-0000-4000-8000-000000000002")
        )
        await transport.publish_heartbeat(
            _heartbeat(envelope_id="50000000-0000-4000-8000-000000000002")
        )
        await _wait_for_event_count(sink, "transport.transient_observed", 4)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(executor.called.wait(), timeout=0.2)

        await transport.faults.start_worker("worker-1")
        await transport.wait_receiver_ready("worker-1", 2)
        await _wait_for_event_count(sink, "transport.registration_observed", 2)
        await asyncio.sleep(0.1)
        assert executor.called.is_set() is False
        assert len(executor.deliveries) == 1

        replacement = _command(
            envelope_id="10000000-0000-4000-8000-000000000003",
            task_id="20000000-0000-4000-8000-000000000003",
        )
        await transport.submit_task(replacement)
        await asyncio.wait_for(executor.called.wait(), timeout=2)
        assert len(executor.deliveries) == 2
        replacement_delivery = typing.cast(typing.Any, executor.deliveries[1])
        assert replacement_delivery.raw == canonical_json(replacement)
        await transport.publish_terminal(
            _terminal(
                envelope_id="30000000-0000-4000-8000-000000000003",
                task_id="20000000-0000-4000-8000-000000000003",
            )
        )
        replacement_observed = await transport.observe_terminal(
            "20000000-0000-4000-8000-000000000003",
            2,
        )
        assert replacement_observed is not None
        assert replacement_observed.envelope["id"] == (
            "30000000-0000-4000-8000-000000000003"
        )
        snapshot = await transport.inspect_state()
        assert snapshot.connection_bytes["in_bytes"] > 0
        assert snapshot.connection_bytes["out_bytes"] > 0
    finally:
        try:
            if transport is not None:
                await transport.close()
        finally:
            server.close()
            _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_core_docker_wrong_token_is_exact_and_secret_free(
    request: pytest.FixtureRequest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _require_explicit_docker(request)
    _assert_owned_docker_inventory_empty()
    server = NatsServer(token=TOKEN, jetstream=False)
    transport: CoreNatsTransport | None = None
    wrong_token = "c" * 64
    try:
        server.start()
        endpoint = server.url
        transport = CoreNatsTransport(
            nats_url=endpoint,
            run_id="run-1",
            token=wrong_token,
            event_sink=_RecordingEventSink(),
        )
        caplog.set_level(logging.DEBUG)
        with pytest.raises(
            PermissionError,
            match=r"^transport authentication failed$",
        ) as raised:
            await transport.submit_task(_command())
        assert raised.value.__cause__ is None
        assert raised.value.__context__ is None
        rendered = str(raised.value) + repr(raised.value) + caplog.text
        for forbidden in (
            wrong_token,
            endpoint,
            "Authorization Violation",
        ):
            assert forbidden not in rendered

        close_result = await asyncio.gather(
            transport.close(),
            return_exceptions=True,
        )
        transport = None
        if close_result[0] is not None:
            assert type(close_result[0]) is PermissionError
            assert str(close_result[0]) == "transport authentication failed"
    finally:
        try:
            if transport is not None:
                await asyncio.gather(
                    transport.close(),
                    return_exceptions=True,
                )
        finally:
            server.close()
            _assert_owned_docker_inventory_empty()


def test_jetstream_config_is_exact_and_durable_names_are_role_hashed() -> None:
    from scripts.research.modes.jetstream_config import (
        durable_name,
        task_stream_config,
        transient_stream_config,
    )

    assert task_stream_config("run-1") == {
        "name": "AGENT_INBOX",
        "subjects": ["agents.*.inbox"],
        "retention": "workqueue",
        "storage": "file",
        "max_age_ns": 86_400_000_000_000,
        "max_bytes": 1_073_741_824,
        "max_msg_size": 1_048_576,
        "discard": "new",
        "duplicate_window_ns": 300_000_000_000,
    }
    assert transient_stream_config("run-1") == {
        "name": "TRANSIENT_EVENTS",
        "subjects": [
            "agents.*.task_progress.>",
            "agents.*.heartbeat",
            "agents.*.status",
        ],
        "retention": "limits",
        "storage": "file",
        "max_age_ns": 3_600_000_000_000,
        "max_bytes": 1_073_741_824,
        "max_msg_size": 1_048_576,
        "discard": "old",
        "duplicate_window_ns": 300_000_000_000,
    }
    assert durable_name("task", "run-1", "worker-1") == "ec_task_975199eb4b31d34b70d5b90b"
    assert durable_name("result", "run-1", "worker-1") == "ec_result_ef6835bd04ff28ddeb7242cb"
    assert durable_name("transient", "run-1", "observer-1") == (
        "ec_transient_cbcd7085c8510080924d6137"
    )


def test_durable_mode_public_exports_and_constructor_seams_are_exact() -> None:
    import scripts.research.modes.all_durable as durable_module
    import scripts.research.modes.edgecitadel as edge_module

    assert list(edge_module.__all__) == ["ABLATIONS", "EdgeCitadelTransport"]
    assert list(durable_module.__all__) == ["AllDurableTransport"]
    assert edge_module.ABLATIONS == {
        "none": {"nats_msg_id": False, "outcome_ledger": False},
        "broker-only": {"nats_msg_id": True, "outcome_ledger": False},
        "full-contract": {"nats_msg_id": True, "outcome_ledger": True},
    }

    edge_signature = inspect.signature(edge_module.EdgeCitadelTransport)
    durable_signature = inspect.signature(durable_module.AllDurableTransport)
    assert tuple(edge_signature.parameters) == (
        "nats_url",
        "run_id",
        "token",
        "event_sink",
        "agent_card",
        "observer_agent_id",
        "ablation",
        "coordinator_restart",
        "worker_stop",
        "worker_start",
        "connection_factory",
        "evidence_clock_ns",
        "epoch_now",
        "uuid4",
        "sleep",
    )
    assert tuple(durable_signature.parameters) == tuple(
        parameter
        for parameter in edge_signature.parameters
        if parameter != "ablation"
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in edge_signature.parameters.values()
    )


def test_durable_modes_supply_the_complete_task_transport_surface() -> None:
    from scripts.research.modes.all_durable import AllDurableTransport
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    required = {
        "start_terminal_observer",
        "start_progress_observer",
        "start_receiver",
        "wait_receiver_ready",
        "submit_task",
        "publish_progress",
        "publish_terminal",
        "publish_heartbeat",
        "observe_terminal",
        "inspect_state",
        "close",
    }
    for transport_type in (EdgeCitadelTransport, AllDurableTransport):
        assert all(callable(getattr(transport_type, name, None)) for name in required)


@_asyncio_test
async def test_durable_publications_use_exact_puback_headers_and_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    class AcknowledgingJetStream:
        def __init__(self) -> None:
            self.calls: list[tuple[str, bytes, Mapping[str, str]]] = []

        async def publish(
            self,
            subject: str,
            data: bytes,
            **kwargs: Mapping[str, str],
        ) -> SimpleNamespace:
            self.calls.append((subject, data, dict(kwargs)))
            stream = (
                "TRANSIENT_EVENTS"
                if subject.endswith("heartbeat")
                else "AGENT_INBOX"
            )
            return SimpleNamespace(stream=stream, seq=len(self.calls), duplicate=True)

    class Connection:
        def __init__(self) -> None:
            self.js = AcknowledgingJetStream()
            self.core_calls: list[tuple[str, bytes]] = []
            self.flushes = 0
            self.stats: dict[str, int] = {}

        def jetstream(self) -> AcknowledgingJetStream:
            return self.js

        async def publish(self, subject: str, data: bytes) -> None:
            self.core_calls.append((subject, data))

        async def flush(self) -> None:
            self.flushes += 1

    connection = Connection()

    async def connect(**kwargs: object) -> object:
        del kwargs
        return connection

    async def streams() -> None:
        return None

    edge_none = EdgeCitadelTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        ablation="none",
        connection_factory=_as_connection_factory(connect),
    )
    monkeypatch.setattr(edge_none, "_ensure_streams", streams)
    receipt = await edge_none.submit_task(_command())
    assert connection.js.calls == [
        ("agents.worker-1.inbox", canonical_json(_command()), {})
    ]
    assert receipt.stream == "AGENT_INBOX"
    assert receipt.stream_sequence == 1
    assert receipt.duplicate is None

    edge_full = EdgeCitadelTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(connect),
    )
    monkeypatch.setattr(edge_full, "_ensure_streams", streams)
    await edge_full.submit_task(_command(envelope_id="10000000-0000-4000-8000-000000000002"))
    assert connection.js.calls[-1] == (
        "agents.worker-1.inbox",
        canonical_json(_command(envelope_id="10000000-0000-4000-8000-000000000002")),
        {"headers": {"Nats-Msg-Id": "10000000-0000-4000-8000-000000000002"}},
    )

    durable = AllDurableTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(connect),
    )
    monkeypatch.setattr(durable, "_ensure_streams", streams)
    durable_receipt = await durable.publish_heartbeat(_heartbeat())
    assert connection.js.calls[-1] == (
        "agents.worker-1.heartbeat",
        canonical_json(_heartbeat()),
        {"headers": {"Nats-Msg-Id": "50000000-0000-4000-8000-000000000001"}},
    )
    assert durable_receipt.stream == "TRANSIENT_EVENTS"
    assert durable_receipt.duplicate is True

    edge_receipt = await edge_full.publish_progress(_progress())
    assert connection.core_calls == [
        ("agents.worker-1.task_progress.20000000-0000-4000-8000-000000000001", canonical_json(_progress()))
    ]
    assert edge_receipt.stream is None
    assert edge_receipt.duplicate is None


@docker_test
@_asyncio_test
async def test_all_durable_result_observer_uses_the_precreated_durable(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
            observer_agent_id="requester-1",
        )
        await transport.start_terminal_observer()
        receipt = await transport.publish_terminal(_terminal())
        observed = await transport.observe_terminal(
            "20000000-0000-4000-8000-000000000001",
            2,
        )
        assert receipt.stream == "AGENT_INBOX"
        assert observed is not None
        assert observed.stream_sequence == receipt.stream_sequence
        assert observed.delivery is not None
        await observed.delivery.ack()
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_replays_transient_frames_published_while_disconnected(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    sink = _RecordingEventSink()

    async def wait_for_observations(count: int) -> list[Mapping[str, object]]:
        for _ in range(100):
            observed = [
                event["data"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed) >= count:
                return observed
            await asyncio.sleep(0.02)
        raise AssertionError("transient observation timed out")

    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
            observer_agent_id="observer-1",
        )
        await transport.start_progress_observer()
        await transport.publish_heartbeat(_heartbeat())
        await wait_for_observations(1)
        await transport.faults.disconnect_progress_observer()
        await transport.publish_heartbeat(
            _heartbeat(envelope_id="50000000-0000-4000-8000-000000000002")
        )
        await transport.faults.reconnect_progress_observer()
        observations = await wait_for_observations(2)
        assert observations[-1]["envelope_id"] == "50000000-0000-4000-8000-000000000002"
        assert observations[-1]["replayed"] is True
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_w3_replays_exactly_the_disconnected_transient_phase(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    sink = _RecordingEventSink()

    def heartbeat(index: int) -> dict[str, object]:
        return _heartbeat(
            envelope_id=f"50000000-0000-4000-8000-{index:012d}",
        )

    async def wait_for_observations(count: int) -> list[Mapping[str, object]]:
        for _ in range(200):
            observed = [
                event["data"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed) >= count:
                return observed
            await asyncio.sleep(0.02)
        raise AssertionError("transient observation timed out")

    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
            observer_agent_id="observer-1",
        )
        await transport.start_progress_observer()
        for index in range(1, 6):
            await transport.publish_heartbeat(heartbeat(index))
        await wait_for_observations(5)
        await transport.faults.disconnect_progress_observer()
        for index in range(6, 16):
            await transport.publish_heartbeat(heartbeat(index))
        await transport.faults.reconnect_progress_observer()
        await wait_for_observations(15)
        for index in range(16, 21):
            await transport.publish_heartbeat(heartbeat(index))
        observations = await wait_for_observations(20)
        assert [item["envelope_id"] for item in observations] == [
            heartbeat(index)["id"] for index in range(1, 21)
        ]
        assert [item["replayed"] for item in observations] == (
            [False] * 5 + [True] * 10 + [False] * 5
        )
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_edgecitadel_loses_transient_frames_published_while_disconnected(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: EdgeCitadelTransport | None = None
    sink = _RecordingEventSink()

    async def wait_for_observations(count: int) -> list[Mapping[str, object]]:
        for _ in range(100):
            observed = [
                event["data"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed) >= count:
                return observed
            await asyncio.sleep(0.02)
        raise AssertionError("transient observation timed out")

    try:
        transport = EdgeCitadelTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
        )
        await transport.start_progress_observer()
        await transport.publish_heartbeat(_heartbeat())
        await wait_for_observations(1)
        await transport.faults.disconnect_progress_observer()
        await transport.publish_heartbeat(
            _heartbeat(envelope_id="50000000-0000-4000-8000-000000000002")
        )
        await asyncio.sleep(0.1)
        await transport.faults.reconnect_progress_observer()
        await transport.publish_heartbeat(
            _heartbeat(envelope_id="50000000-0000-4000-8000-000000000003")
        )
        observations = await wait_for_observations(2)
        assert [item["envelope_id"] for item in observations] == [
            "50000000-0000-4000-8000-000000000001",
            "50000000-0000-4000-8000-000000000003",
        ]
        assert all(item["replayed"] is False for item in observations)
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_edgecitadel_w3_loses_exactly_the_disconnected_transient_phase(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: EdgeCitadelTransport | None = None
    sink = _RecordingEventSink()

    def heartbeat(index: int) -> dict[str, object]:
        return _heartbeat(
            envelope_id=f"50000000-0000-4000-8000-{index:012d}",
        )

    async def wait_for_observations(count: int) -> list[Mapping[str, object]]:
        for _ in range(200):
            observed = [
                event["data"]
                for event in sink.events
                if event["event"] == "transport.transient_observed"
            ]
            if len(observed) >= count:
                return observed
            await asyncio.sleep(0.02)
        raise AssertionError("transient observation timed out")

    try:
        transport = EdgeCitadelTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
        )
        await transport.start_progress_observer()
        for index in range(1, 6):
            await transport.publish_heartbeat(heartbeat(index))
        await wait_for_observations(5)
        await transport.faults.disconnect_progress_observer()
        for index in range(6, 16):
            await transport.publish_heartbeat(heartbeat(index))
        await asyncio.sleep(0.1)
        await transport.faults.reconnect_progress_observer()
        for index in range(16, 21):
            await transport.publish_heartbeat(heartbeat(index))
        observations = await wait_for_observations(10)
        assert [item["envelope_id"] for item in observations] == [
            heartbeat(index)["id"] for index in [*range(1, 6), *range(16, 21)]
        ]
        assert all(item["replayed"] is False for item in observations)
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_edgecitadel_ablations_report_real_jetstream_deduplication(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    full: EdgeCitadelTransport | None = None
    none: EdgeCitadelTransport | None = None
    try:
        full = EdgeCitadelTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
        )
        first = await full.submit_task(_command())
        duplicate = await full.submit_task(_command())
        assert first.duplicate is False
        assert duplicate.duplicate is True
        assert duplicate.stream_sequence == first.stream_sequence

        none = EdgeCitadelTransport(
            nats_url=server.url,
            run_id="run-2",
            token=TOKEN,
            event_sink=_EventSink(),
            ablation="none",
        )
        first_without_dedup = await none.submit_task(
            _command(envelope_id="10000000-0000-4000-8000-000000000002")
        )
        second_without_dedup = await none.submit_task(
            _command(envelope_id="10000000-0000-4000-8000-000000000002")
        )
        assert first_without_dedup.duplicate is None
        assert second_without_dedup.duplicate is None
        assert second_without_dedup.stream_sequence == first_without_dedup.stream_sequence + 1
    finally:
        if full is not None:
            await full.close()
        if none is not None:
            await none.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_edgecitadel_rejects_jetstream_that_captures_a_transient_subject(
    request: pytest.FixtureRequest,
) -> None:
    from nats.js.api import StreamConfig

    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    connection: NATS | None = None
    transport: EdgeCitadelTransport | None = None
    try:
        connection = await nats.connect(
            servers=[server.url],
            token=TOKEN,
            allow_reconnect=False,
            max_reconnect_attempts=0,
        )
        await connection.jetstream().add_stream(
            StreamConfig(name="CAPTURES_HEARTBEAT", subjects=["agents.*.heartbeat"])
        )
        transport = EdgeCitadelTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
        )
        with pytest.raises(RuntimeError, match="transient subject"):
            await transport.start_progress_observer()
    finally:
        if transport is not None:
            await transport.close()
        if connection is not None:
            await connection.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_snapshot_uses_live_stream_message_and_storage_counts(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
        )
        await transport.publish_heartbeat(_heartbeat())
        snapshot = await transport.inspect_state()
        assert set(snapshot.streams) == {"AGENT_INBOX", "TRANSIENT_EVENTS"}
        assert snapshot.message_count == 1
        assert snapshot.storage_bytes >= len(canonical_json(_heartbeat()))
        assert snapshot.pending is None
        assert snapshot.ack_pending is None
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_snapshot_includes_live_consumer_pending_counts(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport
    from scripts.research.modes.jetstream_config import durable_name

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
            observer_agent_id="requester-1",
        )
        await transport.start_terminal_observer()
        snapshot = await transport.inspect_state()
        consumer = snapshot.consumers[durable_name("result", "run-1", "requester-1")]
        assert consumer["pending"] == 0
        assert consumer["ack_pending"] == 0
        assert snapshot.pending == 0
        assert snapshot.ack_pending == 0
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_restart_restores_terminal_observer_with_preserved_storage(
    request: pytest.FixtureRequest,
) -> None:
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None

    async def restart() -> str:
        server.restart(preserve_storage=True)
        return server.url

    try:
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
            observer_agent_id="requester-1",
            coordinator_restart=restart,
        )
        await transport.start_terminal_observer()
        await transport.faults.restart_coordinator()
        receipt = await transport.publish_terminal(_terminal())
        observed = await transport.observe_terminal(
            "20000000-0000-4000-8000-000000000001",
            2,
        )
        assert receipt.stream == "AGENT_INBOX"
        assert observed is not None
        assert observed.stream_sequence == receipt.stream_sequence
        assert observed.delivery is not None
        await observed.delivery.ack()
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_worker_stop_start_retains_its_task_consumer(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    from scripts.research.fixtures.native_control import (
        NativeControlConfig,
        build_agent_card,
    )
    from scripts.research.modes.all_durable import AllDurableTransport
    from scripts.research.modes.jetstream_config import durable_name

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    try:
        config = NativeControlConfig(
            run_id="run-1",
            agent_id="worker-1",
            mode="all-durable",
            behavior="echo",
            delay_ms=0,
            crash_point=None,
            heartbeat_interval_ms=1000,
            outcome_db=str(tmp_path / "outcomes.sqlite3"),
            side_effect_db=str(tmp_path / "effects.sqlite3"),
        )
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
            agent_card=build_agent_card(config),
        )
        await transport.start_receiver("worker-1", _as_task_executor(object()))
        await transport.wait_receiver_ready("worker-1", 2)
        consumer_name = durable_name("task", "run-1", "worker-1")
        first = await transport.inspect_state()
        assert consumer_name in first.consumers
        await transport.faults.stop_worker("worker-1")
        await transport.faults.start_worker("worker-1")
        await transport.wait_receiver_ready("worker-1", 2)
        second = await transport.inspect_state()
        assert consumer_name in second.consumers
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_receiver_binds_production_pull_consumer_to_task_durable(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    from scripts.research.fixtures.native_control import (
        NativeControlConfig,
        build_agent_card,
    )
    from scripts.research.modes.all_durable import AllDurableTransport

    class RecordingExecutor:
        def __init__(self) -> None:
            self.deliveries: list[object] = []

        async def execute(self, delivery: object) -> None:
            self.deliveries.append(delivery)
            await cast(typing.Any, delivery).commit()

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    sink = _RecordingEventSink()
    try:
        config = NativeControlConfig(
            run_id="run-1",
            agent_id="worker-1",
            mode="all-durable",
            behavior="echo",
            delay_ms=0,
            crash_point=None,
            heartbeat_interval_ms=1000,
            outcome_db=str(tmp_path / "outcomes.sqlite3"),
            side_effect_db=str(tmp_path / "effects.sqlite3"),
        )
        executor = RecordingExecutor()
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=sink,
            agent_card=build_agent_card(config),
        )
        await transport.start_receiver(
            "worker-1",
            _as_task_executor(executor),
        )
        await transport.wait_receiver_ready("worker-1", 2)
        await transport.submit_task(_command())
        await _wait_for_event_count(sink, "transport.worker_delivery", 1)
        assert len(executor.deliveries) == 1
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


@docker_test
@_asyncio_test
async def test_all_durable_receiver_registration_stays_outside_jetstream(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> None:
    from scripts.research.fixtures.native_control import (
        NativeControlConfig,
        build_agent_card,
    )
    from scripts.research.modes.all_durable import AllDurableTransport

    _require_explicit_docker(request)
    server = NatsServer(token=TOKEN, jetstream=True).start()
    transport: AllDurableTransport | None = None
    try:
        config = NativeControlConfig(
            run_id="run-1",
            agent_id="worker-1",
            mode="all-durable",
            behavior="echo",
            delay_ms=0,
            crash_point=None,
            heartbeat_interval_ms=1000,
            outcome_db=str(tmp_path / "outcomes.sqlite3"),
            side_effect_db=str(tmp_path / "effects.sqlite3"),
        )
        transport = AllDurableTransport(
            nats_url=server.url,
            run_id="run-1",
            token=TOKEN,
            event_sink=_EventSink(),
            agent_card=build_agent_card(config),
        )
        await transport.start_receiver("worker-1", _as_task_executor(object()))
        await transport.wait_receiver_ready("worker-1", 2)
        snapshot = await transport.inspect_state()
        assert snapshot.message_count == 0
        assert snapshot.storage_bytes == 0
    finally:
        if transport is not None:
            await transport.close()
        server.close()
        _assert_owned_docker_inventory_empty()


def test_durable_mode_resolved_configs_are_exact_fresh_and_transport_compatible() -> None:
    from scripts.research.modes.all_durable import AllDurableTransport
    from scripts.research.modes.edgecitadel import EdgeCitadelTransport

    edge_none = EdgeCitadelTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        ablation="none",
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    edge_broker = EdgeCitadelTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        ablation="broker-only",
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    edge_full = EdgeCitadelTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )
    durable = AllDurableTransport(
        nats_url="nats://127.0.0.1:4222",
        run_id="run-1",
        token=TOKEN,
        event_sink=_EventSink(),
        connection_factory=_as_connection_factory(_unused_connection_factory),
    )

    expected = (
        (edge_none, Mode.EDGECITADEL, False, {
            "mode": "edgecitadel", "ablation": "none", "nats_msg_id": False,
            "outcome_ledger": False,
        }),
        (edge_broker, Mode.EDGECITADEL, False, {
            "mode": "edgecitadel", "ablation": "broker-only", "nats_msg_id": True,
            "outcome_ledger": False,
        }),
        (edge_full, Mode.EDGECITADEL, True, {
            "mode": "edgecitadel", "ablation": "full-contract", "nats_msg_id": True,
            "outcome_ledger": True,
        }),
        (durable, Mode.ALL_DURABLE, True, {
            "mode": "all-durable", "ablation": "full-contract", "nats_msg_id": True,
            "outcome_ledger": True,
        }),
    )
    for transport, mode, ledger_enabled, config in expected:
        assert _accept_task_transport(transport) is transport
        assert transport.mode is mode
        assert transport.outcome_ledger_enabled is ledger_enabled
        assert transport.faults is transport.faults
        first = transport.resolved_config
        second = transport.resolved_config
        assert type(first) is MappingProxyType
        assert first == config
        assert first is not second
        with pytest.raises(TypeError):
            first["mode"] = "changed"  # type: ignore[index]
        assert dict(second) == config
