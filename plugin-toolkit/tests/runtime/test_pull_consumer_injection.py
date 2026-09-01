"""Legacy and injected PullConsumer boundary tests."""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_module
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import FrozenInstanceError, fields
from types import SimpleNamespace
from typing import TypeVar, cast
from unittest.mock import patch

import pytest
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js.api import AckPolicy, ConsumerInfo

import edgecitadel_plugin_runtime.pull_consumer as pull_consumer_module
import edgecitadel_plugin_runtime.template as template_module
from edgecitadel_plugin_runtime.pull_consumer import (
    ConsumerBinding,
    Context,
    PullConsumer,
)
from edgecitadel_plugin_runtime.task_executor import (
    ExecutionResult,
    InboundDelivery,
    InjectedCrash,
    TaskExecutor,
)

WIRE_ID = "10000000-0000-4000-8000-000000000001"
TASK_ID = "20000000-0000-4000-8000-000000000001"
CONTEXT_ID = "30000000-0000-4000-8000-000000000001"
PARENT_ID = "40000000-0000-4000-8000-000000000001"
RESULT_ID = "50000000-0000-4000-8000-000000000001"
PROGRESS_ID = "60000000-0000-4000-8000-000000000001"
NOW = "2026-07-25T12:00:00.000Z"

_TestFunction = TypeVar("_TestFunction", bound=Callable[..., object])


def typed_decorator(
    decorator: object,
) -> Callable[[_TestFunction], _TestFunction]:
    return cast(Callable[[_TestFunction], _TestFunction], decorator)


async_test = typed_decorator(pytest.mark.asyncio)


def cases(
    argnames: str | Sequence[str],
    argvalues: Iterable[object],
) -> Callable[[_TestFunction], _TestFunction]:
    return typed_decorator(pytest.mark.parametrize(argnames, argvalues))


class StopRun(BaseException):
    pass


class FakeSubscription:
    async def fetch(self, *, batch: int, timeout: int) -> list[object]:
        assert batch == 1
        assert timeout == 30
        raise StopRun


class FakeJS:
    def __init__(self, info: ConsumerInfo | None = None) -> None:
        self.info = info
        self.consumer_info_calls: list[tuple[str, str]] = []
        self.subscribe_calls: list[dict[str, object]] = []
        self.publish_calls: list[tuple[str, bytes, Mapping[str, str]]] = []

    async def consumer_info(self, stream: str, durable: str) -> ConsumerInfo:
        self.consumer_info_calls.append((stream, durable))
        if self.info is None:
            raise AssertionError("consumer_info was not configured")
        return self.info

    async def pull_subscribe(self, **kwargs: object) -> FakeSubscription:
        self.subscribe_calls.append(kwargs)
        return FakeSubscription()

    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        headers: Mapping[str, str],
    ) -> None:
        self.publish_calls.append((subject, payload, headers))


class FakeNC:
    def __init__(self, js: FakeJS | None = None) -> None:
        self.js = js or FakeJS()
        self.publish_calls: list[tuple[str, bytes]] = []

    def jetstream(self) -> FakeJS:
        return self.js

    async def publish(self, subject: str, payload: bytes) -> None:
        self.publish_calls.append((subject, payload))


class FakeMessage:
    def __init__(self, raw: bytes) -> None:
        self.data = raw
        self.metadata = SimpleNamespace(
            num_delivered=3,
            sequence=SimpleNamespace(stream=17),
        )
        self.in_progress_count = 0
        self.ack_count = 0
        self.nak_count = 0
        self.term_count = 0

    async def in_progress(self) -> None:
        self.in_progress_count += 1

    async def ack(self) -> None:
        self.ack_count += 1

    async def nak(self) -> None:
        self.nak_count += 1

    async def term(self) -> None:
        self.term_count += 1


class FakeExecutor:
    def __init__(
        self,
        *,
        control_flow: bool = False,
        finalizers: tuple[str, ...] = ("in_progress", "commit"),
    ) -> None:
        self.deliveries: list[InboundDelivery] = []
        self.control_flow = control_flow
        self.finalizers = finalizers

    async def execute(self, delivery: InboundDelivery) -> ExecutionResult:
        self.deliveries.append(delivery)
        if self.control_flow:
            raise InjectedCrash("injected")
        await asyncio.sleep(0)
        for finalizer in self.finalizers:
            if finalizer == "in_progress":
                await delivery.in_progress()
            elif finalizer == "commit":
                await delivery.commit()
            elif finalizer == "retry":
                await delivery.retry()
            elif finalizer == "terminate":
                await delivery.terminate()
            else:
                raise AssertionError("unknown fake finalizer")
        return ExecutionResult("completed", {}, None, "disabled")


class FakeEventSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []

    def emit(self, event: Mapping[str, object]) -> None:
        self.events.append(event)


def binding(**changes: object) -> ConsumerBinding:
    values: dict[str, object] = {
        "stream_name": "AGENT_INBOX",
        "filter_subject": "agents.worker-1.inbox",
        "durable_name": "ec_20260725_a_worker_1_inbox",
        "ack_wait_seconds": 30,
        "max_deliver": 3,
        "max_ack_pending": 1,
    }
    values.update(changes)
    return ConsumerBinding(
        stream_name=cast(str, values["stream_name"]),
        filter_subject=cast(str, values["filter_subject"]),
        durable_name=cast(str, values["durable_name"]),
        ack_wait_seconds=cast(int, values["ack_wait_seconds"]),
        max_deliver=cast(int, values["max_deliver"]),
        max_ack_pending=cast(int, values["max_ack_pending"]),
    )


def consumer_info(**changes: object) -> ConsumerInfo:
    config: dict[str, object] = {
        "durable_name": "ec_20260725_a_worker_1_inbox",
        "ack_policy": "explicit",
        "ack_wait": 30_000_000_000,
        "max_deliver": 3,
        "max_ack_pending": 1,
        "filter_subject": "agents.worker-1.inbox",
    }
    top: dict[str, object] = {
        "stream_name": "AGENT_INBOX",
        "name": "ec_20260725_a_worker_1_inbox",
    }
    for key, value in changes.items():
        if key in top:
            top[key] = value
        else:
            config[key] = value
    response = {
        **top,
        "created": "2026-07-25T00:00:00.000000000Z",
        "config": config,
        "delivered": {"consumer_seq": 0, "stream_seq": 0},
        "ack_floor": {"consumer_seq": 0, "stream_seq": 0},
        "num_ack_pending": 0,
        "num_redelivered": 0,
        "num_waiting": 0,
        "num_pending": 0,
    }
    return cast(ConsumerInfo, ConsumerInfo.from_response(response))


async def legacy_handler(
    envelope: dict[str, object],
    context: Context,
) -> tuple[dict[str, object], str]:
    assert context.agent_id == "shell-1"
    return ({"body": envelope["payload"]}, "completed")


def command_bytes() -> bytes:
    return json.dumps(
        {
            "v": 1,
            "id": WIRE_ID,
            "type": "command",
            "sender_id": "sender-1",
            "recipient_id": "shell-1",
            "task_id": TASK_ID,
            "timestamp": NOW,
            "payload": {"body": "work"},
        }
    ).encode()


def delegation_bytes() -> bytes:
    return json.dumps(
        {
            "v": 1,
            "id": WIRE_ID,
            "type": "delegation",
            "sender_id": "sender-1",
            "recipient_id": "shell-1",
            "task_id": TASK_ID,
            "context_id": CONTEXT_ID,
            "hop_count": 8,
            "timestamp": NOW,
            "payload": {
                "body": "work",
                "parent_task_id": PARENT_ID,
            },
        }
    ).encode()


def mutate(instance: object, name: str, value: object) -> None:
    setattr(instance, name, value)


def test_consumer_binding_is_exact_and_frozen() -> None:
    assert [field.name for field in fields(ConsumerBinding)] == [
        "stream_name",
        "filter_subject",
        "durable_name",
        "ack_wait_seconds",
        "max_deliver",
        "max_ack_pending",
    ]
    instance = binding()
    with pytest.raises(FrozenInstanceError):
        mutate(instance, "stream_name", "OTHER")


def test_constructor_rejects_ambiguous_or_misbound_modes() -> None:
    nc = cast(NATS, FakeNC())
    executor = cast(TaskExecutor, FakeExecutor())
    sink = FakeEventSink()
    constructors: tuple[Callable[[], PullConsumer], ...] = (
        lambda: PullConsumer(agent_id="worker-1", nc=nc),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            handler=legacy_handler,
            executor=executor,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            consumer_binding=binding(),
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            handler=legacy_handler,
            event_sink=sink,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(filter_subject="agents.other.inbox"),
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            ack_wait_sec=300,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            ack_wait_sec=31,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            max_deliver=3,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            max_deliver=4,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            max_ack_pending=1,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            max_ack_pending=2,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            sender_allowlist=None,
        ),
        lambda: PullConsumer(
            agent_id="worker-1",
            nc=nc,
            executor=executor,
            event_sink=sink,
            consumer_binding=binding(),
            sender_allowlist={"sender-1"},
        ),
    )
    for construct in constructors:
        with pytest.raises(ValueError):
            construct()


def test_current_template_and_watchdog_legacy_shapes_remain_valid() -> None:
    nc = cast(NATS, FakeNC())
    template = PullConsumer(
        agent_id="shell-1",
        nc=nc,
        handler=legacy_handler,
        ack_wait_sec=300,
    )
    watchdog = PullConsumer(
        agent_id="watchdog-1",
        nc=nc,
        handler=legacy_handler,
        ack_wait_sec=30,
        max_ack_pending=1,
        max_deliver=3,
    )
    assert template.handler is legacy_handler
    assert watchdog.handler is legacy_handler


@async_test
async def test_legacy_run_uses_bootstrap_and_historical_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    async def fake_ensure_stream(js: object, agent_id: str) -> None:
        calls.append(("stream", (js, agent_id)))

    async def fake_ensure_consumer(
        js: object,
        agent_id: str,
        *,
        ack_wait_sec: int,
        max_ack_pending: int,
        max_deliver: int,
    ) -> None:
        calls.append(
            (
                "consumer",
                (js, agent_id, ack_wait_sec, max_ack_pending, max_deliver),
            )
        )

    monkeypatch.setattr(pull_consumer_module, "ensure_stream", fake_ensure_stream)
    monkeypatch.setattr(pull_consumer_module, "ensure_consumer", fake_ensure_consumer)
    nc = FakeNC()
    consumer = PullConsumer(
        agent_id="shell-1",
        nc=cast(NATS, nc),
        handler=legacy_handler,
        ack_wait_sec=30,
        max_deliver=4,
        max_ack_pending=2,
        sender_allowlist=None,
    )

    with pytest.raises(StopRun):
        await consumer.run()

    assert calls == [
        ("stream", (nc.js, "shell-1")),
        ("consumer", (nc.js, "shell-1", 30, 2, 4)),
    ]
    assert nc.js.consumer_info_calls == []
    assert nc.js.subscribe_calls == [
        {
            "subject": "agents.shell-1.inbox",
            "durable": "shell-1_inbox",
        }
    ]


@async_test
async def test_injected_run_verifies_existing_binding_and_never_bootstraps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("hard-coded bootstrap used")

    monkeypatch.setattr(pull_consumer_module, "ensure_stream", forbidden)
    monkeypatch.setattr(pull_consumer_module, "ensure_consumer", forbidden)
    info = consumer_info()
    assert info.config.ack_wait == 30.0
    nc = FakeNC(FakeJS(info))
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, nc),
        executor=cast(TaskExecutor, FakeExecutor()),
        event_sink=FakeEventSink(),
        consumer_binding=binding(),
    )

    with pytest.raises(StopRun):
        await consumer.run()

    assert nc.js.consumer_info_calls == [
        ("AGENT_INBOX", "ec_20260725_a_worker_1_inbox")
    ]
    assert nc.js.subscribe_calls == [
        {
            "subject": "agents.worker-1.inbox",
            "durable": "ec_20260725_a_worker_1_inbox",
            "stream": "AGENT_INBOX",
        }
    ]


@async_test
async def test_injected_binding_accepts_nats_ack_policy_enum() -> None:
    info = consumer_info()
    info.config.ack_policy = AckPolicy.EXPLICIT
    nc = FakeNC(FakeJS(info))
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, nc),
        executor=cast(TaskExecutor, FakeExecutor()),
        event_sink=FakeEventSink(),
        consumer_binding=binding(),
    )

    with pytest.raises(StopRun):
        await consumer.run()

    assert len(nc.js.subscribe_calls) == 1


@async_test
@cases(
    ("info_change", "value"),
    [
        ("stream_name", "OTHER"),
        ("name", "other"),
        ("durable_name", "other"),
        ("filter_subject", "agents.other.inbox"),
        ("ack_policy", "none"),
        ("ack_wait", 31_000_000_000),
        ("max_deliver", 4),
        ("max_ack_pending", 2),
    ],
)
async def test_injected_run_rejects_every_live_consumer_mismatch(
    info_change: str,
    value: object,
) -> None:
    nc = FakeNC(FakeJS(consumer_info(**{info_change: value})))
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, nc),
        executor=cast(TaskExecutor, FakeExecutor()),
        event_sink=FakeEventSink(),
        consumer_binding=binding(),
    )

    with pytest.raises(ValueError, match="consumer binding"):
        await consumer.run()

    assert nc.js.subscribe_calls == []


@async_test
async def test_injected_delivery_preserves_raw_identity_metadata_and_method_mapping() -> (
    None
):
    executor = FakeExecutor()
    sink = FakeEventSink()
    nc = FakeNC(FakeJS(consumer_info()))
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, nc),
        executor=cast(TaskExecutor, executor),
        event_sink=sink,
        consumer_binding=binding(),
    )
    raw = b'{"unchanged":true}'
    message = FakeMessage(raw)

    await consumer._handle_msg(cast(Msg, message))

    assert len(executor.deliveries) == 1
    delivery = executor.deliveries[0]
    assert delivery.worker_agent_id == "worker-1"
    assert delivery.raw is raw
    assert delivery.delivery_count == 3
    assert delivery.stream_sequence == 17
    assert message.in_progress_count == 1
    assert message.ack_count == 1
    assert message.nak_count == message.term_count == 0
    assert nc.publish_calls == []
    assert nc.js.publish_calls == []
    assert sink.events == []


@async_test
async def test_injected_delivery_maps_retry_and_terminate_without_consumer_events() -> (
    None
):
    executor = FakeExecutor(finalizers=("retry", "terminate"))
    sink = FakeEventSink()
    message = FakeMessage(command_bytes())
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, FakeNC(FakeJS(consumer_info()))),
        executor=cast(TaskExecutor, executor),
        event_sink=sink,
        consumer_binding=binding(),
    )

    await consumer._handle_msg(cast(Msg, message))

    assert message.in_progress_count == message.ack_count == 0
    assert message.nak_count == message.term_count == 1
    assert sink.events == []


@async_test
async def test_injected_consumer_cancels_and_awaits_keepalive() -> None:
    cleanup_complete = asyncio.Event()
    message = FakeMessage(command_bytes())
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, FakeNC(FakeJS(consumer_info()))),
        executor=cast(TaskExecutor, FakeExecutor(finalizers=())),
        event_sink=FakeEventSink(),
        consumer_binding=binding(),
    )

    async def sentinel_keepalive(_: Msg) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_complete.set()

    mutate(consumer, "_keepalive", sentinel_keepalive)
    await consumer._handle_msg(cast(Msg, message))

    assert cleanup_complete.is_set()


@async_test
async def test_injected_control_flow_escapes_without_explicit_nak() -> None:
    executor = FakeExecutor(control_flow=True)
    message = FakeMessage(command_bytes())
    consumer = PullConsumer(
        agent_id="worker-1",
        nc=cast(NATS, FakeNC(FakeJS(consumer_info()))),
        executor=cast(TaskExecutor, executor),
        event_sink=FakeEventSink(),
        consumer_binding=binding(),
    )

    with pytest.raises(InjectedCrash):
        await consumer._handle_msg(cast(Msg, message))

    assert message.ack_count == message.nak_count == message.term_count == 0


@async_test
async def test_legacy_result_bytes_and_publish_ack_order_are_unchanged() -> None:
    nc = FakeNC()
    events: list[str] = []

    async def handler(
        envelope: dict[str, object],
        context: Context,
    ) -> tuple[dict[str, object], str]:
        assert context.agent_id == "shell-1"
        assert cast(object, context.nc) is nc
        assert context.js is nc.js
        events.append("handler")
        return ({"body": "done"}, "completed")

    message = FakeMessage(command_bytes())
    original_js_publish = nc.js.publish
    original_nc_publish = nc.publish
    original_ack = message.ack

    async def js_publish(
        subject: str,
        payload: bytes,
        *,
        headers: Mapping[str, str],
    ) -> None:
        events.append("jetstream")
        await original_js_publish(subject, payload, headers=headers)

    async def nc_publish(subject: str, payload: bytes) -> None:
        events.append("outbox")
        await original_nc_publish(subject, payload)

    async def ack() -> None:
        events.append("ack")
        await original_ack()

    mutate(nc.js, "publish", js_publish)
    mutate(nc, "publish", nc_publish)
    mutate(message, "ack", ack)
    consumer = PullConsumer(
        agent_id="shell-1",
        nc=cast(NATS, nc),
        handler=handler,
        ack_wait_sec=300,
        max_deliver=3,
        max_ack_pending=1,
        sender_allowlist=None,
    )
    with (
        patch.object(uuid_module, "uuid4", return_value=RESULT_ID),
        patch.object(pull_consumer_module, "now_iso", return_value=NOW),
    ):
        await consumer._handle_msg(cast(Msg, message))

    expected = json.dumps(
        {
            "v": 1,
            "id": RESULT_ID,
            "type": "result",
            "sender_id": "shell-1",
            "recipient_id": "sender-1",
            "task_id": TASK_ID,
            "task_state": "completed",
            "timestamp": NOW,
            "payload": {"body": "done"},
        }
    ).encode()
    assert events == ["handler", "jetstream", "outbox", "ack"]
    assert nc.js.publish_calls == [
        (
            "agents.sender-1.inbox",
            expected,
            {"Nats-Msg-Id": RESULT_ID},
        )
    ]
    assert nc.publish_calls == [("agents.shell-1.outbox", expected)]
    assert message.ack_count == 1
    assert message.nak_count == message.term_count == 0


@async_test
async def test_legacy_poison_and_sender_allowlist_termination_are_unchanged() -> None:
    nc = FakeNC()
    consumer = PullConsumer(
        agent_id="shell-1",
        nc=cast(NATS, nc),
        handler=legacy_handler,
        sender_allowlist={"trusted-sender"},
    )
    malformed = FakeMessage(b"{")
    invalid_envelope = FakeMessage(b"{}")
    blocked_sender = FakeMessage(command_bytes())

    await consumer._handle_msg(cast(Msg, malformed))
    await consumer._handle_msg(cast(Msg, invalid_envelope))
    await consumer._handle_msg(cast(Msg, blocked_sender))

    for message in (malformed, invalid_envelope, blocked_sender):
        assert message.term_count == 1
        assert message.ack_count == message.nak_count == 0
    assert nc.js.publish_calls == []
    assert nc.publish_calls == []


@async_test
async def test_legacy_hop_rejection_still_publishes_then_acknowledges() -> None:
    calls = 0

    async def handler(
        envelope: dict[str, object],
        context: Context,
    ) -> tuple[dict[str, object], str]:
        nonlocal calls
        calls += 1
        return ({}, "completed")

    nc = FakeNC()
    message = FakeMessage(delegation_bytes())
    consumer = PullConsumer(
        agent_id="shell-1",
        nc=cast(NATS, nc),
        handler=handler,
    )
    with (
        patch.object(uuid_module, "uuid4", return_value=RESULT_ID),
        patch.object(pull_consumer_module, "now_iso", return_value=NOW),
    ):
        await consumer._handle_msg(cast(Msg, message))

    expected = {
        "v": 1,
        "id": RESULT_ID,
        "type": "result",
        "sender_id": "shell-1",
        "recipient_id": "sender-1",
        "task_id": TASK_ID,
        "task_state": "rejected",
        "timestamp": NOW,
        "payload": {"error": "hop_count_exceeded"},
        "context_id": CONTEXT_ID,
    }
    assert calls == 0
    assert json.loads(nc.js.publish_calls[0][1]) == expected
    assert nc.js.publish_calls[0][0] == "agents.sender-1.inbox"
    assert nc.js.publish_calls[0][2] == {"Nats-Msg-Id": RESULT_ID}
    assert nc.publish_calls == [
        ("agents.shell-1.outbox", json.dumps(expected).encode())
    ]
    assert message.ack_count == 1
    assert message.nak_count == message.term_count == 0


@async_test
async def test_legacy_handler_failure_result_log_and_nak_are_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_calls: list[tuple[object, ...]] = []

    async def failing_handler(
        envelope: dict[str, object],
        context: Context,
    ) -> tuple[dict[str, object], str]:
        raise RuntimeError("private detail")

    async def fake_publish_log(
        nc: NATS,
        agent_id: str,
        *,
        level: str,
        message: str,
        source: str | None = None,
        extra: dict[str, object] | None = None,
    ) -> None:
        log_calls.append((nc, agent_id, level, source, message, extra))

    monkeypatch.setattr(template_module, "publish_log", fake_publish_log)
    nc = FakeNC()
    message = FakeMessage(command_bytes())
    consumer = PullConsumer(
        agent_id="shell-1",
        nc=cast(NATS, nc),
        handler=failing_handler,
    )
    with (
        patch.object(uuid_module, "uuid4", return_value=RESULT_ID),
        patch.object(pull_consumer_module, "now_iso", return_value=NOW),
    ):
        await consumer._handle_msg(cast(Msg, message))

    expected = {
        "v": 1,
        "id": RESULT_ID,
        "type": "result",
        "sender_id": "shell-1",
        "recipient_id": "sender-1",
        "task_id": TASK_ID,
        "task_state": "failed",
        "timestamp": NOW,
        "payload": {"error": "RuntimeError"},
    }
    assert json.loads(nc.js.publish_calls[0][1]) == expected
    assert nc.publish_calls == [
        ("agents.shell-1.outbox", json.dumps(expected).encode())
    ]
    assert log_calls == [
        (
            nc,
            "shell-1",
            "ERROR",
            "handler",
            "handler raised RuntimeError: private detail",
            {"task_id": TASK_ID, "envelope_type": "command"},
        )
    ]
    assert message.nak_count == 1
    assert message.ack_count == message.term_count == 0


@async_test
async def test_legacy_context_progress_preserves_gemma_and_hermes_behavior() -> None:
    nc = FakeNC()
    message = FakeMessage(command_bytes())
    context = Context(
        agent_id="gemma-1",
        nc=cast(NATS, nc),
        js=nc.js,
        msg=cast(Msg, message),
    )

    with (
        patch.object(uuid_module, "uuid4", return_value=PROGRESS_ID),
        patch.object(pull_consumer_module, "now_iso", return_value=NOW),
    ):
        await context.publish_progress(
            TASK_ID,
            body="delta",
            progress=40,
            extra={"delta": "delta", "skill_id": "summarize"},
        )

    expected = json.dumps(
        {
            "v": 1,
            "id": PROGRESS_ID,
            "type": "task.progress",
            "sender_id": "gemma-1",
            "task_id": TASK_ID,
            "task_state": "working",
            "timestamp": NOW,
            "payload": {
                "message": "delta",
                "progress": 40,
                "delta": "delta",
                "skill_id": "summarize",
            },
        }
    ).encode()
    assert nc.publish_calls == [(f"agents.gemma-1.task_progress.{TASK_ID}", expected)]
    assert cast(object, context.nc) is nc
    await context.in_progress()
    assert message.in_progress_count == 1
