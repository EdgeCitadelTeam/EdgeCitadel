"""JetStream pull consumer with one delivery/executor path.

Handler compatibility contract: handler must produce (result_payload: dict, task_state: str)
for command/delegation/cancel, OR return None for types with no reply
(heartbeat, status, broadcast, log — but those don't land on inbox anyway).

Ack happens only after successful handler return AND successful result publish
(for command/delegation/cancel). in_progress() is called periodically to
extend ack_wait.

Handler-based Plugin runtimes are retained as a constructor compatibility surface,
but are adapted to the same ``InboundDelivery`` execution path as injected
``TaskExecutor`` instances.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import cast

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js.api import AckPolicy, ConsumerInfo

from .jetstream import ensure_consumer, ensure_stream

from .task_executor import InboundDelivery, TaskExecutor
from .task_publisher import EventSink
from .validator import ValidationError, default_validator

log = logging.getLogger(__name__)

HOP_COUNT_MAX = 8


class _Unset:
    __slots__ = ()


_UNSET = _Unset()


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass
class Context:
    agent_id: str
    nc: NATS
    js: object
    msg: Msg

    async def in_progress(self) -> None:
        await self.msg.in_progress()

    async def publish_progress(
        self,
        task_id: str,
        *,
        body: str = "",
        progress: int | None = None,
        extra: dict[str, object] | None = None,
    ) -> None:
        """Publish a task.progress envelope. `extra` merges into payload
        alongside body/progress (used by Gemma to carry skill_id during
        streaming)."""
        payload: dict[str, object] = {"message": body}
        if progress is not None:
            payload["progress"] = progress
        if extra:
            payload.update(extra)
        env = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": "task.progress",
            "sender_id": self.agent_id,
            "task_id": task_id,
            "task_state": "working",
            "timestamp": now_iso(),
            "payload": payload,
        }
        await self.nc.publish(
            f"agents.{self.agent_id}.task_progress.{task_id}",
            json.dumps(env).encode(),
        )


Handler = Callable[
    [dict[str, object], Context],
    Awaitable[tuple[dict[str, object], str]],
]


@dataclass(frozen=True)
class ConsumerBinding:
    stream_name: str
    filter_subject: str
    durable_name: str
    ack_wait_seconds: int
    max_deliver: int
    max_ack_pending: int


class _NATSInboundDelivery:
    def __init__(self, worker_agent_id: str, msg: Msg) -> None:
        metadata = msg.metadata
        delivery_count = metadata.num_delivered
        stream_sequence = metadata.sequence.stream
        if type(delivery_count) is not int or delivery_count < 1:
            raise ValueError("invalid JetStream delivery count")
        if type(stream_sequence) is not int or stream_sequence < 1:
            raise ValueError("invalid JetStream stream sequence")
        self.worker_agent_id = worker_agent_id
        self.raw = msg.data
        self.delivery_count = delivery_count
        self.stream_sequence: int | None = stream_sequence
        self._msg = msg

    async def in_progress(self) -> None:
        await self._msg.in_progress()

    async def commit(self) -> None:
        await self._msg.ack()

    async def retry(self) -> None:
        await self._msg.nak()

    async def terminate(self) -> None:
        await self._msg.term()


def _valid_binding(binding: ConsumerBinding) -> bool:
    return (
        bool(binding.stream_name)
        and bool(binding.filter_subject)
        and bool(binding.durable_name)
        and type(binding.ack_wait_seconds) is int
        and binding.ack_wait_seconds > 0
        and type(binding.max_deliver) is int
        and binding.max_deliver > 0
        and type(binding.max_ack_pending) is int
        and binding.max_ack_pending > 0
    )


def _explicit_ack(value: object) -> bool:
    if value is AckPolicy.EXPLICIT:
        return True
    return isinstance(value, str) and value == "explicit"


def _binding_matches(info: ConsumerInfo, binding: ConsumerBinding) -> bool:
    config = info.config
    return (
        info.stream_name == binding.stream_name
        and info.name == binding.durable_name
        and config.durable_name == binding.durable_name
        and config.filter_subject == binding.filter_subject
        and _explicit_ack(config.ack_policy)
        and config.ack_wait == float(binding.ack_wait_seconds)
        and config.max_deliver == binding.max_deliver
        and config.max_ack_pending == binding.max_ack_pending
    )


class _HandlerExecutor:
    """Adapt the pre-injection handler API to an ``InboundDelivery``.

    This is deliberately private: new integrations inject ``TaskExecutor``;
    maintained handler-based Plugin runtimes share the exact same consumer loop and
    NATS delivery finalization while they migrate their business handlers.
    """

    def __init__(self, consumer: PullConsumer) -> None:
        self._consumer = consumer

    async def execute(self, delivery: InboundDelivery) -> None:
        await self._consumer._execute_handler_delivery(delivery)


class PullConsumer:
    def __init__(
        self,
        *,
        agent_id: str,
        nc: NATS,
        handler: Handler | None = None,
        ack_wait_sec: int | _Unset = _UNSET,
        max_deliver: int | _Unset = _UNSET,
        max_ack_pending: int | _Unset = _UNSET,
        sender_allowlist: set[str] | None | _Unset = _UNSET,
        executor: TaskExecutor | None = None,
        event_sink: EventSink | None = None,
        consumer_binding: ConsumerBinding | None = None,
    ) -> None:
        ack_wait_supplied = not isinstance(ack_wait_sec, _Unset)
        max_deliver_supplied = not isinstance(max_deliver, _Unset)
        max_ack_pending_supplied = not isinstance(max_ack_pending, _Unset)
        sender_allowlist_supplied = not isinstance(sender_allowlist, _Unset)
        handler_ack_wait_sec = 300 if isinstance(ack_wait_sec, _Unset) else ack_wait_sec
        handler_max_deliver = 3 if isinstance(max_deliver, _Unset) else max_deliver
        handler_max_ack_pending = (
            1 if isinstance(max_ack_pending, _Unset) else max_ack_pending
        )
        handler_sender_allowlist = (
            None if isinstance(sender_allowlist, _Unset) else sender_allowlist
        )

        if (handler is None) == (executor is None):
            raise ValueError("exactly one of handler and executor is required")
        injected = executor is not None
        if injected:
            if event_sink is None or consumer_binding is None:
                raise ValueError(
                    "injected mode requires event_sink and consumer_binding"
                )
            if (
                ack_wait_supplied
                or max_deliver_supplied
                or max_ack_pending_supplied
                or sender_allowlist_supplied
            ):
                raise ValueError("handler options are invalid in injected mode")
            if (
                not _valid_binding(consumer_binding)
                or consumer_binding.filter_subject != f"agents.{agent_id}.inbox"
            ):
                raise ValueError("consumer binding does not match agent")
        elif event_sink is not None or consumer_binding is not None:
            raise ValueError("injected options are invalid in handler mode")

        self.agent_id = agent_id
        self.nc = nc
        domain = os.environ.get("NATS_DOMAIN")
        self.js = nc.jetstream(domain=domain) if domain else nc.jetstream()
        self.handler = handler
        self.executor = executor
        self.event_sink = event_sink
        self.consumer_binding = consumer_binding
        self.ack_wait_sec = (
            consumer_binding.ack_wait_seconds
            if consumer_binding is not None
            else handler_ack_wait_sec
        )
        self.max_deliver = (
            consumer_binding.max_deliver
            if consumer_binding is not None
            else handler_max_deliver
        )
        self.max_ack_pending = (
            consumer_binding.max_ack_pending
            if consumer_binding is not None
            else handler_max_ack_pending
        )
        self.sender_allowlist = handler_sender_allowlist
        self.validator = default_validator()
        if self.executor is None:
            self.executor = cast(TaskExecutor, _HandlerExecutor(self))
        self._running = False

    async def run(self) -> None:
        if self.consumer_binding is None:
            await ensure_stream(self.js, self.agent_id)
            await ensure_consumer(
                self.js,
                self.agent_id,
                ack_wait_sec=self.ack_wait_sec,
                max_ack_pending=self.max_ack_pending,
                max_deliver=self.max_deliver,
            )
            psub = await self.js.pull_subscribe(
                subject=f"agents.{self.agent_id}.inbox",
                durable=f"{self.agent_id}_inbox",
            )
        else:
            binding = cast(ConsumerBinding, self.consumer_binding)
            info = await self.js.consumer_info(
                binding.stream_name,
                binding.durable_name,
            )
            if not _binding_matches(info, binding):
                raise ValueError("live consumer binding does not match configuration")
            psub = await self.js.pull_subscribe(
                subject=binding.filter_subject,
                durable=binding.durable_name,
                stream=binding.stream_name,
            )

        self._running = True
        while self._running:
            try:
                msgs = await psub.fetch(batch=1, timeout=30)
            except asyncio.TimeoutError:
                continue
            except Exception as e:  # noqa: BLE001
                log.warning("fetch error: %s", e)
                await asyncio.sleep(1)
                continue
            for m in msgs:
                await self._handle_msg(m)

    async def stop(self) -> None:
        self._running = False

    async def _handle_msg(self, msg: Msg) -> None:
        executor = cast(TaskExecutor, self.executor)
        delivery: InboundDelivery = _NATSInboundDelivery(self.agent_id, msg)
        keepalive = asyncio.create_task(self._keepalive(msg))
        try:
            await executor.execute(delivery)
        finally:
            keepalive.cancel()
            try:
                await keepalive
            except asyncio.CancelledError:
                pass

    async def _execute_handler_delivery(self, delivery: InboundDelivery) -> None:
        msg = cast(_NATSInboundDelivery, delivery)._msg
        try:
            env = json.loads(delivery.raw)
        except json.JSONDecodeError:
            await msg.term()
            return

        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("%s: invalid envelope, terminating: %s", self.agent_id, e)
            await msg.term()
            return

        if (
            self.sender_allowlist is not None
            and env["sender_id"] not in self.sender_allowlist
        ):
            log.warning(
                "%s: bridge_sender_not_allowlisted sender=%s",
                self.agent_id,
                env["sender_id"],
            )
            await msg.term()
            return

        if env["type"] == "delegation" and env.get("hop_count", 0) >= HOP_COUNT_MAX:
            await self._publish_result(
                env,
                task_state="rejected",
                error="hop_count_exceeded",
            )
            await msg.ack()
            return

        try:
            ctx = Context(
                agent_id=self.agent_id,
                nc=self.nc,
                js=self.js,
                msg=msg,
            )
            handler = cast(Handler, self.handler)
            result, state = await handler(env, ctx)
            await self._publish_result(env, task_state=state, payload=result)
            await msg.ack()
        except Exception as e:
            log.exception(
                "%s: handler failed: %s",
                self.agent_id,
                e,  # noqa: TRY401
            )
            try:
                await self._publish_result(
                    env,
                    task_state="failed",
                    error=type(e).__name__,
                )
            except Exception:  # noqa: BLE001,S110
                pass
            # Best-effort log envelope so the dashboard's Logs tab surfaces
            # handler crashes alongside successes (LogViewer reads
            # payload.level/source/message).
            try:
                from .template import publish_log

                await publish_log(
                    self.nc,
                    self.agent_id,
                    level="ERROR",
                    source="handler",
                    message=f"handler raised {type(e).__name__}: {e}",
                    extra={
                        "task_id": env.get("task_id"),
                        "envelope_type": env.get("type"),
                    },
                )
            except Exception:  # noqa: BLE001,S110
                pass
            await msg.nak()

    async def _keepalive(self, msg: Msg) -> None:
        cadence = max(1.0, self.ack_wait_sec / 3)
        try:
            while True:
                await asyncio.sleep(cadence)
                await msg.in_progress()
        except asyncio.CancelledError:
            return

    async def _publish_result(
        self,
        inbound: dict[str, object],
        *,
        task_state: str,
        payload: dict[str, object] | None = None,
        error: str | None = None,
    ) -> None:
        if inbound["type"] not in ("command", "delegation", "cancel"):
            return
        out = {
            "v": 1,
            "id": str(uuid.uuid4()),
            "type": "result",
            "sender_id": self.agent_id,
            "recipient_id": inbound["sender_id"],
            "task_id": inbound["task_id"],
            "task_state": task_state,
            "timestamp": now_iso(),
            "payload": (payload or {}) | ({"error": error} if error else {}),
        }
        if inbound.get("context_id"):
            out["context_id"] = inbound["context_id"]
        data = json.dumps(out).encode()
        # durable publish to sender's inbox
        await self.js.publish(
            f"agents.{inbound['sender_id']}.inbox",
            data,
            headers={"Nats-Msg-Id": out["id"]},
        )
        # mirror to own outbox (plain NATS; best-effort)
        await self.nc.publish(f"agents.{self.agent_id}.outbox", data)
