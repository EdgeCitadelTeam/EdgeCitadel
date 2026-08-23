"""Split-plane EdgeCitadel benchmark transport."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import urllib.parse
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, cast

from nats.aio.client import Client as NATS
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import NotFoundError

import nats
from adapters._common.pull_consumer import ConsumerBinding, PullConsumer
from adapters._common.task_executor import TaskExecutor
from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import (
    ValidationError,
    canonical_json,
    default_validator,
)
from scripts.research.modes.base import (
    EventSink,
    Mode,
    ObservedEnvelope,
    TransportSnapshot,
)
from scripts.research.modes.jetstream_config import (
    durable_name,
    task_stream_config,
    transient_stream_config,
)

CoordinatorRestart = Callable[[], Awaitable[str | None]]
WorkerOperation = Callable[[str], Awaitable[None]]
AsyncSleep = Callable[[float], Awaitable[None]]
_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)

ABLATIONS = {
    "none": {"nats_msg_id": False, "outcome_ledger": False},
    "broker-only": {"nats_msg_id": True, "outcome_ledger": False},
    "full-contract": {"nats_msg_id": True, "outcome_ledger": True},
}


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _uuid4() -> str:
    return str(uuid.uuid4()).lower()


def _valid_nats_url(value: object) -> bool:
    if type(value) is not str or not value or any(part.isspace() for part in value):
        return False
    try:
        parsed = urllib.parse.urlsplit(value)
        host = parsed.hostname
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "nats"
        and bool(parsed.netloc)
        and bool(host)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def _enum_value(value: object) -> object:
    return getattr(value, "value", value)


class _ObserverDelivery:
    def __init__(self, message: object) -> None:
        self._message = message
        self._acked = False

    async def ack(self) -> None:
        if not self._acked:
            await cast(Any, self._message).ack()
            self._acked = True


class _ExecutorProxy:
    def __init__(
        self,
        executor: TaskExecutor,
        event_sink: EventSink,
        evidence_clock_ns: Callable[[], int],
        epoch_now: Callable[[], str],
        component: str,
    ) -> None:
        self._executor = executor
        self._event_sink = event_sink
        self._evidence_clock_ns = evidence_clock_ns
        self._epoch_now = epoch_now
        self._component = component

    async def execute(self, delivery: object) -> object:
        raw = cast(Any, delivery).raw
        self._event_sink.emit(
            {
                "monotonic_ns": self._evidence_clock_ns(),
                "epoch_time": self._epoch_now(),
                "component": self._component,
                "event": "transport.worker_delivery",
                "data": {
                    "worker_agent_id": cast(Any, delivery).worker_agent_id,
                    "raw_sha256": hashlib.sha256(raw).hexdigest(),
                    "delivery_count": cast(Any, delivery).delivery_count,
                    "stream_sequence": cast(Any, delivery).stream_sequence,
                },
            }
        )
        return await self._executor.execute(cast(Any, delivery))


class _FaultController:
    def __init__(
        self,
        transport: _JetStreamTransport,
        coordinator_restart: CoordinatorRestart | None,
        worker_stop: WorkerOperation | None,
        worker_start: WorkerOperation | None,
    ) -> None:
        self._transport = transport
        self._coordinator_restart = coordinator_restart
        self._worker_stop = worker_stop
        self._worker_start = worker_start

    async def disconnect_progress_observer(self) -> None:
        await self._transport._disconnect_progress_observer()

    async def reconnect_progress_observer(self) -> None:
        await self._transport._reconnect_progress_observer()

    async def stop_worker(self, agent_id: str) -> None:
        await self._transport._stop_worker(agent_id, self._worker_stop)

    async def start_worker(self, agent_id: str) -> None:
        await self._transport._start_worker(agent_id, self._worker_start)

    async def restart_coordinator(self) -> None:
        await self._transport._restart_coordinator(self._coordinator_restart)


class _JetStreamTransport:
    def __init__(
        self,
        *,
        nats_url: str,
        run_id: str,
        token: str,
        event_sink: EventSink,
        agent_card: Mapping[str, object] | None,
        observer_agent_id: str | None,
        mode: Mode,
        ablation: str,
        nats_msg_id: bool,
        outcome_ledger: bool,
        durable_transients: bool,
        coordinator_restart: CoordinatorRestart | None,
        worker_stop: WorkerOperation | None,
        worker_start: WorkerOperation | None,
        connection_factory: Callable[..., Awaitable[NATS]],
        evidence_clock_ns: Callable[[], int],
        epoch_now: Callable[[], str],
        uuid4: Callable[[], str],
        sleep: AsyncSleep,
    ) -> None:
        if not _valid_nats_url(nats_url):
            raise ValueError("invalid nats_url")
        if type(run_id) is not str or _ID_PATTERN.fullmatch(run_id) is None:
            raise ValueError("invalid run_id")
        if type(token) is not str or _TOKEN_PATTERN.fullmatch(token) is None:
            raise ValueError("invalid token")
        if observer_agent_id is not None and _ID_PATTERN.fullmatch(observer_agent_id) is None:
            raise ValueError("invalid observer_agent_id")
        detached_card: dict[str, object] | None = None
        if agent_card is not None:
            try:
                decoded = cast(object, json.loads(canonical_json(agent_card)))
                if not isinstance(decoded, dict):
                    raise TypeError
                detached_card = cast(dict[str, object], decoded)
            except (TypeError, ValueError, UnicodeError, RecursionError):
                raise ValueError("invalid agent_card") from None
        self._nats_url = nats_url
        self._run_id = run_id
        self._token = token
        self._event_sink = event_sink
        self._agent_card = detached_card
        self._observer_agent_id = observer_agent_id
        self._mode = mode
        self._ablation = ablation
        self._nats_msg_id = nats_msg_id
        self._outcome_ledger = outcome_ledger
        self._durable_transients = durable_transients
        self._connection_factory = connection_factory
        self._evidence_clock_ns = evidence_clock_ns
        self._epoch_now = epoch_now
        self._uuid4 = uuid4
        self._sleep = sleep
        self._nc: NATS | None = None
        self._closed = False
        self._background_failure: BaseException | None = None
        self._operation_lock = asyncio.Lock()
        self._created_streams: dict[str, dict[str, object]] = {}
        self._created_consumers: dict[str, dict[str, object]] = {}
        self._terminal_queues: dict[str, deque[ObservedEnvelope]] = {}
        self._terminal_events: dict[str, asyncio.Event] = {}
        self._terminal_task: asyncio.Task[None] | None = None
        self._transient_task: asyncio.Task[None] | None = None
        self._transient_subscription: object | None = None
        self._receiver_task: asyncio.Task[None] | None = None
        self._receiver_pull: PullConsumer | None = None
        self._terminal_desired = False
        self._progress_desired = False
        self._receiver_desired = False
        self._receiver_ready: asyncio.Event | None = None
        self._receiver_agent_id: str | None = None
        self._receiver_executor: TaskExecutor | None = None
        self._subscriptions: list[object] = []
        self._terminal_observation_index = 0
        self._transient_observation_index = 0
        self._registration_observation_index = 0
        self._transient_cutoff = 0
        self._accumulated_in_bytes = 0
        self._accumulated_out_bytes = 0
        self._faults = _FaultController(
            self, coordinator_restart, worker_stop, worker_start
        )

    @property
    def faults(self) -> _FaultController:
        return self._faults

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def outcome_ledger_enabled(self) -> bool:
        return self._outcome_ledger

    @property
    def resolved_config(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "mode": self._mode.value,
                "ablation": self._ablation,
                "nats_msg_id": self._nats_msg_id,
                "outcome_ledger": self._outcome_ledger,
            }
        )

    async def start_terminal_observer(self) -> None:
        async with self._operation_lock:
            self._check_open()
            if self._terminal_desired:
                raise RuntimeError("terminal observer already started")
            agent_id = self._require_observer_agent()
            await self._ensure_streams()
            binding = await self._ensure_consumer("result", agent_id)
            if binding is None:
                raise RuntimeError("result consumer cannot bind an observer")
            connection = await self._ensure_connection()
            subscription = await connection.jetstream().pull_subscribe(
                subject=binding.filter_subject,
                durable=binding.durable_name,
                stream=binding.stream_name,
            )
            self._terminal_desired = True
            self._terminal_task = asyncio.create_task(
                self._pull_loop(subscription, self._on_terminal)
            )
            self._emit("transport.observer_ready", {"kind": "terminal"})

    async def start_progress_observer(self) -> None:
        async with self._operation_lock:
            self._check_open()
            if self._progress_desired:
                raise RuntimeError("progress observer already started")
            self._progress_desired = True
            try:
                if self._durable_transients:
                    await self._start_durable_progress_observer()
                else:
                    await self._start_core_progress_observer()
            except BaseException:
                self._progress_desired = False
                raise
            self._emit("transport.observer_ready", {"kind": "progress"})

    async def start_receiver(self, agent_id: str, executor: TaskExecutor) -> None:
        async with self._operation_lock:
            self._check_open()
            self._validate_agent_id(agent_id)
            if self._receiver_desired:
                raise RuntimeError("receiver already started")
            self._validate_receiver_card(agent_id)
            await self._ensure_streams()
            binding = await self._ensure_consumer("task", agent_id)
            if binding is None:
                raise RuntimeError("task consumer cannot bind a worker")
            connection = await self._ensure_connection()
            proxy = _ExecutorProxy(
                executor,
                self._event_sink,
                self._evidence_clock_ns,
                self._epoch_now,
                self._mode.value.replace("-", "_"),
            )
            pull = PullConsumer(
                agent_id=agent_id,
                nc=connection,
                executor=cast(TaskExecutor, proxy),
                event_sink=self._event_sink,
                consumer_binding=binding,
            )
            self._receiver_agent_id = agent_id
            self._receiver_executor = executor
            self._receiver_pull = pull
            self._receiver_ready = asyncio.Event()
            self._receiver_desired = True
            self._receiver_task = asyncio.create_task(pull.run())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                if self._receiver_task.done():
                    self._receiver_task.result()
                if pull._running:
                    self._receiver_ready.set()
                    break
                await asyncio.sleep(0.01)
            if self._receiver_ready is None or not self._receiver_ready.is_set():
                await self._stop_receiver_task()
                raise TimeoutError("receiver readiness timed out")
            registration = {
                "v": 1,
                "id": self._uuid4(),
                "type": "register",
                "sender_id": agent_id,
                "timestamp": self._epoch_now(),
                "payload": self._agent_card,
            }
            await self._publish_core(
                f"agents.{agent_id}.register", registration, "register"
            )
            self._emit(
                "transport.receiver_ready", {"kind": "receiver", "agent_id": agent_id}
            )

    async def wait_receiver_ready(self, agent_id: str, timeout_s: float) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if type(timeout_s) not in (int, float) or timeout_s <= 0:
            raise ValueError("invalid timeout_s")
        if self._receiver_agent_id != agent_id or self._receiver_ready is None:
            raise RuntimeError("receiver agent mismatch")
        try:
            await asyncio.wait_for(self._receiver_ready.wait(), timeout=timeout_s)
        except TimeoutError:
            raise TimeoutError from None
        self._check_open()

    async def submit_task(self, envelope: Mapping[str, object]) -> PublicationReceipt:
        decoded = self._validated_envelope(envelope, ("command", "delegation", "cancel"))
        return await self._publish_js(
            f"agents.{decoded['recipient_id']}.inbox", decoded, "submit_task"
        )

    async def publish_progress(self, envelope: Mapping[str, object]) -> PublicationReceipt:
        decoded = self._validated_envelope(envelope, ("task.progress",))
        return await self._publish_transient(
            f"agents.{decoded['sender_id']}.task_progress.{decoded['task_id']}",
            decoded,
            "publish_progress",
        )

    async def publish_terminal(self, envelope: Mapping[str, object]) -> PublicationReceipt:
        decoded = self._validated_envelope(envelope, ("result",))
        return await self._publish_js(
            f"agents.{decoded['recipient_id']}.inbox", decoded, "publish_terminal"
        )

    async def publish_heartbeat(self, envelope: Mapping[str, object]) -> PublicationReceipt:
        decoded = self._validated_envelope(envelope, ("heartbeat",))
        return await self._publish_transient(
            f"agents.{decoded['sender_id']}.heartbeat", decoded, "publish_heartbeat"
        )

    async def _publish_status(self, envelope: Mapping[str, object]) -> PublicationReceipt:
        decoded = self._validated_envelope(envelope, ("status",))
        return await self._publish_transient(
            f"agents.{decoded['sender_id']}.status", decoded, "publish_status"
        )

    async def observe_terminal(
        self, task_id: str, timeout_s: float
    ) -> ObservedEnvelope | None:
        self._check_open()
        if not self._terminal_desired:
            raise RuntimeError("terminal observer is not started")
        if type(task_id) is not str or _UUID_PATTERN.fullmatch(task_id) is None:
            raise ValueError("invalid task_id")
        if type(timeout_s) not in (int, float) or timeout_s < 0:
            raise ValueError("invalid timeout_s")
        queue = self._terminal_queues.setdefault(task_id, deque())
        if queue:
            return queue.popleft()
        if timeout_s == 0:
            return None
        event = self._terminal_events.setdefault(task_id, asyncio.Event())
        event.clear()
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except TimeoutError:
            self._check_open()
            return None
        return queue.popleft() if queue else None

    async def inspect_state(self) -> TransportSnapshot:
        self._check_open()
        current_in, current_out = self._connection_stats()
        streams = {
            name: MappingProxyType(dict(config))
            for name, config in self._created_streams.items()
        }
        consumers: dict[str, Mapping[str, object]] = {
            name: MappingProxyType(dict(config))
            for name, config in self._created_consumers.items()
        }
        storage_bytes = 0
        message_count = 0
        pending: int | None = None
        ack_pending: int | None = None
        if self._nc is not None:
            jetstream = self._nc.jetstream()
            for name in streams:
                info = await jetstream.stream_info(name)
                bytes_used = getattr(info.state, "bytes", None)
                messages = getattr(info.state, "messages", None)
                if type(bytes_used) is not int or bytes_used < 0:
                    raise RuntimeError("invalid JetStream stream state")
                if type(messages) is not int or messages < 0:
                    raise RuntimeError("invalid JetStream stream state")
                storage_bytes += bytes_used
                message_count += messages
            if self._created_consumers:
                pending = 0
                ack_pending = 0
                for name, config in self._created_consumers.items():
                    info = await jetstream.consumer_info(
                        cast(str, config["stream_name"]),
                        name,
                    )
                    consumer_pending = getattr(info, "num_pending", None)
                    consumer_ack_pending = getattr(info, "num_ack_pending", None)
                    if type(consumer_pending) is not int or consumer_pending < 0:
                        raise RuntimeError("invalid JetStream consumer state")
                    if type(consumer_ack_pending) is not int or consumer_ack_pending < 0:
                        raise RuntimeError("invalid JetStream consumer state")
                    consumer = dict(config)
                    consumer["pending"] = consumer_pending
                    consumer["ack_pending"] = consumer_ack_pending
                    consumers[name] = MappingProxyType(consumer)
                    pending += consumer_pending
                    ack_pending += consumer_ack_pending
        return TransportSnapshot(
            mode=self._mode,
            streams=MappingProxyType(streams),
            consumers=MappingProxyType(consumers),
            pending=pending,
            ack_pending=ack_pending,
            connection_bytes=MappingProxyType(
                {
                    "in_bytes": self._accumulated_in_bytes + current_in,
                    "out_bytes": self._accumulated_out_bytes + current_out,
                }
            ),
            storage_bytes=storage_bytes,
            message_count=message_count,
        )

    async def close(self) -> None:
        async with self._operation_lock:
            if self._closed:
                return
            self._closed = True
            self._terminal_desired = False
            self._progress_desired = False
            self._receiver_desired = False
            await self._stop_receiver_task()
            await self._cancel_task("_terminal_task")
            await self._unsubscribe_transient_pull()
            await self._cancel_task("_transient_task")
            if self._subscriptions:
                await asyncio.gather(
                    *(cast(Any, subscription).unsubscribe() for subscription in self._subscriptions),
                    return_exceptions=True,
                )
            self._subscriptions.clear()
            if self._nc is not None:
                current_in, current_out = self._connection_stats()
                try:
                    await self._nc.close()
                finally:
                    self._accumulated_in_bytes += current_in
                    self._accumulated_out_bytes += current_out
                    self._nc = None
            if self._background_failure is not None:
                raise self._background_failure

    async def _ensure_connection(self) -> NATS:
        self._check_open()
        if self._nc is not None:
            return self._nc
        try:
            connection = await self._connection_factory(
                servers=[self._nats_url],
                token=self._token,
                allow_reconnect=False,
                max_reconnect_attempts=0,
                connect_timeout=2,
            )
            await connection.flush()
        except Exception as error:  # noqa: BLE001 - convert transport failures.
            if "authorization" in str(error).casefold():
                raise PermissionError("transport authentication failed") from None
            raise RuntimeError("jetstream connection failed") from None
        self._nc = connection
        return connection

    async def _ensure_streams(self) -> None:
        await self._ensure_stream(task_stream_config(self._run_id))
        if self._durable_transients:
            await self._ensure_stream(transient_stream_config(self._run_id))
        else:
            await self._verify_edgecitadel_transient_subjects()

    async def _verify_edgecitadel_transient_subjects(self) -> None:
        jetstream = (await self._ensure_connection()).jetstream()
        for subject in (
            "agents.probe.task_progress.00000000-0000-4000-8000-000000000001",
            "agents.probe.heartbeat",
            "agents.probe.status",
            "agents.probe.register",
        ):
            try:
                await jetstream.find_stream_name_by_subject(subject)
            except NotFoundError:
                continue
            raise RuntimeError("JetStream captures transient subject")

    async def _ensure_stream(self, expected: Mapping[str, object]) -> None:
        connection = await self._ensure_connection()
        js = connection.jetstream()
        name = cast(str, expected["name"])
        try:
            info = await js.stream_info(name)
        except NotFoundError:
            config = StreamConfig(
                name=name,
                subjects=cast(list[str], expected["subjects"]),
                retention=RetentionPolicy(cast(str, expected["retention"])),
                storage=StorageType(cast(str, expected["storage"])),
                max_age=cast(int, expected["max_age_ns"]) / 1_000_000_000,
                max_bytes=cast(int, expected["max_bytes"]),
                max_msg_size=cast(int, expected["max_msg_size"]),
                discard=DiscardPolicy(cast(str, expected["discard"])),
                duplicate_window=cast(int, expected["duplicate_window_ns"]) / 1_000_000_000,
            )
            await js.add_stream(config)
            info = await js.stream_info(name)
        normalized = self._normalize_stream(info)
        if normalized != dict(expected):
            raise RuntimeError("live stream does not match configuration")
        self._created_streams[name] = normalized

    async def _ensure_consumer(
        self,
        kind: str,
        agent_id: str,
    ) -> ConsumerBinding | None:
        if kind not in {"task", "result", "transient"}:
            raise ValueError("invalid consumer kind")
        if kind == "transient":
            stream_name = "TRANSIENT_EVENTS"
            filter_subject: str | None = None
            max_ack_pending = 256
        else:
            stream_name = "AGENT_INBOX"
            filter_subject = f"agents.{agent_id}.inbox"
            max_ack_pending = 1
        name = durable_name(kind, self._run_id, agent_id)
        expected = {
            "stream_name": stream_name,
            "filter_subject": filter_subject,
            "durable_name": name,
            "ack_policy": "explicit",
            "ack_wait_ns": 30_000_000_000,
            "max_deliver": 3,
            "max_ack_pending": max_ack_pending,
        }
        connection = await self._ensure_connection()
        js = connection.jetstream()
        try:
            info = await js.consumer_info(stream_name, name)
        except NotFoundError:
            config = ConsumerConfig(
                durable_name=name,
                filter_subject=filter_subject,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=30,
                max_deliver=3,
                max_ack_pending=max_ack_pending,
            )
            await js.add_consumer(stream_name, config)
            info = await js.consumer_info(stream_name, name)
        normalized = self._normalize_consumer(info)
        if normalized != expected:
            raise RuntimeError("live consumer does not match configuration")
        self._created_consumers[name] = normalized
        if filter_subject is None:
            return None
        return ConsumerBinding(
            stream_name=stream_name,
            filter_subject=filter_subject,
            durable_name=name,
            ack_wait_seconds=30,
            max_deliver=3,
            max_ack_pending=max_ack_pending,
        )

    @staticmethod
    def _normalize_stream(info: object) -> dict[str, object]:
        config = cast(Any, info).config
        return {
            "name": config.name,
            "subjects": list(config.subjects),
            "retention": _enum_value(config.retention),
            "storage": _enum_value(config.storage),
            "max_age_ns": int(config.max_age * 1_000_000_000),
            "max_bytes": config.max_bytes,
            "max_msg_size": config.max_msg_size,
            "discard": _enum_value(config.discard),
            "duplicate_window_ns": int(config.duplicate_window * 1_000_000_000),
        }

    @staticmethod
    def _normalize_consumer(info: object) -> dict[str, object]:
        value = cast(Any, info)
        config = value.config
        return {
            "stream_name": value.stream_name,
            "filter_subject": config.filter_subject,
            "durable_name": config.durable_name,
            "ack_policy": _enum_value(config.ack_policy),
            "ack_wait_ns": int(config.ack_wait * 1_000_000_000),
            "max_deliver": config.max_deliver,
            "max_ack_pending": config.max_ack_pending,
        }

    async def _publish_js(
        self, subject: str, envelope: Mapping[str, object], operation: str
    ) -> PublicationReceipt:
        await self._ensure_streams()
        data = canonical_json(envelope)
        jetstream = (await self._ensure_connection()).jetstream()
        if self._nats_msg_id:
            acknowledgement = await jetstream.publish(
                subject,
                data,
                headers={"Nats-Msg-Id": cast(str, envelope["id"])},
            )
        else:
            acknowledgement = await jetstream.publish(subject, data)
        stream = getattr(acknowledgement, "stream", None)
        sequence = getattr(acknowledgement, "seq", None)
        if stream != "AGENT_INBOX" or type(sequence) is not int or sequence < 1:
            raise RuntimeError("invalid JetStream publication acknowledgement")
        duplicate = (
            bool(getattr(acknowledgement, "duplicate", False))
            if self._nats_msg_id
            else None
        )
        receipt = PublicationReceipt(
            envelope_id=cast(str, envelope["id"]),
            accepted=True,
            transport=self._mode.value,
            stream=stream,
            stream_sequence=sequence,
            duplicate=duplicate,
            accepted_ns=self._evidence_clock_ns(),
            application_bytes=len(data),
            wire_bytes=None,
        )
        self._emit_publication(operation, envelope, receipt)
        return receipt

    async def _publish_transient(
        self, subject: str, envelope: Mapping[str, object], operation: str
    ) -> PublicationReceipt:
        if self._durable_transients:
            await self._ensure_streams()
            data = canonical_json(envelope)
            acknowledgement = await (await self._ensure_connection()).jetstream().publish(
                subject, data, headers={"Nats-Msg-Id": cast(str, envelope["id"])}
            )
            stream = getattr(acknowledgement, "stream", None)
            sequence = getattr(acknowledgement, "seq", None)
            if stream != "TRANSIENT_EVENTS" or type(sequence) is not int or sequence < 1:
                raise RuntimeError("invalid JetStream publication acknowledgement")
            receipt = PublicationReceipt(
                envelope_id=cast(str, envelope["id"]), accepted=True,
                transport=self._mode.value, stream=stream, stream_sequence=sequence,
                duplicate=bool(getattr(acknowledgement, "duplicate", False)),
                accepted_ns=self._evidence_clock_ns(), application_bytes=len(data),
                wire_bytes=None,
            )
            self._emit_publication(operation, envelope, receipt)
            return receipt
        await self._ensure_streams()
        return await self._publish_core(subject, envelope, operation)

    async def _publish_core(
        self, subject: str, envelope: Mapping[str, object], operation: str
    ) -> PublicationReceipt:
        data = canonical_json(envelope)
        connection = await self._ensure_connection()
        await connection.publish(subject, data)
        await connection.flush()
        receipt = PublicationReceipt(
            envelope_id=cast(str, envelope["id"]), accepted=True,
            transport=self._mode.value, stream=None, stream_sequence=None,
            duplicate=None, accepted_ns=self._evidence_clock_ns(),
            application_bytes=len(data), wire_bytes=None,
        )
        self._emit_publication(operation, envelope, receipt)
        return receipt

    async def _start_core_progress_observer(self) -> None:
        await self._ensure_streams()
        connection = await self._ensure_connection()
        subscriptions = []
        for subject, callback in (
            ("agents.*.task_progress.>", self._on_core_transient),
            ("agents.*.heartbeat", self._on_core_transient),
            ("agents.*.status", self._on_core_transient),
            ("agents.*.register", self._on_registration),
        ):
            subscriptions.append(await connection.subscribe(subject, cb=callback))
        await connection.flush()
        self._subscriptions.extend(subscriptions)

    async def _start_durable_progress_observer(self) -> None:
        agent_id = self._require_observer_agent()
        await self._ensure_streams()
        await self._ensure_consumer("transient", agent_id)
        connection = await self._ensure_connection()
        info = await connection.jetstream().stream_info("TRANSIENT_EVENTS")
        self._transient_cutoff = cast(int, getattr(info.state, "last_seq", 0))
        subscription = await connection.jetstream().pull_subscribe(
            subject="", durable=durable_name("transient", self._run_id, agent_id),
            stream="TRANSIENT_EVENTS",
        )
        self._transient_task = asyncio.create_task(
            self._pull_loop(subscription, self._on_durable_transient)
        )
        self._transient_subscription = subscription
        register = await connection.subscribe("agents.*.register", cb=self._on_registration)
        self._subscriptions.append(register)
        await connection.flush()

    async def _pull_loop(
        self, subscription: object, handler: Callable[[object], Awaitable[None]]
    ) -> None:
        try:
            while not self._closed:
                try:
                    messages = await cast(Any, subscription).fetch(batch=1, timeout=30)
                except asyncio.TimeoutError:
                    continue
                for message in messages:
                    await handler(message)
        except asyncio.CancelledError:
            raise
        except BaseException:  # noqa: BLE001 - observer errors are fatal.
            if self._background_failure is None:
                self._background_failure = RuntimeError("JetStream observer failed")

    async def _on_terminal(self, message: object) -> None:
        envelope = self._decode_message(message)
        task_id = envelope.get("task_id")
        if envelope.get("type") != "result" or type(task_id) is not str:
            raise RuntimeError("invalid JetStream message")
        metadata = cast(Any, message).metadata
        sequence = cast(int, metadata.sequence.stream)
        delivery_count = cast(int, metadata.num_delivered)
        self._terminal_observation_index += 1
        observation = ObservedEnvelope(
            envelope=MappingProxyType(dict(envelope)), observed_ns=self._evidence_clock_ns(),
            observation_index=self._terminal_observation_index, stream_sequence=sequence,
            delivery_count=delivery_count, replayed=False, delivery=_ObserverDelivery(message),
        )
        self._terminal_queues.setdefault(task_id, deque()).append(observation)
        self._terminal_events.setdefault(task_id, asyncio.Event()).set()
        self._emit("transport.terminal_observed", {
            "task_id": task_id, "envelope_id": envelope["id"],
            "observation_index": observation.observation_index, "stream_sequence": sequence,
            "delivery_count": delivery_count, "replayed": False,
        })

    async def _on_durable_transient(self, message: object) -> None:
        envelope = self._decode_message(message)
        if envelope.get("type") not in ("task.progress", "heartbeat", "status"):
            raise RuntimeError("invalid JetStream message")
        metadata = cast(Any, message).metadata
        sequence = cast(int, metadata.sequence.stream)
        delivery_count = cast(int, metadata.num_delivered)
        self._transient_observation_index += 1
        replayed = sequence <= self._transient_cutoff
        self._emit("transport.transient_observed", {
            "envelope_type": envelope["type"], "envelope_id": envelope["id"],
            "task_id": envelope.get("task_id"), "sender_id": envelope["sender_id"],
            "observation_index": self._transient_observation_index, "stream_sequence": sequence,
            "delivery_count": delivery_count, "replayed": replayed,
        })
        await _ObserverDelivery(message).ack()

    async def _on_core_transient(self, message: object) -> None:
        envelope = self._decode_message(message)
        if envelope.get("type") not in ("task.progress", "heartbeat", "status"):
            raise RuntimeError("invalid core message")
        self._transient_observation_index += 1
        self._emit("transport.transient_observed", {
            "envelope_type": envelope["type"], "envelope_id": envelope["id"],
            "task_id": envelope.get("task_id"), "sender_id": envelope["sender_id"],
            "observation_index": self._transient_observation_index, "stream_sequence": None,
            "delivery_count": 1, "replayed": False,
        })

    async def _on_registration(self, message: object) -> None:
        envelope = self._decode_message(message)
        if envelope.get("type") != "register":
            raise RuntimeError("invalid core message")
        try:
            default_validator().validate_register(envelope)
        except (ValidationError, RecursionError):
            raise RuntimeError("invalid core message") from None
        self._registration_observation_index += 1
        self._emit("transport.registration_observed", {
            "envelope_id": envelope["id"], "agent_id": envelope["sender_id"],
            "observation_index": self._registration_observation_index,
            "stream_sequence": None, "delivery_count": 1, "replayed": False,
        })

    async def _disconnect_progress_observer(self) -> None:
        async with self._operation_lock:
            self._check_open()
            if not self._progress_desired:
                raise RuntimeError("progress observer is not started")
            self._progress_desired = False
            await self._unsubscribe_transient_pull()
            await self._cancel_task("_transient_task")
            for subscription in self._subscriptions:
                await cast(Any, subscription).unsubscribe()
            self._subscriptions.clear()
            self._emit("transport.fault_applied", {"action": "disconnect_progress_observer"})

    async def _reconnect_progress_observer(self) -> None:
        async with self._operation_lock:
            self._check_open()
            if self._progress_desired:
                raise RuntimeError("progress observer is not disconnected")
            self._progress_desired = True
            if self._durable_transients:
                await self._start_durable_progress_observer()
            else:
                await self._start_core_progress_observer()
            self._emit("transport.fault_applied", {"action": "reconnect_progress_observer"})

    async def _stop_worker(self, agent_id: str, callback: WorkerOperation | None) -> None:
        async with self._operation_lock:
            self._check_open()
            self._validate_agent_id(agent_id)
            if callback is not None:
                await callback(agent_id)
            elif self._receiver_agent_id == agent_id and self._receiver_desired:
                self._receiver_desired = False
                await self._stop_receiver_task()
            else:
                raise RuntimeError("worker control is unavailable")
            self._emit("transport.fault_applied", {"action": "stop_worker"})

    async def _start_worker(self, agent_id: str, callback: WorkerOperation | None) -> None:
        should_start_local = False
        executor: TaskExecutor | None = None
        async with self._operation_lock:
            self._check_open()
            self._validate_agent_id(agent_id)
            if callback is not None:
                await callback(agent_id)
            elif self._receiver_agent_id == agent_id and not self._receiver_desired:
                executor = self._receiver_executor
                if executor is None:
                    raise RuntimeError("worker control is unavailable")
                should_start_local = True
            else:
                raise RuntimeError("worker control is unavailable")
        if should_start_local:
            await self.start_receiver(agent_id, cast(TaskExecutor, executor))
        self._emit("transport.fault_applied", {"action": "start_worker"})

    async def _restart_coordinator(self, callback: CoordinatorRestart | None) -> None:
        if callback is None:
            raise RuntimeError("coordinator control is unavailable")
        terminal = False
        progress = False
        receiver = False
        agent_id: str | None = None
        executor: TaskExecutor | None = None
        async with self._operation_lock:
            self._check_open()
            terminal = self._terminal_desired
            progress = self._progress_desired
            receiver = self._receiver_desired
            agent_id = self._receiver_agent_id
            executor = self._receiver_executor
            await self._stop_receiver_task()
            await self._cancel_task("_terminal_task")
            await self._unsubscribe_transient_pull()
            await self._cancel_task("_transient_task")
            for subscription in self._subscriptions:
                await cast(Any, subscription).unsubscribe()
            self._subscriptions.clear()
            if self._nc is not None:
                current_in, current_out = self._connection_stats()
                await self._nc.close()
                self._accumulated_in_bytes += current_in
                self._accumulated_out_bytes += current_out
                self._nc = None
            replacement = await callback()
            if replacement is not None:
                if not _valid_nats_url(replacement):
                    raise ValueError("invalid nats_url")
                self._nats_url = replacement
            self._terminal_desired = False
            self._progress_desired = False
            self._receiver_desired = False
            await self._ensure_streams()
        if terminal:
            await self.start_terminal_observer()
        if progress:
            await self.start_progress_observer()
        if receiver and agent_id is not None and executor is not None:
            await self.start_receiver(agent_id, executor)
        self._emit("transport.fault_applied", {"action": "restart_coordinator"})

    async def _stop_receiver_task(self) -> None:
        if self._receiver_pull is not None:
            await self._receiver_pull.stop()
        await self._cancel_task("_receiver_task")
        self._receiver_pull = None

    async def _unsubscribe_transient_pull(self) -> None:
        subscription = self._transient_subscription
        self._transient_subscription = None
        if subscription is not None:
            await cast(Any, subscription).unsubscribe()

    async def _cancel_task(self, attribute: str) -> None:
        task = cast(asyncio.Task[None] | None, getattr(self, attribute))
        setattr(self, attribute, None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _decode_message(message: object) -> dict[str, object]:
        try:
            raw = cast(Any, message).data
            decoded = cast(object, json.loads(raw))
            if not isinstance(decoded, dict) or canonical_json(decoded) != raw:
                raise ValueError
            default_validator().validate_envelope(decoded)
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise RuntimeError("invalid transport message") from None
        return cast(dict[str, object], decoded)

    @staticmethod
    def _validate_agent_id(agent_id: object) -> None:
        if type(agent_id) is not str or _ID_PATTERN.fullmatch(agent_id) is None:
            raise ValueError("invalid agent_id")

    def _validate_receiver_card(self, agent_id: str) -> None:
        if self._agent_card is None:
            raise ValueError("agent_card is required")
        try:
            default_validator().validate_card(self._agent_card)
        except (ValidationError, RecursionError):
            raise ValueError("invalid agent_card") from None
        if self._agent_card.get("name") != agent_id:
            raise ValueError("agent_card name mismatch")

    def _require_observer_agent(self) -> str:
        if self._observer_agent_id is None:
            raise ValueError("observer_agent_id is required")
        return self._observer_agent_id

    @staticmethod
    def _validated_envelope(
        envelope: Mapping[str, object], allowed_types: tuple[str, ...]
    ) -> dict[str, object]:
        try:
            decoded = cast(object, json.loads(canonical_json(envelope)))
            if not isinstance(decoded, dict):
                raise TypeError
            default_validator().validate_envelope(decoded)
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise ValueError("invalid envelope") from None
        if decoded.get("type") not in allowed_types:
            raise ValueError("invalid envelope type")
        return cast(dict[str, object], decoded)

    def _check_open(self) -> None:
        if self._background_failure is not None:
            raise self._background_failure
        if self._closed:
            raise RuntimeError("transport is closed")

    def _connection_stats(self) -> tuple[int, int]:
        if self._nc is None:
            return (0, 0)
        stats = self._nc.stats
        incoming = stats.get("in_bytes", 0)
        outgoing = stats.get("out_bytes", 0)
        return (
            incoming if type(incoming) is int and incoming >= 0 else 0,
            outgoing if type(outgoing) is int and outgoing >= 0 else 0,
        )

    def _emit_publication(
        self, operation: str, envelope: Mapping[str, object], receipt: PublicationReceipt
    ) -> None:
        self._emit("transport.publication_accepted", {
            "operation": operation, "envelope_type": envelope["type"],
            "envelope_id": envelope["id"], "task_id": envelope.get("task_id"),
            "receipt": {
                "envelope_id": receipt.envelope_id, "accepted": receipt.accepted,
                "transport": receipt.transport, "stream": receipt.stream,
                "stream_sequence": receipt.stream_sequence, "duplicate": receipt.duplicate,
                "accepted_ns": receipt.accepted_ns, "application_bytes": receipt.application_bytes,
                "wire_bytes": receipt.wire_bytes,
            },
        })

    def _emit(self, event: str, data: Mapping[str, object]) -> None:
        self._event_sink.emit({
            "monotonic_ns": self._evidence_clock_ns(), "epoch_time": self._epoch_now(),
            "component": self._mode.value.replace("-", "_"), "event": event,
            "data": dict(data),
        })


class EdgeCitadelTransport(_JetStreamTransport):
    def __init__(
        self,
        *,
        nats_url: str,
        run_id: str,
        token: str,
        event_sink: EventSink,
        agent_card: Mapping[str, object] | None = None,
        observer_agent_id: str | None = None,
        ablation: str = "full-contract",
        coordinator_restart: CoordinatorRestart | None = None,
        worker_stop: WorkerOperation | None = None,
        worker_start: WorkerOperation | None = None,
        connection_factory: Callable[..., Awaitable[NATS]] = nats.connect,
        evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
        epoch_now: Callable[[], str] = _now_iso,
        uuid4: Callable[[], str] = _uuid4,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        if ablation not in ABLATIONS:
            raise ValueError("invalid ablation")
        settings = ABLATIONS[ablation]
        super().__init__(
            nats_url=nats_url, run_id=run_id, token=token, event_sink=event_sink,
            agent_card=agent_card, observer_agent_id=observer_agent_id,
            mode=Mode.EDGECITADEL, ablation=ablation,
            nats_msg_id=settings["nats_msg_id"],
            outcome_ledger=settings["outcome_ledger"],
            durable_transients=False, coordinator_restart=coordinator_restart,
            worker_stop=worker_stop, worker_start=worker_start,
            connection_factory=connection_factory, evidence_clock_ns=evidence_clock_ns,
            epoch_now=epoch_now, uuid4=uuid4, sleep=sleep,
        )


__all__ = ["ABLATIONS", "EdgeCitadelTransport"]
