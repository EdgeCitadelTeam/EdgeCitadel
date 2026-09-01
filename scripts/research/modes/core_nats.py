"""Ephemeral run-scoped Core NATS benchmark transport."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import sys
import time
import urllib.parse
import uuid
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import wraps
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, cast

from nats.aio.client import Client as NATS
from nats.errors import AuthorizationError
from nats.errors import Error as NATSError

import nats
from edgecitadel_plugin_runtime.task_types import PublicationReceipt
from edgecitadel_plugin_runtime.validator import (
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

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

    from edgecitadel_plugin_runtime.task_executor import TaskExecutor

CoordinatorRestart = Callable[[], Awaitable[str | None]]
WorkerOperation = Callable[[str], Awaitable[None]]
AsyncSleep = Callable[[float], Awaitable[None]]
_T = TypeVar("_T")
_P = ParamSpec("_P")
_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_UUID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
_RESTART_CLOSE_GRACE_S = 0.1
_PENDING_CLOSE_GRACE_S = 0.1


def _serialize_ordinary(
    method: Callable[
        Concatenate[CoreNatsTransport, _P],
        Coroutine[Any, Any, _T],
    ],
) -> Callable[
    Concatenate[CoreNatsTransport, _P],
    Coroutine[Any, Any, _T],
]:
    @wraps(method)
    async def wrapped(
        self: CoreNatsTransport,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        async with self._ordinary_operation():
            return await method(self, *args, **kwargs)

    return cast(
        "Callable[Concatenate[CoreNatsTransport, _P], Coroutine[Any, Any, _T]]",
        wrapped,
    )


def _serialize_exclusive(
    method: Callable[
        Concatenate[CoreNatsTransport, _P],
        Coroutine[Any, Any, _T],
    ],
) -> Callable[
    Concatenate[CoreNatsTransport, _P],
    Coroutine[Any, Any, _T],
]:
    @wraps(method)
    async def wrapped(
        self: CoreNatsTransport,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> _T:
        async with self._exclusive_operation():
            return await method(self, *args, **kwargs)

    return cast(
        "Callable[Concatenate[CoreNatsTransport, _P], Coroutine[Any, Any, _T]]",
        wrapped,
    )


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _uuid4() -> str:
    return str(uuid.uuid4())


def _valid_nats_url(value: object) -> bool:
    if (
        type(value) is not str
        or not value
        or any(character.isspace() for character in value)
        or "?" in value
        or "#" in value
    ):
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


def _valid_timeout(value: object, *, allow_zero: bool) -> bool:
    if type(value) is int:
        integer = value
        return integer <= sys.float_info.max and (
            integer >= 0 if allow_zero else integer > 0
        )
    if type(value) is not float:
        return False
    number = value
    return math.isfinite(number) and (number >= 0 if allow_zero else number > 0)


def _is_authentication_failure(error: Exception) -> bool:
    return isinstance(error, AuthorizationError) or (
        type(error) is NATSError and "authorization violation" in str(error).casefold()
    )


def _sanitized_failure(error: BaseException, message: str) -> BaseException:
    if type(error) is UnicodeDecodeError:
        sanitized: BaseException = UnicodeDecodeError(
            "utf-8",
            b"?",
            0,
            1,
            message,
        )
    elif type(error) is UnicodeEncodeError:
        sanitized = UnicodeEncodeError(
            "utf-8",
            "?",
            0,
            1,
            message,
        )
    elif type(error) is UnicodeTranslateError:
        sanitized = UnicodeTranslateError("?", 0, 1, message)
    elif type(getattr(error, "exceptions", None)) is tuple:
        child: BaseException = (
            Exception(message)
            if isinstance(error, Exception)
            else BaseException(message)
        )
        try:
            sanitized = type(error)(message, [child])
        except BaseException:  # noqa: BLE001 - use a source-free instance.
            sanitized = RuntimeError(message)
    else:
        try:
            sanitized = type(error)(message)
        except BaseException:  # noqa: BLE001 - use a source-free instance.
            try:
                sanitized = BaseException.__new__(type(error))
                BaseException.__init__(sanitized, message)
            except BaseException:  # noqa: BLE001 - never retain structured data.
                sanitized = RuntimeError(message)
    sanitized.__cause__ = None
    sanitized.__context__ = None
    sanitized.__traceback__ = None
    return sanitized


def _receipt_mapping(receipt: PublicationReceipt) -> dict[str, object]:
    return {
        "envelope_id": receipt.envelope_id,
        "accepted": receipt.accepted,
        "transport": receipt.transport,
        "stream": receipt.stream,
        "stream_sequence": receipt.stream_sequence,
        "duplicate": receipt.duplicate,
        "accepted_ns": receipt.accepted_ns,
        "application_bytes": receipt.application_bytes,
        "wire_bytes": receipt.wire_bytes,
    }


class _CoreDelivery:
    def __init__(self, worker_agent_id: str, raw: bytes) -> None:
        self.worker_agent_id = worker_agent_id
        self.raw = raw
        self.delivery_count = 1
        self.stream_sequence: int | None = None

    async def in_progress(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def retry(self) -> None:
        return None

    async def terminate(self) -> None:
        return None


class _CoreFaultController:
    def __init__(
        self,
        *,
        transport: CoreNatsTransport,
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


class CoreNatsTransport:
    def __init__(
        self,
        *,
        nats_url: str,
        run_id: str,
        token: str,
        event_sink: EventSink,
        agent_card: Mapping[str, object] | None = None,
        coordinator_restart: CoordinatorRestart | None = None,
        worker_stop: WorkerOperation | None = None,
        worker_start: WorkerOperation | None = None,
        connection_factory: Callable[..., Awaitable[NATS]] = nats.connect,
        evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
        epoch_now: Callable[[], str] = _now_iso,
        uuid4: Callable[[], str] = _uuid4,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        if not _valid_nats_url(nats_url):
            raise ValueError("invalid nats_url")
        if type(run_id) is not str or _ID_PATTERN.fullmatch(run_id) is None:
            raise ValueError("invalid run_id")
        if type(token) is not str or _TOKEN_PATTERN.fullmatch(token) is None:
            raise ValueError("invalid token")
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
        self._connection_factory = connection_factory
        self._evidence_clock_ns = evidence_clock_ns
        self._epoch_now = epoch_now
        self._uuid4 = uuid4
        self._sleep = sleep
        self._nc: NATS | None = None
        self._pending_candidates: list[NATS] = []
        self._candidate_close_tasks: dict[int, asyncio.Task[Any]] = {}
        self._connect_lock = asyncio.Lock()
        self._operation_condition = asyncio.Condition()
        self._exclusive_lock = asyncio.Lock()
        self._ordinary_operations = 0
        self._exclusive_active = False
        self._terminal_subscription: Subscription | None = None
        self._progress_subscriptions: list[Subscription] = []
        self._receiver_subscription: Subscription | None = None
        self._subscription_monitors: set[asyncio.Task[None]] = set()
        self._terminal_desired = False
        self._progress_desired = False
        self._receiver_desired = False
        self._external_worker_desired: dict[str, bool] = {}
        self._receiver_agent_id: str | None = None
        self._receiver_executor: TaskExecutor | None = None
        self._receiver_ready: asyncio.Event | None = None
        self._terminal_queues: dict[str, deque[ObservedEnvelope]] = {}
        self._terminal_events: dict[str, asyncio.Event] = {}
        self._terminal_observation_index = 0
        self._transient_observation_index = 0
        self._registration_observation_index = 0
        self._accumulated_in_bytes = 0
        self._accumulated_out_bytes = 0
        self._background_failure: BaseException | None = None
        self._intentional_close = False
        self._closed = False
        self._faults = _CoreFaultController(
            transport=self,
            coordinator_restart=coordinator_restart,
            worker_stop=worker_stop,
            worker_start=worker_start,
        )

    @property
    def mode(self) -> Mode:
        return Mode.CORE_ONLY

    @property
    def outcome_ledger_enabled(self) -> bool:
        return True

    @property
    def faults(self) -> _CoreFaultController:
        return self._faults

    @property
    def resolved_config(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "mode": "core-only",
                "ablation": "full-contract",
                "nats_msg_id": False,
                "outcome_ledger": True,
            }
        )

    @_serialize_ordinary
    async def start_terminal_observer(self) -> None:
        self._check_open()
        if self._terminal_desired:
            raise RuntimeError("terminal observer already started")
        self._terminal_desired = True
        try:
            await self._subscribe_terminal()
        except BaseException:
            self._terminal_desired = False
            raise

    @_serialize_ordinary
    async def start_progress_observer(self) -> None:
        self._check_open()
        if self._progress_desired:
            raise RuntimeError("progress observer already started")
        self._progress_desired = True
        try:
            await self._subscribe_progress()
        except BaseException:
            self._progress_desired = False
            raise

    @_serialize_ordinary
    async def start_receiver(
        self,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if self._receiver_desired or self._receiver_subscription is not None:
            raise RuntimeError("receiver already started")
        self._validate_receiver_card(agent_id)
        self._receiver_agent_id = agent_id
        self._receiver_executor = executor
        self._receiver_desired = True
        try:
            await self._subscribe_receiver()
        except BaseException:
            self._receiver_desired = False
            raise

    @_serialize_ordinary
    async def wait_receiver_ready(self, agent_id: str, timeout_s: float) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if not _valid_timeout(timeout_s, allow_zero=False):
            raise ValueError("invalid timeout_s")
        if (
            self._receiver_agent_id != agent_id
            or self._receiver_ready is None
            or not self._receiver_desired
        ):
            raise RuntimeError("receiver agent mismatch")
        try:
            await asyncio.wait_for(
                self._receiver_ready.wait(),
                timeout=timeout_s,
            )
        except TimeoutError:
            raise TimeoutError from None
        self._check_open()

    @_serialize_ordinary
    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        encoded, decoded = self._validated_envelope(
            envelope,
            ("command", "delegation", "cancel"),
        )
        subject = f"artifact.{self._run_id}.agents.{decoded['recipient_id']}.inbox"
        return await self._publish(
            subject,
            encoded,
            decoded,
            operation="submit_task",
        )

    @_serialize_ordinary
    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        encoded, decoded = self._validated_envelope(
            envelope,
            ("task.progress",),
        )
        subject = (
            f"artifact.{self._run_id}.agents.{decoded['sender_id']}."
            f"task_progress.{decoded['task_id']}"
        )
        return await self._publish(
            subject,
            encoded,
            decoded,
            operation="publish_progress",
        )

    @_serialize_ordinary
    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        encoded, decoded = self._validated_envelope(envelope, ("result",))
        subject = (
            f"artifact.{self._run_id}.agents.{decoded['recipient_id']}."
            f"result.{decoded['task_id']}"
        )
        return await self._publish(
            subject,
            encoded,
            decoded,
            operation="publish_terminal",
        )

    @_serialize_ordinary
    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        encoded, decoded = self._validated_envelope(envelope, ("heartbeat",))
        subject = f"artifact.{self._run_id}.agents.{decoded['sender_id']}.heartbeat"
        return await self._publish(
            subject,
            encoded,
            decoded,
            operation="publish_heartbeat",
        )

    @_serialize_ordinary
    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        self._check_open()
        if not self._terminal_desired:
            raise RuntimeError("terminal observer is not started")
        if type(task_id) is not str or _UUID_PATTERN.fullmatch(task_id) is None:
            raise ValueError("invalid task_id")
        if not _valid_timeout(timeout_s, allow_zero=True):
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
        self._check_open()
        return queue.popleft() if queue else None

    @_serialize_ordinary
    async def inspect_state(self) -> TransportSnapshot:
        self._check_open()
        current_in, current_out = self._connection_stats()
        return TransportSnapshot(
            mode=Mode.CORE_ONLY,
            streams=MappingProxyType({}),
            consumers=MappingProxyType({}),
            pending=None,
            ack_pending=None,
            connection_bytes=MappingProxyType(
                {
                    "in_bytes": self._accumulated_in_bytes + current_in,
                    "out_bytes": self._accumulated_out_bytes + current_out,
                }
            ),
            storage_bytes=0,
            message_count=0,
        )

    @_serialize_exclusive
    async def close(self) -> None:
        if self._closed:
            return
        self._terminal_desired = False
        self._progress_desired = False
        self._receiver_desired = False
        await self._close_connection()
        await self._close_pending_candidates()
        self._closed = True
        if self._background_failure is not None:
            raise self._background_failure

    @asynccontextmanager
    async def _ordinary_operation(self) -> AsyncIterator[None]:
        async with self._operation_condition:
            while self._exclusive_active:
                await self._operation_condition.wait()
            self._ordinary_operations += 1
        try:
            yield
        finally:
            async with self._operation_condition:
                self._ordinary_operations -= 1
                if self._ordinary_operations == 0:
                    self._operation_condition.notify_all()

    @asynccontextmanager
    async def _exclusive_operation(self) -> AsyncIterator[None]:
        async with self._exclusive_lock:
            async with self._operation_condition:
                self._exclusive_active = True
            try:
                async with self._operation_condition:
                    while self._ordinary_operations:
                        await self._operation_condition.wait()
                yield
            finally:
                async with self._operation_condition:
                    self._exclusive_active = False
                    self._operation_condition.notify_all()

    def _check_open(self) -> None:
        if self._background_failure is not None:
            raise self._background_failure
        if self._closed:
            raise RuntimeError("transport is closed")

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

    def _emit(self, event: str, data: Mapping[str, object]) -> None:
        self._event_sink.emit(
            {
                "monotonic_ns": self._evidence_clock_ns(),
                "epoch_time": self._epoch_now(),
                "component": "core_nats",
                "event": event,
                "data": dict(data),
            }
        )

    async def _error_callback(self, error: Exception) -> None:
        if self._intentional_close:
            return
        if _is_authentication_failure(error):
            failure: BaseException = PermissionError("transport authentication failed")
        else:
            failure = _sanitized_failure(error, "core nats connection failed")
        if self._background_failure is None:
            self._background_failure = failure

    async def _disconnected_callback(self) -> None:
        if not self._intentional_close and self._background_failure is None:
            self._background_failure = RuntimeError("core nats disconnected")

    async def _closed_callback(self) -> None:
        if not self._intentional_close and self._background_failure is None:
            self._background_failure = RuntimeError("core nats closed")

    async def _ensure_connection(
        self,
        *,
        cleanup_deadline: float | None = None,
    ) -> NATS:
        self._check_open()
        if self._nc is not None:
            return self._nc
        async with self._connect_lock:
            if self._nc is not None:
                return self._nc
            attempt_authentication_failed = False
            established = False

            async def connection_error(error: Exception) -> None:
                nonlocal attempt_authentication_failed
                if established:
                    await self._error_callback(error)
                elif _is_authentication_failure(error):
                    attempt_authentication_failed = True

            async def connection_disconnected() -> None:
                if established:
                    await self._disconnected_callback()

            async def connection_closed() -> None:
                if established:
                    await self._closed_callback()

            authentication_failed = False
            candidate: NATS | None = None
            connection_failure: BaseException | None = None
            connection_interruption: BaseException | None = None
            try:
                candidate = await self._connection_factory(
                    servers=[self._nats_url],
                    token=self._token,
                    allow_reconnect=False,
                    max_reconnect_attempts=0,
                    connect_timeout=2,
                    error_cb=connection_error,
                    disconnected_cb=connection_disconnected,
                    closed_cb=connection_closed,
                )
                if all(
                    pending is not candidate for pending in self._pending_candidates
                ):
                    self._pending_candidates.append(candidate)
                await candidate.flush()
            except BaseException as error:  # noqa: BLE001 - preserve cancellation.
                authentication_failed = (
                    isinstance(error, Exception) and _is_authentication_failure(error)
                ) or attempt_authentication_failed
                if not isinstance(error, Exception):
                    connection_interruption = (
                        asyncio.CancelledError()
                        if isinstance(error, asyncio.CancelledError)
                        else _sanitized_failure(
                            error,
                            "core nats connection failed",
                        )
                    )
                elif not authentication_failed:
                    connection_failure = _sanitized_failure(
                        error,
                        "core nats connection failed",
                    )
            else:
                authentication_failed = attempt_authentication_failed
            if (
                authentication_failed
                or connection_interruption is not None
                or connection_failure is not None
            ) and candidate is not None:
                await self._release_candidate(
                    candidate,
                    deadline=cleanup_deadline,
                )
                candidate = None
            if connection_interruption is not None:
                raise connection_interruption from None
            if authentication_failed:
                raise PermissionError("transport authentication failed") from None
            if connection_failure is not None:
                raise connection_failure from None
            if candidate is None:
                raise RuntimeError("core nats connection failed")
            established = True
            self._pending_candidates = [
                pending
                for pending in self._pending_candidates
                if pending is not candidate
            ]
            self._nc = candidate
            return candidate

    def _validated_envelope(
        self,
        envelope: Mapping[str, object],
        allowed_types: tuple[str, ...],
    ) -> tuple[bytes, dict[str, object]]:
        self._check_open()
        try:
            encoded = canonical_json(envelope)
            decoded = cast(object, json.loads(encoded))
            if not isinstance(decoded, dict):
                raise TypeError
            default_validator().validate_envelope(decoded)
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise ValueError("invalid envelope") from None
        if decoded.get("type") not in allowed_types:
            raise ValueError("invalid envelope")
        return encoded, cast(dict[str, object], decoded)

    async def _publish(
        self,
        subject: str,
        encoded: bytes,
        decoded: Mapping[str, object],
        *,
        operation: str,
    ) -> PublicationReceipt:
        connection = await self._ensure_connection()
        await self._nats_operation(connection.publish(subject, encoded))
        await self._nats_operation(connection.flush())
        receipt = PublicationReceipt(
            envelope_id=cast(str, decoded["id"]),
            accepted=True,
            transport="core-only",
            stream=None,
            stream_sequence=None,
            duplicate=None,
            accepted_ns=self._evidence_clock_ns(),
            application_bytes=len(encoded),
            wire_bytes=None,
        )
        self._emit(
            "transport.publication_accepted",
            {
                "operation": operation,
                "envelope_type": decoded["type"],
                "envelope_id": decoded["id"],
                "task_id": decoded.get("task_id"),
                "receipt": _receipt_mapping(receipt),
            },
        )
        return receipt

    async def _subscribe_terminal(self) -> None:
        connection = await self._ensure_connection()
        subscription: Subscription | None = None
        try:
            subscription = await self._nats_operation(
                connection.subscribe(
                    f"artifact.{self._run_id}.agents.*.result.*",
                    cb=self._on_terminal,
                )
            )
            self._monitor_subscription(subscription)
            await self._nats_operation(connection.flush())
            self._emit(
                "transport.observer_ready",
                {"kind": "terminal", "agent_id": None},
            )
        except BaseException:
            if subscription is not None:
                await self._rollback_subscriptions(connection, [subscription])
            raise
        self._terminal_subscription = subscription

    async def _subscribe_progress(self) -> None:
        connection = await self._ensure_connection()
        definitions = (
            (
                f"artifact.{self._run_id}.agents.*.task_progress.*",
                self._on_transient,
            ),
            (
                f"artifact.{self._run_id}.agents.*.heartbeat",
                self._on_transient,
            ),
            (
                f"artifact.{self._run_id}.agents.*.status",
                self._on_transient,
            ),
            (
                f"artifact.{self._run_id}.agents.*.register",
                self._on_registration,
            ),
        )
        subscriptions: list[Subscription] = []
        try:
            for subject, callback in definitions:
                subscription = await self._nats_operation(
                    connection.subscribe(subject, cb=callback)
                )
                subscriptions.append(subscription)
                self._monitor_subscription(subscription)
            await self._nats_operation(connection.flush())
            self._emit(
                "transport.observer_ready",
                {"kind": "progress", "agent_id": None},
            )
        except BaseException:
            await self._rollback_subscriptions(connection, subscriptions)
            raise
        self._progress_subscriptions = subscriptions

    async def _subscribe_receiver(self) -> None:
        if self._receiver_agent_id is None or self._receiver_executor is None:
            raise RuntimeError("worker control is unavailable")
        self._validate_receiver_card(self._receiver_agent_id)
        connection = await self._ensure_connection()
        agent_id = self._receiver_agent_id
        executor = self._receiver_executor
        self._receiver_ready = None

        async def receive(message: Msg) -> None:
            await self._on_worker(message, agent_id, executor)

        subscription: Subscription | None = None
        try:
            subscription = await self._nats_operation(
                connection.subscribe(
                    f"artifact.{self._run_id}.agents.{agent_id}.inbox",
                    cb=receive,
                )
            )
            self._monitor_subscription(subscription)
            await self._nats_operation(connection.flush())
            registration = {
                "v": 1,
                "id": self._uuid4(),
                "type": "register",
                "sender_id": agent_id,
                "timestamp": self._epoch_now(),
                "payload": self._agent_card,
            }
            encoded, decoded = self._validated_envelope(registration, ("register",))
            await self._publish(
                f"artifact.{self._run_id}.agents.{agent_id}.register",
                encoded,
                decoded,
                operation="register",
            )
            ready = asyncio.Event()
            ready.set()
            self._emit(
                "transport.receiver_ready",
                {"kind": "receiver", "agent_id": agent_id},
            )
        except BaseException:
            if subscription is not None:
                await self._rollback_subscriptions(connection, [subscription])
            raise
        self._receiver_subscription = subscription
        self._receiver_ready = ready

    @staticmethod
    def _decode_message(message: Msg) -> dict[str, object]:
        try:
            raw = message.data
            decoded = cast(object, json.loads(raw))
            if not isinstance(decoded, dict) or canonical_json(decoded) != raw:
                raise ValueError
            default_validator().validate_envelope(decoded)
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise RuntimeError("invalid core nats message") from None
        return cast(dict[str, object], decoded)

    async def _on_worker(
        self,
        message: Msg,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None:
        raw = message.data
        delivery = _CoreDelivery(agent_id, raw)
        self._emit(
            "transport.worker_delivery",
            {
                "worker_agent_id": agent_id,
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "delivery_count": 1,
                "stream_sequence": None,
            },
        )
        await executor.execute(delivery)

    async def _on_terminal(self, message: Msg) -> None:
        envelope = self._decode_message(message)
        task_id = envelope.get("task_id")
        if (
            envelope.get("type") != "result"
            or type(task_id) is not str
            or _UUID_PATTERN.fullmatch(task_id) is None
        ):
            raise RuntimeError("invalid core nats message")
        self._terminal_observation_index += 1
        observation = ObservedEnvelope(
            envelope=MappingProxyType(dict(envelope)),
            observed_ns=self._evidence_clock_ns(),
            observation_index=self._terminal_observation_index,
            stream_sequence=None,
            delivery_count=1,
            replayed=False,
            delivery=None,
        )
        self._terminal_queues.setdefault(task_id, deque()).append(observation)
        self._terminal_events.setdefault(task_id, asyncio.Event()).set()
        self._emit(
            "transport.terminal_observed",
            {
                "task_id": task_id,
                "envelope_id": envelope["id"],
                "observation_index": observation.observation_index,
                "stream_sequence": None,
                "delivery_count": 1,
                "replayed": False,
            },
        )

    async def _on_transient(self, message: Msg) -> None:
        envelope = self._decode_message(message)
        if envelope.get("type") not in ("task.progress", "heartbeat", "status"):
            raise RuntimeError("invalid core nats message")
        self._transient_observation_index += 1
        self._emit(
            "transport.transient_observed",
            {
                "envelope_type": envelope["type"],
                "envelope_id": envelope["id"],
                "task_id": envelope.get("task_id"),
                "sender_id": envelope["sender_id"],
                "observation_index": self._transient_observation_index,
                "stream_sequence": None,
                "delivery_count": 1,
                "replayed": False,
            },
        )

    async def _on_registration(self, message: Msg) -> None:
        envelope = self._decode_message(message)
        if envelope.get("type") != "register":
            raise RuntimeError("invalid core nats message")
        try:
            default_validator().validate_register(envelope)
        except (ValidationError, RecursionError):
            raise RuntimeError("invalid core nats message") from None
        self._registration_observation_index += 1
        self._emit(
            "transport.registration_observed",
            {
                "envelope_id": envelope["id"],
                "agent_id": envelope["sender_id"],
                "observation_index": self._registration_observation_index,
                "stream_sequence": None,
                "delivery_count": 1,
                "replayed": False,
            },
        )

    @_serialize_ordinary
    async def _disconnect_progress_observer(self) -> None:
        self._check_open()
        if not self._progress_desired or not self._progress_subscriptions:
            raise RuntimeError("progress observer is not started")
        connection = await self._ensure_connection()
        for subscription in self._progress_subscriptions:
            await self._nats_operation(subscription.unsubscribe())
        await self._nats_operation(connection.flush())
        self._progress_subscriptions = []
        self._progress_desired = False
        self._emit(
            "transport.fault_applied",
            {"action": "disconnect_progress_observer"},
        )

    @_serialize_ordinary
    async def _reconnect_progress_observer(self) -> None:
        self._check_open()
        if self._progress_desired or self._progress_subscriptions:
            raise RuntimeError("progress observer is not disconnected")
        self._progress_desired = True
        try:
            await self._subscribe_progress()
        except BaseException:
            self._progress_desired = False
            raise
        self._emit(
            "transport.fault_applied",
            {"action": "reconnect_progress_observer"},
        )

    @_serialize_ordinary
    async def _stop_worker(
        self,
        agent_id: str,
        callback: WorkerOperation | None,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if callback is not None:
            await callback(agent_id)
            self._external_worker_desired[agent_id] = False
        else:
            if (
                self._receiver_agent_id != agent_id
                or not self._receiver_desired
                or self._receiver_subscription is None
            ):
                raise RuntimeError("worker control is unavailable")
            connection = await self._ensure_connection()
            await self._nats_operation(self._receiver_subscription.unsubscribe())
            await self._nats_operation(connection.flush())
            self._receiver_subscription = None
            self._receiver_desired = False
        self._emit("transport.fault_applied", {"action": "stop_worker"})

    @_serialize_ordinary
    async def _start_worker(
        self,
        agent_id: str,
        callback: WorkerOperation | None,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if callback is not None:
            await callback(agent_id)
            self._external_worker_desired[agent_id] = True
        else:
            if (
                self._receiver_agent_id != agent_id
                or self._receiver_desired
                or self._receiver_executor is None
            ):
                raise RuntimeError("worker control is unavailable")
            self._receiver_desired = True
            try:
                await self._subscribe_receiver()
            except BaseException:
                self._receiver_desired = False
                raise
        self._emit("transport.fault_applied", {"action": "start_worker"})

    @_serialize_exclusive
    async def _restart_coordinator(
        self,
        callback: CoordinatorRestart | None,
    ) -> None:
        self._check_open()
        if callback is None:
            raise RuntimeError("coordinator control is unavailable")
        terminal_desired = self._terminal_desired
        progress_desired = self._progress_desired
        receiver_desired = self._receiver_desired
        await self._close_connection()
        self._terminal_subscription = None
        self._progress_subscriptions = []
        self._receiver_subscription = None
        replacement = await callback()
        if replacement is not None:
            if not _valid_nats_url(replacement):
                raise ValueError("invalid nats_url")
            self._nats_url = replacement
        deadline = time.monotonic() + 10
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("core nats restart timed out")
            cleanup_budget = min(_RESTART_CLOSE_GRACE_S, remaining / 2)
            try:
                await asyncio.wait_for(
                    self._ensure_connection(cleanup_deadline=deadline),
                    timeout=remaining - cleanup_budget,
                )
            except PermissionError:
                raise
            except Exception:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                await self._sleep(min(remaining, 0.1))
            else:
                if time.monotonic() >= deadline:
                    await self._close_connection(deadline=deadline)
                    raise TimeoutError("core nats restart timed out")
                break
        if terminal_desired:
            await self._subscribe_terminal()
        if progress_desired:
            await self._subscribe_progress()
        if receiver_desired:
            await self._subscribe_receiver()
        self._emit(
            "transport.fault_applied",
            {"action": "restart_coordinator"},
        )

    def _connection_stats(self) -> tuple[int, int]:
        if self._nc is None:
            return (0, 0)
        stats = self._nc.stats
        raw_in = stats.get("in_bytes", 0)
        raw_out = stats.get("out_bytes", 0)
        return (
            raw_in if type(raw_in) is int and raw_in >= 0 else 0,
            raw_out if type(raw_out) is int and raw_out >= 0 else 0,
        )

    async def _close_connection(self, *, deadline: float | None = None) -> None:
        connection = self._nc
        if connection is None:
            await self._await_subscription_monitors()
            return
        current_in, current_out = self._connection_stats()
        self._intentional_close = True
        try:
            closed, failure = await self._close_managed_candidate(
                connection,
                deadline=deadline,
            )
        finally:
            self._intentional_close = False
        if closed:
            self._accumulated_in_bytes += current_in
            self._accumulated_out_bytes += current_out
            self._nc = None
            await self._await_subscription_monitors()
        if failure is not None:
            if deadline is None or not isinstance(failure, Exception):
                raise failure from None
            return
        if not closed:
            return

    async def _release_candidate(
        self,
        candidate: NATS,
        *,
        deadline: float | None,
    ) -> None:
        if all(pending is not candidate for pending in self._pending_candidates):
            self._pending_candidates.append(candidate)
        cleanup_deadline = (
            deadline
            if deadline is not None
            else time.monotonic() + _PENDING_CLOSE_GRACE_S
        )
        closed, failure = await self._close_managed_candidate(
            candidate,
            deadline=cleanup_deadline,
        )
        if closed:
            self._pending_candidates = [
                pending
                for pending in self._pending_candidates
                if pending is not candidate
            ]
        if failure is not None and not isinstance(failure, Exception):
            raise failure from None

    async def _close_pending_candidates(self) -> None:
        while self._pending_candidates:
            candidate = self._pending_candidates[0]
            closed, failure = await self._close_managed_candidate(
                candidate,
                deadline=time.monotonic() + _PENDING_CLOSE_GRACE_S,
            )
            if closed:
                self._pending_candidates.pop(0)
            if failure is not None:
                raise failure from None
            if not closed:
                raise TimeoutError("core nats close timed out") from None

    async def _close_managed_candidate(
        self,
        candidate: NATS,
        *,
        deadline: float | None,
    ) -> tuple[bool, BaseException | None]:
        key = id(candidate)
        close_task = self._candidate_close_tasks.get(key)
        if close_task is None:
            close_task = asyncio.create_task(candidate.close())
            self._candidate_close_tasks[key] = close_task
        closed, failure, still_running = await self._wait_candidate_close_task(
            close_task,
            deadline=deadline,
        )
        if not still_running and self._candidate_close_tasks.get(key) is close_task:
            del self._candidate_close_tasks[key]
        return closed, failure

    @staticmethod
    async def _close_candidate(
        candidate: NATS,
        *,
        deadline: float | None,
    ) -> bool:
        close_task = asyncio.create_task(candidate.close())
        (
            closed,
            failure,
            still_running,
        ) = await CoreNatsTransport._wait_candidate_close_task(
            close_task,
            deadline=deadline,
        )
        if still_running:
            close_task.add_done_callback(
                CoreNatsTransport._consume_candidate_close_task
            )
        if failure is not None and not isinstance(failure, Exception):
            raise failure from None
        return closed

    @staticmethod
    async def _wait_candidate_close_task(
        close_task: asyncio.Task[Any],
        *,
        deadline: float | None,
    ) -> tuple[bool, BaseException | None, bool]:
        if close_task.done():
            closed, failure = CoreNatsTransport._candidate_close_task_result(close_task)
            return closed, failure, False
        interruption: BaseException | None = None
        try:
            if deadline is None:
                await asyncio.wait((close_task,))
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    await asyncio.sleep(0)
                else:
                    await asyncio.wait((close_task,), timeout=remaining)
        except BaseException as error:  # noqa: BLE001 - retain cancellation.
            interruption = error
            close_task.cancel()
        if close_task.done():
            closed, failure = CoreNatsTransport._candidate_close_task_result(close_task)
            return closed, interruption if interruption is not None else failure, False

        close_task.cancel()
        try:
            await asyncio.sleep(0)
        except BaseException as error:  # noqa: BLE001 - retain cancellation.
            if interruption is None:
                interruption = error
            close_task.cancel()
        if close_task.done():
            closed, _ = CoreNatsTransport._candidate_close_task_result(close_task)
            return closed, interruption, False
        return False, interruption, True

    @staticmethod
    def _candidate_close_task_result(
        close_task: asyncio.Task[Any],
    ) -> tuple[bool, BaseException | None]:
        try:
            close_task.result()
        except asyncio.CancelledError:
            return False, asyncio.CancelledError()
        except Exception as error:  # noqa: BLE001 - return a source-free failure.
            return False, _sanitized_failure(error, "core nats operation failed")
        except BaseException as error:  # noqa: BLE001 - preserve the failure type.
            return False, _sanitized_failure(error, "core nats operation failed")
        return True, None

    @staticmethod
    def _consume_candidate_close_task(close_task: asyncio.Task[Any]) -> None:
        try:
            close_task.result()
        except BaseException:  # noqa: BLE001 - detached cleanup is best-effort.
            return

    def _monitor_subscription(self, subscription: Subscription) -> None:
        processing_task = getattr(subscription, "_wait_for_msgs_task", None)
        if not isinstance(processing_task, asyncio.Future):
            return
        monitor = asyncio.create_task(
            self._capture_subscription_failure(processing_task)
        )
        self._subscription_monitors.add(monitor)
        monitor.add_done_callback(self._subscription_monitors.discard)

    async def _capture_subscription_failure(
        self,
        processing_task: asyncio.Future[object],
    ) -> None:
        try:
            await processing_task
        except asyncio.CancelledError:
            return
        except BaseException as error:  # noqa: BLE001 - injected crashes are fatal.
            failure = _sanitized_failure(error, "core nats subscription failed")
            if self._background_failure is None:
                self._background_failure = failure

    @staticmethod
    async def _nats_operation(operation: Awaitable[_T]) -> _T:
        failure: BaseException | None = None
        try:
            result = await operation
        except Exception as error:  # noqa: BLE001 - preserve type after redaction.
            failure = _sanitized_failure(
                error,
                "core nats operation failed",
            )
        else:
            return result
        if failure is None:
            raise RuntimeError("core nats operation failed")
        raise failure from None

    async def _await_subscription_monitors(self) -> None:
        if self._subscription_monitors:
            await asyncio.gather(
                *tuple(self._subscription_monitors),
                return_exceptions=True,
            )

    async def _rollback_subscriptions(
        self,
        connection: NATS,
        subscriptions: list[Subscription],
    ) -> None:
        if not subscriptions:
            return
        await asyncio.gather(
            *(subscription.unsubscribe() for subscription in subscriptions),
            return_exceptions=True,
        )
        await asyncio.gather(connection.flush(), return_exceptions=True)


__all__ = ["CoreNatsTransport"]
