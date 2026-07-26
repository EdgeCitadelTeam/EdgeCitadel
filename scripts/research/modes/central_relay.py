"""Run-owned central relay transport."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import sys
import time
import urllib.parse
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType, TracebackType
from typing import (  # noqa: UP035
    TYPE_CHECKING,
    AsyncContextManager,
    Protocol,
    TypeVar,
    cast,
)

import httpx
import websockets
from websockets.datastructures import Headers as WebSocketHeaders
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
    InvalidProxyStatus,
    InvalidStatus,
)
from websockets.frames import Close as WebSocketClose
from websockets.http11 import Response as WebSocketResponse

from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import (
    ValidationError,
    canonical_json,
    default_validator,
    normalize_task_correlation,
)
from scripts.research.modes.base import (
    EventSink,
    Mode,
    ObservedEnvelope,
    TransportSnapshot,
)

if TYPE_CHECKING:
    from adapters._common.task_executor import InboundDelivery, TaskExecutor

CoordinatorRestart = Callable[[], Awaitable[str | None]]
WorkerOperation = Callable[[str], Awaitable[None]]
AsyncSleep = Callable[[float], Awaitable[None]]
_T = TypeVar("_T")
_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")
_TOKEN_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_UUID_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\Z"
)
_MAX_BODY_BYTES = 1_048_576
_SUPPRESS_RELAY_TRANSPORT_LOGS = ContextVar(
    "_central_relay_suppress_transport_logs",
    default=False,
)


class _RelayTransportLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        del record
        return not _SUPPRESS_RELAY_TRANSPORT_LOGS.get()


for _relay_transport_logger in (
    "httpx",
    "httpcore.connection",
    "httpcore.http11",
    "httpcore.http2",
    "httpcore.proxy",
    "httpcore.socks",
    "websockets",
    "websockets.asyncio.client",
    "websockets.client",
    "websockets.protocol",
):
    logging.getLogger(_relay_transport_logger).addFilter(_RelayTransportLogFilter())


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _valid_relay_url(value: object) -> bool:
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
        parsed.scheme in ("http", "https")
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


def _sanitized_transport_error(
    error: httpx.TransportError,
) -> httpx.TransportError:
    sanitized = type(error)("relay transport failed")
    sanitized.__cause__ = None
    sanitized.__context__ = None
    sanitized.__traceback__ = None
    return sanitized


def _sanitized_failure(error: BaseException, message: str) -> BaseException:
    if isinstance(error, ConnectionClosed):
        sanitized: BaseException = type(error)(
            WebSocketClose(
                1000 if isinstance(error, ConnectionClosedOK) else 1011,
                message,
            ),
            None,
            None,
        )
    elif isinstance(error, (InvalidStatus, InvalidProxyStatus)):
        sanitized = type(error)(
            WebSocketResponse(
                500,
                message,
                WebSocketHeaders(),
            )
        )
    elif type(error) is UnicodeDecodeError:
        sanitized = UnicodeDecodeError(
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
            sanitized = RuntimeError(message)
    sanitized.__cause__ = None
    sanitized.__context__ = None
    sanitized.__traceback__ = None
    return sanitized


async def _websocket_operation(operation: Awaitable[_T]) -> _T:
    failure: BaseException | None = None
    try:
        result = await operation
    except Exception as error:  # noqa: BLE001 - preserve type after redaction.
        failure = _sanitized_failure(error, "relay websocket failed")
    else:
        return result
    if failure is None:
        raise RuntimeError("relay websocket failed")
    raise failure from None


def _response_mapping(response: httpx.Response) -> dict[str, object]:
    try:
        decoded = cast(object, json.loads(response.content))
        if not isinstance(decoded, dict) or canonical_json(decoded) != response.content:
            raise ValueError
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise RuntimeError("invalid relay response") from None
    return cast(dict[str, object], decoded)


def _validate_relay_envelope(envelope: dict[str, object]) -> None:
    candidate = dict(envelope)
    if candidate.get("type") in ("command", "delegation", "cancel", "result"):
        # The envelope schema accepts either UUID hex case; validate correlation
        # against an equivalent lowercase projection without rewriting payloads.
        for field in ("task_id", "context_id"):
            value = candidate.get(field)
            if isinstance(value, str) and _UUID_PATTERN.fullmatch(value):
                candidate[field] = value.lower()
        payload = candidate.get("payload")
        if isinstance(payload, Mapping):
            candidate_payload = dict(payload)
            parent_task_id = candidate_payload.get("parent_task_id")
            if isinstance(parent_task_id, str) and _UUID_PATTERN.fullmatch(
                parent_task_id
            ):
                candidate_payload["parent_task_id"] = parent_task_id.lower()
            candidate["payload"] = candidate_payload
    default_validator().validate_envelope(candidate)


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


@dataclass
class _RelayBinding:
    lease_id: str
    worker_agent_id: str
    request_envelope_id: str
    request_sender_id: str
    task_id: str
    context_id: str
    hop_count: int
    parent_task_id: object
    lease_ttl_ms: int
    renewal_interval_ms: int
    lease_deadline_ns: int
    owner_task: asyncio.Task[object] | None
    delivery: _RelayDelivery | None = None
    active: bool = True
    finalizing: bool = False


class _RelayDelivery:
    def __init__(
        self,
        transport: CentralRelayTransport,
        binding: _RelayBinding,
        *,
        raw: bytes,
        delivery_count: int,
    ) -> None:
        self.worker_agent_id = binding.worker_agent_id
        self.raw = raw
        self.delivery_count = delivery_count
        self.stream_sequence: int | None = None
        self._transport = transport
        self._binding = binding

    async def in_progress(self) -> None:
        await self._transport._delivery_disposition(self, "renew")

    async def commit(self) -> None:
        await self._transport._delivery_disposition(self, "commit")

    async def retry(self) -> None:
        await self._transport._delivery_disposition(self, "retry")

    async def terminate(self) -> None:
        await self._transport._delivery_disposition(self, "terminate")


class _RelaySocket(Protocol):
    async def recv(self) -> object: ...


class _SanitizedRelaySocket:
    def __init__(self, socket: _RelaySocket) -> None:
        self._socket = socket

    async def recv(self) -> object:
        return await _websocket_operation(self._socket.recv())


class _SanitizedRelaySocketContext:
    def __init__(self, context: AsyncContextManager[object]) -> None:
        self._context = context

    async def __aenter__(self) -> _RelaySocket:
        raw_socket = await _websocket_operation(self._context.__aenter__())
        return _SanitizedRelaySocket(cast(_RelaySocket, raw_socket))

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if exc_value is not None:
            try:
                await self._context.__aexit__(
                    exc_type,
                    exc_value,
                    traceback,
                )
            except Exception:  # noqa: BLE001 - preserve the body failure.
                return False
            return False
        return await _websocket_operation(
            self._context.__aexit__(exc_type, exc_value, traceback)
        )


class _RelayFaultController:
    def __init__(
        self,
        *,
        transport: CentralRelayTransport,
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


class CentralRelayTransport:
    def __init__(
        self,
        *,
        relay_url: str,
        run_id: str,
        token: str,
        event_sink: EventSink,
        coordinator_restart: CoordinatorRestart | None = None,
        worker_stop: WorkerOperation | None = None,
        worker_start: WorkerOperation | None = None,
        http_client: httpx.AsyncClient | None = None,
        websocket_connect: Callable[..., AsyncContextManager[object]] | None = None,
        evidence_clock_ns: Callable[[], int] = time.perf_counter_ns,
        epoch_now: Callable[[], str] = _now_iso,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        if not _valid_relay_url(relay_url):
            raise ValueError("invalid relay_url")
        if type(run_id) is not str or _ID_PATTERN.fullmatch(run_id) is None:
            raise ValueError("invalid run_id")
        if type(token) is not str or _TOKEN_PATTERN.fullmatch(token) is None:
            raise ValueError("invalid token")
        self._relay_url = relay_url
        self._run_id = run_id
        self._token = token
        self._event_sink = event_sink
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._websocket_connect = websocket_connect or websockets.connect
        self._evidence_clock_ns = evidence_clock_ns
        self._epoch_now = epoch_now
        self._sleep = sleep
        self._binding: ContextVar[_RelayBinding | None] = ContextVar(
            "_central_relay_binding",
            default=None,
        )
        self._terminal_observer_started = False
        self._terminal_replay_high_water = 0
        self._terminal_cursors: dict[str, int] = {}
        self._terminal_observation_index = 0
        self._progress_observation_index = 0
        self._progress_started_once = False
        self._progress_desired = False
        self._progress_ready: asyncio.Event | None = None
        self._progress_task: asyncio.Task[None] | None = None
        self._receiver_agent_id: str | None = None
        self._receiver_executor: TaskExecutor | None = None
        self._receiver_desired = False
        self._receiver_ready: asyncio.Event | None = None
        self._receiver_task: asyncio.Task[None] | None = None
        self._background_failure: BaseException | None = None
        self._request_body_bytes = 0
        self._response_body_bytes = 0
        self._faults = _RelayFaultController(
            transport=self,
            coordinator_restart=coordinator_restart,
            worker_stop=worker_stop,
            worker_start=worker_start,
        )
        self._closed = False

    @property
    def mode(self) -> Mode:
        return Mode.CENTRAL_RELAY

    @property
    def outcome_ledger_enabled(self) -> bool:
        return True

    @property
    def faults(self) -> _RelayFaultController:
        return self._faults

    @property
    def resolved_config(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "mode": "central-relay",
                "ablation": "full-contract",
                "nats_msg_id": False,
                "outcome_ledger": True,
            }
        )

    async def start_terminal_observer(self) -> None:
        self._check_open()
        if self._terminal_observer_started:
            raise RuntimeError("terminal observer already started")
        response = await self._request(
            "GET",
            "/healthz",
            operation="start_terminal_observer",
        )
        health = self._parse_health(response)
        self._terminal_replay_high_water = cast(
            int,
            health["committed_terminal_sequence"],
        )
        self._terminal_observer_started = True
        self._emit(
            "transport.observer_ready",
            {"kind": "terminal", "agent_id": None},
        )

    async def start_progress_observer(self) -> None:
        self._check_open()
        if self._progress_started_once:
            raise RuntimeError("progress observer already started")
        self._progress_started_once = True
        self._progress_desired = True
        await self._start_progress_task()

    async def start_receiver(
        self,
        agent_id: str,
        executor: TaskExecutor,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if self._receiver_task is not None or self._receiver_desired:
            raise RuntimeError("receiver already started")
        self._receiver_agent_id = agent_id
        self._receiver_executor = executor
        self._receiver_desired = True
        self._spawn_receiver()

    async def wait_receiver_ready(self, agent_id: str, timeout_s: float) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if not _valid_timeout(timeout_s, allow_zero=False):
            raise ValueError("invalid timeout_s")
        if (
            self._receiver_agent_id != agent_id
            or self._receiver_ready is None
            or self._receiver_task is None
        ):
            raise RuntimeError("receiver agent mismatch")
        ready_waiter = asyncio.create_task(self._receiver_ready.wait())
        try:
            done, _ = await asyncio.wait(
                (ready_waiter, self._receiver_task),
                timeout=timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if ready_waiter in done:
                await ready_waiter
                return
            if self._receiver_task in done:
                await self._receiver_task
            raise TimeoutError
        finally:
            if not ready_waiter.done():
                ready_waiter.cancel()
            await asyncio.gather(ready_waiter, return_exceptions=True)

    async def long_poll(
        self,
        agent_id: str,
        timeout_s: float,
    ) -> InboundDelivery | None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if not _valid_timeout(timeout_s, allow_zero=True) or timeout_s > 30:
            raise ValueError("invalid timeout_s")
        timeout_ms_value = timeout_s * 1000
        timeout_ms = int(timeout_ms_value)
        if timeout_ms != timeout_ms_value:
            raise ValueError("invalid timeout_s")
        current = self._binding.get()
        if current is not None and current.active:
            raise RuntimeError("relay execution already active")
        response = await self._request(
            "GET",
            f"/v1/workers/{agent_id}/lease?timeout_ms={timeout_ms}",
            operation="long_poll",
            allowed_statuses=(204,),
        )
        if response.status_code == 204:
            if response.content:
                raise RuntimeError("invalid relay response")
            return None
        lease = _response_mapping(response)
        expected_keys = {
            "delivery_count",
            "envelope",
            "lease_deadline_ns",
            "lease_id",
            "lease_ttl_ms",
            "renewal_interval_ms",
            "stream_sequence",
        }
        envelope = lease.get("envelope")
        if (
            set(lease) != expected_keys
            or type(lease.get("delivery_count")) is not int
            or cast(int, lease["delivery_count"]) <= 0
            or type(lease.get("lease_deadline_ns")) is not int
            or cast(int, lease["lease_deadline_ns"]) <= 0
            or type(lease.get("lease_id")) is not str
            or _UUID_PATTERN.fullmatch(cast(str, lease["lease_id"])) is None
            or type(lease.get("lease_ttl_ms")) is not int
            or not 3 <= cast(int, lease["lease_ttl_ms"]) <= 300_000
            or type(lease.get("renewal_interval_ms")) is not int
            or lease["renewal_interval_ms"]
            != max(1, cast(int, lease["lease_ttl_ms"]) // 3)
            or lease.get("stream_sequence") is not None
            or not isinstance(envelope, dict)
        ):
            raise RuntimeError("invalid relay response")
        try:
            _validate_relay_envelope(envelope)
            if envelope.get("type") not in ("command", "delegation", "cancel"):
                raise ValueError
            correlation = normalize_task_correlation(envelope)
            if correlation["recipient_id"] != agent_id:
                raise ValueError
            raw = canonical_json(envelope)
            request_payload = cast(
                Mapping[str, object],
                correlation["payload"],
            )
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise RuntimeError("invalid relay response") from None
        owner = asyncio.current_task()
        binding = _RelayBinding(
            lease_id=cast(str, lease["lease_id"]),
            worker_agent_id=agent_id,
            request_envelope_id=cast(str, envelope["id"]),
            request_sender_id=cast(str, correlation["sender_id"]),
            task_id=cast(str, correlation["task_id"]),
            context_id=cast(str, correlation["context_id"]),
            hop_count=cast(int, correlation["hop_count"]),
            parent_task_id=request_payload.get("parent_task_id"),
            lease_ttl_ms=cast(int, lease["lease_ttl_ms"]),
            renewal_interval_ms=lease["renewal_interval_ms"],
            lease_deadline_ns=cast(int, lease["lease_deadline_ns"]),
            owner_task=cast(asyncio.Task[object] | None, owner),
        )
        delivery = _RelayDelivery(
            self,
            binding,
            raw=raw,
            delivery_count=cast(int, lease["delivery_count"]),
        )
        binding.delivery = delivery
        self._binding.set(binding)
        self._emit(
            "transport.worker_delivery",
            {
                "worker_agent_id": agent_id,
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "delivery_count": delivery.delivery_count,
                "stream_sequence": None,
                "lease_id": binding.lease_id,
            },
        )
        return delivery

    async def submit_task(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        encoded = self._validated_envelope(
            envelope,
            ("command", "delegation", "cancel"),
        )
        response = await self._request(
            "POST",
            "/v1/tasks",
            content=encoded,
            operation="submit_task",
        )
        receipt = self._receipt(response, cast(str, json.loads(encoded)["id"]))
        self._emit_publication("submit_task", encoded, receipt)
        return receipt

    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        return await self._publish_event(
            envelope,
            allowed_types=("task.progress",),
            operation="publish_progress",
        )

    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self._check_open()
        binding = self._binding.get()
        if binding is None or not binding.active:
            raise RuntimeError("no active relay execution")
        if binding.owner_task is not asyncio.current_task() or binding.delivery is None:
            raise RuntimeError("relay execution binding mismatch")
        encoded = self._validated_envelope(envelope, ("result",))
        decoded = cast(dict[str, object], json.loads(encoded))
        try:
            correlation = normalize_task_correlation(decoded)
            payload = cast(Mapping[str, object], correlation["payload"])
        except (TypeError, ValueError, ValidationError, RecursionError):
            raise ValueError("terminal does not match relay lease") from None
        if (
            correlation["sender_id"] != binding.worker_agent_id
            or correlation["recipient_id"] != binding.request_sender_id
            or correlation["task_id"] != binding.task_id
            or correlation["context_id"] != binding.context_id
            or correlation["hop_count"] != binding.hop_count
            or payload.get("parent_task_id") != binding.parent_task_id
        ):
            raise ValueError("terminal does not match relay lease")
        response = await self._request(
            "POST",
            f"/v1/leases/{binding.lease_id}/terminal",
            content=encoded,
            operation="publish_terminal",
        )
        receipt = self._receipt(response, cast(str, decoded["id"]))
        if receipt.application_bytes != len(encoded):
            raise RuntimeError("invalid relay response")
        self._emit_publication("publish_terminal", encoded, receipt)
        return receipt

    async def publish_heartbeat(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        return await self._publish_event(
            envelope,
            allowed_types=("heartbeat",),
            operation="publish_heartbeat",
        )

    async def observe_terminal(
        self,
        task_id: str,
        timeout_s: float,
    ) -> ObservedEnvelope | None:
        self._check_open()
        if not self._terminal_observer_started:
            raise RuntimeError("terminal observer is not started")
        if type(task_id) is not str or _UUID_PATTERN.fullmatch(task_id) is None:
            raise ValueError("invalid task_id")
        if not _valid_timeout(timeout_s, allow_zero=True):
            raise ValueError("invalid timeout_s")
        deadline = time.monotonic() + timeout_s
        while True:
            cursor = self._terminal_cursors.get(task_id, 0)
            response = await self._request(
                "GET",
                f"/v1/tasks/{task_id}/terminal?after={cursor}",
                operation="observe_terminal",
                allowed_statuses=(404,),
            )
            if response.status_code == 404:
                error = _response_mapping(response)
                if error != {"error": "terminal_not_found"}:
                    raise RuntimeError("invalid relay response")
                remaining = deadline - time.monotonic()
                if timeout_s == 0 or remaining <= 0:
                    return None
                await self._sleep(min(remaining, 0.01))
                continue
            body = _response_mapping(response)
            envelope = body.get("envelope")
            if (
                set(body) != {"delivery_count", "envelope", "terminal_sequence"}
                or type(body.get("delivery_count")) is not int
                or cast(int, body["delivery_count"]) <= 0
                or type(body.get("terminal_sequence")) is not int
                or cast(int, body["terminal_sequence"]) <= cursor
                or not isinstance(envelope, dict)
            ):
                raise RuntimeError("invalid relay response")
            try:
                _validate_relay_envelope(envelope)
            except (ValidationError, RecursionError):
                raise RuntimeError("invalid relay response") from None
            if envelope.get("type") != "result" or envelope.get("task_id") != task_id:
                raise RuntimeError("invalid relay response")
            sequence = cast(int, body["terminal_sequence"])
            self._terminal_cursors[task_id] = sequence
            self._terminal_observation_index += 1
            observed = ObservedEnvelope(
                envelope=MappingProxyType(dict(envelope)),
                observed_ns=self._evidence_clock_ns(),
                observation_index=self._terminal_observation_index,
                stream_sequence=None,
                delivery_count=cast(int, body["delivery_count"]),
                replayed=sequence <= self._terminal_replay_high_water,
                delivery=None,
            )
            self._emit(
                "transport.terminal_observed",
                {
                    "task_id": task_id,
                    "envelope_id": envelope["id"],
                    "observation_index": observed.observation_index,
                    "stream_sequence": None,
                    "delivery_count": observed.delivery_count,
                    "replayed": observed.replayed,
                },
            )
            return observed

    async def inspect_state(self) -> TransportSnapshot:
        self._check_open()
        response = await self._request(
            "GET",
            "/healthz",
            operation="inspect_state",
        )
        health = self._parse_health(response)
        return TransportSnapshot(
            mode=Mode.CENTRAL_RELAY,
            streams=MappingProxyType({}),
            consumers=MappingProxyType({}),
            pending=cast(int, health["pending"]),
            ack_pending=cast(int, health["ack_pending"]),
            connection_bytes=MappingProxyType(
                {
                    "request_body_bytes": self._request_body_bytes,
                    "response_body_bytes": self._response_body_bytes,
                }
            ),
            storage_bytes=cast(int, health["storage_bytes"]),
            message_count=cast(int, health["message_count"]),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._receiver_desired = False
        self._progress_desired = False
        tasks = [
            task
            for task in (self._receiver_task, self._progress_task)
            if task is not None
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        binding = self._binding.get()
        if binding is not None:
            binding.active = False
            self._binding.set(None)
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
        self._closed = True
        if self._background_failure is not None:
            raise self._background_failure

    def _check_open(self) -> None:
        if self._background_failure is not None:
            raise self._background_failure
        if self._closed:
            raise RuntimeError("transport is closed")

    @staticmethod
    def _validate_agent_id(agent_id: object) -> None:
        if type(agent_id) is not str or _ID_PATTERN.fullmatch(agent_id) is None:
            raise ValueError("invalid agent_id")

    def _task_finished(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        failure = task.exception()
        if failure is not None and self._background_failure is None:
            self._background_failure = failure

    def _emit(self, event: str, data: Mapping[str, object]) -> None:
        self._event_sink.emit(
            {
                "monotonic_ns": self._evidence_clock_ns(),
                "epoch_time": self._epoch_now(),
                "component": "central_relay",
                "event": event,
                "data": dict(data),
            }
        )

    def _emit_publication(
        self,
        operation: str,
        encoded: bytes,
        receipt: PublicationReceipt,
    ) -> None:
        decoded = cast(dict[str, object], json.loads(encoded))
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

    async def _publish_event(
        self,
        envelope: Mapping[str, object],
        *,
        allowed_types: tuple[str, ...],
        operation: str,
    ) -> PublicationReceipt:
        encoded = self._validated_envelope(envelope, allowed_types)
        decoded = cast(dict[str, object], json.loads(encoded))
        response = await self._request(
            "POST",
            "/v1/events",
            content=encoded,
            operation=operation,
        )
        receipt = self._receipt(response, cast(str, decoded["id"]))
        if receipt.application_bytes != len(encoded):
            raise RuntimeError("invalid relay response")
        self._emit_publication(operation, encoded, receipt)
        return receipt

    def _parse_health(self, response: httpx.Response) -> dict[str, object]:
        health = _response_mapping(response)
        integer_fields = (
            "ack_pending",
            "committed_terminal_sequence",
            "message_count",
            "pending",
            "storage_bytes",
        )
        if (
            set(health)
            != {
                "ack_pending",
                "committed_terminal_sequence",
                "message_count",
                "pending",
                "run_id",
                "status",
                "storage_bytes",
            }
            or health.get("run_id") != self._run_id
            or health.get("status") != "ok"
            or any(
                type(health.get(field)) is not int or cast(int, health[field]) < 0
                for field in integer_fields
            )
        ):
            raise RuntimeError("invalid relay response")
        return health

    def _require_delivery_binding(
        self,
        delivery: _RelayDelivery,
    ) -> _RelayBinding:
        binding = self._binding.get()
        if (
            binding is None
            or not binding.active
            or binding.delivery is not delivery
            or binding.worker_agent_id != delivery.worker_agent_id
            or binding.owner_task is not asyncio.current_task()
        ):
            raise RuntimeError("relay execution binding mismatch")
        return binding

    async def _delivery_disposition(
        self,
        delivery: _RelayDelivery,
        disposition: str,
    ) -> None:
        self._check_open()
        binding = self._require_delivery_binding(delivery)
        final = disposition in ("retry", "terminate", "commit")
        if final:
            binding.finalizing = True
        try:
            await self._apply_disposition(binding, disposition)
        except BaseException:
            if final:
                binding.finalizing = False
            raise
        if final:
            binding.active = False
            binding.finalizing = False
            self._binding.set(None)

    async def _apply_disposition(
        self,
        binding: _RelayBinding,
        disposition: str,
    ) -> None:
        content = canonical_json({"disposition": disposition})
        response = await self._request(
            "POST",
            f"/v1/leases/{binding.lease_id}/commit",
            content=content,
            operation=f"lease_{disposition}",
        )
        body = _response_mapping(response)
        if disposition == "renew":
            if (
                set(body)
                != {
                    "disposition",
                    "lease_deadline_ns",
                    "lease_id",
                    "lease_ttl_ms",
                    "renewal_interval_ms",
                    "state",
                }
                or body.get("disposition") != "renew"
                or body.get("lease_id") != binding.lease_id
                or body.get("lease_ttl_ms") != binding.lease_ttl_ms
                or body.get("renewal_interval_ms") != binding.renewal_interval_ms
                or body.get("state") != "active"
                or type(body.get("lease_deadline_ns")) is not int
                or cast(int, body["lease_deadline_ns"]) <= 0
            ):
                raise RuntimeError("invalid relay response")
            binding.lease_deadline_ns = cast(int, body["lease_deadline_ns"])
            return
        if disposition in ("retry", "terminate"):
            expected_state = "queued" if disposition == "retry" else "terminated"
            if body != {
                "disposition": disposition,
                "lease_id": binding.lease_id,
                "state": expected_state,
            }:
                raise RuntimeError("invalid relay response")
            return
        if disposition != "commit":
            raise AssertionError("unknown relay disposition")
        envelope = body.get("envelope")
        if (
            set(body)
            != {
                "disposition",
                "envelope",
                "lease_id",
                "state",
                "terminal_sequence",
            }
            or body.get("disposition") != "commit"
            or body.get("lease_id") != binding.lease_id
            or body.get("state") != "completed"
            or type(body.get("terminal_sequence")) is not int
            or cast(int, body["terminal_sequence"]) <= 0
            or not isinstance(envelope, dict)
        ):
            raise RuntimeError("invalid relay response")
        try:
            _validate_relay_envelope(envelope)
        except (ValidationError, RecursionError):
            raise RuntimeError("invalid relay response") from None
        if (
            envelope.get("type") != "result"
            or envelope.get("task_id") != binding.task_id
        ):
            raise RuntimeError("invalid relay response")

    async def _auto_renew(self, binding: _RelayBinding) -> None:
        while binding.active:
            await self._sleep(binding.renewal_interval_ms / 1000)
            if binding.active and not binding.finalizing:
                try:
                    await self._apply_disposition(binding, "renew")
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 409 and (
                        binding.finalizing or not binding.active
                    ):
                        return
                    raise

    async def _execute_bound(
        self,
        binding: _RelayBinding,
        executor: TaskExecutor,
        delivery: _RelayDelivery,
    ) -> None:
        binding.owner_task = cast(
            asyncio.Task[object] | None,
            asyncio.current_task(),
        )
        self._binding.set(binding)
        await executor.execute(delivery)

    async def _execute_with_renewal(
        self,
        executor: TaskExecutor,
        delivery: _RelayDelivery,
        binding: _RelayBinding,
    ) -> None:
        execution = asyncio.create_task(
            self._execute_bound(binding, executor, delivery)
        )
        renewal = asyncio.create_task(self._auto_renew(binding))
        try:
            done, _ = await asyncio.wait(
                (execution, renewal),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if execution in done:
                await execution
                if renewal.done() and binding.active:
                    await renewal
            else:
                await renewal
                if binding.finalizing or not binding.active:
                    await execution
                else:
                    execution.cancel()
        finally:
            for task in (execution, renewal):
                if not task.done():
                    task.cancel()
            await asyncio.gather(execution, renewal, return_exceptions=True)
            binding.active = False
            binding.finalizing = False
            current = self._binding.get()
            if current is binding:
                self._binding.set(None)

    def _spawn_receiver(self) -> None:
        if self._receiver_agent_id is None or self._receiver_executor is None:
            raise RuntimeError("worker control is unavailable")
        self._receiver_ready = asyncio.Event()
        task = asyncio.create_task(
            self._receiver_loop(
                self._receiver_agent_id,
                self._receiver_executor,
                self._receiver_ready,
            )
        )
        self._receiver_task = task
        task.add_done_callback(self._task_finished)

    async def _receiver_loop(
        self,
        agent_id: str,
        executor: TaskExecutor,
        ready: asyncio.Event,
    ) -> None:
        delivery = await self.long_poll(agent_id, 0)
        self._emit(
            "transport.receiver_ready",
            {"kind": "receiver", "agent_id": agent_id},
        )
        ready.set()
        while self._receiver_desired:
            if delivery is None:
                delivery = await self.long_poll(agent_id, 30)
                if delivery is None:
                    continue
            relay_delivery = cast(_RelayDelivery, delivery)
            binding = self._require_delivery_binding(relay_delivery)
            await self._execute_with_renewal(executor, relay_delivery, binding)
            delivery = None

    def _progress_url(self) -> str:
        parsed = urllib.parse.urlsplit(self._relay_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        base = urllib.parse.urlunsplit((scheme, parsed.netloc, parsed.path, "", ""))
        return f"{base.rstrip('/')}/v1/events"

    def _progress_context(self) -> AsyncContextManager[_RelaySocket]:
        failure: BaseException | None = None
        try:
            context = self._websocket_connect(
                self._progress_url(),
                additional_headers={"Authorization": f"Bearer {self._token}"},
                proxy=None,
                max_size=_MAX_BODY_BYTES,
            )
        except Exception as error:  # noqa: BLE001 - preserve type after redaction.
            failure = _sanitized_failure(error, "relay websocket failed")
        else:
            return _SanitizedRelaySocketContext(context)
        if failure is None:
            raise RuntimeError("relay websocket failed")
        raise failure from None

    async def _start_progress_task(self) -> None:
        ready = asyncio.Event()
        self._progress_ready = ready
        task = asyncio.create_task(self._progress_loop(ready))
        self._progress_task = task
        task.add_done_callback(self._task_finished)
        ready_waiter = asyncio.create_task(ready.wait())
        try:
            done, _ = await asyncio.wait(
                (task, ready_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if task in done:
                await task
            await ready_waiter
        finally:
            if not ready_waiter.done():
                ready_waiter.cancel()
            await asyncio.gather(ready_waiter, return_exceptions=True)

    async def _progress_loop(self, ready: asyncio.Event) -> None:
        log_token = _SUPPRESS_RELAY_TRANSPORT_LOGS.set(True)
        try:
            async with self._progress_context() as socket:
                first = await socket.recv()
                if not isinstance(first, bytes) or self._decode_socket_frame(first) != {
                    "kind": "observer_ready",
                    "run_id": self._run_id,
                }:
                    raise RuntimeError("invalid relay response")
                self._emit(
                    "transport.observer_ready",
                    {"kind": "progress", "agent_id": None},
                )
                ready.set()
                while self._progress_desired:
                    frame = await socket.recv()
                    if not isinstance(frame, bytes):
                        raise RuntimeError(  # noqa: TRY004
                            "invalid relay response"
                        )
                    envelope = self._decode_socket_frame(frame)
                    try:
                        _validate_relay_envelope(envelope)
                    except (ValidationError, RecursionError):
                        raise RuntimeError("invalid relay response") from None
                    if envelope.get("type") not in (
                        "task.progress",
                        "heartbeat",
                        "status",
                    ):
                        raise RuntimeError("invalid relay response")
                    self._progress_observation_index += 1
                    self._emit(
                        "transport.transient_observed",
                        {
                            "envelope_type": envelope["type"],
                            "envelope_id": envelope["id"],
                            "task_id": envelope.get("task_id"),
                            "sender_id": envelope["sender_id"],
                            "observation_index": self._progress_observation_index,
                            "stream_sequence": None,
                            "delivery_count": 1,
                            "replayed": False,
                        },
                    )
        finally:
            _SUPPRESS_RELAY_TRANSPORT_LOGS.reset(log_token)

    @staticmethod
    def _decode_socket_frame(frame: bytes) -> dict[str, object]:
        try:
            decoded = cast(object, json.loads(frame))
            if not isinstance(decoded, dict) or canonical_json(decoded) != frame:
                raise ValueError
        except (TypeError, ValueError, UnicodeError, RecursionError):
            raise RuntimeError("invalid relay response") from None
        return cast(dict[str, object], decoded)

    async def _disconnect_progress_observer(self) -> None:
        self._check_open()
        task = self._progress_task
        if not self._progress_desired or task is None:
            raise RuntimeError("progress observer is not started")
        await self._quiesce_progress_observer()
        self._emit(
            "transport.fault_applied",
            {"action": "disconnect_progress_observer"},
        )

    async def _quiesce_progress_observer(self) -> None:
        task = self._progress_task
        self._progress_desired = False
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        self._progress_task = None
        self._progress_ready = None

    async def _reconnect_progress_observer(self) -> None:
        self._check_open()
        if not self._progress_started_once or self._progress_desired:
            raise RuntimeError("progress observer is not disconnected")
        self._progress_desired = True
        await self._start_progress_task()
        self._emit(
            "transport.fault_applied",
            {"action": "reconnect_progress_observer"},
        )

    async def _stop_worker(
        self,
        agent_id: str,
        callback: WorkerOperation | None,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if callback is not None:
            await callback(agent_id)
        else:
            if (
                self._receiver_agent_id != agent_id
                or not self._receiver_desired
                or self._receiver_task is None
            ):
                raise RuntimeError("worker control is unavailable")
            self._receiver_desired = False
            task = self._receiver_task
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._receiver_task = None
        self._emit("transport.fault_applied", {"action": "stop_worker"})

    async def _start_worker(
        self,
        agent_id: str,
        callback: WorkerOperation | None,
    ) -> None:
        self._check_open()
        self._validate_agent_id(agent_id)
        if callback is not None:
            await callback(agent_id)
        else:
            if (
                self._receiver_agent_id != agent_id
                or self._receiver_desired
                or self._receiver_executor is None
            ):
                raise RuntimeError("worker control is unavailable")
            self._receiver_desired = True
            self._spawn_receiver()
        self._emit("transport.fault_applied", {"action": "start_worker"})

    async def _restart_coordinator(
        self,
        callback: CoordinatorRestart | None,
    ) -> None:
        self._check_open()
        if callback is None:
            raise RuntimeError("coordinator control is unavailable")
        restore_progress = self._progress_desired and self._progress_task is not None
        if restore_progress:
            await self._quiesce_progress_observer()
        try:
            replacement = await callback()
            if replacement is not None:
                if not _valid_relay_url(replacement):
                    raise ValueError("invalid relay_url")
                self._relay_url = replacement
            if self._owns_http_client and self._http_client is not None:
                await self._http_client.aclose()
                self._http_client = None
        except BaseException:
            if restore_progress:
                self._progress_desired = True
                await self._start_progress_task()
            raise
        if restore_progress:
            self._progress_desired = True
            await self._start_progress_task()
        self._emit(
            "transport.fault_applied",
            {"action": "restart_coordinator"},
        )

    def _validated_envelope(
        self,
        envelope: Mapping[str, object],
        allowed_types: tuple[str, ...],
    ) -> bytes:
        try:
            encoded: bytes = canonical_json(envelope)
            decoded = json.loads(encoded)
            if not isinstance(decoded, dict):
                raise TypeError
            _validate_relay_envelope(decoded)
        except (TypeError, ValueError, UnicodeError, ValidationError, RecursionError):
            raise ValueError("invalid envelope") from None
        if decoded.get("type") not in allowed_types:
            raise ValueError("invalid envelope")
        return encoded

    async def _request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        content: bytes | None = None,
        allowed_statuses: tuple[int, ...] = (),
    ) -> httpx.Response:
        self._check_open()
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=5.0,
                    read=35.0,
                    write=5.0,
                    pool=5.0,
                ),
                trust_env=False,
            )
        request_bytes = len(content or b"")
        self._request_body_bytes += request_bytes
        log_token = _SUPPRESS_RELAY_TRANSPORT_LOGS.set(True)
        response: httpx.Response | None = None
        transport_failure: httpx.TransportError | None = None
        try:
            response = await self._http_client.request(
                method,
                f"{self._relay_url.rstrip('/')}{path}",
                content=content,
                headers={
                    "Authorization": f"Bearer {self._token}",
                    **(
                        {"Content-Type": "application/json"}
                        if content is not None
                        else {}
                    ),
                },
            )
        except httpx.TransportError as error:
            transport_failure = _sanitized_transport_error(error)
        finally:
            _SUPPRESS_RELAY_TRANSPORT_LOGS.reset(log_token)
        if transport_failure is not None:
            raise transport_failure from None
        if response is None:
            raise RuntimeError("relay request failed")
        response_bytes = len(response.content)
        self._response_body_bytes += response_bytes
        self._emit(
            "transport.http_exchange",
            {
                "operation": operation,
                "request_body_bytes": request_bytes,
                "response_body_bytes": response_bytes,
                "status_code": response.status_code,
            },
        )
        if response.status_code == 401:
            raise PermissionError("transport authentication failed") from None
        if response.status_code not in allowed_statuses and not response.is_success:
            safe_request = httpx.Request(method, "https://relay.invalid")
            safe_response = httpx.Response(
                response.status_code,
                request=safe_request,
            )
            raise httpx.HTTPStatusError(
                "relay request failed",
                request=safe_request,
                response=safe_response,
            ) from None
        return response

    @staticmethod
    def _receipt(
        response: httpx.Response,
        expected_envelope_id: str,
    ) -> PublicationReceipt:
        try:
            raw = _response_mapping(response)
            if set(raw) != {
                "envelope_id",
                "accepted",
                "transport",
                "stream",
                "stream_sequence",
                "duplicate",
                "accepted_ns",
                "application_bytes",
                "wire_bytes",
            }:
                raise ValueError
            receipt = PublicationReceipt(
                envelope_id=cast(str, raw["envelope_id"]),
                accepted=cast(bool, raw["accepted"]),
                transport=cast(str, raw["transport"]),
                stream=cast(str | None, raw["stream"]),
                stream_sequence=cast(int | None, raw["stream_sequence"]),
                duplicate=cast(bool | None, raw["duplicate"]),
                accepted_ns=cast(int, raw["accepted_ns"]),
                application_bytes=cast(int, raw["application_bytes"]),
                wire_bytes=cast(int | None, raw["wire_bytes"]),
            )
        except (TypeError, ValueError, UnicodeError):
            raise RuntimeError("invalid relay response") from None
        if (
            receipt.envelope_id != expected_envelope_id
            or receipt.accepted is not True
            or receipt.transport != "central-relay"
            or type(receipt.accepted_ns) is not int
            or receipt.accepted_ns <= 0
            or type(receipt.application_bytes) is not int
            or receipt.application_bytes < 0
            or receipt.stream is not None
            or receipt.stream_sequence is not None
            or receipt.duplicate is not None
            or receipt.wire_bytes is not None
        ):
            raise RuntimeError("invalid relay response")
        return receipt


__all__ = ["CentralRelayTransport"]
