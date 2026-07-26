"""Transport-neutral task validation, execution, and finalization."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Literal, Protocol, cast

from adapters._common.outcome_store import (
    OutcomeConflict,
    OutcomeKey,
    OutcomeStore,
    PreparedOutcome,
)
from adapters._common.task_publisher import (
    EventSink,
    ProgressPublisher,
    TerminalPublisher,
)
from adapters._common.task_types import PublicationReceipt
from adapters._common.validator import (
    ValidationError,
    canonical_json,
    default_validator,
    normalize_task_correlation,
    request_fingerprint,
)

Classification = Literal["completed", "failed", "canceled", "rejected", "poison"]
LedgerDecision = Literal[
    "miss",
    "hit",
    "collision",
    "disabled",
    "not_applicable",
]
TerminalState = Literal["completed", "failed", "canceled", "rejected"]
_MAX_JSON_NESTING_DEPTH = 128


class InboundDelivery(Protocol):
    worker_agent_id: str
    raw: bytes
    delivery_count: int
    stream_sequence: int | None

    async def in_progress(self) -> None: ...

    async def commit(self) -> None: ...

    async def retry(self) -> None: ...

    async def terminate(self) -> None: ...


@dataclass(frozen=True)
class PolicyDecision:
    accepted: bool
    reason: str | None


class ExecutionPolicy(Protocol):
    def evaluate(
        self,
        envelope: Mapping[str, object],
        worker_agent_id: str,
    ) -> PolicyDecision: ...


class Clock(Protocol):
    def monotonic_ns(self) -> int: ...

    def now_iso(self) -> str: ...


class UUIDFactory(Protocol):
    def uuid4(self) -> str: ...


class CrashHook(Protocol):
    def hit(self, point: str) -> None: ...


@dataclass
class ExecutionContext:
    agent_id: str
    nc: object | None
    js: object | None
    delivery: InboundDelivery
    progress_publisher: ProgressPublisher
    _task_id: str = field(init=False, repr=False, compare=False)
    _recipient_id: str = field(init=False, repr=False, compare=False)
    _context_id: str = field(init=False, repr=False, compare=False)
    _hop_count: int = field(init=False, repr=False, compare=False)
    _clock: Clock = field(init=False, repr=False, compare=False)
    _uuid_factory: UUIDFactory = field(init=False, repr=False, compare=False)

    def _bind(
        self,
        *,
        task_id: str,
        recipient_id: str,
        context_id: str,
        hop_count: int,
        clock: Clock,
        uuid_factory: UUIDFactory,
    ) -> None:
        self._task_id = task_id
        self._recipient_id = recipient_id
        self._context_id = context_id
        self._hop_count = hop_count
        self._clock = clock
        self._uuid_factory = uuid_factory

    async def in_progress(self) -> None:
        await self.delivery.in_progress()

    async def publish_progress(
        self,
        task_id: str,
        *,
        body: str = "",
        progress: int | None = None,
        extra: Mapping[str, object] | None = None,
    ) -> PublicationReceipt:
        if task_id != self._task_id:
            raise ValueError("progress task_id does not match bound task")
        payload: dict[str, object] = {"message": body}
        if progress is not None:
            payload["progress"] = progress
        if extra is not None:
            payload.update(extra)
        envelope: dict[str, object] = {
            "v": 1,
            "id": self._uuid_factory.uuid4(),
            "type": "task.progress",
            "sender_id": self.agent_id,
            "recipient_id": self._recipient_id,
            "task_id": task_id,
            "context_id": self._context_id,
            "hop_count": self._hop_count,
            "task_state": "working",
            "timestamp": self._clock.now_iso(),
            "payload": _canonical_mapping(payload),
        }
        default_validator().validate_envelope(envelope)
        return await self.progress_publisher.publish_progress(envelope)


Handler = Callable[
    [Mapping[str, object], ExecutionContext],
    Awaitable[tuple[Mapping[str, object], str]],
]


@dataclass(frozen=True)
class ExecutionResult:
    classification: Classification
    terminal_envelope: Mapping[str, object] | None
    receipt: PublicationReceipt | None
    ledger_decision: LedgerDecision


class InjectedCrash(BaseException):
    """Unit-test control flow for named process-crash boundaries."""


class _DecodeFailure(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class _DuplicateKey(ValueError):
    pass


class _NestingTooDeep(ValueError):
    pass


def _reject_constant(_: str) -> None:
    raise _DecodeFailure("nonfinite_number")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey(key)
        value[key] = item
    return value


def _contains_nonfinite(value: object) -> bool:
    if isinstance(value, float):
        return not math.isfinite(value)
    if isinstance(value, Mapping):
        return any(_contains_nonfinite(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_nonfinite(item) for item in value)
    return False


def _require_bounded_nesting(value: object) -> None:
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        if isinstance(current, Mapping):
            child_depth = depth + 1
            if child_depth > _MAX_JSON_NESTING_DEPTH:
                raise _NestingTooDeep
            stack.extend((item, child_depth) for item in current.values())
        elif isinstance(current, (list, tuple)):
            child_depth = depth + 1
            if child_depth > _MAX_JSON_NESTING_DEPTH:
                raise _NestingTooDeep
            stack.extend((item, child_depth) for item in current)


def _detach_json_value(value: object, depth: int = 0) -> object:
    if isinstance(value, Mapping):
        child_depth = depth + 1
        if child_depth > _MAX_JSON_NESTING_DEPTH:
            raise _NestingTooDeep
        detached: dict[str, object] = {}
        for key, item in cast(Mapping[object, object], value).items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            detached[key] = _detach_json_value(item, child_depth)
        return detached
    if isinstance(value, (list, tuple)):
        child_depth = depth + 1
        if child_depth > _MAX_JSON_NESTING_DEPTH:
            raise _NestingTooDeep
        return [_detach_json_value(item, child_depth) for item in value]
    return value


def _canonical_mapping(value: Mapping[str, object]) -> dict[str, object]:
    detached = _detach_json_value(value)
    encoded = canonical_json(detached)
    decoded = cast(object, json.loads(encoded))
    if not isinstance(decoded, dict):
        raise TypeError("mapping canonicalization failed")
    return cast(dict[str, object], decoded)


def _utf8_text(value: object, *, allow_empty: bool) -> bool:
    if not isinstance(value, str) or (not allow_empty and not value):
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _decode(raw: bytes) -> dict[str, object]:
    if raw.startswith(b"\xef\xbb\xbf"):
        raise _DecodeFailure("utf8_bom")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _DecodeFailure("invalid_utf8") from None
    try:
        decoded = cast(
            object,
            json.loads(
                text,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            ),
        )
    except _DuplicateKey:
        raise _DecodeFailure("duplicate_key") from None
    except _DecodeFailure:
        raise
    except RecursionError:
        raise _DecodeFailure("nesting_too_deep") from None
    except (json.JSONDecodeError, TypeError, ValueError):
        raise _DecodeFailure("invalid_json") from None
    if not isinstance(decoded, dict):
        raise _DecodeFailure("non_object_root")
    try:
        _require_bounded_nesting(decoded)
    except _NestingTooDeep:
        raise _DecodeFailure("nesting_too_deep") from None
    if _contains_nonfinite(decoded):
        raise _DecodeFailure("nonfinite_number")
    try:
        return _canonical_mapping(cast(Mapping[str, object], decoded))
    except (_NestingTooDeep, RecursionError):
        raise _DecodeFailure("nesting_too_deep") from None
    except (TypeError, ValueError, UnicodeError):
        raise _DecodeFailure("canonicalization_failed") from None


def _receipt_is_well_formed(receipt: object, envelope_id: str) -> bool:
    if not isinstance(receipt, PublicationReceipt):
        return False
    return (
        receipt.envelope_id == envelope_id
        and type(receipt.accepted) is bool
        and _utf8_text(receipt.transport, allow_empty=False)
        and (receipt.stream is None or _utf8_text(receipt.stream, allow_empty=True))
        and (
            receipt.stream_sequence is None
            or (type(receipt.stream_sequence) is int and receipt.stream_sequence > 0)
        )
        and (receipt.duplicate is None or type(receipt.duplicate) is bool)
        and type(receipt.accepted_ns) is int
        and receipt.accepted_ns >= 0
        and type(receipt.application_bytes) is int
        and receipt.application_bytes >= 0
        and (
            receipt.wire_bytes is None
            or (type(receipt.wire_bytes) is int and receipt.wire_bytes >= 0)
        )
    )


def _terminal_classification(envelope: Mapping[str, object]) -> TerminalState:
    state = envelope.get("task_state")
    if state not in ("completed", "failed", "canceled", "rejected"):
        raise ValueError("cached terminal has invalid task state")
    return state


class TaskExecutor:
    def __init__(
        self,
        *,
        worker_agent_id: str,
        handler: Handler,
        outcome_store: OutcomeStore,
        terminal_publisher: TerminalPublisher,
        progress_publisher: ProgressPublisher,
        policy: ExecutionPolicy,
        event_sink: EventSink,
        clock: Clock,
        uuid_factory: UUIDFactory,
        crash_hook: CrashHook,
        nc: object | None = None,
        js: object | None = None,
    ) -> None:
        self._worker_agent_id = worker_agent_id
        self._handler = handler
        self._outcome_store = outcome_store
        self._terminal_publisher = terminal_publisher
        self._progress_publisher = progress_publisher
        self._policy = policy
        self._event_sink = event_sink
        self._clock = clock
        self._uuid_factory = uuid_factory
        self._crash_hook = crash_hook
        self._nc = nc
        self._js = js
        self._validator = default_validator()

    def _emit(self, event: str, data: Mapping[str, object]) -> None:
        self._event_sink.emit(
            {
                "monotonic_ns": self._clock.monotonic_ns(),
                "epoch_time": self._clock.now_iso(),
                "component": "task_executor",
                "event": event,
                "data": dict(data),
            }
        )

    def _emit_attempt(
        self,
        request: Mapping[str, object],
        fingerprint: str,
        delivery: InboundDelivery,
    ) -> None:
        self._emit(
            "task.request_attempt",
            {
                "worker_agent_id": self._worker_agent_id,
                "request_envelope_id": request["id"],
                "task_id": request["task_id"],
                "context_id": request["context_id"],
                "sender_id": request["sender_id"],
                "recipient_id": request["recipient_id"],
                "request_fingerprint": fingerprint,
                "delivery_count": delivery.delivery_count,
                "stream_sequence": delivery.stream_sequence,
            },
        )

    def _emit_decision(
        self,
        request: Mapping[str, object],
        decision: LedgerDecision,
    ) -> None:
        self._emit(
            "task.ledger_decision",
            {
                "worker_agent_id": self._worker_agent_id,
                "request_envelope_id": request["id"],
                "task_id": request["task_id"],
                "decision": decision,
            },
        )

    async def _poison(
        self,
        delivery: InboundDelivery,
        error_code: str,
    ) -> ExecutionResult:
        self._emit(
            "task.poison",
            {
                "worker_agent_id": self._worker_agent_id,
                "raw_sha256": hashlib.sha256(delivery.raw).hexdigest(),
                "error_code": error_code,
                "delivery_count": delivery.delivery_count,
                "stream_sequence": delivery.stream_sequence,
            },
        )
        await delivery.terminate()
        return ExecutionResult("poison", None, None, "not_applicable")

    def _normalize(self, decoded: Mapping[str, object]) -> dict[str, object]:
        correlated = normalize_task_correlation(decoded)
        request = dict(decoded)
        request.update(correlated)
        return _canonical_mapping(request)

    def _terminal(
        self,
        request: Mapping[str, object],
        state: TerminalState,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        detached_payload = _canonical_mapping(payload)
        if cast(int, request["hop_count"]) > 0:
            request_payload = cast(Mapping[str, object], request["payload"])
            detached_payload["parent_task_id"] = request_payload["parent_task_id"]
        else:
            detached_payload.pop("parent_task_id", None)
        terminal: dict[str, object] = {
            "v": 1,
            "id": self._uuid_factory.uuid4(),
            "type": "result",
            "sender_id": self._worker_agent_id,
            "recipient_id": request["sender_id"],
            "task_id": request["task_id"],
            "context_id": request["context_id"],
            "hop_count": request["hop_count"],
            "task_state": state,
            "timestamp": self._clock.now_iso(),
            "payload": detached_payload,
        }
        self._validator.validate_envelope(terminal)
        return _canonical_mapping(terminal)

    def _outcome(
        self,
        request: Mapping[str, object],
        fingerprint: str,
        terminal: Mapping[str, object],
    ) -> PreparedOutcome:
        payload = cast(Mapping[str, object], terminal["payload"])
        return PreparedOutcome(
            key=OutcomeKey(
                self._worker_agent_id,
                cast(str, request["task_id"]),
            ),
            sender_id=cast(str, request["sender_id"]),
            request_envelope_id=cast(str, request["id"]),
            request_fingerprint=fingerprint,
            terminal_envelope=terminal,
            terminal_payload_hash=hashlib.sha256(canonical_json(payload)).hexdigest(),
            publish_state="prepared",
            completed_at=cast(str, terminal["timestamp"]),
        )

    def _context(
        self,
        delivery: InboundDelivery,
        request: Mapping[str, object],
    ) -> ExecutionContext:
        context = ExecutionContext(
            agent_id=self._worker_agent_id,
            nc=self._nc,
            js=self._js,
            delivery=delivery,
            progress_publisher=self._progress_publisher,
        )
        context._bind(
            task_id=cast(str, request["task_id"]),
            recipient_id=cast(str, request["sender_id"]),
            context_id=cast(str, request["context_id"]),
            hop_count=cast(int, request["hop_count"]),
            clock=self._clock,
            uuid_factory=self._uuid_factory,
        )
        return context

    async def _publish(
        self,
        *,
        delivery: InboundDelivery,
        classification: TerminalState,
        terminal: Mapping[str, object],
        decision: LedgerDecision,
        key: OutcomeKey | None,
    ) -> ExecutionResult:
        pending = ExecutionResult(classification, terminal, None, decision)
        try:
            publication = await self._terminal_publisher.publish_terminal(terminal)
        except Exception:  # noqa: BLE001
            await delivery.retry()
            return pending
        terminal_id = cast(str, terminal["id"])
        if not _receipt_is_well_formed(publication, terminal_id):
            await delivery.retry()
            return pending
        result = replace(pending, receipt=publication)
        if not publication.accepted:
            await delivery.retry()
            return result

        self._crash_hook.hit("after-result-publish-before-publish-mark")
        if key is not None and self._outcome_store.enabled:
            try:
                self._outcome_store.mark_published(key, publication)
            except Exception:  # noqa: BLE001
                await delivery.retry()
                return result
            self._crash_hook.hit("after-publish-mark-before-inbound-commit")
        elif key is not None:
            self._crash_hook.hit("after-publish-mark-before-inbound-commit")
        await delivery.commit()
        return result

    async def _nonledger_terminal(
        self,
        *,
        delivery: InboundDelivery,
        request: Mapping[str, object],
        state: TerminalState,
        payload: Mapping[str, object],
        decision: LedgerDecision,
    ) -> ExecutionResult:
        terminal = self._terminal(request, state, payload)
        self._emit_decision(request, decision)
        return await self._publish(
            delivery=delivery,
            classification=state,
            terminal=terminal,
            decision=decision,
            key=None,
        )

    async def execute(self, delivery: InboundDelivery) -> ExecutionResult:
        if delivery.worker_agent_id != self._worker_agent_id:
            raise ValueError("delivery worker does not match executor worker")

        try:
            decoded = _decode(delivery.raw)
        except _DecodeFailure as exc:
            return await self._poison(delivery, exc.code)
        try:
            self._validator.validate_envelope(decoded)
        except RecursionError:
            return await self._poison(delivery, "nesting_too_deep")
        except ValidationError:
            return await self._poison(delivery, "invalid_envelope")
        if decoded.get("type") not in ("command", "delegation", "cancel"):
            return await self._poison(delivery, "unsupported_type")
        try:
            request = self._normalize(decoded)
        except (_NestingTooDeep, RecursionError):
            return await self._poison(delivery, "nesting_too_deep")
        except (TypeError, ValueError, UnicodeError, ValidationError):
            return await self._poison(delivery, "canonicalization_failed")

        request_type = request["type"]
        fingerprint: str | None = None
        if request_type in ("command", "delegation"):
            fingerprint = request_fingerprint(request)
            self._emit_attempt(request, fingerprint, delivery)

        if request["recipient_id"] != self._worker_agent_id:
            return await self._nonledger_terminal(
                delivery=delivery,
                request=request,
                state="rejected",
                payload={"error": "recipient_mismatch"},
                decision="not_applicable",
            )

        if request_type == "cancel":
            cancellation = self._policy.evaluate(request, self._worker_agent_id)
            cancel_state: TerminalState = (
                "canceled" if cancellation.accepted else "rejected"
            )
            cancel_payload: dict[str, object] = (
                {}
                if cancellation.accepted
                else {"error": cancellation.reason or "policy_rejected"}
            )
            return await self._nonledger_terminal(
                delivery=delivery,
                request=request,
                state=cancel_state,
                payload=cancel_payload,
                decision="not_applicable",
            )

        if fingerprint is None:
            raise AssertionError("executable request has no fingerprint")
        key = OutcomeKey(self._worker_agent_id, cast(str, request["task_id"]))
        try:
            cached = self._outcome_store.lookup(key)
        except Exception:  # noqa: BLE001
            self._emit_decision(request, "miss")
            await delivery.retry()
            return ExecutionResult("failed", None, None, "miss")

        if cached is not None:
            if (
                cached.sender_id != request["sender_id"]
                or cached.request_fingerprint != fingerprint
            ):
                return await self._nonledger_terminal(
                    delivery=delivery,
                    request=request,
                    state="rejected",
                    payload={"error": "task_id_collision"},
                    decision="collision",
                )
            try:
                prepared = self._outcome_store.prepare(
                    replace(
                        cached,
                        request_envelope_id=cast(str, request["id"]),
                    )
                )
                classification = _terminal_classification(prepared.terminal_envelope)
                self._validator.validate_envelope(
                    cast(dict[str, object], prepared.terminal_envelope)
                )
            except Exception:  # noqa: BLE001
                self._emit_decision(request, "hit")
                await delivery.retry()
                return ExecutionResult(
                    _terminal_classification(cached.terminal_envelope),
                    cached.terminal_envelope,
                    None,
                    "hit",
                )
            self._emit_decision(request, "hit")
            self._crash_hook.hit("after-ledger-prepare-before-result-publish")
            return await self._publish(
                delivery=delivery,
                classification=classification,
                terminal=prepared.terminal_envelope,
                decision="hit",
                key=key,
            )

        policy = self._policy.evaluate(request, self._worker_agent_id)
        state: TerminalState
        payload: dict[str, object]
        if policy.accepted:
            self._crash_hook.hit("after-receive-before-handler")
            context = self._context(delivery, request)
            try:
                handler_value = await self._handler(
                    _canonical_mapping(request),
                    context,
                )
            except Exception:  # noqa: BLE001
                self._crash_hook.hit("during-handler-exception-conversion")
                state = "failed"
                payload = {"error": "handler_failed"}
            else:
                try:
                    if not isinstance(handler_value, tuple):
                        raise TypeError("invalid handler return")
                    raw_payload, raw_state = handler_value
                    if raw_state not in (
                        "completed",
                        "failed",
                        "canceled",
                        "rejected",
                    ):
                        raise ValueError("invalid handler state")
                    payload = _canonical_mapping(raw_payload)
                    state = cast(TerminalState, raw_state)
                except (TypeError, ValueError, UnicodeError, RecursionError):
                    state = "failed"
                    payload = {"error": "handler_failed"}
        else:
            state = "rejected"
            payload = {"error": policy.reason or "policy_rejected"}

        terminal = self._terminal(request, state, payload)
        candidate = self._outcome(request, fingerprint, terminal)
        try:
            prepared = self._outcome_store.prepare(candidate)
            decision: LedgerDecision = (
                "miss" if self._outcome_store.enabled else "disabled"
            )
        except OutcomeConflict:
            try:
                winner = self._outcome_store.lookup(key)
            except Exception:  # noqa: BLE001
                winner = None
            if winner is None:
                self._emit_decision(request, "miss")
                await delivery.retry()
                return ExecutionResult(state, terminal, None, "miss")
            if (
                winner.sender_id != request["sender_id"]
                or winner.request_fingerprint != fingerprint
            ):
                return await self._nonledger_terminal(
                    delivery=delivery,
                    request=request,
                    state="rejected",
                    payload={"error": "task_id_collision"},
                    decision="collision",
                )
            try:
                prepared = self._outcome_store.prepare(
                    replace(
                        winner,
                        request_envelope_id=cast(str, request["id"]),
                    )
                )
                state = _terminal_classification(prepared.terminal_envelope)
                self._validator.validate_envelope(
                    cast(dict[str, object], prepared.terminal_envelope)
                )
            except Exception:  # noqa: BLE001
                self._emit_decision(request, "miss")
                await delivery.retry()
                return ExecutionResult(
                    _terminal_classification(winner.terminal_envelope),
                    winner.terminal_envelope,
                    None,
                    "miss",
                )
            decision = "hit"
        except Exception:  # noqa: BLE001
            self._emit_decision(request, "miss")
            await delivery.retry()
            return ExecutionResult(state, terminal, None, "miss")

        self._emit_decision(request, decision)
        self._crash_hook.hit("after-ledger-prepare-before-result-publish")
        return await self._publish(
            delivery=delivery,
            classification=state,
            terminal=prepared.terminal_envelope,
            decision=decision,
            key=key,
        )


__all__ = [
    "Clock",
    "CrashHook",
    "ExecutionContext",
    "ExecutionPolicy",
    "ExecutionResult",
    "Handler",
    "InboundDelivery",
    "InjectedCrash",
    "PolicyDecision",
    "TaskExecutor",
    "UUIDFactory",
]
