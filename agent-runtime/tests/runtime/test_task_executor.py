"""Transport-neutral task-executor contract tests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import FrozenInstanceError, fields, replace
from types import MappingProxyType
from typing import NoReturn, TypeVar, cast

import pytest

import edgecitadel_plugin_runtime.task_publisher as task_publisher_module
from edgecitadel_plugin_runtime.outcome_store import (
    OutcomeConflict,
    OutcomeKey,
    OutcomeStore,
    OutcomeStoreError,
    PreparedOutcome,
)
from edgecitadel_plugin_runtime.task_executor import (
    CrashHook,
    ExecutionContext,
    ExecutionPolicy,
    ExecutionResult,
    InboundDelivery,
    InjectedCrash,
    PolicyDecision,
    TaskExecutor,
)
from edgecitadel_plugin_runtime.task_publisher import (
    EventSink,
    ProgressPublisher,
    TerminalPublisher,
)
from edgecitadel_plugin_runtime.task_types import PublicationReceipt
from edgecitadel_plugin_runtime.validator import canonical_json, request_fingerprint

WIRE_1 = "10000000-0000-4000-8000-000000000001"
WIRE_2 = "10000000-0000-4000-8000-000000000002"
TASK_1 = "20000000-0000-4000-8000-000000000001"
CONTEXT_1 = "30000000-0000-4000-8000-000000000001"
PARENT_1 = "40000000-0000-4000-8000-000000000001"
PARENT_2 = "40000000-0000-4000-8000-000000000002"
TERMINAL_1 = "50000000-0000-4000-8000-000000000001"
TERMINAL_2 = "50000000-0000-4000-8000-000000000002"
TERMINAL_3 = "50000000-0000-4000-8000-000000000003"
PROGRESS_1 = "60000000-0000-4000-8000-000000000001"
NOW_1 = "2026-07-25T12:00:00.000Z"
NOW_2 = "2026-07-25T12:00:01.000Z"

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


CRASH_POINTS = (
    "after-receive-before-handler",
    "after-side-effect-before-ledger-prepare",
    "after-ledger-prepare-before-result-publish",
    "after-result-publish-before-publish-mark",
    "after-publish-mark-before-inbound-commit",
    "during-handler-exception-conversion",
)


def command(
    *,
    wire_id: str = WIRE_1,
    task_id: str = TASK_1,
    sender_id: str = "sender-1",
    recipient_id: str = "worker-1",
    payload: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return {
        "v": 1,
        "id": wire_id,
        "type": "command",
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "task_id": task_id,
        "timestamp": NOW_1,
        "payload": dict(payload or {"body": "work"}),
    }


def delegation(*, wire_id: str = WIRE_1) -> dict[str, object]:
    return {
        "v": 1,
        "id": wire_id,
        "type": "delegation",
        "sender_id": "sender-1",
        "recipient_id": "worker-1",
        "task_id": TASK_1,
        "context_id": CONTEXT_1,
        "hop_count": 2,
        "timestamp": NOW_1,
        "payload": {"body": "work", "parent_task_id": PARENT_1},
    }


def cancel() -> dict[str, object]:
    return {
        "v": 1,
        "id": WIRE_2,
        "type": "cancel",
        "sender_id": "sender-1",
        "recipient_id": "worker-1",
        "task_id": TASK_1,
        "timestamp": NOW_1,
        "payload": {},
    }


def delegated_cancel() -> dict[str, object]:
    envelope = cancel()
    envelope["context_id"] = CONTEXT_1
    envelope["hop_count"] = 2
    envelope["payload"] = {"parent_task_id": PARENT_1}
    return envelope


def encode(envelope: Mapping[str, object]) -> bytes:
    return json.dumps(envelope, ensure_ascii=False).encode()


def nested_list(depth: int) -> object:
    value: object = "leaf"
    for _ in range(depth):
        value = [value]
    return value


class FakeDelivery:
    def __init__(
        self,
        raw: bytes,
        *,
        worker_agent_id: str = "worker-1",
        delivery_count: int = 1,
        stream_sequence: int | None = 7,
    ) -> None:
        self.worker_agent_id = worker_agent_id
        self.raw = raw
        self.delivery_count = delivery_count
        self.stream_sequence = stream_sequence
        self.in_progress_count = 0
        self.commit_count = 0
        self.retry_count = 0
        self.terminate_count = 0
        self.retry_error: BaseException | None = None

    async def in_progress(self) -> None:
        self.in_progress_count += 1

    async def commit(self) -> None:
        self.commit_count += 1

    async def retry(self) -> None:
        self.retry_count += 1
        if self.retry_error is not None:
            raise self.retry_error

    async def terminate(self) -> None:
        self.terminate_count += 1


class FakePolicy:
    def __init__(self, accepted: bool = True, reason: str | None = None) -> None:
        self.decision = PolicyDecision(accepted, reason)
        self.calls: list[tuple[Mapping[str, object], str]] = []

    def evaluate(
        self,
        envelope: Mapping[str, object],
        worker_agent_id: str,
    ) -> PolicyDecision:
        self.calls.append((envelope, worker_agent_id))
        return self.decision


class FakeClock:
    def __init__(self, wall_times: list[str] | None = None) -> None:
        self.monotonic = 100
        self.wall_times = wall_times or [NOW_1, NOW_2] * 100

    def monotonic_ns(self) -> int:
        self.monotonic += 1
        return self.monotonic

    def now_iso(self) -> str:
        return self.wall_times.pop(0)


class FakeUUIDFactory:
    def __init__(self, *values: str) -> None:
        self.values = list(values or (TERMINAL_1, TERMINAL_2, TERMINAL_3))
        self.calls = 0

    def uuid4(self) -> str:
        self.calls += 1
        if not self.values:
            raise AssertionError("unexpected UUID request")
        return self.values.pop(0)


class FakeCrashHook:
    def __init__(self, point: str | None = None) -> None:
        self.point = point
        self.hits: list[str] = []

    def hit(self, point: str) -> None:
        self.hits.append(point)
        if point == self.point:
            raise InjectedCrash(point)


class FakeEventSink:
    def __init__(self) -> None:
        self.events: list[Mapping[str, object]] = []
        self.error: BaseException | None = None

    def emit(self, event: Mapping[str, object]) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)


class FakePublisher:
    def __init__(self) -> None:
        self.envelopes: list[Mapping[str, object]] = []
        self.results: list[object] = []
        self.error: BaseException | None = None

    async def publish_terminal(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.envelopes.append(envelope)
        if self.error is not None:
            raise self.error
        if self.results:
            return cast(PublicationReceipt, self.results.pop(0))
        return receipt(str(envelope["id"]))


class FakeProgressPublisher:
    def __init__(self) -> None:
        self.envelopes: list[Mapping[str, object]] = []

    async def publish_progress(
        self,
        envelope: Mapping[str, object],
    ) -> PublicationReceipt:
        self.envelopes.append(envelope)
        return receipt(str(envelope["id"]), transport="plain-nats")


class FakeStore:
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self.rows: dict[OutcomeKey, PreparedOutcome] = {}
        self.lookup_calls: list[OutcomeKey] = []
        self.prepare_calls: list[PreparedOutcome] = []
        self.mark_calls: list[tuple[OutcomeKey, PublicationReceipt]] = []
        self.lookup_error: BaseException | None = None
        self.prepare_error: BaseException | None = None
        self.mark_error: BaseException | None = None
        self.race_winner: PreparedOutcome | None = None
        self.race_resolution: str | None = None
        self._race_lookup_count = 0

    def lookup(self, key: OutcomeKey) -> PreparedOutcome | None:
        self.lookup_calls.append(key)
        self._race_lookup_count += 1
        if self._race_lookup_count > 1 and self.race_resolution == "absent":
            self.rows.clear()
        if self._race_lookup_count > 1 and self.race_resolution == "store_error":
            raise OutcomeStoreError("lookup")
        if self.lookup_error is not None:
            raise self.lookup_error
        return self.rows.get(key)

    def prepare(self, outcome: PreparedOutcome) -> PreparedOutcome:
        self.prepare_calls.append(outcome)
        if self.race_winner is not None:
            winner = self.race_winner
            self.race_winner = None
            self.rows[winner.key] = winner
            raise OutcomeConflict("race")
        if self.prepare_error is not None:
            raise self.prepare_error
        current = self.rows.get(outcome.key)
        if current is None:
            detached = replace(
                outcome,
                terminal_envelope=json.loads(canonical_json(outcome.terminal_envelope)),
            )
            if self.enabled:
                self.rows[outcome.key] = detached
            return detached
        if (
            current.sender_id != outcome.sender_id
            or current.request_fingerprint != outcome.request_fingerprint
            or current.terminal_envelope != outcome.terminal_envelope
        ):
            raise OutcomeConflict("conflict")
        return current

    def mark_published(
        self,
        key: OutcomeKey,
        publication_receipt: PublicationReceipt,
    ) -> PreparedOutcome:
        self.mark_calls.append((key, publication_receipt))
        if self.mark_error is not None:
            raise self.mark_error
        current = self.rows[key]
        if current.publish_state == "published":
            return current
        marked = replace(
            current,
            publish_state="published",
            receipt=publication_receipt,
        )
        self.rows[key] = marked
        return marked

    def close(self) -> None:
        return None


class FakeHandler:
    def __init__(
        self,
        result: object = ({"body": "done"}, "completed"),
        *,
        error: BaseException | None = None,
        crash_hook: FakeCrashHook | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.crash_hook = crash_hook
        self.calls = 0
        self.envelopes: list[Mapping[str, object]] = []
        self.contexts: list[ExecutionContext] = []
        self.side_effects = 0

    async def __call__(
        self,
        envelope: Mapping[str, object],
        context: ExecutionContext,
    ) -> tuple[Mapping[str, object], str]:
        self.calls += 1
        self.envelopes.append(envelope)
        self.contexts.append(context)
        self.side_effects += 1
        if self.crash_hook is not None:
            self.crash_hook.hit("after-side-effect-before-ledger-prepare")
        if self.error is not None:
            raise self.error
        return cast(tuple[Mapping[str, object], str], self.result)


class IterationFailureMapping(Mapping[str, object]):
    def __getitem__(self, key: str) -> object:
        if key == "body":
            return "unreachable"
        raise KeyError(key)

    def __iter__(self) -> NoReturn:
        raise RuntimeError("mapping iteration failed")

    def __len__(self) -> int:
        return 1


class MutatingHandler(FakeHandler):
    async def __call__(
        self,
        envelope: Mapping[str, object],
        context: ExecutionContext,
    ) -> tuple[Mapping[str, object], str]:
        self.calls += 1
        self.envelopes.append(envelope)
        self.contexts.append(context)
        payload = cast(dict[str, object], envelope["payload"])
        payload["body"] = "mutated-input"
        payload["parent_task_id"] = PARENT_2
        return ({"parent_task_id": PARENT_2, "body": "done"}, "completed")


def receipt(
    envelope_id: str,
    *,
    accepted: bool = True,
    transport: str = "jetstream",
    stream: str | None = "AGENT_INBOX",
    stream_sequence: int | None = 11,
    duplicate: bool | None = False,
    accepted_ns: int = 500,
    application_bytes: int = 128,
    wire_bytes: int | None = 192,
) -> PublicationReceipt:
    return PublicationReceipt(
        envelope_id=envelope_id,
        accepted=accepted,
        transport=transport,
        stream=stream,
        stream_sequence=stream_sequence,
        duplicate=duplicate,
        accepted_ns=accepted_ns,
        application_bytes=application_bytes,
        wire_bytes=wire_bytes,
    )


def prepared_outcome(
    envelope: Mapping[str, object],
    *,
    terminal_id: str = TERMINAL_1,
    sender_id: str | None = None,
    fingerprint: str | None = None,
) -> PreparedOutcome:
    request = dict(envelope)
    terminal = {
        "v": 1,
        "id": terminal_id,
        "type": "result",
        "sender_id": "worker-1",
        "recipient_id": request["sender_id"],
        "task_id": request["task_id"],
        "context_id": request.get("context_id", request["task_id"]),
        "hop_count": request.get("hop_count", 0),
        "task_state": "completed",
        "timestamp": NOW_1,
        "payload": {"body": "winner"},
    }
    if request["type"] == "delegation":
        cast(dict[str, object], terminal["payload"])["parent_task_id"] = cast(
            Mapping[str, object], request["payload"]
        )["parent_task_id"]
    return PreparedOutcome(
        key=OutcomeKey("worker-1", cast(str, request["task_id"])),
        sender_id=sender_id or cast(str, request["sender_id"]),
        request_envelope_id=cast(str, request["id"]),
        request_fingerprint=fingerprint or request_fingerprint(request),
        terminal_envelope=terminal,
        terminal_payload_hash=hashlib.sha256(
            canonical_json(terminal["payload"])
        ).hexdigest(),
        publish_state="prepared",
        completed_at=NOW_1,
    )


def make_executor(
    *,
    handler: FakeHandler | None = None,
    store: OutcomeStore | None = None,
    publisher: FakePublisher | None = None,
    progress_publisher: FakeProgressPublisher | None = None,
    policy: FakePolicy | None = None,
    sink: FakeEventSink | None = None,
    clock: FakeClock | None = None,
    uuids: FakeUUIDFactory | None = None,
    crash_hook: FakeCrashHook | None = None,
) -> tuple[
    TaskExecutor,
    FakeHandler,
    OutcomeStore,
    FakePublisher,
    FakeProgressPublisher,
    FakePolicy,
    FakeEventSink,
    FakeCrashHook,
]:
    actual_handler = handler or FakeHandler()
    actual_store = store or FakeStore()
    actual_publisher = publisher or FakePublisher()
    actual_progress = progress_publisher or FakeProgressPublisher()
    actual_policy = policy or FakePolicy()
    actual_sink = sink or FakeEventSink()
    actual_crash = crash_hook or FakeCrashHook()
    executor = TaskExecutor(
        worker_agent_id="worker-1",
        handler=actual_handler,
        outcome_store=actual_store,
        terminal_publisher=actual_publisher,
        progress_publisher=actual_progress,
        policy=actual_policy,
        event_sink=actual_sink,
        clock=clock or FakeClock(),
        uuid_factory=uuids or FakeUUIDFactory(),
        crash_hook=actual_crash,
        nc=object(),
        js=object(),
    )
    return (
        executor,
        actual_handler,
        actual_store,
        actual_publisher,
        actual_progress,
        actual_policy,
        actual_sink,
        actual_crash,
    )


def assert_protocol_assignments(
    delivery: InboundDelivery,
    terminal: TerminalPublisher,
    progress: ProgressPublisher,
    policy: ExecutionPolicy,
    sink: EventSink,
    crash: CrashHook,
) -> None:
    assert delivery is not None
    assert terminal is not None
    assert progress is not None
    assert policy is not None
    assert sink is not None
    assert crash is not None


def mutate_frozen(instance: object, name: str, value: object) -> None:
    setattr(instance, name, value)


def test_public_contract_is_exact_and_receipt_is_canonical() -> None:
    assert vars(task_publisher_module)["PublicationReceipt"] is PublicationReceipt
    assert not hasattr(task_publisher_module, "PublishReceipt")
    assert not hasattr(task_publisher_module, "TransportReceipt")
    assert [field.name for field in fields(PolicyDecision)] == ["accepted", "reason"]
    assert [
        field.name
        for field in fields(ExecutionContext)
        if not field.name.startswith("_")
    ] == [
        "agent_id",
        "nc",
        "js",
        "delivery",
        "progress_publisher",
    ]
    context_fields = {item.name: item for item in fields(ExecutionContext)}
    assert {
        "_task_id",
        "_recipient_id",
        "_context_id",
        "_hop_count",
        "_clock",
        "_uuid_factory",
    } <= context_fields.keys()
    for name in (
        "_task_id",
        "_recipient_id",
        "_context_id",
        "_hop_count",
        "_clock",
        "_uuid_factory",
    ):
        assert context_fields[name].init is False
        assert context_fields[name].repr is False
        assert context_fields[name].compare is False
    assert [field.name for field in fields(ExecutionResult)] == [
        "classification",
        "terminal_envelope",
        "receipt",
        "ledger_decision",
    ]
    assert issubclass(InjectedCrash, BaseException)
    assert not issubclass(InjectedCrash, Exception)
    with pytest.raises(FrozenInstanceError):
        mutate_frozen(PolicyDecision(True, None), "accepted", False)
    with pytest.raises(FrozenInstanceError):
        mutate_frozen(
            ExecutionResult("poison", None, None, "not_applicable"),
            "receipt",
            receipt(TERMINAL_1),
        )

    fakes = make_executor()
    delivery = FakeDelivery(encode(command()))
    assert_protocol_assignments(
        delivery,
        fakes[3],
        fakes[4],
        fakes[5],
        fakes[6],
        fakes[7],
    )


@async_test
async def test_first_execution_wire_replay_and_semantic_retry_reuse_outcome() -> None:
    publisher = FakePublisher()
    current_receipts = [
        receipt(TERMINAL_1, accepted_ns=500),
        receipt(TERMINAL_1, accepted_ns=501),
        receipt(TERMINAL_1, accepted_ns=502),
    ]
    publisher.results = list(current_receipts)
    executor, handler, store, _, _, _, sink, _ = make_executor(publisher=publisher)
    first_delivery = FakeDelivery(encode(command()))
    replay_delivery = FakeDelivery(encode(command()))
    semantic_delivery = FakeDelivery(encode(command(wire_id=WIRE_2)))

    first = await executor.execute(first_delivery)
    replay = await executor.execute(replay_delivery)
    semantic = await executor.execute(semantic_delivery)

    assert handler.calls == 1
    assert first.classification == replay.classification == semantic.classification
    assert first.ledger_decision == "miss"
    assert replay.ledger_decision == semantic.ledger_decision == "hit"
    assert [first.receipt, replay.receipt, semantic.receipt] == current_receipts
    assert first.terminal_envelope is not None
    assert replay.terminal_envelope is not None
    assert semantic.terminal_envelope is not None
    assert {
        first.terminal_envelope["id"],
        replay.terminal_envelope["id"],
        semantic.terminal_envelope["id"],
    } == {TERMINAL_1}
    assert [envelope["id"] for envelope in publisher.envelopes] == [
        TERMINAL_1,
        TERMINAL_1,
        TERMINAL_1,
    ]
    assert first_delivery.commit_count == 1
    assert replay_delivery.commit_count == 1
    assert semantic_delivery.commit_count == 1
    assert len(cast(FakeStore, store).prepare_calls) == 3
    assert [
        call.request_envelope_id for call in cast(FakeStore, store).prepare_calls
    ] == [WIRE_1, WIRE_1, WIRE_2]
    assert [event["event"] for event in sink.events].count("task.request_attempt") == 3
    assert [
        cast(Mapping[str, object], event["data"])["decision"]
        for event in sink.events
        if event["event"] == "task.ledger_decision"
    ] == ["miss", "hit", "hit"]
    stored = cast(FakeStore, store).rows[OutcomeKey("worker-1", TASK_1)]
    assert stored.receipt == current_receipts[0]
    assert stored.completed_at == stored.terminal_envelope["timestamp"]
    assert (
        stored.terminal_payload_hash
        == hashlib.sha256(
            canonical_json(stored.terminal_envelope["payload"])
        ).hexdigest()
    )


@async_test
@cases(
    "changed",
    [
        command(wire_id=WIRE_2, sender_id="sender-2"),
        command(wire_id=WIRE_2, payload={"body": "different"}),
    ],
)
async def test_task_id_collision_is_nonledger_and_does_not_leak_cached_payload(
    changed: Mapping[str, object],
) -> None:
    executor, handler, store, publisher, _, _, sink, crash = make_executor(
        uuids=FakeUUIDFactory(TERMINAL_1, TERMINAL_2)
    )
    await executor.execute(FakeDelivery(encode(command())))
    before = cast(FakeStore, store).rows.copy()

    result = await executor.execute(FakeDelivery(encode(changed)))

    assert handler.calls == 1
    assert result.classification == "rejected"
    assert result.ledger_decision == "collision"
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["id"] == TERMINAL_2
    assert result.terminal_envelope["payload"] == {"error": "task_id_collision"}
    assert "winner" not in canonical_json(result.terminal_envelope).decode()
    assert cast(FakeStore, store).rows == before
    assert len(cast(FakeStore, store).prepare_calls) == 1
    assert publisher.envelopes[-1] == result.terminal_envelope
    assert len(publisher.envelopes) == 2
    assert [
        cast(Mapping[str, object], event["data"])["decision"]
        for event in sink.events
        if event["event"] == "task.ledger_decision"
    ] == ["miss", "collision"]
    assert crash.hits.count("after-result-publish-before-publish-mark") == 2
    assert crash.hits.count("after-publish-mark-before-inbound-commit") == 1


@async_test
async def test_recipient_guard_precedes_policy_and_ledger_access() -> None:
    executor, handler, store, publisher, _, policy, sink, crash = make_executor()
    delivery = FakeDelivery(encode(command(recipient_id="other-worker")))

    result = await executor.execute(delivery)

    assert result.classification == "rejected"
    assert result.ledger_decision == "not_applicable"
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["payload"] == {"error": "recipient_mismatch"}
    assert handler.calls == 0
    assert policy.calls == []
    assert cast(FakeStore, store).lookup_calls == []
    assert cast(FakeStore, store).prepare_calls == []
    assert delivery.commit_count == 1
    assert [event["event"] for event in sink.events] == [
        "task.request_attempt",
        "task.ledger_decision",
    ]
    assert cast(Mapping[str, object], sink.events[-1]["data"])["decision"] == (
        "not_applicable"
    )
    assert publisher.envelopes == [result.terminal_envelope]
    assert crash.hits == ["after-result-publish-before-publish-mark"]


@async_test
async def test_delivery_worker_mismatch_fails_before_decoding_or_store_access() -> None:
    executor, _, store, publisher, _, _, sink, _ = make_executor()
    delivery = FakeDelivery(b"not-json", worker_agent_id="worker-2")

    with pytest.raises(ValueError, match="delivery worker"):
        await executor.execute(delivery)

    assert cast(FakeStore, store).lookup_calls == []
    assert publisher.envelopes == []
    assert sink.events == []
    assert delivery.terminate_count == 0


@async_test
@cases("reason", ["sender_denied", "capability_denied", "hop_limit_exceeded"])
async def test_policy_rejection_is_a_stable_ledgered_terminal(reason: str) -> None:
    policy = FakePolicy(False, reason)
    executor, handler, store, publisher, _, _, _, crash = make_executor(policy=policy)

    first = await executor.execute(FakeDelivery(encode(command())))
    replay = await executor.execute(FakeDelivery(encode(command(wire_id=WIRE_2))))

    assert handler.calls == 0
    assert first.classification == replay.classification == "rejected"
    assert first.ledger_decision == "miss"
    assert replay.ledger_decision == "hit"
    assert first.terminal_envelope == replay.terminal_envelope
    assert first.terminal_envelope is not None
    assert first.terminal_envelope["payload"] == {"error": reason}
    assert len(cast(FakeStore, store).rows) == 1
    assert len(publisher.envelopes) == 2
    assert "after-receive-before-handler" not in crash.hits


@async_test
@cases(
    ("accepted", "reason", "classification", "payload"),
    [
        (True, None, "canceled", {}),
        (
            False,
            "terminal_already_observed",
            "rejected",
            {"error": "terminal_already_observed"},
        ),
    ],
)
@cases("request_envelope", [cancel(), delegated_cancel()])
async def test_cancel_is_resolved_by_policy_without_fingerprint_or_ledger(
    accepted: bool,
    reason: str | None,
    classification: str,
    payload: Mapping[str, object],
    request_envelope: Mapping[str, object],
) -> None:
    policy = FakePolicy(accepted, reason)
    executor, handler, store, publisher, _, _, sink, crash = make_executor(
        policy=policy
    )
    delivery = FakeDelivery(encode(request_envelope))

    result = await executor.execute(delivery)

    assert result.classification == classification
    assert result.ledger_decision == "not_applicable"
    assert result.terminal_envelope is not None
    expected_payload = dict(payload)
    if cast(int, request_envelope.get("hop_count", 0)) > 0:
        expected_payload["parent_task_id"] = PARENT_1
    assert result.terminal_envelope["payload"] == expected_payload
    assert result.terminal_envelope["context_id"] == request_envelope.get(
        "context_id", TASK_1
    )
    assert result.terminal_envelope["hop_count"] == request_envelope.get("hop_count", 0)
    assert handler.calls == 0
    assert cast(FakeStore, store).lookup_calls == []
    assert cast(FakeStore, store).prepare_calls == []
    assert delivery.commit_count == 1
    assert publisher.envelopes == [result.terminal_envelope]
    assert [event["event"] for event in sink.events] == ["task.ledger_decision"]
    assert cast(Mapping[str, object], sink.events[0]["data"])["decision"] == (
        "not_applicable"
    )
    assert "after-result-publish-before-publish-mark" in crash.hits
    assert "after-publish-mark-before-inbound-commit" not in crash.hits


POISON_CASES = (
    (b"\xef\xbb\xbf{}", "utf8_bom"),
    (b"\xff", "invalid_utf8"),
    (b"{", "invalid_json"),
    (b'{"x":1,"x":2}', "duplicate_key"),
    (b'{"outer":{"x":1,"x":2}}', "duplicate_key"),
    (b'{"value":NaN}', "nonfinite_number"),
    (b'{"value":Infinity}', "nonfinite_number"),
    (b'{"value":1e400}', "nonfinite_number"),
    (b"[]", "non_object_root"),
    (b"{}", "invalid_envelope"),
    (
        (
            b'{"v":1,"id":"10000000-0000-4000-8000-000000000001",'
            b'"type":"command","sender_id":"sender-1","recipient_id":"worker-1",'
            b'"task_id":"20000000-0000-4000-8000-000000000001",'
            b'"timestamp":"2026-07-25T12:00:00.000Z",'
            b'"payload":{"body":"\\ud800"}}'
        ),
        "canonicalization_failed",
    ),
    (
        encode(
            {
                "v": 1,
                "id": TERMINAL_1,
                "type": "result",
                "sender_id": "worker-1",
                "recipient_id": "sender-1",
                "task_id": TASK_1,
                "task_state": "completed",
                "timestamp": NOW_1,
                "payload": {},
            }
        ),
        "unsupported_type",
    ),
)


@async_test
@cases(("raw", "error_code"), POISON_CASES)
async def test_poison_is_redacted_terminated_once_and_never_forged(
    raw: bytes,
    error_code: str,
) -> None:
    executor, handler, store, publisher, _, policy, sink, _ = make_executor()
    delivery = FakeDelivery(raw, delivery_count=3, stream_sequence=None)

    result = await executor.execute(delivery)

    assert result == ExecutionResult("poison", None, None, "not_applicable")
    assert delivery.terminate_count == 1
    assert delivery.commit_count == delivery.retry_count == 0
    assert handler.calls == 0
    assert policy.calls == []
    assert cast(FakeStore, store).lookup_calls == []
    assert publisher.envelopes == []
    assert len(sink.events) == 1
    event = sink.events[0]
    assert set(event) == {"monotonic_ns", "epoch_time", "component", "event", "data"}
    assert event["component"] == "task_executor"
    assert event["event"] == "task.poison"
    data = cast(Mapping[str, object], event["data"])
    assert set(data) == {
        "worker_agent_id",
        "raw_sha256",
        "error_code",
        "delivery_count",
        "stream_sequence",
    }
    assert data["error_code"] == error_code
    assert len(cast(str, data["raw_sha256"])) == 64
    assert raw not in tuple(data.values())
    assert "validation text" not in canonical_json(event).decode()


@async_test
async def test_excessive_request_nesting_is_redacted_poison() -> None:
    raw = encode(command(payload={"body": nested_list(129)}))
    executor, handler, store, publisher, _, policy, sink, _ = make_executor()
    delivery = FakeDelivery(raw)

    result = await executor.execute(delivery)

    assert result == ExecutionResult("poison", None, None, "not_applicable")
    assert delivery.terminate_count == 1
    assert delivery.commit_count == delivery.retry_count == 0
    assert handler.calls == 0
    assert policy.calls == []
    assert cast(FakeStore, store).lookup_calls == []
    assert publisher.envelopes == []
    assert len(sink.events) == 1
    event = sink.events[0]
    assert event["event"] == "task.poison"
    data = cast(Mapping[str, object], event["data"])
    assert data["error_code"] == "nesting_too_deep"
    assert raw not in tuple(data.values())


@async_test
@cases(
    ("handler_result", "handler_error", "classification", "payload"),
    [
        (
            ({"body": "explicit failure"}, "failed"),
            None,
            "failed",
            {"body": "explicit failure"},
        ),
        ("bad-return", None, "failed", {"error": "handler_failed"}),
        (
            [{"body": "list-return"}, "completed"],
            None,
            "failed",
            {"error": "handler_failed"},
        ),
        (
            ({1: "non-string-key"}, "completed"),
            None,
            "failed",
            {"error": "handler_failed"},
        ),
        (
            ({"body": float("nan")}, "completed"),
            None,
            "failed",
            {"error": "handler_failed"},
        ),
        (None, RuntimeError("private detail"), "failed", {"error": "handler_failed"}),
    ],
)
async def test_handler_states_and_contract_failures_are_deterministic(
    handler_result: object,
    handler_error: BaseException | None,
    classification: str,
    payload: Mapping[str, object],
) -> None:
    handler = FakeHandler(handler_result, error=handler_error)
    executor, _, store, _, _, _, _, _ = make_executor(handler=handler)

    result = await executor.execute(FakeDelivery(encode(command())))

    assert result.classification == classification
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["payload"] == payload
    assert "private detail" not in canonical_json(result.terminal_envelope).decode()
    assert len(cast(FakeStore, store).rows) == 1


@async_test
async def test_handler_mapping_iteration_failure_is_a_stable_ledgered_terminal() -> (
    None
):
    handler = FakeHandler((IterationFailureMapping(), "completed"))
    executor, _, store, publisher, _, _, sink, crash = make_executor(handler=handler)
    delivery = FakeDelivery(encode(command()))

    result = await executor.execute(delivery)

    assert result.classification == "failed"
    assert result.ledger_decision == "miss"
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["payload"] == {"error": "handler_failed"}
    assert result.receipt == receipt(TERMINAL_1)
    assert handler.calls == 1
    fake_store = cast(FakeStore, store)
    key = OutcomeKey("worker-1", TASK_1)
    assert len(fake_store.prepare_calls) == 1
    assert fake_store.prepare_calls[0].terminal_envelope == result.terminal_envelope
    assert fake_store.rows[key].terminal_envelope == result.terminal_envelope
    assert fake_store.rows[key].publish_state == "published"
    assert fake_store.mark_calls == [(key, result.receipt)]
    assert publisher.envelopes == [result.terminal_envelope]
    assert delivery.commit_count == 1
    assert delivery.retry_count == delivery.terminate_count == 0
    assert [event["event"] for event in sink.events] == [
        "task.request_attempt",
        "task.ledger_decision",
    ]
    assert cast(Mapping[str, object], sink.events[-1]["data"])["decision"] == "miss"
    assert crash.hits == [
        "after-receive-before-handler",
        "after-ledger-prepare-before-result-publish",
        "after-result-publish-before-publish-mark",
        "after-publish-mark-before-inbound-commit",
    ]


@async_test
async def test_excessive_handler_result_nesting_becomes_generic_failure() -> None:
    handler = FakeHandler(({"body": nested_list(129)}, "completed"))
    executor, _, store, publisher, _, _, _, _ = make_executor(handler=handler)
    delivery = FakeDelivery(encode(command()))

    result = await executor.execute(delivery)

    assert result.classification == "failed"
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["payload"] == {"error": "handler_failed"}
    assert len(cast(FakeStore, store).rows) == 1
    assert publisher.envelopes == [result.terminal_envelope]
    assert delivery.commit_count == 1
    assert delivery.retry_count == delivery.terminate_count == 0


@async_test
async def test_handler_mapping_contract_accepts_read_only_mapping() -> None:
    handler = FakeHandler(
        (
            MappingProxyType(
                {
                    "body": "done",
                    "metadata": MappingProxyType({"status": "ok"}),
                    "items": [MappingProxyType({"index": 1})],
                }
            ),
            "completed",
        )
    )
    executor, _, store, publisher, _, _, _, _ = make_executor(handler=handler)
    delivery = FakeDelivery(encode(command()))

    result = await executor.execute(delivery)

    assert result.classification == "completed"
    assert result.terminal_envelope is not None
    assert result.terminal_envelope["payload"] == {
        "body": "done",
        "metadata": {"status": "ok"},
        "items": [{"index": 1}],
    }
    assert len(cast(FakeStore, store).rows) == 1
    assert publisher.envelopes == [result.terminal_envelope]
    assert delivery.commit_count == 1


class ControlFlow(BaseException):
    pass


@async_test
async def test_handler_control_flow_base_exception_escapes() -> None:
    handler = FakeHandler(error=ControlFlow("stop"))
    executor, _, store, publisher, _, _, _, _ = make_executor(handler=handler)
    delivery = FakeDelivery(encode(command()))

    with pytest.raises(ControlFlow):
        await executor.execute(delivery)

    assert cast(FakeStore, store).prepare_calls == []
    assert publisher.envelopes == []
    assert delivery.commit_count == delivery.retry_count == 0


@async_test
@cases("request_envelope", [command(), delegation()])
async def test_terminal_direction_lineage_and_handler_input_are_normalized_and_detached(
    request_envelope: Mapping[str, object],
) -> None:
    handler = MutatingHandler()
    executor, _, store, _, _, _, _, _ = make_executor(handler=handler)
    raw = encode(request_envelope)

    result = await executor.execute(FakeDelivery(raw))

    assert result.terminal_envelope is not None
    terminal = result.terminal_envelope
    assert terminal["sender_id"] == "worker-1"
    assert terminal["recipient_id"] == request_envelope["sender_id"]
    assert terminal["task_id"] == request_envelope["task_id"]
    assert terminal["context_id"] == request_envelope.get(
        "context_id", request_envelope["task_id"]
    )
    assert terminal["hop_count"] == request_envelope.get("hop_count", 0)
    assert handler.envelopes[0]["context_id"] == request_envelope.get(
        "context_id", request_envelope["task_id"]
    )
    assert handler.envelopes[0]["hop_count"] == request_envelope.get("hop_count", 0)
    assert cast(Mapping[str, object], handler.envelopes[0]["payload"])["body"] == (
        "mutated-input"
    )
    assert cast(Mapping[str, object], request_envelope["payload"])["body"] == "work"
    if request_envelope["type"] == "delegation":
        assert (
            cast(Mapping[str, object], terminal["payload"])["parent_task_id"]
            == PARENT_1
        )
    else:
        assert "parent_task_id" not in cast(Mapping[str, object], terminal["payload"])
    assert encode(request_envelope) == raw
    stored = cast(FakeStore, store).rows[OutcomeKey("worker-1", TASK_1)]
    assert stored.request_fingerprint == request_fingerprint(request_envelope)


@async_test
async def test_events_have_exact_shape_order_and_no_raw_request() -> None:
    executor, _, _, _, _, _, sink, _ = make_executor()
    delivery = FakeDelivery(encode(command()), delivery_count=4, stream_sequence=9)

    await executor.execute(delivery)

    assert [event["event"] for event in sink.events] == [
        "task.request_attempt",
        "task.ledger_decision",
    ]
    for event in sink.events:
        assert set(event) == {
            "monotonic_ns",
            "epoch_time",
            "component",
            "event",
            "data",
        }
        assert type(event["monotonic_ns"]) is int
        assert event["component"] == "task_executor"
    assert [event["epoch_time"] for event in sink.events] == [NOW_1, NOW_1]
    attempt = cast(Mapping[str, object], sink.events[0]["data"])
    assert set(attempt) == {
        "worker_agent_id",
        "request_envelope_id",
        "task_id",
        "context_id",
        "sender_id",
        "recipient_id",
        "request_fingerprint",
        "delivery_count",
        "stream_sequence",
    }
    assert attempt["request_envelope_id"] == WIRE_1
    assert attempt["context_id"] == TASK_1
    assert attempt["delivery_count"] == 4
    assert attempt["stream_sequence"] == 9
    decision = cast(Mapping[str, object], sink.events[1]["data"])
    assert decision == {
        "worker_agent_id": "worker-1",
        "request_envelope_id": WIRE_1,
        "task_id": TASK_1,
        "decision": "miss",
    }
    assert encode(command()) not in canonical_json(sink.events)


@async_test
async def test_disabled_store_reexecutes_and_never_marks() -> None:
    store = FakeStore(enabled=False)
    executor, handler, _, publisher, _, _, sink, crash = make_executor(
        store=store,
        uuids=FakeUUIDFactory(TERMINAL_1, TERMINAL_2),
    )

    first = await executor.execute(FakeDelivery(encode(command())))
    replay = await executor.execute(FakeDelivery(encode(command())))

    assert handler.calls == 2
    assert first.ledger_decision == replay.ledger_decision == "disabled"
    assert first.terminal_envelope is not None
    assert replay.terminal_envelope is not None
    assert first.terminal_envelope["id"] == TERMINAL_1
    assert replay.terminal_envelope["id"] == TERMINAL_2
    assert len(publisher.envelopes) == 2
    assert len(store.prepare_calls) == 2
    assert store.mark_calls == []
    assert [
        cast(Mapping[str, object], event["data"])["decision"]
        for event in sink.events
        if event["event"] == "task.ledger_decision"
    ] == ["disabled", "disabled"]
    assert crash.hits.count("after-publish-mark-before-inbound-commit") == 2


@async_test
@cases(
    "mode",
    [
        "exception",
        "not_receipt",
        "mismatched",
        "invalid_accepted",
        "invalid_transport",
        "invalid_transport_unicode",
        "invalid_stream",
        "invalid_stream_unicode",
        "invalid_stream_sequence",
        "invalid_stream_sequence_type",
        "invalid_duplicate",
        "invalid_accepted_ns",
        "invalid_accepted_ns_type",
        "invalid_application_bytes",
        "invalid_application_bytes_type",
        "invalid_wire_bytes",
        "invalid_wire_bytes_type",
        "not_accepted",
        "mark_exception",
    ],
)
async def test_publication_and_mark_failures_retry_once_without_commit(
    mode: str,
) -> None:
    store = FakeStore()
    publisher = FakePublisher()
    if mode == "exception":
        publisher.error = RuntimeError("publish")
    elif mode == "not_receipt":
        publisher.results = [object()]
    elif mode == "mismatched":
        publisher.results = [receipt(TERMINAL_2)]
    elif mode == "invalid_accepted":
        publisher.results = [replace(receipt(TERMINAL_1), accepted=cast(bool, "yes"))]
    elif mode == "invalid_transport":
        publisher.results = [replace(receipt(TERMINAL_1), transport="")]
    elif mode == "invalid_transport_unicode":
        publisher.results = [replace(receipt(TERMINAL_1), transport="\ud800")]
    elif mode == "invalid_stream":
        publisher.results = [replace(receipt(TERMINAL_1), stream=cast(str | None, 7))]
    elif mode == "invalid_stream_unicode":
        publisher.results = [replace(receipt(TERMINAL_1), stream="\ud800")]
    elif mode == "invalid_stream_sequence":
        publisher.results = [replace(receipt(TERMINAL_1), stream_sequence=0)]
    elif mode == "invalid_stream_sequence_type":
        publisher.results = [
            replace(receipt(TERMINAL_1), stream_sequence=cast(int | None, True))
        ]
    elif mode == "invalid_duplicate":
        publisher.results = [
            replace(receipt(TERMINAL_1), duplicate=cast(bool | None, "no"))
        ]
    elif mode == "invalid_accepted_ns":
        publisher.results = [replace(receipt(TERMINAL_1), accepted_ns=-1)]
    elif mode == "invalid_accepted_ns_type":
        publisher.results = [replace(receipt(TERMINAL_1), accepted_ns=cast(int, True))]
    elif mode == "invalid_application_bytes":
        publisher.results = [replace(receipt(TERMINAL_1), application_bytes=-1)]
    elif mode == "invalid_application_bytes_type":
        publisher.results = [
            replace(receipt(TERMINAL_1), application_bytes=cast(int, True))
        ]
    elif mode == "invalid_wire_bytes":
        publisher.results = [replace(receipt(TERMINAL_1), wire_bytes=-1)]
    elif mode == "invalid_wire_bytes_type":
        publisher.results = [
            replace(receipt(TERMINAL_1), wire_bytes=cast(int | None, True))
        ]
    elif mode == "not_accepted":
        publisher.results = [receipt(TERMINAL_1, accepted=False)]
    else:
        store.mark_error = OutcomeStoreError("mark")
    executor, _, _, _, _, _, _, _ = make_executor(store=store, publisher=publisher)
    delivery = FakeDelivery(encode(command()))

    result = await executor.execute(delivery)

    assert delivery.retry_count == 1
    assert delivery.commit_count == 0
    terminal = publisher.envelopes[0]
    if mode in {"not_accepted", "mark_exception"}:
        assert isinstance(result.receipt, PublicationReceipt)
        expected = ExecutionResult("completed", terminal, result.receipt, "miss")
    else:
        expected = ExecutionResult("completed", terminal, None, "miss")
    assert result == expected
    if mode == "not_accepted":
        assert result.receipt is not None and result.receipt.accepted is False
    if mode == "mark_exception":
        assert result.receipt is not None and result.receipt.accepted is True
    if mode == "mark_exception":
        assert store.mark_calls == [
            (OutcomeKey("worker-1", TASK_1), receipt(TERMINAL_1))
        ]
    else:
        assert store.mark_calls == []


@async_test
async def test_failing_retry_and_event_sink_failure_escape() -> None:
    publisher = FakePublisher()
    publisher.error = RuntimeError("publish")
    executor, _, _, _, _, _, _, _ = make_executor(publisher=publisher)
    delivery = FakeDelivery(encode(command()))
    delivery.retry_error = ControlFlow("retry failed")
    with pytest.raises(ControlFlow, match="retry failed"):
        await executor.execute(delivery)
    assert delivery.retry_count == 1

    sink = FakeEventSink()
    sink.error = ControlFlow("sink failed")
    executor, _, store, _, _, _, _, _ = make_executor(sink=sink)
    delivery = FakeDelivery(encode(command()))
    with pytest.raises(ControlFlow, match="sink failed"):
        await executor.execute(delivery)
    assert cast(FakeStore, store).lookup_calls == []
    assert delivery.retry_count == delivery.commit_count == 0


@async_test
@cases("failure_point", ["lookup", "prepare"])
async def test_initial_store_failures_retry_once_without_publication(
    failure_point: str,
) -> None:
    store = FakeStore()
    if failure_point == "lookup":
        store.lookup_error = OutcomeStoreError("lookup")
    else:
        store.prepare_error = OutcomeStoreError("prepare")
    executor, handler, _, publisher, _, _, sink, _ = make_executor(store=store)
    delivery = FakeDelivery(encode(command()))

    result = await executor.execute(delivery)

    assert result.ledger_decision == "miss"
    assert result.receipt is None
    assert publisher.envelopes == []
    assert delivery.retry_count == 1
    assert delivery.commit_count == 0
    assert handler.calls == (0 if failure_point == "lookup" else 1)
    assert [
        cast(Mapping[str, object], event["data"])["decision"]
        for event in sink.events
        if event["event"] == "task.ledger_decision"
    ] == ["miss"]


@async_test
@cases("resolution", ["hit", "collision", "absent", "store_error"])
async def test_prepare_race_recovers_the_final_winner(resolution: str) -> None:
    request_envelope = command(wire_id=WIRE_2)
    winner = prepared_outcome(command(wire_id=WIRE_1))
    if resolution == "collision":
        winner = replace(winner, sender_id="sender-2")
    store = FakeStore()
    store.race_winner = winner
    if resolution in {"absent", "store_error"}:
        store.race_resolution = resolution

    executor, handler, _, publisher, _, _, sink, _ = make_executor(
        store=store,
        uuids=FakeUUIDFactory(TERMINAL_2, TERMINAL_3),
    )
    delivery = FakeDelivery(encode(request_envelope))

    result = await executor.execute(delivery)

    assert handler.calls == 1
    decisions = [
        cast(Mapping[str, object], event["data"])["decision"]
        for event in sink.events
        if event["event"] == "task.ledger_decision"
    ]
    if resolution == "hit":
        assert result.ledger_decision == "hit"
        assert result.terminal_envelope == winner.terminal_envelope
        assert publisher.envelopes == [winner.terminal_envelope]
        assert delivery.commit_count == 1
        assert decisions == ["hit"]
        assert len(store.prepare_calls) == 2
        assert store.prepare_calls[-1].request_envelope_id == WIRE_2
    elif resolution == "collision":
        assert result.ledger_decision == "collision"
        assert result.terminal_envelope is not None
        assert result.terminal_envelope["payload"] == {"error": "task_id_collision"}
        assert publisher.envelopes == [result.terminal_envelope]
        assert decisions == ["collision"]
    else:
        assert result.ledger_decision == "miss"
        assert result.receipt is None
        assert publisher.envelopes == []
        assert delivery.retry_count == 1
        assert delivery.commit_count == 0
        assert decisions == ["miss"]


@async_test
@cases("point", CRASH_POINTS)
async def test_crash_boundaries_and_reentry(point: str) -> None:
    crash = FakeCrashHook(point)
    handler_error = (
        RuntimeError("handler")
        if point == "during-handler-exception-conversion"
        else None
    )
    handler = FakeHandler(error=handler_error, crash_hook=crash)
    executor, _, store, publisher, _, _, _, _ = make_executor(
        handler=handler,
        crash_hook=crash,
        uuids=FakeUUIDFactory(TERMINAL_1, TERMINAL_2, TERMINAL_3),
    )
    first_delivery = FakeDelivery(encode(command()))

    with pytest.raises(InjectedCrash, match=point):
        await executor.execute(first_delivery)

    fake_store = cast(FakeStore, store)
    before = (
        handler.calls,
        len(fake_store.rows),
        len(fake_store.prepare_calls),
        len(publisher.envelopes),
        len(fake_store.mark_calls),
    )
    expected_before = {
        "after-receive-before-handler": (0, 0, 0, 0, 0),
        "after-side-effect-before-ledger-prepare": (1, 0, 0, 0, 0),
        "after-ledger-prepare-before-result-publish": (1, 1, 1, 0, 0),
        "after-result-publish-before-publish-mark": (1, 1, 1, 1, 0),
        "after-publish-mark-before-inbound-commit": (1, 1, 1, 1, 1),
        "during-handler-exception-conversion": (1, 0, 0, 0, 0),
    }
    assert before == expected_before[point]
    assert first_delivery.commit_count == 0
    assert first_delivery.retry_count == first_delivery.terminate_count == 0
    crash.point = None
    replay_delivery = FakeDelivery(encode(command()))
    result = await executor.execute(replay_delivery)
    assert replay_delivery.commit_count == 1
    assert result.terminal_envelope is not None
    after = (
        handler.calls,
        len(fake_store.rows),
        len(fake_store.prepare_calls),
        len(publisher.envelopes),
        len(fake_store.mark_calls),
    )
    expected_after = {
        "after-receive-before-handler": (1, 1, 1, 1, 1),
        "after-side-effect-before-ledger-prepare": (2, 1, 1, 1, 1),
        "after-ledger-prepare-before-result-publish": (1, 1, 2, 1, 1),
        "after-result-publish-before-publish-mark": (1, 1, 2, 2, 1),
        "after-publish-mark-before-inbound-commit": (1, 1, 2, 2, 2),
        "during-handler-exception-conversion": (2, 1, 1, 1, 1),
    }
    assert after == expected_after[point]
    assert result.terminal_envelope["id"] == TERMINAL_1


@async_test
async def test_execution_context_progress_is_deterministic_bound_and_injected() -> None:
    progress = FakeProgressPublisher()
    clock = FakeClock([NOW_1, NOW_1, NOW_2, NOW_2])
    executor, handler, _, _, _, _, _, _ = make_executor(
        progress_publisher=progress,
        uuids=FakeUUIDFactory(TERMINAL_1, PROGRESS_1),
        clock=clock,
    )
    delivery = FakeDelivery(encode(delegation()))

    terminal_result = await executor.execute(delivery)
    assert terminal_result.terminal_envelope is not None
    assert terminal_result.terminal_envelope["id"] == TERMINAL_1
    assert terminal_result.terminal_envelope["timestamp"] == NOW_1
    context = handler.contexts[0]
    await context.in_progress()
    publication = await context.publish_progress(
        TASK_1,
        body="phase",
        progress=50,
        extra={"progress": 51, "detail": "ok"},
    )

    assert delivery.in_progress_count == 1
    assert publication.envelope_id == PROGRESS_1
    assert progress.envelopes == [
        {
            "v": 1,
            "id": PROGRESS_1,
            "type": "task.progress",
            "sender_id": "worker-1",
            "recipient_id": "sender-1",
            "task_id": TASK_1,
            "context_id": CONTEXT_1,
            "hop_count": 2,
            "task_state": "working",
            "timestamp": NOW_2,
            "payload": {
                "message": "phase",
                "progress": 51,
                "detail": "ok",
            },
        }
    ]
    with pytest.raises(ValueError, match="bound task"):
        await context.publish_progress(PARENT_1)
