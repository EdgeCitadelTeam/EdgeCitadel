from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from importlib import import_module
from typing import Any, Protocol, cast

import pytest

from edgecitadel_plugin_runtime import validator as adapter_validator
from aggregator import validator as validator_module
from aggregator.validator import (
    EnvelopeValidator,
    ValidationError,
    canonical_json,
    normalize_task_correlation,
    request_fingerprint,
)

Envelope = dict[str, Any]


@pytest.fixture(scope="module")
def validator(envelope_schema_path, card_schema_path):
    return EnvelopeValidator(envelope_schema_path, card_schema_path)


def _env(**over: Any) -> Envelope:
    base: Envelope = {
        "v": 1,
        "id": "11111111-2222-4333-8444-555555555555",
        "type": "heartbeat",
        "sender_id": "shell-1",
        "timestamp": "2026-04-23T10:00:00.000Z",
        "payload": {},
    }
    base.update(over)
    return base


def test_accepts_valid(validator):
    validator.validate_envelope(_env())


def test_rejects_unknown_field(validator):
    with pytest.raises(ValidationError) as exc:
        validator.validate_envelope(_env(receiver_id="x"))
    assert "receiver_id" in str(exc.value) or "unexpected" in str(exc.value).lower()


def test_rejects_missing_type(validator):
    bad = _env()
    del bad["type"]
    with pytest.raises(ValidationError):
        validator.validate_envelope(bad)


def test_register_card_must_match_sender_id(validator):
    env = _env(
        type="register",
        sender_id="shell-1",
        payload={
            "name": "shell-1",
            "description": "x",
            "version": "0.1",
            "url": "nats://x",
            "provider": {"organization": "EC"},
            "capabilities": {},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["worker"],
                "runtime.conformance": "L1",
                "runtime.heartbeat_interval_sec": 30,
            },
        },
    )
    validator.validate_envelope(env)
    validator.validate_register(env)  # name == sender_id

    env["payload"]["name"] = "different"
    with pytest.raises(ValidationError, match="sender_id"):
        validator.validate_register(env)


DIRECT_TASK_ID = "899d8a29-8c6c-4fef-b491-1140d8371fef"
CHILD_TASK_ID = "70209f19-a984-47e3-8637-44428ebd8318"
CONTEXT_ID = "6e088543-c9de-4459-a0fe-2191d20dfba1"
PARENT_TASK_ID = "899d8a29-8c6c-4fef-b491-1140d8371fef"


def _command(**over: Any) -> Envelope:
    env = _env(
        type="command",
        sender_id="sender-1",
        recipient_id="worker-1",
        task_id=DIRECT_TASK_ID,
        payload={"command": "printf spine:nonce"},
    )
    env.update(over)
    return env


def _result(**over: Any) -> Envelope:
    env = _env(
        type="result",
        sender_id="worker-1",
        recipient_id="sender-1",
        task_id=DIRECT_TASK_ID,
        task_state="completed",
        payload={"body": "done"},
    )
    env.update(over)
    return env


def _delegation(**over: Any) -> Envelope:
    env = _env(
        type="delegation",
        sender_id="sender-1",
        recipient_id="worker-1",
        task_id=CHILD_TASK_ID,
        context_id=CONTEXT_ID,
        hop_count=1,
        payload={
            "command": "printf child:nonce",
            "parent_task_id": PARENT_TASK_ID,
        },
    )
    env.update(over)
    return env


class TestTaskCorrelation:
    @pytest.mark.parametrize("factory", [_command, _result])
    def test_direct_correlation_defaults_do_not_mutate_input(
        self,
        validator: EnvelopeValidator,
        factory: Callable[..., Envelope],
    ) -> None:
        env = factory()
        original = copy.deepcopy(env)

        validator.validate_envelope(env)
        correlated = normalize_task_correlation(env)

        assert correlated["context_id"] == env["task_id"]
        assert correlated["hop_count"] == 0
        assert env == original
        assert "context_id" not in env
        assert "hop_count" not in env

    def test_direct_result_correlation_preserves_explicit_context(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _result(context_id=CONTEXT_ID)

        validator.validate_envelope(env)
        correlated = normalize_task_correlation(env)

        assert correlated["context_id"] == CONTEXT_ID
        assert correlated["hop_count"] == 0

    def test_direct_cancel_correlation_uses_compatibility_defaults(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _env(
            type="cancel",
            sender_id="sender-1",
            recipient_id="worker-1",
            task_id=DIRECT_TASK_ID,
        )

        validator.validate_envelope(env)
        correlated = normalize_task_correlation(env)

        assert correlated["context_id"] == DIRECT_TASK_ID
        assert correlated["hop_count"] == 0

    @pytest.mark.parametrize(
        ("env", "missing"),
        [
            (
                {
                    key: value
                    for key, value in _delegation().items()
                    if key != "context_id"
                },
                "context_id",
            ),
            (
                {
                    key: value
                    for key, value in _delegation().items()
                    if key != "hop_count"
                },
                "hop_count",
            ),
            (
                _delegation(payload={"command": "missing parent"}),
                "parent_task_id",
            ),
            (
                _result(
                    context_id=CONTEXT_ID,
                    payload={"parent_task_id": PARENT_TASK_ID},
                ),
                "hop_count",
            ),
            (
                _result(hop_count=1),
                "parent_task_id",
            ),
        ],
    )
    def test_delegated_correlation_requires_explicit_fields(
        self, env: Envelope, missing: str
    ) -> None:
        with pytest.raises(ValidationError, match=missing):
            normalize_task_correlation(env)

    def test_delegated_result_correlation_accepts_explicit_fields(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _result(
            context_id=CONTEXT_ID,
            hop_count=1,
            payload={"body": "done", "parent_task_id": PARENT_TASK_ID},
        )

        validator.validate_envelope(env)

    def test_delegated_result_correlation_rejects_non_uuid4_parent(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _result(
            context_id=CONTEXT_ID,
            hop_count=1,
            payload={
                "body": "done",
                "parent_task_id": "899d8a29-8c6c-1fef-b491-1140d8371fef",
            },
        )

        with pytest.raises(ValidationError, match="task_correlation invalid"):
            validator.validate_envelope(env)

    def test_command_correlation_rejects_positive_hop(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _command(
            context_id=CONTEXT_ID,
            hop_count=1,
            payload={
                "command": "printf child:nonce",
                "parent_task_id": PARENT_TASK_ID,
            },
        )

        with pytest.raises(ValidationError, match="hop_count"):
            validator.validate_envelope(env)

    @pytest.mark.parametrize(
        "hop_count",
        [0.0, -0.0, False, True, 1.0],
        ids=["zero-float", "negative-zero-float", "false", "true", "one-float"],
    )
    def test_correlation_rejects_non_integer_runtime_hop_values(
        self, hop_count: object
    ) -> None:
        env = _command(hop_count=hop_count)

        with pytest.raises(ValidationError, match="hop_count"):
            normalize_task_correlation(env)
        with pytest.raises(ValidationError, match="hop_count"):
            request_fingerprint(env)

    @pytest.mark.parametrize(
        "env",
        [
            [],
            {"type": "command"},
            {
                "type": "command",
                "sender_id": "sender-1",
                "recipient_id": "worker-1",
                "task_id": DIRECT_TASK_ID,
                "payload": [],
            },
        ],
        ids=["non-mapping-envelope", "missing-fields", "non-mapping-payload"],
    )
    def test_correlation_normalization_fails_closed_for_malformed_input(
        self, env: object
    ) -> None:
        with pytest.raises(ValidationError, match="task_correlation invalid"):
            normalize_task_correlation(cast(Mapping[str, object], env))

    @pytest.mark.parametrize(
        "env",
        [
            _env(type="register"),
            _env(type="status", agent_state="online"),
            _env(type="heartbeat"),
            _env(type="log"),
            _env(type="broadcast"),
            _env(
                type="task.progress",
                task_id=DIRECT_TASK_ID,
                task_state="working",
            ),
        ],
        ids=["register", "status", "heartbeat", "log", "broadcast", "progress"],
    )
    def test_non_request_correlation_types_keep_base_validation(
        self, validator: EnvelopeValidator, env: Envelope
    ) -> None:
        validator.validate_envelope(env)

    def test_correlation_validation_error_is_stable(
        self, validator: EnvelopeValidator
    ) -> None:
        env = _delegation(
            payload={"parent_task_id": "not-a-uuid"},
        )

        with pytest.raises(ValidationError) as first:
            validator.validate_envelope(env)
        with pytest.raises(ValidationError) as second:
            validator.validate_envelope(env)

        assert str(first.value) == str(second.value)
        assert str(first.value) == (
            "task_correlation invalid: 'not-a-uuid' is not a 'uuid' "
            "at ['payload', 'parent_task_id']"
        )

    def test_canonical_json_correlation_is_mapping_order_independent(self) -> None:
        assert canonical_json({"b": 2, "a": 1}) == canonical_json({"a": 1, "b": 2})
        assert canonical_json({"text": "cafe\u0301"}) == (b'{"text":"cafe\xcc\x81"}')

    @pytest.mark.parametrize(
        "non_finite",
        [float("nan"), float("inf"), float("-inf")],
        ids=["nan", "positive-infinity", "negative-infinity"],
    )
    def test_canonical_json_correlation_rejects_non_finite_values(
        self, non_finite: float
    ) -> None:
        with pytest.raises(ValueError):
            canonical_json({"value": non_finite})

    def test_canonical_json_correlation_rejects_lone_surrogate(self) -> None:
        with pytest.raises(UnicodeEncodeError):
            canonical_json({"value": "\ud800"})

    def test_direct_correlation_fingerprints_match_explicit_defaults(self) -> None:
        implicit = _command()
        explicit = _command(context_id=DIRECT_TASK_ID, hop_count=0)

        assert request_fingerprint(implicit) == request_fingerprint(explicit)

    @pytest.mark.parametrize("envelope_type", ["cancel", "result", "heartbeat"])
    def test_request_correlation_fingerprint_rejects_non_executable_types(
        self, envelope_type: str
    ) -> None:
        with pytest.raises(ValidationError, match="command or delegation"):
            request_fingerprint(_command(type=envelope_type))

    def test_request_correlation_fingerprint_rejects_invalid_projection(self) -> None:
        with pytest.raises(ValidationError, match="task_correlation invalid"):
            request_fingerprint(_command(task_id=DIRECT_TASK_ID.upper()))

    @pytest.mark.parametrize(
        ("field", "replacement"),
        [
            ("sender_id", "sender-2"),
            ("recipient_id", "worker-2"),
            ("task_id", "0ca89e20-d62e-426d-9a45-70e1406cf615"),
            ("context_id", "5a63afca-b94b-414a-bd6b-d3a6b4b61c05"),
            ("hop_count", 2),
            (
                "payload",
                {
                    "command": "printf changed:nonce",
                    "parent_task_id": PARENT_TASK_ID,
                },
            ),
        ],
    )
    def test_request_correlation_fingerprint_includes_exact_projection_fields(
        self, field: str, replacement: Any
    ) -> None:
        env = _delegation()
        changed = copy.deepcopy(env)
        changed[field] = replacement

        assert request_fingerprint(env) != request_fingerprint(changed)

    def test_request_correlation_fingerprint_includes_type(self) -> None:
        correlated = normalize_task_correlation(_command())
        value = {
            field: correlated[field]
            for field in (
                "type",
                "sender_id",
                "recipient_id",
                "task_id",
                "context_id",
                "hop_count",
                "payload",
            )
        }

        changed_type_hash = hashlib.sha256(
            canonical_json({**value, "type": "delegation"})
        ).hexdigest()
        expected_hash = hashlib.sha256(canonical_json(value)).hexdigest()

        assert request_fingerprint(_command()) == expected_hash
        assert expected_hash != changed_type_hash

    def test_request_correlation_fingerprint_preserves_payload_number_types(
        self,
    ) -> None:
        assert request_fingerprint(
            _command(payload={"value": 1})
        ) != request_fingerprint(_command(payload={"value": 1.0}))

    def test_request_correlation_fingerprint_excludes_wire_metadata(self) -> None:
        env = _command()
        changed = {
            **env,
            "id": "f4a34c4c-2b37-4694-b648-8d5bbde8fc77",
            "timestamp": "2026-07-25T11:22:33.444Z",
        }

        assert request_fingerprint(env) == request_fingerprint(changed)

    def test_adapter_validator_reexports_correlation_api(self) -> None:
        for name in (
            "CORRELATED_TYPES",
            "normalize_task_correlation",
            "canonical_json",
            "request_fingerprint",
        ):
            assert getattr(adapter_validator, name) is getattr(validator_module, name)


Headers = dict[str, str] | None


class _ContextLike(Protocol):
    async def publish_progress(
        self,
        task_id: str,
        *,
        body: str = "",
        progress: int | None = None,
        extra: dict[str, object] | None = None,
    ) -> None: ...


class _ContextFactory(Protocol):
    def __call__(
        self,
        *,
        agent_id: str,
        nc: _CaptureNats,
        js: _CaptureJetStream,
        msg: object,
    ) -> _ContextLike: ...


class _PullConsumerLike(Protocol):
    async def _publish_result(
        self,
        inbound: Envelope,
        *,
        task_state: str,
        payload: Envelope | None = None,
        error: str | None = None,
    ) -> None: ...


Handler = Callable[[Envelope, object], Awaitable[tuple[Envelope, str]]]


class _PullConsumerFactory(Protocol):
    def __call__(
        self,
        *,
        agent_id: str,
        nc: _CaptureNats,
        handler: Handler,
    ) -> _PullConsumerLike: ...


def _legacy_producer_factories() -> tuple[_ContextFactory, _PullConsumerFactory]:
    module = import_module("edgecitadel_plugin_runtime.pull_consumer")
    return (
        cast(_ContextFactory, module.Context),
        cast(_PullConsumerFactory, module.PullConsumer),
    )


class _CaptureJetStream:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, Headers]] = []

    async def publish(self, subject: str, data: bytes, headers: Headers = None) -> None:
        self.published.append((subject, data, headers))


class _CaptureNats:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.js = _CaptureJetStream()

    def jetstream(self) -> _CaptureJetStream:
        return self.js

    async def publish(self, subject: str, data: bytes) -> None:
        self.published.append((subject, data))


class TestTaskCorrelationProducerCompatibility:
    @pytest.mark.asyncio
    async def test_progress_correlation_preserves_actual_producer_shape(
        self, validator: EnvelopeValidator
    ) -> None:
        nc = _CaptureNats()
        context_factory, _ = _legacy_producer_factories()
        context = context_factory(agent_id="worker-1", nc=nc, js=nc.js, msg=object())

        await context.publish_progress(
            DIRECT_TASK_ID,
            body="partial",
            progress=50,
            extra={"skill_id": "shell.exec"},
        )

        subject, data = nc.published[0]
        env = json.loads(data)
        assert subject == f"agents.worker-1.task_progress.{DIRECT_TASK_ID}"
        assert set(env) == {
            "v",
            "id",
            "type",
            "sender_id",
            "task_id",
            "task_state",
            "timestamp",
            "payload",
        }
        validator.validate_envelope(env)

    @pytest.mark.parametrize(
        "context_id",
        [None, CONTEXT_ID],
        ids=["implicit-context", "explicit-context"],
    )
    @pytest.mark.asyncio
    async def test_pull_result_correlation_preserves_actual_producer_shape(
        self, validator: EnvelopeValidator, context_id: str | None
    ) -> None:
        nc = _CaptureNats()

        async def handler(_env: Envelope, _context: object) -> tuple[Envelope, str]:
            return {"body": "done"}, "completed"

        _, pull_consumer_factory = _legacy_producer_factories()
        consumer = pull_consumer_factory(
            agent_id="worker-1",
            nc=nc,
            handler=handler,
        )
        inbound = _command()
        if context_id is not None:
            inbound["context_id"] = context_id
        await consumer._publish_result(
            inbound,
            task_state="completed",
            payload={"body": "done"},
        )

        subject, data, headers = nc.js.published[0]
        env = json.loads(data)
        assert subject == "agents.sender-1.inbox"
        assert headers == {"Nats-Msg-Id": env["id"]}
        expected_fields = {
            "v",
            "id",
            "type",
            "sender_id",
            "recipient_id",
            "task_id",
            "task_state",
            "timestamp",
            "payload",
        }
        if context_id is not None:
            expected_fields.add("context_id")
        assert set(env) == expected_fields
        validator.validate_envelope(env)
        correlated = normalize_task_correlation(env)
        assert correlated["context_id"] == (context_id or DIRECT_TASK_ID)
        assert correlated["hop_count"] == 0
