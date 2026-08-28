from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import FrozenInstanceError, fields, is_dataclass
from pathlib import Path
from typing import get_type_hints

import pytest

import edgecitadel_plugin_sdk
from edgecitadel_plugin_sdk import (
    AgentRuntime,
    KnowledgeRecord,
    KnowledgeStore,
    LifecycleHooks,
    LifecycleState,
    LifecycleTransition,
    RuntimeContext,
    SkillDescriptor,
    SkillProvider,
    Transport,
    TransportMessage,
)

PUBLIC_TYPES = {
    "AgentRuntime",
    "KnowledgeRecord",
    "KnowledgeStore",
    "LifecycleHooks",
    "LifecycleState",
    "LifecycleTransition",
    "RuntimeContext",
    "SkillDescriptor",
    "SkillProvider",
    "Transport",
    "TransportMessage",
}


def _public_methods(protocol: type[object]) -> set[str]:
    return {
        name
        for name, value in vars(protocol).items()
        if not name.startswith("_") and callable(value)
    }


def _parameters(method: Callable[..., object]) -> tuple[str, ...]:
    return tuple(inspect.signature(method).parameters)


def test_lifecycle_states_reserve_full_supervisor_vocabulary() -> None:
    assert [state.value for state in LifecycleState] == [
        "discovered",
        "validated",
        "installed",
        "starting",
        "ready",
        "draining",
        "stopped",
        "failed",
    ]


@pytest.mark.parametrize(
    "protocol",
    [AgentRuntime, SkillProvider, KnowledgeStore, Transport, LifecycleHooks],
)
def test_protocols_are_runtime_checkable(protocol: type[object]) -> None:
    assert vars(protocol)["_is_protocol"] is True
    assert vars(protocol)["_is_runtime_protocol"] is True


def test_agent_runtime_has_exact_async_surface() -> None:
    assert _public_methods(AgentRuntime) == {
        "initialize",
        "handle",
        "drain",
        "shutdown",
    }
    assert all(
        inspect.iscoroutinefunction(getattr(AgentRuntime, method))
        for method in _public_methods(AgentRuntime)
    )
    assert get_type_hints(AgentRuntime.initialize) == {
        "context": RuntimeContext,
        "return": type(None),
    }
    assert get_type_hints(AgentRuntime.handle) == {
        "message": Mapping[str, object],
        "return": Mapping[str, object],
    }
    assert _parameters(AgentRuntime.initialize) == ("self", "context")
    assert _parameters(AgentRuntime.handle) == ("self", "message")
    assert _parameters(AgentRuntime.drain) == ("self",)
    assert _parameters(AgentRuntime.shutdown) == ("self",)
    assert get_type_hints(AgentRuntime.drain) == {"return": type(None)}
    assert get_type_hints(AgentRuntime.shutdown) == {"return": type(None)}


def test_remaining_protocols_expose_only_the_reserved_methods() -> None:
    assert _public_methods(SkillProvider) == {"list_skills", "resolve"}
    assert _public_methods(KnowledgeStore) == {"read", "propose"}
    assert _public_methods(Transport) == {"register", "receive", "publish", "drain"}
    assert _public_methods(LifecycleHooks) == {
        "before_transition",
        "after_transition",
    }
    assert all(
        not inspect.iscoroutinefunction(getattr(SkillProvider, method))
        for method in _public_methods(SkillProvider)
    )
    assert all(
        inspect.iscoroutinefunction(getattr(KnowledgeStore, method))
        for method in _public_methods(KnowledgeStore)
    )
    assert all(
        inspect.iscoroutinefunction(getattr(Transport, method))
        for method in ("register", "publish", "drain")
    )
    assert not inspect.iscoroutinefunction(Transport.receive)
    assert all(
        inspect.iscoroutinefunction(getattr(LifecycleHooks, method))
        for method in _public_methods(LifecycleHooks)
    )


def test_protocol_signatures_use_only_portable_values() -> None:
    assert (
        get_type_hints(SkillProvider.list_skills)["return"]
        == tuple[SkillDescriptor, ...]
    )
    assert get_type_hints(SkillProvider.resolve) == {
        "skill_id": str,
        "return": SkillDescriptor | None,
    }
    assert get_type_hints(KnowledgeStore.read) == {
        "plugin_id": str,
        "skill_id": str,
        "skill_version": str,
        "namespace": str,
        "return": KnowledgeRecord | None,
    }
    assert get_type_hints(KnowledgeStore.propose) == {
        "record": KnowledgeRecord,
        "return": KnowledgeRecord,
    }
    assert get_type_hints(Transport.register) == {
        "agent_id": str,
        "return": type(None),
    }
    assert get_type_hints(Transport.receive) == {
        "agent_id": str,
        "return": AsyncIterator[TransportMessage],
    }
    assert get_type_hints(Transport.publish) == {
        "message": TransportMessage,
        "return": type(None),
    }
    assert get_type_hints(LifecycleHooks.before_transition) == {
        "transition": LifecycleTransition,
        "return": type(None),
    }
    assert get_type_hints(LifecycleHooks.after_transition) == {
        "transition": LifecycleTransition,
        "return": type(None),
    }
    assert _parameters(SkillProvider.list_skills) == ("self",)
    assert _parameters(SkillProvider.resolve) == ("self", "skill_id")
    assert _parameters(KnowledgeStore.read) == (
        "self",
        "plugin_id",
        "skill_id",
        "skill_version",
        "namespace",
    )
    assert _parameters(KnowledgeStore.propose) == ("self", "record")
    assert _parameters(Transport.register) == ("self", "agent_id")
    assert _parameters(Transport.receive) == ("self", "agent_id")
    assert _parameters(Transport.publish) == ("self", "message")
    assert _parameters(Transport.drain) == ("self",)
    assert _parameters(LifecycleHooks.before_transition) == ("self", "transition")
    assert _parameters(LifecycleHooks.after_transition) == ("self", "transition")


def test_value_records_are_frozen_and_preserve_their_fields() -> None:
    context = RuntimeContext(
        plugin_id="local.example",
        agent_id="example-agent",
        configuration={"mode": "test"},
        metadata={"trace": "abc"},
    )
    skill = SkillDescriptor(
        name="placeholder",
        description="Return a placeholder response.",
        skill_id="example.placeholder",
        version="0.1.0",
        execution_name="placeholder",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )
    record = KnowledgeRecord(
        plugin_id="local.example",
        skill_id="example.placeholder",
        skill_version="0.1.0",
        namespace="procedures/example",
        revision=1,
        content_hash="0" * 64,
        provenance=("task:123",),
    )
    message = TransportMessage(
        sender_id="sender",
        recipient_id=None,
        message_type="request",
        payload={"task": "123"},
    )
    transition = LifecycleTransition(
        plugin_id="local.example",
        previous_state=LifecycleState.STARTING,
        next_state=LifecycleState.READY,
        detail={"attempt": 1},
    )

    assert context.configuration == {"mode": "test"}
    assert skill.skill_id == "example.placeholder"
    assert record.provenance == ("task:123",)
    assert message.recipient_id is None
    assert transition.next_state is LifecycleState.READY

    for value in (context, skill, record, message, transition):
        assert is_dataclass(value)
        with pytest.raises(FrozenInstanceError):
            setattr(value, fields(value)[0].name, "changed")


def test_value_records_have_exact_typed_fields() -> None:
    expected: dict[type[object], dict[str, object]] = {
        RuntimeContext: {
            "plugin_id": str,
            "agent_id": str,
            "configuration": Mapping[str, object],
            "metadata": Mapping[str, object],
        },
        SkillDescriptor: {
            "name": str,
            "description": str,
            "skill_id": str,
            "version": str,
            "execution_name": str,
            "input_schema": Mapping[str, object],
            "output_schema": Mapping[str, object],
        },
        KnowledgeRecord: {
            "plugin_id": str,
            "skill_id": str,
            "skill_version": str,
            "namespace": str,
            "revision": int,
            "content_hash": str,
            "provenance": tuple[str, ...],
        },
        TransportMessage: {
            "sender_id": str,
            "recipient_id": str | None,
            "message_type": str,
            "payload": Mapping[str, object],
        },
        LifecycleTransition: {
            "plugin_id": str,
            "previous_state": LifecycleState,
            "next_state": LifecycleState,
            "detail": Mapping[str, object] | None,
        },
    }

    for record_type, expected_annotations in expected.items():
        assert [field.name for field in fields(record_type)] == list(  # type: ignore[arg-type]
            expected_annotations
        )
        assert get_type_hints(record_type) == expected_annotations


def test_mapping_fields_do_not_alias_mutable_inputs() -> None:
    configuration: dict[str, object] = {"mode": "test"}
    input_schema: dict[str, object] = {"type": "object"}
    payload: dict[str, object] = {"task": "123"}
    detail: dict[str, object] = {"attempt": 1}

    context = RuntimeContext("plugin", "agent", configuration, {})
    skill = SkillDescriptor(
        "name", "description", "skill.id", "1.0.0", "execute", input_schema, {}
    )
    message = TransportMessage("sender", None, "request", payload)
    transition = LifecycleTransition(
        "plugin", LifecycleState.STARTING, LifecycleState.READY, detail
    )
    configuration["mode"] = "changed"
    input_schema["type"] = "string"
    payload["task"] = "changed"
    detail["attempt"] = 2

    assert context.configuration == {"mode": "test"}
    assert skill.input_schema == {"type": "object"}
    assert message.payload == {"task": "123"}
    assert transition.detail == {"attempt": 1}

    for mapping in (
        context.configuration,
        skill.input_schema,
        message.payload,
        transition.detail,
    ):
        assert mapping is not None
        with pytest.raises(TypeError):
            mapping["new"] = "value"  # type: ignore[index]


class DummyRuntime:
    async def initialize(self, context: RuntimeContext) -> None: ...

    async def handle(self, message: Mapping[str, object]) -> Mapping[str, object]:
        return message

    async def drain(self) -> None: ...

    async def shutdown(self) -> None: ...


class DummySkillProvider:
    def list_skills(self) -> tuple[SkillDescriptor, ...]:
        return ()

    def resolve(self, skill_id: str) -> SkillDescriptor | None:
        return None


class DummyKnowledgeStore:
    async def read(
        self, plugin_id: str, skill_id: str, skill_version: str, namespace: str
    ) -> KnowledgeRecord | None:
        return None

    async def propose(self, record: KnowledgeRecord) -> KnowledgeRecord:
        return record


class DummyTransport:
    async def register(self, agent_id: str) -> None: ...

    def receive(self, agent_id: str) -> AsyncIterator[TransportMessage]:
        async def messages() -> AsyncIterator[TransportMessage]:
            if False:
                yield TransportMessage("sender", agent_id, "unused", {})

        return messages()

    async def publish(self, message: TransportMessage) -> None: ...

    async def drain(self) -> None: ...


class DummyLifecycleHooks:
    async def before_transition(self, transition: LifecycleTransition) -> None: ...

    async def after_transition(self, transition: LifecycleTransition) -> None: ...


@pytest.mark.parametrize(
    ("implementation", "protocol"),
    [
        (DummyRuntime(), AgentRuntime),
        (DummySkillProvider(), SkillProvider),
        (DummyKnowledgeStore(), KnowledgeStore),
        (DummyTransport(), Transport),
        (DummyLifecycleHooks(), LifecycleHooks),
    ],
)
def test_structural_implementations_satisfy_protocols(
    implementation: object, protocol: type[object]
) -> None:
    assert isinstance(implementation, protocol)


def test_receive_returns_an_async_iterator() -> None:
    messages = DummyTransport().receive("agent")
    assert isinstance(messages, AsyncIterator)


def test_package_reexports_the_complete_public_api() -> None:
    assert set(edgecitadel_plugin_sdk.__all__) == PUBLIC_TYPES
    for name in PUBLIC_TYPES:
        assert getattr(edgecitadel_plugin_sdk, name) is globals()[name]


def test_sdk_has_no_framework_or_infrastructure_dependencies() -> None:
    source_root = Path(edgecitadel_plugin_sdk.__file__).parent
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(source_root.glob("*.py"))
    ).lower()
    prohibited = ("pathlib", "yaml", "nats", "fastapi", "pydantic", "starlette")

    assert all(term not in source for term in prohibited)
