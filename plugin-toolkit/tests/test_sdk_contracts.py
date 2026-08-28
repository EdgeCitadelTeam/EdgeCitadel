from __future__ import annotations

import ast
import inspect
import json
import tomllib
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import FrozenInstanceError, asdict, fields, is_dataclass
from pathlib import Path
from typing import cast, get_type_hints

import pytest
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

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
    assert _public_methods(SkillProvider) == {"list_skills", "resolve_by_name"}
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
    assert get_type_hints(SkillProvider.resolve_by_name) == {
        "name": str,
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
    assert _parameters(SkillProvider.resolve_by_name) == ("self", "name")
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
        v=1,
        id="message-id",
        type="command",
        sender_id="sender",
        timestamp="2026-08-28T12:00:00.000Z",
        payload={"task": "123"},
        recipient_id=None,
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
    assert (
        message.recipient_id,
        message.task_id,
        message.context_id,
        message.task_state,
        message.agent_state,
        message.hop_count,
    ) == (None, None, None, None, None, None)
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
            "v": int,
            "id": str,
            "type": str,
            "sender_id": str,
            "timestamp": str,
            "payload": Mapping[str, object],
            "recipient_id": str | None,
            "task_id": str | None,
            "context_id": str | None,
            "task_state": str | None,
            "agent_state": str | None,
            "hop_count": int | None,
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


def _assert_deeply_frozen(
    frozen_mapping: Mapping[str, object], caller_mapping: dict[str, object]
) -> None:
    caller_nested = cast(dict[str, object], caller_mapping["nested"])
    caller_items = cast(list[object], caller_nested["items"])
    caller_item = cast(dict[str, object], caller_items[0])
    caller_mapping.clear()
    caller_item["value"] = "caller-mutated"
    caller_items.append({"value": "caller-added"})

    frozen_nested = cast(Mapping[str, object], frozen_mapping["nested"])
    frozen_items = cast(tuple[object, ...], frozen_nested["items"])
    frozen_item = cast(Mapping[str, object], frozen_items[0])
    assert frozen_item["value"] == "original"
    assert len(frozen_items) == 1
    with pytest.raises(TypeError):
        frozen_mapping["new"] = "value"  # type: ignore[index]
    with pytest.raises(TypeError):
        frozen_item["value"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        frozen_items[0] = "changed"  # type: ignore[index]
    mutable_view = cast(dict[str, object], frozen_mapping)
    mutators: tuple[Callable[[], object], ...] = (
        mutable_view.clear,
        lambda: mutable_view.pop("nested"),
        mutable_view.popitem,
        lambda: mutable_view.setdefault("new", "value"),
        lambda: mutable_view.update({"new": "value"}),
        lambda: mutable_view.__delitem__("nested"),
    )
    for mutate in mutators:
        with pytest.raises(TypeError):
            mutate()


def test_runtime_context_mapping_is_deeply_immutable_and_unaliased() -> None:
    configuration: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    metadata: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    context = RuntimeContext("plugin", "agent", configuration, metadata)

    _assert_deeply_frozen(context.configuration, configuration)
    _assert_deeply_frozen(context.metadata, metadata)


def test_skill_descriptor_mapping_is_deeply_immutable_and_unaliased() -> None:
    input_schema: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    output_schema: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    skill = SkillDescriptor(
        "name",
        "description",
        "skill.id",
        "1.0.0",
        "execute",
        input_schema,
        output_schema,
    )

    _assert_deeply_frozen(skill.input_schema, input_schema)
    _assert_deeply_frozen(skill.output_schema, output_schema)


def test_lifecycle_detail_mapping_is_deeply_immutable_and_unaliased() -> None:
    detail: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    transition = LifecycleTransition(
        "plugin", LifecycleState.STARTING, LifecycleState.READY, detail
    )

    assert transition.detail is not None
    _assert_deeply_frozen(transition.detail, detail)


def test_transport_payload_mapping_is_deeply_immutable_and_unaliased() -> None:
    payload: dict[str, object] = {"nested": {"items": [{"value": "original"}]}}
    message = TransportMessage(
        1,
        "message-id",
        "command",
        "sender",
        "2026-08-28T12:00:00.000Z",
        payload,
    )

    _assert_deeply_frozen(message.payload, payload)


def test_value_records_round_trip_through_dataclass_json_shapes() -> None:
    documents = [
        json.loads(
            json.dumps(
                asdict(
                    RuntimeContext(
                        "plugin",
                        "agent",
                        {"nested": {"items": ["one", "two"]}},
                        {"labels": ["local"]},
                    )
                )
            )
        ),
        json.loads(
            json.dumps(
                asdict(
                    SkillDescriptor(
                        "name",
                        "description",
                        "skill.id",
                        "1.0.0",
                        "execute",
                        {"type": "object", "required": ["query"]},
                        {"type": "object"},
                    )
                )
            )
        ),
        json.loads(
            json.dumps(
                asdict(
                    KnowledgeRecord(
                        "plugin",
                        "skill.id",
                        "1.0.0",
                        "namespace",
                        1,
                        "0" * 64,
                        ("task:1",),
                    )
                )
            )
        ),
        json.loads(
            json.dumps(
                asdict(
                    TransportMessage(
                        1,
                        "message-id",
                        "command",
                        "sender",
                        "2026-08-28T12:00:00.000Z",
                        {"args": {"items": [1, 2]}},
                        recipient_id="recipient",
                        task_id="task-id",
                    )
                )
            )
        ),
        json.loads(
            json.dumps(
                asdict(
                    LifecycleTransition(
                        "plugin",
                        LifecycleState.STARTING,
                        LifecycleState.READY,
                        {"history": [{"attempt": 1}]},
                    )
                )
            )
        ),
    ]

    assert documents[0]["configuration"]["nested"]["items"] == ["one", "two"]
    assert documents[1]["input_schema"]["required"] == ["query"]
    assert documents[2]["provenance"] == ["task:1"]
    assert documents[3]["type"] == "command"
    assert "message_type" not in documents[3]
    assert documents[3]["payload"]["args"]["items"] == [1, 2]
    assert documents[4]["detail"]["history"] == [{"attempt": 1}]


def test_transport_message_serializes_a_canonical_command_envelope() -> None:
    message = TransportMessage(
        v=1,
        id="123e4567-e89b-42d3-a456-426614174000",
        type="command",
        sender_id="sender",
        timestamp="2026-08-28T12:00:00.000Z",
        payload={"args": {"items": [{"value": "original"}]}},
        recipient_id="recipient",
        task_id="123e4567-e89b-42d3-b456-426614174001",
    )
    schema_path = Path(__file__).parents[2] / "schemas" / "envelope.v1.json"
    validator = Draft202012Validator(
        json.loads(schema_path.read_text(encoding="utf-8"))
    )

    dataclass_mapping = asdict(message)
    assert dataclass_mapping["context_id"] is None
    assert dataclass_mapping["task_state"] is None
    assert dataclass_mapping["agent_state"] is None
    assert dataclass_mapping["hop_count"] is None
    assert list(validator.iter_errors(dataclass_mapping))

    wire_mapping = message.to_mapping()

    assert set(wire_mapping) == {
        "v",
        "id",
        "type",
        "sender_id",
        "timestamp",
        "payload",
        "recipient_id",
        "task_id",
    }
    validator.validate(wire_mapping)
    assert json.loads(json.dumps(wire_mapping)) == wire_mapping
    wire_payload = cast(dict[str, object], wire_mapping["payload"])
    wire_args = cast(dict[str, object], wire_payload["args"])
    wire_items = cast(list[object], wire_args["items"])
    wire_item = cast(dict[str, object], wire_items[0])
    assert type(wire_payload) is dict
    assert type(wire_args) is dict
    assert type(wire_items) is list
    assert type(wire_item) is dict
    wire_item["value"] = "wire-mutated"
    wire_items.append({"value": "wire-added"})

    record_payload = cast(Mapping[str, object], message.payload["args"])
    record_items = cast(tuple[object, ...], record_payload["items"])
    record_item = cast(Mapping[str, object], record_items[0])
    assert record_item["value"] == "original"
    assert len(record_items) == 1
    assert message.to_mapping()["payload"] is not wire_mapping["payload"]


def test_transport_message_serialization_preserves_zero_hop_count() -> None:
    message = TransportMessage(
        v=1,
        id="123e4567-e89b-42d3-a456-426614174000",
        type="delegation",
        sender_id="sender",
        timestamp="2026-08-28T12:00:00.000Z",
        payload={},
        recipient_id="recipient",
        task_id="123e4567-e89b-42d3-b456-426614174001",
        context_id="123e4567-e89b-42d3-8456-426614174002",
        hop_count=0,
    )
    schema_path = Path(__file__).parents[2] / "schemas" / "envelope.v1.json"
    validator = Draft202012Validator(
        json.loads(schema_path.read_text(encoding="utf-8"))
    )

    wire_mapping = message.to_mapping()

    assert wire_mapping["hop_count"] == 0
    validator.validate(wire_mapping)


class DummyRuntime:
    async def initialize(self, context: RuntimeContext) -> None: ...

    async def handle(self, message: Mapping[str, object]) -> Mapping[str, object]:
        return message

    async def drain(self) -> None: ...

    async def shutdown(self) -> None: ...


class DummySkillProvider:
    def list_skills(self) -> tuple[SkillDescriptor, ...]:
        return ()

    def resolve_by_name(self, name: str) -> SkillDescriptor | None:
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
                yield TransportMessage(
                    1,
                    "message-id",
                    "command",
                    "sender",
                    "2026-08-28T12:00:00.000Z",
                    {},
                    recipient_id=agent_id,
                )

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


def test_sdk_imports_no_framework_or_infrastructure_dependencies() -> None:
    source_root = Path(edgecitadel_plugin_sdk.__file__).parent
    imported_roots: set[str] = set()
    for path in sorted(source_root.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(
                    alias.name.partition(".")[0] for alias in node.names
                )
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported_roots.add(node.module.partition(".")[0])

    prohibited = {"pathlib", "yaml", "nats", "fastapi", "pydantic", "starlette"}

    assert imported_roots.isdisjoint(prohibited)


def test_sdk_distribution_declares_pep561_marker() -> None:
    project_root = Path(__file__).parents[1]
    package_root = project_root / "src" / "edgecitadel_plugin_sdk"
    pyproject = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )

    assert (package_root / "py.typed").is_file()
    assert pyproject["tool"]["setuptools"]["package-data"][
        "edgecitadel_plugin_sdk"
    ] == ["py.typed"]


def test_contributor_typing_gate_has_an_installable_dependency_extra() -> None:
    project_root = Path(__file__).parents[1]
    pyproject = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )
    readme = (project_root / "README.md").read_text(encoding="utf-8")

    assert pyproject["project"]["optional-dependencies"].get("type") == [
        "mypy>=1.13,<2"
    ]
    assert "python -m pip install -e '.[test,type]'" in readme
