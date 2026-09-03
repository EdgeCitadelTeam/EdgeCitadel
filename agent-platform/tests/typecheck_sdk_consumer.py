"""Static SDK consumer fixture.

``runtime_checkable`` verifies member presence only; static type checking enforces
protocol signatures and async contracts.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

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


class RuntimeImplementation:
    async def initialize(self, context: RuntimeContext) -> None: ...

    async def handle(self, message: Mapping[str, object]) -> Mapping[str, object]:
        return message

    async def drain(self) -> None: ...

    async def shutdown(self) -> None: ...


class SkillImplementation:
    def list_skills(self) -> tuple[SkillDescriptor, ...]:
        return ()

    def resolve_by_name(self, name: str) -> SkillDescriptor | None:
        return None


class KnowledgeImplementation:
    async def read(
        self,
        plugin_id: str,
        skill_id: str,
        skill_version: str,
        namespace: str,
    ) -> KnowledgeRecord | None:
        return None

    async def propose(self, record: KnowledgeRecord) -> KnowledgeRecord:
        return record


class TransportImplementation:
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


class LifecycleImplementation:
    async def before_transition(self, transition: LifecycleTransition) -> None: ...

    async def after_transition(self, transition: LifecycleTransition) -> None: ...


runtime: AgentRuntime = RuntimeImplementation()
skills: SkillProvider = SkillImplementation()
knowledge: KnowledgeStore = KnowledgeImplementation()
transport: Transport = TransportImplementation()
hooks: LifecycleHooks = LifecycleImplementation()


async def exercise_sdk_contracts() -> None:
    context = RuntimeContext("plugin", "agent", {"mode": "test"}, {})
    transition = LifecycleTransition(
        "plugin", LifecycleState.STARTING, LifecycleState.READY, None
    )
    record = KnowledgeRecord(
        "plugin", "skill.id", "1.0.0", "namespace", 1, "0" * 64, ("task:1",)
    )

    await runtime.initialize(context)
    response = await runtime.handle({"request": "value"})
    await runtime.drain()
    await runtime.shutdown()
    for descriptor in skills.list_skills():
        assert descriptor.name
    skills.resolve_by_name("portable-name")
    await knowledge.read("plugin", "skill.id", "1.0.0", "namespace")
    await knowledge.propose(record)
    await transport.register("agent")
    async for message in transport.receive("agent"):
        wire_message = message.to_mapping()
        await transport.publish(message)
        wire_message.get("payload")
    await transport.drain()
    await hooks.before_transition(transition)
    await hooks.after_transition(transition)
    response.get("result")
