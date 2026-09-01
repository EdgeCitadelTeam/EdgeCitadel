"""Public contracts for framework-neutral EdgeCitadel plugins."""

from .knowledge import KnowledgeRecord, KnowledgeStore
from .lifecycle import LifecycleHooks, LifecycleState, LifecycleTransition
from .runtime import AgentRuntime, RuntimeContext
from .skills import SkillDescriptor, SkillProvider
from .transport import Transport, TransportMessage

__all__ = [
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
]
