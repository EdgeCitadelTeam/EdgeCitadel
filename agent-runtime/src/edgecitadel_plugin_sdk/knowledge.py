"""Versioned knowledge values and persistence-neutral store contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class KnowledgeRecord:
    """An auditable reference to versioned plugin knowledge."""

    plugin_id: str
    skill_id: str
    skill_version: str
    namespace: str
    revision: int
    content_hash: str
    provenance: tuple[str, ...]


@runtime_checkable
class KnowledgeStore(Protocol):
    """Read and propose versioned knowledge records."""

    async def read(
        self,
        plugin_id: str,
        skill_id: str,
        skill_version: str,
        namespace: str,
    ) -> KnowledgeRecord | None: ...

    async def propose(self, record: KnowledgeRecord) -> KnowledgeRecord: ...
