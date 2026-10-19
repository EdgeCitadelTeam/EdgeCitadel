"""Portable skill catalog values and provider contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from ._values import _freeze_mapping


@dataclass(frozen=True)
class SkillDescriptor:
    """Framework-neutral metadata for a packaged skill."""

    name: str
    description: str
    skill_id: str
    version: str
    execution_name: str
    input_schema: Mapping[str, object]
    output_schema: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_schema", _freeze_mapping(self.input_schema))
        object.__setattr__(self, "output_schema", _freeze_mapping(self.output_schema))


@runtime_checkable
class SkillProvider(Protocol):
    """Deterministic enumeration and resolution of packaged skills."""

    def list_skills(self) -> tuple[SkillDescriptor, ...]: ...

    def resolve_by_name(self, name: str) -> SkillDescriptor | None: ...
