"""Execution correlation contract, applied only after caller authority checks.

These values are correlation data, never credentials. The recorder must persist
the entire context rather than reconstruct depth from whichever parents remain.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from edgecitadel_plugin_runtime.validator import (
    ValidationError,
    default_validator,
    normalize_task_correlation,
)

from .trace_contract import TraceContractError

_UUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
_TRACE = re.compile(r"[0-9a-f]{32}")


@dataclass(frozen=True)
class TaskTraceContext:
    task_id: str
    context_id: str
    trace_id: str | None
    parent_task_id: str | None = None
    parent_run_id: str | None = None
    hop_count: int = 0
    context_origin: Literal["source_explicit", "legacy_default"] = "source_explicit"

    def __post_init__(self) -> None:
        for value in (self.task_id, self.context_id):
            if not isinstance(value, str) or _UUID.fullmatch(value) is None:
                raise TraceContractError("invalid_task_correlation")
        if self.trace_id is not None and (
            not isinstance(self.trace_id, str)
            or _TRACE.fullmatch(self.trace_id) is None
        ):
            raise TraceContractError("invalid_task_correlation")
        if (
            type(self.hop_count) is not int
            or not 0 <= self.hop_count <= 9_007_199_254_740_991
        ):
            raise TraceContractError("invalid_task_correlation")
        if not isinstance(self.context_origin, str) or self.context_origin not in {
            "source_explicit",
            "legacy_default",
        }:
            raise TraceContractError("invalid_task_correlation")
        if self.parent_task_id is not None:
            if (
                not isinstance(self.parent_task_id, str)
                or _UUID.fullmatch(self.parent_task_id) is None
                or self.parent_task_id == self.task_id
                or self.hop_count == 0
                or self.parent_run_id is not None
            ):
                raise TraceContractError("invalid_task_correlation")
        elif self.hop_count != 0:
            raise TraceContractError("invalid_task_correlation")
        if self.parent_run_id is not None and (
            self.trace_id is None or self.parent_run_id != self.trace_id
        ):
            raise TraceContractError("invalid_task_correlation")

    def child(self, task_id: str) -> TaskTraceContext:
        """Derive identity for an already-authorized child dispatch."""
        return replace(
            self,
            task_id=task_id,
            parent_task_id=self.task_id,
            parent_run_id=None,
            hop_count=self.hop_count + 1,
        )

    def apply(self, envelope: Mapping[str, Any]) -> dict[str, Any]:
        """Stamp reserved correlation fields, preserving non-correlation data.

        Results and cancellation carry the task's original ancestry, not the
        result reporter's newly invented context. Raw handler output cannot
        override these reserved fields.
        """
        message_type = envelope.get("type")
        if message_type not in {"command", "delegation", "result", "cancel"}:
            raise TraceContractError("invalid_task_correlation")
        if not isinstance(envelope.get("payload"), Mapping):
            raise TraceContractError("invalid_task_correlation")
        if envelope.get("task_id") != self.task_id:
            raise TraceContractError("task_correlation_mismatch")
        payload = dict(envelope["payload"])
        payload.pop("parent_task_id", None)
        payload.pop("trace_id", None)
        if self.parent_task_id is not None:
            payload["parent_task_id"] = self.parent_task_id
        if self.trace_id is not None:
            payload["trace_id"] = self.trace_id
        payload["execution_context"] = {
            "schema_version": 1,
            "context_origin": self.context_origin,
            "parent_run_id": self.parent_run_id,
        }
        if message_type in {"command", "delegation"}:
            message_type = "delegation" if self.hop_count else "command"
        result = {
            **envelope,
            "type": message_type,
            "context_id": self.context_id,
            "hop_count": self.hop_count,
            "payload": payload,
        }
        try:
            default_validator().validate_envelope(result)
        except ValidationError as error:
            raise TraceContractError("invalid_task_correlation") from error
        return result

    @classmethod
    def from_envelope(cls, envelope: Mapping[str, Any]) -> TaskTraceContext:
        if not isinstance(envelope, Mapping):
            raise TraceContractError("invalid_task_correlation")
        try:
            default_validator().validate_envelope(dict(envelope))
            normalized = normalize_task_correlation(envelope)
        except (ValidationError, KeyError, TypeError) as error:
            raise TraceContractError("invalid_task_correlation") from error
        payload = cast(dict[str, Any], normalized["payload"])
        origin = "source_explicit" if "context_id" in envelope else "legacy_default"
        parent_run = None
        if "execution_context" in payload:
            metadata = payload["execution_context"]
            if (
                not isinstance(metadata, dict)
                or set(metadata)
                != {"schema_version", "context_origin", "parent_run_id"}
                or type(metadata["schema_version"]) is not int
                or metadata["schema_version"] != 1
            ):
                raise TraceContractError("unsupported_execution_context")
            origin = metadata["context_origin"]
            parent_run = metadata["parent_run_id"]
        return cls(
            task_id=cast(str, normalized["task_id"]),
            context_id=cast(str, normalized["context_id"]),
            trace_id=payload.get("trace_id"),
            parent_task_id=payload.get("parent_task_id"),
            parent_run_id=parent_run,
            hop_count=cast(int, normalized["hop_count"]),
            context_origin=cast(Literal["source_explicit", "legacy_default"], origin),
        )
