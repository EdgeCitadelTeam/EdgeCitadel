"""Execution correlation contract, applied only after caller authority checks.

These values are correlation data, never credentials. The recorder must persist
the entire context rather than reconstruct depth from whichever parents remain.
The inherited authority only ever narrows what the executing node's own grant
allows; it cannot grant anything a local grant does not:

- ``authority_scope``: recipients the delegating invocation could reach;
- ``effect_scope``: effect rules it could authorize;
- ``lease``: the child's hard limits, within the parent's remainder;
- ``authority_chain``: the delegated task IDs from the root to the parent. A
  revocation or ingress filter naming any of them applies to this task too.
  Contexts saved before schema 32 know only their parent.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal, cast

from edgecitadel_plugin_runtime.validator import (
    ValidationError,
    default_validator,
    normalize_task_correlation,
)

from .delegation_authority import EffectRule, Lease, effect_rules
from .trace_contract import TraceContractError

_UUID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)
_TRACE = re.compile(r"[0-9a-f]{32}")
_AGENT = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
MAX_SCOPE = 256
MAX_CHAIN = 64


@dataclass(frozen=True)
class TaskTraceContext:
    task_id: str
    context_id: str
    trace_id: str | None
    parent_task_id: str | None = None
    parent_run_id: str | None = None
    hop_count: int = 0
    context_origin: Literal["source_explicit", "legacy_default"] = "source_explicit"
    # None: no inherited restriction (a root task).
    authority_scope: tuple[str, ...] | None = None
    effect_scope: tuple[EffectRule, ...] | None = None
    lease: Lease | None = None
    authority_chain: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Saved and transported contexts are JSON; keep one canonical form.
        if self.effect_scope is not None:
            object.__setattr__(
                self, "effect_scope", tuple(sorted(effect_rules(self.effect_scope)))
            )
        object.__setattr__(self, "lease", Lease.parse(self.lease))
        if self.authority_scope is not None:
            scope = self.authority_scope
            if (
                not isinstance(scope, (list, tuple))
                or len(scope) > MAX_SCOPE
                or not all(
                    isinstance(agent, str) and _AGENT.fullmatch(agent)
                    for agent in scope
                )
                or len(set(scope)) != len(scope)
            ):
                raise TraceContractError("invalid_task_correlation")
            # Saved and transported contexts are JSON lists; keep one canonical form.
            object.__setattr__(self, "authority_scope", tuple(sorted(scope)))
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
        chain = self.authority_chain
        if (
            not isinstance(chain, (list, tuple))
            or len(chain) > min(MAX_CHAIN, self.hop_count)
            or not all(
                isinstance(task, str) and _UUID.fullmatch(task) for task in chain
            )
            or len(set(chain)) != len(chain)
            or self.task_id in chain
            or (
                (not chain or chain[-1] != self.parent_task_id)
                if self.parent_task_id is not None
                else bool(chain)
            )
        ):
            raise TraceContractError("invalid_task_correlation")
        object.__setattr__(self, "authority_chain", tuple(chain))

    def child(
        self,
        task_id: str,
        *,
        recipients: frozenset[str],
        effects: frozenset[EffectRule],
        lease: Lease | None,
    ) -> TaskTraceContext:
        """Derive identity for an already-authorized child dispatch.

        The authority is the dispatching invocation's effective authority,
        which the child can only narrow further.
        """
        return replace(
            self,
            task_id=task_id,
            parent_task_id=self.task_id,
            parent_run_id=None,
            hop_count=self.hop_count + 1,
            authority_scope=tuple(sorted(recipients)),
            effect_scope=tuple(sorted(effects)),
            lease=lease,
            authority_chain=(*self.authority_chain, self.task_id),
        )

    @property
    def authorities(self) -> tuple[str, ...]:
        """The delegations this task acts under: its ancestors and itself."""
        return (*self.authority_chain, self.task_id)

    def execution_context(self) -> dict[str, Any]:
        """The payload capsule that carries this context across a transport hop."""
        return {
            "schema_version": 1,
            "context_origin": self.context_origin,
            "parent_run_id": self.parent_run_id,
            "authority_scope": list(self.authority_scope)
            if self.authority_scope is not None
            else None,
            "effect_scope": [asdict(rule) for rule in self.effect_scope]
            if self.effect_scope is not None
            else None,
            "lease": asdict(self.lease) if self.lease is not None else None,
            "authority_chain": list(self.authority_chain),
        }

    def saved(self) -> str:
        """The canonical JSON this context is persisted and compared as."""
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))

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
        payload["execution_context"] = self.execution_context()
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
        metadata: dict[str, Any] = {
            "parent_run_id": None,
            "authority_scope": None,
            "effect_scope": None,
            "lease": None,
            "authority_chain": [],
        }
        if "execution_context" in payload:
            capsule = payload["execution_context"]
            if (
                not isinstance(capsule, dict)
                or set(capsule) != {"schema_version", "context_origin", *metadata}
                or type(capsule["schema_version"]) is not int
                or capsule["schema_version"] != 1
            ):
                raise TraceContractError("unsupported_execution_context")
            origin = capsule["context_origin"]
            metadata = {key: capsule[key] for key in metadata}
        if (normalized["hop_count"] or metadata["parent_run_id"]) and (
            metadata["authority_scope"] is None or metadata["effect_scope"] is None
        ):
            # A delegated task always carries the delegator's authority; its
            # absence would silently widen authority at this hop.
            raise TraceContractError("missing_authority_scope")
        return cls(
            task_id=cast(str, normalized["task_id"]),
            context_id=cast(str, normalized["context_id"]),
            trace_id=payload.get("trace_id"),
            parent_task_id=payload.get("parent_task_id"),
            hop_count=cast(int, normalized["hop_count"]),
            context_origin=cast(Literal["source_explicit", "legacy_default"], origin),
            **metadata,
        )
