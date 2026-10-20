"""Execution-scoped MCP metadata, kept outside model-visible tool arguments."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any
from uuid import uuid4

from edgecitadel_agentd.trace_producer import RuntimeTrace

_active_trace: ContextVar[RuntimeTrace | None] = ContextVar(
    "hermes_execution", default=None
)


def bind_agent_execution(agent: Any, trace: RuntimeTrace) -> None:
    original = agent.run_conversation

    def scoped(*args: Any, **kwargs: Any) -> Any:
        scope = _active_trace.set(trace)
        try:
            kwargs["task_id"] = trace.task_id
            return original(*args, **kwargs)
        finally:
            _active_trace.reset(scope)

    agent.run_conversation = scoped


def scoped_delegate_handler(call_mcp: Callable[..., Any]) -> Callable[..., str]:
    """Create a registry handler using an operator-configured MCP transport.

    The transport receives one stable request ID for its own retry attempts.
    Hermes propagates this ContextVar to concurrent tool workers.
    """

    def delegate(arguments: dict[str, Any], **_kwargs: Any) -> str:
        trace = _active_trace.get()
        if trace is None or trace.binding_id is None or trace.task_id is None:
            return json.dumps({"error": "execution_binding_unavailable"})
        if not isinstance(arguments, dict) or set(arguments) - {
            "recipient_id",
            "request",
            "skill_id",
            "deadline_at_ms",
        }:
            return json.dumps({"error": "invalid_delegation_arguments"})
        if not all(
            isinstance(arguments.get(name), str) for name in ("recipient_id", "request")
        ):
            return json.dumps({"error": "invalid_delegation_arguments"})
        metadata = {
            "edgecitadel_execution": {
                "schema_version": 1,
                "binding_id": trace.binding_id,
                "request_id": str(uuid4()),
            }
        }
        return json.dumps(call_mcp("edgecitadel_delegate", dict(arguments), metadata))

    return delegate
