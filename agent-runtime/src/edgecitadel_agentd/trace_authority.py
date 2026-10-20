"""Authority decisions over trusted store snapshots, never caller identity claims.

The recorder must load these values and apply the decision under the same store
transaction. These pure rules deliberately neither authenticate nor persist.
"""

from __future__ import annotations

from dataclasses import dataclass

from .trace_contract import TraceContractError


@dataclass(frozen=True)
class SessionAuthority:
    connector_id: str
    agent_id: str
    session_id: str
    connector_revoked: bool
    session_closed: bool
    lease_expires_at_ms: int
    trace_allowed: bool


@dataclass(frozen=True)
class ExecutionAuthority:
    task_id: str
    recipient_id: str
    claimed_session_id: str | None
    state: str


@dataclass(frozen=True)
class BindingAuthority:
    connector_id: str
    agent_id: str
    session_id: str
    task_id: str | None
    closed: bool = False


def _session(session: SessionAuthority, now_ms: int) -> None:
    if session.connector_revoked:
        raise TraceContractError("connector_revoked")
    if session.session_closed or session.lease_expires_at_ms <= now_ms:
        raise TraceContractError("session_unavailable")
    if not session.trace_allowed:
        raise TraceContractError("trace_not_authorized")


def _execution(session: SessionAuthority, task: ExecutionAuthority) -> None:
    if (
        task.recipient_id != session.agent_id
        or task.claimed_session_id != session.session_id
    ):
        raise TraceContractError("execution_not_owned")


def authorize_bind(
    session: SessionAuthority, *, now_ms: int, task: ExecutionAuthority | None
) -> None:
    _session(session, now_ms)
    if task is not None:
        _execution(session, task)
        if task.state not in {"accepted", "running"}:
            raise TraceContractError("execution_not_active")


def authorize_binding_operation(
    session: SessionAuthority,
    binding: BindingAuthority,
    *,
    now_ms: int,
    task: ExecutionAuthority | None,
    operation: str,
    delegation_allowed: bool = False,
) -> None:
    _session(session, now_ms)
    if operation not in {"append", "dispatch", "finish"}:
        raise TraceContractError("invalid_binding_operation")
    if (
        binding.connector_id != session.connector_id
        or binding.agent_id != session.agent_id
        or binding.session_id != session.session_id
    ):
        raise TraceContractError("binding_not_owned")
    if binding.closed:
        raise TraceContractError("binding_closed")
    if binding.task_id is None:
        if task is not None:
            raise TraceContractError("binding_task_mismatch")
    else:
        if task is None or task.task_id != binding.task_id:
            raise TraceContractError("binding_task_mismatch")
        _execution(session, task)
        # Completion callbacks can arrive just after the store commits a terminal
        # task state. They cannot reopen the task or authorize another dispatch.
        allowed = {"accepted", "running"}
        if operation != "dispatch":
            allowed |= {"completed", "failed", "rejected", "cancelled", "expired"}
        if task.state not in allowed:
            raise TraceContractError("execution_not_active")
    if operation == "dispatch" and not delegation_allowed:
        raise TraceContractError("delegation_not_authorized")


@dataclass(frozen=True)
class ImportAuthority:
    administrator_authenticated: bool
    enabled: bool
    source_id: str
    allowed_agents: frozenset[str]


def authorize_historical_import(
    authority: ImportAuthority, *, source_id: str, agent_id: str
) -> None:
    """Import grants are not live sessions and never authorize task dispatch."""
    if (
        not authority.administrator_authenticated
        or not authority.enabled
        or authority.source_id != source_id
        or agent_id not in authority.allowed_agents
    ):
        raise TraceContractError("import_not_authorized")
