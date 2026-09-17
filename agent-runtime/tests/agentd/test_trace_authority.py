from dataclasses import replace

import pytest

from edgecitadel_agentd.trace_authority import (
    BindingAuthority,
    ExecutionAuthority,
    ImportAuthority,
    SessionAuthority,
    authorize_bind,
    authorize_binding_operation,
    authorize_historical_import,
)
from edgecitadel_agentd.trace_contract import TraceContractError

SESSION = SessionAuthority(
    "connector-a", "agent-a", "session-a", False, False, 2000, True
)
TASK = ExecutionAuthority("task-a", "agent-a", "session-a", "running")
BINDING = BindingAuthority("connector-a", "agent-a", "session-a", "task-a")


def operation(session=SESSION, binding=BINDING, task=TASK, name="append", grant=False):
    authorize_binding_operation(
        session,
        binding,
        now_ms=1000,
        task=task,
        operation=name,
        delegation_allowed=grant,
    )


def test_grant_revocation_stops_dispatch_but_preserves_completion_evidence():
    authorize_bind(SESSION, now_ms=1000, task=TASK)
    operation(name="dispatch", grant=True)
    with pytest.raises(TraceContractError, match="delegation_not_authorized"):
        operation(name="dispatch")
    operation(task=replace(TASK, state="completed"))
    operation(task=replace(TASK, state="completed"), name="finish")
    with pytest.raises(TraceContractError, match="execution_not_active"):
        operation(task=replace(TASK, state="completed"), name="dispatch", grant=True)


@pytest.mark.parametrize(
    "change",
    [
        {"connector_revoked": True},
        {"session_closed": True},
        {"lease_expires_at_ms": 1000},
        {"trace_allowed": False},
    ],
)
def test_session_revocation_stops_all_new_writes(change):
    session = replace(SESSION, **change)
    with pytest.raises(TraceContractError):
        authorize_bind(session, now_ms=1000, task=TASK)
    with pytest.raises(TraceContractError):
        operation(session=session)


@pytest.mark.parametrize(
    "change",
    [
        {"recipient_id": "agent-b"},
        {"claimed_session_id": "session-b"},
        {"claimed_session_id": None},
    ],
)
def test_task_participation_is_insufficient_without_executing_session(change):
    task = replace(TASK, **change)
    with pytest.raises(TraceContractError, match="execution_not_owned"):
        authorize_bind(SESSION, now_ms=1000, task=task)
    with pytest.raises(TraceContractError, match="execution_not_owned"):
        operation(task=task)


@pytest.mark.parametrize(
    "change",
    [
        {"connector_id": "connector-b"},
        {"agent_id": "agent-b"},
        {"session_id": "session-b"},
        {"closed": True},
        {"task_id": "task-b"},
    ],
)
def test_foreign_or_closed_binding_cannot_be_reused(change):
    with pytest.raises(TraceContractError):
        operation(binding=replace(BINDING, **change))


def test_observational_root_needs_no_executable_task():
    authorize_bind(SESSION, now_ms=1000, task=None)
    root = replace(BINDING, task_id=None)
    operation(binding=root, task=None)
    operation(binding=root, task=None, name="dispatch", grant=True)
    with pytest.raises(TraceContractError, match="binding_task_mismatch"):
        operation(binding=root)


def test_import_has_separate_administrator_source_and_agent_scope():
    grant = ImportAuthority(True, True, "archive-a", frozenset({"agent-a"}))
    authorize_historical_import(grant, source_id="archive-a", agent_id="agent-a")
    for modified in [
        replace(grant, administrator_authenticated=False),
        replace(grant, enabled=False),
        replace(grant, source_id="archive-b"),
        replace(grant, allowed_agents=frozenset({"agent-b"})),
    ]:
        with pytest.raises(TraceContractError, match="import_not_authorized"):
            authorize_historical_import(
                modified, source_id="archive-a", agent_id="agent-a"
            )


def test_rpc_shapes_do_not_accept_caller_actor_or_ancestry():
    import json
    from pathlib import Path
    from uuid import uuid4

    from edgecitadel_agentd.trace_contract import (
        validate_append_request,
        validate_binding_request,
    )

    bind = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "session_id": str(uuid4()),
        "task_id": None,
        "context_id": None,
    }
    validate_binding_request(bind)
    for field in ("agent_id", "trace_id", "parent_task_id", "source_epoch"):
        with pytest.raises(TraceContractError):
            validate_binding_request({**bind, field: str(uuid4())})
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )
    event = next(f["event"] for f in fixtures["fixtures"] if f["name"] == "tool")
    keys = (
        "schema_version",
        "kind",
        "phase",
        "span_id",
        "parent_span_id",
        "occurred_at",
        "duration_ms",
        "attributes",
    )
    observation = {key: event[key] for key in keys}
    request = {
        "schema_version": 1,
        "binding_id": str(uuid4()),
        "observation_id": str(uuid4()),
        "observation": observation,
    }
    validate_append_request(request)
    for field in ("agent_id", "trace_id", "task_id", "source_seq", "evidence_kind"):
        with pytest.raises(TraceContractError):
            validate_append_request(
                {**request, "observation": {**observation, field: event[field]}}
            )
    with pytest.raises(TraceContractError):
        validate_append_request(
            {**request, "observation": {**observation, "kind": "security"}}
        )
