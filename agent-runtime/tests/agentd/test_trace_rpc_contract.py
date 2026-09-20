import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest

from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    validate_event,
    validate_finish_request,
    validate_import_request,
    validate_rpc_reply,
)

FIXTURES = json.loads(
    (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
)["fixtures"]


def event(kind):
    return deepcopy(next(f["event"] for f in FIXTURES if f["name"] == kind))


def test_run_closure_does_not_require_fabricated_success():
    for phase in ("unknown", "interrupted"):
        value = event("run")
        value["phase"] = phase
        value["attributes"] = {"reason": "session_unavailable"}
        validate_event(value)
    request = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": str(uuid4()),
        "outcome": "unknown",
        "reason": "unknown",
    }
    validate_finish_request(request)
    request["task_id"] = str(uuid4())
    with pytest.raises(TraceContractError):
        validate_finish_request(request)


@pytest.mark.parametrize("kind", ["run", "task", "model", "tool"])
def test_historical_import_has_no_live_actor_or_source_position(kind):
    excluded = {
        "event_id",
        "node_id",
        "source_epoch",
        "source_seq",
        "agent_id",
        "trace_id",
        "evidence_kind",
        "causes",
        "supersedes_event_id",
    }
    observation = {k: v for k, v in event(kind).items() if k not in excluded}
    request = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "record_id": str(uuid4()),
        "historical_run_id": str(uuid4()),
        "import_source_id": "archive-a",
        "agent_id": "worker-a",
        "observation": observation,
    }
    validate_import_request(request)
    for field, value in (
        ("trace_id", "a" * 32),
        ("source_seq", 20),
        ("evidence_kind", "source_observed"),
    ):
        bad = deepcopy(request)
        bad["observation"][field] = value
        with pytest.raises(TraceContractError):
            validate_import_request(bad)
    bad = deepcopy(request)
    bad["observation"]["task_id"] = bad["observation"]["parent_task_id"] = str(uuid4())
    with pytest.raises(TraceContractError):
        validate_import_request(bad)


@pytest.mark.parametrize("operation", ["bind", "append", "finish", "import"])
def test_reply_matches_request_and_operation(operation):
    request_id = str(uuid4())
    receipt = {"event_id": str(uuid4()), "source_epoch": str(uuid4()), "source_seq": 1}
    result = (
        receipt
        if operation != "bind"
        else {
            "binding_id": str(uuid4()),
            "trace_id": "a" * 32,
            "task_id": None,
            "context_id": None,
            "execution_attempt_id": str(uuid4()),
        }
    )
    if operation == "import":
        result = {**result, "trace_id": "a" * 32}
    reply = {
        "schema_version": 1,
        "request_id": request_id,
        "operation": operation,
        "status": "ok",
        "result": result,
    }
    validate_rpc_reply(reply, operation=operation, request_id=request_id)
    for expected_operation, expected_id in (
        ("wrong", request_id),
        (operation, str(uuid4())),
    ):
        with pytest.raises(TraceContractError, match="rpc_reply_mismatch"):
            validate_rpc_reply(
                reply, operation=expected_operation, request_id=expected_id
            )


@pytest.mark.parametrize(
    "code",
    [
        "invalid_metadata",
        "unsupported_version",
        "not_authorized",
        "session_unavailable",
        "binding_closed",
        "identity_mismatch",
        "idempotency_conflict",
        "storage_unavailable",
        "quota_exceeded",
    ],
)
def test_errors_have_fixed_retry_policy_and_cannot_echo_secrets(code):
    request_id = str(uuid4())
    reply = {
        "schema_version": 1,
        "request_id": request_id,
        "operation": "append",
        "status": "error",
        "code": code,
        "retryable": code == "storage_unavailable",
    }
    validate_rpc_reply(reply, operation="append", request_id=request_id)
    bad = {**reply, "retryable": not reply["retryable"]}
    with pytest.raises(TraceContractError, match="invalid_retryability"):
        validate_rpc_reply(bad, operation="append", request_id=request_id)
    bad = {**reply, "message": "SECRET_SENTINEL"}
    with pytest.raises(TraceContractError) as error:
        validate_rpc_reply(bad, operation="append", request_id=request_id)
    assert "SECRET_SENTINEL" not in str(error.value)


def test_historical_identity_is_stable_and_separate_from_live_and_other_imports():
    from edgecitadel_agentd.trace_import import historical_identity

    params = {
        "namespace_id": "00000000-0000-4000-8000-000000000001",
        "import_source_id": "archive-a",
        "agent_id": "worker-a",
        "historical_run_id": "00000000-0000-4000-8000-000000000002",
        "kind": "task",
        "original_id": "00000000-0000-4000-8000-000000000003",
    }
    result = historical_identity(**params)
    assert result == "0c58d9e4-dba3-4174-af19-2f8084ffe02a"
    assert historical_identity(**params) == result
    assert result != params["original_id"]
    for field, value in {
        "namespace_id": "00000000-0000-4000-8000-000000000004",
        "import_source_id": "archive-b",
        "agent_id": "worker-b",
        "historical_run_id": "00000000-0000-4000-8000-000000000005",
        "kind": "span",
        "original_id": "00000000-0000-4000-8000-000000000006",
    }.items():
        assert historical_identity(**{**params, field: value}) != result


def test_bound_dispatch_request_has_no_caller_parent_or_trace():
    from edgecitadel_agentd.trace_contract import validate_dispatch_request

    request = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": str(uuid4()),
        "recipient_id": "worker-a",
        "request": "owned command",
        "skill_id": None,
        "deadline_at_ms": None,
    }
    validate_dispatch_request(request)
    for key in ("trace_id", "parent_task_id", "parent_run_id", "sender_id"):
        with pytest.raises(TraceContractError):
            validate_dispatch_request({**request, key: str(uuid4())})
    result = {
        "schema_version": 1,
        "request_id": request["request_id"],
        "operation": "dispatch",
        "status": "ok",
        "result": {
            "task_id": str(uuid4()),
            "trace_id": "a" * 32,
            "context_id": None,
            "parent_task_id": None,
            "parent_run_id": "a" * 32,
            "state": "queued",
        },
    }
    validate_rpc_reply(result, operation="dispatch", request_id=request["request_id"])
    for patch in (
        {"parent_run_id": None},
        {"parent_task_id": str(uuid4())},
        {"parent_run_id": "b" * 32},
    ):
        bad = {**result, "result": {**result["result"], **patch}}
        with pytest.raises(TraceContractError, match="invalid_dispatch_parent"):
            validate_rpc_reply(
                bad, operation="dispatch", request_id=request["request_id"]
            )
