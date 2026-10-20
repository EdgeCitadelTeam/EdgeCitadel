from uuid import uuid4

import pytest

from edgecitadel_agentd.client import AgentdClientError
from edgecitadel_agentd.trace_producer import RuntimeTrace


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "invalid_reply", "error_reply"])
async def test_optional_trace_failure_is_bounded_and_redacted(caplog, failure):
    class UnavailableClient:
        def call(self, operation, **params):
            if failure == "exception":
                raise AgentdClientError("private-credential-sentinel")
            if failure == "invalid_reply":
                return {"secret": "private-credential-sentinel"}
            return {
                "schema_version": 1,
                "operation": "bind",
                "request_id": params["request_id"],
                "status": "error",
                "code": "storage_unavailable",
                "retryable": True,
            }

    trace = RuntimeTrace(UnavailableClient())
    trace.dropped_observations = 2**31 - 1
    await trace.bind(session_id=str(uuid4()), task_id=str(uuid4()))
    assert trace.binding_id is None
    assert trace.dropped_observations == 2**31 - 1
    assert "private-credential-sentinel" not in caplog.text
    await trace.finish("completed")
    assert len(caplog.records) == 1


@pytest.mark.asyncio
async def test_finish_failure_does_not_retry_external_work(caplog):
    calls = []

    class UnavailableClient:
        def call(self, operation, **params):
            calls.append((operation, params))
            raise AgentdClientError("private-result-sentinel")

    trace = RuntimeTrace(UnavailableClient())
    trace.binding_id = str(uuid4())
    await trace.finish("completed")
    assert len(calls) == 1
    assert calls[0][0] == "trace.finish"
    assert calls[0][1]["outcome"] == "completed"
    assert trace.dropped_observations == 1
    assert "private-result-sentinel" not in caplog.text


@pytest.mark.asyncio
async def test_observation_failures_do_not_repeat_operation_or_hide_its_error(caplog):
    calls = []
    effects = []

    class UnavailableClient:
        def call(self, operation, **params):
            calls.append(params)
            raise AgentdClientError("private-credential-sentinel")

    trace = RuntimeTrace(UnavailableClient())
    trace.binding_id = str(uuid4())
    with pytest.raises(ValueError, match="private-operation-error"):
        async with trace.operation("tool", "owned-effect"):
            effects.append("executed")
            raise ValueError("private-operation-error")
    assert effects == ["executed"]
    assert len(calls) == 2
    assert calls[0]["observation_id"] != calls[1]["observation_id"]
    assert calls[0]["observation"]["span_id"] == calls[1]["observation"]["span_id"]
    assert calls[1]["observation"]["phase"] == "failed"
    assert trace.dropped_observations == 2
    assert "private-" not in caplog.text
    assert "private-operation-error" not in str(calls)
