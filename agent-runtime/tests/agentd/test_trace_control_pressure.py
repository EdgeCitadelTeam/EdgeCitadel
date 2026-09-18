import json
from uuid import uuid4

import pytest
from test_trace_dispatch_store import configured  # noqa: F401
from test_trace_security import store  # noqa: F401

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_history import read_history
from edgecitadel_agentd.trace_loss import report_loss
from edgecitadel_agentd.trace_producer import RuntimeTrace
from edgecitadel_agentd.trace_security import (
    flush_authentication_rejections,
    note_authentication_rejection,
)


def loss_params(binding):
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": binding["binding_id"],
        "producer_id": str(uuid4()),
        "dropped_observations": 1,
    }


@pytest.mark.parametrize("failure", ["pressure", "measurement"])
def test_loss_admission_preserves_existing_receipt(configured, monkeypatch, failure):  # noqa: F811
    source, token, binding = configured
    params = loss_params(binding)

    def call(value):
        return report_loss(
            source, node_id="edge-a", connector_id="native", token=token, params=value
        )

    committed = call(params)
    before = list(source._connection.iterdump())
    if failure == "pressure":
        monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 1)
        expected = "quota_exceeded"
    else:

        def unavailable(_db):
            raise OSError("PRIVATE_SENTINEL")

        monkeypatch.setattr(trace_capacity, "physical_storage", unavailable)
        expected = "storage_unavailable"
    with pytest.raises(TraceContractError, match=expected) as error:
        call(loss_params(binding))
    assert "PRIVATE_SENTINEL" not in str(error.value)
    assert call(params) == committed
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        call({**params, "dropped_observations": 2})
    assert list(source._connection.iterdump()) == before
    monkeypatch.undo()
    assert call(loss_params(binding))["status"] == "ok"


@pytest.mark.parametrize("initialized", [False, True])
def test_security_pressure_keeps_pending_count_and_rate_receipt(
    store,  # noqa: F811
    monkeypatch,
    initialized,  # noqa: F811
):  # noqa: F811
    if initialized:
        note_authentication_rejection(store)
        assert flush_authentication_rejections(store, now_ms=120000)
    note_authentication_rejection(store)
    before = list(store._connection.iterdump())
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 1)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        flush_authentication_rejections(store, now_ms=180000)
    assert list(store._connection.iterdump()) == before
    assert store._authentication_rejections == 1
    if initialized:
        assert not flush_authentication_rejections(store, now_ms=120000)
        assert list(store._connection.iterdump()) == before
        assert store._authentication_rejections == 1
    monkeypatch.undo()
    assert flush_authentication_rejections(store, now_ms=180000)
    assert store._authentication_rejections == 0
    latest = json.loads(
        store._connection.execute(
            "SELECT event_json FROM trace_journal ORDER BY source_seq DESC LIMIT 1"
        ).fetchone()[0]
    )
    assert latest["attributes"]["count"] == 1


def test_security_measurement_error_keeps_pending_count(store, monkeypatch):  # noqa: F811
    note_authentication_rejection(store)
    before = list(store._connection.iterdump())

    def unavailable(_db):
        raise OSError("PRIVATE_SENTINEL")

    monkeypatch.setattr(trace_capacity, "physical_storage", unavailable)
    with pytest.raises(TraceContractError, match="storage_unavailable") as error:
        flush_authentication_rejections(store, now_ms=120000)
    assert "PRIVATE_SENTINEL" not in str(error.value)
    assert store._authentication_rejections == 1
    assert list(store._connection.iterdump()) == before


@pytest.mark.asyncio
async def test_producer_work_and_unknown_coverage_survive_control_pressure(
    configured,  # noqa: F811
    monkeypatch,  # noqa: F811
):
    source, token, binding = configured

    class Client:
        def call(self, operation, **params):
            args = {
                "node_id": "edge-a",
                "connector_id": "native",
                "token": token,
                "params": params,
            }
            if operation == "trace.loss":
                return report_loss(source, **args)
            if operation == "trace.append":
                return source.append_trace(**args)
            assert operation == "trace.finish"
            return source.finish_trace(**args)

    trace = RuntimeTrace(Client())
    trace.binding_id = binding["binding_id"]
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 1)
    effects = []
    async with trace.operation("tool", "owned-effect"):
        effects.append("once")
    await trace.report_loss()
    assert trace.dropped_observations == 2 and trace._reported_drops == 0
    pending = dict(trace._pending_loss)
    history = read_history(source, connector_id="native", token=token, params={})
    assert history["coverage"]["producer_loss"] == "unknown"
    assert (
        source._connection.execute(
            "SELECT COUNT(*) FROM trace_requests WHERE operation='loss'"
        ).fetchone()[0]
        == 0
    )
    monkeypatch.undo()
    await trace.report_loss()
    assert trace._pending_loss is None and trace._reported_drops == 2
    assert (
        source._connection.execute(
            "SELECT request_id FROM trace_requests WHERE operation='loss'"
        ).fetchone()[0]
        == pending["request_id"]
    )
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 1)
    await trace.finish("unknown")
    assert effects == ["once"]
    assert (
        source._connection.execute(
            "SELECT closed_at_ms FROM trace_bindings WHERE binding_id=?",
            (binding["binding_id"],),
        ).fetchone()[0]
        is not None
    )
