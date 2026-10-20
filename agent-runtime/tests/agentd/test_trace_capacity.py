import json
import sqlite3
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd import service, trace_capacity
from edgecitadel_agentd.service import PROTOCOL_VERSION, dispatch
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_producer import RuntimeTrace


def test_mandatory_quota_failure_is_not_reported_as_malformed_json(monkeypatch):
    def reject(*args, **kwargs):
        raise TraceContractError("quota_exceeded")

    monkeypatch.setattr(service, "dispatch", reject)
    handler = service.AgentdRequestHandler.__new__(service.AgentdRequestHandler)
    handler.rfile = BytesIO(b'{"operation":"task.transition"}\n')
    handler.wfile = BytesIO()
    handler.server = SimpleNamespace(
        store=None, transport=None, telemetry=None, supervisor=None, admin_token=None
    )
    handler.handle()
    reply = json.loads(handler.wfile.getvalue())
    assert reply == {
        "ok": False,
        "error": {"code": "operation_failed", "message": "quota_exceeded"},
    }


@pytest.mark.asyncio
async def test_optional_quota_preserves_work_and_reserved_loss_and_closure(
    tmp_path, monkeypatch
):
    (tmp_path / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="native",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]

    class Client:
        def call(self, operation, **params):
            return dispatch(
                store,
                {
                    "version": PROTOCOL_VERSION,
                    "operation": operation,
                    "params": params,
                    "connector_id": "native",
                    "token": token,
                },
            )

    client = Client()
    root = client.call(
        "trace.bind",
        schema_version=1,
        request_id=str(uuid4()),
        session_id=session,
        task_id=None,
        context_id=None,
    )["result"]
    trace = RuntimeTrace(client)
    trace.binding_id = root["binding_id"]
    used = store._connection.execute(
        "SELECT event_bytes FROM trace_storage_usage"
    ).fetchone()[0]
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", used)
    monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 16384)
    effects = []
    try:
        async with trace.operation("tool", "owned-effect"):
            effects.append("once")
        await trace.finish("unknown")
        assert effects == ["once"] and trace.dropped_observations == 2
        events = [
            json.loads(r[0])
            for r in store._connection.execute("SELECT event_json FROM trace_journal")
        ]
        assert [e["kind"] for e in events] == ["run", "coverage", "run"]
        assert events[1]["attributes"]["dropped_observations"] == 2
        assert events[2]["phase"] == "unknown"
        usage = tuple(
            store._connection.execute(
                "SELECT event_bytes,event_count FROM trace_storage_usage"
            ).fetchone()
        )
        actual = tuple(
            store._connection.execute(
                "SELECT sum(event_bytes),count(*) FROM trace_journal"
            ).fetchone()
        )
        assert usage == actual
        assert used < usage[0] <= used + 16384
        # Exhausting even the reserve reports failure and consumes no positions.
        monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
        before = list(
            store._connection.execute("SELECT next_source_seq FROM trace_sources")
        )
        rejected = client.call(
            "trace.bind",
            schema_version=1,
            request_id=str(uuid4()),
            session_id=session,
            task_id=None,
            context_id=None,
        )
        assert rejected["code"] == "quota_exceeded" and not rejected["retryable"]
        assert (
            list(store._connection.execute("SELECT next_source_seq FROM trace_sources"))
            == before
        )
        assert (
            tuple(
                store._connection.execute(
                    "SELECT event_bytes,event_count FROM trace_storage_usage"
                ).fetchone()
            )
            == usage
        )
    finally:
        store.close()


def test_v10_upgrade_accounts_existing_payload_and_transaction_rollback(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    store = AgentdStore(path)
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="native",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    params = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "session_id": session,
        "task_id": None,
        "context_id": None,
    }
    store.bind_trace(
        node_id="owned-edge", connector_id="native", token=token, params=params
    )
    before = tuple(
        store._connection.execute(
            "SELECT event_bytes,event_count FROM trace_storage_usage"
        ).fetchone()
    )
    with store._connection:
        for name in ("insert", "update", "delete"):
            store._connection.execute(f"DROP TRIGGER trace_usage_{name}")
        store._connection.execute("DROP TABLE trace_storage_usage")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=10")
    store.close()
    store = AgentdStore(path)
    try:
        assert (
            tuple(
                store._connection.execute(
                    "SELECT event_bytes,event_count FROM trace_storage_usage"
                ).fetchone()
            )
            == before
        )
        store._connection.execute(
            "CREATE TRIGGER owned_spool_failure BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT,'owned failure'); END"
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.bind_trace(
                node_id="owned-edge",
                connector_id="native",
                token=token,
                params={**params, "request_id": str(uuid4())},
            )
        assert (
            tuple(
                store._connection.execute(
                    "SELECT event_bytes,event_count FROM trace_storage_usage"
                ).fetchone()
            )
            == before
        )
        store._connection.execute("DROP TRIGGER owned_spool_failure")
        # A receipt retry does not consume capacity again.
        store.bind_trace(
            node_id="owned-edge", connector_id="native", token=token, params=params
        )
        assert (
            tuple(
                store._connection.execute(
                    "SELECT event_bytes,event_count FROM trace_storage_usage"
                ).fetchone()
            )
            == before
        )
    finally:
        store.close()
