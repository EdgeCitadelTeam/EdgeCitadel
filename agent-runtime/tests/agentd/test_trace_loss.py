import json
from uuid import uuid4

import pytest

from edgecitadel_agentd.client import AgentdClientError
from edgecitadel_agentd.service import PROTOCOL_VERSION, dispatch
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_history import read_history
from edgecitadel_agentd.trace_producer import RuntimeTrace


def test_loss_report_rejects_foreign_binding_and_invalid_counters(tmp_path):
    (tmp_path / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    try:
        tokens = {
            name: store.register_connector(
                connector_id=name,
                host_type="codex",
                agent_id=name,
                capabilities=["edgecitadel_trace"],
            )
            for name in ("owner", "other")
        }
        session = store.open_session(connector_id="owner", token=tokens["owner"])[
            "session_id"
        ]
        binding = store.bind_trace(
            node_id="owned-edge",
            connector_id="owner",
            token=tokens["owner"],
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session,
                "task_id": None,
                "context_id": None,
            },
        )["result"]
        params = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "binding_id": binding["binding_id"],
            "producer_id": str(uuid4()),
            "dropped_observations": 1,
        }

        def call(name, value):
            return dispatch(
                store,
                {
                    "version": PROTOCOL_VERSION,
                    "operation": "trace.loss",
                    "connector_id": name,
                    "token": tokens[name],
                    "params": value,
                },
            )

        assert call("other", params)["code"] == "not_authorized"
        for count in (0, -1, True, 2**31, "private-sentinel"):
            reply = call("owner", {**params, "dropped_observations": count})
            assert reply["code"] == "invalid_metadata"
            assert "private-sentinel" not in json.dumps(reply)
        assert (
            store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[
                0
            ]
            == 1
        )
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["lost_response", "rollback"])
async def test_recovered_producer_loss_is_durable_without_repeating_work(
    tmp_path, fault
):
    state = tmp_path / "state"
    state.mkdir()
    (state / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(state / "agentd/agentd.sqlite3")
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="native",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    binding = store.bind_trace(
        node_id="owned-edge",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": None,
            "context_id": None,
        },
    )["result"]
    reports = []

    class Client:
        failed_append = False

        def call(self, operation, **params):
            if operation == "trace.append" and not self.failed_append:
                self.failed_append = True
                raise AgentdClientError("private-sentinel")
            if operation == "trace.loss":
                reports.append(dict(params))
                if fault == "rollback" and len(reports) == 1:
                    store._connection.execute(
                        "CREATE TRIGGER owned_loss_failure BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT,'owned failure'); END"
                    )
            try:
                reply = dispatch(
                    store,
                    {
                        "version": PROTOCOL_VERSION,
                        "operation": operation,
                        "params": params,
                        "connector_id": "native",
                        "token": token,
                    },
                )
            finally:
                if (
                    operation == "trace.loss"
                    and fault == "rollback"
                    and len(reports) == 1
                ):
                    store._connection.execute("DROP TRIGGER owned_loss_failure")
            if (
                operation == "trace.loss"
                and fault == "lost_response"
                and len(reports) == 1
            ):
                raise AgentdClientError("lost reply")
            return reply

    trace = RuntimeTrace(Client())
    trace.binding_id = binding["binding_id"]
    effects = []
    try:
        async with trace.operation("tool", "owned-effect"):
            effects.append("once")
        await trace.finish("unknown")
        assert effects == ["once"]
        assert trace.dropped_observations == 1
        assert len(reports) == 2 and reports[0] == reports[1]
        coverage = [
            json.loads(row[0])
            for row in store._connection.execute(
                "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            )
        ]
        assert len(coverage) == 1
        assert coverage[0]["phase"] == "unknown"
        assert coverage[0]["attributes"]["dropped_observations"] == 1
        assert "lost_ranges" not in coverage[0]["attributes"]
        assert "private-sentinel" not in json.dumps(coverage)
        history = read_history(store, connector_id="native", token=token, params={})
        assert history["coverage"]["producer_loss"] == "reported"
        event_id = coverage[0]["event_id"]
        assert (
            store._connection.execute(
                "SELECT count(*) FROM trace_spool WHERE event_id=?", (event_id,)
            ).fetchone()[0]
            == 1
        )
        store.close()
        store = AgentdStore(state / "agentd/agentd.sqlite3")
        assert (
            store._connection.execute(
                "SELECT count(*) FROM trace_journal WHERE event_id=?", (event_id,)
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()
