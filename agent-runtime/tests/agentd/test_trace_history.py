import json
from uuid import uuid4

import pytest

from edgecitadel_agentd.service import PROTOCOL_VERSION, dispatch
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_history import read_history


def test_trace_filter_selects_its_historical_source(tmp_path):
    store = AgentdStore(tmp_path / "history.sqlite3")
    try:
        token = store.register_connector(
            connector_id="reader",
            host_type="codex",
            agent_id="reader",
            capabilities=["edgecitadel_trace"],
        )
        session = store.open_session(connector_id="reader", token=token)["session_id"]
        traces = []
        for node in ("old-source", "new-source"):
            reply = store.bind_trace(
                node_id=node,
                connector_id="reader",
                token=token,
                params={
                    "schema_version": 1,
                    "request_id": str(uuid4()),
                    "session_id": session,
                    "task_id": None,
                    "context_id": None,
                },
            )
            traces.append(reply["result"]["trace_id"])
        result = read_history(
            store, connector_id="reader", token=token, params={"trace_id": traces[0]}
        )
        assert len(result["events"]) == 1
        assert result["events"][0]["trace_id"] == traces[0]
        assert [source["node_id"] for source in result["sources"]] == ["old-source"]
    finally:
        store.close()


def test_local_history_pagination_scope_restart_and_revocation(tmp_path):
    path = tmp_path / "agentd.sqlite3"
    store = AgentdStore(path)
    tokens = {}
    traces = {}
    try:
        for name in ("alice", "bob"):
            tokens[name] = store.register_connector(
                connector_id=name,
                host_type="codex",
                agent_id=name,
                capabilities=["edgecitadel_trace"],
            )
            session = store.open_session(connector_id=name, token=tokens[name])[
                "session_id"
            ]
            for _ in range(3):
                bound = store.bind_trace(
                    node_id="owned-edge",
                    connector_id=name,
                    token=tokens[name],
                    params={
                        "schema_version": 1,
                        "request_id": str(uuid4()),
                        "session_id": session,
                        "task_id": None,
                        "context_id": None,
                    },
                )
                traces[name] = bound["result"]["trace_id"]
            store.close_session(
                connector_id=name, token=tokens[name], session_id=session
            )

        def read(name="alice", **params):
            return dispatch(
                store,
                {
                    "version": PROTOCOL_VERSION,
                    "operation": "trace.history",
                    "connector_id": name,
                    "token": tokens[name],
                    "params": params,
                },
            )

        first = read(limit=2)
        assert len(first["events"]) == 2
        all_events = list(first["events"])
        page = first
        while page["next_source_seq"] is not None:
            page = read(
                source_epoch=first["source_epoch"],
                after_source_seq=page["next_source_seq"],
                limit=2,
            )
            all_events.extend(page["events"])
        assert len(all_events) == 6
        assert len({e["event_id"] for e in all_events}) == 6
        assert {e["agent_id"] for e in all_events} == {"alice"}
        assert read(trace_id=traces["bob"])["events"] == []
        assert read(source_epoch=str(uuid4()))["source_epoch"] is None
        assert first["coverage"]["partial"] is True
        assert first["coverage"]["producer_loss"] == "unknown"
        before = json.dumps(read(), sort_keys=True)
        store.close()
        store = AgentdStore(path)
        assert json.dumps(read(), sort_keys=True) == before
        for params in (
            {"limit": 33},
            {"limit": True},
            {"after_source_seq": 1},
            {"agent_id": "bob"},
            {"trace_id": "secret"},
        ):
            with pytest.raises(StoreError, match="invalid local history"):
                read(**params)
        store.revoke_connector("alice")
        with pytest.raises(StoreError):
            read()
        assert len(read("bob")["events"]) == 6
    finally:
        store.close()
