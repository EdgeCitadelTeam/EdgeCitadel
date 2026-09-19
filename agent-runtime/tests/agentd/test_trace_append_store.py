import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError


@pytest.fixture
def setup(tmp_path):
    store = AgentdStore(tmp_path / "state" / "agentd" / "agentd.sqlite3")
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="agent-a",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    roots = [
        store.bind_trace(
            node_id="edge-a",
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
        for _ in range(2)
    ]
    try:
        yield store, token, session, roots
    finally:
        store.close()


def request(binding, span=None, phase="started", parent=None):
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    template = next(item["event"] for item in fixtures if item["name"] == "tool")
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
    observation = {key: deepcopy(template[key]) for key in keys}
    observation.update(span_id=span or str(uuid4()), parent_span_id=parent, phase=phase)
    return {
        "schema_version": 1,
        "binding_id": binding["binding_id"],
        "observation_id": str(uuid4()),
        "observation": observation,
    }


def append(store, token, params):
    return store.append_trace(
        node_id="edge-a", connector_id="native", token=token, params=params
    )


def test_start_finish_are_distinct_immutable_events_with_stable_retries(setup):
    store, token, _, roots = setup
    start = request(roots[0])
    first = append(store, token, start)
    finish = request(roots[0], start["observation"]["span_id"], "finished")
    last = append(store, token, finish)
    assert first["result"]["event_id"] != last["result"]["event_id"]
    assert append(store, token, start) == first
    assert append(store, token, finish) == last
    reopened = AgentdStore(store.path)
    try:
        assert append(reopened, token, finish) == last
    finally:
        reopened.close()
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_operations").fetchone()[0]
        == 1
    )
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0]
        == 4
    )
    altered = deepcopy(start)
    altered["observation"]["attributes"]["name"] = "different-tool"
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        append(store, token, altered)


def test_cross_binding_span_and_parent_are_rejected(setup):
    store, token, _, roots = setup
    start = request(roots[0])
    append(store, token, start)
    span = start["observation"]["span_id"]
    with pytest.raises(TraceContractError, match="span_identity_mismatch"):
        append(store, token, request(roots[1], span, "finished"))
    with pytest.raises(TraceContractError, match="span_parent_not_owned"):
        append(store, token, request(roots[1], parent=span))
    append(store, token, request(roots[0], parent=span))
    with pytest.raises(TraceContractError, match="span_boundary_conflict"):
        append(store, token, request(roots[0], span))


def test_terminal_before_start_preserves_unknown_start_until_late_evidence(setup):
    store, token, _, roots = setup
    span = str(uuid4())
    append(store, token, request(roots[0], span, "finished"))
    before = store._connection.execute(
        "SELECT * FROM trace_operations WHERE span_id=?", (span,)
    ).fetchone()
    assert before["started_event_id"] is None
    append(store, token, request(roots[0], span, "started"))
    after = store._connection.execute(
        "SELECT * FROM trace_operations WHERE span_id=?", (span,)
    ).fetchone()
    assert after["phase"] == "finished"
    assert after["terminal_event_id"] == before["terminal_event_id"]
    assert after["started_event_id"] is not None


def test_late_transaction_failure_rolls_back_span_journal_and_spool(setup):
    store, token, _, roots = setup
    baseline = store._connection.execute(
        "SELECT COUNT(*) FROM trace_journal"
    ).fetchone()[0]
    store._connection.execute(
        "CREATE TRIGGER owned_append_fault BEFORE INSERT ON trace_requests WHEN NEW.operation='append' BEGIN SELECT RAISE(ABORT,'owned late failure'); END"
    )
    params = request(roots[0])
    with pytest.raises(sqlite3.IntegrityError, match="owned late failure"):
        append(store, token, params)
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_operations").fetchone()[0]
        == 0
    )
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0]
        == baseline
    )
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_spool").fetchone()[0]
        == baseline
    )
    store._connection.execute("DROP TRIGGER owned_append_fault")
    assert append(store, token, params)["result"]["source_seq"] == baseline + 1


def test_session_closure_denies_append_and_receipt_lookup(setup):
    store, token, session, roots = setup
    params = request(roots[0])
    append(store, token, params)
    store.close_session(connector_id="native", token=token, session_id=session)
    with pytest.raises(TraceContractError, match="session_unavailable"):
        append(store, token, params)
    with pytest.raises(TraceContractError, match="session_unavailable"):
        append(store, token, request(roots[0]))


def test_service_routes_scoped_rpc_using_enrolled_node_and_bounded_errors(setup):
    from edgecitadel_agentd.service import dispatch

    store, token, session, _roots = setup
    (store.path.parent.parent / "node.json").write_text('{"agent_id":"edge-a"}')
    envelope = {"version": 1, "connector_id": "native", "token": token}
    result = dispatch(
        store,
        {
            **envelope,
            "operation": "trace.bind",
            "params": {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session,
                "task_id": None,
                "context_id": None,
            },
        },
    )
    assert result["status"] == "ok"
    params = request(result["result"])
    reply = dispatch(store, {**envelope, "operation": "trace.append", "params": params})
    assert reply["status"] == "ok"
    bad = deepcopy(params)
    bad["observation"]["attributes"]["secret"] = "RPC_SECRET_SENTINEL"
    error = dispatch(store, {**envelope, "operation": "trace.append", "params": bad})
    assert error["code"] == "invalid_metadata" and not error["retryable"]
    assert "RPC_SECRET_SENTINEL" not in json.dumps(error)
    assert (
        dispatch(
            store,
            {
                **envelope,
                "operation": "trace.append",
                "params": {**params, "node_id": "forged"},
            },
        )["code"]
        == "invalid_metadata"
    )
    store.close_session(connector_id="native", token=token, session_id=session)
    assert (
        dispatch(store, {**envelope, "operation": "trace.append", "params": params})[
            "code"
        ]
        == "session_unavailable"
    )


def test_v8_operation_migration_preserves_bindings_and_rolls_back_failure(setup):
    store, _token, _session, _roots = setup
    db = store._connection
    before = [tuple(row) for row in db.execute("SELECT * FROM trace_bindings")]
    with db:
        db.execute("DROP TABLE trace_task_contexts")
        db.execute("DROP TABLE trace_operations")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(db)
        db.execute("PRAGMA user_version=8")
    captured = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source):
            captured.append(self._connection)
            super()._execute_migration_sql(source)
            raise RuntimeError("owned operation migration failure")

    try:
        with pytest.raises(RuntimeError, match="owned operation migration failure"):
            FailingStore(store.path)
    finally:
        for connection in captured:
            connection.close()
    assert db.execute("PRAGMA user_version").fetchone()[0] == 8
    assert not db.execute(
        "SELECT name FROM sqlite_master WHERE name='trace_operations'"
    ).fetchall()
    migrated = AgentdStore(store.path)
    try:
        assert migrated._connection.execute("PRAGMA user_version").fetchone()[0] == 25
        assert [
            tuple(row)
            for row in migrated._connection.execute("SELECT * FROM trace_bindings")
        ] == before
    finally:
        migrated.close()


def finish_request(root, outcome="unknown"):
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": root["binding_id"],
        "outcome": outcome,
        "reason": "unknown",
    }


def finish(store, token, params):
    return store.finish_trace(
        node_id="edge-a", connector_id="native", token=token, params=params
    )


def test_finish_closes_open_boundaries_and_retry_does_not_reopen(setup):
    store, token, session, roots = setup
    tool = request(roots[0])
    append(store, token, tool)
    model = request(roots[0])
    model["observation"]["kind"] = "model"
    model["observation"]["attributes"] = {
        "name": "owned-model",
        "input_tokens": None,
        "output_tokens": None,
        "usage_unavailable_reason": "not_reported",
    }
    append(store, token, model)
    params = finish_request(roots[0])
    receipt = finish(store, token, params)
    assert finish(store, token, params) == receipt
    rows = store._connection.execute(
        "SELECT phase FROM trace_operations WHERE binding_id=?",
        (roots[0]["binding_id"],),
    ).fetchall()
    assert [row[0] for row in rows] == ["interrupted", "interrupted"]
    events = [
        json.loads(row[0])
        for row in store._connection.execute("SELECT event_json FROM trace_journal")
    ]
    interrupted = [e for e in events if e["phase"] == "interrupted"]
    assert len(interrupted) == 2 and all(e["duration_ms"] is None for e in interrupted)
    assert (
        next(e for e in interrupted if e["kind"] == "model")["attributes"][
            "usage_unavailable_reason"
        ]
        == "interrupted"
    )
    with pytest.raises(TraceContractError, match="binding_closed"):
        append(store, token, request(roots[0]))
    with pytest.raises(TraceContractError, match="binding_closed"):
        finish(store, token, finish_request(roots[0]))
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        finish(store, token, {**params, "outcome": "completed"})
    store.close_session(connector_id="native", token=token, session_id=session)
    with pytest.raises(TraceContractError, match="session_unavailable"):
        finish(store, token, params)


def test_finish_failure_rolls_back_closure_and_interrupted_boundaries(setup):
    store, token, _, roots = setup
    append(store, token, request(roots[0]))
    db = store._connection
    count = db.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0]
    db.execute(
        "CREATE TRIGGER owned_finish_failure BEFORE INSERT ON trace_requests WHEN NEW.operation='finish' BEGIN SELECT RAISE(ABORT,'owned finish failure'); END"
    )
    params = finish_request(roots[0])
    with pytest.raises(sqlite3.IntegrityError, match="owned finish failure"):
        finish(store, token, params)
    assert (
        db.execute(
            "SELECT closed_at_ms FROM trace_bindings WHERE binding_id=?",
            (roots[0]["binding_id"],),
        ).fetchone()[0]
        is None
    )
    assert db.execute("SELECT phase FROM trace_operations").fetchone()[0] == "started"
    assert db.execute("SELECT COUNT(*) FROM trace_journal").fetchone()[0] == count
    assert db.execute("SELECT COUNT(*) FROM trace_spool").fetchone()[0] == count
    db.execute("DROP TRIGGER owned_finish_failure")
    assert finish(store, token, params)["status"] == "ok"


@pytest.mark.parametrize(
    "terminal,closure", [("failed", "failed"), ("rejected", "unknown")]
)
def test_task_closure_cannot_claim_success_or_change_task_outcome(
    setup, terminal, closure
):
    store, token, session, _roots = setup
    task = store.create_task(
        sender_id="origin", recipient_id="agent-a", payload={}, queue_transport=False
    )
    for state, actor in [
        ("offered", "edgecitadel-system"),
        ("accepted", "agent-a"),
        ("running", "agent-a"),
    ]:
        store.transition_task(
            task_id=task["task_id"],
            state=state,
            actor_id=actor,
            session_id=session,
            queue_transport=False,
        )
    root = store.bind_trace(
        node_id="edge-a",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": task["task_id"],
            "context_id": None,
        },
    )["result"]
    with pytest.raises(TraceContractError, match="task_outcome_mismatch"):
        finish(store, token, finish_request(root, "completed"))
    assert store.get_task(task["task_id"])["state"] == "running"
    store.transition_task(
        task_id=task["task_id"],
        state=terminal,
        actor_id="agent-a",
        session_id=session,
        queue_transport=False,
    )
    with pytest.raises(TraceContractError, match="task_outcome_mismatch"):
        finish(store, token, finish_request(root, "completed"))
    assert finish(store, token, finish_request(root, closure))["status"] == "ok"
    assert store.get_task(task["task_id"])["state"] == terminal
