import json
import sqlite3
from contextlib import closing
from uuid import uuid4

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_correlation import TaskTraceContext


@pytest.fixture
def configured(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="agent-a",
        capabilities=["edgecitadel_trace", "edgecitadel_delegate"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    binding = store.bind_trace(
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
    try:
        yield store, token, binding
    finally:
        store.close()


def request(binding):
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": binding["binding_id"],
        "recipient_id": "worker",
        "request": "private request body",
        "skill_id": None,
        "deadline_at_ms": None,
    }


def dispatch(store, token, params):
    return store.dispatch_trace(
        node_id="edge-a", connector_id="native", token=token, params=params
    )


def snapshot(store):
    return {
        table: [
            tuple(row) for row in store._connection.execute(f"SELECT * FROM {table}")
        ]
        for table in (
            "tasks",
            "transport_outbox",
            "trace_task_contexts",
            "trace_requests",
            "trace_journal",
            "trace_spool",
            "trace_sources",
            "trace_export_generations",
        )
    }


@pytest.mark.parametrize("allowed", [True, False])
def test_dispatch_pressure_preserves_receipts_and_rejects_new_work(
    configured, monkeypatch, allowed
):
    from edgecitadel_agentd import trace_capacity

    store, token, binding = configured
    if not allowed:
        with store._connection:
            store._connection.execute(
                "UPDATE connectors SET capabilities_json=? WHERE connector_id='native'",
                (json.dumps({"items": ["edgecitadel_trace"]}),),
            )
    original = request(binding)
    committed = dispatch(store, token, original)
    before = "\n".join(store._connection.iterdump())
    pressure = trace_capacity.physical_storage(store._connection)["pressure_bytes"]
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", pressure)
    fresh = request(binding)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        dispatch(store, token, fresh)
    assert "\n".join(store._connection.iterdump()) == before
    assert dispatch(store, token, original) == committed
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        dispatch(store, token, {**original, "request": "changed"})
    assert "\n".join(store._connection.iterdump()) == before
    monkeypatch.setattr(
        trace_capacity, "PHYSICAL_PRESSURE_BYTES", pressure + 1024 * 1024
    )
    resumed = dispatch(store, token, fresh)
    assert resumed["status"] == committed["status"]
    assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == (
        2 if allowed else 0
    )


def test_dispatch_pressure_inspection_failure_preserves_retry(configured, monkeypatch):
    from edgecitadel_agentd import trace_capacity

    store, token, binding = configured
    original = request(binding)
    committed = dispatch(store, token, original)
    before = "\n".join(store._connection.iterdump())

    def unavailable(_db):
        raise OSError("SECRET_SENTINEL")

    monkeypatch.setattr(trace_capacity, "physical_storage", unavailable)
    with pytest.raises(TraceContractError, match="storage_unavailable") as error:
        dispatch(store, token, request(binding))
    assert "SECRET_SENTINEL" not in str(error.value)
    assert dispatch(store, token, original) == committed
    assert "\n".join(store._connection.iterdump()) == before


@pytest.mark.parametrize("allowed", [True, False])
def test_dispatch_pinned_wal_pressure_and_recovery(configured, monkeypatch, allowed):
    from edgecitadel_agentd import trace_capacity

    store, token, binding = configured
    db = store._connection
    if not allowed:
        with db:
            db.execute(
                "UPDATE connectors SET capabilities_json=? WHERE connector_id='native'",
                (json.dumps({"items": ["edgecitadel_trace"]}),),
            )
    original = request(binding)
    committed = dispatch(store, token, original)
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    limit = trace_capacity.physical_storage(db)["pressure_bytes"] + 192 * 1024
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", limit)
    with closing(sqlite3.connect(store.path)) as reader:
        reader.execute("BEGIN")
        initial = reader.execute("SELECT COUNT(*) FROM trace_requests").fetchone()[0]
        for _ in range(128):
            before = "\n".join(db.iterdump())
            fresh = request(binding)
            try:
                dispatch(store, token, fresh)
            except TraceContractError as error:
                assert error.code == "quota_exceeded"
                break
        else:
            pytest.fail("dispatch WAL growth did not close admission")
        assert "\n".join(db.iterdump()) == before
        assert dispatch(store, token, original) == committed
        assert "\n".join(db.iterdump()) == before
        assert trace_capacity.physical_storage(db)["pressure_bytes"] >= limit
        store.reconcile()
        assert (
            reader.execute("SELECT COUNT(*) FROM trace_requests").fetchone()[0]
            == initial
        )
        with pytest.raises(TraceContractError, match="quota_exceeded"):
            dispatch(store, token, fresh)
        reader.rollback()
        store.reconcile()
        assert trace_capacity.physical_storage(db)["wal_file_bytes"] == 0
        assert trace_capacity.physical_storage(db)["pressure_bytes"] < limit
        assert dispatch(store, token, fresh)["status"] == committed["status"]
    assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert not db.execute("PRAGMA foreign_key_check").fetchall()


def test_dispatch_pressure_still_allows_binding_closure(configured, monkeypatch):
    from edgecitadel_agentd import trace_capacity

    store, token, binding = configured
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 1)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        dispatch(store, token, request(binding))
    reply = store.finish_trace(
        node_id="edge-a",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "binding_id": binding["binding_id"],
            "outcome": "unknown",
            "reason": "unknown",
        },
    )
    assert reply["status"] == "ok"
    assert (
        store._connection.execute(
            "SELECT closed_at_ms FROM trace_bindings WHERE binding_id=?",
            (binding["binding_id"],),
        ).fetchone()[0]
        is not None
    )


def test_native_dispatch_retry_and_wire_context(configured):
    store, token, binding = configured
    params = request(binding)
    reply = dispatch(store, token, params)
    assert reply["status"] == "ok"
    result = reply["result"]
    assert result["parent_run_id"] == binding["trace_id"]
    wire = store.pending_transport()[0]["envelope"]
    context = TaskTraceContext.from_envelope(wire)
    assert context.task_id == result["task_id"]
    assert context.trace_id == binding["trace_id"]
    assert context.parent_run_id == binding["trace_id"]
    assert context.hop_count == 0
    before = snapshot(store)
    assert dispatch(store, token, params) == reply
    assert snapshot(store) == before
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        dispatch(store, token, {**params, "request": "changed"})
    assert snapshot(store) == before
    exported = "".join(
        row[0]
        for row in store._connection.execute("SELECT event_json FROM trace_journal")
    )
    assert params["request"] not in exported


def test_denied_dispatch_is_durable_and_prior_receipt_survives_grant_change(configured):
    store, token, binding = configured
    params = request(binding)
    first = dispatch(store, token, params)
    with store._connection:
        store._connection.execute(
            "UPDATE connectors SET capabilities_json=? WHERE connector_id='native'",
            (json.dumps({"items": ["edgecitadel_trace"]}),),
        )
    assert dispatch(store, token, params) == first
    denied = request(binding)
    reply = dispatch(store, token, denied)
    assert reply["code"] == "not_authorized"
    assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
    before = snapshot(store)
    assert dispatch(store, token, denied) == reply
    assert snapshot(store) == before
    events = [
        json.loads(row[0])
        for row in store._connection.execute("SELECT event_json FROM trace_journal")
    ]
    assert [(e["kind"], e["phase"]) for e in events][-2:] == [
        ("permission", "denied"),
        ("dispatch", "denied"),
    ]


def test_receipt_failure_rolls_back_child_outbox_and_all_evidence(configured):
    store, token, binding = configured
    before = snapshot(store)
    store._connection.execute(
        "CREATE TRIGGER owned_fail BEFORE INSERT ON trace_requests WHEN NEW.operation='dispatch' BEGIN SELECT RAISE(ABORT, 'owned failure'); END"
    )
    params = request(binding)
    with pytest.raises(sqlite3.IntegrityError, match="owned failure"):
        dispatch(store, token, params)
    assert snapshot(store) == before
    store._connection.execute("DROP TRIGGER owned_fail")
    assert dispatch(store, token, params)["status"] == "ok"


def test_receiver_persists_ancestry_for_next_dispatch_and_result(configured, tmp_path):
    store, token, binding = configured
    first = dispatch(store, token, request(binding))["result"]
    wire = store.pending_transport()[0]["envelope"]
    with closing(AgentdStore(tmp_path / "receiver.sqlite3")) as receiver:
        receiver_token = receiver.register_connector(
            connector_id="native",
            host_type="codex",
            agent_id="worker",
            capabilities=["edgecitadel_trace", "edgecitadel_delegate"],
        )
        session = receiver.open_session(connector_id="native", token=receiver_token)[
            "session_id"
        ]
        receiver.ingest_transport_envelope(wire)
        for state in ("accepted", "running"):
            receiver.transition_task(
                task_id=first["task_id"],
                state=state,
                actor_id="worker",
                session_id=session,
            )
        child_binding = receiver.bind_trace(
            node_id="edge-b",
            connector_id="native",
            token=receiver_token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session,
                "task_id": first["task_id"],
                "context_id": None,
            },
        )["result"]
        second = dispatch(
            receiver,
            receiver_token,
            {**request(child_binding), "recipient_id": "third"},
        )["result"]
        child_wire = next(
            item["envelope"]
            for item in receiver.pending_transport()
            if item["task_id"] == second["task_id"]
        )
        context = TaskTraceContext.from_envelope(child_wire)
        assert context.parent_task_id == first["task_id"]
        assert context.parent_run_id is None
        assert context.hop_count == 1
        assert context.trace_id == first["trace_id"]
        assert context.context_id == first["context_id"]
        receiver.transition_task(
            task_id=first["task_id"],
            state="completed",
            actor_id="worker",
            session_id=session,
        )
        result_wire = [
            item["envelope"]
            for item in receiver.pending_transport()
            if item["task_id"] == first["task_id"]
        ][-1]
        assert TaskTraceContext.from_envelope(
            result_wire
        ) == TaskTraceContext.from_envelope(wire)


def test_managed_package_grant_controls_new_dispatch(tmp_path):
    with closing(AgentdStore(tmp_path / "managed.sqlite3")) as store:
        token = store.register_connector(
            connector_id="managed-agent-a",
            host_type="managed-agent",
            agent_id="agent-a",
            capabilities=[],
        )
        record = {
            "package_id": "edgecitadel.test",
            "desired_state": "running",
            "agent_ids": ["agent-a"],
            "outbound_agents": ["worker"],
        }
        store.reconcile_managed_agents([record])
        session = store.open_session(connector_id="managed-agent-a", token=token)[
            "session_id"
        ]
        bound = store.bind_trace(
            node_id="edge-a",
            connector_id="managed-agent-a",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session,
                "task_id": None,
                "context_id": None,
            },
        )["result"]
        params = request(bound)

        def send(params):
            return store.dispatch_trace(
                node_id="edge-a",
                connector_id="managed-agent-a",
                token=token,
                params=params,
            )

        first = send(params)
        assert first["status"] == "ok"
        store.reconcile_managed_agents([{**record, "outbound_agents": []}])
        assert send(params) == first
        assert send(request(bound))["code"] == "not_authorized"
        assert (
            store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        )


def test_v9_migration_preserves_existing_journal(configured):
    store, token, binding = configured
    path = store.path
    before = [
        tuple(row) for row in store._connection.execute("SELECT * FROM trace_journal")
    ]
    with store._connection:
        store._connection.execute("DROP TABLE trace_task_contexts")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        store._connection.execute("PRAGMA user_version=9")
    with closing(AgentdStore(path)) as migrated:
        assert migrated._connection.execute("PRAGMA user_version").fetchone()[0] == 22
        assert [
            tuple(row)
            for row in migrated._connection.execute("SELECT * FROM trace_journal")
        ] == before
        assert dispatch(migrated, token, request(binding))["status"] == "ok"


def test_claim_acceptance_failure_rolls_back_offering_and_claim(configured):
    store, token, _binding = configured
    session = store.open_session(connector_id="native", token=token)["session_id"]
    task = store.create_task(sender_id="origin", recipient_id="agent-a", payload={})
    before = snapshot(store)
    events = [tuple(row) for row in store._connection.execute("SELECT * FROM events")]
    store._connection.execute(
        "CREATE TRIGGER owned_claim_fail BEFORE INSERT ON task_attempts "
        "WHEN NEW.state='accepted' BEGIN SELECT RAISE(ABORT, 'owned claim failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned claim failure"):
        store.claim_next_task(connector_id="native", token=token, session_id=session)
    assert snapshot(store) == before
    assert [
        tuple(row) for row in store._connection.execute("SELECT * FROM events")
    ] == events
    assert store.get_task(task["task_id"])["state"] == "queued"
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM task_attempts WHERE task_id=?", (task["task_id"],)
        ).fetchone()[0]
        == 0
    )
    store._connection.execute("DROP TRIGGER owned_claim_fail")
    claimed = store.claim_next_task(
        connector_id="native", token=token, session_id=session
    )
    assert claimed["state"] == "accepted"


@pytest.mark.parametrize(
    ("change", "error"),
    [
        ("connector", "connector authentication failed"),
        ("session", "session_unavailable"),
        ("lease", "session_unavailable"),
        ("trace_capability", "trace_not_authorized"),
        ("binding", "binding_closed"),
    ],
)
def test_dispatch_receipt_does_not_bypass_current_caller_authority(
    configured, change, error
):
    from edgecitadel_agentd.store import StoreError

    store, token, binding = configured
    params = request(binding)
    committed = dispatch(store, token, params)
    task_id = committed["result"]["task_id"]
    task_before = store.get_task(task_id)
    if change == "connector":
        store.revoke_connector("native")
    elif change == "session":
        session_id = store._connection.execute(
            "SELECT session_id FROM trace_bindings WHERE binding_id=?",
            (binding["binding_id"],),
        ).fetchone()[0]
        store.close_session(connector_id="native", token=token, session_id=session_id)
    elif change == "lease":
        with store._connection:
            store._connection.execute("UPDATE sessions SET lease_expires_at_ms=0")
    elif change == "trace_capability":
        # Model a trusted administrative capability change; the connector's own
        # token intentionally cannot alter its grants.
        with store._connection:
            store._connection.execute(
                "UPDATE connectors SET capabilities_json=? WHERE connector_id='native'",
                (json.dumps({"items": ["edgecitadel_delegate"]}),),
            )
    else:
        store.finish_trace(
            node_id="edge-a",
            connector_id="native",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": binding["binding_id"],
                "outcome": "unknown",
                "reason": "unknown",
            },
        )
    before = list(store._connection.iterdump())
    for candidate in (params, request(binding)):
        with pytest.raises((StoreError, TraceContractError), match=error):
            dispatch(store, token, candidate)
        assert list(store._connection.iterdump()) == before
    assert store.get_task(task_id) == task_before
    assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
