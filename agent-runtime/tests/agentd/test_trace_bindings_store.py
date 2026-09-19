import sqlite3
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_contract import TraceContractError


@pytest.fixture
def configured(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="agent-a",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    try:
        yield store, token, session
    finally:
        store.close()


def request(session, task_id=None):
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "session_id": session,
        "task_id": task_id,
        "context_id": None,
    }


def bind(store, token, params):
    return store.bind_trace(
        node_id="edge-a", connector_id="native", token=token, params=params
    )


def counts(store):
    return tuple(
        store._connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        for name in (
            "tasks",
            "trace_bindings",
            "trace_requests",
            "trace_journal",
            "trace_spool",
        )
    )


def test_native_binding_retry_survives_restart_without_creating_work(configured):
    store, token, session = configured
    params = request(session)
    first = bind(store, token, params)
    assert counts(store) == (0, 1, 1, 1, 1)
    assert bind(store, token, params) == first
    reopened = AgentdStore(store.path)
    try:
        assert bind(reopened, token, params) == first
        assert counts(reopened) == (0, 1, 1, 1, 1)
    finally:
        reopened.close()
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        bind(store, token, {**params, "context_id": str(uuid4())})
    assert counts(store) == (0, 1, 1, 1, 1)


def test_simultaneous_retry_creates_one_binding_and_one_event(configured):
    store, token, session = configured
    params = request(session)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: bind(store, token, params), range(12)))
    assert all(result == results[0] for result in results)
    assert counts(store) == (0, 1, 1, 1, 1)


def test_binding_spool_failure_rolls_back_every_new_row(configured):
    store, token, session = configured
    store._connection.execute(
        "CREATE TRIGGER owned_fail BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT, 'owned failure'); END"
    )
    params = request(session)
    with pytest.raises(sqlite3.IntegrityError, match="owned failure"):
        bind(store, token, params)
    assert counts(store) == (0, 0, 0, 0, 0)
    store._connection.execute("DROP TRIGGER owned_fail")
    bind(store, token, params)
    assert counts(store) == (0, 1, 1, 1, 1)


def test_revoked_or_closed_session_cannot_retrieve_binding_by_retry(configured):
    store, token, session = configured
    params = request(session)
    bind(store, token, params)
    store.close_session(connector_id="native", token=token, session_id=session)
    with pytest.raises(TraceContractError, match="session_unavailable"):
        bind(store, token, params)
    new_session = store.open_session(connector_id="native", token=token)["session_id"]
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        bind(store, token, {**params, "session_id": new_session})
    store.revoke_connector("native")
    with pytest.raises(StoreError, match="authentication failed"):
        bind(store, token, request(new_session))
    assert counts(store) == (0, 1, 1, 2, 2)


def test_task_binding_reuses_claim_attempt_and_rejects_foreign_task(configured):
    store, token, session = configured
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
    first_params = request(session, task["task_id"])
    first = bind(store, token, first_params)
    second = bind(store, token, request(session, task["task_id"]))
    assert first["result"] == second["result"]
    assert counts(store) == (1, 1, 2, 1, 1)
    other_session = store.open_session(connector_id="native", token=token)["session_id"]
    with pytest.raises(TraceContractError, match="execution_not_owned"):
        bind(store, token, request(other_session, task["task_id"]))
    foreign = store.create_task(
        sender_id="agent-a", recipient_id="agent-b", payload={}, queue_transport=False
    )
    with pytest.raises(TraceContractError, match="execution_not_owned"):
        bind(store, token, request(session, foreign["task_id"]))
    store.transition_task(
        task_id=task["task_id"],
        state="completed",
        actor_id="agent-a",
        session_id=session,
        queue_transport=False,
    )
    assert bind(store, token, first_params) == first
    with pytest.raises(TraceContractError, match="execution_not_active"):
        bind(store, token, request(session, task["task_id"]))


def test_expired_session_and_removed_capability_are_rechecked(configured):
    store, token, session = configured
    params = request(session)
    bind(store, token, params)
    with store._connection:
        store._connection.execute(
            "UPDATE sessions SET lease_expires_at_ms=1 WHERE session_id=?", (session,)
        )
    with pytest.raises(TraceContractError, match="session_unavailable"):
        bind(store, token, params)
    session = store.open_session(connector_id="native", token=token)["session_id"]
    with store._connection:
        store._connection.execute(
            "UPDATE connectors SET capabilities_json=? WHERE connector_id='native'",
            ('{"items":[]}',),
        )
    with pytest.raises(TraceContractError, match="trace_not_authorized"):
        bind(store, token, request(session))
    assert counts(store) == (0, 1, 1, 1, 1)


def test_v7_journal_survives_binding_migration_and_injected_failure(configured):
    store, token, session = configured
    bind(store, token, request(session))
    path = store.path
    with store._connection:
        journal = [
            tuple(row)
            for row in store._connection.execute("SELECT * FROM trace_journal")
        ]
        spool = [
            tuple(row) for row in store._connection.execute("SELECT * FROM trace_spool")
        ]
        store._connection.execute("DROP TABLE trace_task_contexts")
        store._connection.execute("DROP TABLE trace_operations")
        store._connection.execute("DROP TABLE trace_requests")
        store._connection.execute("DROP TABLE trace_bindings")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=7")
    captured = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source):
            captured.append(self._connection)
            super()._execute_migration_sql(source)
            raise RuntimeError("owned binding migration failure")

    try:
        with pytest.raises(RuntimeError, match="owned binding migration failure"):
            FailingStore(path)
    finally:
        for connection in captured:
            connection.close()
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 7
        assert not db.execute(
            "SELECT name FROM sqlite_master WHERE name='trace_bindings'"
        ).fetchall()
        assert db.execute("SELECT * FROM trace_journal").fetchall() == journal
        assert db.execute("SELECT * FROM trace_spool").fetchall() == spool
    migrated = AgentdStore(path)
    try:
        assert migrated._connection.execute("PRAGMA user_version").fetchone()[0] == 27
        assert (
            migrated._connection.execute(
                "SELECT COUNT(*) FROM trace_bindings"
            ).fetchone()[0]
            == 0
        )
        assert [
            tuple(row)
            for row in migrated._connection.execute("SELECT * FROM trace_journal")
        ] == journal
    finally:
        migrated.close()


@pytest.mark.parametrize("with_task", [False, True])
def test_physical_pressure_stops_new_receipts_but_preserves_retry(
    configured, monkeypatch, with_task
):
    from test_trace_crash import snapshot

    from edgecitadel_agentd import trace_capacity

    store, token, session = configured
    task_id = None
    if with_task:
        task = store.create_task(
            sender_id="origin",
            recipient_id="agent-a",
            payload={},
            queue_transport=False,
        )
        task_id = task["task_id"]
        for state, actor in [
            ("offered", "edgecitadel-system"),
            ("accepted", "agent-a"),
            ("running", "agent-a"),
        ]:
            store.transition_task(
                task_id=task_id,
                state=state,
                actor_id=actor,
                session_id=session,
                queue_transport=False,
            )
    original = request(session, task_id)
    committed = bind(store, token, original)
    before = snapshot(store)
    pressure = trace_capacity.physical_storage(store._connection)["pressure_bytes"]
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", pressure)
    new_request = request(session, task_id)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        bind(store, token, new_request)
    assert snapshot(store) == before
    assert bind(store, token, original) == committed
    assert snapshot(store) == before
    expected_error = "binding_context_mismatch" if with_task else "idempotency_conflict"
    with pytest.raises(TraceContractError, match=expected_error):
        bind(store, token, {**original, "context_id": str(uuid4())})
    monkeypatch.setattr(
        trace_capacity, "PHYSICAL_PRESSURE_BYTES", pressure + 1024 * 1024
    )
    recovered = bind(store, token, new_request)
    if with_task:
        assert recovered["result"] == committed["result"]
    else:
        assert recovered["result"]["binding_id"] != committed["result"]["binding_id"]
    assert not store._connection.execute("PRAGMA foreign_key_check").fetchall()


def test_pressure_measurement_failure_is_fixed_error_and_does_not_break_retry(
    configured, monkeypatch
):
    from test_trace_crash import snapshot

    from edgecitadel_agentd import trace_capacity

    store, token, session = configured
    original = request(session)
    committed = bind(store, token, original)
    before = snapshot(store)

    def unavailable(_db):
        raise OSError("SECRET_SENTINEL")

    monkeypatch.setattr(trace_capacity, "physical_storage", unavailable)
    with pytest.raises(TraceContractError, match="storage_unavailable") as error:
        bind(store, token, request(session))
    assert "SECRET_SENTINEL" not in str(error.value)
    assert snapshot(store) == before
    assert bind(store, token, original) == committed


def test_reused_binding_receipts_hit_real_pinned_wal_pressure(configured, monkeypatch):
    from test_trace_crash import snapshot

    from edgecitadel_agentd import trace_capacity

    store, token, session = configured
    db = store._connection
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
    original = request(session, task["task_id"])
    committed = bind(store, token, original)
    db.execute("PRAGMA journal_mode=WAL").fetchall()
    db.execute("PRAGMA main.wal_checkpoint(TRUNCATE)")
    baseline = trace_capacity.physical_storage(db)
    limit = baseline["pressure_bytes"] + 64 * 1024
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", limit)
    reader = sqlite3.connect(store.path)
    try:
        reader.execute("BEGIN")
        initial_receipts = reader.execute(
            "SELECT COUNT(*) FROM trace_requests"
        ).fetchone()[0]
        for _ in range(128):
            before = snapshot(store)
            fresh = request(session, task["task_id"])
            try:
                assert bind(store, token, fresh)["result"] == committed["result"]
            except TraceContractError as error:
                assert error.code == "quota_exceeded"
                assert snapshot(store) == before
                break
        else:
            pytest.fail("receipt-only WAL growth did not close admission")
        physical = trace_capacity.physical_storage(db)
        assert physical["pressure_bytes"] >= limit
        wal = store.path.with_name(store.path.name + "-wal").stat()
        assert max(wal.st_size, wal.st_blocks * 512) >= 64 * 1024
        assert counts(store)[1] == 1  # One execution binding, many retry receipts.
        assert counts(store)[2] > initial_receipts
        assert counts(store)[3:] == (1, 1)  # No journal or spool growth.
        assert bind(store, token, original) == committed
        assert snapshot(store) == before
        store.reconcile()
        assert (
            reader.execute("SELECT COUNT(*) FROM trace_requests").fetchone()[0]
            == initial_receipts
        )
        assert trace_capacity.physical_storage(db)["pressure_bytes"] >= limit
        with pytest.raises(TraceContractError, match="quota_exceeded"):
            bind(store, token, fresh)
        reader.rollback()
        store.reconcile()
        assert trace_capacity.physical_storage(db)["wal_file_bytes"] == 0
        assert trace_capacity.physical_storage(db)["pressure_bytes"] < limit
        assert bind(store, token, fresh)["result"] == committed["result"]
        assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        reader.close()
