from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.service import dispatch
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_contract import TraceContractError, validate_event
from edgecitadel_agentd.trace_security import (
    MAX_REJECTION_COUNT,
    flush_authentication_rejections,
    note_authentication_rejection,
)


@pytest.fixture
def store(tmp_path):
    (tmp_path / "node.json").write_text(json.dumps({"agent_id": "node-a"}))
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    yield store
    store.close()


def events(store):
    return [
        json.loads(row[0])
        for row in store._connection.execute(
            "SELECT event_json FROM trace_journal ORDER BY source_seq"
        )
    ]


def test_concurrent_anonymous_failures_coalesce_without_input_identity(store):
    def denied(index):
        request = {
            "version": 1,
            "operation": "trace.bind",
            "params": {"task_id": f"task-sentinel-{index}"},
            "connector_id": f"actor-sentinel-{index}",
            "token": "secret-sentinel",
        }
        if index % 3 == 0:
            request.pop("token")
        elif index % 3 == 1:
            request.update(operation="trace.import", admin_token="secret-sentinel")
        with pytest.raises(StoreError):
            dispatch(store, request, admin_token="owned-admin")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(denied, range(100)))
    assert events(store) == []
    assert store._authentication_rejections == 100
    assert flush_authentication_rejections(store, now_ms=120001)
    (event,) = events(store)
    validate_event(event)
    assert event["attributes"] == {"reason": "authentication_failed", "count": 100}
    for key in (
        "agent_id",
        "trace_id",
        "task_id",
        "context_id",
        "parent_task_id",
        "parent_run_id",
        "execution_attempt_id",
        "span_id",
        "parent_span_id",
        "duration_ms",
        "supersedes_event_id",
    ):
        assert event[key] is None
    assert event["causes"] == [] and event["evidence_kind"] == "source_observed"
    assert "sentinel" not in "\n".join(store._connection.iterdump())
    for table in ("tasks", "trace_bindings", "sessions"):
        assert (
            store._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            == 0
        )
    assert (
        store._connection.execute("SELECT count(*) FROM trace_spool").fetchone()[0] == 1
    )


def test_minute_restart_clock_rollback_and_counter_saturation(store):
    note_authentication_rejection(store)
    assert flush_authentication_rejections(store, now_ms=120000)
    original = events(store)
    reopened = AgentdStore(store.path)
    try:
        reopened._authentication_rejections = MAX_REJECTION_COUNT
        note_authentication_rejection(reopened)
        assert reopened._authentication_rejections == MAX_REJECTION_COUNT
        assert not flush_authentication_rejections(reopened, now_ms=179999)
        assert events(reopened) == original
        assert flush_authentication_rejections(reopened, now_ms=180000)
        assert events(reopened)[1]["attributes"]["count"] == MAX_REJECTION_COUNT
        note_authentication_rejection(reopened)
        assert not flush_authentication_rejections(reopened, now_ms=120001)
        assert reopened._authentication_rejections == 1
        assert flush_authentication_rejections(reopened, now_ms=240000)
        assert len(events(reopened)) == 3
    finally:
        reopened.close()


def test_failed_spool_write_rolls_back_and_preserves_pending_count(store):
    note_authentication_rejection(store)
    store._connection.execute(
        "CREATE TRIGGER owned_failure BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT,'owned fault'); END"
    )
    before = list(store._connection.iterdump())
    with pytest.raises(sqlite3.IntegrityError):
        flush_authentication_rejections(store, now_ms=120000)
    assert list(store._connection.iterdump()) == before
    assert store._authentication_rejections == 1
    store._connection.execute("DROP TRIGGER owned_failure")
    assert flush_authentication_rejections(store, now_ms=120000)
    assert store._authentication_rejections == 0
    assert not flush_authentication_rejections(store, now_ms=120000)


def test_reserved_capacity_failure_never_authorizes_or_discards_count(
    store, monkeypatch
):
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 0)
    note_authentication_rejection(store)
    assert flush_authentication_rejections(store, now_ms=120000)
    monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
    with pytest.raises(StoreError):
        dispatch(store, {"version": 1, "operation": "trace.bind"})
    before = list(store._connection.iterdump())
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        flush_authentication_rejections(store, now_ms=180000)
    assert list(store._connection.iterdump()) == before
    assert store._authentication_rejections == 1


def test_authenticated_permission_denial_is_not_anonymous(store):
    token = store.register_connector(
        connector_id="reader", host_type="codex", agent_id="reader", capabilities=[]
    )
    with pytest.raises(StoreError, match="not authorized"):
        dispatch(
            store,
            {
                "version": 1,
                "operation": "trace.bind",
                "connector_id": "reader",
                "token": token,
            },
        )
    assert store._authentication_rejections == 0
    assert not flush_authentication_rejections(store, now_ms=120000)


def test_missing_node_is_fixed_error_and_volatile_count_is_not_lifetime_total(store):
    (store.path.parent.parent / "node.json").unlink()
    note_authentication_rejection(store)
    with pytest.raises(TraceContractError, match="storage_unavailable"):
        flush_authentication_rejections(store, now_ms=120000)
    assert store._authentication_rejections == 1
    reopened = AgentdStore(store.path)
    try:
        assert reopened._authentication_rejections == 0
        assert events(reopened) == []
    finally:
        reopened.close()


def test_owned_socket_reconciler_persists_failure_without_caller_metadata(tmp_path):
    import threading
    import time

    from edgecitadel_agentd.client import AgentdClient, AgentdClientError
    from edgecitadel_agentd.service import serve, socket_path_for

    state = tmp_path / "agentd"
    (tmp_path / "node.json").write_text(json.dumps({"agent_id": "node-a"}))
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(state, stop), daemon=True)
    thread.start()
    try:
        socket = socket_path_for(state)
        for _ in range(200):
            if socket.exists():
                break
            assert thread.is_alive()
            stop.wait(0.01)
        else:
            pytest.fail("owned audit service did not start")
        caller = AgentdClient(
            socket, connector_id="actor-sentinel", token="secret-sentinel"
        )
        with pytest.raises(AgentdClientError, match="authentication failed"):
            caller.call("trace.bind", task_id="task-sentinel")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with sqlite3.connect(state / "agentd.sqlite3") as db:
                rows = db.execute(
                    "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='security'"
                ).fetchall()
            if rows:
                break
            stop.wait(0.05)
        assert len(rows) == 1
        event = json.loads(rows[0][0])
        assert event["attributes"]["count"] == 1
        assert "sentinel" not in rows[0][0]
    finally:
        stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()


def test_rate_receipts_are_protected_from_production_pruning(store):
    from edgecitadel_agentd.trace_retention import prune_active_history

    note_authentication_rejection(store)
    assert flush_authentication_rejections(store, now_ms=120000)
    before = events(store)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        assert (
            prune_active_history(store._connection, node_id="node-a", now_ms=180000)
            == 0
        )
    assert events(store) == before
    note_authentication_rejection(store)
    assert not flush_authentication_rejections(store, now_ms=120000)


@pytest.mark.parametrize("token", ["non-ascii-秘密", "\ud800"])
def test_non_ascii_credentials_are_counted_as_rejections(store, token):
    for request in (
        {"version": 1, "operation": "connector.list", "admin_token": token},
        {
            "version": 1,
            "operation": "trace.bind",
            "connector_id": "unknown",
            "token": token,
        },
    ):
        with pytest.raises(StoreError, match="authentication"):
            dispatch(store, request, admin_token="owned-admin")
    assert store._authentication_rejections == 2
