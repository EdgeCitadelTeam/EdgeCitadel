"""Historical import exercises the actual management route and store transaction."""

from __future__ import annotations

import json
import sqlite3
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection, paired_connect

import pytest

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.service import dispatch
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_import import configure_import, import_trace


@pytest.fixture
def store(tmp_path):
    (tmp_path / "node.json").write_text(json.dumps({"agent_id": "node-a"}))
    result = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    yield result
    result.close()


def request():
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    event = deepcopy(next(item["event"] for item in fixtures if item["name"] == "tool"))
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
    return {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "record_id": str(uuid4()),
        "historical_run_id": str(uuid4()),
        "import_source_id": "archive-a",
        "agent_id": "worker-a",
        "observation": {
            key: value for key, value in event.items() if key not in excluded
        },
    }


def grant(store, enabled=True):
    return configure_import(
        store,
        administrator_authenticated=True,
        params={
            "import_source_id": "archive-a",
            "agent_id": "worker-a",
            "enabled": enabled,
        },
    )


def record(store, params):
    return import_trace(
        store, node_id="node-a", administrator_authenticated=True, params=params
    )


def snapshot(store):
    return list(store._connection.iterdump())


def test_admin_route_without_live_session_and_restart(store):
    params = request()

    def call(operation, params):
        return dispatch(
            store,
            {
                "version": 1,
                "operation": operation,
                "params": params,
                "admin_token": "owned-admin",
            },
            admin_token="owned-admin",
        )

    call(
        "trace.import.configure",
        {"import_source_id": "archive-a", "agent_id": "worker-a", "enabled": True},
    )
    first = call("trace.import", params)
    db = store._connection
    for table in ("tasks", "sessions", "trace_bindings", "transport_outbox"):
        assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    event = json.loads(db.execute("SELECT event_json FROM trace_journal").fetchone()[0])
    assert event["evidence_kind"] == "historical_import"
    assert event["task_id"] != params["observation"]["task_id"]
    assert db.execute("SELECT count(*) FROM trace_spool").fetchone()[0] == 1
    reopened = AgentdStore(store.path)
    try:
        assert record(reopened, params) == first
        retry = {**params, "request_id": str(uuid4())}
        assert record(reopened, retry) == {**first, "request_id": retry["request_id"]}
        assert (
            reopened._connection.execute(
                "SELECT count(*) FROM trace_journal"
            ).fetchone()[0]
            == 1
        )
    finally:
        reopened.close()


def test_authority_revocation_conflict_and_namespace_preservation(store):
    params = request()
    with pytest.raises(TraceContractError, match="import_not_authorized"):
        record(store, params)
    grant(store)
    first = record(store, params)
    namespace = store._connection.execute(
        "SELECT namespace_id FROM trace_import_grants"
    ).fetchone()[0]
    changed = deepcopy(params)
    changed["observation"]["attributes"]["name"] = "different-tool"
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        record(store, changed)
    grant(store, False)
    with pytest.raises(TraceContractError, match="import_not_authorized"):
        record(store, params)
    grant(store)
    assert (
        store._connection.execute(
            "SELECT namespace_id FROM trace_import_grants"
        ).fetchone()[0]
        == namespace
    )
    assert record(store, params) == first
    for key in ("agent_id", "import_source_id"):
        with pytest.raises(TraceContractError, match="import_not_authorized"):
            record(store, {**params, key: "other"})
    with pytest.raises(StoreError, match="management authentication failed"):
        dispatch(
            store,
            {"version": 1, "operation": "trace.import", "params": params},
            admin_token="owned-admin",
        )


@pytest.mark.parametrize(
    "table", ["trace_journal", "trace_spool", "trace_import_records"]
)
def test_failure_rolls_back_source_event_spool_and_receipt(store, table):
    grant(store)
    store._connection.execute(
        f"CREATE TRIGGER fail_import BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'owned failure'); END"
    )
    before = snapshot(store)
    with pytest.raises(sqlite3.IntegrityError, match="owned failure"):
        record(store, request())
    assert snapshot(store) == before


def test_pressure_blocks_new_evidence_but_not_authorized_retries_or_revocation(
    store, monkeypatch
):
    grant(store)
    params = request()
    first = record(store, params)
    monkeypatch.setattr(trace_capacity, "PHYSICAL_PRESSURE_BYTES", 0)
    assert record(store, params) == first
    before = snapshot(store)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        record(store, {**params, "record_id": str(uuid4())})
    assert snapshot(store) == before
    grant(store, False)
    with pytest.raises(TraceContractError, match="import_not_authorized"):
        record(store, params)


def test_archive_content_and_unresolved_parent_are_not_live_claims(store):
    grant(store)
    params = request()
    params["observation"]["parent_task_id"] = None
    params["observation"]["parent_run_id"] = "a" * 32
    params["observation"]["attributes"].update(
        content_available=True, local_content_ref=str(uuid4())
    )
    record(store, params)
    event = json.loads(
        store._connection.execute("SELECT event_json FROM trace_journal").fetchone()[0]
    )
    assert event["parent_run_id"] is None
    assert event["attributes"].get("content_available") is not True
    assert "local_content_ref" not in event["attributes"]


def test_migration_from_v17_and_atomic_migration_failure(store):
    db = store._connection
    with db:
        db.execute("DROP TABLE trace_import_records")
        db.execute("DROP TABLE trace_import_grants")
        flatten_connection(db)
        db.execute("PRAGMA user_version=17")
    captured = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source):
            if "CREATE TABLE trace_import_grants" in source:
                captured.append(self._connection)
                super()._execute_migration_sql(source)
                raise RuntimeError("owned migration failure")
            super()._execute_migration_sql(source)

    try:
        with pytest.raises(RuntimeError, match="owned migration failure"):
            FailingStore(store.path)
    finally:
        for connection in captured:
            connection.close()
    assert db.execute("PRAGMA user_version").fetchone()[0] == 17
    assert (
        db.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'trace_import_%'"
        ).fetchall()
        == []
    )
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        grant(reopened)
        record(reopened, request())
    finally:
        reopened.close()


def test_local_history_is_agent_scoped_and_import_namespace_isolated(store):
    from edgecitadel_agentd.trace_history import read_history

    grant(store)
    params = request()
    record(store, params)
    configure_import(
        store,
        administrator_authenticated=True,
        params={
            "import_source_id": "archive-a",
            "agent_id": "worker-b",
            "enabled": True,
        },
    )
    record(store, {**params, "agent_id": "worker-b"})
    events = [
        json.loads(row[0])
        for row in store._connection.execute("SELECT event_json FROM trace_journal")
    ]
    for field in ("event_id", "trace_id", "span_id"):
        assert events[0][field] != events[1][field]
    token = store.register_connector(
        connector_id="reader",
        host_type="codex",
        agent_id="worker-a",
        capabilities=["edgecitadel_trace"],
    )
    history = read_history(store, connector_id="reader", token=token, params={})
    assert len(history["events"]) == 1
    assert history["events"][0]["agent_id"] == "worker-a"


def test_standard_error_reply_and_normal_quota_rollback(store, monkeypatch):
    params = request()
    reply = dispatch(
        store,
        {
            "version": 1,
            "operation": "trace.import",
            "admin_token": "owned-admin",
            "params": params,
        },
        admin_token="owned-admin",
    )
    assert reply["status"] == "error" and reply["code"] == "not_authorized"
    grant(store)
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 0)
    before = snapshot(store)
    with pytest.raises(TraceContractError, match="quota_exceeded"):
        record(store, params)
    assert snapshot(store) == before


def test_storage_probe_failure_is_retryable_and_does_not_echo_exception(
    store, monkeypatch
):
    grant(store)

    def fail(_db):
        raise OSError("private-path-sentinel")

    monkeypatch.setattr(trace_capacity, "physical_storage", fail)
    before = snapshot(store)
    result = dispatch(
        store,
        {
            "version": 1,
            "operation": "trace.import",
            "admin_token": "owned-admin",
            "params": request(),
        },
        admin_token="owned-admin",
    )
    assert result["code"] == "storage_unavailable"
    assert result["retryable"] is True
    assert "private-path-sentinel" not in json.dumps(result)
    assert snapshot(store) == before


def test_import_over_owned_socket_survives_service_restart(tmp_path):
    import threading

    from edgecitadel_agentd.client import AgentdClient, AgentdClientError
    from service_test_support import serve
    from edgecitadel_agentd.service import socket_path_for

    state = tmp_path / "agentd"
    (tmp_path / "node.json").write_text(json.dumps({"agent_id": "node-a"}))
    params = request()
    first = None
    for _ in range(2):
        stop = threading.Event()
        thread = threading.Thread(target=serve, args=(state, stop), daemon=True)
        thread.start()
        try:
            socket = socket_path_for(state)
            for _attempt in range(200):
                if socket.exists():
                    break
                assert thread.is_alive()
                stop.wait(0.01)
            else:
                pytest.fail("owned import service did not start")
            admin = AgentdClient(
                socket, admin_token=(state / "admin.token").read_text().strip()
            )
            with pytest.raises(AgentdClientError, match="authentication failed"):
                AgentdClient(socket).call("trace.import", **params)
            admin.call(
                "trace.import.configure",
                import_source_id="archive-a",
                agent_id="worker-a",
                enabled=True,
            )
            result = admin.call("trace.import", **params)
            assert result["status"] == "ok"
            if first is None:
                first = result
            else:
                assert result == first
            assert admin.call("health")["active_sessions"] == 0
        finally:
            stop.set()
            thread.join(timeout=10)
            assert not thread.is_alive()
    with paired_connect(state / "agentd.sqlite3") as db:
        assert (
            db.execute("SELECT count(*) FROM trace_import_records").fetchone()[0] == 1
        )
        assert (
            db.execute(
                "SELECT count(*) FROM trace_spool s JOIN trace_journal j ON j.node_id=s.node_id AND j.source_epoch=s.source_epoch AND j.event_id=s.journal_event_id WHERE json_extract(j.event_json,'$.evidence_kind')='historical_import'"
            ).fetchone()[0]
            == 1
        )
        for table in ("tasks", "sessions", "trace_bindings", "transport_outbox"):
            assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0


def test_pruned_import_retry_does_not_recreate_lost_payload(store):
    import time

    from edgecitadel_agentd.trace_retention import prune_active_history

    grant(store)
    params = request()
    first = record(store, params)
    db = store._connection
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert (
            prune_active_history(
                db, node_id="node-a", now_ms=time.time_ns() // 1_000_000
            )
            == 1
        )
    assert (
        db.execute(
            "SELECT 1 FROM trace_journal WHERE event_id=?",
            (first["result"]["event_id"],),
        ).fetchone()
        is None
    )
    assert (
        db.execute(
            "SELECT state FROM trace_spool WHERE event_id=?",
            (first["result"]["event_id"],),
        ).fetchone()[0]
        == "lost_with_marker"
    )
    before = snapshot(store)
    assert (
        record(store, {**params, "request_id": str(uuid4())})["result"]
        == first["result"]
    )
    assert snapshot(store) == before


def test_populated_import_fenced_restore_preserves_namespace_and_original_receipt(
    tmp_path,
):
    from storage_test_support import stage_restore
    from edgecitadel_agentd.restore import (
        RestorePendingError,
        require_startable,
    )

    old = tmp_path / "old"
    restored = tmp_path / "restored"
    original = AgentdStore(old / "agentd.sqlite3")
    params = request()
    try:
        grant(original)
        first = record(original, params)
        namespace = original._connection.execute(
            "SELECT namespace_id FROM trace_import_grants"
        ).fetchone()[0]
    finally:
        original.close()
    marker = stage_restore(
        snapshot_dir=old,
        previous_state_dir=old,
        destination_dir=restored,
        node_id="node-a",
        expected_source_epoch=first["result"]["source_epoch"],
    )
    for directory in (old, restored):
        with pytest.raises(RestorePendingError):
            require_startable(directory)
    # Inspect the fenced store through its internal API; do not activate services.
    imported = AgentdStore(restored / "agentd.sqlite3")
    try:
        before = snapshot(imported)
        assert record(imported, params) == first
        assert snapshot(imported) == before
        assert (
            imported._connection.execute(
                "SELECT namespace_id FROM trace_import_grants"
            ).fetchone()[0]
            == namespace
        )
        fresh = record(imported, {**params, "record_id": str(uuid4())})
        assert (
            fresh["result"]["source_epoch"]
            == marker["source_epoch"]
            != first["result"]["source_epoch"]
        )
        assert fresh["result"]["source_seq"] == marker["source_seq"] + 1
        grant(imported, False)
        with pytest.raises(TraceContractError, match="import_not_authorized"):
            record(imported, params)
        assert imported._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        imported.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("event_id", "untrusted-sentinel"),
        ("source_seq", 999),
        ("evidence_kind", "source_observed"),
        ("nested", {"token": "secret-sentinel"}),
    ],
)
def test_forged_archive_metadata_is_atomic_and_never_persisted(store, field, value):
    grant(store)
    params = request()
    params["observation"][field] = value
    before = snapshot(store)
    with pytest.raises(TraceContractError):
        record(store, params)
    assert snapshot(store) == before


def test_configure_cannot_override_namespace(store):
    before = snapshot(store)
    with pytest.raises(TraceContractError, match="invalid_import_grant"):
        configure_import(
            store,
            administrator_authenticated=True,
            params={
                "import_source_id": "archive-a",
                "agent_id": "worker-a",
                "enabled": True,
                "namespace_id": str(uuid4()),
            },
        )
    assert snapshot(store) == before


def test_final_stamped_size_is_revalidated_before_commit(store, monkeypatch):
    from edgecitadel_agentd import trace_contract

    grant(store)
    params = request()
    # Reduce the real bound to place this otherwise valid fixture at its edge.
    # The daemon's longer node ID expands the final event beyond that bound.
    monkeypatch.setattr(trace_contract, "MAX_EVENT_BYTES", 768)
    trace_contract.validate_import_request(params)
    before = snapshot(store)
    with pytest.raises(TraceContractError, match="oversize_record"):
        import_trace(
            store, node_id="n" * 64, administrator_authenticated=True, params=params
        )
    assert snapshot(store) == before


def test_concurrent_retries_allocate_one_record_and_sequence(store):
    from concurrent.futures import ThreadPoolExecutor

    grant(store)
    params = request()
    with ThreadPoolExecutor(max_workers=8) as pool:
        replies = list(
            pool.map(
                lambda _: record(store, {**params, "request_id": str(uuid4())}),
                range(24),
            )
        )
    assert all(reply["result"] == replies[0]["result"] for reply in replies)
    for table in ("trace_journal", "trace_spool", "trace_import_records"):
        assert (
            store._connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            == 1
        )
