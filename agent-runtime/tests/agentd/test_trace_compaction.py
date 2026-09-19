import sqlite3
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest
from test_trace_crash import append_request, prepare

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_compaction import compact_closed_execution
from edgecitadel_agentd.trace_contract import TraceContractError


def populated(tmp_path, *, close_session=True):
    store, token, session, binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    requests = []
    for parent in (None, "child"):
        params = append_request(binding)
        if parent:
            params["observation"]["parent_span_id"] = requests[0]["observation"][
                "span_id"
            ]
        store.append_trace(
            node_id="owned-edge", connector_id="owner", token=token, params=params
        )
        requests.append(params)
    dispatch = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": binding["binding_id"],
        "recipient_id": "worker",
        "request": "owned work",
        "skill_id": None,
        "deadline_at_ms": None,
    }
    store.dispatch_trace(
        node_id="owned-edge", connector_id="owner", token=token, params=dispatch
    )
    finish = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": binding["binding_id"],
        "outcome": "unknown",
        "reason": "unknown",
    }
    reply = store.finish_trace(
        node_id="owned-edge", connector_id="owner", token=token, params=finish
    )
    if close_session:
        store.close_session(
            connector_id="owner", token=token, session_id=session["session_id"]
        )
    with store._connection:
        store._connection.execute("UPDATE trace_bindings SET closed_at_ms=1")
        if close_session:
            store._connection.execute("UPDATE sessions SET closed_at_ms=1")
    return store, token, session, binding, requests, dispatch, finish, reply


def compact(store, limit=256, before=2):
    with store._lock, store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return compact_closed_execution(
            store._connection, before_ms=before, limit=limit
        )


def evidence(store):
    return {
        t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
        for t in (
            "tasks",
            "transport_outbox",
            "trace_journal",
            "trace_spool",
            "trace_sources",
            "trace_export_generations",
            "trace_storage_usage",
        )
    }


def test_closed_session_compaction_preserves_evidence_and_rejects_old_retries(tmp_path):
    store, token, _session, _binding, appends, dispatch, finish, _reply = populated(
        tmp_path
    )
    try:
        before = evidence(store)
        assert compact(store) == {"receipts": 4, "operations": 1, "bind_payloads": 1}
        assert compact(store) == {"receipts": 0, "operations": 1, "bind_payloads": 0}
        assert compact(store) == {"receipts": 0, "operations": 0, "bind_payloads": 0}
        assert evidence(store) == before
        assert [
            r[0]
            for r in store._connection.execute("SELECT operation FROM trace_requests")
        ] == ["bind"]
        assert (
            store._connection.execute("SELECT COUNT(*) FROM trace_bindings").fetchone()[
                0
            ]
            == 1
        )
        for method, params in [
            (store.append_trace, appends[0]),
            (store.dispatch_trace, dispatch),
            (store.finish_trace, finish),
        ]:
            with pytest.raises(TraceContractError, match="session_unavailable"):
                method(
                    node_id="owned-edge",
                    connector_id="owner",
                    token=token,
                    params=params,
                )
        assert evidence(store) == before
        # Bind request IDs have connector-wide scope: keep their receipts so a
        # new session cannot recycle one to acquire a different observational root.
        request_id = store._connection.execute(
            "SELECT request_id FROM trace_requests"
        ).fetchone()[0]
        new_session = store.open_session(connector_id="owner", token=token)
        with pytest.raises(TraceContractError, match="idempotency_conflict"):
            store.bind_trace(
                node_id="owned-edge",
                connector_id="owner",
                token=token,
                params={
                    "schema_version": 1,
                    "request_id": request_id,
                    "session_id": new_session["session_id"],
                    "task_id": None,
                    "context_id": None,
                },
            )
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


def test_closed_binding_in_renewable_session_keeps_finish_receipt(tmp_path):
    store, token, session, _binding, _appends, _dispatch, finish, reply = populated(
        tmp_path, close_session=False
    )
    try:
        with store._connection:
            store._connection.execute("UPDATE sessions SET lease_expires_at_ms=1")
        assert compact(store) == {"receipts": 0, "operations": 0, "bind_payloads": 0}
        store.renew_session(
            connector_id="owner", token=token, session_id=session["session_id"]
        )
        assert (
            store.finish_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=finish
            )
            == reply
        )
    finally:
        store.close()


def test_compaction_failure_rolls_back_receipts_and_operations(tmp_path):
    store, *_rest = populated(tmp_path)
    try:
        before = {
            t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
            for t in ("trace_requests", "trace_operations")
        }
        store._connection.execute(
            "CREATE TRIGGER owned_compact_fault BEFORE DELETE ON trace_operations BEGIN SELECT RAISE(ABORT,'owned compact fault'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="owned compact fault"):
            compact(store)
        assert {
            t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
            for t in before
        } == before
    finally:
        store.close()


def test_compaction_age_boundary_and_batch_limit(tmp_path):
    store, *_rest = populated(tmp_path)
    try:
        assert compact(store, before=1) == {
            "receipts": 0,
            "operations": 0,
            "bind_payloads": 0,
        }
        assert compact(store, limit=1) == {
            "receipts": 1,
            "operations": 1,
            "bind_payloads": 1,
        }
        assert (
            store._connection.execute("SELECT COUNT(*) FROM trace_requests").fetchone()[
                0
            ]
            == 4
        )
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


def test_reconcile_runs_bounded_compaction_without_changing_recent_evidence(tmp_path):
    store, *_rest = populated(tmp_path)
    try:
        before = evidence(store)
        store.reconcile()
        assert (
            store._connection.execute("SELECT COUNT(*) FROM trace_requests").fetchone()[
                0
            ]
            == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM trace_operations"
            ).fetchone()[0]
            == 1
        )
        assert evidence(store) == before
    finally:
        store.close()


def test_v14_index_migration_preserves_receipts_and_operations(tmp_path):
    store, *_rest = populated(tmp_path)
    path = store.path
    before = {
        t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
        for t in ("trace_requests", "trace_operations")
    }
    store.close()
    with sqlite3.connect(path) as db:
        for name in (
            "trace_bindings_closed",
            "trace_requests_binding",
            "trace_operations_parent",
        ):
            db.execute(f"DROP INDEX {name}")
        db.execute("DROP TABLE IF EXISTS trace_import_records")
        db.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(db)
        db.execute("PRAGMA user_version=14")
    reopened = AgentdStore(path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 27
        assert {
            t: [tuple(r) for r in reopened._connection.execute(f"SELECT * FROM {t}")]
            for t in before
        } == before
        indexes = {
            r[0]
            for r in reopened._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            )
        }
        assert {
            "trace_bindings_closed",
            "trace_requests_binding",
            "trace_operations_parent",
        } <= indexes
    finally:
        reopened.close()


def test_closed_bind_reply_becomes_small_identity_tombstone(tmp_path):
    store, token, session, *_rest = populated(tmp_path)
    try:
        db = store._connection
        original = dict(
            db.execute("SELECT * FROM trace_requests WHERE operation='bind'").fetchone()
        )
        history = evidence(store)
        compact(store)
        retained = dict(
            db.execute("SELECT * FROM trace_requests WHERE operation='bind'").fetchone()
        )
        assert retained == {**original, "result_json": "{}"}
        assert evidence(store) == history
        params = {
            "schema_version": 1,
            "request_id": original["request_id"],
            "session_id": session["session_id"],
            "task_id": None,
            "context_id": None,
        }
        with pytest.raises(TraceContractError, match="session_unavailable"):
            store.bind_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=params
            )
        new_session = store.open_session(connector_id="owner", token=token)
        with pytest.raises(TraceContractError, match="idempotency_conflict"):
            store.bind_trace(
                node_id="owned-edge",
                connector_id="owner",
                token=token,
                params={**params, "session_id": new_session["session_id"]},
            )
        assert (
            dict(
                db.execute(
                    "SELECT * FROM trace_requests WHERE operation='bind'"
                ).fetchone()
            )
            == retained
        )
    finally:
        store.close()


def test_bind_reply_retained_until_both_closures_pass_cutoff(tmp_path):
    store, *_rest = populated(tmp_path, close_session=False)
    try:
        db = store._connection
        original = db.execute(
            "SELECT result_json FROM trace_requests WHERE operation='bind'"
        ).fetchone()[0]
        compact(store)
        assert (
            db.execute(
                "SELECT result_json FROM trace_requests WHERE operation='bind'"
            ).fetchone()[0]
            == original
        )
        with db:
            db.execute("UPDATE sessions SET closed_at_ms=2")
        compact(store, before=2)
        assert (
            db.execute(
                "SELECT result_json FROM trace_requests WHERE operation='bind'"
            ).fetchone()[0]
            == original
        )
        compact(store, before=3)
        assert (
            db.execute(
                "SELECT result_json FROM trace_requests WHERE operation='bind'"
            ).fetchone()[0]
            == "{}"
        )
    finally:
        store.close()


def test_bind_tombstones_are_compacted_in_bounded_batches(tmp_path):
    store, token, session, _binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    try:
        for _ in range(2):
            store.bind_trace(
                node_id="owned-edge",
                connector_id="owner",
                token=token,
                params={
                    "schema_version": 1,
                    "request_id": str(uuid4()),
                    "session_id": session["session_id"],
                    "task_id": None,
                    "context_id": None,
                },
            )
        store.close_session(
            connector_id="owner", token=token, session_id=session["session_id"]
        )
        with store._connection:
            store._connection.execute("UPDATE trace_bindings SET closed_at_ms=1")
            store._connection.execute("UPDATE sessions SET closed_at_ms=1")
        before = evidence(store)
        for expected in (1, 2, 3):
            assert compact(store, limit=1)["bind_payloads"] == 1
            assert (
                store._connection.execute(
                    "SELECT COUNT(*) FROM trace_requests WHERE operation='bind' AND result_json='{}'"
                ).fetchone()[0]
                == expected
            )
            assert evidence(store) == before
        assert compact(store)["bind_payloads"] == 0
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM trace_requests WHERE operation='bind'"
            ).fetchone()[0]
            == 3
        )
    finally:
        store.close()
