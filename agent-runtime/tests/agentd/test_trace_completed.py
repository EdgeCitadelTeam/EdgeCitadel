import hashlib
import json
import os
import sqlite3
from uuid import uuid4

import pytest

from edgecitadel_agentd import storage_workspace
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_completed import (
    SPOOL_COLUMNS,
    attach_receipt,
    export_page,
    materialize,
)
from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    canonical_bytes,
    validate_finish_request,
)
from edgecitadel_agentd.trace_exporter import (
    ExportScope,
    checkpoint_broker_ack,
    selected_batch,
)
from edgecitadel_agentd.trace_history import read_history
from edgecitadel_agentd.trace_inspect import inspect_source
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_reservations import Obligation, reserve
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request
from test_trace_settlement_apply import reply


@pytest.fixture
def completed_store(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_workspace, "WORKSPACE_BYTES", 256 * 1024)
    if not hasattr(os, "posix_fallocate"):
        monkeypatch.setattr(
            os,
            "posix_fallocate",
            lambda fd, offset, size: os.pwrite(fd, b"\0" * size, offset),
            raising=False,
        )
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    token = store.register_connector(
        connector_id="reader",
        host_type="codex",
        agent_id="reader",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="reader", token=token)["session_id"]
    binding = store.bind_trace(
        node_id="edge-a",
        connector_id="reader",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": None,
            "context_id": None,
        },
    )["result"]
    db = store._connection
    original = json.loads(
        db.execute("SELECT event_json FROM trace_journal").fetchone()[0]
    )
    obligation = Obligation("run", binding["binding_id"], "terminal")
    with db:
        db.execute("BEGIN IMMEDIATE")
        reserve(db, obligation)
    physical = storage_workspace.CompletionWorkspace(tmp_path / "completion.reserve")
    db.install_workspace(physical)
    try:
        yield store, token, binding, obligation, original
    finally:
        store.close()


def complete(fixture):
    store, token, binding, obligation, original = fixture
    db = store._connection
    value = {
        **original,
        "event_id": str(uuid4()),
        "phase": "completed",
        "attributes": {"reason": "unknown"},
    }
    params = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": binding["binding_id"],
        "outcome": "completed",
        "reason": "unknown",
    }
    with db:
        db.execute("BEGIN IMMEDIATE")
        event = TraceJournal(db).record(
            "edge-a", value, selected=True, completion=obligation
        )
        result = {
            "schema_version": 1,
            "operation": "finish",
            "request_id": params["request_id"],
            "status": "ok",
            "result": {
                key: event[key] for key in ("event_id", "source_epoch", "source_seq")
            },
        }
        attach_receipt(
            db,
            obligation,
            {
                "connector_id": "reader",
                "operation": "finish",
                "scope": binding["binding_id"],
                "request_id": params["request_id"],
                "request_sha256": hashlib.sha256(
                    validate_finish_request(params)
                ).hexdigest(),
                "binding_id": binding["binding_id"],
                "result_json": canonical_bytes(result).decode(),
            },
        )
    return event, params, result


def snapshot(db):
    return {
        name: [
            tuple(row)
            for row in db.execute(
                f"SELECT {','.join(SPOOL_COLUMNS) if name == 'spool' else '*'} FROM trace_{name}_all ORDER BY 1,2,3"
            )
        ]
        for name in ("journal", "spool", "requests", "storage_usage")
    }


def test_completed_fact_is_visible_to_history_export_inspection_and_exact_retry(
    completed_store,
):
    store, token, binding, obligation, original = completed_store
    db = store._connection
    assert db.execute("SELECT count(*) FROM trace_journal_all").fetchone()[0] == 1
    event, params, result = complete(completed_store)
    assert db.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM trace_journal_all").fetchone()[0] == 2
    history = read_history(
        store,
        connector_id="reader",
        token=token,
        params={"trace_id": binding["trace_id"]},
    )
    assert history["events"] == [original, event]
    scope = ExportScope(
        *tuple(
            db.execute(
                "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
            ).fetchone()
        )
    )
    batch = selected_batch(store, scope)
    assert [json.loads(record.payload)["event"] for record in batch] == [
        original,
        event,
    ]
    assert (
        store.finish_trace(
            node_id="edge-a", connector_id="reader", token=token, params=params
        )
        == result
    )
    with pytest.raises(StoreError):
        store.finish_trace(
            node_id="edge-a", connector_id="reader", token="wrong", params=params
        )
    with pytest.raises(TraceContractError, match="idempotency_conflict"):
        store.finish_trace(
            node_id="edge-a",
            connector_id="reader",
            token=token,
            params={**params, "outcome": "failed"},
        )
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert (
            TraceJournal(db).record(
                "edge-a", event, selected=True, completion=obligation
            )
            == event
        )
    report = inspect_source(store.path, scope=scope.values())
    assert len(report["records"]) == 2
    for record in batch:
        checkpoint_broker_ack(store, record)
    assert selected_batch(store, scope) == []
    assert len(selected_batch(store, scope, replay=True)) == 2
    request = page_request(store, scope.values())
    response = reply(request, through=2)
    assert apply_page(store, request, response) == "applied"
    assert [row[0] for row in db.execute("SELECT state FROM trace_spool_all")] == [
        "core_settled",
        "core_settled",
    ]
    before = snapshot(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert materialize(db, 1)
    assert snapshot(db) == before
    assert (
        db.execute(
            "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
        ).fetchone()[0]
        == 0
    )
    assert (
        store.finish_trace(
            node_id="edge-a", connector_id="reader", token=token, params=params
        )
        == result
    )


def test_failed_materialization_keeps_the_completed_fact_authoritative(completed_store):
    store = completed_store[0]
    complete(completed_store)
    db = store._connection
    before = snapshot(db)
    db.execute(
        "CREATE TRIGGER owned_fail BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT,'owned refusal'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned refusal"), db:
        db.execute("BEGIN IMMEDIATE")
        materialize(db, 1)
    assert snapshot(db) == before
    assert db.execute("SELECT filled FROM trace_completion_slots").fetchone()[0] == 1


def test_reserved_export_identity_and_parent_cannot_be_changed(completed_store):
    store = completed_store[0]
    event, _, _ = complete(completed_store)
    db = store._connection
    before = snapshot(db)
    with pytest.raises(sqlite3.IntegrityError, match="requires materialization"), db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "UPDATE trace_spool_all SET export_seq=999 WHERE event_id=?",
            (event["event_id"],),
        )
    assert snapshot(db) == before
    with pytest.raises(sqlite3.IntegrityError, match="completed record reference"), db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM trace_spool")
        db.execute("DELETE FROM trace_journal")
        db.execute("DELETE FROM trace_export_generations")
    assert snapshot(db) == before


def test_rollback_keeps_the_obligation_pending_and_positions_unspent(completed_store):
    store, _, _, obligation, original = completed_store
    db = store._connection
    before = snapshot(db)
    with pytest.raises(RuntimeError, match="owned failure"), db:
        db.execute("BEGIN IMMEDIATE")
        TraceJournal(db).record(
            "edge-a",
            {**original, "event_id": str(uuid4()), "phase": "completed"},
            selected=True,
            completion=obligation,
        )
        raise RuntimeError("owned failure")
    assert snapshot(db) == before
    assert db.execute("SELECT filled FROM trace_completion_slots").fetchone()[0] == 0
    assert db.execute("SELECT next_source_seq FROM trace_sources").fetchone()[0] == 2
    assert (
        db.execute("SELECT next_export_seq FROM trace_export_generations").fetchone()[0]
        == 2
    )


def test_export_page_does_not_scan_the_retained_scope(completed_store):
    store = completed_store[0]
    db = store._connection
    scope = tuple(
        db.execute(
            "SELECT node_id,source_epoch,export_generation FROM trace_export_generations"
        ).fetchone()
    )
    # Opaque query fixture: paging must not parse or materialize the retained
    # payloads after its first page. Event validation belongs to the exporter.
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            """WITH RECURSIVE n(value) AS (VALUES(2) UNION ALL SELECT value+1 FROM n WHERE value<5001)
            INSERT INTO trace_journal(node_id,source_epoch,event_id,source_seq,event_sha256,event_json,event_bytes,received_at_ms)
            SELECT ?,?,'owned-page-'||value,value,?,'{}',2,1 FROM n""",
            (*scope[:2], "0" * 64),
        )
        db.execute(
            """INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256)
            SELECT node_id,source_epoch,?,source_seq,event_id,event_id,event_sha256 FROM trace_journal WHERE source_seq>=2""",
            (scope[2],),
        )
    steps = 0

    def budget():
        nonlocal steps
        steps += 100
        return steps > 10000

    db.set_progress_handler(budget, 100)
    try:
        rows = export_page(db, scope, after=0, limit=32)
    finally:
        db.set_progress_handler(None, 0)
    assert [row["export_seq"] for row in rows] == list(range(1, 33))
