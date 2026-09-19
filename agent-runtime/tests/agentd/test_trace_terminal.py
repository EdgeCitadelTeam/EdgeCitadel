import json
import os
import sqlite3
from uuid import uuid4

import pytest

from edgecitadel_agentd import storage_workspace, trace_reservations
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_completed import materialize
from edgecitadel_agentd.trace_contract import TraceContractError
from test_trace_append_store import append, finish, finish_request, request


@pytest.fixture
def installed(tmp_path, monkeypatch):
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
        connector_id="native",
        host_type="codex",
        agent_id="agent-a",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    physical = storage_workspace.CompletionWorkspace(tmp_path / "completion.reserve")
    store._connection.install_workspace(physical)
    try:
        yield store, token, session
    finally:
        store.close()


def bind(installed):
    store, token, session = installed
    return store.bind_trace(
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


def logical(db):
    return {
        name: [
            dict(row)
            for row in db.execute(f"SELECT * FROM trace_{name}_all ORDER BY 1")
        ]
        for name in ("bindings", "operations", "journal", "requests")
    }


def test_admission_refuses_before_new_binding_or_operation_and_resumes_after_materialization(
    installed, monkeypatch
):
    store, token, _ = installed
    monkeypatch.setattr(trace_reservations, "MAX_SLOTS", 2)
    root = bind(installed)
    start = request(root)
    started = append(store, token, start)
    db = store._connection
    before = logical(db)
    for action in (
        lambda: bind(installed),
        lambda: append(store, token, request(root)),
    ):
        with pytest.raises(TraceContractError, match="quota_exceeded"):
            action()
        assert logical(db) == before
    assert append(store, token, start) == started
    end = request(root, start["observation"]["span_id"], "finished")
    last = append(store, token, end)
    assert append(store, token, end) == last
    assert db.execute("SELECT phase FROM trace_operations").fetchone()[0] == "started"
    assert (
        db.execute("SELECT phase FROM trace_operations_all").fetchone()[0] == "finished"
    )
    closing = finish_request(root)
    reply = finish(store, token, closing)
    assert finish(store, token, closing) == reply
    assert db.execute("SELECT closed_at_ms FROM trace_bindings").fetchone()[0] is None
    assert (
        db.execute("SELECT closed_at_ms FROM trace_bindings_all").fetchone()[0]
        is not None
    )
    before = logical(db)
    reopened = AgentdStore(store.path)
    try:
        assert logical(reopened._connection) == before
        assert finish(reopened, token, closing) == reply
    finally:
        reopened.close()
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert materialize(db, 1)
        assert materialize(db, 2)
    assert logical(db) == before
    assert bind(installed)["binding_id"] != root["binding_id"]


def test_finish_closes_all_open_operations_without_indexed_state_growth(installed):
    store, token, _ = installed
    root = bind(installed)
    for _ in range(3):
        append(store, token, request(root))
    db = store._connection
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    params = finish_request(root)
    reply = finish(store, token, params)
    assert db.execute("PRAGMA page_count").fetchone()[0] == pages
    assert [row[0] for row in db.execute("SELECT phase FROM trace_operations_all")] == [
        "interrupted"
    ] * 3
    assert [row[0] for row in db.execute("SELECT phase FROM trace_operations")] == [
        "started"
    ] * 3
    assert (
        db.execute(
            "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
        ).fetchone()[0]
        == 4
    )
    assert finish(store, token, params) == reply
    with pytest.raises(TraceContractError, match="binding_closed"):
        append(store, token, request(root))


def test_failed_finish_receipt_rolls_back_all_closures_and_sequences(installed):
    store, token, _ = installed
    root = bind(installed)
    append(store, token, request(root))
    db = store._connection
    before = logical(db)
    db.execute("""CREATE TRIGGER owned_finish_failure BEFORE UPDATE ON trace_completion_slots
        WHEN json_extract(CAST(NEW.record AS TEXT),'$.receipt.operation')='finish'
        BEGIN SELECT RAISE(ABORT,'owned finish refusal'); END""")
    params = finish_request(root)
    with pytest.raises(sqlite3.IntegrityError, match="owned finish refusal"):
        finish(store, token, params)
    assert logical(db) == before
    assert (
        db.execute(
            "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
        ).fetchone()[0]
        == 0
    )
    assert db.execute("SELECT next_source_seq FROM trace_sources").fetchone()[0] == 3
    db.execute("DROP TRIGGER owned_finish_failure")
    assert finish(store, token, params)["status"] == "ok"


def test_terminal_metadata_cannot_claim_another_binding(installed):
    store, token, _ = installed
    root = bind(installed)
    append(store, token, request(root))
    finish(store, token, finish_request(root))
    db = store._connection
    row = db.execute(
        "SELECT slot_id,record FROM trace_completion_slots WHERE owner_kind='run'"
    ).fetchone()
    value = json.loads(row["record"])
    value["binding"]["binding_id"] = str(uuid4())
    from edgecitadel_agentd.trace_reservations import encode_record

    with pytest.raises(sqlite3.IntegrityError, match="completed record reference"), db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "UPDATE trace_completion_slots SET record=? WHERE slot_id=?",
            (encode_record(value), row["slot_id"]),
        )
