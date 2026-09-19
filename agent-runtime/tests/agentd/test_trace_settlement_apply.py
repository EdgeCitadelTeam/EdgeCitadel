import json
import os
import select
import sqlite3
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request


@pytest.fixture
def source(tmp_path):
    store = AgentdStore(tmp_path / "state.sqlite3")
    event = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"][0]["event"]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        journal = TraceJournal(store._connection)
        epoch, generation = journal.initialize("edge-a")
        for _ in range(5):
            event["event_id"] = str(uuid4())
            journal.record("edge-a", event, selected=True)
    try:
        yield store, ("edge-a", epoch, generation)
    finally:
        store.close()


def reply(request, through=3, *, epoch=None):
    page = {
        key: request[key]
        for key in (
            "schema_version",
            "node_id",
            "source_epoch",
            "export_generation",
            "after_export_seq",
        )
    }
    page.update(
        collector_epoch=epoch or request["collector_epoch"] or str(uuid4()),
        settled_export_seq=through,
        rejected_ranges=[],
        lost_ranges=[],
        more=False,
    )
    return {
        "schema_version": 2,
        "request_id": request["request_id"],
        "status": "ok",
        "page": page,
    }


def states(store):
    return [
        tuple(row)
        for row in store._connection.execute(
            "SELECT state,collector_epoch,core_outcome FROM trace_spool ORDER BY export_seq"
        )
    ]


def test_atomic_classification_duplicate_and_restart_cursor(source):
    store, scope = source
    request = page_request(store, scope)
    response = reply(request)
    response["page"]["rejected_ranges"] = [{"first": 2, "last": 2}]
    response["page"]["lost_ranges"] = [{"first": 3, "last": 3}]
    assert apply_page(store, request, response) == "applied"
    epoch = response["page"]["collector_epoch"]
    assert (
        states(store)
        == [("core_settled", epoch, kind) for kind in ("accepted", "rejected", "lost")]
        + [("pending", None, None)] * 2
    )
    assert apply_page(store, request, response) == "duplicate"
    assert (
        store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        == 5
    )
    reopened = AgentdStore(store.path)
    try:
        resumed = page_request(reopened, scope)
        assert (resumed["collector_epoch"], resumed["after_export_seq"]) == (epoch, 3)
        assert apply_page(reopened, request, response) == "duplicate"
        assert apply_page(reopened, resumed, reply(resumed, 5)) == "applied"
        assert page_request(reopened, scope)["after_export_seq"] == 5
    finally:
        reopened.close()


@pytest.mark.parametrize("table", ["trace_spool", "trace_source_settlements"])
def test_failure_rolls_back_both_retirement_and_cursor(source, table):
    store, scope = source
    request = page_request(store, scope)
    before = list(store._connection.iterdump())
    operation = "UPDATE" if table == "trace_spool" else "INSERT"
    store._connection.execute(
        f"CREATE TEMP TRIGGER fail BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'injected'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="injected"):
        apply_page(store, request, reply(request))
    assert list(store._connection.iterdump()) == before
    assert page_request(store, scope)["after_export_seq"] == 0


def test_reordered_and_changed_epoch_pages_cannot_advance(source):
    store, scope = source
    request = page_request(store, scope)
    response = reply(request)
    apply_page(store, request, response)
    current = page_request(store, scope)
    forged = {**current, "after_export_seq": 4}
    with pytest.raises(TraceContractError, match="base_mismatch"):
        apply_page(store, forged, reply(forged, 5))
    with pytest.raises(TraceContractError, match="collector_epoch_changed"):
        apply_page(store, request, reply(request, epoch=str(uuid4())))
    apply_page(store, current, reply(current, 5))
    with pytest.raises(TraceContractError, match="base_mismatch"):
        apply_page(store, request, response)
    assert page_request(store, scope)["after_export_seq"] == 5


def test_future_positions_and_unknown_scope_are_refused(source):
    store, scope = source
    request = page_request(store, scope)
    with pytest.raises(TraceContractError, match="beyond_assigned"):
        apply_page(store, request, reply(request, 6))
    with pytest.raises(TraceContractError, match="unknown_export_generation"):
        page_request(store, (scope[0], scope[1], str(uuid4())))
    assert states(store) == [("pending", None, None)] * 5


@pytest.mark.parametrize(
    "code", ["unknown_source", "collector_changed", "temporarily_unavailable"]
)
def test_error_replies_cannot_retire_rows(source, code):
    store, scope = source
    request = page_request(store, scope)
    response = {
        "schema_version": 2,
        "request_id": request["request_id"],
        "status": "error",
        "code": code,
        "retry_after_ms": 500,
    }
    before = list(store._connection.iterdump())
    assert apply_page(store, request, response) == code
    assert list(store._connection.iterdump()) == before


def test_sparse_loss_and_retired_epoch_do_not_use_row_count(source):
    store, scope = source
    with store._connection:
        store._connection.execute("DELETE FROM trace_spool WHERE export_seq=2")
        store._connection.execute("UPDATE trace_sources SET active=0")
        store._connection.execute("UPDATE trace_export_generations SET active=0")
    request = page_request(store, scope)
    response = reply(request, 5)
    response["page"]["lost_ranges"] = [{"first": 2, "last": 2}]
    assert apply_page(store, request, response) == "applied"
    assert page_request(store, scope)["after_export_seq"] == 5
    assert len(states(store)) == 4


def test_nested_transaction_is_not_committed_or_rolled_back(source):
    store, scope = source
    request = page_request(store, scope)
    store._connection.execute("BEGIN IMMEDIATE")
    with pytest.raises(TraceContractError, match="committed_store"):
        apply_page(store, request, reply(request))
    with pytest.raises(TraceContractError, match="committed_store"):
        page_request(store, scope)
    assert store._connection.in_transaction
    store._connection.rollback()


def test_schema_18_upgrade_is_atomic(source):
    store, _ = source
    with store._connection:
        store._connection.execute("DROP TABLE trace_source_settlements")
        store._connection.execute("ALTER TABLE trace_spool DROP COLUMN core_outcome")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=18")
    captured = []

    class FailingStore(AgentdStore):
        def _execute_migration_sql(self, source_sql):
            super()._execute_migration_sql(source_sql)
            if "trace_source_settlements" in source_sql:
                captured.append(self._connection)
                raise RuntimeError("migration fault")

    try:
        with pytest.raises(RuntimeError, match="migration fault"):
            FailingStore(store.path)
    finally:
        for connection in captured:
            connection.close()
    assert store._connection.execute("PRAGMA user_version").fetchone()[0] == 18
    assert "core_outcome" not in {
        row[1] for row in store._connection.execute("PRAGMA table_info(trace_spool)")
    }
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 25
        assert states(reopened) == [("pending", None, None)] * 5
    finally:
        reopened.close()


@pytest.mark.parametrize("boundary", ["before", "after"])
def test_sigkill_at_page_commit_boundary(source, tmp_path, boundary):
    store, scope = source
    request = page_request(store, scope)
    response = reply(request)
    exchange = tmp_path / "exchange.json"
    exchange.write_text(json.dumps({"request": request, "reply": response}))
    program = """
import json, sys, time
from pathlib import Path
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_settlement_apply import apply_page
store = AgentdStore(Path(sys.argv[1]))
exchange = json.loads(Path(sys.argv[2]).read_text())
def trace(sql):
    if sys.argv[3] == 'before' and sql.startswith('INSERT INTO trace_source_settlements'):
        print('boundary', flush=True)
        time.sleep(60)
store._connection.set_trace_callback(trace)
apply_page(store, exchange['request'], exchange['reply'])
print('boundary', flush=True)
time.sleep(60)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(store.path), str(exchange), boundary],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(Path(__file__).parents[2] / "src")
            + os.pathsep
            + os.environ.get("PYTHONPATH", ""),
        },
    )
    try:
        assert select.select([process.stdout], [], [], 10)[0], (
            "owned writer did not reach boundary"
        )
        assert process.stdout.readline().strip() == "boundary"
        process.kill()
        assert process.wait(timeout=5) != 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()
    reopened = AgentdStore(store.path)
    try:
        assert page_request(reopened, scope)["after_export_seq"] == (
            0 if boundary == "before" else 3
        )
        assert [item[0] for item in states(reopened)] == (
            ["pending"] * 5
            if boundary == "before"
            else ["core_settled"] * 3 + ["pending"] * 2
        )
        assert apply_page(reopened, request, response) == (
            "applied" if boundary == "before" else "duplicate"
        )
    finally:
        reopened.close()
