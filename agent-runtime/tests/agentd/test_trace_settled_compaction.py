import json
import sqlite3
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest
from test_trace_settlement_apply import reply, source  # noqa: F401
from test_trace_sync import core  # noqa: F401

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_collector_recovery import begin_recovery, recover_batch
from edgecitadel_agentd.trace_compaction import compact_settled_spool
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_exporter import ExportScope, selected_batch
from edgecitadel_agentd.trace_retention import prune_active_history
from edgecitadel_agentd.trace_settlement_apply import apply_page, page_request


def prepare(store, scope, *, limit=2):
    request = page_request(store, scope)
    response = reply(request)
    apply_page(store, request, response)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        assert (
            prune_active_history(
                store._connection, node_id=scope[0], now_ms=1, limit=limit
            )
            == limit
        )
    return response["page"]["collector_epoch"]


def compact(store, limit=256):
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return compact_settled_spool(store._connection, limit=limit)[0]


def test_retirement_keeps_payloads_and_restore_declares_exact_loss(source):  # noqa: F811
    store, scope = source
    old = prepare(store, scope)
    db = store._connection
    checkpoint = page_request(store, scope)
    payloads = list(
        db.execute("SELECT event_json FROM trace_journal ORDER BY event_id")
    )
    assert compact(store, 1) == 1
    assert compact(store) == 1
    assert compact(store) == 0
    assert (
        list(db.execute("SELECT event_json FROM trace_journal ORDER BY event_id"))
        == payloads
    )
    assert (
        page_request(store, scope)["after_export_seq"] == checkpoint["after_export_seq"]
    )
    assert [
        r[0]
        for r in db.execute("SELECT export_seq FROM trace_spool ORDER BY export_seq")
    ] == [3, 4, 5, 6]
    reopened = AgentdStore(store.path)
    try:
        begin_recovery(reopened, scope, expected_epoch=old)
        assert recover_batch(reopened, scope)
        markers = [
            json.loads(r[0])
            for r in reopened._connection.execute(
                "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage' ORDER BY source_seq"
            )
        ]
        assert markers[-1]["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
        assert markers[-1]["attributes"]["through_export_seq"] == 6
        request = page_request(reopened, scope)
        assert request["collector_epoch"] is None and request["after_export_seq"] == 0
        with pytest.raises(TraceContractError, match="retired_collector_epoch"):
            apply_page(reopened, request, reply(request, 7, epoch=old))
        fresh = reply(request, 7)
        fresh["page"]["lost_ranges"] = [{"first": 1, "last": 2}]
        assert apply_page(reopened, request, fresh) == "applied"
        assert (
            reopened._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            == 0
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("fence", ["missing", "epoch", "behind", "scanning", "ready"])
def test_checkpoint_and_recovery_fences(source, fence):  # noqa: F811
    store, scope = source
    old = prepare(store, scope)
    db = store._connection
    with db:
        if fence == "missing":
            db.execute("DELETE FROM trace_source_settlements")
        elif fence == "epoch":
            db.execute(
                "UPDATE trace_source_settlements SET collector_epoch=?", (str(uuid4()),)
            )
        elif fence == "behind":
            db.execute("UPDATE trace_source_settlements SET applied_through=0")
    if fence in {"scanning", "ready"}:
        begin_recovery(store, scope, expected_epoch=old)
        if fence == "ready":
            with db:
                db.execute("UPDATE trace_collector_recovery SET phase='ready'")
    before = list(db.iterdump())
    assert compact(store) == 0
    assert list(db.iterdump()) == before


def test_retirement_rollback_and_unsettled_rows(source):  # noqa: F811
    store, scope = source
    prepare(store, scope, limit=5)
    db = store._connection
    db.execute(
        "CREATE TEMP TRIGGER fail BEFORE DELETE ON trace_spool WHEN OLD.export_seq=2 BEGIN SELECT RAISE(ABORT,'owned fault'); END"
    )
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="owned fault"):
        compact(store)
    assert list(db.iterdump()) == before
    db.execute("DROP TRIGGER fail")
    assert compact(store) == 3
    assert [
        r[0]
        for r in db.execute("SELECT export_seq FROM trace_spool ORDER BY export_seq")
    ] == [4, 5, 6]


def test_upgrade_preserves_rows_and_uses_partial_index(source):  # noqa: F811
    store, scope = source
    prepare(store, scope)
    db = store._connection
    rows = [tuple(r) for r in db.execute("SELECT * FROM trace_spool")]
    with db:
        db.execute("DROP INDEX trace_spool_settled_empty")
        flatten_connection(db)
        db.execute("PRAGMA user_version=21")
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 24
        assert [
            tuple(r) for r in reopened._connection.execute("SELECT * FROM trace_spool")
        ] == rows
        plan = reopened._connection.execute(
            "EXPLAIN QUERY PLAN SELECT rowid FROM trace_spool INDEXED BY trace_spool_settled_empty "
            "WHERE state='core_settled' AND journal_event_id IS NULL "
            "ORDER BY node_id,source_epoch,export_generation,export_seq LIMIT 256"
        ).fetchall()
        assert any("trace_spool_settled_empty" in r[3] for r in plan)
        assert not any("TEMP B-TREE" in r[3] for r in plan)
        assert compact(reopened) == 2
    finally:
        reopened.close()


def test_real_core_commit_retirement_and_new_collector_replay(source, core):  # noqa: F811
    from aggregator.trace_store import initialize

    store, scope = source
    original, ingest, settlement = core
    for record in selected_batch(store, ExportScope(*scope)):
        ingest(original, record.subject, record.payload, received_at_ms=1)
    request = page_request(store, scope)
    committed = settlement(original, request)
    assert committed["page"]["settled_export_seq"] == 5
    apply_page(store, request, committed)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        assert (
            prune_active_history(store._connection, node_id=scope[0], now_ms=1, limit=2)
            == 2
        )
    assert compact(store) == 2
    begin_recovery(store, scope, expected_epoch=committed["page"]["collector_epoch"])
    assert recover_batch(store, scope)
    replacement = sqlite3.connect(store.path.parent / "replacement-core.db")
    try:
        initialize(replacement)
        records = selected_batch(store, ExportScope(*scope), replay=True)
        assert [r.export_seq for r in records] == [3, 4, 5, 6, 7]
        for record in records:
            ingest(replacement, record.subject, record.payload, received_at_ms=2)
        request = page_request(store, scope)
        result = settlement(replacement, request)
        assert result["page"]["settled_export_seq"] == 7
        assert result["page"]["lost_ranges"] == [{"first": 1, "last": 2}]
        assert apply_page(store, request, result) == "applied"
        assert (
            replacement.execute("SELECT COUNT(*) FROM trace_raw_events").fetchone()[0]
            == 5
        )
    finally:
        replacement.close()


@pytest.mark.parametrize("limit", [0, 257, True])
def test_retirement_requires_valid_bounded_transaction(source, limit):  # noqa: F811
    store, _scope = source
    with pytest.raises(TraceContractError, match="trace_transaction_required"):
        compact_settled_spool(store._connection)
    with pytest.raises(TraceContractError, match="invalid_compaction_limit"):
        compact(store, limit)


def test_reconcile_passes_fenced_prefix_and_revisits_after_wrap(source):  # noqa: F811
    store, scope = source
    collector = prepare(store, scope)
    db = store._connection
    with db:
        db.execute(
            "UPDATE trace_spool SET collector_epoch='fenced' WHERE journal_event_id IS NULL"
        )
        db.executemany(
            "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,"
            "event_id,event_sha256,state,collector_epoch) VALUES(?,?,?,?,?,?,'core_settled',?)",
            [
                (
                    *scope,
                    position,
                    str(uuid4()),
                    "0" * 64,
                    collector if position == 261 else "fenced",
                )
                for position in range(7, 262)
            ],
        )
        db.execute("UPDATE trace_source_settlements SET applied_through=261")
    store.reconcile(now_ms=1)
    assert db.execute("SELECT 1 FROM trace_spool WHERE export_seq=261").fetchone()
    store.reconcile(now_ms=1)
    assert (
        db.execute("SELECT 1 FROM trace_spool WHERE export_seq=261").fetchone() is None
    )
    assert (
        db.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE journal_event_id IS NULL"
        ).fetchone()[0]
        == 256
    )
    with db:
        db.execute(
            "UPDATE trace_spool SET collector_epoch=? WHERE export_seq=1", (collector,)
        )
    for _ in range(3):
        store.reconcile(now_ms=1)
    assert db.execute("SELECT 1 FROM trace_spool WHERE export_seq=1").fetchone() is None


def test_reconcile_commit_failure_does_not_advance_scan(source, monkeypatch):  # noqa: F811
    import edgecitadel_agentd.store as store_module

    store, scope = source
    prepare(store, scope)
    db = store._connection
    db.execute("CREATE TABLE retirement_parent(id INTEGER PRIMARY KEY)")
    db.execute(
        "CREATE TABLE retirement_child(parent_id INTEGER REFERENCES retirement_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    )
    original = store_module.compact_settled_spool

    def fail_at_commit(connection, **kwargs):
        result = original(connection, limit=1, **kwargs)
        assert result[0] == 1 and result[1] is not None
        connection.execute("INSERT INTO retirement_child VALUES(1)")
        return result

    monkeypatch.setattr(store_module, "compact_settled_spool", fail_at_commit)
    before = list(db.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
        store.reconcile(now_ms=1)
    assert list(db.iterdump()) == before
    assert store._settled_retirement_after is None
    monkeypatch.setattr(store_module, "compact_settled_spool", original)
    store.reconcile(now_ms=1)
    assert (
        db.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE journal_event_id IS NULL"
        ).fetchone()[0]
        == 0
    )


def test_keyset_seek_is_indexed_and_restart_revisits_prefix(source):  # noqa: F811
    store, scope = source
    collector = prepare(store, scope)
    db = store._connection
    with db:
        db.execute("UPDATE trace_spool SET collector_epoch='fenced' WHERE export_seq=1")
        removed, cursor = compact_settled_spool(db, limit=1)
    assert removed == 0 and cursor == (*scope, 1)
    plan = db.execute(
        "EXPLAIN QUERY PLAN SELECT rowid FROM trace_spool INDEXED BY trace_spool_settled_empty "
        "WHERE state='core_settled' AND journal_event_id IS NULL "
        "AND (node_id,source_epoch,export_generation,export_seq) > (?,?,?,?) "
        "ORDER BY node_id,source_epoch,export_generation,export_seq LIMIT ?",
        (*cursor, 1),
    ).fetchall()
    assert any("SEARCH" in r[3] and "trace_spool_settled_empty" in r[3] for r in plan)
    assert not any("TEMP B-TREE" in r[3] for r in plan)
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert compact_settled_spool(db, limit=1, after=cursor)[0] == 1
        db.execute(
            "UPDATE trace_spool SET collector_epoch=? WHERE export_seq=1", (collector,)
        )
    store._settled_retirement_after = cursor
    reopened = AgentdStore(store.path)
    try:
        assert reopened._settled_retirement_after is None
        reopened.reconcile(now_ms=1)
        assert (
            reopened._connection.execute(
                "SELECT 1 FROM trace_spool WHERE export_seq=1"
            ).fetchone()
            is None
        )
    finally:
        reopened.close()
