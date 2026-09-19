"""Per-event loss rows retire only behind exact retained selected ranges."""

import sqlite3
import time
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest
from test_trace_crash import snapshot
from test_trace_retention import prune
from test_trace_retention import recorded as retention_fixture
from test_trace_retired_retention import prune as prune_retired
from test_trace_retired_retention import rotate

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_compaction import compact_lost_spool

recorded = retention_fixture


def compact(store, limit=256):
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return compact_lost_spool(store._connection, limit=limit)


def test_exact_lost_ranges_compact_in_bounded_batches_without_changing_markers(
    recorded,
):
    prune(recorded)
    before = snapshot(recorded)
    assert compact(recorded, 1) == 1
    assert compact(recorded, 1) == 1
    assert compact(recorded) == 0
    after = snapshot(recorded)
    for table in before:
        if table != "trace_spool":
            assert after[table] == before[table]
    assert [
        r[0]
        for r in recorded._connection.execute(
            "SELECT state FROM trace_spool ORDER BY export_seq"
        )
    ] == ["core_settled", "pending"]
    assert recorded._connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_current_selected_marker_covers_retired_epoch(recorded):
    old, new = rotate(recorded)
    prune_retired(recorded)
    before = snapshot(recorded)
    assert compact(recorded) == 2
    after = snapshot(recorded)
    for table in before:
        if table != "trace_spool":
            assert after[table] == before[table]
    assert (
        recorded._connection.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE source_epoch=?", (old,)
        ).fetchone()[0]
        == 1
    )
    assert (
        recorded._connection.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE source_epoch=?", (new,)
        ).fetchone()[0]
        == 2
    )


def test_unselected_marker_does_not_authorize_deletion(recorded):
    prune(recorded)
    with recorded._connection:
        recorded._connection.execute("DELETE FROM trace_spool WHERE state='pending'")
    before = snapshot(recorded)
    assert compact(recorded) == 0
    assert snapshot(recorded) == before


def test_other_generation_cannot_borrow_loss_evidence(recorded):
    prune(recorded)
    db = recorded._connection
    generation = str(uuid4())
    with db:
        db.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,next_export_seq_bytes,active) SELECT node_id,source_epoch,?,next_export_seq_bytes,0 FROM trace_export_generations",
            (generation,),
        )
        db.execute(
            "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256,state,collector_epoch) SELECT node_id,source_epoch,?,export_seq,event_id,journal_event_id,event_sha256,state,collector_epoch FROM trace_spool WHERE export_seq=1",
            (generation,),
        )
    assert compact(recorded) == 2
    assert (
        db.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE export_generation=?", (generation,)
        ).fetchone()[0]
        == 1
    )
    assert compact(recorded) == 0


def test_delete_failure_rolls_back_entire_batch(recorded):
    prune(recorded)
    before = snapshot(recorded)
    recorded._connection.execute(
        "CREATE TRIGGER owned_compaction_fault BEFORE DELETE ON trace_spool WHEN OLD.export_seq=2 BEGIN SELECT RAISE(ABORT,'owned compaction fault'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned compaction fault"):
        compact(recorded)
    assert snapshot(recorded) == before


def test_v16_migration_preserves_rows_and_reconcile_compacts(recorded):
    prune(recorded)
    before = snapshot(recorded)
    with recorded._connection:
        recorded._connection.execute("DROP INDEX trace_loss_scope")
        recorded._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        recorded._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(recorded._connection)
        recorded._connection.execute("PRAGMA user_version=16")
    reopened = AgentdStore(recorded.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        assert snapshot(reopened) == before
        plan = reopened._connection.execute(
            "EXPLAIN QUERY PLAN SELECT event_id FROM trace_journal WHERE node_id=? "
            "AND COALESCE(json_extract(event_json,'$.attributes.affected_source_epoch'),source_epoch)=? "
            "AND json_extract(event_json,'$.attributes.export_generation')=? "
            "AND json_extract(event_json,'$.kind')='coverage' AND json_extract(event_json,'$.phase')='lost'",
            ("edge-a", str(uuid4()), str(uuid4())),
        ).fetchall()
        assert any("trace_loss_scope" in row[3] for row in plan)
        reopened.reconcile(now_ms=int(time.time() * 1000))
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM trace_spool WHERE state='lost_with_marker'"
            ).fetchone()[0]
            == 0
        )
    finally:
        reopened.close()
