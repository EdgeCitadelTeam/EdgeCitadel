import json
import sqlite3
import time
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest
from test_trace_retention import recorded as retention_fixture

from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError, coverage_scope
from edgecitadel_agentd.trace_restore import rotate_restored_source
from edgecitadel_agentd.trace_retention import maintain_capacity, prune_retired_history

recorded = retention_fixture


def rotate(store):
    old = store._connection.execute(
        "SELECT source_epoch FROM trace_sources WHERE active=1"
    ).fetchone()[0]
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        marker = rotate_restored_source(
            store._connection, node_id="edge-a", expected_source_epoch=old
        )
    return old, marker["source_epoch"]


def prune(store):
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return prune_retired_history(
            store._connection, node_id="edge-a", now_ms=int(time.time() * 1000)
        )


def test_retired_epoch_loss_is_written_under_current_identity(recorded):
    store = recorded
    old, new = rotate(store)
    old_source = tuple(
        store._connection.execute(
            "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
        ).fetchone()
    )
    old_generation = tuple(
        store._connection.execute(
            "SELECT * FROM trace_export_generations WHERE source_epoch=?", (old,)
        ).fetchone()
    )
    identities = [
        tuple(r)
        for r in store._connection.execute(
            "SELECT event_id,event_sha256,export_seq FROM trace_spool WHERE source_epoch=? ORDER BY export_seq",
            (old,),
        )
    ]
    assert prune(store) == 4
    marker = json.loads(
        store._connection.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
    )
    assert marker["source_epoch"] == new
    assert coverage_scope(marker) == ("edge-a", old, old_generation[2])
    assert marker["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
    assert marker["attributes"]["through_export_seq"] == 3
    assert (
        tuple(
            store._connection.execute(
                "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
            ).fetchone()
        )
        == old_source
    )
    assert (
        tuple(
            store._connection.execute(
                "SELECT * FROM trace_export_generations WHERE source_epoch=?", (old,)
            ).fetchone()
        )
        == old_generation
    )
    assert [
        tuple(r)
        for r in store._connection.execute(
            "SELECT event_id,event_sha256,export_seq FROM trace_spool WHERE source_epoch=? ORDER BY export_seq",
            (old,),
        )
    ] == identities
    assert [
        r[0]
        for r in store._connection.execute(
            "SELECT state FROM trace_spool WHERE source_epoch=? ORDER BY export_seq",
            (old,),
        )
    ] == ["lost_with_marker", "lost_with_marker", "core_settled"]
    assert prune(store) == 0
    bad = {
        **marker,
        "attributes": {**marker["attributes"], "affected_source_epoch": "invalid"},
    }
    with pytest.raises(TraceContractError):
        coverage_scope(bad)


def test_shared_payload_marks_each_export_generation_before_delete(recorded):
    store = recorded
    db = store._connection
    epoch, old_generation = db.execute(
        "SELECT source_epoch,export_generation FROM trace_export_generations"
    ).fetchone()
    new_generation = str(uuid4())
    first = db.execute(
        "SELECT event_id,event_sha256 FROM trace_spool WHERE export_seq=1"
    ).fetchone()
    with db:
        db.execute("UPDATE trace_export_generations SET active=0")
        db.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,next_export_seq_bytes) VALUES ('edge-a',?,?,CAST('00000000000000000002' AS BLOB))",
            (epoch, new_generation),
        )
        db.execute(
            "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256) VALUES ('edge-a',?,?,1,?,?,?)",
            (epoch, new_generation, first[0], first[0], first[1]),
        )
    assert prune(store) == 3
    markers = [
        json.loads(r[0])
        for r in db.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        )
    ]
    by_generation = {coverage_scope(m)[2]: m for m in markers}
    assert by_generation[old_generation]["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2}
    ]
    assert by_generation[new_generation]["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 1}
    ]
    assert all("affected_source_epoch" not in m["attributes"] for m in markers)
    assert db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_retired_payload_and_markers_roll_back_together(recorded):
    store = recorded
    rotate(store)
    tables = [
        "trace_sources",
        "trace_export_generations",
        "trace_journal",
        "trace_spool",
        "trace_storage_usage",
    ]
    before = {
        t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
        for t in tables
    }
    store._connection.execute(
        "CREATE TRIGGER owned_retired_fault BEFORE DELETE ON trace_journal BEGIN SELECT RAISE(ABORT,'owned retired fault'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned retired fault"):
        prune(store)
    assert {
        t: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {t}")]
        for t in tables
    } == before


def test_arrival_expiry_reclaims_old_epoch_without_touching_restore_marker(recorded):
    store = recorded
    old, new = rotate(store)
    with store._connection:
        store._connection.execute(
            "UPDATE trace_journal SET received_at_ms=1 WHERE source_epoch=?", (old,)
        )
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        assert (
            maintain_capacity(store._connection, now_ms=1000, expire_before_ms=2) == 4
        )
    marker = json.loads(
        store._connection.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
    )
    assert marker["attributes"]["reason"] == "retention_expired"
    assert marker["source_epoch"] == new
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='source'"
        ).fetchone()[0]
        == 1
    )


def test_unreferenced_generations_do_not_block_retired_payload_reclamation(recorded):
    store = recorded
    old, _new = rotate(store)
    with store._connection:
        for _index in range(10):
            store._connection.execute(
                "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,active) VALUES ('edge-a',?,?,0)",
                (old, str(uuid4())),
            )
    assert prune(store) == 4
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
        == 1
    )


def test_large_referenced_generation_fanout_keeps_payloads_replayable(recorded):
    store = recorded
    old, _new = rotate(store)
    first = store._connection.execute(
        "SELECT event_id,event_sha256 FROM trace_spool WHERE source_epoch=? AND export_seq=1",
        (old,),
    ).fetchone()
    with store._connection:
        for _index in range(8):
            generation = str(uuid4())
            store._connection.execute(
                "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,active,next_export_seq_bytes) VALUES ('edge-a',?,?,0,CAST('00000000000000000002' AS BLOB))",
                (old, generation),
            )
            store._connection.execute(
                "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256) VALUES ('edge-a',?,?,1,?,?,?)",
                (old, generation, first[0], first[0], first[1]),
            )
    before = [tuple(r) for r in store._connection.execute("SELECT * FROM trace_spool")]
    assert prune(store) == 0
    assert [
        tuple(r) for r in store._connection.execute("SELECT * FROM trace_spool")
    ] == before
    assert (
        store._connection.execute(
            "SELECT COUNT(*) FROM trace_journal WHERE source_epoch=?", (old,)
        ).fetchone()[0]
        == 4
    )


def test_v15_reference_index_migration_preserves_spool_and_uses_event_key(recorded):
    store = recorded
    before = [tuple(r) for r in store._connection.execute("SELECT * FROM trace_spool")]
    with store._connection:
        store._connection.execute("DROP INDEX trace_spool_journal")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=15")
    reopened = AgentdStore(store.path)
    try:
        assert reopened._connection.execute("PRAGMA user_version").fetchone()[0] == 29
        assert [
            tuple(r) for r in reopened._connection.execute("SELECT * FROM trace_spool")
        ] == before
        plan = reopened._connection.execute(
            "EXPLAIN QUERY PLAN SELECT export_generation,state,export_seq FROM trace_spool WHERE node_id=? AND source_epoch=? AND journal_event_id=?",
            ("edge-a", before[0][1], before[0][4]),
        ).fetchall()
        assert any(
            "trace_spool_journal" in r[3] and "journal_event_id=?" in r[3] for r in plan
        )
    finally:
        reopened.close()
