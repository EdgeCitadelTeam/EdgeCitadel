"""Loss marker replacement conserves coverage, including marker positions."""

import json
import sqlite3
import time
from uuid import uuid4

import pytest
from test_trace_crash import snapshot
from test_trace_journal import event, write
from test_trace_retention import prune
from test_trace_retention import recorded as retention_fixture

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.trace_compaction import compact_lost_spool
from edgecitadel_agentd.trace_marker_compaction import coalesce_loss_markers

recorded = retention_fixture


def two_markers(store):
    prune(store)
    write(store, event())
    prune(store)


def merge(store):
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return coalesce_loss_markers(store._connection, now_ms=int(time.time() * 1000))


def markers(store):
    return [
        json.loads(r[0])
        for r in store._connection.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        )
    ]


def test_replacement_preserves_prior_loss_and_includes_pending_marker_positions(
    recorded,
):
    two_markers(recorded)
    old_ids = {e["event_id"] for e in markers(recorded)}
    before = recorded._connection.execute(
        "SELECT event_bytes FROM trace_storage_usage"
    ).fetchone()[0]
    assert merge(recorded) == 1
    (replacement,) = markers(recorded)
    assert replacement["event_id"] not in old_ids
    assert replacement["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2},
        {"first": 4, "last": 6},
    ]
    assert replacement["attributes"]["through_export_seq"] == 6
    assert (
        recorded._connection.execute(
            "SELECT event_bytes FROM trace_storage_usage"
        ).fetchone()[0]
        < before
    )
    with recorded._connection:
        recorded._connection.execute("BEGIN IMMEDIATE")
        assert compact_lost_spool(recorded._connection) == 3
    assert [
        tuple(r)
        for r in recorded._connection.execute(
            "SELECT export_seq,state FROM trace_spool ORDER BY export_seq"
        )
    ] == [(3, "core_settled"), (7, "pending")]
    assert merge(recorded) == 0


def test_settled_marker_position_remains_settled_without_payload(recorded):
    two_markers(recorded)
    with recorded._connection:
        recorded._connection.execute(
            "UPDATE trace_spool SET state='core_settled' WHERE export_seq=4"
        )
    assert merge(recorded) == 1
    (replacement,) = markers(recorded)
    assert replacement["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2},
        {"first": 5, "last": 6},
    ]
    row = recorded._connection.execute(
        "SELECT state,journal_event_id FROM trace_spool WHERE export_seq=4"
    ).fetchone()
    assert tuple(row) == ("core_settled", None)


def test_marker_insert_failure_restores_all_deleted_evidence(recorded):
    two_markers(recorded)
    before = snapshot(recorded)
    recorded._connection.execute(
        "CREATE TRIGGER owned_marker_failure BEFORE INSERT ON trace_journal BEGIN SELECT RAISE(ABORT,'owned marker failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned marker failure"):
        merge(recorded)
    assert snapshot(recorded) == before


def test_full_logical_capacity_can_reuse_replaced_marker_bytes(recorded, monkeypatch):
    two_markers(recorded)
    used = recorded._connection.execute(
        "SELECT event_bytes FROM trace_storage_usage"
    ).fetchone()[0]
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", used)
    monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
    assert merge(recorded) == 1
    assert (
        recorded._connection.execute(
            "SELECT event_bytes FROM trace_storage_usage"
        ).fetchone()[0]
        < used
    )


def test_marker_referenced_by_another_generation_is_preserved(recorded):
    two_markers(recorded)
    db = recorded._connection
    generation = str(uuid4())
    with db:
        db.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation,next_export_seq,active) SELECT node_id,source_epoch,?,next_export_seq,0 FROM trace_export_generations",
            (generation,),
        )
        db.execute(
            "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256,state,collector_epoch) SELECT node_id,source_epoch,?,export_seq,event_id,journal_event_id,event_sha256,state,collector_epoch FROM trace_spool WHERE export_seq=4",
            (generation,),
        )
    before = snapshot(recorded)
    assert merge(recorded) == 0
    assert snapshot(recorded) == before


def test_actor_scopes_do_not_merge(recorded):
    prune(recorded)
    write(recorded, {**event(), "agent_id": "other-actor"})
    prune(recorded)
    before = snapshot(recorded)
    assert len(markers(recorded)) == 2
    assert merge(recorded) == 0
    assert snapshot(recorded) == before


def test_reconcile_coalesces_and_compacts_lost_rows(recorded):
    two_markers(recorded)
    recorded.reconcile(now_ms=int(time.time() * 1000))
    assert len(markers(recorded)) == 1
    assert (
        recorded._connection.execute("SELECT COUNT(*) FROM trace_spool").fetchone()[0]
        == 2
    )
    assert recorded._connection.execute("PRAGMA foreign_key_check").fetchall() == []


def retired_markers(store):
    from test_trace_retired_retention import prune as prune_retired
    from test_trace_retired_retention import rotate

    from edgecitadel_agentd.trace_journal import TraceJournal

    old, new = rotate(store)
    prune_retired(store)
    (original,) = markers(store)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        for _ in range(2):
            TraceJournal(store._connection).record(
                "edge-a", {**original, "event_id": str(uuid4())}, selected=True
            )
    return old, new


def test_retired_scope_and_discarded_marker_positions_stay_separate(recorded):
    from edgecitadel_agentd.trace_contract import coverage_scope

    old, new = retired_markers(recorded)
    db = recorded._connection
    original_scope = coverage_scope(markers(recorded)[0])
    original_source = tuple(
        db.execute(
            "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
        ).fetchone()
    )
    with db:
        db.execute(
            "UPDATE trace_spool SET state='core_settled' WHERE source_epoch=? AND export_seq=3",
            (new,),
        )
    assert merge(recorded) == 1
    result = {coverage_scope(e): e for e in markers(recorded)}
    assert result[original_scope]["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2}
    ]
    assert result[original_scope]["attributes"]["through_export_seq"] == 3
    (current,) = [e for scope, e in result.items() if scope != original_scope]
    assert coverage_scope(current)[1] == new
    assert current["attributes"]["lost_ranges"] == [
        {"first": 2, "last": 2},
        {"first": 4, "last": 4},
    ]
    assert tuple(
        db.execute(
            "SELECT state,journal_event_id FROM trace_spool WHERE source_epoch=? AND export_seq=3",
            (new,),
        ).fetchone()
    ) == ("core_settled", None)
    assert (
        tuple(
            db.execute(
                "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
            ).fetchone()
        )
        == original_source
    )
    assert not db.execute("PRAGMA foreign_key_check").fetchall()


def test_second_scope_marker_failure_rolls_back_replacement(recorded):
    retired_markers(recorded)
    before = snapshot(recorded)
    recorded._connection.execute(
        "CREATE TRIGGER owned_current_loss_failure BEFORE INSERT ON trace_journal WHEN json_extract(NEW.event_json,'$.kind')='coverage' AND json_extract(NEW.event_json,'$.attributes.affected_source_epoch') IS NULL BEGIN SELECT RAISE(ABORT,'owned current loss failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned current loss failure"):
        merge(recorded)
    assert snapshot(recorded) == before


def test_retired_markers_do_not_merge_with_current_loss(recorded):
    retired_markers(recorded)
    write(recorded, event())
    prune(recorded)
    assert merge(recorded) == 1
    # The separate current marker can merge only with the replacement's
    # current-stream loss in the following bounded maintenance pass.
    assert len(markers(recorded)) == 3
    assert merge(recorded) == 1
    assert len(markers(recorded)) == 2


def test_inactive_generation_in_same_epoch_keeps_distinct_coverage(recorded):
    from test_trace_retired_retention import prune as prune_retired

    from edgecitadel_agentd.trace_contract import coverage_scope
    from edgecitadel_agentd.trace_journal import TraceJournal

    db = recorded._connection
    epoch, old_generation = db.execute(
        "SELECT source_epoch,export_generation FROM trace_export_generations"
    ).fetchone()
    current_generation = str(uuid4())
    with db:
        db.execute("UPDATE trace_export_generations SET active=0")
        db.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation) VALUES ('edge-a',?,?)",
            (epoch, current_generation),
        )
    prune_retired(recorded)
    (original,) = markers(recorded)
    with db:
        db.execute("BEGIN IMMEDIATE")
        for _ in range(2):
            TraceJournal(db).record(
                "edge-a", {**original, "event_id": str(uuid4())}, selected=True
            )
    assert merge(recorded) == 1
    result = {coverage_scope(e): e for e in markers(recorded)}
    assert set(result) == {
        ("edge-a", epoch, old_generation),
        ("edge-a", epoch, current_generation),
    }
    assert result[("edge-a", epoch, old_generation)]["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2}
    ]
    assert result[("edge-a", epoch, current_generation)]["attributes"][
        "lost_ranges"
    ] == [{"first": 1, "last": 3}]
    assert all("affected_source_epoch" not in e["attributes"] for e in result.values())
    assert not db.execute("PRAGMA foreign_key_check").fetchall()


def test_all_settled_retired_markers_need_no_current_loss_marker(recorded):
    old, new = retired_markers(recorded)
    db = recorded._connection
    with db:
        db.execute(
            "UPDATE trace_spool SET state='core_settled' WHERE source_epoch=? AND export_seq>=2",
            (new,),
        )
    assert merge(recorded) == 2
    (replacement,) = markers(recorded)
    assert replacement["attributes"]["affected_source_epoch"] == old
    assert replacement["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
    receipts = db.execute(
        "SELECT state,journal_event_id FROM trace_spool WHERE source_epoch=? AND export_seq BETWEEN 2 AND 4",
        (new,),
    ).fetchall()
    assert [tuple(r) for r in receipts] == [("core_settled", None)] * 3


def fragmented_markers(store):
    """Synthetic sparse export history with two overlapping 100-range groups."""
    from edgecitadel_agentd.trace_journal import TraceJournal

    prune(store)
    (original,) = markers(store)
    db = store._connection
    with db:
        db.execute("BEGIN IMMEDIATE")
        # Positions below 1000 stand for already-pruned historical assignments.
        # Sparse ledgers retain their assigned high-watermark independently.
        db.execute("UPDATE trace_export_generations SET next_export_seq=1000")
        for start in (10, 10, 210, 210):
            ranges = [{"first": n, "last": n} for n in range(start, start + 200, 2)]
            TraceJournal(db).record(
                "edge-a",
                {
                    **original,
                    "event_id": str(uuid4()),
                    "attributes": {
                        **original["attributes"],
                        "through_export_seq": 999,
                        "lost_ranges": ranges,
                    },
                },
                selected=True,
            )
    return {
        n
        for e in markers(store)
        for r in e["attributes"]["lost_ranges"]
        for n in range(r["first"], r["last"] + 1)
    } | {
        r[0]
        for r in db.execute(
            "SELECT export_seq FROM trace_spool WHERE state IN ('pending','broker_acked') AND journal_event_id IS NOT NULL"
        )
    }


def test_fragmented_union_uses_multiple_bounded_markers(recorded):
    expected = fragmented_markers(recorded)
    before_bytes = recorded._connection.execute(
        "SELECT event_bytes FROM trace_storage_usage"
    ).fetchone()[0]
    assert merge(recorded) == 3
    replacements = markers(recorded)
    assert len(replacements) == 2
    assert all(1 <= len(e["attributes"]["lost_ranges"]) <= 128 for e in replacements)
    actual = {
        n
        for e in replacements
        for r in e["attributes"]["lost_ranges"]
        for n in range(r["first"], r["last"] + 1)
    }
    assert actual == expected
    stable = snapshot(recorded)
    assert merge(recorded) == 0
    assert snapshot(recorded) == stable  # No replacement without a byte reduction.
    assert 3 not in actual  # Settled position remains a hole.
    assert (
        recorded._connection.execute(
            "SELECT event_bytes FROM trace_storage_usage"
        ).fetchone()[0]
        < before_bytes
    )
    assert recorded._connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_second_fragment_write_failure_restores_every_original_marker(recorded):
    fragmented_markers(recorded)
    before = snapshot(recorded)
    recorded._connection.execute(
        "CREATE TRIGGER owned_second_fragment_failure BEFORE INSERT ON trace_journal WHEN json_extract(NEW.event_json,'$.attributes.lost_ranges[0].first')>100 BEGIN SELECT RAISE(ABORT,'owned second fragment failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned second fragment failure"):
        merge(recorded)
    assert snapshot(recorded) == before


def test_markers_from_retired_writer_are_replaced_under_current_identity(recorded):
    from test_trace_retired_retention import rotate

    from edgecitadel_agentd.trace_contract import coverage_scope

    two_markers(recorded)
    old, current = rotate(recorded)
    db = recorded._connection
    old_source = tuple(
        db.execute(
            "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
        ).fetchone()
    )
    old_generation = tuple(
        db.execute(
            "SELECT * FROM trace_export_generations WHERE source_epoch=?", (old,)
        ).fetchone()
    )
    assert merge(recorded) == 1
    (replacement,) = markers(recorded)
    assert replacement["source_epoch"] == current
    assert coverage_scope(replacement) == ("edge-a", old, old_generation[2])
    assert replacement["attributes"]["lost_ranges"] == [
        {"first": 1, "last": 2},
        {"first": 4, "last": 6},
    ]
    assert replacement["attributes"]["through_export_seq"] == 6
    assert (
        tuple(
            db.execute(
                "SELECT * FROM trace_sources WHERE source_epoch=?", (old,)
            ).fetchone()
        )
        == old_source
    )
    assert (
        tuple(
            db.execute(
                "SELECT * FROM trace_export_generations WHERE source_epoch=?", (old,)
            ).fetchone()
        )
        == old_generation
    )
    assert (
        db.execute(
            "SELECT COUNT(*) FROM trace_spool WHERE source_epoch=? AND state='pending'",
            (current,),
        ).fetchone()[0]
        == 2
    )
    assert not db.execute("PRAGMA foreign_key_check").fetchall()


def test_twice_retired_marker_origin_and_affected_scope_stay_separate(recorded):
    from test_trace_retired_retention import rotate

    from edgecitadel_agentd.trace_contract import coverage_scope

    oldest, middle = retired_markers(recorded)
    retired, current = rotate(recorded)
    assert retired == middle
    db = recorded._connection
    with db:
        db.execute(
            "UPDATE trace_spool SET state='core_settled' WHERE source_epoch=? AND export_seq=3",
            (middle,),
        )
    old_sources = [
        tuple(r)
        for r in db.execute(
            "SELECT * FROM trace_sources WHERE active=0 ORDER BY source_epoch"
        )
    ]
    old_generations = [
        tuple(r)
        for r in db.execute(
            "SELECT * FROM trace_export_generations WHERE source_epoch<>? ORDER BY source_epoch,export_generation",
            (current,),
        )
    ]
    assert merge(recorded) == 1
    result = {coverage_scope(e)[1]: e for e in markers(recorded)}
    assert set(result) == {oldest, middle}
    assert all(e["source_epoch"] == current for e in result.values())
    assert result[oldest]["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
    assert result[middle]["attributes"]["lost_ranges"] == [
        {"first": 2, "last": 2},
        {"first": 4, "last": 4},
    ]
    assert tuple(
        db.execute(
            "SELECT state,journal_event_id FROM trace_spool WHERE source_epoch=? AND export_seq=3",
            (middle,),
        ).fetchone()
    ) == ("core_settled", None)
    assert [
        tuple(r)
        for r in db.execute(
            "SELECT * FROM trace_sources WHERE active=0 ORDER BY source_epoch"
        )
    ] == old_sources
    assert [
        tuple(r)
        for r in db.execute(
            "SELECT * FROM trace_export_generations WHERE source_epoch<>? ORDER BY source_epoch,export_generation",
            (current,),
        )
    ] == old_generations


def test_retired_origin_replacement_failure_preserves_all_identities(recorded):
    from test_trace_retired_retention import rotate

    _oldest, middle = retired_markers(recorded)
    rotate(recorded)
    before = snapshot(recorded)
    # middle is an internally generated UUID, never caller-provided SQL text.
    recorded._connection.execute(
        f"CREATE TRIGGER owned_retired_origin_failure BEFORE INSERT ON trace_journal WHEN json_extract(NEW.event_json,'$.attributes.affected_source_epoch')='{middle}' BEGIN SELECT RAISE(ABORT,'owned retired origin failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned retired origin failure"):
        merge(recorded)
    assert snapshot(recorded) == before
