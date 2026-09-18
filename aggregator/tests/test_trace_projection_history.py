import sqlite3
from contextlib import closing
from copy import deepcopy
from uuid import uuid4

import pytest
from test_trace_graph_projection import READ, TRACE
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event, ingest

from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_store as projection
from aggregator.trace_projection_tables import select_tables

core = core_fixture


def graph(db, state=None):
    return projection.read_graph(
        db,
        trace_id=TRACE,
        generation=state.generation if state else None,
        at_cursor=state.change_cursor if state else None,
    )


def workload(db):
    generation = str(uuid4())
    root = event("completed", seq=2)
    child = event(seq=1, task_id=str(uuid4()), parent_task_id=root["task_id"])
    later = event(
        "completed", seq=3, task_id=str(uuid4()), parent_task_id=root["task_id"]
    )
    loss = event(
        "lost",
        kind="coverage",
        seq=4,
        trace_id=TRACE,
        attributes={
            "export_generation": generation,
            "through_export_seq": 4,
            "lost_ranges": [{"first": 4, "last": 4}],
        },
    )
    unsupported = event(
        "unknown",
        kind="coverage",
        seq=5,
        trace_id=TRACE,
        attributes={
            "export_generation": generation,
            "through_export_seq": 6,
            "unsupported_families": ["tool"],
        },
    )
    for value, position in [
        (child, 2),
        (root, 1),
        (later, 3),
        (loss, 5),
        (root, 4),
        (unsupported, 6),
        (event("failed", seq=6), 7),
        (event("started", kind="model", seq=7), 8),
        (event("finished", kind="model", seq=8), 9),
    ]:
        assert put(db, value, generation, position).outcome in {"accepted", "duplicate"}
    snapshots = [graph(db)]
    for _ in range(9):
        projection.project_batch(db, limit=1)
        snapshots.append(graph(db))
    assert (
        snapshots[1]["unresolved_ancestry"] and snapshots[1]["coverage"]["catching_up"]
    )
    assert (
        not snapshots[2]["unresolved_ancestry"]
        and not snapshots[2]["coverage"]["catching_up"]
    )
    assert snapshots[4]["coverage"]["gap"] and not snapshots[5]["coverage"]["gap"]
    assert snapshots[5]["coverage"]["unsupported_families"] == []
    assert snapshots[6]["coverage"]["unsupported_families"] == ["tool"]
    return snapshots


def test_every_cursor_reconstructs_nodes_ancestry_outcomes_and_coverage(core):
    snapshots = workload(core)
    for expected in snapshots:
        assert graph(core, expected["state"]) == expected
        assert (
            core.execute(
                "SELECT name FROM sqlite_temp_master WHERE type='view'"
            ).fetchall()
            == []
        )
        assert not core.in_transaction
    root = next(
        n
        for n in snapshots[-1]["nodes"]
        if n["kind"] == "task"
        and n.get("task_id") == READ["input_events"][0]["task_id"]
    )
    assert root["conflict"] and root["state"] == "completed"


def test_multi_event_batch_and_rebuild_preserve_each_intermediate_cursor(core):
    snapshots = workload(core)
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    for expected in snapshots:
        state = expected["state"]
        actual = projection.read_graph(
            core,
            trace_id=TRACE,
            generation=candidate.generation,
            at_cursor=state.change_cursor,
        )
        assert actual["state"].ingest_cursor == state.ingest_cursor
        assert {k: v for k, v in actual.items() if k != "state"} == {
            k: v for k, v in expected.items() if k != "state"
        }
    with pytest.raises(ValueError, match="generation_mismatch"):
        graph(core, snapshots[-1]["state"])


def test_retained_base_and_later_playback_survive_incremental_compaction(core):
    snapshots = workload(core)
    current = snapshots[-1]["state"]
    raw = core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
    for _ in range(200):
        result = history.compact_batch(
            core, generation=current.generation, through_cursor=5, limit=2
        )
        assert result["deleted_rows"] <= 2
        assert graph(core, snapshots[5]["state"]) == snapshots[5]
        assert graph(core) == snapshots[-1]
        with pytest.raises(ValueError, match="cursor_expired"):
            graph(core, snapshots[4]["state"])
        if result["complete"]:
            break
    else:
        pytest.fail("compaction did not converge")
    for expected in snapshots[5:]:
        assert graph(core, expected["state"]) == expected
    with pytest.raises(ValueError, match="cursor_expired"):
        projection.read_changes(core, generation=current.generation, after=4)
    changes = projection.read_changes(core, generation=current.generation, after=5)[
        "changes"
    ]
    assert [c["cursor"] for c in changes] == [6, 7, 8, 9]
    assert (
        core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
        == raw
    )
    assert (
        core.execute(
            "SELECT MIN(cursor) FROM trace_projection_history_cursors"
        ).fetchone()[0]
        == 5
    )
    assert (
        history.compact_batch(
            core, generation=current.generation, through_cursor=5, limit=2
        )["deleted_rows"]
        == 0
    )


def test_compaction_abort_keeps_old_floor_and_history(core):
    snapshots = workload(core)
    core.execute(
        "CREATE TRIGGER compact_fault BEFORE DELETE ON trace_projection_history_rows "
        "BEGIN SELECT RAISE(ABORT,'compaction failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="compaction failure"):
        history.compact_batch(
            core, generation=snapshots[-1]["state"].generation, through_cursor=5
        )
    assert list(core.iterdump()) == before
    assert graph(core, snapshots[1]["state"]) == snapshots[1]


def test_reader_pinned_before_compaction_can_finish_old_snapshot(core):
    snapshots = workload(core)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    writes, errors = [], []

    def compact(sql):
        if 'SELECT node_json FROM "trace_projected_tasks"' not in sql or writes:
            return
        writes.append(True)
        try:
            with closing(sqlite3.connect(path)) as writer:
                while not history.compact_batch(
                    writer,
                    generation=snapshots[-1]["state"].generation,
                    through_cursor=9,
                )["complete"]:
                    pass
        except Exception as error:
            errors.append(error)

    core.set_trace_callback(compact)
    try:
        actual = graph(core, snapshots[1]["state"])
    finally:
        core.set_trace_callback(None)
    assert writes and not errors
    assert actual == snapshots[1]
    with pytest.raises(ValueError, match="cursor_expired"):
        graph(core, snapshots[1]["state"])
    assert graph(core, snapshots[-1]["state"]) == snapshots[-1]


def test_historical_query_error_leaves_no_views_or_transaction(core):
    snapshots = workload(core)

    def deny_history(action, arg1, arg2, database, trigger):
        if action == sqlite3.SQLITE_READ and arg1 == "trace_projection_history_rows":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    core.set_authorizer(deny_history)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            graph(core, snapshots[1]["state"])
    finally:
        core.set_authorizer(None)
    assert not core.in_transaction
    assert (
        core.execute("SELECT name FROM sqlite_temp_master WHERE type='view'").fetchall()
        == []
    )
    assert graph(core) == snapshots[-1]


def test_frozen_join_history_and_late_parent_use_observation_order(core):
    snapshots = []
    for value in (
        READ["input_events"][1],
        READ["input_events"][3],
        READ["input_events"][0],
        READ["input_events"][2],
    ):
        ingest(core, deepcopy(value))
        projection.project_batch(core)
        snapshots.append(graph(core))
    for expected in snapshots:
        assert graph(core, expected["state"]) == expected
    assert (
        snapshots[0]["unresolved_ancestry"] and not snapshots[-1]["unresolved_ancestry"]
    )


def test_row_deletion_is_a_tombstone_and_base_cleanup_never_resurrects_it(core):
    snapshots = workload(core)
    old = snapshots[-1]["state"]
    # Exercise the capture boundary that future policy-driven graph expiry uses.
    # This synthetic maintenance change is not a production expiry scheduler.
    with core:
        core.execute("BEGIN IMMEDIATE")
        tables = select_tables(core)
        history.start_change(tables, old.change_cursor + 1, old.ingest_cursor)
        tables.execute("DELETE FROM {trace_projected_entities}")
        tables.execute(
            "UPDATE {trace_projection_state} SET change_cursor=?",
            (old.change_cursor + 1,),
        )
        history.finish_changes(tables)
    deleted = graph(core)
    assert any(n["kind"] == "model" for n in snapshots[-1]["nodes"])
    assert not any(n["kind"] == "model" for n in deleted["nodes"])
    assert graph(core, old) == snapshots[-1]
    state = deleted["state"]
    for _ in range(200):
        result = history.compact_batch(
            core,
            generation=state.generation,
            through_cursor=state.change_cursor,
            limit=2,
        )
        assert graph(core, state) == deleted
        if result["complete"]:
            break
    else:
        pytest.fail("tombstone compaction did not converge")
    assert (
        core.execute(
            "SELECT count(*) FROM trace_projection_history_rows WHERE table_name='trace_projected_entities'"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize(
    "cursor,error",
    [
        (-1, "invalid_projection_cursor"),
        (True, "invalid_projection_cursor"),
        (1, "cursor_ahead"),
    ],
)
def test_invalid_or_future_cursors_are_rejected(core, cursor, error):
    state = projection.initialize(core)
    with pytest.raises(ValueError, match=error):
        projection.read_graph(
            core, trace_id=TRACE, generation=state.generation, at_cursor=cursor
        )


def test_retained_range_and_observation_watermark_reject_expired_snapshots(core):
    snapshots = workload(core)
    state = snapshots[-1]["state"]
    history.compact_batch(core, generation=state.generation, through_cursor=5, limit=1)
    retained = history.retained_range(core)
    assert retained == {
        "state": state,
        "from_cursor": 5,
        "from_ingest_seq": 5,
        "through_cursor": 9,
    }
    task_id = READ["input_events"][0]["task_id"]
    with pytest.raises(ValueError, match="cursor_expired"):
        projection.read_outcomes(
            core, generation=state.generation, trace_id=TRACE, task_id=task_id, as_of=4
        )
    # Earlier terminal candidates still belong to a retained current graph.
    candidates = projection.read_outcomes(
        core, generation=state.generation, trace_id=TRACE, task_id=task_id, as_of=9
    )["outcomes"]
    assert {candidate["phase"] for candidate in candidates} == {"completed", "failed"}


def test_source_wide_fact_does_not_leak_into_earlier_run_coverage(core):
    value = event(seq=1)
    ingest(core, value)
    projection.project_batch(core)
    before = graph(core)
    ingest(
        core,
        event(
            "unknown",
            kind="coverage",
            seq=2,
            attributes={"export_generation": str(uuid4()), "through_export_seq": 0},
        ),
    )
    projection.project_batch(core)
    assert graph(core)["coverage_reasons"]["source_uncertainty"]
    assert not before["coverage_reasons"]["source_uncertainty"]
    assert graph(core, before["state"]) == before


def test_history_capture_rolls_back_with_failed_projection_checkpoint(core):
    value = event("completed")
    ingest(core, value)
    core.execute(
        "CREATE TRIGGER capture_fault BEFORE UPDATE ON trace_projection_state "
        "BEGIN SELECT RAISE(ABORT,'capture failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="capture failure"):
        projection.project_batch(core)
    assert list(core.iterdump()) == before
    assert (
        core.execute("SELECT count(*) FROM trace_projection_history_rows").fetchone()[0]
        == 0
    )
    core.execute("DROP TRIGGER capture_fault")
    state = projection.project_batch(core)
    assert graph(core, state) == graph(core)


def test_late_branch_after_compaction_does_not_change_retained_base(core):
    snapshots = workload(core)
    state = snapshots[-1]["state"]
    while not history.compact_batch(
        core, generation=state.generation, through_cursor=9
    )["complete"]:
        pass
    ingest(
        core,
        event(
            "completed",
            seq=10,
            task_id=str(uuid4()),
            parent_task_id=READ["input_events"][0]["task_id"],
        ),
    )
    projection.project_batch(core)
    later = graph(core)
    assert len(later["nodes"]) == len(snapshots[-1]["nodes"]) + 1
    assert graph(core, state) == snapshots[-1]
    assert history.retained_range(core)["through_cursor"] == 10


def test_version_three_cannot_claim_historical_capture(core):
    core.execute("UPDATE trace_projection_state SET version=3")
    core.commit()
    with pytest.raises(ValueError, match="version_unavailable"):
        projection.initialize(core)
