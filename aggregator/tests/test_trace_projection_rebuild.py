import select
import sqlite3
import subprocess
import sys
from contextlib import closing
from uuid import uuid4

import pytest
from test_trace_graph_projection import READ, TRACE
from test_trace_projection_store import core as core_fixture, event, ingest

from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_store as projection
from aggregator import trace_retention

core = core_fixture


def graph(db, generation=None):
    return projection.read_graph(db, trace_id=TRACE, build_generation=generation)


def seed(db):
    for value in READ["input_events"]:
        ingest(db, value)
    ingest(
        db,
        event(
            "unknown",
            kind="coverage",
            seq=99,
            trace_id=TRACE,
            attributes={
                "export_generation": str(uuid4()),
                "through_export_seq": 0,
                "unsupported_families": ["tool"],
            },
        ),
    )
    ingest(db, event("completed", seq=96))
    ingest(db, event("failed", seq=97))
    ingest(db, event("finished", kind="model", seq=98))
    return projection.project_batch(db)


def catch_up(db, generation):
    while True:
        state = projection.project_batch(db, build_generation=generation, limit=2)
        if (
            state.ingest_cursor
            == db.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0]
        ):
            return state


def test_rebuild_resume_equivalence_switch_and_stale_cursors(core):
    old = seed(core)
    expected = graph(core)
    expected_changes = projection.read_changes(
        core, generation=old.generation, after=0
    )["changes"]
    task_id = READ["input_events"][0]["task_id"]
    expected_task = projection.read_task(core, trace_id=TRACE, task_id=task_id)["node"]
    expected_outcomes = projection.read_outcomes(
        core,
        generation=old.generation,
        trace_id=TRACE,
        task_id=task_id,
        as_of=old.ingest_cursor,
    )["outcomes"]
    candidate = rebuild.begin(core)
    assert candidate.generation != old.generation
    assert graph(core) == expected
    assert graph(core, candidate.generation)["nodes"] == []
    partial = projection.project_batch(
        core, build_generation=candidate.generation, limit=1
    )
    assert partial.ingest_cursor == 1 and graph(core) == expected
    path = core.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as resumed:
        complete = catch_up(resumed, candidate.generation)
        actual = graph(resumed, candidate.generation)
        assert {k: v for k, v in actual.items() if k != "state"} == {
            k: v for k, v in expected.items() if k != "state"
        }
        assert complete.ingest_cursor == old.ingest_cursor
        assert complete.change_cursor == old.change_cursor
        assert rebuild.activate(resumed, generation=candidate.generation) == complete
    assert projection.initialize(core) == complete
    assert (
        projection.read_task(core, trace_id=TRACE, task_id=task_id)["node"]
        == expected_task
    )
    assert (
        projection.read_outcomes(
            core,
            generation=complete.generation,
            trace_id=TRACE,
            task_id=task_id,
            as_of=complete.ingest_cursor,
        )["outcomes"]
        == expected_outcomes
    )
    assert graph(core)["state"] == complete
    with pytest.raises(ValueError, match="projection_generation_mismatch"):
        projection.read_changes(core, generation=old.generation, after=0)
    assert (
        projection.read_changes(core, generation=complete.generation, after=0)[
            "changes"
        ]
        == expected_changes
    )


def test_late_ingestion_requires_catchup_before_activation(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    ingest(core, event("completed", seq=100))
    with pytest.raises(ValueError, match="projection_rebuild_not_caught_up"):
        rebuild.activate(core, generation=candidate.generation)
    assert graph(core)["state"] == old
    catch_up(core, candidate.generation)
    new = rebuild.activate(core, generation=candidate.generation)
    assert new.ingest_cursor == old.ingest_cursor + 1
    assert projection.project_batch(core) == new


def test_activation_failure_preserves_both_generations(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    core.execute(
        "CREATE TRIGGER switch_fault BEFORE UPDATE ON trace_projection_generations "
        "WHEN NEW.status='active' BEGIN SELECT RAISE(ABORT,'switch failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="switch failure"):
        rebuild.activate(core, generation=candidate.generation)
    assert list(core.iterdump()) == before
    assert graph(core)["state"] == old
    core.execute("DROP TRIGGER switch_fault")
    rebuild.activate(core, generation=candidate.generation)


def test_candidate_creation_is_atomic_and_does_not_touch_raw(core):
    seed(core)
    core.execute(
        "CREATE TRIGGER build_fault BEFORE INSERT ON trace_projection_generations "
        "WHEN NEW.status='building' BEGIN SELECT RAISE(ABORT,'build failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="build failure"):
        rebuild.begin(core)
    assert list(core.iterdump()) == before


def test_reader_snapshot_stays_on_old_generation_during_switch(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    writes, errors = [], []

    def switch(sql):
        if 'FROM "trace_projected_tasks"' not in sql or writes:
            return
        writes.append(True)
        try:
            with closing(sqlite3.connect(path)) as writer:
                rebuild.activate(writer, generation=candidate.generation)
        except Exception as error:
            errors.append(error)

    core.set_trace_callback(switch)
    try:
        before = graph(core)
    finally:
        core.set_trace_callback(None)
    assert writes and not errors
    assert before["state"] == old
    assert graph(core)["state"].generation == candidate.generation
    assert not core.in_transaction


def test_retired_cleanup_is_bounded_raw_safe_and_allows_next_build(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    with pytest.raises(ValueError, match="cleanup_or_resume"):
        rebuild.begin(core)
    catch_up(core, candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    with pytest.raises(ValueError, match="retired_generation_required"):
        rebuild.cleanup_batch(core, generation=candidate.generation)
    with pytest.raises(ValueError, match="build_unavailable"):
        rebuild.cancel(core, generation=candidate.generation)
    expected = graph(core)
    raw = core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
    deleted = 0
    for _ in range(200):
        result = rebuild.cleanup_batch(core, generation=old.generation, limit=1)
        assert result["deleted_rows"] <= 1
        deleted += result["deleted_rows"]
        assert graph(core) == expected
        if result["complete"]:
            break
    else:
        pytest.fail("cleanup did not finish")
    assert deleted > 0
    assert (
        core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
        == raw
    )
    assert (
        core.execute("SELECT count(*) FROM trace_projection_generations").fetchone()[0]
        == 1
    )
    second = rebuild.begin(core)
    catch_up(core, second.generation)
    rebuild.activate(core, generation=second.generation)
    assert graph(core)["nodes"] == expected["nodes"]


def test_restore_fences_candidate_and_failed_build_can_be_cancelled(core):
    seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    core.execute("UPDATE trace_collector SET collector_epoch=?", (str(uuid4()),))
    core.commit()
    with pytest.raises(ValueError, match="rebuild_required"):
        rebuild.activate(core, generation=candidate.generation)
    rebuild.cancel(core, generation=candidate.generation)
    for _ in range(100):
        if rebuild.cleanup_batch(core, generation=candidate.generation)["complete"]:
            break
    replacement = rebuild.begin(core)
    catch_up(core, replacement.generation)
    rebuild.activate(core, generation=replacement.generation)
    assert graph(core)["nodes"]


def test_incompatible_active_version_is_replayed_not_reused(core):
    old = seed(core)
    core.execute("UPDATE trace_projection_state SET version=2")
    core.commit()
    with pytest.raises(ValueError, match="version_unavailable"):
        projection.initialize(core)
    candidate = rebuild.begin(core)
    assert candidate.ingest_cursor == 0
    catch_up(core, candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert graph(core)["state"].ingest_cursor == old.ingest_cursor


def test_rebuild_expired_inputs_reports_loss_without_resurrecting_graph(core):
    seed(core)
    assert graph(core)["nodes"]
    trace_retention.expire_payloads(core, now_ms=trace_retention.RETENTION_MS + 2)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    rebuilt = graph(core, candidate.generation)
    assert not rebuilt["nodes"] and rebuilt["coverage"]["partial"]
    rebuild.activate(core, generation=candidate.generation)
    changes = projection.read_changes(core, generation=candidate.generation, after=0)[
        "changes"
    ]
    assert all(
        c["kind"] == "payload_expired" and c["trace_id"] is None for c in changes
    )


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_sigkill_switch_recovers_one_active_generation(core, boundary):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    program = """
import sqlite3,sys,time
from aggregator.trace_projection_rebuild import activate
db=sqlite3.connect(sys.argv[1])
def pause():
    print("ready",flush=True)
    while True: time.sleep(1)
if sys.argv[3]=="before_commit":
    db.create_function("owned_pause",0,pause)
    db.execute("CREATE TEMP TRIGGER owned_fault BEFORE UPDATE ON trace_projection_generations WHEN NEW.status='active' BEGIN SELECT owned_pause(); END")
activate(db,generation=sys.argv[2])
pause()
"""
    with subprocess.Popen(
        [sys.executable, "-c", program, path, candidate.generation, boundary],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            ready, _, _ = select.select([child.stdout], [], [], 20)
            assert ready, "owned child did not reach activation boundary"
            assert child.stdout.readline().strip() == "ready"
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
    assert graph(core)["state"].generation == (
        old.generation if boundary == "before_commit" else candidate.generation
    )
    assert (
        core.execute(
            "SELECT count(*) FROM trace_projection_generations WHERE status='active'"
        ).fetchone()[0]
        == 1
    )
    assert core.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_compatible_predecessor_rollback_catches_up_and_invalidates_old_cursors(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    ingest(core, event("completed", seq=100))
    projection.project_batch(core)
    expected = graph(core)
    rollback = rebuild.prepare_rollback(core, generation=old.generation)
    assert rollback.generation not in {old.generation, candidate.generation}
    with pytest.raises(ValueError, match="not_caught_up"):
        rebuild.activate(core, generation=rollback.generation)
    catch_up(core, rollback.generation)
    rebuild.activate(core, generation=rollback.generation)
    actual = graph(core)
    assert {k: v for k, v in actual.items() if k != "state"} == {
        k: v for k, v in expected.items() if k != "state"
    }
    for stale in (old.generation, candidate.generation):
        with pytest.raises(ValueError, match="generation_mismatch"):
            projection.read_changes(core, generation=stale, after=0)


def test_cleanup_ends_rollback_eligibility(core):
    old = seed(core)
    candidate = rebuild.begin(core)
    catch_up(core, candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    rebuild.cleanup_batch(core, generation=old.generation, limit=1)
    with pytest.raises(ValueError, match="rollback_unavailable"):
        rebuild.prepare_rollback(core, generation=old.generation)
