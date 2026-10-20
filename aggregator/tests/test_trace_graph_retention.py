import select
import sqlite3
import subprocess
import sys
from contextlib import closing
from uuid import uuid4

import pytest
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retention
from aggregator import trace_projection_store as projection

core = core_fixture
NOW = retention.RETENTION_MS + 3


def graph(db, value, state=None):
    return projection.read_graph(
        db,
        trace_id=value["trace_id"],
        generation=state.generation if state else None,
        at_cursor=state.change_cursor if state else None,
    )


def seed(db):
    value = event("completed", seq=1)
    put(db, value, str(uuid4()), 1, received_at_ms=1)
    projection.project_batch(db)
    return value


def clean(db, generation=None):
    for _ in range(100):
        result = retention.cleanup_batch(
            db, now_ms=NOW, limit=1, build_generation=generation
        )
        assert result["deleted_rows"] <= 1
        if result["complete"]:
            return
    pytest.fail("retired trace cleanup did not finish")


def test_expiry_hides_atomically_and_preserves_historical_graph_and_candidates(core):
    value = seed(core)
    put(core, event("failed", seq=2), str(uuid4()), 1, received_at_ms=2)
    state = projection.project_batch(core)
    before = graph(core, value)
    raw = core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
    positions = core.execute(
        "SELECT * FROM trace_ingest_positions ORDER BY ingest_seq"
    ).fetchall()
    assert (
        retention.expire_one(core, now_ms=NOW - 1)["status"] == "idle"
    )  # Exact cutoff is retained.
    expired = retention.expire_one(core, now_ms=NOW)
    assert (
        expired["status"] == "retired" and expired["cursor"] == state.change_cursor + 1
    )
    with pytest.raises(ValueError, match="trace_expired"):
        graph(core, value)
    with pytest.raises(ValueError, match="trace_expired"):
        projection.read_task(core, trace_id=value["trace_id"], task_id=value["task_id"])
    assert graph(core, value, state) == before
    path = core.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as resumed:
        clean(resumed)
    assert graph(core, value)["nodes"] == []
    assert graph(core, value, state) == before
    outcomes = projection.read_outcomes(
        core,
        generation=state.generation,
        trace_id=value["trace_id"],
        task_id=value["task_id"],
        as_of=state.ingest_cursor,
        at_cursor=state.change_cursor,
    )["outcomes"]
    assert {row["phase"] for row in outcomes} == {"completed", "failed"}
    assert (
        core.execute("SELECT * FROM trace_raw_events ORDER BY ingest_seq").fetchall()
        == raw
    )
    assert (
        core.execute(
            "SELECT * FROM trace_ingest_positions ORDER BY ingest_seq"
        ).fetchall()
        == positions
    )
    assert core.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0] == 2
    changes = projection.read_changes(
        core, generation=state.generation, after=state.change_cursor
    )["changes"]
    assert changes[0]["kind"] == "trace_expired"
    assert all(change["ingest_seq"] is None for change in changes)
    assert changes[-1]["change"]["cleanup_complete"]


def test_duplicate_receipts_neither_renew_nor_resurrect_expired_graph(core):
    value = seed(core)
    assert put(core, value, str(uuid4()), 1, received_at_ms=NOW).outcome == "duplicate"
    projection.project_batch(core)
    assert retention.expire_one(core, now_ms=NOW)["status"] == "retired"
    clean(core)
    assert (
        put(core, value, str(uuid4()), 1, received_at_ms=NOW + 1).outcome == "duplicate"
    )
    projection.project_batch(core)
    assert graph(core, value)["nodes"] == []
    assert core.execute("SELECT count(*) FROM trace_projection_runs").fetchone()[0] == 0
    fresh = event("running", seq=2, task_id=str(uuid4()))
    put(core, fresh, str(uuid4()), 1, received_at_ms=NOW + 2)
    projection.project_batch(core)
    assert {node["task_id"] for node in graph(core, fresh)["nodes"]} == {
        fresh["task_id"]
    }
    assert retention.expire_one(core, now_ms=NOW + 3)["status"] == "idle"


def test_unprojected_fresh_input_prevents_premature_expiry(core):
    value = seed(core)
    put(core, event("failed", seq=2), str(uuid4()), 1, received_at_ms=NOW)
    before = core.total_changes
    assert retention.expire_one(core, now_ms=NOW)["status"] == "catching_up"
    assert core.total_changes == before
    projection.project_batch(core)
    assert retention.expire_one(core, now_ms=NOW)["status"] == "idle"
    assert graph(core, value)["nodes"][0]["conflict"]


def test_raw_ingestion_continues_while_cleanup_fences_projection(core):
    value = seed(core)
    retention.expire_one(core, now_ms=NOW)
    fresh = event("failed", seq=2)
    other = event(seq=3, trace_id=uuid4().hex, task_id=str(uuid4()))
    assert put(core, fresh, str(uuid4()), 1, received_at_ms=NOW).outcome == "accepted"
    assert put(core, other, str(uuid4()), 1, received_at_ms=NOW).outcome == "accepted"
    before = list(core.iterdump())
    with pytest.raises(ValueError, match="retirement_pending"):
        projection.project_batch(core)
    assert list(core.iterdump()) == before
    clean(core)
    state = projection.project_batch(core)
    assert state.ingest_cursor == 3
    node = graph(core, value)["nodes"][0]
    assert node["state"] == "failed" and node["outcome_candidate_count"] == 1
    assert graph(core, other)["nodes"]


def test_test_only_runs_expire_first_and_mixed_provenance_is_normal(core):
    normal = event(seq=1, trace_id=uuid4().hex)
    disposable = event(seq=2, trace_id=uuid4().hex, test_run_id=str(uuid4()))
    mixed = event(seq=3, trace_id=uuid4().hex, test_run_id=str(uuid4()))
    for value, stamp in [(normal, 1), (disposable, 2), (mixed, 1)]:
        put(core, value, str(uuid4()), 1, received_at_ms=stamp)
    put(
        core,
        event(seq=4, trace_id=mixed["trace_id"]),
        str(uuid4()),
        1,
        received_at_ms=2,
    )
    projection.project_batch(core)
    assert retention.expire_one(core, now_ms=NOW)["trace_id"] == disposable["trace_id"]
    clean(core)
    assert retention.expire_one(core, now_ms=NOW)["trace_id"] == normal["trace_id"]
    assert graph(core, mixed)["nodes"]


@pytest.mark.parametrize("boundary", ["expiry", "cleanup"])
def test_retirement_failure_rolls_back_visibility_history_and_checkpoint(
    core, boundary
):
    value = seed(core)
    if boundary == "cleanup":
        retention.expire_one(core, now_ms=NOW)
    core.execute(
        "CREATE TRIGGER retirement_fault BEFORE INSERT ON trace_projection_changes "
        "BEGIN SELECT RAISE(ABORT,'retirement failure'); END"
    )
    before = list(core.iterdump())
    operation = (
        retention.expire_one if boundary == "expiry" else retention.cleanup_batch
    )
    with pytest.raises(sqlite3.IntegrityError, match="retirement failure"):
        operation(core, now_ms=NOW)
    assert list(core.iterdump()) == before
    if boundary == "expiry":
        assert graph(core, value)["nodes"]


def test_rebuild_must_apply_retirement_policy_before_switching_readers(core):
    value = seed(core)
    retention.expire_one(core, now_ms=NOW)
    clean(core)
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    with pytest.raises(ValueError, match="retention_catchup_required"):
        rebuild.activate(core, generation=candidate.generation)
    assert (
        retention.expire_one(core, now_ms=NOW, build_generation=candidate.generation)[
            "status"
        ]
        == "retired"
    )
    clean(core, candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert graph(core, value)["nodes"] == []


def test_rollback_cannot_revive_graph_retired_by_current_policy(core):
    value = seed(core)
    old = projection.initialize(core)
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    retention.expire_one(core, now_ms=NOW)
    clean(core)
    rollback = rebuild.prepare_rollback(core, generation=old.generation)
    with pytest.raises(ValueError, match="retention_catchup_required"):
        rebuild.activate(core, generation=rollback.generation)
    retention.expire_one(core, now_ms=NOW, build_generation=rollback.generation)
    clean(core, rollback.generation)
    rebuild.activate(core, generation=rollback.generation)
    assert graph(core, value)["nodes"] == []


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_sigkill_retirement_has_one_visibility_boundary(core, boundary):
    value = seed(core)
    old = graph(core, value)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    program = """
import sqlite3,sys,time
from aggregator.trace_projection_retention import expire_one
db=sqlite3.connect(sys.argv[1])
def pause():
    print("ready",flush=True)
    while True: time.sleep(1)
if sys.argv[3]=="before_commit":
    db.create_function("owned_pause",0,pause)
    db.execute("CREATE TEMP TRIGGER owned_fault BEFORE INSERT ON trace_projection_changes WHEN NEW.kind='trace_expired' BEGIN SELECT owned_pause(); END")
expire_one(db,now_ms=int(sys.argv[2]))
pause()
"""
    with subprocess.Popen(
        [sys.executable, "-c", program, path, str(NOW), boundary],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            ready, _, _ = select.select([child.stdout], [], [], 20)
            assert ready, "owned child did not reach retirement boundary"
            assert child.stdout.readline().strip() == "ready"
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
    if boundary == "before_commit":
        assert graph(core, value) == old
    else:
        with pytest.raises(ValueError, match="trace_expired"):
            graph(core, value)
    assert graph(core, value, old["state"]) == old
    assert core.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_missing_current_policy_is_not_silently_treated_as_no_retention(core):
    seed(core)
    core.execute("DROP TABLE trace_projection_retention_state")
    with pytest.raises(ValueError, match="retention_state_unavailable"):
        retention.expire_one(core, now_ms=NOW)


def test_maintenance_cycle_cleans_then_projects_pending_fresh_evidence(core):
    from aggregator.trace_projection_maintenance import run_cycle

    value = seed(core)
    first = run_cycle(core, now_ms=NOW)
    assert first["retirement"]["status"] == "retired"
    fresh = event("failed", seq=2)
    put(core, fresh, str(uuid4()), 1, received_at_ms=NOW)
    cleanup_passes = 0
    for _ in range(100):
        result = run_cycle(core, now_ms=NOW)
        assert not core.in_transaction
        if result["phase"] == "cleanup":
            cleanup_passes += 1
            assert result["deleted_rows"] <= 256
        elif result["phase"] == "projection":
            assert result["retirement"]["status"] == "idle"
            break
    else:
        pytest.fail("maintenance did not finish retirement")
    assert cleanup_passes
    node = graph(core, value)["nodes"][0]
    assert node["state"] == "failed" and node["outcome_candidate_count"] == 1
    assert core.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0] == 2


def test_maintenance_does_not_retire_until_its_bounded_projection_catches_up(core):
    from aggregator.trace_projection_maintenance import run_cycle

    value = event(seq=1)
    for seq in range(1, projection.MAX_BATCH + 2):
        put(
            core,
            event(seq=seq),
            str(uuid4()),
            1,
            received_at_ms=1 if seq <= projection.MAX_BATCH else NOW,
        )
    first = run_cycle(core, now_ms=NOW)
    assert first["projected_ingest_cursor"] == projection.MAX_BATCH
    assert first["retirement"]["status"] == "catching_up"
    second = run_cycle(core, now_ms=NOW)
    assert second["retirement"]["status"] == "idle"
    assert graph(core, value)["nodes"]


def test_reader_already_in_snapshot_can_finish_across_retirement(core):
    value = seed(core)
    expected = graph(core, value)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    writes, errors = [], []

    def expire(sql):
        if 'SELECT expired_cursor FROM "trace_projection_runs"' not in sql or writes:
            return
        writes.append(True)
        try:
            with closing(sqlite3.connect(path)) as writer:
                assert retention.expire_one(writer, now_ms=NOW)["status"] == "retired"
        except Exception as error:
            errors.append(error)

    core.set_trace_callback(expire)
    try:
        actual = graph(core, value)
    finally:
        core.set_trace_callback(None)
    assert writes and not errors
    assert actual == expected
    with pytest.raises(ValueError, match="trace_expired"):
        graph(core, value)
