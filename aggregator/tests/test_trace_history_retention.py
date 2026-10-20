import sqlite3
from contextlib import closing
from copy import deepcopy
from uuid import uuid4

import pytest
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_store as projection
from aggregator import trace_retention

core = core_fixture


def test_strict_receipt_cutoff_preserves_fresh_cursor_despite_reversed_clocks(core):
    generation = str(uuid4())
    oldest = event(seq=1, occurred_at="2099-01-01T00:00:00.000Z")
    exact = event("completed", seq=2, occurred_at="2001-01-01T00:00:00.000Z")
    later_old = event("failed", seq=3)
    for value, timestamp, position in [
        (oldest, 99, 1),
        (exact, 100, 2),
        (later_old, 10, 3),
    ]:
        assert (
            put(core, value, generation, position, received_at_ms=timestamp).outcome
            == "accepted"
        )
    snapshots = []
    for _ in range(3):
        projection.project_batch(core, limit=1)
        snapshots.append(projection.read_graph(core, trace_id=oldest["trace_id"]))
    now = history.RETENTION_MS + 100
    first = history.expire_history_batch(core, now_ms=now, limit=1)
    assert first["floor_cursor"] == 1 and first["deleted_rows"] <= 1
    assert first["eligible_prefix_complete"]
    state = snapshots[-1]["state"]
    for index in (1, 2):
        assert (
            projection.read_graph(
                core,
                trace_id=oldest["trace_id"],
                generation=state.generation,
                at_cursor=index + 1,
            )
            == snapshots[index]
        )
    # Cursor 2 protects the whole following range, including older timestamp 3.
    assert history.expire_history_batch(core, now_ms=now, limit=1)["floor_cursor"] == 1
    assert (
        history.expire_history_batch(core, now_ms=now + 1, limit=1)["floor_cursor"] == 3
    )
    assert history.expire_history_batch(core, now_ms=0, limit=1)["floor_cursor"] == 3
    with pytest.raises(ValueError, match="cursor_expired"):
        projection.read_graph(
            core, trace_id=oldest["trace_id"], generation=state.generation, at_cursor=2
        )


def test_receipt_only_duplicates_conflicts_and_rejections_keep_their_core_time(core):
    value, generation = event(), str(uuid4())
    assert put(core, value, generation, 1, received_at_ms=10).outcome == "accepted"
    assert put(core, value, generation, 2, received_at_ms=20).outcome == "duplicate"
    changed = deepcopy(value)
    changed["phase"] = "failed"
    assert put(core, changed, generation, 1, received_at_ms=30).outcome == "conflict"
    invalid = event(seq=2, schema_version=2)
    assert put(core, invalid, generation, 3, received_at_ms=40).outcome == "rejected"
    projection.project_batch(core)
    assert core.execute(
        "SELECT received_at_ms FROM trace_projection_history_cursors WHERE cursor>0 ORDER BY cursor"
    ).fetchall() == [(10,), (20,), (30,), (40,)]
    result = history.expire_history_batch(core, now_ms=history.RETENTION_MS + 25)
    assert result["floor_cursor"] == 2
    assert history.retained_range(core)["from_ingest_seq"] == 2


def test_receipt_time_survives_payload_expiry_rebuild_and_restart(core):
    generation = str(uuid4())
    for seq in (1, 2):
        put(core, event(seq=seq), generation, seq, received_at_ms=seq * 10)
    projection.project_batch(core)
    assert (
        trace_retention.expire_payloads(core, now_ms=history.RETENTION_MS + 50)[
            "expired_payloads"
        ]
        == 2
    )
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as resumed:
        result = history.expire_history_batch(resumed, now_ms=history.RETENTION_MS + 15)
        assert result["floor_cursor"] == 1
        assert (
            history.retained_range(resumed)["state"].generation == candidate.generation
        )
    assert history.retained_range(core)["from_cursor"] == 1


def test_age_scan_and_pruning_both_have_per_pass_limits(core):
    generation = str(uuid4())
    count = history.SCAN_ROWS + 10
    for seq in range(1, count + 1):
        put(
            core,
            event("authentication_rejected", kind="security", seq=seq),
            generation,
            seq,
            received_at_ms=1,
        )
    while projection.project_batch(core).ingest_cursor < count:
        pass
    first = history.expire_history_batch(core, now_ms=history.RETENTION_MS + 2, limit=1)
    assert first["scanned_rows"] == history.SCAN_ROWS
    assert first["floor_cursor"] == history.SCAN_ROWS
    assert first["deleted_rows"] <= 1 and not first["eligible_prefix_complete"]
    second = history.expire_history_batch(
        core, now_ms=history.RETENTION_MS + 2, limit=1
    )
    assert second["scanned_rows"] == 10 and second["floor_cursor"] == count
    assert second["deleted_rows"] <= 1 and second["eligible_prefix_complete"]
    assert core.execute("SELECT count(*) FROM trace_raw_events").fetchone()[0] == count


def test_age_decision_and_pruning_failure_roll_back_together(core):
    generation = str(uuid4())
    for seq in (1, 2):
        put(core, event(seq=seq), generation, seq, received_at_ms=1)
    projection.project_batch(core)
    core.execute(
        "CREATE TRIGGER age_fault BEFORE DELETE ON trace_projection_history_rows "
        "BEGIN SELECT RAISE(ABORT,'age cleanup failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="age cleanup failure"):
        history.expire_history_batch(core, now_ms=history.RETENTION_MS + 2)
    assert list(core.iterdump()) == before
    assert history.retained_range(core)["from_cursor"] == 0


def test_idle_fresh_history_pass_does_not_rewrite_projection(core):
    put(core, event(), str(uuid4()), 1, received_at_ms=100)
    projection.project_batch(core)
    before = list(core.iterdump())
    changed = core.total_changes
    result = history.expire_history_batch(core, now_ms=history.RETENTION_MS + 100)
    assert result["floor_cursor"] == result["deleted_rows"] == 0
    assert result["complete"] and result["eligible_prefix_complete"]
    assert core.total_changes == changed
    assert list(core.iterdump()) == before


@pytest.mark.parametrize("now_ms", [-1, True, 2**53, "100"])
def test_invalid_age_clock_does_not_mutate_history(core, now_ms):
    before = list(core.iterdump())
    with pytest.raises(ValueError, match="invalid_retention_time"):
        history.expire_history_batch(core, now_ms=now_ms)
    assert list(core.iterdump()) == before


def test_version_four_requires_rebuild_instead_of_guessing_receipt_time(core):
    core.execute("UPDATE trace_projection_state SET version=4")
    core.commit()
    with pytest.raises(ValueError, match="version_unavailable"):
        history.expire_history_batch(core, now_ms=history.RETENTION_MS + 1)
