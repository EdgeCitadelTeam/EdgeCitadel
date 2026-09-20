import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import validate_read_response
from test_trace_event_pages import KEY, SCOPE, assert_error
from test_trace_graph_pages import project_all, read as graph
from test_trace_graph_projection import TRACE
from test_trace_graph_retention import NOW, clean
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_history_pages as pages
from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retirement
from aggregator import trace_projection_store as projection

core = core_fixture


def read(db, **kwargs):
    return pages.read_history(
        db,
        trace_id=kwargs.pop("trace_id", TRACE),
        signing_key=KEY,
        scope_hash=kwargs.pop("scope_hash", SCOPE),
        **kwargs,
    )


def add(db, seq, phase="running", **kwargs):
    value = event(phase, seq=seq, trace_id=TRACE, **kwargs)
    put(db, value, str(uuid4()), 1)
    project_all(db)
    return value


def all_pages(db, page):
    items = list(page["items"])
    while page["next_cursor"]:
        later = read(db, cursor=page["next_cursor"], limit=1)
        assert later["snapshot_cursor"] == page["snapshot_cursor"]
        assert later["retained_from"] == page["retained_from"]
        items.extend(later["items"])
        page = later
    return items


def test_discovery_freezes_membership_and_opens_exact_graph_versions(core):
    for seq in range(1, 5):
        add(core, seq, "completed" if seq == 4 else "running")
    first = read(core, limit=1)
    validate_read_response(first)
    add(core, 5, "failed")
    items = all_pages(core, first)
    assert [item["position"] for item in items] == [4, 3, 2, 1]
    assert read(core, cursor=first["snapshot_cursor"], limit=1) == first
    assert read(core)["upper_position"] == 5
    for item in items:
        snapshot = graph(core, at=item["at"])
        assert snapshot["ingest_high_watermark"] == item["position"]
        assert snapshot["nodes"][0]["state"] == (
            "completed" if item["position"] == 4 else "running"
        )


def test_sparse_unrelated_clocks_are_bounded_and_continue_without_skipping(
    core, monkeypatch
):
    add(core, 1)
    for seq in range(2, 9):
        # Different source/epoch keeps coverage of the selected run unchanged.
        put(
            core,
            event(seq=seq, trace_id="b" * 32, source_epoch=str(uuid4())),
            str(uuid4()),
            1,
        )
    project_all(core)
    monkeypatch.setattr(pages, "SCAN_LIMIT", 2)
    first = read(core)
    assert [item["position"] for item in first["items"]] == [8]
    second = read(core, cursor=first["next_cursor"])
    assert second["items"] == [] and second["next_cursor"]
    assert [item["position"] for item in all_pages(core, first)] == [8, 1]


def test_history_stops_at_run_creation_after_older_fleet_traffic(core, monkeypatch):
    for seq in range(1, 9):
        put(core, event(seq=seq, trace_id="b" * 32), str(uuid4()), 1)
    project_all(core)
    add(core, 9)
    add(core, 10, "completed")
    monkeypatch.setattr(pages, "SCAN_LIMIT", 2)
    page = read(core)
    assert [item["position"] for item in page["items"]] == [10, 9]
    assert page["next_cursor"] is None
    assert page["retained_from"] == 0
    assert not any(item["is_retained_base"] for item in page["items"])
    assert graph(core, at=page["items"][-1]["at"])["nodes"][0]["state"] == "running"


def test_compaction_returns_retained_base_and_expires_inflight_range(core):
    for seq in range(1, 5):
        add(core, seq)
    old = read(core, limit=1)
    state = project_all(core)
    history.compact_batch(core, generation=state.generation, through_cursor=2)
    assert_error("history_expired", lambda: read(core, cursor=old["next_cursor"]))
    items = all_pages(core, read(core, limit=1))
    assert [item["position"] for item in items] == [4, 3, 2]
    assert items[-1]["is_retained_base"]
    assert graph(core, at=items[-1]["at"])["ingest_high_watermark"] == 2


def test_cursor_is_bound_to_trace_access_generation_and_kind(core):
    add(core, 1)
    page = read(core)
    cursor = page["snapshot_cursor"]
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, cursor=cursor, trace_id="b" * 32)
    )
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, cursor=cursor, scope_hash="a" * 64)
    )
    assert_error("cursor_scope_mismatch", lambda: graph(core, at=cursor))
    assert_error("invalid_cursor", lambda: read(core, cursor=cursor + "x"))
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert_error("generation_changed", lambda: read(core, cursor=cursor))


def test_retirement_keeps_older_present_snapshots_discoverable(core):
    add(core, 1, "completed")
    retired = retirement.expire_one(core, now_ms=NOW)
    assert retired["status"] == "retired"
    clean(core)
    items = all_pages(core, read(core, limit=1))
    assert any(
        item["trace_state"] != "present" and item["at"] is None for item in items
    )
    retained = [item for item in items if item["at"]]
    assert (
        retained
        and graph(core, at=retained[-1]["at"])["nodes"][0]["state"] == "completed"
    )


@pytest.mark.parametrize("limit", [0, 101, True, "1"])
def test_invalid_limit_is_rejected(core, limit):
    assert_error("invalid_request", lambda: read(core, limit=limit))


def test_unknown_run_and_missing_clock_are_explicit(core):
    assert_error("not_found", lambda: read(core))
    add(core, 1)
    add(core, 2)
    core.execute("DELETE FROM trace_projection_history_cursors WHERE cursor=1")
    core.commit()
    assert_error("history_expired", lambda: read(core))


def test_coverage_repair_attributed_to_another_run_is_discoverable(core):
    generation = str(uuid4())
    put(core, event(seq=1, trace_id=TRACE), generation, 2)
    project_all(core)
    put(core, event(seq=2, trace_id="b" * 32), generation, 1)
    project_all(core)
    # Put an unrelated update above the repair so the repair cannot pass merely
    # because the API includes the latest boundary unconditionally.
    put(
        core,
        event(seq=3, trace_id="c" * 32, source_epoch=str(uuid4())),
        str(uuid4()),
        1,
    )
    project_all(core)
    items = all_pages(core, read(core, limit=1))
    assert [item["position"] for item in items] == [3, 2, 1]
    assert graph(core, at=items[-1]["at"])["coverage"]["catching_up"]
    assert not graph(core, at=items[-2]["at"])["coverage"]["catching_up"]


def test_compaction_during_read_keeps_one_snapshot_and_fences_the_next_page(
    core, monkeypatch
):
    for seq in range(1, 5):
        add(core, seq)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    state = project_all(core)
    original = pages._summary
    wrote = False

    def compact_during_read(*args):
        nonlocal wrote
        if not wrote:
            with closing(sqlite3.connect(path)) as writer:
                history.compact_batch(
                    writer, generation=state.generation, through_cursor=3
                )
            wrote = True
        return original(*args)

    monkeypatch.setattr(pages, "_summary", compact_during_read)
    page = read(core, limit=2)
    assert wrote and page["retained_from"] == 0
    assert [item["position"] for item in page["items"]] == [4, 3]
    assert_error("history_expired", lambda: read(core, cursor=page["next_cursor"]))
