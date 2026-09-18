import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import validate_read_response
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor
from test_trace_event_pages import KEY, SCOPE, assert_error, seed
from test_trace_graph_projection import READ, TRACE
from test_trace_graph_retention import NOW
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_event_pages as events
from aggregator import trace_graph_pages as pages
from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retirement
from aggregator import trace_projection_store as projection

core = core_fixture


def read(db, trace_id=TRACE, **kwargs):
    return pages.read_graph(
        db, trace_id=trace_id, signing_key=KEY, scope_hash=SCOPE, **kwargs
    )


def project_all(db):
    while True:
        state = projection.project_batch(db)
        if (
            state.ingest_cursor
            == db.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0]
        ):
            return state


def populate(db, count, *, ring=False):
    tasks = [str(uuid4()) for _ in range(count)]
    generation = str(uuid4())
    for index, task in enumerate(tasks):
        value = event(
            seq=index + 1,
            trace_id=TRACE,
            task_id=task,
            parent_task_id=tasks[(index + 1) % count] if ring else None,
        )
        assert put(db, value, generation, index + 1).outcome == "accepted"
    return tasks, project_all(db)


def collect(db, first):
    nodes, edges = {}, {}
    page = first
    count = 0
    while True:
        validate_read_response(page)
        assert len(page["nodes"]) <= 500 and len(page["edges"]) <= 1000
        assert (
            page["at"] == first["at"]
            and page["resume_cursor"] == first["resume_cursor"]
        )
        nodes.update((n["id"], n) for n in page["nodes"])
        for edge in page["edges"]:
            assert edge["id"] not in edges
            edges[edge["id"]] = edge
        count += 1
        assert count < 20
        if not page["expansions"]:
            return nodes, edges, count
        assert len(page["expansions"]) == 1
        page = read(db, expand=page["expansions"][0]["cursor"])
        assert page["page_kind"] == "expansion"


def test_small_graph_matches_reducer_and_supplies_distinct_event_and_live_cursors(core):
    for value in READ["input_events"]:
        put(core, value, str(uuid4()), 1)
    state = project_all(core)
    result = read(core)
    internal = projection.read_graph(core, trace_id=TRACE)
    validate_read_response(result)
    assert result["nodes"] == internal["nodes"]
    assert result["edges"] == internal["edges"]
    assert result["coverage"] == internal["coverage"]
    assert result["total_nodes"] == len(internal["nodes"])
    assert result["expansions"] == [] and result["page_kind"] == "snapshot"
    for kind, field, upper in (
        ("graph", "at", state.ingest_cursor),
        ("changes", "resume_cursor", state.change_cursor),
    ):
        claims = decode_cursor(
            result[field],
            KEY,
            CursorScope(kind, TRACE, SCOPE, state.generation),
            retained_from=0,
        )
        assert claims["snapshot"] == state.change_cursor and claims["upper"] == upper
    observed = events.read_events(
        core, trace_id=TRACE, signing_key=KEY, scope_hash=SCOPE, as_of=result["at"]
    )
    assert observed["events"] == READ["input_events"]
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, at=result["resume_cursor"])
    )


def test_historical_graph_does_not_leak_new_nodes_but_reports_current_raw_lag(core):
    values, _, _ = seed(core, count=1)
    trace = values[0]["trace_id"]
    first = read(core, trace)
    put(core, event("completed", seq=2), str(uuid4()), 1)
    pending = read(core, trace)
    assert pending["freshness"]["ingest_cursor"] == 2
    assert pending["freshness"]["projection_cursor"] == 1
    assert pending["ingest_high_watermark"] == 1
    project_all(core)
    historical = read(core, trace, at=first["at"])
    assert historical["nodes"] == first["nodes"]
    assert historical["at"] == first["at"]
    assert historical["freshness"]["projection_cursor"] == 2
    assert read(core, trace)["nodes"][0]["state"] == "completed"


def test_large_disconnected_graph_reaches_terminal_additive_page(core):
    tasks, _ = populate(core, 503)
    first = read(core)
    assert len(first["nodes"]) == 500 and first["total_nodes"] == 503
    nodes, edges, count = collect(core, first)
    assert set(nodes) == {"task:" + task for task in tasks}
    assert not edges and count == 2


def test_cycle_status_is_global_even_when_edges_cross_page_boundaries(core):
    tasks, _ = populate(core, 503, ring=True)
    first = read(core)
    nodes, edges, count = collect(core, first)
    assert set(nodes) == {"task:" + task for task in tasks}
    assert len(edges) == 503 and count >= 4
    assert {e["status"] for e in edges.values()} == {"invalid"}
    assert {e["kind"] for e in edges.values()} == {"parent_task"}


def test_expansion_is_frozen_across_new_observations_and_graph_cleanup(core):
    tasks, _ = populate(core, 501)
    first = read(core)
    late = event("completed", seq=502, task_id=tasks[0], trace_id=TRACE)
    put(core, late, str(uuid4()), 1)
    project_all(core)
    assert retirement.expire_one(core, now_ms=NOW)["status"] == "retired"
    # Use production batch size for this deliberately larger graph.
    for _ in range(50):
        if retirement.cleanup_batch(core, now_ms=NOW)["complete"]:
            break
    else:
        pytest.fail("cleanup did not complete")
    nodes, _, _ = collect(core, first)
    assert len(nodes) == 501 and nodes["task:" + tasks[0]]["state"] == "running"
    assert_error("not_found", lambda: read(core))


def test_expired_and_foreign_expansion_tokens_do_not_restart_at_current_state(core):
    _, state = populate(core, 501)
    first = read(core)
    expansion = first["expansions"][0]["cursor"]
    assert_error("cursor_scope_mismatch", lambda: read(core, at=expansion))
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, "b" * 32, expand=expansion)
    )
    assert_error(
        "invalid_request", lambda: read(core, at=first["at"], expand=expansion)
    )
    put(core, event(seq=502, trace_id=TRACE), str(uuid4()), 1)
    fresh = project_all(core)
    history.compact_batch(
        core, generation=state.generation, through_cursor=fresh.change_cursor
    )
    assert_error("history_expired", lambda: read(core, expand=expansion))


def test_readonly_graph_and_rebuild_generation_fence(core):
    values, _, _ = seed(core)
    trace = values[0]["trace_id"]
    first = read(core, trace)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        assert read(reader, trace, at=first["at"]) == first
        assert reader.total_changes == 0 and not reader.in_transaction
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert_error("generation_changed", lambda: read(core, trace, at=first["at"]))


def test_empty_observational_trace_is_distinct_from_unknown_trace(core):
    value = event(
        "unknown",
        kind="coverage",
        trace_id=TRACE,
        attributes={
            "export_generation": str(uuid4()),
            "through_export_seq": 0,
        },
    )
    put(core, value, str(uuid4()), 1)
    project_all(core)
    result = read(core)
    validate_read_response(result)
    assert result["nodes"] == [] and result["total_nodes"] == 0
    assert_error("not_found", lambda: read(core, "b" * 32))


def test_ambiguous_parent_breaks_cycle_without_invalidating_every_descendant(core):
    tasks, _ = populate(core, 503, ring=True)
    missing = str(uuid4())
    put(
        core,
        event(seq=504, trace_id=TRACE, task_id=tasks[0], parent_task_id=missing),
        str(uuid4()),
        1,
    )
    put(
        core,
        event(
            "observed",
            kind="link",
            seq=505,
            trace_id=TRACE,
            attributes={
                "relation": "join",
                "from_task_id": tasks[1],
                "to_task_id": tasks[2],
            },
        ),
        str(uuid4()),
        1,
    )
    project_all(core)
    nodes, edges, _ = collect(core, read(core))
    assert nodes["task:" + missing]["kind"] == "unresolved"
    invalid = [edge for edge in edges.values() if edge["status"] == "invalid"]
    assert len(invalid) == 2
    assert {edge["to"] for edge in invalid} == {"task:" + tasks[0]}
    assert all(
        edge["status"] == "resolved" for edge in edges.values() if edge not in invalid
    )
    assert sum(edge["kind"] == "join" for edge in edges.values()) == 1


def test_dense_graph_paginates_edges_even_when_all_nodes_fit(core):
    tasks, _ = populate(core, 33)
    generation = str(uuid4())
    pairs = [(a, b) for a in tasks for b in tasks if a != b][:1001]
    for i, (parent, child) in enumerate(pairs, 34):
        put(
            core,
            event(
                "observed",
                kind="link",
                seq=i,
                trace_id=TRACE,
                attributes={
                    "relation": "join",
                    "from_task_id": parent,
                    "to_task_id": child,
                },
            ),
            generation,
            i - 33,
        )
    project_all(core)
    first = read(core)
    assert len(first["nodes"]) == 33 and first["edges"] == []
    nodes, edges, count = collect(core, first)
    assert len(nodes) == 33 and len(edges) == 1001 and count == 3
    assert {edge["status"] for edge in edges.values()} == {"resolved"}
