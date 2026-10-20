import json
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes, validate_read_response
from edgecitadel_agentd.trace_cursor import CursorScope, decode_cursor
from test_trace_event_pages import KEY, SCOPE, assert_error, seed
from test_trace_graph_pages import collect, populate, project_all, read as graph
from test_trace_graph_projection import TRACE
from test_trace_graph_retention import NOW, clean
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_change_pages as pages
from aggregator import trace_event_pages as event_pages
from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retirement
from aggregator import trace_projection_store as projection

core = core_fixture


def read(db, after, trace_id=TRACE, **kwargs):
    return pages.read_changes(
        db, trace_id=trace_id, after=after, signing_key=KEY, scope_hash=SCOPE, **kwargs
    )


def position(token, generation):
    return decode_cursor(
        token, KEY, CursorScope("changes", TRACE, SCOPE, generation), retained_from=0
    )["position"]


def apply(db, nodes, edges, change):
    if change["mode"] == "clear":
        nodes.clear()
        edges.clear()
    elif change["mode"] == "snapshot":
        replacement = graph(db, at=change["at"])
        new_nodes, new_edges, _ = collect(db, replacement)
        nodes.clear()
        nodes.update(new_nodes)
        edges.clear()
        edges.update(new_edges)
    else:
        for key in change["remove_node_ids"]:
            nodes.pop(key, None)
        for key in change["remove_edge_ids"]:
            edges.pop(key, None)
        nodes.update((n["id"], n) for n in change["upsert_nodes"])
        edges.update((e["id"], e) for e in change["upsert_edges"])


def client_state(snapshot):
    return (
        {n["id"]: n for n in snapshot["nodes"]},
        {e["id"]: e for e in snapshot["edges"]},
    )


def test_snapshot_resume_replays_late_parent_and_conflict_as_atomic_patches(core):
    child = event(
        seq=1, trace_id=TRACE, task_id=str(uuid4()), parent_task_id=str(uuid4())
    )
    put(core, child, str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    assert any(n["kind"] == "unresolved" for n in initial["nodes"])
    put(
        core,
        event(seq=2, trace_id=TRACE, task_id=child["parent_task_id"]),
        str(uuid4()),
        1,
    )
    put(
        core,
        event(
            "completed",
            seq=3,
            trace_id=TRACE,
            task_id=child["task_id"],
            parent_task_id=child["parent_task_id"],
        ),
        str(uuid4()),
        1,
    )
    put(
        core,
        event(
            "failed",
            seq=4,
            trace_id=TRACE,
            task_id=child["task_id"],
            parent_task_id=child["parent_task_id"],
        ),
        str(uuid4()),
        1,
    )
    project_all(core)
    first = read(core, initial["resume_cursor"], limit=1)
    assert len(first["changes"]) == 1 and first["next_cursor"]
    nodes, edges = client_state(initial)
    cursors = []
    page = first
    while True:
        validate_read_response(page)
        for change in page["changes"]:
            assert change["mode"] == "patch"
            cursors.append(position(change["cursor"], initial["projection_generation"]))
            apply(core, nodes, edges, change)
            apply(core, nodes, edges, change)  # Applying a retry is idempotent.
            at = graph(core, at=change["at"])
            assert nodes == {n["id"]: n for n in at["nodes"]}
            assert edges == {e["id"]: e for e in at["edges"]}
            inspected = event_pages.read_events(
                core,
                trace_id=TRACE,
                as_of=change["at"],
                signing_key=KEY,
                scope_hash=SCOPE,
            )
            assert len(inspected["events"]) == change["ingest_high_watermark"]
        if not page["next_cursor"]:
            break
        page = read(core, page["next_cursor"], limit=1)
    assert cursors == [2, 3, 4]
    assert read(core, page["through_cursor"])["changes"] == []


def test_receipt_for_another_trace_can_change_selected_source_coverage(core):
    generation = str(uuid4())
    selected = event(seq=1, trace_id=TRACE)
    put(core, selected, generation, 2)
    project_all(core)
    initial = graph(core)
    assert initial["coverage"]["catching_up"]
    put(core, event(seq=2, trace_id="b" * 32), generation, 1)
    project_all(core)
    response = read(core, initial["resume_cursor"])
    assert len(response["changes"]) == 1
    change = response["changes"][0]
    assert change["upsert_nodes"] == [] and change["upsert_edges"] == []
    assert not change["coverage"]["catching_up"]
    assert change["coverage"] == graph(core)["coverage"]


def test_identical_visible_observation_still_advances_inspector_snapshot(core):
    value = event(seq=1, trace_id=TRACE)
    put(core, value, str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    put(core, {**value, "source_seq": 2, "event_id": str(uuid4())}, str(uuid4()), 1)
    project_all(core)
    response = read(core, initial["resume_cursor"])
    assert (
        len(response["changes"]) == 1 and response["changes"][0]["at"] != initial["at"]
    )
    assert response["changes"][0]["upsert_nodes"] == []


def test_sparse_global_scan_advances_without_skipping_selected_changes(
    core, monkeypatch
):
    monkeypatch.setattr(pages, "SCAN_LIMIT", 2)
    put(core, event(seq=1, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    for seq in range(2, 7):
        put(core, event(seq=seq, trace_id="b" * 32), str(uuid4()), 1)
    put(core, event("completed", seq=7, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)
    response = read(core, initial["resume_cursor"])
    assert not response["changes"] and response["next_cursor"]
    seen = []
    while True:
        seen.extend(response["changes"])
        if not response["next_cursor"]:
            break
        response = read(core, response["next_cursor"])
    assert (
        len(seen) == 1
        and position(seen[0]["cursor"], initial["projection_generation"]) == 7
    )
    assert position(response["through_cursor"], initial["projection_generation"]) == 7


@pytest.mark.parametrize("ring", [False, True])
def test_large_existing_node_update_is_atomic_patch_and_expiry_is_clear(core, ring):
    tasks, _ = populate(core, 501, ring=ring)
    initial = graph(core)
    nodes, edges, _ = collect(core, initial)
    put(
        core,
        event("completed", seq=502, trace_id=TRACE, task_id=tasks[0]),
        str(uuid4()),
        1,
    )
    project_all(core)
    response = read(core, initial["resume_cursor"])
    change = response["changes"][0]
    assert change["mode"] == "patch"
    assert [node["id"] for node in change["upsert_nodes"]] == ["task:" + tasks[0]]
    assert change["upsert_edges"] == [] and change["remove_edge_ids"] == []
    apply(core, nodes, edges, change)
    assert len(nodes) == 501 and nodes["task:" + tasks[0]]["state"] == "completed"
    expected_nodes, expected_edges, _ = collect(core, graph(core, at=change["at"]))
    assert nodes == expected_nodes and edges == expected_edges
    assert retirement.expire_one(core, now_ms=NOW)["status"] == "retired"
    expired = read(core, response["through_cursor"])
    assert expired["changes"][0]["mode"] == "clear"
    assert expired["changes"][0]["trace_state"] == "expired"
    assert expired["changes"][0]["at"] is None
    apply(core, nodes, edges, expired["changes"][0])
    assert nodes == {} and edges == {}


def test_cleanup_and_fresh_incarnation_replay_without_old_nodes(core):
    values, _, _ = seed(core, count=1)
    trace = values[0]["trace_id"]
    initial = graph(core, trace)
    retirement.expire_one(core, now_ms=NOW)
    clean(core)
    put(
        core,
        event(seq=2, trace_id=trace, task_id=str(uuid4())),
        str(uuid4()),
        1,
        received_at_ms=NOW,
    )
    project_all(core)
    nodes, edges = client_state(initial)
    response = read(core, initial["resume_cursor"], trace)
    for change in response["changes"]:
        apply(core, nodes, edges, change)
    assert nodes == {n["id"]: n for n in graph(core, trace)["nodes"]}
    assert "task:" + values[0]["task_id"] not in nodes


def test_cursor_scope_generation_and_history_floor(core):
    put(core, event(seq=1, trace_id=TRACE), str(uuid4()), 1)
    state = project_all(core)
    initial = graph(core)
    assert_error("cursor_scope_mismatch", lambda: read(core, initial["at"]))
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, initial["resume_cursor"], "b" * 32)
    )
    assert_error("invalid_cursor", lambda: read(core, "PRIVATE_SENTINEL"))
    put(core, event(seq=2, trace_id=TRACE), str(uuid4()), 1)
    fresh = project_all(core)
    history.compact_batch(
        core, generation=state.generation, through_cursor=fresh.change_cursor
    )
    assert_error("history_expired", lambda: read(core, initial["resume_cursor"]))
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert_error("generation_changed", lambda: read(core, initial["resume_cursor"]))


def test_readonly_replay_and_byte_bound_do_not_acknowledge_unserved_changes(
    core, monkeypatch
):
    put(core, event(seq=1, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    for seq in range(2, 8):
        put(core, event(seq=seq, trace_id=TRACE, task_id=str(uuid4())), str(uuid4()), 1)
    project_all(core)
    two = read(core, initial["resume_cursor"], limit=2)
    budget = len(canonical_bytes(two)) + 100
    monkeypatch.setattr(pages, "MAX_RESPONSE_BYTES", budget)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        after = initial["resume_cursor"]
        seen = []
        while True:
            response = read(reader, after)
            assert len(canonical_bytes(response)) <= budget
            assert response["changes"]
            seen.extend(
                position(c["cursor"], initial["projection_generation"])
                for c in response["changes"]
            )
            assert (
                position(response["through_cursor"], initial["projection_generation"])
                == seen[-1]
            )
            if not response["next_cursor"]:
                break
            after = response["next_cursor"]
        assert seen == list(range(2, 8))
        assert reader.total_changes == 0 and not reader.in_transaction


@pytest.mark.parametrize("limit", [0, 501, True])
def test_invalid_limit(core, limit):
    assert_error("invalid_request", lambda: read(core, "PRIVATE_SENTINEL", limit=limit))


def test_commit_during_replay_is_delivered_on_next_request_without_snapshot_race(
    core, monkeypatch
):
    from aggregator import trace_store

    put(core, event(seq=1, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    put(core, event(seq=2, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    original = pages._snapshot
    wrote = False

    def commit_during_snapshot(*args):
        nonlocal wrote
        if not wrote:
            with closing(sqlite3.connect(path)) as writer:
                trace_store.initialize(writer)
                put(writer, event(seq=3, trace_id=TRACE), str(uuid4()), 1)
                project_all(writer)
            wrote = True
        return original(*args)

    monkeypatch.setattr(pages, "_snapshot", commit_during_snapshot)
    first = read(core, initial["resume_cursor"])
    second = read(core, first["through_cursor"])
    assert wrote
    assert [
        position(c["cursor"], initial["projection_generation"])
        for c in first["changes"]
    ] == [2]
    assert [
        position(c["cursor"], initial["projection_generation"])
        for c in second["changes"]
    ] == [3]
    assert first["next_cursor"] is None
    assert first["changes"][0]["ingest_high_watermark"] == 2


@pytest.mark.parametrize("parent", ["none", "known", "missing"])
def test_large_new_node_uses_patch_only_for_proven_leaf(core, parent):
    tasks, _ = populate(core, 501, ring=True)
    initial = graph(core)
    nodes, edges, _ = collect(core, initial)
    task_id = str(uuid4())
    parent_id = {
        "none": None,
        "known": tasks[0],
        "missing": str(uuid4()),
    }[parent]
    put(
        core,
        event(seq=502, trace_id=TRACE, task_id=task_id, parent_task_id=parent_id),
        str(uuid4()),
        1,
    )
    project_all(core)
    response = read(core, initial["resume_cursor"])
    assert len(response["changes"]) == 1
    change = response["changes"][0]
    assert change["mode"] == ("patch" if parent in {"none", "known"} else "snapshot")
    apply(core, nodes, edges, change)
    expected_nodes, expected_edges, _ = collect(core, graph(core, at=change["at"]))
    assert nodes == expected_nodes and edges == expected_edges
    assert "task:" + task_id in nodes


@pytest.mark.parametrize("cycle", [False, True])
def test_large_late_parent_requires_global_resolution(core, cycle):
    tasks, _ = populate(core, 501)
    late_parent = str(uuid4())
    put(
        core,
        event(seq=502, trace_id=TRACE, task_id=tasks[0], parent_task_id=late_parent),
        str(uuid4()),
        1,
    )
    project_all(core)
    initial = graph(core)
    nodes, edges, _ = collect(core, initial)
    assert nodes["task:" + late_parent]["kind"] == "unresolved"
    put(
        core,
        event(
            seq=503,
            trace_id=TRACE,
            task_id=late_parent,
            parent_task_id=tasks[0] if cycle else None,
        ),
        str(uuid4()),
        1,
    )
    project_all(core)
    change = read(core, initial["resume_cursor"])["changes"][0]
    assert change["mode"] == "snapshot"
    apply(core, nodes, edges, change)
    expected_nodes, expected_edges, _ = collect(core, graph(core, at=change["at"]))
    assert nodes == expected_nodes and edges == expected_edges
    assert {edge["status"] for edge in edges.values()} == (
        {"invalid"} if cycle else {"resolved"}
    )


def test_large_tool_updates_preserve_each_historical_commit(core):
    populate(core, 501)
    fixtures = json.loads(
        (
            Path(__file__).parents[2]
            / "agent-runtime/tests/fixtures/traces/events.v1.json"
        ).read_text()
    )
    tool = next(
        item["event"] for item in fixtures["fixtures"] if item["name"] == "tool"
    )
    tool = {**tool, "trace_id": TRACE, "source_seq": 502, "event_id": str(uuid4())}
    put(core, tool, str(uuid4()), 1)
    project_all(core)
    initial = graph(core)
    nodes, edges, _ = collect(core, initial)
    for seq, phase in [(503, "finished"), (504, "failed")]:
        put(
            core,
            {**tool, "source_seq": seq, "event_id": str(uuid4()), "phase": phase},
            str(uuid4()),
            1,
        )
    project_all(core)
    changes = read(core, initial["resume_cursor"])["changes"]
    assert len(changes) == 2
    for change in changes:
        assert change["mode"] == "patch" and len(change["upsert_nodes"]) == 1
        apply(core, nodes, edges, change)
        expected_nodes, expected_edges, _ = collect(core, graph(core, at=change["at"]))
        assert nodes == expected_nodes and edges == expected_edges
    assert changes[0]["upsert_nodes"][0]["state"] == "finished"
    assert changes[1]["upsert_nodes"][0]["conflict"]


def test_large_relationship_change_requires_full_resolution(core):
    tasks, _ = populate(core, 501)
    initial = graph(core)
    put(
        core,
        event(
            "completed",
            seq=502,
            trace_id=TRACE,
            task_id=tasks[0],
            parent_task_id=tasks[1],
        ),
        str(uuid4()),
        1,
    )
    project_all(core)
    change = read(core, initial["resume_cursor"])["changes"][0]
    assert change["mode"] == "snapshot"
    nodes, edges, _ = collect(core, initial)
    apply(core, nodes, edges, change)
    expected_nodes, expected_edges, _ = collect(core, graph(core, at=change["at"]))
    assert nodes == expected_nodes and edges == expected_edges
    assert any(
        edge["from"] == "task:" + tasks[1] and edge["to"] == "task:" + tasks[0]
        for edge in edges.values()
    )
