import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes, validate_read_response
from test_trace_event_pages import KEY, assert_error
from test_trace_graph_pages import project_all
from test_trace_graph_retention import NOW, clean
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_list_pages as pages
from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retirement
from aggregator import trace_projection_store as projection

core = core_fixture
POLICY = {"mode": "trusted_fleet", "policy_version": 1}


def read(db, **kwargs):
    return pages.read_list(db, signing_key=KEY, access_policy=POLICY, **kwargs)


def add(db, seq, *, trace=None, phase="running", role="recipient", **fields):
    value = event(
        phase,
        seq=seq,
        trace_id=trace or f"{seq:032x}",
        attributes={"source_role": role},
        **fields,
    )
    assert put(db, value, str(uuid4()), 1).outcome == "accepted"
    return value


def drain(db, first, **filters):
    result = first["items"][:]
    page = first
    while page["next_cursor"]:
        page = read(db, cursor=page["next_cursor"], **filters)
        validate_read_response(page)
        assert page["snapshot_cursor"] == first["snapshot_cursor"]
        result.extend(page["items"])
        assert len(result) < 2000
    return result


def test_creation_order_does_not_change_with_late_activity_and_snapshot_restarts(core):
    values = [add(core, i) for i in range(1, 5)]
    project_all(core)
    first = read(core, limit=1)
    assert first["items"][0]["trace_id"] == values[-1]["trace_id"]
    late = add(core, 5, trace=values[0]["trace_id"], phase="completed")
    add(core, 6)
    project_all(core)
    old = drain(core, first, limit=2)
    assert [i["trace_id"] for i in old] == [v["trace_id"] for v in reversed(values)]
    assert [i["created_projection_cursor"] for i in old] == [4, 3, 2, 1]
    assert old[-1]["outcome"] is None
    assert (
        read(core, cursor=first["snapshot_cursor"], limit=1)["items"] == first["items"]
    )
    fresh = read(core)["items"]
    assert (
        fresh[-1]["trace_id"] == late["trace_id"]
        and fresh[-1]["outcome"] == "completed"
    )


def test_initial_marker_cannot_collide_with_largest_trace_identity(core):
    add(core, 1, trace="f" * 32)
    project_all(core)
    first = read(core, limit=1)
    assert first["items"][0]["trace_id"] == "f" * 32
    assert first["next_cursor"] is None
    assert read(core, cursor=first["snapshot_cursor"]) == first


def test_root_owner_outcome_ignores_completed_children_and_sender_deadlines(core):
    root = add(core, 1, agent_id="owner")
    add(
        core,
        2,
        trace=root["trace_id"],
        phase="completed",
        task_id=str(uuid4()),
        parent_task_id=root["task_id"],
        agent_id="child",
    )
    add(
        core,
        3,
        trace=root["trace_id"],
        phase="expired",
        role="sender",
        agent_id="sender",
    )
    project_all(core)
    item = read(core)["items"][0]
    assert item["root_task_id"] == root["task_id"] and item["root_agent_id"] == "owner"
    assert item["outcome"] is None
    assert read(core, agent_id="child")["items"] == [item]
    assert read(core, outcome="completed")["items"] == []
    add(core, 4, trace=root["trace_id"], phase="completed", agent_id="owner")
    project_all(core)
    assert read(core, outcome="completed")["items"][0]["outcome"] == "completed"
    add(core, 5, trace=root["trace_id"], phase="failed", agent_id="owner")
    project_all(core)
    assert read(core)["items"][0]["outcome"] is None


def test_daemon_recovery_is_not_mislabeled_as_root_agent(core):
    root = add(core, 1, agent_id="owner")
    add(
        core,
        2,
        trace=root["trace_id"],
        phase="expired",
        role="daemon",
        agent_id="edgecitadel-system",
    )
    project_all(core)
    item = read(core)["items"][0]
    assert item["outcome"] == "expired" and item["root_agent_id"] == "owner"
    add(core, 3, trace=root["trace_id"], phase="completed", agent_id="owner")
    project_all(core)
    assert read(core)["items"][0]["outcome"] == "completed"


def test_multiple_or_contradictory_root_claims_remain_unknown(core):
    root = add(core, 1, phase="completed")
    add(core, 2, trace=root["trace_id"], task_id=str(uuid4()), phase="completed")
    project_all(core)
    item = read(core)["items"][0]
    assert item["root_task_id"] is None and item["outcome"] is None
    # A later explicit parent claim prevents an apparent orphan being promoted.
    another = add(core, 3, phase="completed")
    add(
        core,
        4,
        trace=another["trace_id"],
        parent_task_id=str(uuid4()),
        phase="completed",
    )
    project_all(core)
    assert read(core)["items"][0]["root_task_id"] is None


def test_native_root_and_unknown_interruption_do_not_invent_task_or_success(core):
    native = event(
        "started",
        kind="run",
        seq=1,
        task_id=None,
        parent_task_id=None,
        parent_run_id=None,
        agent_id="native-owner",
    )
    put(core, native, str(uuid4()), 1)
    project_all(core)
    item = read(core)["items"][0]
    assert (
        item["root_task_id"] is None
        and item["root_agent_id"] == "native-owner"
        and item["outcome"] is None
    )
    completed = {
        **native,
        "event_id": str(uuid4()),
        "source_seq": 2,
        "phase": "completed",
    }
    put(core, completed, str(uuid4()), 1)
    project_all(core)
    assert read(core)["items"][0]["outcome"] == "completed"
    put(
        core,
        {
            **completed,
            "event_id": str(uuid4()),
            "source_seq": 3,
            "phase": "interrupted",
        },
        str(uuid4()),
        1,
    )
    project_all(core)
    assert read(core)["items"][0]["outcome"] is None


def test_filter_membership_is_frozen_and_scope_policy_cannot_be_reused(core):
    first = add(core, 1, phase="completed", agent_id="one")
    add(core, 2, phase="completed", agent_id="one")
    project_all(core)
    page = read(core, limit=1, agent_id="one", outcome="completed")
    add(core, 3, trace=first["trace_id"], phase="failed", agent_id="two")
    project_all(core)
    old = read(core, cursor=page["next_cursor"], agent_id="one", outcome="completed")
    assert old["items"][0]["trace_id"] == first["trace_id"]
    assert old["items"][0]["outcome"] == "completed"
    for filters in (
        {},
        {"agent_id": "two", "outcome": "completed"},
        {"agent_id": "one", "outcome": "failed"},
    ):
        assert_error(
            "cursor_scope_mismatch",
            lambda: read(core, cursor=page["next_cursor"], **filters),
        )
    assert_error(
        "cursor_scope_mismatch",
        lambda: pages.read_list(
            core,
            signing_key=KEY,
            access_policy={**POLICY, "policy_version": 2},
            cursor=page["next_cursor"],
            agent_id="one",
            outcome="completed",
        ),
    )


def test_sparse_filter_scan_is_bounded_and_resumable(core, monkeypatch):
    monkeypatch.setattr(pages, "SCAN_LIMIT", 2)
    wanted = add(core, 1, agent_id="wanted")
    for i in range(2, 7):
        add(core, i, agent_id="other")
    project_all(core)
    page = read(core, agent_id="wanted")
    assert page["items"] == [] and page["next_cursor"]
    result = drain(core, page, agent_id="wanted")
    assert [item["trace_id"] for item in result] == [wanted["trace_id"]]


def test_history_compaction_cleanup_and_recreation_preserve_list_snapshot(core):
    first = add(core, 1, agent_id="one")
    second = add(core, 2, agent_id="two")
    state = project_all(core)
    page = read(core, limit=1)
    assert retirement.expire_one(core, now_ms=NOW)["status"] == "retired"
    clean(core)
    history.compact_batch(
        core, generation=state.generation, through_cursor=state.change_cursor
    )
    assert [i["trace_id"] for i in drain(core, page)] == [
        second["trace_id"],
        first["trace_id"],
    ]
    add(core, 3, trace=first["trace_id"], agent_id="fresh")
    fresh = project_all(core)
    rows = read(core)["items"]
    assert len(rows) == 2
    assert rows[0]["trace_id"] == first["trace_id"]
    assert rows[0]["created_projection_cursor"] == fresh.change_cursor
    assert read(core, agent_id="one")["items"] == []
    assert read(core, agent_id="fresh")["items"][0]["trace_id"] == first["trace_id"]
    assert [i["trace_id"] for i in drain(core, page)] == [
        second["trace_id"],
        first["trace_id"],
    ]
    history.compact_batch(
        core, generation=state.generation, through_cursor=fresh.change_cursor
    )
    assert_error("history_expired", lambda: read(core, cursor=page["next_cursor"]))


def test_read_only_and_generation_change(core):
    add(core, 1)
    project_all(core)
    page = read(core)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        assert read(reader, cursor=page["snapshot_cursor"]) == page
        assert reader.total_changes == 0 and not reader.in_transaction
    candidate = rebuild.begin(core)
    projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert_error(
        "generation_changed", lambda: read(core, cursor=page["snapshot_cursor"])
    )


def test_byte_budget_preserves_all_matching_runs(core, monkeypatch):
    for i in range(1, 7):
        add(core, i)
    project_all(core)
    two = read(core, limit=2)
    budget = len(canonical_bytes(two)) + 100
    monkeypatch.setattr(pages, "MAX_RESPONSE_BYTES", budget)
    first = read(core)
    assert len(canonical_bytes(first)) <= budget and first["next_cursor"]
    assert len(drain(core, first)) == 6


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"limit": 101},
        {"limit": True},
        {"outcome": "running"},
        {"agent_id": "PRIVATE_SENTINEL /"},
    ],
)
def test_invalid_list_queries(core, kwargs):
    assert_error("invalid_request", lambda: read(core, **kwargs))


def test_empty_list_has_reusable_snapshot_cursor(core):
    page = read(core)
    validate_read_response(page)
    assert page["items"] == [] and page["next_cursor"] is None
    assert read(core, cursor=page["snapshot_cursor"]) == page


def test_sender_can_identify_root_without_proving_recipient_outcome(core):
    root = add(core, 1, phase="expired", role="sender", agent_id="sender")
    project_all(core)
    item = read(core)["items"][0]
    assert item["root_task_id"] == root["task_id"]
    assert item["root_agent_id"] is None and item["outcome"] is None
