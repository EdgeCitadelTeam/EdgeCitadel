import json
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes, validate_read_response
from edgecitadel_agentd.trace_cursor import (
    CursorScope,
    cursor_scope_hash,
    decode_cursor,
    encode_cursor,
)
from test_trace_graph_retention import NOW, clean
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import trace_event_pages as pages
from aggregator import trace_projection_history as history
from aggregator import trace_projection_rebuild as rebuild
from aggregator import trace_projection_retention as retirement
from aggregator import trace_projection_store as projection
from aggregator import trace_retention, trace_store

core = core_fixture
KEY = b"owned-event-page-fixture-key-32-bytes"
SCOPE = cursor_scope_hash({}, {"mode": "trusted_fleet", "version": 1})


def seed(db, count=3):
    values = [event(seq=seq) for seq in range(1, count + 1)]
    for value in values:
        assert put(db, value, str(uuid4()), 1).outcome == "accepted"
    state = projection.project_batch(db)
    return values, state, token(state, values[0]["trace_id"])


def token(state, trace_id, **overrides):
    return encode_cursor(
        {
            "schema_version": 1,
            "kind": "graph",
            "trace_id": trace_id,
            "scope_hash": SCOPE,
            "projection_generation": state.generation,
            "snapshot": state.change_cursor,
            "position": state.change_cursor,
            "upper": state.ingest_cursor,
            "key": None,
            **overrides,
        },
        KEY,
    )


def read(db, values, snapshot, **kwargs):
    return pages.read_events(
        db,
        trace_id=values[0]["trace_id"],
        as_of=snapshot,
        signing_key=KEY,
        scope_hash=SCOPE,
        **kwargs,
    )


def assert_error(code, callback):
    with pytest.raises(pages.TraceReadError, match=f"^{code}$") as error:
        callback()
    validate_read_response(error.value.response)
    assert "PRIVATE_SENTINEL" not in json.dumps(error.value.response)
    return error.value


def test_stable_pages_exclude_late_and_unrelated_events_and_duplicate_receipts(core):
    values, state, snapshot = seed(core)
    first = read(core, values, snapshot, limit=1)
    assert first["events"] == values[:1]
    validate_read_response(first)
    claims = decode_cursor(
        first["next_cursor"],
        KEY,
        CursorScope("events", values[0]["trace_id"], SCOPE, state.generation),
        retained_from=0,
    )
    assert (claims["position"], claims["upper"], claims["snapshot"]) == (1, 3, 3)
    # An older occurrence time still belongs to the later ingestion snapshot.
    late = event(seq=4, occurred_at="2020-01-01T00:00:00.000Z")
    unrelated = event(seq=5, trace_id="b" * 32)
    put(core, late, str(uuid4()), 1)
    put(core, unrelated, str(uuid4()), 1)
    assert put(core, values[0], str(uuid4()), 1).outcome == "duplicate"
    fresh = projection.project_batch(core)
    rest = read(core, values, snapshot, after=first["next_cursor"], limit=500)
    assert rest["events"] == values[1:] and rest["next_cursor"] is None
    assert read(core, values, token(fresh, values[0]["trace_id"]))["events"] == [
        *values,
        late,
    ]
    assert not core.in_transaction


def test_read_only_connection_creates_no_persistent_state(core):
    values, _, snapshot = seed(core)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    before = list(core.iterdump())
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as reader:
        assert read(reader, values, snapshot)["events"] == values
        assert not reader.in_transaction and reader.total_changes == 0
        assert reader.execute("SELECT name FROM sqlite_temp_master").fetchall() == []
    assert list(core.iterdump()) == before


@pytest.mark.parametrize("kind", ["changes", "events", "expansion"])
def test_cursor_kinds_are_not_interchangeable(core, kind):
    values, state, _ = seed(core)
    wrong = token(
        state,
        values[0]["trace_id"],
        kind=kind,
        key="branch" if kind == "expansion" else None,
    )
    assert_error("cursor_scope_mismatch", lambda: read(core, values, wrong))


def test_cursor_scope_signature_and_cross_snapshot_rejected(core):
    values, state, snapshot = seed(core)
    other_scope = token(state, values[0]["trace_id"], scope_hash="c" * 64)
    other_trace = token(state, "b" * 32)
    for wrong in (other_scope, other_trace):
        assert_error("cursor_scope_mismatch", lambda: read(core, values, wrong))
    assert_error("invalid_cursor", lambda: read(core, values, "PRIVATE_SENTINEL"))
    wrong_signature = snapshot[:-1] + ("A" if snapshot[-1] != "A" else "B")
    assert_error("invalid_cursor", lambda: read(core, values, wrong_signature))
    first = read(core, values, snapshot, limit=1)
    put(core, event(seq=4), str(uuid4()), 1)
    fresh = projection.project_batch(core)
    assert_error(
        "cursor_scope_mismatch",
        lambda: read(
            core,
            values,
            token(fresh, values[0]["trace_id"]),
            after=first["next_cursor"],
        ),
    )
    assert_error(
        "cursor_scope_mismatch", lambda: read(core, values, snapshot, after=snapshot)
    )


def test_signed_watermarks_still_require_authoritative_history(core):
    values, state, _ = seed(core)
    wrong_upper = token(state, values[0]["trace_id"], upper=99)
    assert_error("invalid_cursor", lambda: read(core, values, wrong_upper))
    ahead = token(state, values[0]["trace_id"], snapshot=99, position=99, upper=99)
    assert_error("invalid_cursor", lambda: read(core, values, ahead))
    unknown = token(state, "b" * 32)
    assert_error("not_found", lambda: read(core, [{"trace_id": "b" * 32}], unknown))


def test_compaction_preserves_base_membership_and_expires_older_tokens(core):
    values, state, snapshot = seed(core)
    old = token(state, values[0]["trace_id"], snapshot=1, position=1, upper=1)
    for _ in range(30):
        result = history.compact_batch(
            core,
            generation=state.generation,
            through_cursor=state.change_cursor,
            limit=2,
        )
        if result["deleted_rows"] == 0:
            break
    assert read(core, values, snapshot)["events"] == values
    error = assert_error("history_expired", lambda: read(core, values, old))
    assert error.response["retained_from"] == state.change_cursor


def test_graph_cleanup_keeps_old_membership_and_recreation_does_not_mix_it(core):
    values, state, snapshot = seed(core)
    assert retirement.expire_one(core, now_ms=NOW)["status"] == "retired"
    clean(core)
    assert (
        core.execute("SELECT count(*) FROM trace_projection_run_events").fetchone()[0]
        == 0
    )
    assert read(core, values, snapshot)["events"] == values
    late = event(seq=4)
    put(core, late, str(uuid4()), 1, received_at_ms=NOW)
    fresh = projection.project_batch(core)
    assert read(core, values, token(fresh, values[0]["trace_id"]))["events"] == [late]
    assert read(core, values, snapshot)["events"] == values


def test_payload_expiry_is_explicit_instead_of_silent_omission(core):
    values, _, snapshot = seed(core)
    first = read(core, values, snapshot, limit=1)
    assert trace_retention.expire_payloads(core, now_ms=NOW)["expired_payloads"] == 3
    error = assert_error(
        "history_expired",
        lambda: read(core, values, snapshot, after=first["next_cursor"]),
    )
    assert error.status_code == 410
    assert error.response["resnapshot_required"]


def test_rebuild_changes_generation_and_replays_membership(core):
    values, _, snapshot = seed(core)
    candidate = rebuild.begin(core)
    fresh = projection.project_batch(core, build_generation=candidate.generation)
    rebuild.activate(core, generation=candidate.generation)
    assert_error("generation_changed", lambda: read(core, values, snapshot))
    assert read(core, values, token(fresh, values[0]["trace_id"]))["events"] == values


def test_byte_budget_shortens_pages_without_dropping_observations(core, monkeypatch):
    values, _, snapshot = seed(core, count=8)
    first_two = read(core, values, snapshot, limit=2)
    budget = len(canonical_bytes(first_two)) + 100
    monkeypatch.setattr(pages, "MAX_RESPONSE_BYTES", budget)
    results, after = [], None
    while True:
        result = read(core, values, snapshot, limit=500, after=after)
        assert len(canonical_bytes(result)) <= budget
        assert result["events"]
        validate_read_response(result)
        results.extend(result["events"])
        after = result["next_cursor"]
        if after is None:
            break
    assert results == values


def test_corrupt_payload_is_not_exposed(core):
    values, _, snapshot = seed(core)
    encoded = json.dumps({**values[0], "raw_arguments": "PRIVATE_SENTINEL"})
    separated = core.execute(
        "SELECT 1 FROM sqlite_master WHERE name='trace_payloads'"
    ).fetchone()
    table = "trace_payloads" if separated else "trace_raw_events"
    with core:
        # Deliberately model on-disk corruption beyond the normal writer guard.
        if separated:
            core.execute("DROP TRIGGER trace_payload_immutable")
        core.execute(f"UPDATE {table} SET event_json=? WHERE ingest_seq=1", (encoded,))
    assert_error("unavailable", lambda: read(core, values, snapshot))
    assert not core.in_transaction
    assert core.execute("SELECT name FROM sqlite_temp_master").fetchall() == []


@pytest.mark.parametrize("limit", [0, 501, True, "1"])
def test_invalid_page_limits(core, limit):
    values, _, snapshot = seed(core)
    assert_error("invalid_request", lambda: read(core, values, snapshot, limit=limit))


def test_concurrent_payload_expiry_cannot_tear_a_page_snapshot(core, monkeypatch):
    values, _, snapshot = seed(core)
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    original = pages.read_payload
    expired = False

    def expire_during_read(connection, seq):
        nonlocal expired
        if not expired:
            with closing(sqlite3.connect(path)) as writer:
                trace_store.initialize(writer)
                assert (
                    trace_retention.expire_payloads(writer, now_ms=NOW)[
                        "expired_payloads"
                    ]
                    == 3
                )
            expired = True
        return original(connection, seq)

    monkeypatch.setattr(pages, "read_payload", expire_during_read)
    assert read(core, values, snapshot)["events"] == values
    assert expired
    assert_error("history_expired", lambda: read(core, values, snapshot))


def test_real_two_mib_page_boundary_preserves_all_accepted_observations(core):
    export_generation = str(uuid4())
    attributes = {
        "export_generation": export_generation,
        "through_export_seq": 9007199254740991,
        "lost_ranges": [
            {"first": 9007199254000000 + i * 2, "last": 9007199254000000 + i * 2}
            for i in range(128)
        ],
    }
    values = [
        event(
            "lost", kind="coverage", seq=seq, trace_id="a" * 32, attributes=attributes
        )
        for seq in range(1, 301)
    ]
    for seq, value in enumerate(values, 1):
        assert put(core, value, export_generation, seq).outcome == "accepted"
    while True:
        state = projection.project_batch(core)
        if state.ingest_cursor == len(values):
            break
    snapshot = token(state, values[0]["trace_id"])
    first = read(core, values, snapshot, limit=500)
    assert len(canonical_bytes(first, limit=pages.MAX_RESPONSE_BYTES)) <= 2 * 1024**2
    assert 0 < len(first["events"]) < len(values)
    assert first["next_cursor"] is not None
    second = read(core, values, snapshot, limit=500, after=first["next_cursor"])
    assert second["next_cursor"] is None
    assert first["events"] + second["events"] == values


def test_node_filter_has_snapshot_bound_sparse_continuations_and_distinct_scope(core):
    values, state, snapshot = seed(core, 3)
    selected = "task:" + values[0]["task_id"]
    first = read(core, values, snapshot, node_id=selected, limit=1)
    assert first["events"] == values[:1]
    assert_error(
        "cursor_scope_mismatch",
        lambda: read(core, values, snapshot, after=first["next_cursor"]),
    )
    assert_error(
        "cursor_scope_mismatch",
        lambda: read(
            core, values, snapshot, node_id="task:other", after=first["next_cursor"]
        ),
    )
    sparse = read(core, values, snapshot, node_id="run:unrelated", limit=1)
    assert sparse["events"] == [] and sparse["next_cursor"]
    while sparse["next_cursor"]:
        sparse = read(
            core,
            values,
            snapshot,
            node_id="run:unrelated",
            limit=1,
            after=sparse["next_cursor"],
        )
        assert sparse["events"] == []


def test_step_filter_uses_projector_identity_for_native_attempt_and_logical_root(core):
    from aggregator.trace_graph_projection import entity_claims

    value = event(
        "started", kind="run", task_id=None, parent_task_id=None, parent_run_id=None
    )
    assert put(core, value, str(uuid4()), 1).outcome == "accepted"
    state = projection.project_batch(core)
    for claim in entity_claims(value):
        result = read(
            core, [value], token(state, value["trace_id"]), node_id=claim["id"]
        )
        assert result["events"] == [value]
