import json
import sqlite3
import hashlib
from itertools import count
from copy import deepcopy
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import canonical_bytes, validate_read_response
from test_trace_graph_projection import READ
from test_trace_projection_store import core as core_fixture, event as make_event

from aggregator import trace_projection_store as projection, trace_retention
from aggregator.trace_ingest import ingest_wire

core = core_fixture
_sequences = count(1)


def event(*args, **kwargs):
    kwargs["seq"] = next(_sequences)
    return make_event(*args, **kwargs)


def put(db, value, generation, position):
    wrapper = dict(
        schema_version=1,
        node_id=value["node_id"],
        source_epoch=value["source_epoch"],
        export_generation=generation,
        export_seq=position,
        event=value,
        event_sha256=hashlib.sha256(canonical_bytes(value)).hexdigest(),
    )
    return ingest_wire(
        db,
        f"edgecitadel.telemetry.v1.{value['node_id']}",
        canonical_bytes(wrapper),
        received_at_ms=1,
    )


def graph(db, trace_id):
    result = projection.read_graph(db, trace_id=trace_id)
    validate_read_response(
        {
            **READ["expected_graph"],
            "trace_id": trace_id,
            "nodes": result["nodes"],
            "edges": result["edges"],
            "total_nodes": len(result["nodes"]),
            "coverage": result["coverage"],
        }
    )
    return result


def marker(value, generation, through, *, ranges=(), scoped=True, epoch=None, **attrs):
    return event(
        "lost" if ranges else "unknown",
        kind="coverage",
        node_id=value["node_id"],
        source_epoch=epoch or value["source_epoch"],
        trace_id=value["trace_id"] if scoped else None,
        attributes=dict(
            export_generation=generation,
            through_export_seq=through,
            affected_source_epoch=value["source_epoch"],
            lost_ranges=[dict(first=a, last=b) for a, b in ranges],
            **attrs,
        ),
    )


def positions(value):
    return {
        (s["node_id"], s["source_epoch"], s["export_generation"]): s["export_seq"]
        for s in value["coverage"]["reconciled_through"]
    }


def test_sparse_receipts_do_not_read_ahead_of_projection_snapshot(core):
    value, generation = event("completed"), str(uuid4())
    put(core, value, generation, 2)
    put(core, event(seq=2), generation, 1)
    projection.project_batch(core, limit=1)
    before = graph(core, value["trace_id"])
    assert before["coverage"]["catching_up"]
    assert list(positions(before).values()) == [0]
    assert before["nodes"][0]["state"] == "completed"
    projection.project_batch(core, limit=1)
    after = graph(core, value["trace_id"])
    assert not after["coverage"]["catching_up"]
    assert list(positions(after).values()) == [2]
    assert after["coverage"]["partial"] and after["coverage"]["unknown_sources"]


def test_checkpoint_requirement_and_duplicate_generation_do_not_add_nodes(core):
    value, generation, replay = event(), str(uuid4()), str(uuid4())
    put(core, value, generation, 1)
    put(core, marker(value, generation, 3), generation, 2)
    projection.project_batch(core)
    before = graph(core, value["trace_id"])
    assert before["coverage"]["catching_up"]
    assert put(core, value, generation, 3).outcome == "duplicate"
    assert put(core, value, replay, 1).outcome == "duplicate"
    state = projection.project_batch(core)
    after = graph(core, value["trace_id"])
    assert not after["coverage"]["catching_up"]
    assert sorted(positions(after).values()) == [1, 3]
    assert before["nodes"] == after["nodes"]
    changes = projection.read_changes(core, generation=state.generation, after=2)[
        "changes"
    ]
    assert len(changes) == 2
    assert all(
        c["kind"] == "receipt" and c["trace_id"] == value["trace_id"] for c in changes
    )


def test_rejection_does_not_trust_payload_run_membership(core):
    value, generation, victim = event(), str(uuid4()), uuid4().hex
    put(core, value, generation, 1)
    invalid = event(seq=2, trace_id=victim, schema_version=2)
    assert put(core, invalid, generation, 2).outcome == "rejected"
    state = projection.project_batch(core)
    known = graph(core, value["trace_id"])
    claimed = graph(core, victim)
    assert known["coverage_reasons"]["source_uncertainty"]
    assert not known["coverage"]["gap"]
    assert not claimed["nodes"] and not claimed["coverage"]["reconciled_through"]
    assert not claimed["coverage_reasons"]["source_uncertainty"]
    last = projection.read_changes(core, generation=state.generation, after=1)[
        "changes"
    ][0]
    assert last["trace_id"] is None and last["kind"] == "receipt"
    assert victim not in json.dumps(last)


@pytest.mark.parametrize("scoped", [False, True])
def test_source_loss_does_not_invent_run_gap_and_scoped_loss_can_be_repaired(
    core, scoped
):
    value, generation = event(), str(uuid4())
    put(core, value, generation, 1)
    loss = marker(value, generation, 3, ranges=[(2, 3)], scoped=scoped)
    put(core, loss, generation, 4)
    projection.project_batch(core)
    before = graph(core, value["trace_id"])
    assert before["coverage"]["gap"] is scoped
    assert before["coverage_reasons"]["source_uncertainty"]
    assert not before["coverage"]["catching_up"]
    put(core, value, generation, 2)
    put(core, value, generation, 3)
    projection.project_batch(core)
    after = graph(core, value["trace_id"])
    assert not after["coverage"]["gap"]
    assert after["coverage_reasons"]["source_uncertainty"]
    assert before["nodes"] == after["nodes"]


def test_old_epoch_large_loss_range_is_compact_and_correctly_scoped(core):
    value, generation, writer_generation, writer_epoch = (
        event(),
        str(uuid4()),
        str(uuid4()),
        str(uuid4()),
    )
    put(core, value, generation, 1)
    maximum = 9007199254740991
    put(
        core,
        marker(value, generation, maximum, ranges=[(2, maximum)], epoch=writer_epoch),
        writer_generation,
        1,
    )
    projection.project_batch(core)
    result = graph(core, value["trace_id"])
    assert positions(result) == {
        (value["node_id"], value["source_epoch"], generation): maximum,
        (value["node_id"], writer_epoch, writer_generation): 1,
    }
    assert result["coverage"]["gap"]
    assert (
        core.execute("SELECT COUNT(*) FROM trace_projection_intervals").fetchone()[0]
        == 4
    )


def test_scoped_unsupported_and_unpositioned_loss_are_not_global_capability(core):
    value, other, generation = event(), event(trace_id=uuid4().hex), str(uuid4())
    put(core, value, generation, 1)
    put(core, other, generation, 2)
    put(
        core,
        marker(
            value, generation, 2, unsupported_families=["tool"], dropped_observations=3
        ),
        generation,
        3,
    )
    projection.project_batch(core)
    first, second = graph(core, value["trace_id"]), graph(core, other["trace_id"])
    assert first["coverage"]["gap"] and first["coverage"]["unsupported_families"] == [
        "tool"
    ]
    assert first["coverage_reasons"]["run_coverage_unknown"]
    assert (
        not second["coverage"]["gap"]
        and second["coverage"]["unsupported_families"] == []
    )
    put(
        core,
        marker(value, generation, 3, scoped=False, unsupported_families=["model"]),
        generation,
        4,
    )
    projection.project_batch(core)
    result = graph(core, other["trace_id"])
    assert result["coverage_reasons"]["source_uncertainty"]
    assert result["coverage"]["unsupported_families"] == []


def test_expired_marker_still_consumes_durable_loss_without_inventing_run(core):
    value, generation = event(), str(uuid4())
    put(core, value, generation, 3)
    projection.project_batch(core)
    put(core, marker(value, generation, 3, ranges=[(1, 2)]), generation, 4)
    trace_retention.expire_payloads(core, now_ms=trace_retention.RETENTION_MS + 2)
    state = projection.project_batch(core)
    result = graph(core, value["trace_id"])
    assert not result["coverage"]["catching_up"]
    assert result["coverage_reasons"]["source_uncertainty"]
    assert not result["coverage"][
        "gap"
    ]  # Expired body cannot establish which run lost content.
    change = projection.read_changes(core, generation=state.generation, after=1)[
        "changes"
    ][0]
    assert change["kind"] == "payload_expired" and change["trace_id"] is None
    assert change["change"]["scope_updates"][0]["export_seq"] == 4


def test_restore_marks_previous_epoch_and_security_stays_diagnostic(core):
    value, generation = event(), str(uuid4())
    put(core, value, generation, 1)
    restored = event(
        "restored",
        kind="source",
        source_epoch=str(uuid4()),
        attributes=dict(
            previous_source_epoch=value["source_epoch"], export_generation=str(uuid4())
        ),
    )
    put(core, restored, str(uuid4()), 1)
    security = event("authentication_rejected", kind="security")
    put(core, security, generation, 2)
    state = projection.project_batch(core)
    result = graph(core, value["trace_id"])
    assert (
        result["coverage_reasons"]["source_uncertainty"]
        and not result["coverage"]["gap"]
    )
    assert len(result["nodes"]) == 1
    changes = projection.read_changes(core, generation=state.generation, after=1)[
        "changes"
    ]
    assert all(c["kind"] == "source_fact" and c["trace_id"] is None for c in changes)


def test_existing_position_conflict_is_projected_without_reassigning_membership(core):
    value, generation = event(), str(uuid4())
    put(core, value, generation, 1)
    changed = deepcopy(value)
    changed["trace_id"] = uuid4().hex
    assert put(core, changed, generation, 1).outcome == "conflict"
    state = projection.project_batch(core)
    assert state.ingest_cursor == state.change_cursor == 2
    assert graph(core, value["trace_id"])["coverage_reasons"]["source_uncertainty"]
    assert graph(core, changed["trace_id"])["nodes"] == []


def test_v2_requires_rebuild(core):
    core.execute("UPDATE trace_projection_state SET version=2")
    core.commit()
    with pytest.raises(ValueError, match="projection_version_unavailable"):
        projection.initialize(core)


def test_receipt_only_failure_rolls_back_coverage_and_checkpoint(core):
    value, generation = event(), str(uuid4())
    put(core, value, generation, 2)
    projection.project_batch(core)
    assert put(core, value, generation, 1).outcome == "duplicate"
    core.execute(
        "CREATE TRIGGER coverage_fault BEFORE UPDATE ON trace_projection_state "
        "BEGIN SELECT RAISE(ABORT,'coverage checkpoint failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="coverage checkpoint failure"):
        projection.project_batch(core)
    assert list(core.iterdump()) == before
    assert graph(core, value["trace_id"])["coverage"]["catching_up"]
    core.execute("DROP TRIGGER coverage_fault")
    projection.project_batch(core)
    assert not graph(core, value["trace_id"])["coverage"]["catching_up"]
