import json
import sqlite3
from copy import deepcopy
from itertools import permutations
from pathlib import Path
from uuid import uuid4

import pytest

from edgecitadel_agentd.trace_contract import TraceContractError, event_sha256
from aggregator import trace_payloads, trace_store
from aggregator.trace_payload_read import read_payload
from aggregator.trace_task_projection import TaskObservation, reduce_task

FIXTURES = Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces"
READ = json.loads((FIXTURES / "read.v1.json").read_text())


def observed(phase, *, source_seq=1, ingest_seq=1, role="recipient", **fields):
    event = deepcopy(READ["input_events"][0])
    event.update(
        event_id=str(uuid4()),
        source_seq=source_seq,
        phase=phase,
        attributes={"source_role": role},
        **fields,
    )
    return TaskObservation(ingest_seq, event)


def test_frozen_task_nodes_are_reduced_from_events_not_expected_states():
    tasks = [event for event in READ["input_events"] if event["kind"] == "task"]
    actual = [
        reduce_task([TaskObservation(i + 1, event)]).node
        for i, event in enumerate(tasks)
    ]
    assert actual == READ["expected_graph"]["nodes"]
    # Child outcomes never change the still-running root observation.
    assert actual[0]["state"] == "running"


def test_source_sequence_wins_over_arrival_and_wall_clock():
    started = observed("running", source_seq=3, ingest_seq=2)
    queued = observed(
        "queued", source_seq=1, ingest_seq=3, occurred_at="2026-09-17T12:00:00.000Z"
    )
    offered = observed("offered", source_seq=2, ingest_seq=1)
    values = [started, queued, offered]
    for ordering in permutations(values):
        projected = reduce_task(ordering)
        assert projected.node["state"] == "running"
        assert projected.perspectives[0]["event_id"] == started.event["event_id"]
        assert projected.outcomes == ()
        assert not projected.ambiguous_live_state
        delivered = reduce_task(
            TaskObservation(i + 1, item.event) for i, item in enumerate(ordering)
        )
        assert delivered.node == projected.node


def test_requeue_is_latest_source_state_not_monotonic_phase_rank():
    offered = observed("offered", source_seq=1, ingest_seq=2)
    queued = observed("queued", source_seq=2, ingest_seq=1)
    assert reduce_task([offered, queued]).node["state"] == "queued"


@pytest.mark.parametrize("sender_first", [True, False])
def test_sender_deadline_cannot_overwrite_recipient_completion(sender_first):
    sender = observed(
        "expired",
        ingest_seq=1 if sender_first else 2,
        role="sender",
        node_id="edge-sender",
        agent_id="sender",
    )
    recipient = observed("completed", ingest_seq=2 if sender_first else 1)
    result = reduce_task([sender, recipient])
    assert result.node["state"] == "completed"
    assert result.node["agent_id"] == "worker-a"
    assert result.node["conflict"]
    assert result.node["outcome_candidate_count"] == 2
    assert {item["source_role"]: item["phase"] for item in result.outcomes} == {
        "sender": "expired",
        "recipient": "completed",
    }
    assert [item["ingest_seq"] for item in result.outcomes] == [1, 2]


def test_terminal_conflict_rebuild_preserves_first_commit_and_all_candidates():
    failed = observed("failed", source_seq=2, ingest_seq=2)
    completed = observed("completed", source_seq=3, ingest_seq=1)
    completed.event["supersedes_event_id"] = failed.event["event_id"]
    result = reduce_task([failed, completed])
    assert result == reduce_task([completed, failed, failed])
    assert result.node["state"] == "completed"
    assert result.node["outcome_candidate_count"] == 2
    assert result.outcomes[0]["supersedes_event_id"] == failed.event["event_id"]
    # Different persisted arrival history intentionally changes presentation,
    # without hiding either terminal candidate or changing original event bytes.
    changed = reduce_task(
        [
            TaskObservation(1, failed.event),
            TaskObservation(2, completed.event),
        ]
    )
    assert changed.node["state"] == "failed"
    assert changed.node["conflict"]


def test_cancelled_is_displayed_without_losing_original_or_evidence_kind():
    event = observed("cancelled", evidence_kind="compatibility_synthesized")
    result = reduce_task([event])
    assert result.node["state"] == "canceled"
    assert result.node["original_state"] == "cancelled"
    assert result.outcomes[0]["evidence_kind"] == "compatibility_synthesized"
    assert (
        result.outcomes[0]["execution_attempt_id"]
        == event.event["execution_attempt_id"]
    )


def test_independent_live_epochs_are_not_ordered_by_uuid_or_clock():
    running = observed("running", source_seq=100, ingest_seq=1)
    queued = observed("queued", source_seq=1, ingest_seq=2, source_epoch=str(uuid4()))
    for order in ([running, queued], [queued, running]):
        result = reduce_task(order)
        assert result.node["state"] == "unknown"
        assert result.ambiguous_live_state
        assert len(result.perspectives) == 2


def test_rejects_mixed_scope_and_conflicting_immutable_receipts():
    one = observed("running")
    different_task = observed("running", ingest_seq=2, task_id=str(uuid4()))
    with pytest.raises(ValueError, match="scope_mismatch"):
        reduce_task([one, different_task])
    changed = deepcopy(one.event)
    changed["phase"] = "completed"
    with pytest.raises(TraceContractError, match="event_identity_conflict"):
        reduce_task([one, TaskObservation(2, changed)])
    with pytest.raises(TraceContractError, match="source_position_conflict"):
        reduce_task([one, observed("running", ingest_seq=2)])
    with pytest.raises(ValueError, match="ingest_position_conflict"):
        reduce_task([one, observed("running", source_seq=2)])


def test_model_completion_cannot_be_used_as_task_outcome():
    fixtures = json.loads((FIXTURES / "events.v1.json").read_text())["fixtures"]
    model = next(item["event"] for item in fixtures if item["name"] == "model")
    with pytest.raises(ValueError, match="requires_task_events"):
        reduce_task([TaskObservation(1, model)])


@pytest.mark.parametrize("separated", [False, True])
def test_core_commit_replay_and_reopen_supply_stable_task_projection(
    tmp_path, separated
):
    path = tmp_path / "core.db"
    completed = observed("completed")
    sender = observed("expired", node_id="edge-sender", role="sender")
    generation = str(uuid4())
    with sqlite3.connect(path) as db:
        trace_store.initialize(db)
        if separated:
            trace_payloads.prepare(db)
        for observation in (completed, sender):
            event = observation.event
            record = {
                "schema_version": 1,
                "node_id": event["node_id"],
                "source_epoch": event["source_epoch"],
                "export_generation": generation,
                "export_seq": 1,
                "event_sha256": event_sha256(event),
                "event": event,
            }
            subject = f"edgecitadel.telemetry.v1.{event['node_id']}"
            trace_store.ingest(db, subject, record, received_at_ms=1000)
            record["export_generation"] = str(uuid4())
            assert (
                trace_store.ingest(db, subject, record, received_at_ms=2000).outcome
                == "duplicate"
            )
            contradictory = deepcopy(record)
            contradictory["export_seq"] = 2
            contradictory["event"]["phase"] = "failed"
            contradictory["event_sha256"] = event_sha256(contradictory["event"])
            assert (
                trace_store.ingest(
                    db, subject, contradictory, received_at_ms=3000
                ).outcome
                == "conflict"
            )
        raw = [
            TaskObservation(seq, read_payload(db, seq)["event"])
            for (seq,) in db.execute("SELECT ingest_seq FROM trace_raw_events")
        ]
        original = reduce_task(raw)
    with sqlite3.connect(path) as db:
        rebuilt = reduce_task(
            [
                TaskObservation(seq, read_payload(db, seq)["event"])
                for (seq,) in db.execute(
                    "SELECT ingest_seq FROM trace_raw_events ORDER BY ingest_seq DESC"
                )
            ]
        )
    assert rebuilt == original
    assert rebuilt.node["state"] == "completed"
    assert rebuilt.node["outcome_candidate_count"] == 2
    assert len(rebuilt.outcomes) == 2
