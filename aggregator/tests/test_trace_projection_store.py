import json
import select
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from edgecitadel_agentd.trace_contract import event_sha256

from aggregator import trace_payloads, trace_projection_store as projection
from aggregator import trace_retention, trace_store
from aggregator.trace_task_projection import TaskObservation, reduce_task

EVENTS = json.loads(
    (
        Path(__file__).parents[2] / "agent-runtime/tests/fixtures/traces/events.v1.json"
    ).read_text()
)["fixtures"]


def event(phase="running", *, seq=1, kind="task", **fields):
    value = deepcopy(next(item["event"] for item in EVENTS if item["name"] == kind))
    value.update(event_id=str(uuid4()), source_seq=seq, phase=phase, **fields)
    return value


def ingest(db, value):
    return trace_store.ingest(
        db,
        f"edgecitadel.telemetry.v1.{value['node_id']}",
        {
            "schema_version": 1,
            "node_id": value["node_id"],
            "source_epoch": value["source_epoch"],
            "export_generation": str(uuid4()),
            "export_seq": 1,
            "event_sha256": event_sha256(value),
            "event": value,
        },
        received_at_ms=1,
    )


@pytest.fixture(params=[False, True], ids=["inline", "separated"])
def core(tmp_path, request):
    with closing(sqlite3.connect(tmp_path / "core.db")) as db:
        db.execute("PRAGMA journal_mode=WAL")
        trace_store.initialize(db)
        if request.param:
            trace_payloads.prepare(db)
            assert trace_payloads.migrate_batch(db)
        projection.initialize(db)
        yield db


def snapshot(db, value):
    return projection.read_task(
        db, trace_id=value["trace_id"], task_id=value["task_id"]
    )


def test_batch_checkpoint_replay_conflicts_and_reopen(core):
    values = [
        event(),
        event("completed", seq=2),
        event("expired", node_id="sender", attributes={"source_role": "sender"}),
        event("finished", kind="model", seq=3),
    ]
    accepted = [(ingest(core, value).ingest_seq, value) for value in values]
    for expected in range(1, 5):
        state = projection.project_batch(core, limit=1)
        assert state.ingest_cursor == expected
    result = snapshot(core, values[0])
    expected = reduce_task(
        [
            TaskObservation(seq, value)
            for seq, value in accepted
            if value["kind"] == "task"
        ]
    )
    assert result["node"] == expected.node
    assert state.change_cursor == 4
    assert ingest(core, values[0]).outcome == "duplicate"
    changed = deepcopy(values[0])
    changed["phase"] = "failed"
    assert ingest(core, changed).outcome == "conflict"
    caught_up = projection.project_batch(core)
    assert caught_up.ingest_cursor == 6
    assert caught_up.change_cursor == 6
    assert projection.project_batch(core) == caught_up
    changes = projection.read_changes(core, generation=state.generation, after=0)[
        "changes"
    ]
    assert [change["ingest_seq"] for change in changes] == [1, 2, 3, 4, 5, 6]
    assert changes[2]["change"]["node"] == expected.node
    assert changes[3]["change"]["node_updates"][0]["node"]["kind"] == "model"
    path = core.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path)) as reopened:
        assert projection.initialize(reopened) == caught_up
        assert snapshot(reopened, values[0])["node"] == expected.node
        assert projection.project_batch(reopened) == caught_up


@pytest.mark.parametrize("boundary", ["change", "checkpoint"])
def test_fault_rolls_back_node_evidence_change_and_checkpoint(core, boundary):
    value = event("completed")
    ingest(core, value)
    statement = (
        "BEFORE INSERT ON trace_projection_changes"
        if boundary == "change"
        else "BEFORE UPDATE ON trace_projection_state"
    )
    core.execute(
        f"CREATE TRIGGER projection_fault {statement} "
        "BEGIN SELECT RAISE(ABORT,'owned projection failure'); END"
    )
    before = list(core.iterdump())
    with pytest.raises(sqlite3.IntegrityError, match="owned projection failure"):
        projection.project_batch(core)
    assert list(core.iterdump()) == before
    assert not core.in_transaction
    # Raw ingestion can still commit while projection is faulted.
    assert ingest(core, event("failed", seq=2)).outcome == "accepted"
    core.execute("DROP TRIGGER projection_fault")
    state = projection.project_batch(core)
    assert state.ingest_cursor == state.change_cursor == 2
    assert snapshot(core, value)["node"]["outcome_candidate_count"] == 2


def test_expired_unprojected_payload_has_durable_unattributed_gap(core):
    value = event("completed")
    ingest(core, value)
    assert (
        trace_retention.expire_payloads(core, now_ms=trace_retention.RETENTION_MS + 2)[
            "expired_payloads"
        ]
        == 1
    )
    state = projection.project_batch(core)
    changes = projection.read_changes(core, generation=state.generation, after=0)[
        "changes"
    ]
    assert len(changes) == 1
    assert changes[0]["kind"] == "payload_expired"
    assert changes[0]["trace_id"] is None
    assert changes[0]["change"]["event_id"] == value["event_id"]
    assert snapshot(core, value)["node"] is None
    assert projection.project_batch(core) == state


def test_corrupted_identity_does_not_advance_projection(core):
    value = event("completed")
    ingest(core, value)
    with core:
        core.execute("UPDATE trace_raw_events SET event_sha256=?", ("f" * 64,))
    before = list(core.iterdump())
    with pytest.raises(ValueError, match="raw_identity_mismatch"):
        projection.project_batch(core)
    assert list(core.iterdump()) == before


def test_batch_boundary_preserves_unprocessed_observations(core):
    values = [event("completed", seq=i + 1, task_id=str(uuid4())) for i in range(70)]
    for value in values:
        ingest(core, value)
    before = list(core.iterdump())
    with pytest.raises(ValueError, match="invalid_projection_batch_limit"):
        projection.project_batch(core, limit=True)
    assert list(core.iterdump()) == before
    first = projection.project_batch(core)
    assert 0 < first.ingest_cursor < len(values)
    assert snapshot(core, values[first.ingest_cursor])["node"] is None
    final = projection.project_batch(core)
    assert final.ingest_cursor == final.change_cursor == len(values)
    assert snapshot(core, values[-1])["node"]["state"] == "completed"


@pytest.mark.parametrize(
    "mutation,diagnostic",
    [
        ("UPDATE trace_collector SET collector_epoch='restored'", "rebuild_required"),
        ("UPDATE trace_projection_state SET version=999", "version_unavailable"),
    ],
)
def test_epoch_and_version_fence_reads_and_writes(core, mutation, diagnostic):
    value = event()
    ingest(core, value)
    state = projection.project_batch(core)
    with core:
        core.execute(mutation)
    before = list(core.iterdump())
    for operation in [
        lambda: projection.initialize(core),
        lambda: projection.project_batch(core),
        lambda: snapshot(core, value),
        lambda: projection.read_changes(core, generation=state.generation, after=0),
    ]:
        with pytest.raises(ValueError, match=diagnostic):
            operation()
        assert not core.in_transaction
    assert list(core.iterdump()) == before


def test_read_snapshot_cannot_mix_old_cursor_with_new_node(core):
    first = event()
    ingest(core, first)
    initial = projection.project_batch(core)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    errors, writes = [], []

    def write_between_state_and_node(sql):
        if 'FROM "trace_projected_tasks"' not in sql or writes:
            return
        writes.append(True)
        try:
            with closing(sqlite3.connect(path)) as writer:
                ingest(writer, event("completed", seq=2))
                projection.project_batch(writer)
        except Exception as error:
            errors.append(error)

    core.set_trace_callback(write_between_state_and_node)
    try:
        old = snapshot(core, first)
    finally:
        core.set_trace_callback(None)
    assert writes and not errors
    assert old["state"] == initial
    assert old["node"]["state"] == "running"
    latest = snapshot(core, first)
    assert latest["state"].change_cursor == 2
    assert latest["node"]["state"] == "completed"
    assert not core.in_transaction
    assert core.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0] == 0


def test_outcome_pages_remain_at_selected_snapshot(core):
    value = event("completed")
    ingest(core, value)
    state = projection.project_batch(core)
    args = dict(
        generation=state.generation,
        trace_id=value["trace_id"],
        task_id=value["task_id"],
        as_of=state.ingest_cursor,
        limit=1,
    )
    first = projection.read_outcomes(core, **args)["outcomes"]
    ingest(core, event("failed", seq=2))
    projection.project_batch(core)
    assert len(first) == 1
    assert (
        projection.read_outcomes(core, after=first[0]["ingest_seq"], **args)["outcomes"]
        == []
    )
    assert (
        len(
            projection.read_outcomes(core, **{**args, "as_of": 2, "limit": 2})[
                "outcomes"
            ]
        )
        == 2
    )
    with pytest.raises(ValueError, match="generation_mismatch"):
        projection.read_changes(core, generation=str(uuid4()), after=0)
    with pytest.raises(ValueError, match="cursor_ahead"):
        projection.read_changes(core, generation=state.generation, after=3)


def test_competing_projectors_do_not_duplicate_changes(core):
    for i in range(8):
        ingest(core, event("completed", seq=i + 1, task_id=str(uuid4())))
    path = core.execute("PRAGMA database_list").fetchone()[2]

    def run():
        with closing(sqlite3.connect(path)) as db:
            for _ in range(9):
                state = projection.project_batch(db, limit=1)
                if state.ingest_cursor == 8:
                    return state
            raise AssertionError("projector failed to catch up")

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda _: run(), range(3)))
    assert len(set(results)) == 1
    assert results[0].change_cursor == 8
    assert core.execute("SELECT count(*) FROM trace_projected_tasks").fetchone()[0] == 8
    assert (
        core.execute("SELECT count(*) FROM trace_projection_changes").fetchone()[0] == 8
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_persisted_selection_matches_reducer_at_each_arrival(core, reverse):
    values = [
        event("offered", seq=2),
        event("queued", seq=3),
        event("running", seq=1, source_epoch=str(uuid4())),
        event("expired", node_id="sender", attributes={"source_role": "sender"}),
        event("completed", seq=4),
        event("failed", seq=5),
    ]
    if reverse:
        values.reverse()
    observed = []
    for value in values:
        seq = ingest(core, value).ingest_seq
        observed.append(TaskObservation(seq, value))
        projection.project_batch(core)
        expected = reduce_task(observed)
        actual = snapshot(core, value)
        assert actual["node"] == expected.node
        assert actual["ambiguous_live_state"] == expected.ambiguous_live_state


@pytest.mark.parametrize("boundary", ["before_commit", "after_commit"])
def test_owned_process_kill_at_projection_commit(core, boundary):
    value = event("completed")
    ingest(core, value)
    ingest(core, event("finished", kind="model", seq=2))
    ingest(core, event("observed", kind="link", seq=3))
    path = core.execute("PRAGMA database_list").fetchone()[2]
    program = """
import sqlite3,sys,time
from aggregator.trace_projection_store import project_batch
db=sqlite3.connect(sys.argv[1])
def pause():
    print("ready",flush=True)
    while True: time.sleep(1)
if sys.argv[2]=="before_commit":
    db.create_function("owned_pause",0,pause)
    db.execute("CREATE TEMP TRIGGER owned_fault BEFORE UPDATE ON trace_projection_state BEGIN SELECT owned_pause(); END")
project_batch(db)
pause()
"""
    with subprocess.Popen(
        [sys.executable, "-c", program, path, boundary],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            ready, _, _ = select.select([child.stdout], [], [], 20)
            assert ready, "owned child did not reach commit boundary"
            assert child.stdout.readline().strip() == "ready"
        finally:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
    current = snapshot(core, value)
    if boundary == "before_commit":
        assert current["node"] is None
        assert current["state"].change_cursor == current["state"].ingest_cursor == 0
        assert projection.read_graph(core, trace_id=value["trace_id"])["nodes"] == []
    else:
        assert current["node"]["state"] == "completed"
        assert current["state"].change_cursor == current["state"].ingest_cursor == 3
    final = projection.project_batch(core)
    assert final.change_cursor == final.ingest_cursor == 3
    assert any(
        node["kind"] == "model"
        for node in projection.read_graph(core, trace_id=value["trace_id"])["nodes"]
    )
    assert core.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
