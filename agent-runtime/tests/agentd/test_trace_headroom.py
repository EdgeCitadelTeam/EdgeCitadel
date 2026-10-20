"""Ordinary traffic cannot consume positions promised to pending completions."""

import pytest

from edgecitadel_agentd.store import StoreError
from edgecitadel_agentd.trace_contract import TraceContractError
from edgecitadel_agentd.trace_counters import MAX_COUNTER, encode_counter
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_seed import seed_existing_work
import test_trace_task_completion as task_tests
import test_trace_seed as seed_tests
from test_trace_journal import event
from test_trace_local_completion import local_rows

installed = task_tests.installed
existing = seed_tests.existing


@pytest.mark.parametrize("counter", ["source", "export", "both"])
def test_reserved_terminal_keeps_last_position_and_exact_retry(installed, counter):
    store, _, _ = installed
    task = task_tests.running(installed)
    db = store._connection
    with db:
        if counter in {"source", "both"}:
            db.execute(
                "UPDATE trace_sources SET next_source_seq_bytes=?",
                (encode_counter(MAX_COUNTER - 1),),
            )
        if counter in {"export", "both"}:
            db.execute(
                "UPDATE trace_export_generations SET next_export_seq_bytes=?",
                (encode_counter(MAX_COUNTER - 1),),
            )
    before = task_tests.snapshot(store)
    with pytest.raises(TraceContractError, match="trace_sequence_exhausted"), db:
        db.execute("BEGIN IMMEDIATE")
        TraceJournal(db).record("owned-edge", event(), selected=True)
    assert task_tests.snapshot(store) == before
    with pytest.raises(StoreError, match="trace_sequence_exhausted"):
        store.create_task(sender_id="origin", recipient_id="remote", payload={})
    assert task_tests.snapshot(store) == before
    result = task_tests.complete(installed, task)
    after = task_tests.snapshot(store)
    assert task_tests.complete(installed, task) == result
    assert task_tests.snapshot(store) == after
    if counter in {"source", "both"}:
        assert (
            db.execute("SELECT next_source_seq FROM trace_sources").fetchone()[0]
            == MAX_COUNTER
        )
    if counter in {"export", "both"}:
        assert (
            db.execute(
                "SELECT next_export_seq FROM trace_export_generations"
            ).fetchone()[0]
            == MAX_COUNTER
        )


def test_local_event_does_not_spend_export_headroom(installed):
    store, _, _ = installed
    task = task_tests.running(installed)
    db = store._connection
    with db:
        db.execute(
            "UPDATE trace_export_generations SET next_export_seq_bytes=?",
            (encode_counter(MAX_COUNTER - 1),),
        )
        TraceJournal(db).record("owned-edge", event(), selected=False)
    assert (
        db.execute("SELECT next_export_seq FROM trace_export_generations").fetchone()[0]
        == MAX_COUNTER - 1
    )
    task_tests.complete(installed, task)
    assert (
        db.execute("SELECT next_export_seq FROM trace_export_generations").fetchone()[0]
        == MAX_COUNTER
    )


def test_session_admission_and_presence_protect_last_closure_id(installed):
    store, token, session = installed
    db = store._connection
    with db:
        db.execute(
            "UPDATE trace_presence_counter SET next_id=?",
            (encode_counter(MAX_COUNTER - 1),),
        )
    before = local_rows(db)
    with pytest.raises(TraceContractError, match="trace_sequence_exhausted"):
        store.observe_presence(agent_id="worker", state="degraded", reason="ordinary")
    assert local_rows(db) == before
    with pytest.raises(TraceContractError, match="trace_sequence_exhausted"):
        store.open_session(connector_id="native", token=token)
    assert local_rows(db) == before
    store.close_session(connector_id="native", token=token, session_id=session)
    assert (
        db.execute("SELECT max(presence_id) FROM presence_history_all").fetchone()[0]
        == MAX_COUNTER - 1
    )
    assert (
        int(db.execute("SELECT next_id FROM trace_presence_counter").fetchone()[0])
        == MAX_COUNTER
    )


@pytest.mark.parametrize("counter", ["source", "export", "presence"])
def test_seeding_refuses_inadequate_sequence_headroom_atomically(existing, counter):
    store, *_ = existing
    seed_tests.install(store)
    db = store._connection
    with db:
        if counter == "presence":
            db.execute(
                "UPDATE trace_presence_counter SET next_id=?",
                (encode_counter(MAX_COUNTER),),
            )
        else:
            table, column = (
                ("trace_sources", "next_source_seq_bytes")
                if counter == "source"
                else ("trace_export_generations", "next_export_seq_bytes")
            )
            db.execute(
                f"UPDATE {table} SET {column}=?", (encode_counter(MAX_COUNTER - 5),)
            )
    before = task_tests.snapshot(store)
    with pytest.raises(TraceContractError, match="trace_sequence_exhausted"):
        seed_existing_work(store)
    assert task_tests.snapshot(store) == before
    assert not db.workspace.borrowed
