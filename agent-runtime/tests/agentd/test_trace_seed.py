"""Existing-work reservation is atomic and preserves authoritative facts."""

import os
import sqlite3

import pytest

from edgecitadel_agentd import storage_workspace, trace_reservations, trace_seed
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_agentd.trace_contract import TraceContractError
import test_trace_task_completion as task_tests

installed = task_tests.installed


@pytest.fixture
def existing(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_workspace, "WORKSPACE_BYTES", 256 * 1024)
    if not hasattr(os, "posix_fallocate"):
        monkeypatch.setattr(
            os,
            "posix_fallocate",
            lambda fd, offset, size: os.pwrite(fd, b"\0" * size, offset),
            raising=False,
        )
    (tmp_path / "node.json").write_text('{"agent_id":"owned-edge"}')
    store = AgentdStore(tmp_path / "agentd/agentd.sqlite3")
    token = store.register_connector(
        connector_id="native", host_type="codex", agent_id="worker", capabilities=[]
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    accepted = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    queued = store.create_task(sender_id="origin", recipient_id="remote", payload={})
    try:
        yield store, token, session, accepted, queued
    finally:
        store.close()


def install(store):
    store._connection.install_workspace(
        storage_workspace.CompletionWorkspace(store.path.parent / "completion.reserve")
    )


def facts(store):
    result = task_tests.snapshot(store)
    result.pop("trace_completion_slots")
    for name in ("connectors", "sessions", "presence_history_all"):
        result[name] = [
            tuple(row)
            for row in store._connection.execute(f"SELECT * FROM {name} ORDER BY 1")
        ]
    return result


def test_existing_accepted_and_queued_work_seed_once_then_recover(existing):
    store, token, session, accepted, queued = existing
    before = facts(store)
    install(store)
    result = trace_seed.seed_existing_work(store)
    assert result == {
        "required_pending": 8,
        "newly_reserved": 8,
        "occupied": 8,
        "completed": 0,
    }
    assert facts(store) == before
    assert trace_seed.seed_existing_work(store) == {**result, "newly_reserved": 0}
    store.close_session(connector_id="native", token=token, session_id=session)
    assert store.get_task(accepted["task_id"])["state"] == "queued"
    assert store.get_task(queued["task_id"])["state"] == "queued"
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_completion_slots WHERE filled=1"
        ).fetchone()[0]
        == 2
    )
    # Requeued old work needs first-cycle admission records; its immutable
    # attempt recovery remains occupied and cannot be reassigned.
    completed = task_tests.snapshot(store)["trace_completion_slots"]
    trace_seed.seed_existing_work(store)
    after = task_tests.snapshot(store)["trace_completion_slots"]
    assert [row for row in after if row[4]] == [row for row in completed if row[4]]


@pytest.mark.parametrize("failure", ["capacity", "allocation", "midway"])
def test_seed_failure_rolls_back_every_reservation_and_fact(
    existing, monkeypatch, failure
):
    store, *_ = existing
    install(store)
    db = store._connection
    before = task_tests.snapshot(store)
    before_facts = facts(store)
    if failure == "capacity":
        monkeypatch.setattr(trace_reservations, "MAX_SLOTS", 7)
        error, message = TraceContractError, "quota_exceeded"
    elif failure == "allocation":
        db.execute(
            f"PRAGMA max_page_count={db.execute('PRAGMA page_count').fetchone()[0]}"
        )
        error, message = sqlite3.OperationalError, "full"
    else:
        original = trace_seed.reserve
        calls = 0

        def failing(db, obligation):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise RuntimeError("owned seed failure")
            return original(db, obligation)

        monkeypatch.setattr(trace_seed, "reserve", failing)
        error, message = RuntimeError, "owned seed failure"
    with pytest.raises(error, match=message):
        trace_seed.seed_existing_work(store)
    assert task_tests.snapshot(store) == before
    assert facts(store) == before_facts
    assert not db.in_transaction
    assert not db.workspace.borrowed


def test_seed_requires_owned_pre_admission_boundary(existing):
    store, *_ = existing
    with pytest.raises(TraceContractError, match="invalid_reservation_seed_boundary"):
        trace_seed.seed_existing_work(store)
    install(store)
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(
            TraceContractError, match="invalid_reservation_seed_boundary"
        ):
            trace_seed.seed_existing_work(store)


def test_filled_first_cycle_records_and_terminal_work_are_preserved(installed):
    store, *_ = installed
    task = task_tests.running(installed)
    before = task_tests.snapshot(store)
    assert trace_seed.seed_existing_work(store) == {
        "required_pending": 3,
        "newly_reserved": 0,
        "occupied": 6,
        "completed": 3,
    }
    assert task_tests.snapshot(store) == before
    task_tests.complete(installed, task)
    before = task_tests.snapshot(store)
    assert trace_seed.seed_existing_work(store) == {
        "required_pending": 2,
        "newly_reserved": 0,
        "occupied": 6,
        "completed": 4,
    }
    assert task_tests.snapshot(store) == before


def test_seed_survives_pair_reopen_and_running_task_completion(existing):
    store, _, session, accepted, _ = existing
    store.transition_task(
        task_id=accepted["task_id"],
        state="running",
        actor_id="worker",
        session_id=session,
    )
    before = facts(store)
    path = store.path
    store.close()
    reopened = AgentdStore(path)
    try:
        install(reopened)
        assert trace_seed.seed_existing_work(reopened)["newly_reserved"] == 7
        assert facts(reopened) == before
        reopened.transition_task(
            task_id=accepted["task_id"],
            state="completed",
            actor_id="worker",
            session_id=session,
            result={},
        )
        assert reopened.get_task(accepted["task_id"])["state"] == "completed"
    finally:
        reopened.close()


def test_seed_reserves_existing_run_and_operation(tmp_path, monkeypatch):
    import test_trace_terminal as terminal_tests
    from test_trace_append_store import append, request

    monkeypatch.setattr(storage_workspace, "WORKSPACE_BYTES", 256 * 1024)
    if not hasattr(os, "posix_fallocate"):
        monkeypatch.setattr(
            os,
            "posix_fallocate",
            lambda fd, offset, size: os.pwrite(fd, b"\0" * size, offset),
            raising=False,
        )
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    try:
        token = store.register_connector(
            connector_id="native",
            host_type="codex",
            agent_id="agent-a",
            capabilities=["edgecitadel_trace"],
        )
        session = store.open_session(connector_id="native", token=token)["session_id"]
        binding = terminal_tests.bind((store, token, session))
        append(store, token, request(binding))
        before = terminal_tests.logical(store._connection)
        install(store)
        assert trace_seed.seed_existing_work(store)["newly_reserved"] == 4
        assert terminal_tests.logical(store._connection) == before
        store.close_session(connector_id="native", token=token, session_id=session)
        assert (
            store._connection.execute(
                "SELECT phase FROM trace_operations_all"
            ).fetchone()[0]
            == "interrupted"
        )
        assert (
            store._connection.execute(
                "SELECT closed_at_ms FROM trace_bindings_all"
            ).fetchone()[0]
            is not None
        )
    finally:
        store.close()
