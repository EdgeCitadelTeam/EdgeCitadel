import json
import sqlite3
import time
from contextlib import closing

import pytest

from edgecitadel_agentd.store import AgentdStore


@pytest.fixture
def configured(tmp_path):
    (tmp_path / "node.json").write_text('{"agent_id":"owned-edge"}')
    with closing(AgentdStore(tmp_path / "agentd/agentd.sqlite3")) as store:
        token = store.register_connector(
            connector_id="native",
            host_type="codex",
            agent_id="worker",
            capabilities=["edgecitadel_trace"],
        )
        session = store.open_session(connector_id="native", token=token)["session_id"]
        yield store, token, session


def events(store):
    return [
        json.loads(row[0])
        for row in store._connection.execute(
            "SELECT event_json FROM trace_journal ORDER BY source_seq"
        )
    ]


def test_lifecycle_boundaries_are_durable_and_sanitized(configured):
    store, token, session = configured
    task = store.create_task(
        sender_id="origin", recipient_id="worker", payload={"secret": "private-body"}
    )
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    for state in ("running", "completed"):
        store.transition_task(
            task_id=task["task_id"],
            state=state,
            actor_id="worker",
            session_id=session,
            reason="private-reason",
            result={"body": "private-result"} if state == "completed" else None,
        )
    before = events(store)
    assert [e["phase"] for e in before] == [
        "queued",
        "offered",
        "accepted",
        "running",
        "completed",
    ]
    assert before[-1]["attributes"] == {"source_role": "recipient", "reason": "unknown"}
    assert all("private-" not in json.dumps(e) for e in before)
    assert (
        store._connection.execute("SELECT COUNT(*) FROM trace_spool").fetchone()[0] == 5
    )
    store.transition_task(
        task_id=task["task_id"],
        state="completed",
        actor_id="worker",
        session_id=session,
        result={"body": "private-result"},
    )
    assert events(store) == before
    with closing(AgentdStore(store.path)) as restarted:
        assert events(restarted) == before


@pytest.mark.parametrize("state,phase", [("accepted", "queued"), ("running", "failed")])
def test_session_recovery_records_actual_state(configured, state, phase):
    store, token, session = configured
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    if state == "running":
        store.transition_task(
            task_id=task["task_id"], state=state, actor_id="worker", session_id=session
        )
    store.close_session(connector_id="native", token=token, session_id=session)
    assert events(store)[-1]["phase"] == phase
    assert events(store)[-1]["attributes"]["source_role"] == "daemon"
    assert store.get_task(task["task_id"])["state"] == phase


def test_deadline_expiry_records_daemon_boundary(configured):
    store, _, _ = configured
    deadline = time.time_ns() // 1_000_000 + 1000
    task = store.create_task(
        sender_id="origin", recipient_id="remote", payload={}, deadline_at_ms=deadline
    )
    store.reconcile(now_ms=deadline + 1)
    assert store.get_task(task["task_id"])["state"] == "expired"
    assert events(store)[-1]["attributes"] == {
        "source_role": "daemon",
        "reason": "deadline_expired",
    }


def test_spool_failure_rolls_back_state_claim_and_legacy_events(configured):
    store, token, session = configured
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    tables = (
        "tasks",
        "events",
        "task_attempts",
        "trace_journal",
        "trace_spool",
        "trace_sources",
        "trace_export_generations",
    )

    def snapshot():
        return {
            name: [
                tuple(row) for row in store._connection.execute(f"SELECT * FROM {name}")
            ]
            for name in tables
        }

    before = snapshot()
    store._connection.execute(
        "CREATE TRIGGER owned_fail BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT, 'owned spool failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned spool failure"):
        store.claim_next_task(connector_id="native", token=token, session_id=session)
    assert snapshot() == before
    assert store.get_task(task["task_id"])["state"] == "queued"


def test_remote_result_preserves_synthesized_provenance(configured):
    from uuid import uuid4

    store, _, _ = configured
    task = store.create_task(sender_id="worker", recipient_id="remote", payload={})
    store.ingest_transport_envelope(
        {
            "v": 1,
            "id": str(uuid4()),
            "type": "result",
            "task_id": task["task_id"],
            "sender_id": "remote",
            "recipient_id": "worker",
            "task_state": "completed",
            "timestamp": "2026-09-16T12:00:00.000Z",
            "payload": {"body": "private-result"},
        }
    )
    observed = events(store)
    assert [e["phase"] for e in observed] == [
        "queued",
        "offered",
        "accepted",
        "running",
        "completed",
    ]
    assert all(e["evidence_kind"] == "compatibility_synthesized" for e in observed[1:4])
    assert observed[-1]["evidence_kind"] == "integration_reported"
    assert all(e["attributes"]["source_role"] == "sender" for e in observed)
    assert "private-result" not in json.dumps(observed)


def test_incoming_command_offer_is_locally_observed(configured):
    from uuid import uuid4

    store, _, _ = configured
    store.ingest_transport_envelope(
        {
            "v": 1,
            "id": str(uuid4()),
            "type": "command",
            "task_id": str(uuid4()),
            "sender_id": "remote",
            "recipient_id": "worker",
            "timestamp": "2026-09-16T12:00:00.000Z",
            "payload": {"body": "private-request"},
        }
    )
    observed = events(store)
    assert [e["phase"] for e in observed] == ["queued", "offered"]
    assert observed[0]["attributes"]["source_role"] == "recipient"
    assert observed[-1]["attributes"]["source_role"] == "daemon"
    assert all(e["evidence_kind"] == "source_observed" for e in observed)


def test_generic_event_cannot_forge_task_lifecycle(configured):
    store, _, _ = configured
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    before = events(store)
    store.append_event(
        event_type="task.completed",
        agent_id="intruder",
        task_id=task["task_id"],
        trace_id=task["trace_id"],
        attributes={},
    )
    assert events(store) == before
    assert store.get_task(task["task_id"])["state"] == "queued"


@pytest.mark.parametrize(
    "terminal,actor,role",
    [
        ("completed", "worker", "recipient"),
        ("failed", "worker", "recipient"),
        ("rejected", "worker", "recipient"),
        ("cancelled", "worker", "recipient"),
        ("cancelled", "origin", "sender"),
        ("expired", "edgecitadel-system", "daemon"),
        ("undeliverable", "edgecitadel-system", "daemon"),
    ],
)
def test_colocated_terminal_attribution(configured, terminal, actor, role):
    store, token, session = configured
    store.register_connector(
        connector_id="origin-connector",
        host_type="codex",
        agent_id="origin",
        capabilities=[],
    )
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    if terminal != "undeliverable":
        store.claim_next_task(connector_id="native", token=token, session_id=session)
        store.transition_task(
            task_id=task["task_id"],
            state="running",
            actor_id="worker",
            session_id=session,
        )
    store.transition_task(
        task_id=task["task_id"], state=terminal, actor_id=actor, session_id=session
    )
    event = events(store)[-1]
    assert event["phase"] == terminal
    assert event["agent_id"] == actor
    assert event["attributes"]["source_role"] == role
    before = events(store)
    store.transition_task(
        task_id=task["task_id"], state=terminal, actor_id=actor, session_id=session
    )
    assert events(store) == before


@pytest.mark.parametrize("action", ["close", "revoke", "expire"])
def test_session_loss_closes_native_run_once(configured, action):
    from uuid import uuid4

    store, token, session = configured
    bound = store.bind_trace(
        node_id="owned-edge",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": None,
            "context_id": None,
        },
    )["result"]
    store.append_trace(
        node_id="owned-edge",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "binding_id": bound["binding_id"],
            "observation_id": str(uuid4()),
            "observation": {
                "schema_version": 1,
                "kind": "tool",
                "phase": "started",
                "span_id": str(uuid4()),
                "parent_span_id": None,
                "occurred_at": "2026-09-16T12:00:00.000Z",
                "duration_ms": None,
                "attributes": {"name": "owned-tool"},
            },
        },
    )
    if action == "close":
        store.close_session(connector_id="native", token=token, session_id=session)
    elif action == "revoke":
        store.revoke_connector("native")
    else:
        store.reconcile(now_ms=time.time_ns() // 1_000_000 + 60_000)
    observed = events(store)
    assert [(e["kind"], e["phase"]) for e in observed][-2:] == [
        ("tool", "interrupted"),
        ("run", "interrupted"),
    ]
    assert all(e["duration_ms"] is None for e in observed[-2:])
    assert store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
    store.reconcile(now_ms=time.time_ns() // 1_000_000 + 120_000)
    assert events(store) == observed


def test_session_closure_spool_failure_rolls_back_session_and_binding(configured):
    from uuid import uuid4

    store, token, session = configured
    store.bind_trace(
        node_id="owned-edge",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": None,
            "context_id": None,
        },
    )
    before = events(store)
    store._connection.execute(
        "CREATE TRIGGER owned_close_fail BEFORE INSERT ON trace_spool BEGIN SELECT RAISE(ABORT, 'owned closure failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned closure failure"):
        store.close_session(connector_id="native", token=token, session_id=session)
    assert events(store) == before
    assert (
        store._connection.execute(
            "SELECT closed_at_ms FROM sessions WHERE session_id=?", (session,)
        ).fetchone()[0]
        is None
    )
    assert (
        store._connection.execute("SELECT closed_at_ms FROM trace_bindings").fetchone()[
            0
        ]
        is None
    )
    store._connection.execute("DROP TRIGGER owned_close_fail")
    with closing(AgentdStore(store.path)) as restarted:
        restarted.reconcile(now_ms=time.time_ns() // 1_000_000 + 60_000)
        assert events(restarted)[-1]["phase"] == "interrupted"


@pytest.mark.parametrize("running,expected", [(False, "queued"), (True, "failed")])
def test_task_recovery_and_binding_closure_share_transaction(
    configured, running, expected
):
    from uuid import uuid4

    store, token, session = configured
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    bound = store.bind_trace(
        node_id="owned-edge",
        connector_id="native",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": task["task_id"],
            "context_id": None,
        },
    )["result"]
    if running:
        store.transition_task(
            task_id=task["task_id"],
            state="running",
            actor_id="worker",
            session_id=session,
        )
    before = events(store)
    store._connection.execute(
        "CREATE TRIGGER owned_run_close_fail BEFORE INSERT ON trace_journal WHEN json_extract(NEW.event_json,'$.kind')='run' BEGIN SELECT RAISE(ABORT, 'owned run closure failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="owned run closure failure"):
        store.close_session(connector_id="native", token=token, session_id=session)
    assert store.get_task(task["task_id"])["state"] == (
        "running" if running else "accepted"
    )
    assert events(store) == before
    store._connection.execute("DROP TRIGGER owned_run_close_fail")
    store.close_session(connector_id="native", token=token, session_id=session)
    assert store.get_task(task["task_id"])["state"] == expected
    last = events(store)[-1]
    assert (last["kind"], last["phase"]) == ("run", "interrupted")
    assert last["execution_attempt_id"] == bound["execution_attempt_id"]
