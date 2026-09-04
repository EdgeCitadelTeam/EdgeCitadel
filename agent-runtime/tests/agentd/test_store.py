from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

import pytest

import edgecitadel_agentd.store as store_module
from edgecitadel_agentd.store import AgentdStore, StoreError


@pytest.fixture
def store(tmp_path: Path) -> AgentdStore:
    value = AgentdStore(tmp_path / "private" / "agentd.sqlite3")
    yield value
    value.close()


def register(store: AgentdStore, connector_id: str = "pi-local") -> str:
    return store.register_connector(
        connector_id=connector_id,
        host_type="pi",
        agent_id=f"edge-one-{connector_id}",
        capabilities=["delegate", "inbox", "trace"],
    )


def test_store_initializes_private_wal_database(store: AgentdStore) -> None:
    assert store.path.stat().st_mode & 0o777 == 0o600
    assert store.path.parent.stat().st_mode & 0o777 == 0o700
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_version_five_database_migrates_context_id_atomically(tmp_path: Path) -> None:
    path = tmp_path / "private" / "agentd.sqlite3"
    original = AgentdStore(path)
    original.close()
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE tasks DROP COLUMN context_id")
        connection.execute("PRAGMA user_version=5")

    migrated = AgentdStore(path)
    try:
        with sqlite3.connect(path) as connection:
            columns = {row[1] for row in connection.execute("PRAGMA table_info(tasks)")}
            assert connection.execute("PRAGMA user_version").fetchone()[0] == 6
        assert "context_id" in columns
    finally:
        migrated.close()


def test_connector_authentication_session_and_presence(store: AgentdStore) -> None:
    token = register(store)
    session = store.open_session(connector_id="pi-local", token=token)
    assert session["session_id"]
    assert store.health()["active_sessions"] == 1

    store.close_session(
        connector_id="pi-local",
        token=token,
        session_id=str(session["session_id"]),
    )
    assert store.health()["active_sessions"] == 0


def test_agent_discovery_combines_local_and_nats_observed_presence(
    store: AgentdStore,
) -> None:
    token = register(store)
    store.open_session(connector_id="pi-local", token=token)
    store.observe_presence(
        agent_id="remote-gemma",
        state="online",
        reason="heartbeat",
    )

    agents = {str(agent["agent_id"]): agent for agent in store.list_agents()}

    assert agents["edge-one-pi-local"]["local"] is True
    assert agents["edge-one-pi-local"]["state"] == "online"
    assert agents["remote-gemma"]["state"] == "online"
    assert agents["remote-gemma"]["reason"] == "heartbeat"
    assert agents["remote-gemma"]["source"] == "nats-observed"
    assert agents["remote-gemma"]["local"] is False
    assert agents["remote-gemma"]["capabilities"] == []
    assert isinstance(agents["remote-gemma"]["observed_at_ms"], int)


def test_wrong_and_revoked_connector_credentials_are_denied(
    store: AgentdStore,
) -> None:
    token = register(store)
    with pytest.raises(StoreError, match="authentication failed"):
        store.open_session(connector_id="pi-local", token="wrong")
    store.revoke_connector("pi-local")
    with pytest.raises(StoreError, match="authentication failed"):
        store.open_session(connector_id="pi-local", token=token)


def test_only_managed_connector_can_be_explicitly_reissued(
    store: AgentdStore,
) -> None:
    old_token = store.register_connector(
        connector_id="managed-gemma-1",
        host_type="managed-agent",
        agent_id="gemma-1",
        capabilities=["reason"],
    )
    store.revoke_connector("managed-gemma-1")

    new_token = store.reissue_managed_connector("managed-gemma-1", "gemma-1")

    assert new_token != old_token
    store.authenticate("managed-gemma-1", new_token)
    with pytest.raises(StoreError, match="authentication failed"):
        store.authenticate("managed-gemma-1", old_token)
    native_token = register(store)
    store.revoke_connector("pi-local")
    with pytest.raises(StoreError, match="cannot be reissued"):
        store.reissue_managed_connector("pi-local", "edge-one-pi-local")
    assert native_token


def test_session_lease_expiry_marks_connector_unavailable(store: AgentdStore) -> None:
    token = register(store)
    session = store.open_session(connector_id="pi-local", token=token, lease_seconds=10)
    outcome = store.reconcile(int(session["lease_expires_at_ms"]))
    assert outcome == {"expired_sessions": 1, "expired_tasks": 0}
    assert store.health()["active_sessions"] == 0


def test_task_transitions_are_authorized_and_terminally_idempotent(
    store: AgentdStore,
) -> None:
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="edge-one-codex",
        skill_id="review",
        payload={"request": "review the diff"},
    )
    task_id = str(task["task_id"])
    store.transition_task(
        task_id=task_id, state="offered", actor_id="edgecitadel-system"
    )
    with pytest.raises(StoreError, match="only the recipient"):
        store.transition_task(task_id=task_id, state="accepted", actor_id="sender")
    token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=["inbox"],
    )
    session = store.open_session(connector_id="codex-local", token=token)
    session_id = str(session["session_id"])
    store.transition_task(
        task_id=task_id,
        state="accepted",
        actor_id="edge-one-codex",
        session_id=session_id,
    )
    store.transition_task(
        task_id=task_id,
        state="running",
        actor_id="edge-one-codex",
        session_id=session_id,
    )
    completed = store.transition_task(
        task_id=task_id,
        state="completed",
        actor_id="edge-one-codex",
        session_id=session_id,
    )
    replay = store.transition_task(
        task_id=task_id,
        state="completed",
        actor_id="edge-one-codex",
        session_id=session_id,
    )
    assert completed["state"] == replay["state"] == "completed"
    with pytest.raises(StoreError, match="illegal task transition"):
        store.transition_task(
            task_id=task_id,
            state="failed",
            actor_id="edge-one-codex",
            session_id=session_id,
        )


def test_task_claim_is_atomic_across_sessions(store: AgentdStore) -> None:
    token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=["inbox"],
    )
    first = store.open_session(connector_id="codex-local", token=token)
    second = store.open_session(connector_id="codex-local", token=token)
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="edge-one-codex",
        payload={"request": "review"},
    )

    claimed = store.claim_next_task(
        connector_id="codex-local",
        token=token,
        session_id=str(first["session_id"]),
    )
    assert claimed is not None
    assert claimed["task_id"] == task["task_id"]
    assert claimed["state"] == "accepted"
    assert (
        store.claim_next_task(
            connector_id="codex-local",
            token=token,
            session_id=str(second["session_id"]),
        )
        is None
    )
    with pytest.raises(StoreError, match="claimed by another session"):
        store.transition_task(
            task_id=str(task["task_id"]),
            state="running",
            actor_id="edge-one-codex",
            session_id=str(second["session_id"]),
        )


def test_session_loss_requeues_unstarted_work_and_fails_running_work(
    store: AgentdStore,
) -> None:
    token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=["inbox"],
    )
    session = store.open_session(connector_id="codex-local", token=token)
    session_id = str(session["session_id"])
    first = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="edge-one-codex",
        payload={"request": "not started"},
    )
    claimed = store.claim_next_task(
        connector_id="codex-local", token=token, session_id=session_id
    )
    assert claimed is not None and claimed["task_id"] == first["task_id"]
    store.close_session(connector_id="codex-local", token=token, session_id=session_id)
    assert store.get_task(str(first["task_id"]))["state"] == "queued"

    second_session = store.open_session(connector_id="codex-local", token=token)
    second_session_id = str(second_session["session_id"])
    claimed = store.claim_next_task(
        connector_id="codex-local", token=token, session_id=second_session_id
    )
    assert claimed is not None
    store.transition_task(
        task_id=str(claimed["task_id"]),
        state="running",
        actor_id="edge-one-codex",
        session_id=second_session_id,
    )
    store.close_session(
        connector_id="codex-local", token=token, session_id=second_session_id
    )
    failed = store.get_task(str(first["task_id"]))
    assert failed["state"] == "failed"
    assert failed["terminal_reason"] == "executor_session_lost"
    assert failed["result"] == {
        "error": "executor_session_lost",
        "retry_safe": False,
    }


def test_deadline_reconciliation_replaces_watchdog_terminal_behavior(
    store: AgentdStore,
) -> None:
    deadline = int(time.time() * 1000) + 1000
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="offline-agent",
        payload={"request": "work"},
        deadline_at_ms=deadline,
    )
    outcome = store.reconcile(deadline)
    assert outcome == {"expired_sessions": 0, "expired_tasks": 1}
    current = store.get_task(str(task["task_id"]))
    assert current["state"] == "expired"
    assert current["terminal_reason"] == "deadline_exceeded"


@pytest.mark.parametrize(
    "metadata",
    [
        {"token": "canary"},
        {"nested": {"prompt": "canary"}},
        {"items": [{"tool_input": "canary"}]},
    ],
)
def test_trace_metadata_rejects_sensitive_content(
    store: AgentdStore, metadata: dict[str, object]
) -> None:
    with pytest.raises(StoreError, match="sensitive field"):
        store.append_event(
            event_type="native.test",
            agent_id="edge-one-pi",
            task_id=None,
            trace_id="trace-one",
            attributes=metadata,
        )


def test_trace_query_and_purge(store: AgentdStore) -> None:
    span_id = store.record_span(
        trace_id="trace-one",
        operation="edgecitadel.delegate",
        status="ok",
        agent_id="edge-one-pi",
        attributes={"recipient_id": "edge-one-codex"},
    )
    store.append_event(
        event_type="task.queued",
        agent_id="edge-one-pi",
        task_id="10000000-0000-4000-8000-000000000001",
        trace_id="trace-one",
        attributes={"state": "queued"},
    )
    trace = store.get_trace("trace-one")
    assert trace["spans"][0]["span_id"] == span_id
    assert trace["events"][0]["attributes"] == {"state": "queued"}
    assert store.list_traces()[0]["trace_id"] == "trace-one"
    assert store.purge_telemetry() == {"spans": 1, "events": 1, "presence": 0}


def test_remote_task_queues_one_idempotent_transport_message(
    store: AgentdStore,
) -> None:
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="remote-agent",
        payload={"request": "work"},
        task_id="10000000-0000-4000-8000-000000000001",
        context_id="30000000-0000-4000-8000-000000000001",
    )
    pending = store.pending_transport()
    assert len(pending) == 1
    assert pending[0]["task_id"] == task["task_id"]
    assert pending[0]["envelope"]["task_id"] == "10000000-0000-4000-8000-000000000001"
    assert (
        pending[0]["envelope"]["context_id"] == "30000000-0000-4000-8000-000000000001"
    )

    store.mark_transport_published(str(pending[0]["message_id"]))
    assert store.pending_transport() == []


def test_task_content_is_encrypted_at_rest(store: AgentdStore) -> None:
    canary = "secret-prompt-canary-92d08b"
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="remote-agent",
        payload={"request": canary},
    )
    assert task["payload"] == {"request": canary}
    store.transition_task(
        task_id=str(task["task_id"]),
        state="undeliverable",
        actor_id="edgecitadel-system",
        result={"body": canary},
    )
    store._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert canary.encode() not in store.path.read_bytes()
    assert canary.encode() not in (store.path.parent / "payload.key").read_bytes()


def test_encrypted_database_refuses_to_start_without_matching_payload_key(
    tmp_path: Path,
) -> None:
    path = tmp_path / "private" / "agentd.sqlite3"
    first = AgentdStore(path)
    first.create_task(
        sender_id="edge-one-pi",
        recipient_id="edge-one-codex",
        payload={"body": "private"},
    )
    first.close()
    (path.parent / "payload.key").unlink()

    with pytest.raises(StoreError, match="restore agentd.sqlite3 and payload.key"):
        AgentdStore(path)
    assert not (path.parent / "payload.key").exists()


def test_unchanged_connector_reconciliation_does_not_emit_audit_noise(
    store: AgentdStore,
) -> None:
    capabilities = ["edgecitadel_inbox", "edgecitadel_trace"]
    token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=capabilities,
    )

    store.configure_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=capabilities,
    )
    store.update_connector(
        connector_id="codex-local",
        token=token,
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=capabilities,
    )

    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM events WHERE event_type IN "
                "('connector.metadata_updated', "
                "'connector.capabilities_reconciled')"
            ).fetchone()[0]
            == 0
        )


def test_schema_migration_rolls_back_all_statements_on_failure(tmp_path: Path) -> None:
    path = tmp_path / "private" / "agentd.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE connectors (connector_id TEXT PRIMARY KEY)")
        connection.execute("PRAGMA user_version=1")

    with pytest.raises(sqlite3.OperationalError):
        AgentdStore(path)

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert "managed_agents" not in tables
    assert "transport_outbox" not in tables


def test_conflicting_terminal_result_is_rejected_and_audited(
    store: AgentdStore,
) -> None:
    sender_token = register(store, "pi-local")
    sender_session = store.open_session(connector_id="pi-local", token=sender_token)
    recipient_token = store.register_connector(
        connector_id="codex-local",
        host_type="codex",
        agent_id="edge-one-codex",
        capabilities=["inbox"],
    )
    recipient_session = store.open_session(
        connector_id="codex-local", token=recipient_token
    )
    task = store.create_task(
        sender_id="edge-one-pi-local",
        recipient_id="edge-one-codex",
        payload={"body": "work"},
    )
    task_id = str(task["task_id"])
    claimed = store.claim_next_task(
        connector_id="codex-local",
        token=recipient_token,
        session_id=str(recipient_session["session_id"]),
    )
    assert claimed is not None
    store.transition_task(
        task_id=task_id,
        state="running",
        actor_id="edge-one-codex",
        session_id=str(recipient_session["session_id"]),
    )
    store.transition_task(
        task_id=task_id,
        state="completed",
        actor_id="edge-one-codex",
        session_id=str(recipient_session["session_id"]),
        result={"body": "first"},
    )

    with pytest.raises(StoreError, match="conflicting terminal task result"):
        store.transition_task(
            task_id=task_id,
            state="completed",
            actor_id="edge-one-codex",
            session_id=str(recipient_session["session_id"]),
            result={"body": "second"},
        )

    assert store.get_task(task_id)["result"] == {"body": "first"}
    with sqlite3.connect(store.path) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = 'task.result_conflict'"
            ).fetchone()[0]
            == 1
        )
    store.close_session(
        connector_id="pi-local",
        token=sender_token,
        session_id=str(sender_session["session_id"]),
    )


def test_transport_rejects_wrong_actor_and_conflicting_late_result(
    store: AgentdStore,
) -> None:
    task = store.create_task(
        sender_id="edge-one-pi",
        recipient_id="remote-agent",
        payload={"body": "work"},
        task_id="10000000-0000-4000-8000-000000000001",
    )
    task_id = str(task["task_id"])
    store.transition_task(
        task_id=task_id, state="offered", actor_id="edgecitadel-system"
    )
    store.transition_task(task_id=task_id, state="accepted", actor_id="remote-agent")
    store.transition_task(task_id=task_id, state="running", actor_id="remote-agent")
    store.transition_task(
        task_id=task_id,
        state="completed",
        actor_id="remote-agent",
        result={"body": "done"},
        queue_transport=False,
    )
    base_result = {
        "v": 1,
        "id": "20000000-0000-4000-8000-000000000002",
        "type": "result",
        "sender_id": "remote-agent",
        "recipient_id": "edge-one-pi",
        "task_id": task_id,
        "task_state": "completed",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "payload": {"body": "done"},
    }

    wrong_actor = {**base_result, "sender_id": "other-agent"}
    with pytest.raises(StoreError, match="only the recipient"):
        store.ingest_transport_envelope(wrong_actor)

    conflicting = {
        **base_result,
        "id": "20000000-0000-4000-8000-000000000003",
        "task_state": "failed",
        "payload": {"error": "late failure"},
    }
    with pytest.raises(StoreError, match="conflicting terminal task result"):
        store.ingest_transport_envelope(conflicting)

    assert store.get_task(task_id)["state"] == "completed"
    assert store.get_task(task_id)["result"] == {"body": "done"}


def test_transport_redelivery_does_not_duplicate_logical_task(
    store: AgentdStore,
) -> None:
    envelope = {
        "v": 1,
        "id": "20000000-0000-4000-8000-000000000002",
        "type": "command",
        "sender_id": "remote-agent",
        "recipient_id": "edge-one-pi",
        "task_id": "10000000-0000-4000-8000-000000000001",
        "context_id": "30000000-0000-4000-8000-000000000001",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "payload": {
            "request": "work",
            "trace_id": "10000000000040008000000000000001",
        },
    }
    first = store.ingest_transport_envelope(envelope)
    second = store.ingest_transport_envelope(envelope)

    assert (
        first["task_id"] == second["task_id"] == "10000000-0000-4000-8000-000000000001"
    )
    assert second["state"] == "offered"
    assert second["context_id"] == "30000000-0000-4000-8000-000000000001"
    assert len(store.list_tasks()) == 1


def test_system_max_delivery_result_marks_sender_task_undeliverable(
    store: AgentdStore,
) -> None:
    task_id = "10000000-0000-4000-8000-000000000001"
    store.create_task(
        sender_id="edge-one-pi",
        recipient_id="remote-agent",
        payload={"body": "work"},
        task_id=task_id,
    )

    result = store.ingest_transport_envelope(
        {
            "v": 1,
            "id": "20000000-0000-4000-8000-000000000002",
            "type": "result",
            "sender_id": "edgecitadel-system",
            "recipient_id": "edge-one-pi",
            "task_id": task_id,
            "task_state": "failed",
            "timestamp": "2026-01-01T00:00:00.000Z",
            "payload": {
                "error": "recipient_unavailable",
                "recipient_id": "remote-agent",
                "trigger": "max_deliveries",
            },
        }
    )

    assert result["state"] == "undeliverable"
    assert result["terminal_reason"] == "recipient_unavailable"


def test_system_result_cannot_mark_a_different_recipient_undeliverable(
    store: AgentdStore,
) -> None:
    task_id = "10000000-0000-4000-8000-000000000001"
    store.create_task(
        sender_id="edge-one-pi",
        recipient_id="remote-agent",
        payload={"body": "work"},
        task_id=task_id,
    )
    envelope = {
        "v": 1,
        "id": "20000000-0000-4000-8000-000000000002",
        "type": "result",
        "sender_id": "edgecitadel-system",
        "recipient_id": "edge-one-pi",
        "task_id": task_id,
        "task_state": "failed",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "payload": {
            "error": "recipient_unavailable",
            "recipient_id": "other-agent",
            "trigger": "max_deliveries",
        },
    }

    with pytest.raises(StoreError, match="only the recipient"):
        store.ingest_transport_envelope(envelope)


def test_transport_rejects_conflicting_duplicate_task_id(
    store: AgentdStore,
) -> None:
    envelope = {
        "v": 1,
        "id": "20000000-0000-4000-8000-000000000002",
        "type": "command",
        "sender_id": "remote-agent",
        "recipient_id": "edge-one-pi",
        "task_id": "10000000-0000-4000-8000-000000000001",
        "timestamp": "2026-01-01T00:00:00.000Z",
        "payload": {
            "request": "work",
            "trace_id": "10000000000040008000000000000001",
        },
    }
    store.ingest_transport_envelope(envelope)
    conflicting = {
        **envelope,
        "id": "20000000-0000-4000-8000-000000000003",
        "payload": {
            "request": "different work",
            "trace_id": "10000000000040008000000000000001",
        },
    }

    with pytest.raises(StoreError, match="conflicting duplicate task envelope"):
        store.ingest_transport_envelope(conflicting)

    assert store.get_task(str(envelope["task_id"]))["payload"] == envelope["payload"]
    with sqlite3.connect(store.path) as connection:
        row = connection.execute(
            "SELECT attributes_json FROM events "
            "WHERE event_type = 'task.duplicate_conflict'"
        ).fetchone()
    assert row is not None
    assert json.loads(row[0]) == {"conflicting_fields": ["payload"]}


def test_transport_rejects_invalid_envelope_before_persistence(
    store: AgentdStore,
) -> None:
    with pytest.raises(StoreError, match="transport envelope is invalid"):
        store.ingest_transport_envelope(
            {
                "v": 1,
                "id": "not-a-uuid",
                "type": "command",
                "sender_id": "remote-agent",
                "recipient_id": "edge-one-pi",
                "task_id": "10000000-0000-4000-8000-000000000001",
                "timestamp": "2026-01-01T00:00:00.000Z",
                "payload": {"request": "work"},
            }
        )
    assert store.list_tasks() == []


def test_reconcile_applies_bounded_metadata_retention(store: AgentdStore) -> None:
    store.record_span(
        trace_id="a" * 32,
        operation="old-operation",
        status="ok",
        started_at_ms=1,
        ended_at_ms=2,
    )

    store.reconcile(now_ms=31 * 24 * 60 * 60 * 1000)

    with pytest.raises(StoreError, match="trace was not found"):
        store.get_trace("a" * 32)


def test_reconcile_applies_incremental_record_caps(
    store: AgentdStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store_module, "MAX_EVENT_RECORDS", 2)
    monkeypatch.setattr(store_module, "MAX_SPAN_RECORDS", 2)
    monkeypatch.setattr(store_module, "RETENTION_DELETE_BATCH", 2)
    now_ms = int(time.time() * 1000)
    for index in range(4):
        trace_id = f"{index:032x}"
        store.append_event(
            event_type="native.test",
            agent_id=None,
            task_id=None,
            trace_id=trace_id,
            attributes={"index": index},
        )
        store.record_span(
            trace_id=trace_id,
            operation="test",
            status="ok",
            started_at_ms=now_ms + index,
        )

    store.reconcile()

    health = store.health()
    records = health["telemetry_records"]
    assert isinstance(records, dict)
    assert records["events"] == 2
    assert records["spans"] == 2
    assert isinstance(health["database_bytes"], int)
    assert health["database_bytes"] > 0
