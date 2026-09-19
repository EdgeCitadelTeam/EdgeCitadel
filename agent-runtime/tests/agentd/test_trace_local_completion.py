import sqlite3

import pytest

from edgecitadel_agentd.trace_completed import materialize
from edgecitadel_agentd.trace_counters import encode_counter
import test_trace_task_completion as task_tests

installed = task_tests.installed
running = task_tests.running


def local_rows(db):
    return {
        table: [tuple(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1")]
        for table in (
            "sessions",
            "connectors",
            "tasks",
            "events_all",
            "presence_history_all",
            "trace_journal_all",
            "trace_presence_counter",
            "trace_sources",
            "trace_completion_slots",
        )
    }


def test_presence_completion_has_monotonic_identity_without_canonical_positions(
    installed,
):
    store, token, session = installed
    db = store._connection
    with db:
        db.execute(
            "UPDATE trace_presence_counter SET next_id=?", (encode_counter(127),)
        )
    store.observe_presence(
        agent_id="worker", state="degraded", reason="owned newer presence"
    )
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    store.close_session(connector_id="native", token=token, session_id=session)
    assert db.execute("PRAGMA page_count").fetchone()[0] == pages
    assert db.execute("SELECT count(*) FROM trace_journal_all").fetchone()[0] == 0
    assert (
        db.execute("SELECT event_count FROM trace_storage_usage_all").fetchone()[0] == 0
    )
    latest = db.execute(
        "SELECT * FROM presence_history_all ORDER BY presence_id DESC LIMIT 1"
    ).fetchone()
    assert latest["presence_id"] == 128 and latest["state"] == "unavailable"
    assert db.execute("SELECT count(*) FROM presence_history").fetchone()[0] == 2
    assert store.health()["telemetry_records"]["presence_history"] == 3
    assert (
        next(agent for agent in store.list_agents() if agent["agent_id"] == "worker")[
            "reason"
        ]
        == "native_session_closed"
    )
    db.execute("PRAGMA max_page_count=1073741823")
    before = local_rows(db)
    with db:
        db.execute("BEGIN IMMEDIATE")
        assert materialize(db, 2)
    after = local_rows(db)
    assert {
        key: value for key, value in after.items() if key != "trace_completion_slots"
    } == {
        key: value for key, value in before.items() if key != "trace_completion_slots"
    }
    store.observe_presence(
        agent_id="worker", state="online", reason="owned later presence"
    )
    assert (
        db.execute("SELECT max(presence_id) FROM presence_history_all").fetchone()[0]
        == 129
    )


def test_presence_failure_rolls_back_session_task_recovery_and_positions(installed):
    store, token, session = installed
    task = store.create_task(sender_id="origin", recipient_id="worker", payload={})
    store.claim_next_task(connector_id="native", token=token, session_id=session)
    db = store._connection
    db.execute("""CREATE TRIGGER owned_local_failure BEFORE UPDATE ON trace_completion_slots
    WHEN json_extract(CAST(NEW.record AS TEXT),'$.local.presence') IS NOT NULL
    BEGIN SELECT RAISE(ABORT,'owned presence failure'); END""")
    before = local_rows(db)
    with pytest.raises(sqlite3.IntegrityError, match="owned presence failure"):
        store.close_session(connector_id="native", token=token, session_id=session)
    assert local_rows(db) == before
    db.execute("DROP TRIGGER owned_local_failure")
    store.close_session(connector_id="native", token=token, session_id=session)
    assert store.get_task(task["task_id"])["state"] == "queued"


def test_revocation_closes_running_task_and_preserves_local_audit_at_page_limit(
    installed,
):
    store, _, _ = installed
    task = running(installed)
    db = store._connection
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count={pages}")
    store.revoke_connector("native")
    assert db.execute("PRAGMA page_count").fetchone()[0] == pages
    assert store.get_task(task["task_id"])["state"] == "failed"
    assert (
        db.execute(
            "SELECT count(*) FROM events WHERE event_type='connector.revoked'"
        ).fetchone()[0]
        == 0
    )
    assert (
        db.execute(
            "SELECT count(*) FROM events_all WHERE event_type='connector.revoked'"
        ).fetchone()[0]
        == 1
    )
    assert db.execute("SELECT count(*) FROM trace_journal_all").fetchone()[0] == 5
    assert (
        db.execute("SELECT event_count FROM trace_storage_usage_all").fetchone()[0] == 5
    )
    assert len(store.pending_transport()) == 1
    with pytest.raises(sqlite3.IntegrityError, match="completed record reference"), db:
        db.execute("DELETE FROM sessions")


def test_reissuing_managed_connector_materializes_old_revocation_before_reusing_slot(
    installed,
):
    store, _, _ = installed
    store.register_connector(
        connector_id="managed",
        host_type="managed-agent",
        agent_id="managed-worker",
        capabilities=[],
    )
    store.revoke_connector("managed")
    store.reissue_managed_connector("managed", "managed-worker")
    store.revoke_connector("managed")
    db = store._connection
    assert (
        db.execute(
            "SELECT count(*) FROM events_all WHERE event_type='connector.revoked'"
        ).fetchone()[0]
        == 2
    )
    assert (
        db.execute(
            "SELECT count(*) FROM events WHERE event_type='connector.revoked'"
        ).fetchone()[0]
        == 1
    )
    assert db.execute("SELECT count(*) FROM trace_journal_all").fetchone()[0] == 0
