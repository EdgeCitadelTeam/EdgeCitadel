import json
import sqlite3
import time
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

from storage_test_support import flatten_connection

import pytest

from edgecitadel_agentd import trace_capacity
from edgecitadel_agentd.store import TELEMETRY_RETENTION_MS, AgentdStore
from edgecitadel_agentd.trace_history import read_history
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_retention import prune_active_history


@pytest.fixture
def recorded(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    template = next(f["event"] for f in fixtures if f["name"] == "tool")
    template["occurred_at"] = "2000-01-01T00:00:00.000Z"
    with store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        for index in range(4):
            event = deepcopy(template)
            event["event_id"] = str(uuid4())
            TraceJournal(store._connection).record("edge-a", event, selected=index < 3)
        store._connection.execute(
            "UPDATE trace_spool SET state='broker_acked' WHERE export_seq=2"
        )
        store._connection.execute(
            "UPDATE trace_spool SET state='core_settled' WHERE export_seq=3"
        )
    try:
        yield store
    finally:
        store.close()


def prune(store):
    with store._lock, store._connection:
        store._connection.execute("BEGIN IMMEDIATE")
        return prune_active_history(
            store._connection, node_id="edge-a", now_ms=int(time.time() * 1000)
        )


def test_pending_loss_marker_is_durable_before_content_removal(recorded):
    store = recorded
    original = [
        tuple(r)
        for r in store._connection.execute(
            "SELECT event_id,event_sha256 FROM trace_spool ORDER BY export_seq"
        )
    ]
    assert prune(store) == 4
    rows = store._connection.execute("SELECT event_json FROM trace_journal").fetchall()
    assert len(rows) == 1
    marker = json.loads(rows[0][0])
    assert marker["kind"] == "coverage" and marker["phase"] == "lost"
    assert marker["attributes"]["lost_ranges"] == [{"first": 1, "last": 2}]
    assert marker["attributes"]["through_export_seq"] == 3
    assert marker["trace_id"] is None
    spool = store._connection.execute(
        "SELECT * FROM trace_spool ORDER BY export_seq"
    ).fetchall()
    assert [r["state"] for r in spool] == [
        "lost_with_marker",
        "lost_with_marker",
        "core_settled",
        "pending",
    ]
    assert all(r["journal_event_id"] is None for r in spool[:3])
    assert [(r["event_id"], r["event_sha256"]) for r in spool[:3]] == original
    assert tuple(
        store._connection.execute(
            "SELECT event_bytes,event_count FROM trace_storage_usage"
        ).fetchone()
    ) == tuple(
        store._connection.execute(
            "SELECT sum(event_bytes),count(*) FROM trace_journal"
        ).fetchone()
    )
    assert prune(store) == 0  # Never discard the only loss evidence recursively.
    with sqlite3.connect(store.path) as reopened:
        assert reopened.execute("SELECT count(*) FROM trace_journal").fetchone()[0] == 1
        assert reopened.execute("PRAGMA foreign_key_check").fetchall() == []


def test_pruning_rollback_keeps_original_payload_and_export_state(recorded):
    store = recorded
    tables = (
        "trace_journal",
        "trace_spool",
        "trace_sources",
        "trace_export_generations",
        "trace_storage_usage",
    )
    before = {
        name: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {name}")]
        for name in tables
    }
    store._connection.execute(
        "CREATE TRIGGER owned_delete_failure BEFORE DELETE ON trace_journal BEGIN SELECT RAISE(ABORT,'owned failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError):
        prune(store)
    assert {
        name: [tuple(r) for r in store._connection.execute(f"SELECT * FROM {name}")]
        for name in tables
    } == before


def test_open_root_and_other_export_generation_are_preserved(recorded):
    store = recorded
    token = store.register_connector(
        connector_id="native",
        host_type="codex",
        agent_id="native",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="native", token=token)["session_id"]
    root = store.bind_trace(
        node_id="edge-a",
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
    with store._connection:
        store._connection.execute("UPDATE trace_export_generations SET active=0")
        store._connection.execute(
            "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation) SELECT node_id,source_epoch,? FROM trace_sources",
            (str(uuid4()),),
        )
    assert prune(store) == 1  # Only the old local-only event is eligible.
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_journal WHERE trace_id=? AND json_extract(event_json,'$.kind')='run'",
            (root["trace_id"],),
        ).fetchone()[0]
        == 1
    )
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_spool WHERE journal_event_id IS NULL"
        ).fetchone()[0]
        == 0
    )


def test_reconcile_reclaims_pressure_and_scoped_reads_disclose_loss(
    recorded, monkeypatch
):
    store = recorded
    actor = store._connection.execute(
        "SELECT agent_id FROM trace_journal LIMIT 1"
    ).fetchone()[0]
    token = store.register_connector(
        connector_id="reader",
        host_type="codex",
        agent_id=actor,
        capabilities=["edgecitadel_trace"],
    )
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 1)
    store.reconcile()
    history = read_history(store, connector_id="reader", token=token, params={})
    assert history["coverage"]["local_history_pruned"]
    assert history["events"][0]["kind"] == "coverage"
    assert all(event["kind"] == "coverage" for event in history["events"])
    assert any(event["phase"] == "lost" for event in history["events"])


def test_reserve_exhaustion_leaves_pending_content_intact(recorded, monkeypatch):
    store = recorded
    monkeypatch.setattr(trace_capacity, "NORMAL_LIMIT_BYTES", 1)
    monkeypatch.setattr(trace_capacity, "CONTROL_RESERVE_BYTES", 0)
    before = [
        tuple(r) for r in store._connection.execute("SELECT * FROM trace_journal")
    ]
    store.reconcile()
    assert [
        tuple(r) for r in store._connection.execute("SELECT * FROM trace_journal")
    ] == before
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_spool WHERE journal_event_id IS NULL"
        ).fetchone()[0]
        == 0
    )


def test_expiry_uses_arrival_time_and_preserves_boundary_freshness(recorded):
    store = recorded
    now = int(time.time() * 1000)
    store.reconcile(now_ms=now)
    # Producer timestamps are decades old; all four arrivals are fresh.
    assert (
        store._connection.execute("SELECT count(*) FROM trace_journal").fetchone()[0]
        == 4
    )
    with store._connection:
        store._connection.execute(
            "UPDATE trace_journal SET received_at_ms=? WHERE source_seq=1",
            (now - TELEMETRY_RETENTION_MS - 1,),
        )
        store._connection.execute(
            "UPDATE trace_journal SET received_at_ms=? WHERE source_seq=2",
            (now - TELEMETRY_RETENTION_MS,),
        )
    store.reconcile(now_ms=now)
    marker = json.loads(
        store._connection.execute(
            "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
        ).fetchone()[0]
    )
    assert marker["attributes"]["reason"] == "retention_expired"
    assert marker["attributes"]["lost_ranges"] == [{"first": 1, "last": 1}]
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_journal WHERE source_seq=2"
        ).fetchone()[0]
        == 1
    )


def test_v11_upgrade_preserves_event_hashes_and_starts_conservative_age(recorded):
    store = recorded
    before = [
        tuple(r)
        for r in store._connection.execute(
            "SELECT event_id,event_sha256,event_json FROM trace_journal ORDER BY source_seq"
        )
    ]
    with store._connection:
        store._connection.execute("DROP INDEX trace_journal_retention")
        store._connection.execute(
            "ALTER TABLE trace_journal DROP COLUMN received_at_ms"
        )
        store._connection.execute("DROP TABLE IF EXISTS trace_import_records")
        store._connection.execute("DROP TABLE IF EXISTS trace_import_grants")
        flatten_connection(store._connection)
        store._connection.execute("PRAGMA user_version=11")
    started = int(time.time() * 1000)
    migrated = AgentdStore(store.path)
    try:
        assert migrated._connection.execute("PRAGMA user_version").fetchone()[0] == 25
        assert [
            tuple(r)
            for r in migrated._connection.execute(
                "SELECT event_id,event_sha256,event_json FROM trace_journal ORDER BY source_seq"
            )
        ] == before
        assert all(
            r[0] >= started
            for r in migrated._connection.execute(
                "SELECT received_at_ms FROM trace_journal"
            )
        )
        migrated.reconcile()
        assert (
            migrated._connection.execute(
                "SELECT count(*) FROM trace_journal"
            ).fetchone()[0]
            == 4
        )
    finally:
        migrated.close()


def test_expiry_pins_active_root_until_session_closes(recorded):
    store = recorded
    token = store.register_connector(
        connector_id="active",
        host_type="codex",
        agent_id="active",
        capabilities=["edgecitadel_trace"],
    )
    session = store.open_session(connector_id="active", token=token)["session_id"]
    root = store.bind_trace(
        node_id="edge-a",
        connector_id="active",
        token=token,
        params={
            "schema_version": 1,
            "request_id": str(uuid4()),
            "session_id": session,
            "task_id": None,
            "context_id": None,
        },
    )["result"]
    with store._connection:
        store._connection.execute(
            "UPDATE trace_journal SET received_at_ms=0 WHERE trace_id=?",
            (root["trace_id"],),
        )
    store.reconcile()
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_journal WHERE trace_id=?", (root["trace_id"],)
        ).fetchone()[0]
        == 1
    )
    store.close_session(connector_id="active", token=token, session_id=session)
    store.reconcile()
    assert (
        store._connection.execute(
            "SELECT count(*) FROM trace_journal WHERE trace_id=? AND json_extract(event_json,'$.phase')='started'",
            (root["trace_id"],),
        ).fetchone()[0]
        == 1  # Closure does not authorize dropping unsettled mandatory evidence.
    )
