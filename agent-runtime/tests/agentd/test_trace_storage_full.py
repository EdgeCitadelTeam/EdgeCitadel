"""Owned SQLite page-ceiling tests exercise real SQLITE_FULL, not mocked errors."""

import json
import sqlite3
import threading
import time
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from test_trace_crash import prepare, snapshot

from edgecitadel_agentd.service import PROTOCOL_VERSION, dispatch
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_producer import RuntimeTrace
from edgecitadel_agentd.trace_retention import maintain_capacity


def fill_journal(store):
    fixtures = json.loads(
        (Path(__file__).parents[1] / "fixtures/traces/events.v1.json").read_text()
    )["fixtures"]
    template = next(f["event"] for f in fixtures if f["name"] == "tool")
    db = store._connection
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    ceiling = db.execute("PRAGMA page_count").fetchone()[0]
    assert db.execute(f"PRAGMA max_page_count={ceiling}").fetchone()[0] == ceiling
    for _index in range(2048):
        event = deepcopy(template)
        event["event_id"] = str(uuid4())
        before = snapshot(store)
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                TraceJournal(db).record("owned-edge", event, selected=True)
        except sqlite3.OperationalError as error:
            assert error.sqlite_errorcode == sqlite3.SQLITE_FULL
            assert snapshot(store) == before
            return ceiling
    raise AssertionError("owned SQLite page ceiling did not reject a journal write")


def recover(store, ceiling):
    store._connection.execute(f"PRAGMA max_page_count={ceiling + 256}")


def test_full_dispatch_rolls_back_and_retry_has_one_child(tmp_path):
    store, token, _session, binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    try:
        ceiling = fill_journal(store)
        params = {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "binding_id": binding["binding_id"],
            "recipient_id": "worker",
            "request": "x" * 4096,
            "skill_id": None,
            "deadline_at_ms": None,
        }
        before = snapshot(store)
        with pytest.raises(sqlite3.OperationalError) as raised:
            store.dispatch_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=params
            )
        assert raised.value.sqlite_errorcode == sqlite3.SQLITE_FULL
        assert snapshot(store) == before
        recover(store, ceiling)
        reply = store.dispatch_trace(
            node_id="owned-edge", connector_id="owner", token=token, params=params
        )
        assert (
            store.dispatch_trace(
                node_id="owned-edge", connector_id="owner", token=token, params=params
            )
            == reply
        )
        assert (
            store._connection.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 1
        )
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM transport_outbox"
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


def test_full_retention_preserves_original_error_and_payloads(tmp_path):
    store, _token, _session, _binding = prepare(
        tmp_path / "state/agentd/agentd.sqlite3"
    )
    try:
        ceiling = fill_journal(store)
        before = snapshot(store)
        with pytest.raises(sqlite3.OperationalError) as raised, store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            maintain_capacity(
                store._connection,
                now_ms=int(time.time() * 1000),
                expire_before_ms=int(time.time() * 1000) + 1,
            )
        assert raised.value.sqlite_errorcode == sqlite3.SQLITE_FULL
        assert snapshot(store) == before
        recover(store, ceiling)
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        store.close()


@pytest.mark.asyncio
async def test_full_optional_observation_runs_effect_once_and_reports_after_recovery(
    tmp_path,
):
    store, token, _session, binding = prepare(tmp_path / "state/agentd/agentd.sqlite3")

    class Client:
        def call(self, operation, **params):
            return dispatch(
                store,
                {
                    "version": PROTOCOL_VERSION,
                    "operation": operation,
                    "params": params,
                    "connector_id": "owner",
                    "token": token,
                },
            )

    trace = RuntimeTrace(Client())
    trace.binding_id = binding["binding_id"]
    try:
        ceiling = fill_journal(store)
        effects = []
        async with trace.operation("tool", "owned-full-effect"):
            effects.append("once")
        assert effects == ["once"]
        assert trace.dropped_observations > 0
        recover(store, ceiling)
        await trace.report_loss()
        reports = [
            json.loads(r[0])
            for r in store._connection.execute(
                "SELECT event_json FROM trace_journal WHERE json_extract(event_json,'$.kind')='coverage'"
            )
        ]
        assert len(reports) == 1
        assert (
            reports[0]["attributes"]["dropped_observations"]
            == trace.dropped_observations
        )
        assert effects == ["once"]
        assert store._connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        store.close()


def test_daemon_reconciliation_survives_full_storage_and_clears_degraded_health(
    tmp_path, monkeypatch
):
    from edgecitadel_agentd import service
    from edgecitadel_agentd.client import AgentdClient, AgentdClientError

    store, *_rest = prepare(tmp_path / "state/agentd/agentd.sqlite3")
    ceiling = fill_journal(store)
    with store._connection:
        store._connection.execute(
            "UPDATE trace_journal SET received_at_ms=1 WHERE json_extract(event_json,'$.kind')='tool'"
        )
    thread_errors = []
    monkeypatch.setattr(
        threading, "excepthook", lambda args: thread_errors.append(args.exc_value)
    )
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve,
        args=(store.path.parent, stop),
        kwargs={"open_store": lambda: store},
        daemon=True,
    )
    thread.start()
    client = AgentdClient(service.socket_path_for(store.path.parent), timeout=0.2)

    def wait_health(degraded):
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            assert not thread_errors, thread_errors
            try:
                health = client.call("health")
                if (health.get("reconciliation") == "storage_unavailable") == degraded:
                    return health
            except AgentdClientError:
                pass
            time.sleep(0.02)
        raise AssertionError("owned reconciliation did not reach expected health state")

    try:
        health = wait_health(True)
        assert health["status"] == "degraded" and health["database"] == "ok"
        with store._lock:
            recover(store, ceiling)
        assert wait_health(False)["status"] == "ready"
    finally:
        stop.set()
        thread.join(timeout=10)
        assert not thread.is_alive()
