from functools import partial
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_service import service  # noqa: F401

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.service import dispatch
from edgecitadel_agentd.store import AgentdStore, StoreError
from edgecitadel_agentd.trace_inspect import inspect_source
from edgecitadel_agentd.trace_journal import TraceJournal
from edgecitadel_agentd.trace_sync_service import TraceSyncService


def test_real_socket_requires_admin_and_cannot_enable_disabled_service(service):  # noqa: F811
    socket, state, _ = service
    client = AgentdClient(socket)
    with pytest.raises(AgentdClientError, match="management authentication failed"):
        client.call("trace.sync.control", action="stop")
    admin = AgentdClient(
        socket, admin_token=(state / "admin.token").read_text().strip()
    )
    assert admin.call("trace.sync.control", action="stop")["state"] == "disabled"
    with pytest.raises(AgentdClientError, match="disabled by process configuration"):
        admin.call("trace.sync.control", action="start")
    assert client.call("health")["telemetry"]["enabled"] is False
    root = Path(__file__).parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "edgecitadel_agentd.trace_control",
            "--state-dir",
            str(state),
            "stop",
        ],
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["state"] == "disabled"


def test_retry_clears_only_named_fault_and_inspection_exposes_others(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    sync = TraceSyncService(tmp_path, partial(AgentdStore, store.path), enabled=True)
    try:
        with store._connection:
            store._connection.execute("BEGIN IMMEDIATE")
            a = TraceJournal(store._connection).initialize("edge-a")
            b = TraceJournal(store._connection).initialize("edge-b")
            store._connection.execute(
                "UPDATE trace_export_generations SET sync_fault='invalid_export_record'"
            )
        scope = ["edge-a", *a]
        sync.control(store, {"action": "start"})
        sync.control(store, {"action": "stop"})
        assert (
            inspect_source(store.path, scope=tuple(scope))["generation"]["sync_fault"]
            == "invalid_export_record"
        )
        sync.control(store, {"action": "retry", "scope": scope})
        assert (
            inspect_source(store.path, scope=tuple(scope))["generation"]["sync_fault"]
            is None
        )
        assert (
            inspect_source(store.path, scope=("edge-b", *b))["generation"]["sync_fault"]
            == "invalid_export_record"
        )
        assert (
            store._connection.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
        )
        sync.close()
        with pytest.raises(StoreError, match="lifecycle is closed"):
            sync.control(store, {"action": "start"})
        sync.start()
        assert not sync._thread.is_alive()
    finally:
        sync.close()
        store.close()


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"action": "retry"},
        {"action": "retry", "scope": ["x"]},
        {"action": "stop", "scope": ["a", "b", "c"]},
        {"action": "start", "force": True},
    ],
)
def test_invalid_controls_do_not_start_or_mutate(tmp_path, params):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    sync = TraceSyncService(tmp_path, partial(AgentdStore, store.path), enabled=True)
    try:
        before = list(store._connection.iterdump())
        with pytest.raises(StoreError, match="invalid telemetry control request"):
            sync.control(store, params)
        assert sync._thread is None
        assert list(store._connection.iterdump()) == before
        with pytest.raises(StoreError, match="unknown telemetry export generation"):
            sync.control(store, {"action": "retry", "scope": ["a", "b", "c"]})
        assert sync._thread is None
    finally:
        sync.close()
        store.close()


def test_management_auth_precedes_lifecycle_control(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")

    class Forbidden:
        def control(self, *args):
            raise AssertionError("unauthorized control reached lifecycle")

    try:
        for credential in (None, "wrong", "非ascii"):
            with pytest.raises(StoreError, match="management authentication failed"):
                dispatch(
                    store,
                    {
                        "version": 1,
                        "operation": "trace.sync.control",
                        "admin_token": credential,
                        "params": {"action": "stop"},
                    },
                    telemetry=Forbidden(),
                    admin_token="real-admin",
                )
    finally:
        store.close()


def test_concurrent_lifecycle_commands_leave_no_orphan_thread(tmp_path):
    store = AgentdStore(tmp_path / "agentd.sqlite3")
    sync = TraceSyncService(tmp_path, partial(AgentdStore, store.path), enabled=True)
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            calls = [
                pool.submit(sync.control, store, {"action": action})
                for action in ["start", "stop"] * 4
            ]
            for call in calls:
                call.result(timeout=15)
        sync.close()
        assert sync._thread is not None and not sync._thread.is_alive()
        assert sync.status()["connected"] is False
    finally:
        sync.close()
        store.close()
