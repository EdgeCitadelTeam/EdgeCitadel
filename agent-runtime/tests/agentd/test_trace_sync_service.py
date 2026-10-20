from functools import partial
import asyncio
import json
import threading
import time

import pytest

import edgecitadel_agentd.trace_sync_service as lifecycle
from edgecitadel_agentd.store import AgentdStore
from edgecitadel_plugin_runtime.telemetry_stream import TelemetryConfigurationError


def wait_until(predicate):
    deadline = time.monotonic() + 3
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.005)


@pytest.fixture
def configured(tmp_path, monkeypatch):
    (tmp_path / "node.json").write_text(
        json.dumps(
            {
                "version": 2,
                "agent_id": "edge-a",
                "mode": "edge",
                "messaging_mode": "nats_leaf",
                "jetstream_domain": "EDGE_LOCAL",
                "plugin_nats_url": "nats://127.0.0.1:4223",
                "plugin_nats_token": "private-test-token",
            }
        )
    )
    store = AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")
    clients = []

    class Client:
        def __init__(self):
            self.is_closed, self.is_connected = False, False
            clients.append(self)

        async def connect(self, **kwargs):
            self.thread = threading.get_ident()
            self.kwargs = kwargs
            self.is_connected = True

        def jetstream(self, **kwargs):
            assert kwargs == {}  # Never select EDGE_LOCAL for Core telemetry.
            return self

        async def close(self):
            assert threading.get_ident() == self.thread
            self.is_closed, self.is_connected = True, False

    async def ensure(js, *, create):
        assert create is False

    monkeypatch.setattr(lifecycle, "NATS", Client)
    monkeypatch.setattr(lifecycle, "ensure_telemetry_stream", ensure)
    yield tmp_path, store, clients
    store.close()


def test_disabled_service_has_no_thread_database_or_network_side_effects(tmp_path):
    service = lifecycle.TraceSyncService(
        tmp_path, partial(AgentdStore, tmp_path / "missing" / "agentd.sqlite3")
    )
    service.start()
    service.stop()
    assert service._thread is None
    assert not (tmp_path / "missing").exists()
    assert service.status()["state"] == "disabled"


def test_separate_thread_store_and_connection_do_not_block_command_handle(
    configured, monkeypatch
):
    path, command_store, clients = configured
    entered, release = threading.Event(), threading.Event()
    captures = []

    class Manager:
        def __init__(self, store, js, nc):
            self.state, self.fault, self.active = "running", None, {}
            captures.append((store, threading.get_ident()))

        async def run(self, stop):
            entered.set()
            # Model synchronous telemetry work that stalls its own event loop.
            release.wait(3)
            await stop.wait()

    async def forbidden_management(*args, **kwargs):
        raise AssertionError("Leaf startup must not use local management APIs")

    monkeypatch.setattr(lifecycle, "ensure_telemetry_stream", forbidden_management)
    monkeypatch.setattr(lifecycle, "TraceSyncManager", Manager)
    service = lifecycle.TraceSyncService(
        path, partial(AgentdStore, command_store.path), enabled=True
    )
    service.start()
    try:
        assert entered.wait(2)
        service.start()  # Does not allocate another thread or connection.
        assert len(captures) == 1 and len(clients) == 1
        assert captures[0][0]._connection is not command_store._connection
        assert captures[0][1] != threading.get_ident()
        started = time.monotonic()
        command_store.create_task(
            sender_id="caller", recipient_id="worker", payload={"body": "work"}
        )
        assert command_store.health()["status"] == "ready"
        assert service.status()["enabled"]
        assert time.monotonic() - started < 0.5
    finally:
        release.set()
        service.stop()
    assert clients[0].is_closed
    assert command_store.health()["status"] == "ready"
    assert not service._thread.is_alive()
    assert service.status()["state"] == "stopped"
    service.start()
    try:
        wait_until(lambda: len(captures) == 2)
    finally:
        service.stop()
    assert all(client.is_closed for client in clients)


def test_configuration_fault_pauses_without_exposing_credentials(
    configured, monkeypatch
):
    path, store, clients = configured

    node = json.loads((path / "node.json").read_text())
    node["messaging_mode"] = "single-client"
    (path / "node.json").write_text(json.dumps(node))

    async def drift(js, *, create):
        assert create is False
        raise TelemetryConfigurationError("private-test-token")

    monkeypatch.setattr(lifecycle, "ensure_telemetry_stream", drift)
    service = lifecycle.TraceSyncService(
        path, partial(AgentdStore, store.path), enabled=True
    )
    service.start()
    wait_until(lambda: not service._thread.is_alive())
    assert service.status()["fault"] == "telemetry_configuration_error"
    assert service.status()["state"] == "paused"
    assert "private-test-token" not in json.dumps(service.status())
    assert clients[0].is_closed
    service.stop()


def test_stop_cancels_initial_connect_and_closes_owned_connection(
    configured, monkeypatch
):
    path, store, clients = configured
    entered = threading.Event()
    client_type = lifecycle.NATS

    async def connect(self, **kwargs):
        self.thread = threading.get_ident()
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(client_type, "connect", connect)
    service = lifecycle.TraceSyncService(
        path, partial(AgentdStore, store.path), enabled=True
    )
    service.start()
    assert entered.wait(2)
    service.stop()
    assert clients[0].is_closed and not service._thread.is_alive()
    assert store.health()["status"] == "ready"


def test_direct_source_retries_missing_core_stream_without_creating_it(
    configured, monkeypatch
):
    from nats.js.errors import NotFoundError

    path, store, clients = configured
    node = json.loads((path / "node.json").read_text())
    node["messaging_mode"] = "single-client"
    (path / "node.json").write_text(json.dumps(node))
    calls = []
    entered = threading.Event()

    async def verify(js, *, create):
        calls.append(create)
        if len(calls) == 1:
            raise NotFoundError(code=404)

    async def manage(self, owned_store, nc, js):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(lifecycle, "ensure_telemetry_stream", verify)
    monkeypatch.setattr(lifecycle.TraceSyncService, "_manage", manage)
    service = lifecycle.TraceSyncService(
        path, partial(AgentdStore, store.path), enabled=True
    )
    service.start()
    try:
        assert entered.wait(4)
        assert calls == [False, False]
        assert len(clients) == 2 and clients[0].is_closed
    finally:
        service.stop()
