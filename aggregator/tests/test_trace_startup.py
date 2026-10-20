"""Main-app lifecycle component tests; live broker acceptance runs on jim-eq."""

import time
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_trace_graph_projection import TRACE
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator import (
    main,
    trace_read_service,
    trace_projector,
    trace_collector,
    memory,
)

core = core_fixture


@pytest.fixture
def configured(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setenv("EDGECITADEL_TRACE_READS", "1")
    monkeypatch.setenv("EDGECITADEL_TRACE_COLLECTOR", "1")
    return tmp_path


def test_default_has_no_read_routes_key_or_projector(configured, monkeypatch):
    monkeypatch.delenv("EDGECITADEL_TRACE_READS")
    with TestClient(main.make_app(for_testing=True)) as client:
        assert client.get("/api/traces").status_code == 404
        status = client.get("/api/system/status").json()
        assert status["trace_projection"] == {"state": "disabled"}
        assert status["trace_reads"] == {"enabled": False}
    assert not (configured / "trace-cursor.key").exists()


@pytest.mark.parametrize(
    "key,value,code",
    [
        ("EDGECITADEL_TRACE_READS", "yes", "trace_read_configuration_unavailable"),
        ("EDGECITADEL_TRACE_COLLECTOR", "0", "trace_read_collector_required"),
    ],
)
def test_invalid_configuration_fails_before_services(
    configured, monkeypatch, key, value, code
):
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match=f"^{code}$"):
        main.make_app()
    assert not (configured / "trace-cursor.key").exists()


def test_enabled_app_projects_and_serves_then_closes_readers_before_writer(
    core, configured, monkeypatch
):
    path = Path(core.execute("PRAGMA database_list").fetchone()[2])
    monkeypatch.setenv("DB_PATH", str(path))
    put(
        core,
        event(seq=1, trace_id=TRACE),
        str(uuid4()),
        1,
        received_at_ms=time.time_ns() // 1_000_000,
    )
    closed = []
    original_read_close = trace_read_service.TraceReadService.close
    original_projector_close = trace_projector.TraceProjectorService.close

    def close_reads(self):
        closed.append("reads")
        original_read_close(self)

    def close_projector(self):
        closed.append("projector")
        original_projector_close(self)
        assert not self.status()["worker_alive"]

    monkeypatch.setattr(trace_read_service.TraceReadService, "close", close_reads)
    monkeypatch.setattr(trace_projector.TraceProjectorService, "close", close_projector)
    with TestClient(main.make_app(for_testing=True)) as client:
        assert client.get("/api/traces").status_code == 200
        deadline = time.monotonic() + 5
        while True:
            response = client.get("/api/traces")
            if response.status_code == 200 and response.json()["items"]:
                break
            assert time.monotonic() < deadline, response.text
            time.sleep(0.02)
        assert response.json()["items"][0]["trace_id"] == TRACE
        assert client.get("/api/system/status").json()["trace_projection"][
            "worker_alive"
        ]
    assert closed == ["reads", "projector"]
    assert (path.parent / "trace-cursor.key").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "failure",
    ["aggregator", "memory", "collector", "projector", "projector_close", None],
)
def test_partial_startup_and_shutdown_unwind_owned_services(
    configured, monkeypatch, failure
):
    calls = []
    monkeypatch.setenv("NATS_URL", "nats://owned-unused")
    monkeypatch.setenv("NATS_TOKEN", "unused")

    class Aggregator:
        def __init__(self, **kwargs):
            self.router = SimpleNamespace(nc=None)

        async def start(self):
            calls.append("aggregator.start")
            if failure == "aggregator":
                raise RuntimeError("injected")

        async def stop(self):
            calls.append("aggregator.close")

    class Memory:
        def __init__(self, **kwargs):
            pass

        async def start(self):
            calls.append("memory.start")
            if failure == "memory":
                raise RuntimeError("injected")

        async def stop(self):
            calls.append("memory.close")

    def worker(name):
        class Worker:
            def __init__(self, *args):
                pass

            def start(self):
                calls.append(name + ".start")
                if failure == name:
                    raise RuntimeError("injected")

            def close(self):
                calls.append(name + ".close")
                if failure == name + "_close":
                    raise RuntimeError("injected")

        return Worker

    close = trace_read_service.TraceReadService.close

    def close_reads(self):
        calls.append("reads.close")
        close(self)

    monkeypatch.setattr(main, "AggregatorApp", Aggregator)
    monkeypatch.setattr(memory, "MemoryService", Memory)
    monkeypatch.setattr(trace_collector, "TraceCollectorService", worker("collector"))
    monkeypatch.setattr(trace_projector, "TraceProjectorService", worker("projector"))
    monkeypatch.setattr(trace_read_service.TraceReadService, "close", close_reads)
    if failure:
        with pytest.raises(RuntimeError, match="injected"), TestClient(main.make_app()):
            pass
    else:
        with TestClient(main.make_app()):
            pass
    started = [call.removesuffix(".start") for call in calls if call.endswith(".start")]
    assert [call for call in calls if call.endswith(".close")] == [
        "reads.close",
        *[name + ".close" for name in reversed(started)],
    ]
