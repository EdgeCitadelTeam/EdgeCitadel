"""Monitor thread, HTTP intake and status, decision ledger and startup wiring."""

import json
import secrets
import sqlite3
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aggregator import main, trace_store
from aggregator.containment_monitor.config import ConfigError, MonitorConfig
from aggregator.containment_monitor.control import (
    FakeControlClient,
    LocalAgentdControlClient,
    NatsControlClient,
)
from aggregator.containment_monitor.routes import make_containment_router
from aggregator.containment_monitor.service import (
    ContainmentMonitorService,
    service_from_environment,
)
from containment_support import TraceBuilder, ingest

ADMIN = {"x-edgecitadel-admin-token": "t" * 32}


def wait_for(predicate, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition not reached")


@pytest.fixture
def core_db(tmp_path):
    path = tmp_path / "core.db"
    with sqlite3.connect(path) as core:
        core.execute("PRAGMA journal_mode=WAL")
        trace_store.initialize(core)
        trace = TraceBuilder()
        parent = trace.start(trace.root("p", "n0", 0), 1)
        child_task = trace.dispatch(parent, "c", "n1", 2)
        trace.arrive(child_task, 3)
        child = trace.start(child_task, 4)
        result = trace.complete(child, 6)
        for event in trace.events:
            ingest(core, event)
    return path, result, child


def test_service_follows_core_and_answers_alerts_over_http(
    core_db, tmp_path, monkeypatch
):
    path, result, child = core_db
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", ADMIN["x-edgecitadel-admin-token"])
    ledger = tmp_path / "ledger.jsonl"
    service = ContainmentMonitorService(
        path,
        config=MonitorConfig(grants={"p": frozenset({"c"}), "c": frozenset()}),
        control=FakeControlClient(),
        decision_log=ledger,
        poll_seconds=0.02,
    )
    app = FastAPI()
    app.include_router(make_containment_router(service))
    service.start()
    try:
        with TestClient(app) as client:
            assert client.get("/api/containment/status").status_code == 401
            wrong = {"x-edgecitadel-admin-token": "w" * 32}
            assert (
                client.get("/api/containment/status", headers=wrong).status_code == 401
            )
            assert (
                client.post(
                    "/api/containment/alerts", json={"kind": "operator"}
                ).status_code
                == 401
            )
            status = wait_for(
                lambda: (
                    s := client.get("/api/containment/status", headers=ADMIN).json()
                ).get("cursor")
                and s["cursor"]["lag"] == 0
                and s
            )
            assert (
                status["state"] == "running" and status["g_p"]["nodes"] == 7
            )  # 2 invocations, 2 tasks, 3 versions
            for payload, code in (
                (
                    {"kind": "validator_failure", "task_id": child},
                    "version_subject_required",
                ),
                ({"kind": "nonsense", "version_id": result}, "invalid_alert_kind"),
                (
                    {"kind": "operator", "version_id": result, "extra": 1},
                    "unknown_alert_field",
                ),
                ([], "invalid_alert"),
            ):
                reply = client.post(
                    "/api/containment/alerts", json=payload, headers=ADMIN
                )
                assert (reply.status_code, reply.json()["detail"]) == (400, code)
            accepted = client.post(
                "/api/containment/alerts",
                json={"kind": "validator_failure", "version_id": result},
                headers=ADMIN,
            )
            assert accepted.status_code == 202
            alert_id = accepted.json()["alert_id"]
            status = wait_for(
                lambda: (
                    s := client.get("/api/containment/status", headers=ADMIN).json()
                ).get("last_decision")
                and s
            )
    finally:
        service.close()
    decision = status["last_decision"]
    assert decision["alert"]["alert_id"] == alert_id
    assert status["root_cause"][0]["node"] == f"inv:{child}"
    assert status["g_c"]["nodes"] > 0 and status["last_solve"]["status"] == "optimal"
    assert status["in_force"], "the decision left holds in force"
    for record in status["in_force"]:
        assert {"t_alert", "t_decision", "t_sent", "t_applied", "t_confirmed"} <= set(
            record
        )
    (line,) = ledger.read_text().splitlines()
    logged = json.loads(line)
    assert logged["type"] == "decision" and logged["alert"]["alert_id"] == alert_id
    assert logged["phase1"] and logged["status"] == decision["status"]


def test_monitor_database_is_separate_and_single_writer(core_db, tmp_path):
    path, _, _ = core_db
    first = ContainmentMonitorService(path, poll_seconds=0.02)
    second = ContainmentMonitorService(path, poll_seconds=0.02)
    first.start()
    try:
        wait_for(lambda: first.status().get("state") == "running")
        second.start()
        wait_for(lambda: second.status().get("fault") == "monitor_writer_active")
    finally:
        first.close()
        second.close()
    with sqlite3.connect(path) as core:
        tables = {
            row[0]
            for row in core.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    assert not {
        t for t in tables if t.startswith("containment_")
    }  # Core schema untouched
    assert (path.parent / "containment.sqlite3").is_file()


@pytest.fixture
def startup(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setenv("EDGECITADEL_TRACE_COLLECTOR", "1")
    monkeypatch.setenv("EDGECITADEL_CONTAINMENT_MONITOR", "1")
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", ADMIN["x-edgecitadel-admin-token"])
    return tmp_path


def test_main_starts_and_stops_the_monitor_with_the_app(startup):
    with TestClient(main.make_app(for_testing=True)) as client:
        status = wait_for(
            lambda: (
                s := client.get("/api/containment/status", headers=ADMIN).json()
            ).get("state")
            in {"waiting", "running"}
            and s
        )
        assert status["worker_alive"]
    assert client.app  # lifespan exited: the service thread was closed


def test_main_rejects_invalid_monitor_configuration(startup, monkeypatch):
    monkeypatch.setenv("EDGECITADEL_CONTAINMENT_MONITOR", "yes")
    with pytest.raises(
        ValueError, match="^containment_monitor_configuration_unavailable$"
    ):
        main.make_app()
    monkeypatch.setenv("EDGECITADEL_CONTAINMENT_MONITOR", "1")
    monkeypatch.setenv("EDGECITADEL_TRACE_COLLECTOR", "0")
    with pytest.raises(ValueError, match="^containment_monitor_collector_required$"):
        main.make_app()
    monkeypatch.delenv("EDGECITADEL_CONTAINMENT_MONITOR")
    with TestClient(main.make_app(for_testing=True)) as client:
        assert client.get("/api/containment/status", headers=ADMIN).status_code == 404


def test_configuration_file_builds_the_authority_mirror_and_control(
    startup, monkeypatch
):
    config = startup / "containment.json"
    config.write_text(
        json.dumps(
            {
                "monitor": {
                    "grants": {"p": ["c"]},
                    "horizon": 2,
                    "best_effort_guard": True,
                    "control_latency_ms": {"n0": 120},
                },
                "control": {
                    "agentd": {
                        "n0": {
                            "socket": str(startup / "agentd.sock"),
                            "admin_token_file": str(startup / "admin.token"),
                        }
                    }
                },
            }
        )
    )
    monkeypatch.setenv("EDGECITADEL_CONTAINMENT_CONFIG", str(config))
    service = service_from_environment(startup / "core.db")
    assert (
        service.config.grants == {"p": frozenset({"c"})} and service.config.horizon == 2
    )
    assert (
        service.config.control_latency("n0") == 120 and service.config.best_effort_guard
    )
    assert isinstance(service.control, LocalAgentdControlClient)
    assert service.control.owner == service.config.owner == "core-monitor"
    key = startup / "core-monitor.key"
    key.write_text(secrets.token_hex(32) + "\n")
    short = startup / "short.key"
    short.write_text("x" * 16)
    monkeypatch.setenv("NATS_URL", "nats://127.0.0.1:1")
    config.write_text(
        json.dumps(
            {
                "monitor": {"owner": "lab-monitor"},
                "control": {
                    "nats": {"key_file": str(key), "confirmation_timeout_ms": 2000}
                },
            }
        )
    )
    service = service_from_environment(startup / "core.db")
    try:
        assert isinstance(service.control, NatsControlClient)
        assert service.control.owner == "lab-monitor"
        assert service.control.confirmation_timeout_s == 2
        assert service.control.key == key.read_bytes().strip()
    finally:
        service.control.close()
    for bad in (
        {"monitor": {"grants": {"P": ["c"]}}},
        {"monitor": {"boundary": "maybe"}},
        {"monitor": {"unknown": 1}},
        {"control": {"agentd": {"n0": {"socket": "x"}}}},
        {"control": {"nats": {"key_file": str(short)}}},
        {"control": {"nats": {"key_file": str(key), "command_lifetime_ms": 1000}}},
        {"control": {"nats": {"key_file": str(key)}, "agentd": {}}},
        {"extra": {}},
    ):
        config.write_text(json.dumps(bad))
        with pytest.raises(ConfigError):
            service_from_environment(startup / "core.db")
    assert MonitorConfig.from_json({}) == MonitorConfig()


def test_status_of_unconfigured_control_records_unconfirmed_actions(core_db, tmp_path):
    path, result, _ = core_db
    from aggregator.containment_monitor.model import parse_external_alert
    from aggregator.containment_monitor.service import (
        ContainmentMonitor,
        open_core,
        open_monitor,
    )
    from aggregator.containment_monitor.control import UnconfiguredControlClient

    engine = ContainmentMonitor(
        open_core(path),
        open_monitor(tmp_path / "m.db"),
        MonitorConfig(),
        UnconfiguredControlClient(),
    )
    engine.catch_up()
    engine.submit(
        parse_external_alert(
            {"kind": "validator_failure", "version_id": result}, now_ms=engine.clock()
        )
    )
    (decision,) = engine.handle_alerts()
    assert decision.phase1 and all(
        r.error == "node_unreachable" for r in decision.phase1
    )
    assert engine.status()["in_force"] == []
    engine.close()
    assert Path(tmp_path / "m.db").is_file()


def test_monitor_waits_for_the_collector_epoch(core_db, tmp_path):
    """The collector creates its tables before it inserts the row naming its
    epoch; a monitor starting in between waits instead of failing."""
    path, _, _ = core_db
    with sqlite3.connect(path) as core:
        core.execute("DELETE FROM trace_collector")
    service = ContainmentMonitorService(path, poll_seconds=0.02)
    service.start()
    try:
        wait_for(lambda: service.status().get("fault") == "collector_unavailable")
        with sqlite3.connect(path) as core:
            trace_store.initialize(core)
        wait_for(lambda: service.status().get("state") == "running")
    finally:
        service.close()
