import json
import sqlite3
import threading
import time
from contextlib import closing
from uuid import uuid4

import pytest
from test_trace_projection_coverage import put
from test_trace_projection_store import event

from aggregator import trace_payloads, trace_store
from aggregator import trace_projection_store as projection
from aggregator import trace_projector as worker


def prepare(path, separated=False):
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        trace_store.initialize(db)
        if separated:
            trace_payloads.prepare(db)
            assert trace_payloads.migrate_batch(db)


@pytest.fixture(params=[False, True], ids=["inline", "separated"])
def core_path(tmp_path, request):
    path = tmp_path / "core.db"
    prepare(path, request.param)
    return path


def wait_for(service, predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = service.status()
        if predicate(status):
            return status
        time.sleep(0.005)
    pytest.fail(f"projector state did not converge: {service.status()}")


def running(service, through=0):
    return wait_for(
        service,
        lambda s: s["state"] == "running"
        and s["checkpoint"]["ingest_cursor"] >= through,
    )


def ingest(path, seq=1):
    value = event("completed" if seq == 1 else "failed", seq=seq)
    with closing(sqlite3.connect(path)) as db:
        assert (
            put(
                db, value, str(uuid4()), 1, received_at_ms=time.time_ns() // 1_000_000
            ).outcome
            == "accepted"
        )
    return value


def test_owned_worker_projects_restarts_and_closes_without_reset(core_path):
    value = ingest(core_path)
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        service.start()
        first = running(service, 1)
        assert first["worker_alive"] and first["fault"] is None
        assert first["checkpoint"]["lag_ingest_commits"] == 0
        first["checkpoint"]["ingest_cursor"] = -99
        assert service.status()["checkpoint"]["ingest_cursor"] == 1
        ingest(core_path, 2)
        observed = running(service, 2)
        assert observed["last_success_at_ms"] and observed["cycles"] >= 2
        with closing(sqlite3.connect(core_path)) as db:
            result = projection.read_graph(db, trace_id=value["trace_id"])
            assert result["nodes"][0]["conflict"]
        service.stop()
        assert not service.status()["worker_alive"]
        service.start()
        restarted = running(service, 2)
        assert (
            restarted["checkpoint"]["generation"]
            == observed["checkpoint"]["generation"]
        )
        assert restarted["checkpoint"]["change_cursor"] == 2
    finally:
        service.close()
    assert service.status()["state"] == "stopped" and service.status()["closed"]
    with pytest.raises(RuntimeError, match="lifecycle_closed"):
        service.start()
    # No worker connection/transaction retains the write lock after shutdown.
    with closing(sqlite3.connect(core_path, timeout=0)) as db:
        db.execute("BEGIN IMMEDIATE")
        db.rollback()


def test_database_contention_retries_then_recovers(core_path):
    ingest(core_path)
    service = worker.TraceProjectorService(core_path)
    with closing(sqlite3.connect(core_path)) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        try:
            service.start()
            retrying = wait_for(service, lambda s: s["state"] == "retrying")
            assert retrying["fault"] == "projector_database_busy"
            blocker.rollback()
            assert running(service, 1)["fault"] is None
        finally:
            blocker.rollback()
            service.close()


def test_process_ownership_prevents_second_worker_and_releases_on_close(core_path):
    first = worker.TraceProjectorService(core_path)
    second = worker.TraceProjectorService(core_path)
    try:
        first.start()
        observed = running(first)
        second.start()
        paused = wait_for(
            second, lambda s: s["state"] == "paused" and not s["worker_alive"]
        )
        assert paused["fault"] == "projector_writer_active"
        first.close()
        second.start()
        assert (
            running(second)["checkpoint"]["generation"]
            == observed["checkpoint"]["generation"]
        )
    finally:
        first.close()
        second.close()


def test_waiting_worker_never_creates_a_parallel_raw_store(tmp_path):
    path = tmp_path / "not-created-yet.db"
    service = worker.TraceProjectorService(path)
    try:
        service.start()
        wait_for(service, lambda s: s["state"] == "waiting")
        assert not path.exists()
        assert not (tmp_path / (path.name + ".trace-projector")).exists()
        prepare(path)
        status = running(service)
        assert status["checkpoint"]["ingest_cursor"] == 0
        assert status["checkpoint"]["collector_ingest_cursor"] == 0
    finally:
        service.close()


def test_existing_application_db_waits_for_collector_without_migration(tmp_path):
    path = tmp_path / "app.db"
    with closing(sqlite3.connect(path)) as db:
        db.execute("CREATE TABLE owned_application(value)")
    service = worker.TraceProjectorService(path)
    try:
        service.start()
        wait_for(service, lambda s: s["state"] == "waiting")
        with closing(sqlite3.connect(path)) as db:
            assert db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall() == [("owned_application",)]
        prepare(path)
        running(service)
    finally:
        service.close()


def test_non_wal_core_is_paused_without_changing_journal_mode(tmp_path):
    path = tmp_path / "core.db"
    with closing(sqlite3.connect(path)) as db:
        trace_store.initialize(db)
    service = worker.TraceProjectorService(path)
    try:
        service.start()
        status = wait_for(
            service, lambda s: s["state"] == "paused" and not s["worker_alive"]
        )
        assert status["fault"] == "projector_wal_required"
        with closing(sqlite3.connect(path)) as db:
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        service.close()


def test_incompatible_projection_pauses_instead_of_resetting_it(core_path):
    with closing(sqlite3.connect(core_path)) as db:
        state = projection.initialize(db)
        db.execute("UPDATE trace_projection_state SET version=5")
        db.commit()
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        status = wait_for(
            service, lambda s: s["state"] == "paused" and not s["worker_alive"]
        )
        assert status["fault"] == "projector_rebuild_required"
        assert status["cycles"] == 0 and status["checkpoint"] is None
        with closing(sqlite3.connect(core_path)) as db:
            assert db.execute(
                "SELECT version,generation FROM trace_projection_state"
            ).fetchone() == (5, state.generation)
    finally:
        service.close()


def test_collector_restore_fences_a_running_worker(core_path):
    ingest(core_path)
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        running(service, 1)
        with closing(sqlite3.connect(core_path)) as db:
            trace_store.initialize(db)
            db.execute("UPDATE trace_collector SET collector_epoch=?", (str(uuid4()),))
            db.commit()
        status = wait_for(
            service, lambda s: s["state"] == "paused" and not s["worker_alive"]
        )
        assert status["fault"] == "projector_rebuild_required"
    finally:
        service.close()


def test_nontransient_database_failure_is_visible_and_sanitized(core_path):
    ingest(core_path)
    with closing(sqlite3.connect(core_path)) as db:
        projection.initialize(db)
        db.execute(
            "CREATE TRIGGER owned_failure BEFORE INSERT ON trace_projected_tasks BEGIN SELECT RAISE(ABORT,'PRIVATE_PAYLOAD_SENTINEL'); END"
        )
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        status = wait_for(
            service, lambda s: s["state"] == "paused" and not s["worker_alive"]
        )
        assert status["fault"] == "projector_storage_unavailable"
        assert "PRIVATE_PAYLOAD_SENTINEL" not in json.dumps(status)
        with closing(sqlite3.connect(core_path)) as db:
            assert (
                db.execute("SELECT ingest_seq FROM trace_collector").fetchone()[0] == 1
            )
            assert (
                db.execute(
                    "SELECT ingest_cursor FROM trace_projection_state"
                ).fetchone()[0]
                == 0
            )
    finally:
        service.close()


def test_shutdown_interrupts_sql_rolls_back_and_closes_in_owner_thread(
    core_path, monkeypatch
):
    ready = threading.Event()
    closed_in = []
    original_connect = sqlite3.connect

    class ObservedConnection(sqlite3.Connection):
        def close(self):
            closed_in.append(threading.get_ident())
            super().close()

    monkeypatch.setattr(
        worker.sqlite3,
        "connect",
        lambda *args, **kwargs: original_connect(
            *args, **kwargs, factory=ObservedConnection
        ),
    )
    with closing(original_connect(core_path)) as db:
        db.execute("CREATE TABLE owned_probe(value)")

    def slow_cycle(db, **kwargs):
        with db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("INSERT INTO owned_probe VALUES(1)")
            ready.set()
            db.execute(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000) SELECT sum(x) FROM c"
            ).fetchone()
        return {"phase": "projection"}

    monkeypatch.setattr(worker, "run_cycle", slow_cycle)
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        assert ready.wait(5), "worker did not enter owned transaction"
        service.close()
        assert (
            service.status()["state"] == "stopped"
            and not service.status()["worker_alive"]
        )
        assert service.status()["fault"] is None
        assert closed_in and all(
            identity != threading.get_ident() for identity in closed_in
        )
        with closing(original_connect(core_path, timeout=0)) as db:
            assert db.execute("SELECT count(*) FROM owned_probe").fetchone()[0] == 0
            db.execute("BEGIN IMMEDIATE")
            db.rollback()
    finally:
        service.close()


def test_unexpected_software_fault_is_failed_not_healthy(core_path, monkeypatch):
    def fail(db, **kwargs):
        raise RuntimeError("PRIVATE_RUNTIME_SENTINEL")

    monkeypatch.setattr(worker, "run_cycle", fail)
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        status = wait_for(
            service, lambda s: s["state"] == "failed" and not s["worker_alive"]
        )
        assert status["fault"] == "projector_lifecycle_fault"
        assert "PRIVATE_RUNTIME_SENTINEL" not in json.dumps(status)
        assert status["last_success_at_ms"] is None
    finally:
        service.close()


def test_shutdown_timeout_keeps_worker_visible_and_prevents_restart(
    core_path, monkeypatch
):
    ready = threading.Event()
    release = threading.Event()

    def blocked_cycle(db, **kwargs):
        ready.set()
        assert release.wait(5)
        return {"phase": "projection"}

    monkeypatch.setattr(worker, "run_cycle", blocked_cycle)
    monkeypatch.setattr(worker, "SHUTDOWN_SECONDS", 0.01)
    service = worker.TraceProjectorService(core_path)
    try:
        service.start()
        assert ready.wait(5)
        with pytest.raises(RuntimeError, match="shutdown_timeout"):
            service.close()
        status = service.status()
        assert status["state"] == "stopping" and status["worker_alive"]
        assert status["closed"]
        with pytest.raises(RuntimeError, match="lifecycle_closed"):
            service.start()
        release.set()
        wait_for(service, lambda s: not s["worker_alive"])
        assert service.status()["state"] == "stopping"
        assert service.status()["cycles"] == 0
        service.close()
        assert service.status()["state"] == "stopped"
    finally:
        release.set()
        monkeypatch.setattr(worker, "SHUTDOWN_SECONDS", 10)
        service.close()
