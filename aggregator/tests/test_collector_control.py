import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from aggregator.main import make_app
from aggregator.trace_collector import TraceCollectorService


@pytest.mark.parametrize("token", [None, "wrong"])
def test_control_requires_existing_admin_credential(tmp_path, monkeypatch, token):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", "admin-control")
    with TestClient(make_app(for_testing=True)) as client:
        response = client.post(
            "/api/system/telemetry/control",
            json={"action": "stop"},
            headers={"X-EdgeCitadel-Admin-Token": token} if token else {},
        )
        assert response.status_code == 401


def test_control_cannot_enable_disabled_collector_and_rejects_extra_fields(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "core.db"))
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", "admin-control")
    with TestClient(make_app(for_testing=True)) as client:
        headers = {"X-EdgeCitadel-Admin-Token": "admin-control"}
        for action in ("stop", "start", "retry"):
            assert (
                client.post(
                    "/api/system/telemetry/control",
                    json={"action": action},
                    headers=headers,
                ).status_code
                == 409
            )
        for params in ({"action": "delete"}, {"action": "start", "force": True}):
            assert (
                client.post(
                    "/api/system/telemetry/control", json=params, headers=headers
                ).status_code
                == 422
            )


def test_concurrent_controls_and_terminal_close(tmp_path, monkeypatch):
    collector = TraceCollectorService(tmp_path / "core.db", "unused", "unused")

    async def run():
        await asyncio.Event().wait()

    monkeypatch.setattr(collector, "_run", run)
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [
                pool.submit(collector.control, action)
                for action in ("start", "stop", "retry") * 3
            ]
            for future in futures:
                future.result(timeout=15)
        collector.close()
        assert not collector._thread.is_alive()
        assert collector.status()["state"] == "stopped"
        with pytest.raises(RuntimeError, match="collector_lifecycle_closed"):
            collector.control("start")
        collector.start()
        assert not collector._thread.is_alive()
        assert not (tmp_path / "core.db").exists()
    finally:
        collector.close()
