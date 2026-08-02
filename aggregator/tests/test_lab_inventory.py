"""Fail-closed contracts for the lab-only reservation API."""

from __future__ import annotations

import hashlib

from fastapi.testclient import TestClient

from aggregator.main import make_app


def _client(tmp_path, monkeypatch) -> TestClient:
    token = "t" * 64
    monkeypatch.setenv("LAB_RUN_ID", "ec-lab-01")
    monkeypatch.setenv("LAB_TOKEN_SHA256", hashlib.sha256(token.encode()).hexdigest())
    monkeypatch.setenv("LAB_INVENTORY_PATH", str(tmp_path / "inventory.sqlite"))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "aggregator.sqlite"))
    client = TestClient(make_app(for_testing=True))
    client.headers.update({"Authorization": f"Bearer {token}"})
    return client


def _reservation() -> dict[str, str]:
    return {
        "agent_id": "shell-1",
        "qualified_agent_id": "ec-lab-01--shell-1",
        "reservation_id": "reservation-1",
        "declared_host_id": "node-lab-01",
    }


def test_lab_routes_are_disabled_without_a_run(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("LAB_RUN_ID", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "aggregator.sqlite"))
    with TestClient(make_app(for_testing=True)) as client:
        assert client.get("/api/lab/status").status_code == 404


def test_lab_reservation_rejects_missing_or_wrong_bearer_token(tmp_path, monkeypatch) -> None:
    with _client(tmp_path, monkeypatch) as client:
        assert client.post("/api/lab/reservations", headers={"Authorization": ""}, json=_reservation()).status_code == 401
        assert client.post("/api/lab/reservations", headers={"Authorization": "Bearer wrong"}, json=_reservation()).status_code == 401


def test_active_reservation_conflicts_and_retained_owner_can_resume(tmp_path, monkeypatch) -> None:
    with _client(tmp_path, monkeypatch) as client:
        first = client.post("/api/lab/reservations", json=_reservation())
        assert first.status_code == 201
        repeated = client.post("/api/lab/reservations", json=_reservation())
        assert repeated.status_code == 409
        assert repeated.json()["detail"] == "agent_id has an active reservation"
        assert client.patch("/api/lab/reservations/shell-1/retain", json=_reservation()).status_code == 200
        assert client.post("/api/lab/reservations", json=_reservation()).status_code == 200


def test_retain_retry_is_idempotent_only_for_the_exact_owner(tmp_path, monkeypatch) -> None:
    with _client(tmp_path, monkeypatch) as client:
        assert client.post("/api/lab/reservations", json=_reservation()).status_code == 201
        assert client.patch(
            "/api/lab/reservations/shell-1/retain", json=_reservation()
        ).status_code == 200
        assert client.patch(
            "/api/lab/reservations/shell-1/retain", json=_reservation()
        ).status_code == 200

        mismatched = {**_reservation(), "reservation_id": "reservation-2"}
        assert client.patch(
            "/api/lab/reservations/shell-1/retain", json=mismatched
        ).status_code == 409
        status = client.get("/api/lab/status").json()
        assert [row["event"] for row in status["reservation_events"]] == [
            "reserved",
            "retained",
        ]


def test_release_preserves_events_and_status_is_secret_free(tmp_path, monkeypatch) -> None:
    with _client(tmp_path, monkeypatch) as client:
        assert client.post("/api/lab/reservations", json=_reservation()).status_code == 201
        assert client.request("DELETE", "/api/lab/reservations/shell-1", json=_reservation()).status_code == 204
        status = client.get("/api/lab/status")
        assert status.status_code == 200
        assert status.json()["run_id"] == "ec-lab-01"
        assert status.json()["reservations"] == []
        assert [row["event"] for row in status.json()["reservation_events"]] == ["reserved", "released"]
        assert "Authorization" not in status.text


def test_node_report_requires_matching_reservation_and_records_peer_separately(tmp_path, monkeypatch) -> None:
    with _client(tmp_path, monkeypatch) as client:
        assert client.post("/api/lab/reservations", json=_reservation()).status_code == 201
        report = {
            **_reservation(),
            "machine_id_sha256": "a" * 64,
            "hostname": "node-lab-01",
            "os_release": "ubuntu-24.04",
            "architecture": "x86_64",
            "launcher_source_commit": "b" * 40,
            "source_snapshot_sha256": "c" * 64,
            "network_path": {
                "source_ip": "127.0.0.1", "destination_ip": "127.0.0.1",
                "interface": "lo", "route_output_sha256": "d" * 64,
                "controller_dns_name": "127.0.0.1",
            },
            "preflight_valid": True,
            "lifecycle_state": "active",
            "cleanup": None,
            "checked_at": "2026-07-27T00:00:00Z",
        }
        response = client.post("/api/lab/node-reports", json=report)
        assert response.status_code == 200
        saved = response.json()
        assert saved["network_path"] == report["network_path"]
        assert saved["server_observed_peer_ip"] == "testclient"
        report["reservation_id"] = "other"
        assert client.post("/api/lab/node-reports", json=report).status_code == 409
