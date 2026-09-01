import hashlib
import time

from fastapi.testclient import TestClient

from aggregator import database as db
from aggregator.main import make_app


def _client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "enrollment.db"))
    monkeypatch.setenv("EDGECITADEL_DB_WIPE", "1")
    monkeypatch.setenv("EDGECITADEL_ADMIN_TOKEN", "admin-test-token")
    monkeypatch.setenv("NATS_TOKEN", "broker-test-token")
    monkeypatch.setenv("NATS_LEAF_USERNAME", "leaf-test-user")
    monkeypatch.setenv("NATS_LEAF_PASSWORD", "leaf-test-password")
    return TestClient(make_app(for_testing=True))


def test_invitation_requires_admin_credential(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        response = client.post(
            "/api/enrollment/invitations",
            json={"agent_id": "macmini-agent"},
        )
    assert response.status_code == 401


def test_invitation_is_single_use_and_does_not_store_plaintext(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        created = client.post(
            "/api/enrollment/invitations",
            headers={"X-EdgeCitadel-Admin-Token": "admin-test-token"},
            json={"agent_id": "macmini-agent", "expires_in_seconds": 600},
        )
        assert created.status_code == 201
        token = created.json()["token"]

        with db._conn() as connection:
            row = connection.execute(
                "SELECT token_hash FROM enrollment_invitations"
            ).fetchone()
        assert row["token_hash"] == hashlib.sha256(token.encode()).hexdigest()
        assert token not in row["token_hash"]

        first = client.post("/api/enrollment/redeem", json={"token": token})
        second = client.post("/api/enrollment/redeem", json={"token": token})

    assert first.status_code == 200
    assert first.json() == {
        "agent_id": "macmini-agent",
        "nats_token": "broker-test-token",
    }
    assert second.status_code == 400


def test_expired_invitation_cannot_be_redeemed(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        token = "x" * 40
        now = time.time()
        db.create_enrollment_invitation(
            token_hash=hashlib.sha256(token.encode()).hexdigest(),
            agent_id="expired-agent",
            created_at=now - 120,
            expires_at=now - 60,
        )
        response = client.post("/api/enrollment/redeem", json={"token": token})

    assert response.status_code == 400


def test_nats_leaf_enrollment_returns_only_leaf_credential(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        created = client.post(
            "/api/enrollment/invitations",
            headers={"X-EdgeCitadel-Admin-Token": "admin-test-token"},
            json={"agent_id": "leaf-edge"},
        )
        response = client.post(
            "/api/enrollment/redeem",
            json={"token": created.json()["token"], "messaging_mode": "nats_leaf"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "agent_id": "leaf-edge",
        "leaf_username": "leaf-test-user",
        "leaf_password": "leaf-test-password",
    }


def test_missing_leaf_configuration_does_not_consume_invitation(tmp_path, monkeypatch):
    with _client(tmp_path, monkeypatch) as client:
        created = client.post(
            "/api/enrollment/invitations",
            headers={"X-EdgeCitadel-Admin-Token": "admin-test-token"},
            json={"agent_id": "leaf-edge"},
        )
        token = created.json()["token"]
        monkeypatch.delenv("NATS_LEAF_PASSWORD")
        first = client.post(
            "/api/enrollment/redeem",
            json={"token": token, "messaging_mode": "nats_leaf"},
        )
        monkeypatch.setenv("NATS_LEAF_PASSWORD", "leaf-test-password")
        second = client.post(
            "/api/enrollment/redeem",
            json={"token": token, "messaging_mode": "nats_leaf"},
        )

    assert first.status_code == 503
    assert second.status_code == 200
