import json
from contextlib import contextmanager
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_trace_graph_pages import project_all
from test_trace_graph_projection import TRACE
from test_trace_projection_coverage import put
from test_trace_projection_store import core as core_fixture, event

from aggregator.trace_read_service import TraceReadService
from aggregator.trace_read_routes import make_trace_router
from aggregator import trace_read_routes as routes

core = core_fixture
TOKEN = "owned-fleet-read-only-fixture-token-32bytes"
ORIGIN = "http://testserver"


@contextmanager
def client_for(core, tmp_path, token=TOKEN):
    path = core.execute("PRAGMA database_list").fetchone()[2]
    from pathlib import Path

    service = TraceReadService(
        Path(path), tmp_path / "cursor.key", read_token=token, allowed_origins={ORIGIN}
    )
    app = FastAPI()
    app.include_router(make_trace_router(service))
    try:
        with TestClient(app) as client:
            yield client, service
    finally:
        service.close()


def auth():
    return {"Authorization": "Bearer " + TOKEN, "Origin": ORIGIN}


def seed(core):
    put(core, event(seq=1, trace_id=TRACE), str(uuid4()), 1)
    project_all(core)


def graph(client):
    response = client.get("/api/traces/" + TRACE, headers=auth())
    assert response.status_code == 200
    return response.json()


def authenticate(socket, token=TOKEN):
    socket.send_json({"type": "authenticate", "token": token})


def test_every_http_read_requires_credential_before_lookup(core, tmp_path, monkeypatch):
    with client_for(core, tmp_path) as (client, service):

        async def forbidden(*args, **kwargs):
            pytest.fail("unauthorized request reached query workers")

        monkeypatch.setattr(service, "query", forbidden)
        for path in (
            "/api/traces",
            "/api/traces/PRIVATE_SENTINEL",
            "/api/traces/PRIVATE_SENTINEL/events",
            "/api/traces/PRIVATE_SENTINEL/changes",
        ):
            response = client.get(
                path,
                headers={
                    "X-Forwarded-For": "127.0.0.1",
                    "X-EdgeCitadel-Admin-Token": TOKEN,
                },
            )
            assert response.status_code == 401
            assert response.json()["code"] == "not_authorized"
            assert (
                "PRIVATE_SENTINEL" not in response.text and TOKEN not in response.text
            )


def test_http_routes_share_real_graph_event_change_and_list_cursors(core, tmp_path):
    seed(core)
    with client_for(core, tmp_path) as (client, _):
        listed = client.get("/api/traces", headers=auth())
        assert (
            listed.status_code == 200 and listed.json()["items"][0]["trace_id"] == TRACE
        )
        assert listed.headers["cache-control"] == "no-store"
        assert listed.headers["x-edgecitadel-access-mode"] == "trusted-fleet"
        snapshot = graph(client)
        put(core, event("completed", seq=2, trace_id=TRACE), str(uuid4()), 1)
        project_all(core)
        observed = client.get(
            f"/api/traces/{TRACE}/events",
            params={"as_of": snapshot["at"]},
            headers=auth(),
        )
        assert observed.status_code == 200 and len(observed.json()["events"]) == 1
        changes = client.get(
            f"/api/traces/{TRACE}/changes",
            params={"after": snapshot["resume_cursor"]},
            headers=auth(),
        )
        assert changes.status_code == 200 and len(changes.json()["changes"]) == 1
        assert changes.json()["changes"][0]["upsert_nodes"][0]["state"] == "completed"
        wrong = client.get(
            f"/api/traces/{TRACE}/changes",
            params={"after": snapshot["at"]},
            headers=auth(),
        )
        assert (
            wrong.status_code == 400 and wrong.json()["code"] == "cursor_scope_mismatch"
        )


@pytest.mark.parametrize(
    "query", ["limit=PRIVATE_SENTINEL", "limit=1&limit=2", "unknown=PRIVATE_SENTINEL"]
)
def test_query_errors_are_fixed_and_do_not_echo_input(core, tmp_path, query):
    with client_for(core, tmp_path) as (client, _):
        response = client.get("/api/traces?" + query, headers=auth())
        assert (
            response.status_code == 400 and response.json()["code"] == "invalid_request"
        )
        assert "PRIVATE_SENTINEL" not in response.text


def test_credential_restart_preserves_cursors_and_rotation_invalidates_scope(
    core, tmp_path
):
    seed(core)
    with client_for(core, tmp_path) as (client, _):
        at = graph(client)["at"]
    with client_for(core, tmp_path) as (client, _):
        assert (
            client.get(
                f"/api/traces/{TRACE}", params={"at": at}, headers=auth()
            ).status_code
            == 200
        )
    replacement = "rotated-read-only-fleet-fixture-token-32bytes"
    with client_for(core, tmp_path, replacement) as (client, _):
        assert client.get("/api/traces", headers=auth()).status_code == 401
        changed = client.get(
            f"/api/traces/{TRACE}",
            params={"at": at},
            headers={"Authorization": "Bearer " + replacement},
        )
        assert (
            changed.status_code == 400
            and changed.json()["code"] == "cursor_scope_mismatch"
        )


def test_http_and_websocket_origin_and_opening_credential_checks(
    core, tmp_path, monkeypatch
):
    with client_for(core, tmp_path) as (client, service):

        async def forbidden(*args, **kwargs):
            pytest.fail("denied request performed a trace lookup")

        monkeypatch.setattr(service, "query", forbidden)
        assert (
            client.get(
                "/api/traces", headers={**auth(), "Origin": "https://untrusted.invalid"}
            ).status_code
            == 403
        )
        with pytest.raises(WebSocketDisconnect) as denied:
            with client.websocket_connect(
                f"/ws/traces/{TRACE}?after=PRIVATE_SENTINEL",
                headers={"Origin": "https://untrusted.invalid"},
            ):
                pass
        assert denied.value.code == 4403
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=PRIVATE_SENTINEL", headers={"Origin": ORIGIN}
        ) as socket:
            authenticate(socket, "PRIVATE_SENTINEL")
            error = socket.receive_json()
            assert error[
                "code"
            ] == "not_authorized" and "PRIVATE_SENTINEL" not in json.dumps(error)
            with pytest.raises(WebSocketDisconnect) as denied:
                socket.receive_json()
            assert denied.value.code == 4401


def test_websocket_replays_then_heartbeats_and_reconnects_from_applied_cursor(
    core, tmp_path
):
    seed(core)
    with client_for(core, tmp_path) as (client, _):
        first = graph(client)
        put(core, event("completed", seq=2, trace_id=TRACE), str(uuid4()), 1)
        project_all(core)
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=" + first["resume_cursor"],
            headers={"Origin": ORIGIN},
        ) as socket:
            authenticate(socket)
            change = socket.receive_json()
            heartbeat = socket.receive_json()
            assert change["kind"] == "trace_change"
            assert heartbeat["kind"] == "trace_heartbeat"
            assert change["change"]["upsert_nodes"][0]["state"] == "completed"
            cursor = change["change"]["cursor"]
        put(core, event("failed", seq=3, trace_id=TRACE), str(uuid4()), 1)
        project_all(core)
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=" + cursor, headers={"Origin": ORIGIN}
        ) as socket:
            authenticate(socket)
            next_change = socket.receive_json()
            assert next_change["kind"] == "trace_change"
            assert next_change["change"]["upsert_nodes"][0]["conflict"]


def test_websocket_auth_timeout_does_not_leave_a_session(core, tmp_path, monkeypatch):
    monkeypatch.setattr(routes, "AUTH_TIMEOUT_SECONDS", 0.01)
    with client_for(core, tmp_path) as (client, _):
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=x", headers={"Origin": ORIGIN}
        ) as socket:
            with pytest.raises(WebSocketDisconnect) as timeout:
                socket.receive_json()
            assert timeout.value.code == 1013


def test_websocket_slow_send_closes_and_original_resume_still_replays(
    core, tmp_path, monkeypatch
):
    import asyncio
    from starlette.websockets import WebSocket

    seed(core)
    with client_for(core, tmp_path) as (client, _):
        initial = graph(client)
        put(core, event("completed", seq=2, trace_id=TRACE), str(uuid4()), 1)
        project_all(core)
        original = WebSocket.send_text

        async def stalled(self, data):
            await asyncio.sleep(10)

        monkeypatch.setattr(WebSocket, "send_text", stalled)
        monkeypatch.setattr(routes, "SEND_TIMEOUT_SECONDS", 0.01)
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=" + initial["resume_cursor"],
            headers={"Origin": ORIGIN},
        ) as socket:
            authenticate(socket)
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_json()
            assert closed.value.code == 1013
        monkeypatch.setattr(WebSocket, "send_text", original)
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=" + initial["resume_cursor"],
            headers={"Origin": ORIGIN},
        ) as socket:
            authenticate(socket)
            assert (
                socket.receive_json()["change"]["upsert_nodes"][0]["state"]
                == "completed"
            )


def test_websocket_admission_is_bounded_before_authentication(
    core, tmp_path, monkeypatch
):
    monkeypatch.setattr(routes, "MAX_SOCKETS", 1)
    with client_for(core, tmp_path) as (client, _):
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=x", headers={"Origin": ORIGIN}
        ):
            with pytest.raises(WebSocketDisconnect) as full:
                with client.websocket_connect(
                    f"/ws/traces/{TRACE}?after=x", headers={"Origin": ORIGIN}
                ):
                    pass
            assert full.value.code == 1013
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=x", headers={"Origin": ORIGIN}
        ) as socket:
            authenticate(socket, "bad")
            assert socket.receive_json()["code"] == "not_authorized"


def test_websocket_rebuild_between_replay_and_heartbeat_requires_resnapshot(
    core, tmp_path, monkeypatch
):
    import sqlite3
    from contextlib import closing
    from aggregator import trace_projection_rebuild as rebuild
    from aggregator import trace_projection_store as projection

    seed(core)
    path = core.execute("PRAGMA database_list").fetchone()[2]
    with client_for(core, tmp_path) as (client, service):
        initial = graph(client)
        original = service.query
        switched = False

        async def switch_before_status(kind, **parameters):
            nonlocal switched
            if kind == "status" and not switched:
                with closing(sqlite3.connect(path)) as writer:
                    candidate = rebuild.begin(writer)
                    projection.project_batch(
                        writer, build_generation=candidate.generation
                    )
                    rebuild.activate(writer, generation=candidate.generation)
                switched = True
            return await original(kind, **parameters)

        monkeypatch.setattr(service, "query", switch_before_status)
        with client.websocket_connect(
            f"/ws/traces/{TRACE}?after=" + initial["resume_cursor"],
            headers={"Origin": ORIGIN},
        ) as socket:
            authenticate(socket)
            error = socket.receive_json()
            assert (
                error["code"] == "generation_changed" and error["resnapshot_required"]
            )
            with pytest.raises(WebSocketDisconnect) as closed:
                socket.receive_json()
            assert closed.value.code == 1008
        assert switched
