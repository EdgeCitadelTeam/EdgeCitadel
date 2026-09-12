from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.service import serve, socket_path_for
from edgecitadel_agentd.mcp import NativeMcpServer


@pytest.fixture
def service(tmp_path: Path) -> tuple[Path, Path, threading.Event]:
    state_dir = tmp_path / "state"
    socket_path = socket_path_for(state_dir)
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(state_dir, stop), daemon=True)
    thread.start()
    for _ in range(100):
        if socket_path.exists():
            break
        stop.wait(0.01)
    else:
        pytest.fail("agentd socket did not become ready")
    yield socket_path, state_dir, stop
    stop.set()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_managed_mcp_delegation_obeys_package_grant_without_execution_session(
    service: tuple[Path, Path, threading.Event],
) -> None:
    socket_path, service_dir, _ = service
    admin = AgentdClient(
        socket_path, admin_token=(service_dir / "admin.token").read_text().strip()
    )
    registration = admin.call(
        "connector.register",
        connector_id="managed-hermes",
        host_type="managed-agent",
        agent_id="hermes",
        capabilities=["hermes-reasoner"],
    )
    token_path = service_dir.parent / "connectors/managed-hermes.token"
    token_path.parent.mkdir()
    token_path.write_text(registration["token"])
    # This fixture puts the service directly under state; the MCP expects agentd/.
    with patch("edgecitadel_agentd.mcp.socket_path_for", return_value=socket_path):
        server = NativeMcpServer(
            state_dir=service_dir.parent,
            connector_id="managed-hermes",
            host_type="managed-agent",
            agent_id="hermes",
        )
    try:
        assert {tool["name"] for tool in server.tools} == {
            "edgecitadel_delegate",
            "edgecitadel_task_status",
        }
        assert admin.call("health")["active_sessions"] == 0
        with pytest.raises(ValueError, match="unknown"):
            server._call_tool("edgecitadel_inbox", {})
        request = {"recipient_id": "codex", "request": "ping"}
        with pytest.raises(AgentdClientError, match="not authorized by its package"):
            server._call_tool("edgecitadel_delegate", request)
        record = {
            "package_id": "edgecitadel.hermes",
            "desired_state": "running",
            "agent_ids": ["hermes"],
            "outbound_agents": ["codex"],
        }
        admin.call("managed.reconcile", records=[record])
        task = server._call_tool("edgecitadel_delegate", request)
        assert task["sender_id"] == "hermes"
        assert (
            server._call_tool("edgecitadel_task_status", {"task_id": task["task_id"]})
            == task
        )
        with pytest.raises(AgentdClientError, match="not authorized by its package"):
            server._call_tool(
                "edgecitadel_delegate", {**request, "recipient_id": "other"}
            )
        admin.call("managed.reconcile", records=[{**record, "outbound_agents": []}])
        with pytest.raises(AgentdClientError, match="not authorized by its package"):
            server._call_tool("edgecitadel_delegate", request)
        admin.call(
            "managed.reconcile", records=[{**record, "desired_state": "stopped"}]
        )
        with pytest.raises(AgentdClientError, match="not authorized by its package"):
            server._call_tool("edgecitadel_delegate", request)
    finally:
        server.close()
    assert admin.call("health")["active_sessions"] == 0


def test_deep_state_directory_uses_private_bounded_socket_path(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / ("nested-" * 30)
    socket_path = socket_path_for(state_dir)
    assert len(str(socket_path).encode()) <= 100
    assert socket_path.parent.stat().st_mode & 0o777 == 0o700


def test_health_and_authenticated_connector_session(
    service: tuple[Path, Path, threading.Event],
) -> None:
    socket_path, state_dir, _ = service
    anonymous = AgentdClient(socket_path)
    health = anonymous.call("health")
    assert health == {
        "status": "ready",
        "database": "ok",
        "schema_version": 6,
        "active_sessions": 0,
        "database_bytes": health["database_bytes"],
        "telemetry_records": {
            "events": 0,
            "spans": 0,
            "presence_history": 0,
        },
        "transport": {
            "configured": False,
            "connected": False,
            "mode": "unconfigured",
            "detail": "node state is not configured",
        },
    }
    assert isinstance(health["database_bytes"], int)
    assert health["database_bytes"] > 0
    admin = AgentdClient(
        socket_path, admin_token=(state_dir / "admin.token").read_text().strip()
    )
    registration = admin.call(
        "connector.register",
        connector_id="claude-local",
        host_type="claude-code",
        agent_id="edge-one-claude",
        capabilities=["delegate", "inbox", "trace"],
    )
    client = AgentdClient(
        socket_path,
        connector_id="claude-local",
        token=registration["token"],
    )
    session = client.call("session.open", lease_seconds=30)
    assert session["session_id"]
    assert anonymous.call("health")["active_sessions"] == 1
    assert client.call("session.close", session_id=session["session_id"]) == {
        "closed": True
    }


def test_unauthenticated_operations_are_denied(
    service: tuple[Path, Path, threading.Event],
) -> None:
    socket_path, _, _ = service
    with pytest.raises(AgentdClientError, match="authentication is required"):
        AgentdClient(socket_path).call("task.list")
    with pytest.raises(AgentdClientError, match="management authentication failed"):
        AgentdClient(socket_path).call(
            "connector.register",
            connector_id="unauthorized",
            host_type="codex",
            agent_id="unauthorized",
            capabilities=[],
        )


def test_connector_cannot_escalate_or_call_undeclared_capability(
    service: tuple[Path, Path, threading.Event],
) -> None:
    socket_path, state_dir, _ = service
    admin = AgentdClient(
        socket_path, admin_token=(state_dir / "admin.token").read_text().strip()
    )
    registration = admin.call(
        "connector.register",
        connector_id="pi-local",
        host_type="pi",
        agent_id="edge-one-pi",
        capabilities=["edgecitadel_agents"],
    )
    client = AgentdClient(
        socket_path, connector_id="pi-local", token=registration["token"]
    )
    with pytest.raises(AgentdClientError, match="not authorized"):
        client.call("task.create", recipient_id="remote-agent", payload={})
    with pytest.raises(AgentdClientError, match="cannot be changed"):
        client.call(
            "connector.update",
            host_type="pi",
            agent_id="edge-one-pi",
            capabilities=["edgecitadel_agents", "edgecitadel_delegate"],
        )
