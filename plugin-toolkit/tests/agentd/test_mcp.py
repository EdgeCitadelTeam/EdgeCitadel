from __future__ import annotations

import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from edgecitadel_agentd.client import AgentdClient, AgentdClientError
from edgecitadel_agentd.mcp import TOOLS, NativeMcpServer
from edgecitadel_agentd.service import serve, socket_path_for


def test_native_mcp_session_tools_and_cleanup(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    service_dir = state_dir / "agentd"
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(service_dir, stop), daemon=True)
    thread.start()
    socket_path = socket_path_for(service_dir)
    for _ in range(100):
        if socket_path.exists():
            break
        stop.wait(0.01)
    else:
        raise AssertionError("agentd socket did not become ready")

    try:
        admin = AgentdClient(
            socket_path,
            admin_token=(service_dir / "admin.token").read_text().strip(),
        )
        codex_registration = cast(
            Mapping[str, object],
            admin.call(
                "connector.register",
                connector_id="codex-local",
                host_type="codex",
                agent_id="edge-one-codex",
                capabilities=[str(tool["name"]) for tool in TOOLS],
            ),
        )
        token_path = state_dir / "connectors/codex-local.token"
        token_path.parent.mkdir(mode=0o700, parents=True)
        token_path.write_text(str(codex_registration["token"]) + "\n")
        token_path.chmod(0o600)
        server = NativeMcpServer(
            state_dir=state_dir,
            connector_id="codex-local",
            host_type="codex",
            agent_id="edge-one-codex",
        )
        initialized = server.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {},
            }
        )
        assert initialized is not None
        initialized_result = cast(Mapping[str, object], initialized["result"])
        server_info = cast(Mapping[str, object], initialized_result["serverInfo"])
        assert server_info["name"] == "edgecitadel"

        tools = server.handle(
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        assert tools is not None
        tools_result = cast(Mapping[str, object], tools["result"])
        tool_items = cast(list[Mapping[str, object]], tools_result["tools"])
        names = {tool["name"] for tool in tool_items}
        assert "edgecitadel_delegate" in names
        assert "edgecitadel_task_update" in names

        sender_registration = cast(
            Mapping[str, object],
            admin.call(
                "connector.register",
                connector_id="pi-local",
                host_type="pi",
                agent_id="edge-one-pi",
                capabilities=["edgecitadel_delegate", "edgecitadel_task_status"],
            ),
        )
        sender = AgentdClient(
            socket_path,
            connector_id="pi-local",
            token=str(sender_registration["token"]),
        )
        sender.call("session.open")
        incoming = cast(
            Mapping[str, object],
            sender.call(
                "task.create",
                recipient_id="edge-one-codex",
                payload={"body": "review this"},
            ),
        )
        for request_id, state, result in (
            (30, "accepted", None),
            (31, "running", None),
            (32, "completed", {"body": "review complete"}),
        ):
            update = server.handle(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {
                        "name": "edgecitadel_task_update",
                        "arguments": {
                            "task_id": incoming["task_id"],
                            "state": state,
                            **({"result": result} if result is not None else {}),
                        },
                    },
                }
            )
            assert update is not None
        completed = cast(
            Mapping[str, object],
            sender.call("task.get", task_id=incoming["task_id"]),
        )
        assert completed["result"] == {"body": "review complete"}

        delegated = server.handle(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "edgecitadel_delegate",
                    "arguments": {
                        "recipient_id": "remote-agent",
                        "request": "review",
                    },
                },
            }
        )
        assert delegated is not None
        delegated_result = cast(Mapping[str, object], delegated["result"])
        payload = cast(Mapping[str, object], delegated_result["structuredContent"])
        assert payload["state"] == "queued"
        server.close()
        server = None
    finally:
        if "server" in locals() and server is not None:
            server.close()
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive()

    assert (state_dir / "connectors/codex-local.token").stat().st_mode & 0o777 == 0o600


def test_native_packages_do_not_contain_broker_credentials() -> None:
    root = Path(__file__).parents[3] / "native-plugins"
    forbidden = ("NATS_TOKEN", "LEAF_PASSWORD", "plugin_nats_token")
    files = [path for path in root.rglob("*") if path.is_file()]
    assert files
    for path in files:
        content = path.read_text(errors="replace")
        assert not any(value in content for value in forbidden), path


def test_native_mcp_lease_renewer_retries_after_agentd_unavailable() -> None:
    class Stop:
        calls = 0

        def wait(self, _timeout: float) -> bool:
            self.calls += 1
            return self.calls > 2

    class Client:
        calls = 0

        def call(self, operation: str, **params: object) -> object:
            assert operation == "session.renew"
            assert params == {"session_id": "session-one"}
            self.calls += 1
            if self.calls == 1:
                raise AgentdClientError("agentd is unavailable")
            return {"lease_expires_at_ms": 1}

    server = NativeMcpServer.__new__(NativeMcpServer)
    server._stop = cast(Any, Stop())
    server.client = cast(Any, Client())
    server.session_id = "session-one"
    server._session_lock = threading.Lock()

    server._renew_lease()

    assert server.client.calls == 2


def test_native_mcp_lease_renewer_reopens_an_expired_session() -> None:
    class Stop:
        calls = 0

        def wait(self, _timeout: float) -> bool:
            self.calls += 1
            return self.calls > 1

    class Client:
        operations: list[tuple[str, dict[str, object]]] = []

        def call(self, operation: str, **params: object) -> object:
            self.operations.append((operation, params))
            if operation == "session.renew":
                raise AgentdClientError("active session was not found")
            assert operation == "session.open"
            return {"session_id": "session-two"}

    server = NativeMcpServer.__new__(NativeMcpServer)
    server._stop = cast(Any, Stop())
    server.client = cast(Any, Client())
    server.session_id = "session-one"
    server._session_lock = threading.Lock()

    server._renew_lease()

    assert server.session_id == "session-two"
    assert server.client.operations == [
        ("session.renew", {"session_id": "session-one"}),
        ("session.open", {}),
    ]
