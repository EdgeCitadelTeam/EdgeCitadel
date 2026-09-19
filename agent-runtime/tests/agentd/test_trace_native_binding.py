"""M2 candidate seam on real native MCP sessions/private RPC, not app hook proof."""

import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from storage_test_support import paired_connect

import pytest

from edgecitadel_agentd.client import AgentdClient
from edgecitadel_agentd.mcp import TOOLS, NativeMcpServer
from edgecitadel_agentd.service import serve, socket_path_for
from edgecitadel_agentd.trace_authority import (
    BindingAuthority,
    SessionAuthority,
    authorize_bind,
    authorize_binding_operation,
)


class CandidateNativeServer(NativeMcpServer):
    """Prototype holds a binding on the MCP session, outside model arguments.

    Production M3 must move authority lookup + dispatch into one store transaction;
    this test deliberately does not claim to solve that race or native turn hooks.
    """

    def _native_delegate(self, arguments, request_id):
        return self._call_tool("edgecitadel_delegate", arguments)

    def issue_root(self, context_id):
        session = self.authority()
        authorize_bind(session, now_ms=int(time.time() * 1000), task=None)
        self.binding = BindingAuthority(
            self.connector_id, self.agent_id, self.session_id, None
        )
        self.trace_id = uuid4().hex
        self.context_id = context_id
        self.attempt_id = str(uuid4())

    def authority(self):
        path = self.state_dir / "agentd/agentd.sqlite3"
        with paired_connect(path) as db:
            db.row_factory = sqlite3.Row
            row = db.execute(
                "SELECT c.connector_id, c.agent_id, c.revoked_at_ms, c.capabilities_json, "
                "s.session_id, s.closed_at_ms, s.lease_expires_at_ms "
                "FROM sessions s JOIN connectors c USING(connector_id) "
                "WHERE s.session_id=? AND c.connector_id=?",
                (self.session_id, self.connector_id),
            ).fetchone()
        assert row is not None
        return SessionAuthority(
            row["connector_id"],
            row["agent_id"],
            row["session_id"],
            row["revoked_at_ms"] is not None,
            row["closed_at_ms"] is not None,
            row["lease_expires_at_ms"],
            "edgecitadel_trace" in json.loads(row["capabilities_json"])["items"],
        )

    def _call_tool(self, name, arguments):
        if name != "edgecitadel_delegate":
            return super()._call_tool(name, arguments)
        if set(arguments) != {"recipient_id", "request"}:
            raise ValueError("invalid native delegation arguments")
        authorize_binding_operation(
            self.authority(),
            self.binding,
            now_ms=int(time.time() * 1000),
            task=None,
            operation="dispatch",
            delegation_allowed=True,
        )
        return self.client.call(
            "task.create",
            recipient_id=arguments["recipient_id"],
            context_id=self.context_id,
            trace_id=self.trace_id,
            payload={
                "body": arguments["request"],
                "execution_context": {
                    "schema_version": 1,
                    "context_origin": "source_explicit",
                    "parent_run_id": self.trace_id,
                },
            },
        )


def delegate(server, **extra):
    reply = server.handle(
        {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/call",
            "params": {
                "name": "edgecitadel_delegate",
                "arguments": {
                    "recipient_id": "owned-remote",
                    "request": "owned deterministic request",
                    **extra,
                },
            },
        }
    )
    return reply["result"]


@pytest.fixture
def native_pair(tmp_path):
    state = tmp_path / "state"
    service = state / "agentd"
    stop = threading.Event()
    thread = threading.Thread(target=serve, args=(service, stop), daemon=True)
    thread.start()
    servers = []
    try:
        for _ in range(200):
            if socket_path_for(service).exists() and (service / "admin.token").exists():
                break
            stop.wait(0.01)
        else:
            raise AssertionError("owned agentd did not start")
        admin = AgentdClient(
            socket_path_for(service),
            admin_token=(service / "admin.token").read_text().strip(),
        )
        registration = admin.call(
            "connector.register",
            connector_id="codex-owned",
            host_type="codex",
            agent_id="owned-codex",
            capabilities=[tool["name"] for tool in TOOLS],
        )
        token = state / "connectors/codex-owned.token"
        token.parent.mkdir(mode=0o700)
        token.write_text(registration["token"] + "\n")
        token.chmod(0o600)
        context = str(uuid4())
        for _ in range(2):
            server = CandidateNativeServer(
                state_dir=state,
                connector_id="codex-owned",
                host_type="codex",
                agent_id="owned-codex",
            )
            servers.append(server)
            server.issue_root(context)
        yield servers
    finally:
        for server in servers:
            server.close()
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_two_native_sessions_share_conversation_without_sharing_root(native_pair):
    first, second = native_pair
    assert first.session_id != second.session_id
    assert first.trace_id != second.trace_id
    assert first.attempt_id != second.attempt_id
    assert first.context_id == second.context_id
    assert first.client.call("task.list") == []  # Root creation did not create work.
    barrier = threading.Barrier(2)

    def call(server):
        barrier.wait(timeout=3)
        return delegate(server)["structuredContent"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        children = list(pool.map(call, native_pair))
    for server, child in zip(native_pair, children, strict=True):
        assert child["trace_id"] == server.trace_id
        assert child["context_id"] == server.context_id
        assert child["payload"]["execution_context"]["parent_run_id"] == server.trace_id
        assert "parent_task_id" not in child["payload"]
    assert len(first.client.call("task.list")) == 2


def test_native_callback_rejects_model_parent_and_old_session_binding(native_pair):
    server, other = native_pair
    assert delegate(server, parent_task_id=str(uuid4()))["isError"]
    assert delegate(server, trace_id=other.trace_id)["isError"]
    original = server.trace_id
    server.client.call("session.close", session_id=server.session_id)
    assert delegate(server)["isError"]
    reopened = server.client.call("session.open")
    with server._session_lock:
        server.session_id = reopened["session_id"]
    assert delegate(server)["isError"]  # Renew/reopen cannot transfer old authority.
    server.issue_root(server.context_id)
    assert server.trace_id != original
    assert "structuredContent" in delegate(server)
    assert len(server.client.call("task.list")) == 1


def test_durable_bind_and_append_over_real_private_socket(native_pair):
    server = native_pair[0]
    (server.state_dir / "node.json").write_text('{"agent_id":"owned-edge"}')
    params = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "session_id": server.session_id,
        "task_id": None,
        "context_id": None,
    }
    bound = server.client.call("trace.bind", **params)
    assert bound["status"] == "ok"
    assert server.client.call("trace.bind", **params) == bound
    observation = {
        "schema_version": 1,
        "binding_id": bound["result"]["binding_id"],
        "observation_id": str(uuid4()),
        "observation": {
            "schema_version": 1,
            "kind": "tool",
            "phase": "started",
            "span_id": str(uuid4()),
            "parent_span_id": None,
            "occurred_at": "2026-09-16T12:00:00.000Z",
            "duration_ms": None,
            "attributes": {"name": "owned-fixture-tool"},
        },
    }
    receipt = server.client.call("trace.append", **observation)
    assert receipt["status"] == "ok"
    assert server.client.call("trace.append", **observation) == receipt
    closure = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": bound["result"]["binding_id"],
        "outcome": "unknown",
        "reason": "unknown",
    }
    closed = server.client.call("trace.finish", **closure)
    assert closed["status"] == "ok"
    assert server.client.call("trace.finish", **closure) == closed
    assert server.client.call("trace.append", **observation)["code"] == "binding_closed"
    assert server.client.call("task.list") == []


def test_durable_dispatch_over_real_private_socket(native_pair):
    server = native_pair[0]
    (server.state_dir / "node.json").write_text('{"agent_id":"owned-edge"}')
    bound = server.client.call(
        "trace.bind",
        schema_version=1,
        request_id=str(uuid4()),
        session_id=server.session_id,
        task_id=None,
        context_id=None,
    )
    params = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "binding_id": bound["result"]["binding_id"],
        "recipient_id": "owned-remote",
        "request": "owned dispatch request",
        "skill_id": None,
        "deadline_at_ms": None,
    }
    receipt = server.client.call("trace.dispatch", **params)
    assert receipt["status"] == "ok"
    assert server.client.call("trace.dispatch", **params) == receipt
    assert len(server.client.call("task.list")) == 1
    assert receipt["result"]["parent_run_id"] == bound["result"]["trace_id"]


def test_mcp_metadata_dispatch_is_atomic_and_rejects_model_parent(native_pair):
    server = native_pair[0]
    (server.state_dir / "node.json").write_text('{"agent_id":"owned-edge"}')
    binding = server.client.call(
        "trace.bind",
        schema_version=1,
        request_id=str(uuid4()),
        session_id=server.session_id,
        task_id=None,
        context_id=None,
    )["result"]
    params = {
        "name": "edgecitadel_delegate",
        "arguments": {"recipient_id": "owned-remote", "request": "owned child"},
        "_meta": {
            "edgecitadel_execution": {
                "schema_version": 1,
                "binding_id": binding["binding_id"],
                "request_id": str(uuid4()),
            }
        },
    }

    def call(params):
        return server.handle(
            {
                "jsonrpc": "2.0",
                "id": str(uuid4()),
                "method": "tools/call",
                "params": params,
            }
        )["result"]

    first = call(params)
    assert first["structuredContent"]["parent_run_id"] == binding["trace_id"]
    assert call(params)["structuredContent"] == first["structuredContent"]
    assert len(server.client.call("task.list")) == 1

    assert call(
        {**params, "arguments": {**params["arguments"], "parent_task_id": str(uuid4())}}
    )["isError"]
    assert call({**params, "_meta": "malformed"})["isError"]
    assert len(server.client.call("task.list")) == 1


def test_production_native_roots_retry_isolation_and_session_replacement(native_pair):
    prototype = native_pair[0]
    (prototype.state_dir / "node.json").write_text('{"agent_id":"owned-edge"}')
    servers = [
        NativeMcpServer(
            state_dir=prototype.state_dir,
            connector_id=prototype.connector_id,
            host_type="codex",
            agent_id=prototype.agent_id,
        )
        for _ in range(2)
    ]
    request = {
        "jsonrpc": "2.0",
        "id": 17,
        "method": "tools/call",
        "params": {
            "name": "edgecitadel_delegate",
            "arguments": {
                "recipient_id": "owned-remote",
                "request": "owned native child",
            },
        },
    }
    try:
        assert servers[0].client.call("task.list") == []
        with ThreadPoolExecutor(max_workers=2) as pool:
            replies = list(pool.map(lambda s: s.handle(request)["result"], servers))
        children = [r["structuredContent"] for r in replies]
        assert children[0]["parent_run_id"] != children[1]["parent_run_id"]
        for server, child in zip(servers, children, strict=True):
            assert server.handle(request)["result"]["structuredContent"] == child
            assert child["parent_task_id"] is None
        assert len(servers[0].client.call("task.list")) == 2
        server = servers[0]
        forged = {
            **request,
            "id": 18,
            "params": {
                **request["params"],
                "arguments": {
                    **request["params"]["arguments"],
                    "parent_run_id": children[1]["parent_run_id"],
                },
            },
        }
        assert server.handle(forged)["result"]["isError"]
        old_binding = server._root_binding[1]
        server.client.call("session.close", session_id=server.session_id)
        assert server.handle(request)["result"]["isError"]
        with server._session_lock:
            server.session_id = server.client.call("session.open")["session_id"]
        replacement = server.handle(request)["result"]["structuredContent"]
        assert replacement["parent_run_id"] != children[0]["parent_run_id"]
        assert server._root_binding[1] != old_binding
        assert len(server.client.call("task.list")) == 3
    finally:
        for server in servers:
            server.close()
    with paired_connect(prototype.state_dir / "agentd/agentd.sqlite3") as db:
        bindings = db.execute(
            "SELECT task_id, closed_at_ms FROM trace_bindings"
        ).fetchall()
        assert len(bindings) == 3
        assert all(
            task_id is None and closed is not None for task_id, closed in bindings
        )
