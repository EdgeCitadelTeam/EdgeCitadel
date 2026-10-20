"""Native-host MCP server backed by the private agentd connector API."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

from .client import AgentdClient, AgentdClientError
from .service import socket_path_for

TOOLS = [
    {
        "name": "edgecitadel_agents",
        "description": (
            "List local and recently NATS-observed Agent endpoints. Remote state "
            "is cached when transport is disconnected."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_delegate",
        "description": "Queue a task for another EdgeCitadel Agent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "recipient_id": {"type": "string"},
                "skill_id": {"type": "string"},
                "request": {"type": "string", "maxLength": 16384},
                "deadline_at_ms": {"type": "integer"},
            },
            "required": ["recipient_id", "request"],
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_inbox",
        "description": "List pending tasks for the active native Agent session.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_task_status",
        "description": "Read one EdgeCitadel task and its current state.",
        "inputSchema": {
            "type": "object",
            "properties": {"task_id": {"type": "string"}},
            "required": ["task_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_task_update",
        "description": "Accept, run, complete, fail, reject, or cancel an inbound task.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "state": {
                    "type": "string",
                    "enum": [
                        "accepted",
                        "running",
                        "completed",
                        "failed",
                        "rejected",
                        "cancelled",
                    ],
                },
                "reason": {"type": "string", "maxLength": 1024},
                "result": {
                    "oneOf": [
                        {"type": "string", "maxLength": 65536},
                        {"type": "object"},
                    ]
                },
            },
            "required": ["task_id", "state"],
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_trace",
        "description": (
            "Read durable local trace metadata for this Agent; coverage is partial. "
            "For the next page, pass the returned source_epoch and next_source_seq "
            "as after_source_seq."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "trace_id": {"type": "string"},
                "source_epoch": {"type": "string"},
                "after_source_seq": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 32},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "edgecitadel_diagnose",
        "description": "Check the host-local EdgeCitadel service and database.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
]


class NativeMcpServer:
    def __init__(
        self,
        *,
        state_dir: Path,
        connector_id: str,
        host_type: str,
        agent_id: str,
    ) -> None:
        self.state_dir = state_dir
        self.connector_id = connector_id
        self.host_type = host_type
        self.agent_id = agent_id
        self.token_path = state_dir / "connectors" / f"{connector_id}.token"
        if not self.token_path.is_file():
            raise AgentdClientError(
                "Native connector is not registered; start it through edgecitadel native-mcp"
            )
        token = self.token_path.read_text().strip()
        self.client = AgentdClient(
            socket_path_for(state_dir / "agentd"),
            connector_id=connector_id,
            token=token,
        )
        # A Managed Agent's adapter owns the execution session. Its upstream
        # model gets delegation/status only and must not compete for inbox work.
        self.managed = host_type == "managed-agent"
        self.tools = [
            tool
            for tool in TOOLS
            if not self.managed
            or tool["name"] in {"edgecitadel_delegate", "edgecitadel_task_status"}
        ]
        if self.managed:
            return
        self.client.call(
            "connector.update",
            host_type=host_type,
            agent_id=agent_id,
            capabilities=[tool["name"] for tool in TOOLS],
        )
        opened = cast(Mapping[str, object], self.client.call("session.open"))
        self.session_id = str(opened["session_id"])
        self._root_binding: tuple[str, str] | None = None
        self._session_lock = threading.Lock()
        self._stop = threading.Event()
        self._lease_thread = threading.Thread(target=self._renew_lease, daemon=True)
        self._lease_thread.start()

    def close(self) -> None:
        if self.managed:
            return
        self._stop.set()
        self._lease_thread.join(timeout=2)
        with self._session_lock:
            session_id = self.session_id
        try:
            if self._root_binding is not None and self._root_binding[0] == session_id:
                self.client.call(
                    "trace.finish",
                    schema_version=1,
                    request_id=str(uuid4()),
                    binding_id=self._root_binding[1],
                    outcome="unknown",
                    reason="unknown",
                )
        except AgentdClientError:
            pass
        try:
            self.client.call("session.close", session_id=session_id)
        except AgentdClientError:
            pass

    def _renew_lease(self) -> None:
        while not self._stop.wait(20):
            with self._session_lock:
                session_id = self.session_id
            try:
                self.client.call("session.renew", session_id=session_id)
            except AgentdClientError as error:
                # agentd may be temporarily unavailable during an upgrade or
                # restart. Retry transient failures and replace a session that
                # agentd already expired while it was unavailable.
                if str(error) == "active session was not found":
                    try:
                        opened = cast(
                            Mapping[str, object], self.client.call("session.open")
                        )
                    except AgentdClientError:
                        continue
                    with self._session_lock:
                        if self.session_id == session_id:
                            self.session_id = str(opened["session_id"])
                continue

    def handle(self, request: Mapping[str, object]) -> dict[str, object] | None:
        method = request.get("method")
        request_id = request.get("id")
        if method == "notifications/initialized":
            return None
        if method == "initialize":
            return self._result(
                request_id,
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "edgecitadel", "version": "0.1.0"},
                },
            )
        if method == "tools/list":
            return self._result(request_id, {"tools": self.tools})
        if method == "tools/call":
            params = cast(Mapping[str, object], request.get("params", {}))
            name = params.get("name")
            arguments = cast(dict[str, Any], params.get("arguments", {}))
            try:
                metadata = params.get("_meta", {})
                if not isinstance(metadata, Mapping):
                    raise TypeError("invalid MCP metadata")
                if "edgecitadel_execution" in metadata:
                    if name != "edgecitadel_delegate":
                        raise ValueError("execution metadata requires delegation")
                    result = self._call_bound_delegate(
                        arguments, metadata["edgecitadel_execution"]
                    )
                elif name == "edgecitadel_delegate" and not self.managed:
                    result = self._native_delegate(arguments, request_id)
                else:
                    result = self._call_tool(str(name), arguments)
            except (AgentdClientError, KeyError, TypeError, ValueError) as error:
                return self._result(
                    request_id,
                    {
                        "content": [{"type": "text", "text": str(error)}],
                        "isError": True,
                    },
                )
            return self._result(
                request_id,
                {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, indent=2, sort_keys=True),
                        }
                    ],
                    "structuredContent": result,
                },
            )
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "method not found"},
        }

    def _native_delegate(self, arguments: dict[str, Any], request_id: object) -> object:
        # JSON-RPC IDs are unique within the MCP connection. A repeated call
        # retains its receipt ID without an unbounded in-memory request cache.
        if type(request_id) not in (str, int):
            raise ValueError("delegation requires a JSON-RPC request ID")
        with self._session_lock:
            session_id = self.session_id

            def identity(value: object) -> str:
                digest = hashlib.sha256(
                    json.dumps([session_id, value]).encode()
                ).digest()
                return str(UUID(bytes=digest[:16], version=4))

            reply = self.client.call(
                "trace.bind",
                schema_version=1,
                request_id=identity("native-root"),
                session_id=session_id,
                task_id=None,
                context_id=None,
            )
            if not isinstance(reply, dict) or reply.get("status") != "ok":
                raise AgentdClientError("native_execution_binding_unavailable")
            binding_id = reply["result"]["binding_id"]
            self._root_binding = (session_id, binding_id)
            return self._call_bound_delegate(
                arguments,
                {
                    "schema_version": 1,
                    "binding_id": binding_id,
                    "request_id": identity(["dispatch", request_id]),
                },
            )

    def _call_bound_delegate(
        self, arguments: dict[str, Any], metadata: object
    ) -> object:
        if "edgecitadel_delegate" not in {tool["name"] for tool in self.tools}:
            raise ValueError("unknown EdgeCitadel tool")
        if not isinstance(metadata, Mapping) or set(metadata) != {
            "schema_version",
            "binding_id",
            "request_id",
        }:
            raise ValueError("invalid execution metadata")
        if (
            type(metadata["schema_version"]) is not int
            or metadata["schema_version"] != 1
        ):
            raise ValueError("unsupported execution metadata")
        if not isinstance(arguments, dict) or set(arguments) - {
            "recipient_id",
            "request",
            "skill_id",
            "deadline_at_ms",
        }:
            raise ValueError("invalid delegation arguments")
        reply = self.client.call(
            "trace.dispatch",
            schema_version=1,
            request_id=metadata["request_id"],
            binding_id=metadata["binding_id"],
            recipient_id=arguments["recipient_id"],
            request=arguments["request"],
            skill_id=arguments.get("skill_id"),
            deadline_at_ms=arguments.get("deadline_at_ms"),
        )
        if not isinstance(reply, dict) or reply.get("status") != "ok":
            code = (
                reply.get("code", "dispatch_unavailable")
                if isinstance(reply, dict)
                else "dispatch_unavailable"
            )
            raise AgentdClientError(str(code))
        return reply["result"]

    def _call_tool(self, name: str, arguments: dict[str, Any]) -> object:
        if name not in {tool["name"] for tool in self.tools}:
            raise ValueError("unknown EdgeCitadel tool")
        if name == "edgecitadel_agents":
            return self.client.call("agent.list")
        if name == "edgecitadel_delegate":
            recipient_id = arguments["recipient_id"]
            request = arguments["request"]
            if not isinstance(recipient_id, str) or not isinstance(request, str):
                raise ValueError("recipient_id and request must be strings")
            return self.client.call(
                "task.create",
                recipient_id=recipient_id,
                skill_id=arguments.get("skill_id"),
                payload={"body": request},
                deadline_at_ms=arguments.get("deadline_at_ms"),
            )
        if name == "edgecitadel_inbox":
            return self.client.call(
                "task.list", recipient_id=self.agent_id, include_terminal=False
            )
        if name == "edgecitadel_task_status":
            return self.client.call("task.get", task_id=arguments["task_id"])
        if name == "edgecitadel_task_update":
            task_id = arguments["task_id"]
            state = arguments["state"]
            if not isinstance(task_id, str) or not isinstance(state, str):
                raise ValueError("task_id and state must be strings")
            raw_result = arguments.get("result")
            if isinstance(raw_result, str):
                result: Mapping[str, object] | None = {"body": raw_result}
            elif isinstance(raw_result, Mapping):
                result = cast(Mapping[str, object], raw_result)
            elif raw_result is None:
                result = None
            else:
                raise ValueError("result must be a string or object")
            return self.client.call(
                "task.transition",
                task_id=task_id,
                state=state,
                reason=arguments.get("reason"),
                session_id=self.session_id,
                result=result,
            )
        if name == "edgecitadel_trace":
            return self.client.call("trace.history", **arguments)
        if name == "edgecitadel_diagnose":
            return AgentdClient(socket_path_for(self.state_dir / "agentd")).call(
                "health"
            )
        raise ValueError("unknown EdgeCitadel tool")

    @staticmethod
    def _result(request_id: object, result: object) -> dict[str, object]:
        return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _default_agent_id(state_dir: Path, host_type: str) -> str:
    node_path = state_dir / "node.json"
    node_id = "local"
    if node_path.exists():
        try:
            document = json.loads(node_path.read_text())
            if isinstance(document, dict) and isinstance(document.get("agent_id"), str):
                node_id = document["agent_id"]
        except json.JSONDecodeError:
            pass
    suffix = f"-{host_type}"
    return f"{node_id[: 64 - len(suffix)].rstrip('_-')}{suffix}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edgecitadel-native-mcp")
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument(
        "--host-type",
        choices=("pi", "claude-code", "codex", "managed-agent"),
        required=True,
    )
    parser.add_argument("--connector-id")
    parser.add_argument("--agent-id")
    args = parser.parse_args(argv)
    if args.host_type == "managed-agent" and (
        not args.connector_id or not args.agent_id
    ):
        parser.error("Managed MCP requires the installed connector-id and agent-id")
    connector_id = args.connector_id or f"{args.host_type}-local"
    agent_id = args.agent_id or _default_agent_id(args.state_dir, args.host_type)
    try:
        server = NativeMcpServer(
            state_dir=args.state_dir,
            connector_id=connector_id,
            host_type=args.host_type,
            agent_id=agent_id,
        )
    except AgentdClientError as error:
        print(f"EdgeCitadel service unavailable: {error}", file=sys.stderr)
        return 1
    try:
        for line in sys.stdin:
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError
                response = server.handle(request)
            except (json.JSONDecodeError, ValueError):
                response = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": "parse error"},
                }
            if response is not None:
                print(json.dumps(response, separators=(",", ":")), flush=True)
    finally:
        server.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
