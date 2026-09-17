"""Operator-run Hermes HTTP server with execution-bound EdgeCitadel delegation."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import signal
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4


def register_delegation(server: Any, registry: Any, toolsets: dict[str, Any]) -> None:
    from .delegation import scoped_delegate_handler

    name = "edgecitadel_delegate"
    toolset = "edgecitadel-scoped"
    if registry.get_toolset_for_tool(name) is not None or toolset in toolsets:
        raise RuntimeError("EdgeCitadel delegation is already registered")
    definition = next(tool for tool in server.tools if tool["name"] == name)

    def call_mcp(name: str, arguments: dict, metadata: dict) -> Any:
        reply = server.handle(
            {
                "jsonrpc": "2.0",
                "id": str(uuid4()),
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments, "_meta": metadata},
            }
        )
        result = reply["result"]
        if result.get("isError"):
            return {"error": "delegation_rejected"}
        return result["structuredContent"]

    registry.register(
        name=name,
        toolset=toolset,
        schema={
            "name": name,
            "description": definition["description"],
            "parameters": definition["inputSchema"],
        },
        handler=scoped_delegate_handler(call_mcp),
    )
    toolsets[toolset] = {
        "description": "Execution-bound EdgeCitadel delegation",
        "tools": [name],
    }


async def serve(args: argparse.Namespace) -> None:
    from edgecitadel_agentd.mcp import NativeMcpServer

    from .http_bridge import bound_api_adapter

    api = importlib.import_module("gateway.platforms.api_server")
    config = importlib.import_module("gateway.config")
    registry = importlib.import_module("tools.registry").registry
    toolsets = importlib.import_module("toolsets").TOOLSETS
    api_key = args.token_file.read_text().strip()
    if not api_key:
        raise ValueError("HTTP bearer token file is empty")
    server = NativeMcpServer(
        state_dir=args.state_dir,
        connector_id=args.connector_id,
        host_type="managed-agent",
        agent_id=args.agent_id,
    )
    adapter = None
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        register_delegation(server, registry, toolsets)
        adapter = bound_api_adapter(api.APIServerAdapter)(
            config.PlatformConfig(
                enabled=True,
                extra={"host": "127.0.0.1", "port": args.port, "key": api_key},
            ),
            trace_client=server.client,
        )
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        if not await adapter.connect():
            raise RuntimeError("Hermes HTTP server failed to start")
        await stop.wait()
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)
        if adapter is not None:
            await adapter.disconnect()
        server.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--connector-id", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--port", type=int, default=8642)
    args = parser.parse_args(argv)
    if sys.version_info < (3, 12):  # noqa: UP036 - source launcher can run in older Hermes envs
        parser.error("Hermes and EdgeCitadel must share a Python 3.12+ environment")
    if not 1 <= args.port <= 65535:
        parser.error("port must be between 1 and 65535")
    for path in (args.state_dir, args.token_file):
        if not path.is_absolute():
            parser.error("state-dir and token-file must be absolute paths")
    asyncio.run(serve(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
