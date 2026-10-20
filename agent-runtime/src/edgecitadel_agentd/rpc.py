"""Subprocess bridge used by the distribution-neutral root CLI."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from .client import AgentdClient, AgentdClientError
from .service import socket_path_for


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edgecitadel-agentd-rpc")
    parser.add_argument("--state-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        request = json.load(sys.stdin)
        if not isinstance(request, dict):
            raise ValueError("request must be an object")
        operation = request.pop("operation")
        params = request.pop("params", {})
        connector_id = request.pop("connector_id", None)
        token = request.pop("token", None)
        admin_token = request.pop("admin_token", None)
        if not isinstance(operation, str):
            raise ValueError("operation must be a string")
        if not isinstance(params, dict):
            raise ValueError("params must be an object")
        if request:
            raise ValueError("request contains unexpected top-level fields")
        client = AgentdClient(
            socket_path_for(args.state_dir),
            connector_id=connector_id,
            token=token,
            admin_token=admin_token,
        )
        result = client.call(operation, **params)
    except (AgentdClientError, KeyError, TypeError, ValueError) as error:
        print(json.dumps({"ok": False, "error": str(error)}))
        return 1
    print(json.dumps({"ok": True, "result": result}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
