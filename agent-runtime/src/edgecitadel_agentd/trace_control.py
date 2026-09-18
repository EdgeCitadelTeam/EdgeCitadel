"""Administrator control of source telemetry independently of task transport."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .client import AgentdClient, AgentdClientError
from .service import ADMIN_TOKEN_NAME, socket_path_for


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", type=Path, required=True, help="agentd service directory"
    )
    parser.add_argument("action", choices=("stop", "start", "retry"))
    parser.add_argument("--scope", nargs=3, metavar=("NODE", "EPOCH", "GENERATION"))
    args = parser.parse_args()
    if (args.action == "retry") != (args.scope is not None):
        parser.error("--scope is required only for retry")
    try:
        token = (args.state_dir / ADMIN_TOKEN_NAME).read_text().strip()
        client = AgentdClient(
            socket_path_for(args.state_dir), admin_token=token, timeout=15
        )
        params: dict[str, object] = {"action": args.action}
        if args.scope is not None:
            params["scope"] = args.scope
        result = client.call("trace.sync.control", **params)
    except (OSError, AgentdClientError):
        print(json.dumps({"error": "telemetry_control_failed"}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
