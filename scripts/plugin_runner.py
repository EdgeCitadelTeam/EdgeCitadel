#!/usr/bin/env python3
"""Compatibility process runner for legacy AgentPlugin restart policies."""

from __future__ import annotations

import argparse
import signal
import subprocess
import sys
import time
from collections.abc import Sequence


def should_restart(policy: str, returncode: int) -> bool:
    """Return whether the declared policy requires another child process."""
    return policy == "always" or (policy == "on-failure" and returncode != 0)


def run(command: Sequence[str], policy: str) -> int:
    """Run a child in this runner's process group until policy says to stop."""
    stopping = False
    child: subprocess.Popen[bytes] | None = None

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        if child is not None and child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    while True:
        try:
            child = subprocess.Popen(list(command))
        except OSError as error:
            print(f"AgentPlugin runner could not start child: {error}", file=sys.stderr)
            return 126
        while child.poll() is None:
            time.sleep(0.1)
        returncode = child.returncode
        if stopping or not should_restart(policy, returncode):
            return returncode
        time.sleep(1)
        if stopping:
            return returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--restart-policy", choices=("always", "on-failure", "never"), required=True
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a Managed Agent command is required after --")
    return run(command, args.restart_policy)


if __name__ == "__main__":
    raise SystemExit(main())
