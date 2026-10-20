"""Command-line interface for non-executing plugin package validation."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from .errors import PluginError
from .inventory import build_inventory, write_lock
from .validator import validate_package


def main(argv: Sequence[str] | None = None) -> int:
    """Validate or lock a plugin package without executing plugin code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "lock":
            package = validate_package(args.plugin_root, verify_integrity=False)
            lock_path = write_lock(package)
            payload: dict[str, object] = {
                "lockfile": str(lock_path),
                "packageId": package.package_id,
                "status": "locked",
            }
        else:
            package = validate_package(args.plugin_root)
            payload = build_inventory(package)
    except PluginError as error:
        sys.stderr.write(f"error: {error}\n")
        return 2

    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="edgecitadel-supervisor")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("lock", "validate"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("plugin_root")
    return parser
