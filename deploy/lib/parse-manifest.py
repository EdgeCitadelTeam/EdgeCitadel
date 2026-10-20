#!/usr/bin/env python3
"""Read deploy/manifest.toml and emit a value at a dotted path.

Usage:
  parse-manifest.py [--manifest PATH] get <dotted.key> [--format VAL]

Formats:
  raw   (default) — print the value as Python repr or scalar
  json  — JSON-encode the value
  lines — for arrays, print one element per line; for scalars, same as raw

Exit codes:
  0   success
  2   key not found in manifest
  3   manifest file missing or unparseable
"""
from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path


def _resolve_path(manifest_arg: str | None) -> Path:
    if manifest_arg:
        return Path(manifest_arg).resolve()
    here = Path(__file__).resolve()
    return here.parent.parent / "manifest.toml"


def _get(data: dict, dotted: str):
    cur = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            print(f"key '{dotted}' not found in manifest", file=sys.stderr)
            sys.exit(2)
        cur = cur[part]
    return cur


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--manifest", help="path to manifest.toml")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("get", help="read a value at a dotted key")
    g.add_argument("key", help="dotted path, e.g. ollama.models")
    g.add_argument("--format", choices=["raw", "json", "lines"], default="raw")
    args = p.parse_args()

    manifest_path = _resolve_path(args.manifest)
    if not manifest_path.is_file():
        print(f"manifest not found at {manifest_path}", file=sys.stderr)
        return 3
    try:
        with manifest_path.open("rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        print(f"malformed manifest: {e}", file=sys.stderr)
        return 3

    val = _get(data, args.key)

    if args.format == "json":
        print(json.dumps(val))
    elif args.format == "lines":
        if isinstance(val, list):
            for item in val:
                print(item)
        else:
            print(val)
    else:
        print(val)
    return 0


if __name__ == "__main__":
    sys.exit(main())
