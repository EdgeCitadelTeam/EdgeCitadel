#!/usr/bin/env python3
"""Round-trip end-to-end smoke test.

POSTs a reasoning.chat command to a target agent, polls /api/messages for
a result envelope with non-empty body, asserts it arrives within --timeout.

Usage:
  smoke.py [--base-url URL] [--agent ID] [--timeout SEC]

Defaults: base-url=http://localhost, agent=gemma-1, timeout=30
"""
import argparse
import json
import sys
import time
import urllib.request


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://localhost")
    ap.add_argument("--agent", default="gemma-1")
    ap.add_argument("--timeout", type=int, default=30)
    args = ap.parse_args()

    cmd_url = f"{args.base_url}/api/command/{args.agent}?sender_id=deploy-smoke"

    # CommandRequest schema (aggregator/models.py): {body, args?, skill_id?, context_id?}
    # Server generates task_id and returns it in CommandResponse.
    body = {"skill_id": "reasoning.chat", "body": "ping"}
    req = urllib.request.Request(
        cmd_url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            resp = json.loads(r.read())
    except Exception as e:
        print(f"  round-trip ✗ POST failed: {e}", file=sys.stderr)
        return 1
    task_id = resp.get("task_id")
    if not task_id:
        print(f"  round-trip ✗ no task_id in response: {resp}", file=sys.stderr)
        return 1

    msgs_url = f"{args.base_url}/api/messages?task_id={task_id}"
    deadline = t0 + args.timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(msgs_url, timeout=5) as r:
                msgs = json.loads(r.read())
        except Exception as e:
            print(f"  round-trip ✗ poll failed: {e}", file=sys.stderr)
            return 1
        results = [m for m in msgs if m.get("type") == "result"]
        if results and (results[0].get("payload") or {}).get("body"):
            elapsed = time.time() - t0
            print(f"  round-trip ✓ {elapsed:.1f}s")
            return 0
        time.sleep(1)

    print(f"  round-trip ✗ no result within {args.timeout}s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
