"""Read-only lookups in Core's trace store, run on the controller host.

The alert names the faulty invocation (``attempt_id``), the identity the
monitor's provenance graph is keyed by. Workers know their task ids, not their
execution attempts, so the controller resolves the attempt from the
``run/started`` event Core already ingested. Usage:
``python -m harness.coreq attempt --db <core db> --task <task id>``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from aggregator.trace_payload_read import read_payload


def attempt_of(db: Path, task_id: str) -> str | None:
    core = sqlite3.connect(f"{db.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0)
    try:
        rows = core.execute(
            "SELECT ingest_seq FROM trace_raw_events ORDER BY ingest_seq"
        ).fetchall()
        for (seq,) in rows:
            payload = read_payload(core, seq)
            event = payload["event"] if payload else None
            if (
                event is not None
                and event["kind"] == "run"
                and event["phase"] == "started"
                and event["task_id"] == task_id
            ):
                return str(event["execution_attempt_id"])
        return None
    finally:
        core.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.coreq")
    sub = parser.add_subparsers(dest="command", required=True)
    attempt = sub.add_parser("attempt")
    attempt.add_argument("--db", required=True, type=Path)
    attempt.add_argument("--task", required=True)
    args = parser.parse_args(argv)
    print(json.dumps({"attempt_id": attempt_of(args.db, args.task)}))


if __name__ == "__main__":
    main()
