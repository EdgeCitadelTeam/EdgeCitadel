"""Read-only lookups in Core's trace store, run on the controller host.

The alert names the faulty invocation (``attempt_id``), the identity the
monitor's provenance graph is keyed by. Workers know their task ids, not their
execution attempts, so the controller resolves the attempt from the
``run/started`` event Core already ingested. After a run, ``effects`` lists
the effect gateway's decisions Core collected (requested, committed, denied
with its reason): the gateway's side of every effect, beside the sink's
independent ledger. Usage:
``python -m harness.coreq attempt --db <core db> --task <task id>``,
``python -m harness.coreq effects --db <core db>``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from aggregator.trace_payload_read import read_payload


def events(db: Path):
    """Every event Core ingested, in ingest order."""
    core = sqlite3.connect(f"{db.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0)
    try:
        rows = core.execute(
            "SELECT ingest_seq FROM trace_raw_events ORDER BY ingest_seq"
        ).fetchall()
        for (seq,) in rows:
            payload = read_payload(core, seq)
            if payload is not None:
                yield payload["event"]
    finally:
        core.close()


def attempt_of(db: Path, task_id: str) -> str | None:
    for event in events(db):
        if (
            event["kind"] == "run"
            and event["phase"] == "started"
            and event["task_id"] == task_id
        ):
            return str(event["execution_attempt_id"])
    return None


def gateway_effects(db: Path) -> list[dict]:
    out = []
    for event in events(db):
        if event["kind"] != "effect":
            continue
        attributes = event.get("attributes") or {}
        out.append(
            {
                "phase": event["phase"],
                "effect_id": attributes.get("effect_id"),
                "node_id": event["node_id"],
                "agent_id": event.get("agent_id"),
                "task_id": event.get("task_id"),
                "target": attributes.get("target"),
                "adapter": attributes.get("adapter"),
                "scheduled_at_ms": attributes.get("scheduled_at_ms"),
                "committed_at_ms": attributes.get("committed_at_ms"),
                "reason": attributes.get("reason"),
                "occurred_at": event.get("occurred_at"),
            }
        )
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.coreq")
    sub = parser.add_subparsers(dest="command", required=True)
    attempt = sub.add_parser("attempt")
    attempt.add_argument("--db", required=True, type=Path)
    attempt.add_argument("--task", required=True)
    effects = sub.add_parser("effects")
    effects.add_argument("--db", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "attempt":
        print(json.dumps({"attempt_id": attempt_of(args.db, args.task)}))
    else:
        for record in gateway_effects(args.db):
            print(json.dumps(record))


if __name__ == "__main__":
    main()
