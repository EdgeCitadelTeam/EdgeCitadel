"""Read-only lookups in Core's trace store, run on the controller host.

The alert names the faulty invocation (``attempt_id``), the identity the
monitor's provenance graph is keyed by. Workers know their task ids, not their
execution attempts, so the controller resolves the attempt from the
``run/started`` event Core already ingested. After a run, ``effects`` lists
the effect gateway's decisions Core collected (requested, committed, denied
with its reason): the gateway's side of every effect, beside the sink's
independent ledger; ``ingest`` summarizes Core's ingest lag per node: for
every raw event, the time Core received it minus the time its node says it
occurred (two clocks: the hosts are assumed synchronized, as on the tailnet).
Usage:
``python -m harness.coreq attempt --db <core db> --task <task id>``,
``python -m harness.coreq effects --db <core db>``,
``python -m harness.coreq ingest --db <core db> [--since-ms <run start>]``.
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


def _occurred_ms(value: str) -> int:
    from datetime import datetime

    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def ingest_lags(db: Path, since_ms: int | None = None) -> dict[str, list[int]]:
    """Per node, every raw event's Core receive time minus its occurrence (ms),
    for the events that occurred at or after ``since_ms``."""
    core = sqlite3.connect(f"{db.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0)
    try:
        out: dict[str, list[int]] = {}
        for node, received, seq, raw in core.execute(
            "SELECT node_id,received_at_ms,ingest_seq,event_json FROM trace_raw_events"
        ):
            try:
                # Separated payloads leave event_json empty: read the payload.
                event = json.loads(raw) if raw else (read_payload(core, seq) or {})
                event = event.get("event", event)
                occurred = _occurred_ms(event["occurred_at"])
            except (ValueError, KeyError, TypeError, AttributeError):
                continue
            if since_ms is not None and occurred < since_ms:
                continue
            out.setdefault(str(node), []).append(int(received) - occurred)
        return out
    finally:
        core.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.coreq")
    sub = parser.add_subparsers(dest="command", required=True)
    attempt = sub.add_parser("attempt")
    attempt.add_argument("--db", required=True, type=Path)
    attempt.add_argument("--task", required=True)
    effects = sub.add_parser("effects")
    effects.add_argument("--db", required=True, type=Path)
    lags = sub.add_parser("ingest")
    lags.add_argument("--db", required=True, type=Path)
    lags.add_argument("--since-ms", type=int)
    args = parser.parse_args(argv)
    if args.command == "attempt":
        print(json.dumps({"attempt_id": attempt_of(args.db, args.task)}))
    elif args.command == "ingest":
        from .runstats import distribution

        per_node = ingest_lags(args.db, args.since_ms)
        print(
            json.dumps(
                {
                    "all": distribution(
                        v for values in per_node.values() for v in values
                    ),
                    "by_node": {
                        n: distribution(v) for n, v in sorted(per_node.items())
                    },
                }
            )
        )
    else:
        for record in gateway_effects(args.db):
            print(json.dumps(record))


if __name__ == "__main__":
    main()
