"""Read-only lookups in Core's trace store, run on the controller host.

The alert names the faulty invocation (``attempt_id``), the identity the
monitor's provenance graph is keyed by. Workers know their task ids, not their
execution attempts, so the controller resolves the attempt from the
``run/started`` event Core already ingested. After a run, ``effects`` lists
the effect gateway's decisions Core collected (requested, committed, denied
with its reason): the gateway's side of every effect, beside the sink's
independent ledger. ``boundary`` lists, for a detector-triggered run, every
invocation's attempt (``run/started``) and every event at the runtime's
authority or resource boundary (a dispatch or effect denied, a lease alert, a
run or task ended on its lease or deadline), each on its node's clock. Usage:
``python -m harness.coreq attempt --db <core db> --task <task id>``,
``python -m harness.coreq effects --db <core db>``,
``python -m harness.coreq boundary --db <core db>``.
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


def boundary_record(event: dict) -> dict | None:
    """The event as a boundary record, or None when it is not one.

    ``run/started`` names each invocation's attempt. The rest are denials and
    ends the scorer may count as violations (``metrics.VIOLATION_REASONS``
    picks): ``reason`` is agentd's reason, or the alert of a
    ``containment/alert``.
    """
    kind, phase = event["kind"], event["phase"]
    attributes = event.get("attributes") or {}
    if kind == "run" and phase == "started":
        reason = None
    elif kind in {"dispatch", "effect"} and phase == "denied":
        reason = attributes.get("reason")
    elif kind == "containment" and phase == "alert":
        reason = attributes.get("alert")
    elif kind == "run" and attributes.get("reason") is not None:
        reason = attributes["reason"]
    elif kind == "task" and (phase == "expired" or attributes.get("reason")):
        if attributes.get("source_role") == "sender":
            return None  # the sender's mirror of a remote task
        reason = attributes.get("reason") or phase
    else:
        return None
    return {
        "kind": kind,
        "phase": phase,
        "reason": reason,
        "node_id": event["node_id"],
        "agent_id": event.get("agent_id"),
        "task_id": event.get("task_id"),
        "attempt_id": event.get("execution_attempt_id"),
        "occurred_at": event["occurred_at"],
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="harness.coreq")
    sub = parser.add_subparsers(dest="command", required=True)
    attempt = sub.add_parser("attempt")
    attempt.add_argument("--db", required=True, type=Path)
    attempt.add_argument("--task", required=True)
    for name in ("effects", "boundary"):
        sub.add_parser(name).add_argument("--db", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.command == "attempt":
        print(json.dumps({"attempt_id": attempt_of(args.db, args.task)}))
    elif args.command == "effects":
        for record in gateway_effects(args.db):
            print(json.dumps(record))
    else:
        for event in events(args.db):
            record = boundary_record(event)
            if record is not None:
                print(json.dumps(record))


if __name__ == "__main__":
    main()
