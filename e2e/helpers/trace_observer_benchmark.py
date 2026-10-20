"""Matched SQLite ingestion component benchmark; no broker or browser qualification.

Use private output on jim-eq for reported timings. Only newly created fixture
databases are opened. Production services and their databases are never touched.
"""

import argparse
import asyncio
import hashlib
import json
import platform
import sqlite3
import statistics
import tempfile
import time
from contextlib import nullcontext
from pathlib import Path
from uuid import uuid4

from edgecitadel_agentd.trace_contract import canonical_bytes

from aggregator import trace_collector, trace_payloads, trace_store

try:
    from .trace_commit_observer import CommitObserver, instrument_collector
except ImportError:
    from trace_commit_observer import CommitObserver, instrument_collector


def records(count):
    epoch, export = str(uuid4()), str(uuid4())
    result = []
    for index in range(count):
        # Repeated small runs exercise accepted envelopes of the baseline shape.
        step = index % 50
        if step == 0:
            trace, context, root = uuid4().hex, str(uuid4()), str(uuid4())
        if step not in (0, 49) and step % 2:
            span = str(uuid4())
        event = dict(
            schema_version=1,
            event_id=str(uuid4()),
            node_id="observer-benchmark",
            source_epoch=epoch,
            source_seq=index + 1,
            agent_id="synthetic-worker",
            trace_id=trace,
            context_id=context,
            task_id=None,
            parent_task_id=None,
            parent_run_id=None,
            execution_attempt_id=None,
            span_id=root if step in (0, 49) else span,
            parent_span_id=None if step in (0, 49) else root,
            kind="run" if step in (0, 49) else "tool",
            phase="started"
            if step == 0 or step % 2 and step != 49
            else "completed"
            if step == 49
            else "finished",
            occurred_at="2026-09-19T00:00:00.000Z",
            duration_ms=None,
            evidence_kind="source_observed",
            causes=[],
            supersedes_event_id=None,
            attributes={}
            if step in (0, 49)
            else {"name": "synthetic.observer.benchmark"},
        )
        result.append(
            dict(
                schema_version=1,
                node_id=event["node_id"],
                source_epoch=epoch,
                export_generation=export,
                export_seq=index + 1,
                event_sha256=hashlib.sha256(canonical_bytes(event)).hexdigest(),
                event=event,
            )
        )
    return result


class LocalAck:
    """Acknowledgment stub: deliberately excludes network/broker costs."""

    def __init__(self, record):
        self.subject = "edgecitadel.telemetry.v1." + record["node_id"]
        self.data = canonical_bytes(record)
        self.acks = 0

    async def ack_sync(self, timeout):
        self.acks += 1


async def run_arm(directory, cohort, *, observed, warmup):
    directory.mkdir(mode=0o700)
    event = cohort[0]["event"]
    observer = (
        CommitObserver(
            node_id=event["node_id"],
            source_epoch=event["source_epoch"],
            agent_ids=[event["agent_id"]],
            capacity=len(cohort),
        )
        if observed
        else None
    )
    messages = [LocalAck(record) for record in cohort]
    callbacks = 0

    def committed(_):
        nonlocal callbacks
        callbacks += 1

    wiring = (
        instrument_collector(trace_collector, observer) if observed else nullcontext()
    )
    with wiring:
        db = trace_collector.sqlite3.connect(directory / "fixture.sqlite3")
        try:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA busy_timeout=50")
            db.execute("PRAGMA cache_spill=OFF")
            trace_store.initialize(db)
            trace_payloads.prepare(db)
            while not trace_payloads.migrate_batch(db):
                pass
            pragmas = {
                key: db.execute("PRAGMA " + key).fetchone()[0]
                for key in (
                    "journal_mode",
                    "synchronous",
                    "wal_autocheckpoint",
                    "page_size",
                )
            }
            for message in messages[:warmup]:
                result = await trace_collector.ingest_delivery(
                    db, message, on_commit=committed
                )
                if result.outcome != "accepted":
                    raise RuntimeError("warmup ingestion rejected")
            measured = messages[warmup:]
            cpu_start, wall_start = time.process_time_ns(), time.perf_counter_ns()
            for message in measured:
                result = await trace_collector.ingest_delivery(
                    db, message, on_commit=committed
                )
                if result.outcome != "accepted":
                    raise RuntimeError("measured ingestion rejected")
            wall_ns, cpu_ns = (
                time.perf_counter_ns() - wall_start,
                time.process_time_ns() - cpu_start,
            )
            rows = db.execute(
                "SELECT event_id,event_sha256,source_seq,ingest_seq FROM trace_raw_events ORDER BY ingest_seq"
            ).fetchall()
            expected = [
                (r["event"]["event_id"], r["event_sha256"], i, i)
                for i, r in enumerate(cohort, 1)
            ]
            if (
                rows != expected
                or callbacks != len(cohort)
                or any(m.acks != 1 for m in messages)
            ):
                raise RuntimeError("ingestion or acknowledgment cohort mismatch")
            serialization_ns = serialization_bytes = 0
            if observer is not None:
                start = time.perf_counter_ns()
                report = observer.report()
                serialized = json.dumps(report)
                serialization_ns = time.perf_counter_ns() - start
                serialization_bytes = len(serialized.encode())
                positions = {r["event_id"]: r["ingest_seq"] for r in report["records"]}
                if not report["valid"] or positions != {row[0]: row[3] for row in rows}:
                    raise RuntimeError("observer cohort mismatch")
            return dict(
                observed=observed,
                measured_events=len(measured),
                warmup_events=warmup,
                wall_ns=wall_ns,
                cpu_ns=cpu_ns,
                pragmas=pragmas,
                verified_events=len(rows),
                exact_ack_and_callback_counts=True,
                content_digest=hashlib.sha256(json.dumps(rows).encode()).hexdigest(),
                report_serialization_ns=serialization_ns,
                report_bytes=serialization_bytes,
            )
        finally:
            db.close()


def summarize(pairs):
    ratios = {"wall": [], "cpu": []}
    for pair in pairs:
        control, observed = pair["control"], pair["observed"]
        for key in (
            "content_digest",
            "measured_events",
            "warmup_events",
            "pragmas",
            "verified_events",
        ):
            if control[key] != observed[key]:
                raise ValueError("unmatched benchmark pair")
        for metric in ratios:
            base = control[metric + "_ns"]
            if base <= 0 or observed[metric + "_ns"] <= 0:
                raise ValueError("invalid benchmark timing")
            ratios[metric].append(100 * (observed[metric + "_ns"] / base - 1))
    if not pairs:
        raise ValueError("no benchmark pairs")
    return {metric + "_percent_deltas": values for metric, values in ratios.items()} | {
        metric + "_median_percent_delta": statistics.median(values)
        for metric, values in ratios.items()
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--samples", type=int, default=1000)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--pairs", type=int, default=4)
    args = parser.parse_args()
    if not (
        1 <= args.samples <= 15000 and 1 <= args.warmup <= 2500 and 2 <= args.pairs <= 8
    ):
        parser.error("require samples 1..15000, warmup 1..2500, pairs 2..8")
    if (
        not args.directory.is_absolute()
        or not args.directory.is_dir()
        or list(args.directory.iterdir())
    ):
        parser.error("empty private absolute output directory required")
    cohort = records(args.samples + args.warmup)
    pairs = []
    for index in range(args.pairs):
        order = [False, True] if index % 2 == 0 else [True, False]
        pair = {"order": order}
        for observed in order:
            name = "observed" if observed else "control"
            # Release only this arm's owned fixture files after verification.
            # Cleanup and report serialization are outside the timed interval.
            with tempfile.TemporaryDirectory(
                prefix=f"pair-{index}-{name}-", dir=args.directory
            ) as owned:
                pair[name] = asyncio.run(
                    run_arm(
                        Path(owned) / "database",
                        cohort,
                        observed=observed,
                        warmup=args.warmup,
                    )
                )
        pairs.append(pair)
        print(json.dumps({"completed_pairs": len(pairs)}), flush=True)
    report = dict(
        scope="SQLite ingestion component; ACK stub; no broker/browser/task-execution or full-system overhead claim",
        python=platform.python_version(),
        sqlite=sqlite3.sqlite_version,
        helper_sha256={
            name: hashlib.sha256(
                Path(__file__).with_name(name).read_bytes()
            ).hexdigest()
            for name in ("trace_observer_benchmark.py", "trace_commit_observer.py")
        },
        pairs=pairs,
        summary=summarize(pairs),
    )
    (args.directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["summary"]), flush=True)


if __name__ == "__main__":
    main()
