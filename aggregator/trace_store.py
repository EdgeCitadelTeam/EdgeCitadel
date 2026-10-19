"""Core raw-ingestion persistence; transactions are independent of broker ACKs.

Callers supply a connection to the Aggregator database. A successful return from
ingest means the transaction committed, not that a generation is complete.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from uuid import uuid4

from edgecitadel_agentd.trace_contract import (
    TraceContractError,
    canonical_bytes,
    coverage_scope,
    validate_export,
    validate_export_header,
)

from . import trace_capacity, trace_payloads

MAX_REJECTED_POSITIONS = 4096

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trace_collector (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    collector_epoch TEXT NOT NULL,
    ingest_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS trace_raw_events (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    event_sha256 TEXT NOT NULL,
    event_json TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    ingest_seq INTEGER NOT NULL UNIQUE,
    payload_expired_at_ms INTEGER,
    PRIMARY KEY(node_id, source_epoch, event_id),
    UNIQUE(node_id, source_epoch, source_seq)
);
CREATE TABLE IF NOT EXISTS trace_ingest_positions (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    export_seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_sha256 TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('accepted', 'duplicate', 'conflict')),
    received_at_ms INTEGER NOT NULL,
    ingest_seq INTEGER NOT NULL UNIQUE,
    PRIMARY KEY(node_id, source_epoch, export_generation, export_seq)
);
CREATE TABLE IF NOT EXISTS trace_ingest_conflicts (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    export_seq INTEGER NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN ('export_position', 'event_identity', 'source_position')),
    expected_sha256 TEXT NOT NULL,
    received_sha256 TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    ingest_seq INTEGER NOT NULL UNIQUE,
    PRIMARY KEY(node_id, source_epoch, export_generation, export_seq)
);
CREATE TABLE IF NOT EXISTS trace_rejected_positions (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    export_seq INTEGER NOT NULL,
    event_sha256 TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    ingest_seq INTEGER NOT NULL UNIQUE,
    PRIMARY KEY(node_id, source_epoch, export_generation, export_seq)
);
CREATE TABLE IF NOT EXISTS trace_rejected_identities (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_sha256 TEXT NOT NULL,
    PRIMARY KEY(node_id, source_epoch, event_id)
);
CREATE TABLE IF NOT EXISTS trace_poison_counts (
    reason TEXT PRIMARY KEY CHECK(reason IN ('wire', 'wrapper', 'origin')),
    observations INTEGER NOT NULL,
    last_received_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS trace_loss_ranges (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    first_seq INTEGER NOT NULL,
    last_seq INTEGER NOT NULL,
    writer_epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    range_index INTEGER NOT NULL,
    PRIMARY KEY(node_id, writer_epoch, event_id, range_index)
);
CREATE INDEX IF NOT EXISTS trace_loss_scope
    ON trace_loss_ranges(node_id, source_epoch, export_generation, first_seq, last_seq);
"""


@dataclass(frozen=True)
class IngestResult:
    collector_epoch: str
    ingest_seq: int
    outcome: str


def initialize(connection: sqlite3.Connection, *, backfill: bool = True) -> None:
    """Prepare Core tables; optionally finish accounting in resumable batches."""
    if connection.in_transaction:
        raise ValueError("trace_store_requires_idle_connection")
    separated_payloads = trace_payloads.is_prepared(connection)
    if separated_payloads:
        trace_payloads.open_layout(connection)
    connection.executescript(SCHEMA_SQL)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(trace_raw_events)")
        }
        if "payload_expired_at_ms" not in columns:
            connection.execute(
                "ALTER TABLE trace_raw_events ADD COLUMN payload_expired_at_ms INTEGER"
            )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS trace_retention_state ("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "after_ingest_seq INTEGER NOT NULL CHECK(after_ingest_seq>=0))"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS trace_inline_test_expiry ON trace_raw_events("
            "received_at_ms,ingest_seq) WHERE payload_expired_at_ms IS NULL "
            "AND json_extract(NULLIF(event_json,''),'$.test_run_id') IS NOT NULL"
        )
        connection.execute("INSERT OR IGNORE INTO trace_retention_state VALUES(1,0)")
        connection.execute(
            "INSERT OR IGNORE INTO trace_collector(singleton, collector_epoch) VALUES(1, ?)",
            (str(uuid4()),),
        )
        trace_capacity.initialize(connection)
    if backfill:
        while not trace_capacity.backfill_step(connection):
            pass
        if separated_payloads:
            while not trace_payloads.migrate_batch(connection):
                pass


def ingest(
    connection: sqlite3.Connection,
    subject: str,
    record: dict,
    *,
    received_at_ms: int,
) -> IngestResult:
    """Atomically retain a valid event and exact export-position disposition.

    Invalid input raises without recording settlement. Transport decoding, poison
    admission, coverage application and bounded-retention policy are not supplied
    by this helper. Do not enable a consumer until those gates are implemented.
    """
    # Snapshot only validated canonical data, never the caller's mutable objects.
    record = json.loads(validate_export(record))
    if subject != f"edgecitadel.telemetry.v1.{record['node_id']}":
        raise TraceContractError("origin_mismatch")
    if type(received_at_ms) is not int or received_at_ms < 0:
        raise ValueError("invalid_ingest_time")
    if connection.in_transaction:
        raise ValueError("trace_store_requires_idle_connection")
    separated_payloads = trace_payloads.is_prepared(connection)
    if separated_payloads:
        trace_payloads.open_layout(connection)
    event = record["event"]
    scope = (record["node_id"], record["source_epoch"])
    position = (*scope, record["export_generation"], record["export_seq"])
    digest = record["event_sha256"]
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        epoch, cursor = connection.execute(
            "SELECT collector_epoch, ingest_seq FROM trace_collector WHERE singleton = 1"
        ).fetchone()
        existing = connection.execute(
            "SELECT event_id, event_sha256, outcome, ingest_seq FROM trace_ingest_positions "
            "WHERE node_id = ? AND source_epoch = ? AND export_generation = ? AND export_seq = ?",
            position,
        ).fetchone()
        rejected = connection.execute(
            "SELECT event_sha256,ingest_seq FROM trace_rejected_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
            position,
        ).fetchone()
        if rejected:
            # A previously rejected position cannot be corrected in place. A
            # corrected event needs a fresh selected export identity.
            existing = (None, rejected[0], "rejected", rejected[1])
            if rejected[0] == digest:
                return IngestResult(epoch, rejected[1], "rejected")
        if existing and existing[:2] == (event["event_id"], digest):
            return IngestResult(epoch, existing[3], existing[2])
        conflict = connection.execute(
            "SELECT ingest_seq FROM trace_ingest_conflicts "
            "WHERE node_id = ? AND source_epoch = ? AND export_generation = ? AND export_seq = ?",
            position,
        ).fetchone()
        if existing and conflict:
            return IngestResult(epoch, conflict[0], "conflict")
        reason = None
        expected = None
        outcome = "accepted"
        if existing:
            reason, expected = "export_position", existing[1]
        else:
            rejected_identity = connection.execute(
                "SELECT event_sha256 FROM trace_rejected_identities "
                "WHERE node_id=? AND source_epoch=? AND event_id=?",
                (*scope, event["event_id"]),
            ).fetchone()
            identity = connection.execute(
                "SELECT event_sha256 FROM trace_raw_events "
                "WHERE node_id = ? AND source_epoch = ? AND event_id = ?",
                (*scope, event["event_id"]),
            ).fetchone()
            source_position = connection.execute(
                "SELECT event_sha256 FROM trace_raw_events "
                "WHERE node_id = ? AND source_epoch = ? AND source_seq = ?",
                (*scope, event["source_seq"]),
            ).fetchone()
            if rejected_identity:
                reason, expected = "event_identity", rejected_identity[0]
            elif identity:
                if identity[0] == digest:
                    outcome = "duplicate"
                else:
                    reason, expected = "event_identity", identity[0]
            elif source_position:
                reason, expected = "source_position", source_position[0]
        trace_capacity.admit_write(connection)
        cursor += 1
        if reason:
            outcome = "conflict"
            connection.execute(
                "INSERT INTO trace_ingest_conflicts VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*position, reason, expected, digest, received_at_ms, cursor),
            )
        elif outcome == "accepted":
            connection.execute(
                "INSERT INTO trace_raw_events "
                "(node_id,source_epoch,event_id,source_seq,event_sha256,event_json,received_at_ms,ingest_seq) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    *scope,
                    event["event_id"],
                    event["source_seq"],
                    digest,
                    "" if separated_payloads else canonical_bytes(event).decode(),
                    received_at_ms,
                    cursor,
                ),
            )
            if separated_payloads:
                connection.execute(
                    "INSERT INTO trace_payloads VALUES(?,?,?)",
                    (cursor, received_at_ms, canonical_bytes(event).decode()),
                )
            if event["kind"] == "coverage":
                affected = coverage_scope(event)
                connection.executemany(
                    "INSERT INTO trace_loss_ranges VALUES(?,?,?,?,?,?,?,?)",
                    [
                        (
                            *affected,
                            item["first"],
                            item["last"],
                            event["source_epoch"],
                            event["event_id"],
                            index,
                        )
                        for index, item in enumerate(
                            event["attributes"].get("lost_ranges", [])
                        )
                    ],
                )
        if not existing:
            connection.execute(
                "INSERT INTO trace_ingest_positions VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*position, event["event_id"], digest, outcome, received_at_ms, cursor),
            )
        connection.execute(
            "UPDATE trace_collector SET ingest_seq = ? WHERE singleton = 1", (cursor,)
        )
        trace_capacity.check(connection, rejection_limit=MAX_REJECTED_POSITIONS)
    return IngestResult(epoch, cursor, outcome)


def reject_payload(
    connection: sqlite3.Connection,
    subject: str,
    record: dict,
    *,
    received_at_ms: int,
) -> IngestResult:
    """Retain a bounded hash-only receipt for a known envelope's invalid payload.

    Receipts are never evicted to make room: quota failure leaves delivery
    unacknowledged. No caller-supplied error text or event fields are persisted.
    """
    record = json.loads(validate_export_header(record))
    if subject != f"edgecitadel.telemetry.v1.{record['node_id']}" or any(
        record["event"].get(key) != record[key] for key in ("node_id", "source_epoch")
    ):
        raise TraceContractError("origin_mismatch")
    # Do not allow this API to manufacture rejection for a valid event.
    try:
        validate_export(record)
    except TraceContractError:
        pass
    else:
        raise ValueError("valid_payload_cannot_be_rejected")
    if type(received_at_ms) is not int or received_at_ms < 0:
        raise ValueError("invalid_ingest_time")
    if connection.in_transaction:
        raise ValueError("trace_store_requires_idle_connection")
    separated_payloads = trace_payloads.is_prepared(connection)
    if separated_payloads:
        trace_payloads.open_layout(connection)
    position = tuple(
        record[key]
        for key in ("node_id", "source_epoch", "export_generation", "export_seq")
    )
    digest = record["event_sha256"]
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        epoch, cursor = connection.execute(
            "SELECT collector_epoch,ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()
        existing = connection.execute(
            "SELECT event_sha256,outcome,ingest_seq FROM trace_ingest_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
            position,
        ).fetchone()
        rejected = connection.execute(
            "SELECT event_sha256,ingest_seq FROM trace_rejected_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
            position,
        ).fetchone()
        if rejected:
            existing = (rejected[0], "rejected", rejected[1])
        if existing:
            if existing[0] == digest:
                return IngestResult(epoch, existing[2], existing[1])
            conflict = connection.execute(
                "SELECT ingest_seq FROM trace_ingest_conflicts "
                "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq=?",
                position,
            ).fetchone()
            if conflict:
                return IngestResult(epoch, conflict[0], "conflict")
            trace_capacity.admit_write(connection)
            cursor += 1
            connection.execute(
                "INSERT INTO trace_ingest_conflicts VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    *position,
                    "export_position",
                    existing[0],
                    digest,
                    received_at_ms,
                    cursor,
                ),
            )
            outcome = "conflict"
        else:
            if (
                connection.execute(
                    "SELECT count(*) FROM trace_rejected_positions"
                ).fetchone()[0]
                >= MAX_REJECTED_POSITIONS
            ):
                raise TraceContractError("rejection_capacity_exceeded")
            trace_capacity.admit_write(connection)
            cursor += 1
            connection.execute(
                "INSERT INTO trace_rejected_positions VALUES(?,?,?,?,?,?,?)",
                (*position, digest, received_at_ms, cursor),
            )
            event_id = record["event"].get("event_id")
            if isinstance(event_id, str) and re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                event_id,
            ):
                # Only a syntactically valid identity can become terminal. A
                # later malformed variant must not invalidate an accepted event.
                connection.execute(
                    "INSERT OR IGNORE INTO trace_rejected_identities "
                    "SELECT ?,?,?,? WHERE NOT EXISTS "
                    "(SELECT 1 FROM trace_raw_events WHERE node_id=? AND source_epoch=? AND event_id=?)",
                    (*position[:2], event_id, digest, *position[:2], event_id),
                )
            outcome = "rejected"
        connection.execute(
            "UPDATE trace_collector SET ingest_seq=? WHERE singleton=1", (cursor,)
        )
        trace_capacity.check(connection, rejection_limit=MAX_REJECTED_POSITIONS)
    return IngestResult(epoch, cursor, outcome)


def record_poison(
    connection: sqlite3.Connection,
    reason: str,
    *,
    received_at_ms: int,
) -> IngestResult:
    """Three fixed counter rows; no origin, subject, hash or payload quarantine."""
    if reason not in ("wire", "wrapper", "origin"):
        raise ValueError("invalid_poison_reason")
    if type(received_at_ms) is not int or received_at_ms < 0:
        raise ValueError("invalid_ingest_time")
    if connection.in_transaction:
        raise ValueError("trace_store_requires_idle_connection")
    separated_payloads = trace_payloads.is_prepared(connection)
    if separated_payloads:
        trace_payloads.open_layout(connection)
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        trace_capacity.admit_write(connection)
        connection.execute(
            "INSERT INTO trace_poison_counts VALUES(?,1,?) "
            "ON CONFLICT(reason) DO UPDATE SET observations=min(observations+1,9007199254740991), "
            "last_received_at_ms=max(last_received_at_ms,excluded.last_received_at_ms)",
            (reason, received_at_ms),
        )
        epoch, cursor = connection.execute(
            "SELECT collector_epoch,ingest_seq FROM trace_collector WHERE singleton=1"
        ).fetchone()
    return IngestResult(epoch, cursor, "quarantined")
