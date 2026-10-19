"""Local immutable journal and export intent, sharing the task-store transaction."""

from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from typing import Any
from uuid import uuid4

from .trace_content import fit_event_content, sanitize_event_content
from .trace_counters import MAX_COUNTER, encode_counter
from .trace_headroom import pending, require
from .trace_reservations import Obligation
from .trace_completed import fill_completed
from .storage_workspace import ReservedConnection
from .trace_capacity import admit_event
from .trace_contract import TraceContractError, validate_event, validate_export_header

TRACE_SCHEMA_SQL = """
CREATE TABLE trace_sources (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    next_source_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_source_seq > 0),
    test_run_id TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    PRIMARY KEY(node_id, source_epoch)
);
CREATE UNIQUE INDEX trace_one_active_source ON trace_sources(node_id) WHERE active=1;
CREATE TABLE trace_export_generations (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    next_export_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_export_seq > 0),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    PRIMARY KEY(node_id, source_epoch, export_generation),
    FOREIGN KEY(node_id, source_epoch) REFERENCES trace_sources(node_id, source_epoch)
);
CREATE UNIQUE INDEX trace_one_active_export ON trace_export_generations(node_id, source_epoch) WHERE active=1;
CREATE TABLE trace_journal (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    event_id TEXT NOT NULL,
    source_seq INTEGER NOT NULL CHECK(source_seq > 0),
    trace_id TEXT,
    agent_id TEXT,
    task_id TEXT,
    event_sha256 TEXT NOT NULL,
    event_json TEXT NOT NULL,
    event_bytes INTEGER NOT NULL CHECK(event_bytes > 0),
    PRIMARY KEY(node_id, source_epoch, event_id),
    UNIQUE(node_id, source_epoch, source_seq),
    FOREIGN KEY(node_id, source_epoch) REFERENCES trace_sources(node_id, source_epoch)
);
CREATE INDEX trace_journal_run ON trace_journal(trace_id, source_seq);
CREATE INDEX trace_journal_actor ON trace_journal(agent_id, trace_id, source_seq);
CREATE TABLE trace_spool (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    export_seq INTEGER NOT NULL CHECK(export_seq > 0),
    event_id TEXT NOT NULL,
    journal_event_id TEXT,
    event_sha256 TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK(state IN ('pending', 'broker_acked', 'core_settled', 'lost_with_marker')),
    collector_epoch TEXT,
    PRIMARY KEY(node_id, source_epoch, export_generation, export_seq),
    UNIQUE(node_id, source_epoch, export_generation, event_id),
    FOREIGN KEY(node_id, source_epoch, export_generation)
        REFERENCES trace_export_generations(node_id, source_epoch, export_generation),
    FOREIGN KEY(node_id, source_epoch, journal_event_id)
        REFERENCES trace_journal(node_id, source_epoch, event_id),
    CHECK(journal_event_id IS NOT NULL OR state IN ('lost_with_marker', 'core_settled'))
);
CREATE INDEX trace_spool_pending ON trace_spool(state, node_id, source_epoch, export_generation, export_seq);
"""


class TraceJournal:
    """Internal API: caller holds the store lock and owns an active transaction.

    Caller must authenticate and authorize before constructing an event. This
    class validates/stamps/persists metadata; it does not establish authority.
    It never commits. Exceptions must roll back the caller transaction.
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def _transaction(self) -> None:
        if not self.connection.in_transaction:
            raise TraceContractError("trace_transaction_required")

    def initialize(self, node_id: str) -> tuple[str, str]:
        self._transaction()
        if (
            not isinstance(node_id, str)
            or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", node_id) is None
        ):
            raise TraceContractError("invalid_source_node")
        row = self.connection.execute(
            "SELECT source_epoch FROM trace_sources WHERE node_id=? AND active=1",
            (node_id,),
        ).fetchone()
        if row is None:
            epoch, generation = str(uuid4()), str(uuid4())
            self.connection.execute(
                "INSERT INTO trace_sources(node_id,source_epoch,test_run_id) "
                "VALUES (?,?,(SELECT test_run_id FROM trace_sources WHERE node_id=? ORDER BY rowid DESC LIMIT 1))",
                (node_id, epoch, node_id),
            )
            self.connection.execute(
                "INSERT INTO trace_export_generations(node_id,source_epoch,export_generation) VALUES (?,?,?)",
                (node_id, epoch, generation),
            )
        else:
            epoch = str(row[0])
            generation_row = self.connection.execute(
                "SELECT export_generation FROM trace_export_generations "
                "WHERE node_id=? AND source_epoch=? AND active=1",
                (node_id, epoch),
            ).fetchone()
            if generation_row is None:
                raise TraceContractError("missing_export_generation")
            generation = str(generation_row[0])
        return epoch, generation

    def record(
        self,
        node_id: str,
        event: dict[str, Any],
        *,
        selected: bool,
        reserve_capacity: bool = False,
        completion: Obligation | None = None,
    ) -> dict[str, Any]:
        self._transaction()
        if (
            completion is not None
            and self.connection.execute(
                "SELECT 1 FROM trace_sources WHERE node_id=? AND active=1", (node_id,)
            ).fetchone()
            is None
        ):
            raise TraceContractError("completion_source_missing")
        epoch, generation = self.initialize(node_id)
        previous = self.connection.execute(
            "SELECT source_seq,event_sha256 FROM trace_journal_all WHERE node_id=? AND source_epoch=? AND event_id=?",
            (node_id, epoch, event["event_id"]),
        ).fetchone()
        sequence = (
            previous[0]
            if previous
            else self.connection.execute(
                "SELECT next_source_seq FROM trace_sources WHERE node_id=? AND source_epoch=?",
                (node_id, epoch),
            ).fetchone()[0]
        )
        if not previous and sequence >= MAX_COUNTER:
            raise TraceContractError("trace_sequence_exhausted")
        stamped = {
            **sanitize_event_content(event),
            "node_id": node_id,
            "source_epoch": epoch,
            "source_seq": sequence,
        }
        # Caller-provided fields cannot downgrade retention. The daemon owns
        # provenance, and the stamped value participates in the immutable hash.
        stamped.pop("test_run_id", None)
        test_run = self.connection.execute(
            "SELECT test_run_id FROM trace_sources WHERE node_id=? AND source_epoch=?",
            (node_id, epoch),
        ).fetchone()[0]
        if test_run is not None:
            stamped["test_run_id"] = test_run
        fit_event_content(stamped)
        encoded = validate_event(stamped)
        digest = hashlib.sha256(encoded).hexdigest()
        if previous:
            if previous[1] != digest:
                raise TraceContractError("idempotency_conflict")
            return stamped
        canonical_pending, _ = pending(self.connection)
        needed = canonical_pending + int(completion is None)
        require(sequence, needed)
        if selected:
            export_next = self.connection.execute(
                "SELECT next_export_seq FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                (node_id, epoch, generation),
            ).fetchone()[0]
            require(export_next, needed)
        if completion is not None:
            db = self.connection
            if not isinstance(db, ReservedConnection) or db.workspace is None:
                raise TraceContractError("reserved_completion_unavailable")
            export_sequence = None
            if selected:
                export_sequence = db.execute(
                    "SELECT next_export_seq FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                    (node_id, epoch, generation),
                ).fetchone()[0]
                if export_sequence >= MAX_COUNTER:
                    raise TraceContractError("trace_sequence_exhausted")
                validate_export_header(
                    {
                        "schema_version": 1,
                        "node_id": node_id,
                        "source_epoch": epoch,
                        "export_generation": generation,
                        "export_seq": export_sequence,
                        "event_sha256": digest,
                        "event": stamped,
                    }
                )
            db.use_completion_workspace()
            fill_completed(
                db,
                completion,
                {
                    "event": stamped,
                    "event_sha256": digest,
                    "event_bytes": len(encoded),
                    "received_at_ms": time.time_ns() // 1_000_000,
                    "export_generation": generation if selected else None,
                    "export_seq": export_sequence,
                    "state": "pending",
                    "collector_epoch": None,
                    "core_outcome": None,
                },
            )
            db.execute(
                "UPDATE trace_sources SET next_source_seq_bytes=? WHERE node_id=? AND source_epoch=?",
                (encode_counter(sequence + 1), node_id, epoch),
            )
            if export_sequence is not None:
                db.execute(
                    "UPDATE trace_export_generations SET next_export_seq_bytes=? WHERE node_id=? AND source_epoch=? AND export_generation=?",
                    (encode_counter(export_sequence + 1), node_id, epoch, generation),
                )
            return stamped
        admit_event(
            self.connection,
            event_bytes=len(encoded),
            kind=stamped["kind"],
            reserve_capacity=reserve_capacity,
        )
        self.connection.execute(
            "INSERT INTO trace_journal(node_id,source_epoch,event_id,source_seq,trace_id,agent_id,task_id,event_sha256,event_json,event_bytes,received_at_ms) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                node_id,
                epoch,
                stamped["event_id"],
                sequence,
                stamped["trace_id"],
                stamped["agent_id"],
                stamped["task_id"],
                digest,
                encoded.decode(),
                len(encoded),
                time.time_ns() // 1_000_000,
            ),
        )
        self.connection.execute(
            "UPDATE trace_sources SET next_source_seq_bytes=? WHERE node_id=? AND source_epoch=?",
            (encode_counter(sequence + 1), node_id, epoch),
        )
        if selected:
            export_sequence = self.connection.execute(
                "SELECT next_export_seq FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                (node_id, epoch, generation),
            ).fetchone()[0]
            if export_sequence >= MAX_COUNTER:
                raise TraceContractError("trace_sequence_exhausted")
            # The stamped event already passed full validation above. Origin
            # fields are constructed from that same writer identity; validate
            # the wrapper and content hash without a second event-schema walk.
            validate_export_header(
                {
                    "schema_version": 1,
                    "node_id": node_id,
                    "source_epoch": epoch,
                    "export_generation": generation,
                    "export_seq": export_sequence,
                    "event_sha256": digest,
                    "event": stamped,
                }
            )
            self.connection.execute(
                "INSERT INTO trace_spool(node_id,source_epoch,export_generation,export_seq,event_id,journal_event_id,event_sha256) VALUES (?,?,?,?,?,?,?)",
                (
                    node_id,
                    epoch,
                    generation,
                    export_sequence,
                    stamped["event_id"],
                    stamped["event_id"],
                    digest,
                ),
            )
            self.connection.execute(
                "UPDATE trace_export_generations SET next_export_seq_bytes=? WHERE node_id=? AND source_epoch=? AND export_generation=?",
                (encode_counter(export_sequence + 1), node_id, epoch, generation),
            )
        return stamped
