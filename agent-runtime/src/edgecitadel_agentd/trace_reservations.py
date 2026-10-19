"""Private, preallocated terminal records owned by admitted source work."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from .trace_contract import TraceContractError, canonical_bytes
from .trace_headroom import admit_reservation

SLOT_BYTES = 32 * 1024
MAX_SLOTS = 512
_EMPTY = b"{}".ljust(SLOT_BYTES, b" ")
_OWNER_KINDS = {"task", "attempt", "run", "operation", "session", "connector"}

SCHEMA_SQL = f"""
CREATE TABLE trace_completion_slots (
    slot_id INTEGER PRIMARY KEY CHECK(slot_id BETWEEN 1 AND {MAX_SLOTS}),
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    filled INTEGER NOT NULL DEFAULT 0 CHECK(filled IN (0,1)),
    record BLOB NOT NULL CHECK(typeof(record)='blob' AND length(record)={SLOT_BYTES}),
    CHECK(length(owner_kind)<=16 AND length(owner_id)<=128 AND length(purpose)<=32)
);
CREATE UNIQUE INDEX trace_completion_owner
ON trace_completion_slots(owner_kind,owner_id,purpose) WHERE owner_id<>'';
"""


@dataclass(frozen=True)
class Obligation:
    kind: str
    owner_id: str
    purpose: str

    def __post_init__(self) -> None:
        if (
            self.kind not in _OWNER_KINDS
            or not isinstance(self.owner_id, str)
            or not 1 <= len(self.owner_id.encode("utf-8")) <= 128
            or not isinstance(self.purpose, str)
            or not 1 <= len(self.purpose.encode("utf-8")) <= 32
        ):
            raise ValueError("invalid completion obligation")

    @property
    def key(self) -> tuple[str, str, str]:
        return self.kind, self.owner_id, self.purpose


def _transaction(db: sqlite3.Connection) -> None:
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")


def reserve(db: sqlite3.Connection, obligation: Obligation) -> int:
    """Allocate before effects, in the admission transaction.

    A new record physically occupies SQLite pages when the transaction commits.
    Empty records can be reused; filled records cannot be reassigned until their
    exact facts have been materialized transactionally. No source/export sequence
    is spent here. Physical quota or commit failure rolls back the admission.
    """
    _transaction(db)
    current = db.execute(
        "SELECT slot_id,filled FROM trace_completion_slots "
        "WHERE owner_kind=? AND owner_id=? AND purpose=?",
        obligation.key,
    ).fetchone()
    if current is not None:
        if current[1]:
            raise TraceContractError("completion_obligation_already_filled")
        admit_reservation(db, obligation.kind, new=False)
        return int(current[0])
    admit_reservation(db, obligation.kind, new=True)
    empty = db.execute(
        "SELECT slot_id FROM trace_completion_slots WHERE owner_id='' "
        "ORDER BY slot_id LIMIT 1"
    ).fetchone()
    if empty is not None:
        slot_id = int(empty[0])
        db.execute(
            "UPDATE trace_completion_slots SET owner_kind=?,owner_id=?,purpose=? "
            "WHERE slot_id=?",
            (*obligation.key, slot_id),
        )
        return slot_id
    slot_id = db.execute(
        "SELECT COALESCE(MAX(slot_id),0)+1 FROM trace_completion_slots"
    ).fetchone()[0]
    if slot_id > MAX_SLOTS:
        raise TraceContractError("quota_exceeded")
    db.execute(
        "INSERT INTO trace_completion_slots "
        "(slot_id,owner_kind,owner_id,purpose,record) VALUES (?,?,?,?,?)",
        (slot_id, *obligation.key, _EMPTY),
    )
    return int(slot_id)


def encode_record(record: dict[str, Any]) -> bytes:
    if type(record) is not dict or not record:
        raise TraceContractError("invalid_completion_record")
    encoded = canonical_bytes(record, limit=SLOT_BYTES)
    return encoded.ljust(SLOT_BYTES, b" ")


def fill(db: sqlite3.Connection, obligation: Obligation, record: dict[str, Any]) -> int:
    """Replace only fixed-size, unindexed values in an already allocated row.

    The caller owns validation/authorization and journal-workspace acquisition.
    It also commits task state, event positions and this record atomically. The
    flag's 0/1 SQLite serial types have equal size; ownership/index keys stay fixed.
    """
    _transaction(db)
    encoded = encode_record(record)
    row = db.execute(
        "SELECT slot_id,filled FROM trace_completion_slots "
        "WHERE owner_kind=? AND owner_id=? AND purpose=?",
        obligation.key,
    ).fetchone()
    if row is None:
        raise TraceContractError("completion_reservation_missing")
    if row[1]:
        raise TraceContractError("completion_obligation_already_filled")
    db.execute(
        "UPDATE trace_completion_slots SET filled=1,record=? WHERE slot_id=?",
        (encoded, row[0]),
    )
    return int(row[0])


def read(db: sqlite3.Connection, slot_id: int) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT filled,record FROM trace_completion_slots WHERE slot_id=?", (slot_id,)
    ).fetchone()
    if row is None or not row[0]:
        return None
    return json.loads(row[1])


def release_unused(db: sqlite3.Connection, obligation: Obligation) -> None:
    """Release an unspent obligation; never discard a completed fact."""
    _transaction(db)
    row = db.execute(
        "SELECT slot_id,filled FROM trace_completion_slots "
        "WHERE owner_kind=? AND owner_id=? AND purpose=?",
        obligation.key,
    ).fetchone()
    if row is None:
        return
    if row[1]:
        raise TraceContractError("completion_record_requires_materialization")
    db.execute(
        "UPDATE trace_completion_slots SET owner_kind='',owner_id='',purpose='' "
        "WHERE slot_id=?",
        (row[0],),
    )
