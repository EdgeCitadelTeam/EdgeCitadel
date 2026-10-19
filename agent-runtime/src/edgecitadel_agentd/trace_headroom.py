"""Sequence positions owed to physically reserved, unfinished completions."""

from __future__ import annotations

import sqlite3

from .trace_contract import TraceContractError
from .trace_counters import MAX_COUNTER

CANONICAL_KINDS = frozenset({"task", "attempt", "run", "operation"})


def pending(db: sqlite3.Connection) -> tuple[int, int]:
    """Derive obligations from the bounded slot pool; never duplicate its state.

    Slots have no source identity until filled. Conservatively protect all
    canonical obligations in every active source/export counter. Each session
    may need one presence announcement; connector audits spend no sequence.
    """
    canonical = presence = 0
    for kind, count in db.execute(
        "SELECT owner_kind,count(*) FROM trace_completion_slots "
        "WHERE owner_id<>'' AND filled=0 GROUP BY owner_kind"
    ):
        if kind in CANONICAL_KINDS:
            canonical += count
        elif kind == "session":
            presence += count
    return canonical, presence


def require(next_value: int, obligations: int) -> None:
    # MAX_COUNTER is the exhausted sentinel; it cannot identify an event.
    if MAX_COUNTER - next_value < obligations:
        raise TraceContractError("trace_sequence_exhausted")


def admit_reservation(db: sqlite3.Connection, kind: str, *, new: bool) -> None:
    canonical, presence = pending(db)
    canonical += int(new and kind in CANONICAL_KINDS)
    presence += int(new and kind == "session")
    if canonical:
        for (next_value,) in db.execute(
            "SELECT next_source_seq FROM trace_sources WHERE active=1 "
            "UNION ALL SELECT g.next_export_seq FROM trace_export_generations g "
            "JOIN trace_sources s USING(node_id,source_epoch) "
            "WHERE g.active=1 AND s.active=1"
        ):
            require(next_value, canonical)
    if presence:
        require(presence_counter(db), presence)


def presence_counter(db: sqlite3.Connection) -> int:
    row = db.execute(
        "SELECT next_id FROM trace_presence_counter WHERE singleton=1"
    ).fetchone()
    if row is None:
        raise TraceContractError("presence_counter_missing")
    return int(row[0])
