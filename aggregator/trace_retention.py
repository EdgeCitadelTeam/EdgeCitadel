"""Bounded Core payload expiry preserving identity and settlement evidence."""

from __future__ import annotations

import sqlite3

from . import trace_capacity, trace_payloads

RETENTION_MS = 7 * 24 * 60 * 60 * 1000
SCAN_ROWS = 256


def expire_payloads(connection: sqlite3.Connection, *, now_ms: int) -> dict[str, int]:
    """One keyset scan transaction; expiry never recreates or deletes identities.

    Eligibility uses Core receipt time. Scanning wraps so a recent row skipped on
    an earlier pass can expire later. Rows, counters and cursor commit together.
    Physical pressure may refuse maintenance; no task API is invoked.
    """
    if type(now_ms) is not int or not 0 <= now_ms <= 2**53 - 1:
        raise ValueError("invalid_retention_time")
    if connection.in_transaction:
        raise ValueError("retention_requires_idle_connection")
    if trace_payloads.is_prepared(connection):
        count = trace_payloads.expire_batch(
            connection,
            before_ms=max(0, now_ms - RETENTION_MS),
            now_ms=now_ms,
        )
        return {
            "scanned_rows": count,
            "expired_payloads": count,
            "after_ingest_seq": 0,
            "observed_at_ms": now_ms,
            "retention_ms": RETENTION_MS,
        }
    trace_capacity.reclaim(connection)
    with connection:
        connection.execute("BEGIN IMMEDIATE")
        trace_capacity.usage(connection)
        after = connection.execute(
            "SELECT after_ingest_seq FROM trace_retention_state WHERE singleton=1"
        ).fetchone()[0]
        rows = connection.execute(
            "SELECT ingest_seq,received_at_ms,payload_expired_at_ms "
            "FROM trace_raw_events WHERE ingest_seq>? ORDER BY ingest_seq LIMIT ?",
            (after, SCAN_ROWS),
        ).fetchall()
        eligible = [
            row[0] for row in rows if row[1] < now_ms - RETENTION_MS and row[2] is None
        ]
        next_after = rows[-1][0] if len(rows) == SCAN_ROWS else 0
        if eligible or next_after != after:
            trace_capacity.admit_write(connection)
            connection.executemany(
                "UPDATE trace_raw_events SET event_json='',payload_expired_at_ms=? "
                "WHERE ingest_seq=? AND payload_expired_at_ms IS NULL",
                [(now_ms, sequence) for sequence in eligible],
            )
            connection.execute(
                "UPDATE trace_retention_state SET after_ingest_seq=? WHERE singleton=1",
                (next_after,),
            )
    return {
        "scanned_rows": len(rows),
        "expired_payloads": len(eligible),
        "after_ingest_seq": next_after,
        "observed_at_ms": now_ms,
        "retention_ms": RETENTION_MS,
    }
