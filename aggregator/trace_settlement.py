"""Exact settlement snapshots from committed Core evidence."""

from __future__ import annotations

import heapq
import itertools
import json
import sqlite3

from edgecitadel_agentd.trace_contract import (
    validate_settlement_reply,
    validate_settlement_request,
)
from edgecitadel_agentd.trace_settlement_pages import (
    validate_page_reply,
    validate_page_request,
)


def _merged_ranges(rows):
    """Coalesce a sorted SQL stream without expanding positions or buffering it."""
    current = None
    for first, last in rows:
        if current is not None and first <= current[1] + 1:
            current = (current[0], max(current[1], last))
        else:
            if current is not None:
                yield current
            current = (first, last)
    if current is not None:
        yield current


def _endpoints(rows, kind):
    for first, last in _merged_ranges(rows):
        yield first, kind, 1
        yield last + 1, kind, -1


def settlement_reply(connection: sqlite3.Connection, request: dict) -> dict:
    """Read one snapshot; holes stop progress and unknown scopes have no checkpoint.

    SQL cursors and three coalesced interval streams keep working memory bounded.
    More than 128 disjoint ranges in a class returns an explicit unavailable reply;
    ranges are never dropped to squeeze a falsely complete checkpoint onto the wire.
    """
    request = json.loads(validate_settlement_request(request))
    reply = _read_settlement(connection, request, paged=False)
    validate_settlement_reply(reply, request=request)
    return reply


def settlement_page_reply(connection: sqlite3.Connection, request: dict) -> dict:
    """V2 page covers only (after_export_seq, settled_export_seq], never its prefix."""
    request = json.loads(validate_page_request(request))
    reply = _read_settlement(connection, request, paged=True)
    validate_page_reply(reply, request=request)
    return reply


def _read_settlement(
    connection: sqlite3.Connection, request: dict, *, paged: bool
) -> dict:
    if connection.in_transaction:
        raise ValueError("settlement_requires_idle_connection")
    scope = tuple(
        request[key] for key in ("node_id", "source_epoch", "export_generation")
    )
    after = request["after_export_seq"] if paged else 0
    version = 2 if paged else 1
    reply = {
        "schema_version": version,
        "request_id": request["request_id"],
        "status": "error",
        "code": "unknown_source",
        "retry_after_ms": 500,
    }
    with connection:
        connection.execute("BEGIN")
        epoch = connection.execute(
            "SELECT collector_epoch FROM trace_collector WHERE singleton=1"
        ).fetchone()[0]
        if (
            paged
            and request["collector_epoch"] is not None
            and request["collector_epoch"] != epoch
        ):
            reply["code"] = "collector_changed"
            return reply
        known = any(
            connection.execute(
                f"SELECT 1 FROM {table} WHERE node_id=? AND source_epoch=? AND export_generation=? LIMIT 1",
                scope,
            ).fetchone()
            is not None
            for table in (
                "trace_ingest_positions",
                "trace_rejected_positions",
                "trace_loss_ranges",
            )
        )
        upper = 9_007_199_254_740_991
        if paged:
            # Bound dense evidence reads while preserving constant-size jumps over
            # large explicit loss ranges. A simple position window would turn a
            # trillion-position loss marker into billions of needless requests.
            cutoff = connection.execute(
                "SELECT export_seq FROM trace_ingest_positions "
                "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq>? "
                "UNION ALL SELECT export_seq FROM trace_rejected_positions "
                "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq>? "
                "ORDER BY export_seq LIMIT 1 OFFSET 512",
                (*scope, after, *scope, after),
            ).fetchone()
            if cutoff is not None:
                upper = cutoff[0] - 1
        accepted = connection.execute(
            "SELECT export_seq,export_seq FROM trace_ingest_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? "
            "AND outcome IN ('accepted','duplicate') AND export_seq>? AND export_seq<=? ORDER BY export_seq",
            (*scope, after, upper),
        )
        rejected = connection.execute(
            "SELECT export_seq,export_seq FROM trace_ingest_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND outcome='conflict' AND export_seq>? AND export_seq<=? "
            "UNION ALL SELECT export_seq,export_seq FROM trace_rejected_positions "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq>? AND export_seq<=? ORDER BY export_seq",
            (*scope, after, upper, *scope, after, upper),
        )
        lost = connection.execute(
            "SELECT first_seq,last_seq FROM trace_loss_ranges "
            "WHERE node_id=? AND source_epoch=? AND export_generation=? AND last_seq>? AND first_seq<=? ORDER BY first_seq,last_seq",
            (*scope, after, upper),
        )
        endpoints = heapq.merge(
            _endpoints(accepted, 0),
            _endpoints(rejected, 1),
            _endpoints(
                ((max(first, after + 1), min(last, upper)) for first, last in lost), 2
            ),
        )
        counts = [0, 0, 0]
        previous, through = after + 1, after
        ranges = {1: [], 2: []}
        overflow = False
        for position, group in itertools.groupby(endpoints, key=lambda item: item[0]):
            known = True
            if position > previous:
                active = next((kind for kind in range(3) if counts[kind]), None)
                if active is None:
                    break
                if active in ranges:
                    target = ranges[active]
                    if target and target[-1]["last"] + 1 == previous:
                        target[-1]["last"] = position - 1
                    elif len(target) < 128:
                        target.append({"first": previous, "last": position - 1})
                    else:
                        overflow = True
                        break
                through = position - 1
            for _, kind, delta in group:
                counts[kind] += delta
            previous = position
        if overflow and not paged:
            reply["code"] = "temporarily_unavailable"
        elif known:
            reply = {
                "schema_version": version,
                "request_id": request["request_id"],
                "status": "ok",
                "page" if paged else "checkpoint": {
                    "schema_version": version,
                    "node_id": scope[0],
                    "source_epoch": scope[1],
                    "export_generation": scope[2],
                    "collector_epoch": epoch,
                    "settled_export_seq": through,
                    "rejected_ranges": ranges[1],
                    "lost_ranges": ranges[2],
                },
            }
            if paged:
                reply["page"].update(
                    after_export_seq=after,
                    more=overflow
                    or (through == upper and upper < 9_007_199_254_740_991),
                )
        # Close SQL cursors even when a gap or range limit ended the sweep early.
        accepted.close()
        rejected.close()
        lost.close()
    return reply
