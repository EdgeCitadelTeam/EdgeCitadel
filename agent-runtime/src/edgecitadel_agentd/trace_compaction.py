"""Bounded removal of retry metadata after its session is permanently closed."""

from __future__ import annotations

import sqlite3

from .trace_contract import TraceContractError

SettledCursor = tuple[str, str, str, int]


def compact_settled_spool(
    db: sqlite3.Connection, *, limit: int = 256, after: SettledCursor | None = None
) -> tuple[int, SettledCursor | None]:
    """Retire payload-free positions backed by this collector's durable cursor.

    Keep payload mappings for restore replay. Missing retired positions are
    explicit loss on collector recovery, never settlement in the new epoch.
    Caller owns the transaction; inspect at most limit indexed candidates.
    Publish the returned scan cursor only after commit; None restarts the scan.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if type(limit) is not int or not 1 <= limit <= 256:
        raise TraceContractError("invalid_compaction_limit")
    seek = ""
    parameters: tuple[object, ...] = (limit,)
    if after is not None:
        seek = "AND (node_id,source_epoch,export_generation,export_seq) > (?,?,?,?) "
        parameters = (*after, limit)
    rows = db.execute(
        "SELECT rowid,node_id,source_epoch,export_generation,export_seq,collector_epoch "
        "FROM trace_spool INDEXED BY trace_spool_settled_empty "
        "WHERE state='core_settled' AND journal_event_id IS NULL "
        + seek
        + "ORDER BY node_id,source_epoch,export_generation,export_seq LIMIT ?",
        parameters,
    ).fetchall()
    removed = 0
    for rowid, node, epoch, generation, position, collector in rows:
        scope = (node, epoch, generation)
        checkpoint = db.execute(
            "SELECT collector_epoch,applied_through FROM trace_source_settlements "
            "WHERE node_id=? AND source_epoch=? AND export_generation=?",
            scope,
        ).fetchone()
        recovery = db.execute(
            "SELECT phase FROM trace_collector_recovery "
            "WHERE node_id=? AND source_epoch=? AND export_generation=?",
            scope,
        ).fetchone()
        if (
            checkpoint is not None
            and collector == checkpoint[0]
            and position <= checkpoint[1]
            and (recovery is None or recovery[0] == "live")
        ):
            db.execute("DELETE FROM trace_spool WHERE rowid=?", (rowid,))
            removed += 1
    cursor = None
    if len(rows) == limit:
        last = rows[-1]
        cursor = (last[1], last[2], last[3], last[4])
    return removed, cursor


def compact_closed_execution(
    db: sqlite3.Connection, *, before_ms: int, limit: int = 256
) -> dict[str, int]:
    """Preserve bind identities and all journal/export evidence.

    Both binding and session closure must predate the retention cutoff. Expired
    leases alone do not qualify: they can still be renewed before reconciliation.
    Closed bind reply bodies become empty tombstones; hashes/identities remain.
    Terminal operation leaves preserve parent foreign keys. Each category is
    limited independently to at most 256 rows per call.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if type(limit) is not int or not 1 <= limit <= 256:
        raise TraceContractError("invalid_compaction_limit")
    receipts = db.execute(
        "SELECT r.rowid FROM trace_bindings b JOIN sessions s USING(session_id) "
        "JOIN trace_requests r USING(binding_id) "
        "WHERE b.closed_at_ms<? AND s.closed_at_ms<? AND r.operation<>'bind' "
        "ORDER BY b.closed_at_ms,r.rowid LIMIT ?",
        (before_ms, before_ms, limit),
    ).fetchall()
    for row in receipts:
        db.execute("DELETE FROM trace_requests WHERE rowid=?", (row[0],))
    bind_payloads = db.execute(
        "SELECT r.rowid FROM trace_bindings b JOIN sessions s USING(session_id) "
        "JOIN trace_requests r USING(binding_id) "
        "WHERE b.closed_at_ms<? AND s.closed_at_ms<? AND r.operation='bind' "
        "AND r.result_json<>'{}' ORDER BY b.closed_at_ms,r.rowid LIMIT ?",
        (before_ms, before_ms, limit),
    ).fetchall()
    for row in bind_payloads:
        db.execute(
            "UPDATE trace_requests SET result_json='{}' WHERE rowid=?", (row[0],)
        )
    operations = db.execute(
        "SELECT o.span_id FROM trace_bindings b JOIN sessions s USING(session_id) "
        "JOIN trace_operations o USING(binding_id) "
        "WHERE b.closed_at_ms<? AND s.closed_at_ms<? "
        "AND o.phase<>'started' AND o.terminal_event_id IS NOT NULL "
        "AND NOT EXISTS (SELECT 1 FROM trace_operations child WHERE child.parent_span_id=o.span_id) "
        "ORDER BY b.closed_at_ms,o.span_id LIMIT ?",
        (before_ms, before_ms, limit),
    ).fetchall()
    for row in operations:
        db.execute("DELETE FROM trace_operations WHERE span_id=?", (row[0],))
    return {
        "receipts": len(receipts),
        "operations": len(operations),
        "bind_payloads": len(bind_payloads),
    }


def compact_lost_spool(db: sqlite3.Connection, *, limit: int = 256) -> int:
    """Replace payload-free per-event loss tombstones with retained range evidence.

    At most limit candidate tombstones are inspected. The selected marker and its
    export intent remain untouched, including when a current writer covers a
    retired epoch. Missing/mismatched/unselected evidence never permits deletion.
    Caller owns the transaction; exceptions must roll it back.
    """
    if not db.in_transaction:
        raise TraceContractError("trace_transaction_required")
    if type(limit) is not int or not 1 <= limit <= 256:
        raise TraceContractError("invalid_compaction_limit")
    candidates = db.execute(
        "SELECT rowid,node_id,source_epoch,export_generation,export_seq FROM trace_spool "
        "WHERE state='lost_with_marker' AND journal_event_id IS NULL "
        "ORDER BY node_id,source_epoch,export_generation,export_seq LIMIT ?",
        (limit,),
    ).fetchall()
    removed = 0
    for rowid, node, epoch, generation, position in candidates:
        covered = db.execute(
            "SELECT 1 FROM trace_journal j "
            "WHERE j.node_id=? "
            "AND COALESCE(json_extract(j.event_json,'$.attributes.affected_source_epoch'),j.source_epoch)=? "
            "AND json_extract(j.event_json,'$.attributes.export_generation')=? "
            "AND json_extract(j.event_json,'$.kind')='coverage' "
            "AND json_extract(j.event_json,'$.phase')='lost' "
            "AND EXISTS (SELECT 1 FROM json_each(j.event_json,'$.attributes.lost_ranges') r "
            "WHERE json_extract(r.value,'$.first')<=? AND json_extract(r.value,'$.last')>=?) "
            "AND EXISTS (SELECT 1 FROM trace_spool marker WHERE marker.node_id=j.node_id "
            "AND marker.source_epoch=j.source_epoch AND marker.journal_event_id=j.event_id "
            "AND marker.state IN ('pending','broker_acked','core_settled')) LIMIT 1",
            (node, epoch, generation, position, position),
        ).fetchone()
        if covered is not None:
            db.execute("DELETE FROM trace_spool WHERE rowid=?", (rowid,))
            removed += 1
    return removed
