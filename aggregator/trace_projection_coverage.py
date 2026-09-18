"""Snapshot-bounded coverage from committed source facts and receipt positions.

Only accepted trace identities establish run membership. Source-wide loss or a
rejection cannot identify its missing run; it stays source-level uncertainty.
Reconciliation describes known source positions, never a closed participant set.
"""

from __future__ import annotations

import json

from edgecitadel_agentd.trace_contract import coverage_scope

from .trace_projection_tables import ProjectionTables

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS {trace_projection_run_events} (
        node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,event_id TEXT NOT NULL,
        event_sha256 TEXT NOT NULL,trace_id TEXT NOT NULL,ingest_seq INTEGER NOT NULL,agent_id TEXT,
        PRIMARY KEY(node_id,source_epoch,event_id)
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_scope_progress} (
        node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,export_generation TEXT NOT NULL,
        through_seq INTEGER NOT NULL DEFAULT 0,
        has_loss INTEGER NOT NULL DEFAULT 0,has_rejection INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(node_id,source_epoch,export_generation)
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_intervals} (
        node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,export_generation TEXT NOT NULL,
        disposition TEXT NOT NULL,first_seq INTEGER NOT NULL,last_seq INTEGER NOT NULL,
        PRIMARY KEY(node_id,source_epoch,export_generation,disposition,first_seq)
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_run_scopes} (
        trace_id TEXT NOT NULL,node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,
        export_generation TEXT NOT NULL,required_through INTEGER NOT NULL,
        PRIMARY KEY(trace_id,node_id,source_epoch,export_generation)
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_run_coverage} (
        trace_id TEXT PRIMARY KEY,unknown INTEGER NOT NULL,unpositioned_loss INTEGER NOT NULL,
        unsupported_json TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_source_coverage} (
        node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,uncertain INTEGER NOT NULL,
        PRIMARY KEY(node_id,source_epoch)
    )""",
    """CREATE TABLE IF NOT EXISTS {trace_projection_run_losses} (
        trace_id TEXT NOT NULL,node_id TEXT NOT NULL,source_epoch TEXT NOT NULL,
        export_generation TEXT NOT NULL,first_seq INTEGER NOT NULL,last_seq INTEGER NOT NULL,
        PRIMARY KEY(trace_id,node_id,source_epoch,export_generation,first_seq,last_seq)
    )""",
)


def _required(db: ProjectionTables, trace_id: str, scope: tuple, through: int) -> None:
    db.execute(
        "INSERT INTO {trace_projection_run_scopes} VALUES(?,?,?,?,?) "
        "ON CONFLICT(trace_id,node_id,source_epoch,export_generation) DO UPDATE SET "
        "required_through=max(required_through,excluded.required_through)",
        (trace_id, *scope, through),
    )


def _merge_interval(
    db: ProjectionTables, scope: tuple, disposition: str, first: int, last: int
) -> None:
    # Keep only consumed receipts. Querying the live ledger would leak future
    # ingestion into old projection snapshots or scan the entire future backlog.
    overlap = db.execute(
        "SELECT first_seq,last_seq FROM {trace_projection_intervals} WHERE node_id=? "
        "AND source_epoch=? AND export_generation=? AND disposition=? AND first_seq<=? AND last_seq>=?",
        (*scope, disposition, last + 1, first - 1),
    ).fetchall()
    if overlap:
        first = min(first, min(row[0] for row in overlap))
        last = max(last, max(row[1] for row in overlap))
        db.execute(
            "DELETE FROM {trace_projection_intervals} WHERE node_id=? AND source_epoch=? "
            "AND export_generation=? AND disposition=? AND first_seq>=? AND first_seq<=?",
            (*scope, disposition, first, last),
        )
    db.execute(
        "INSERT INTO {trace_projection_intervals} VALUES(?,?,?,?,?,?)",
        (*scope, disposition, first, last),
    )


def _include(
    db: ProjectionTables, scope: tuple, first: int, last: int, outcome: str
) -> None:
    _merge_interval(db, scope, "known", first, last)
    if outcome in {"accepted", "duplicate"}:
        _merge_interval(db, scope, "accepted", first, last)
    frontier = db.execute(
        "SELECT last_seq FROM {trace_projection_intervals} WHERE node_id=? AND source_epoch=? "
        "AND export_generation=? AND disposition='known' AND first_seq=1",
        scope,
    ).fetchone()
    db.execute(
        "INSERT INTO {trace_projection_scope_progress} VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(node_id,source_epoch,export_generation) DO UPDATE SET "
        "through_seq=excluded.through_seq,has_loss=max(has_loss,excluded.has_loss),"
        "has_rejection=max(has_rejection,excluded.has_rejection)",
        (
            *scope,
            frontier[0] if frontier else 0,
            int(outcome == "lost"),
            int(outcome in {"rejected", "conflict"}),
        ),
    )


def _record_fact(db: ProjectionTables, fact: tuple) -> None:
    node, epoch, trace_id, kind, phase, attrs = fact
    uncertain = (
        (
            kind == "coverage"
            and (
                phase in {"unknown", "lost"}
                or bool(attrs.get("unsupported_families"))
                or attrs.get("dropped_observations", 0) > 0
            )
        )
        or (kind == "source" and phase == "restored")
        or kind == "payload"
    )
    if trace_id is None or kind in {"source", "payload"}:
        if uncertain:
            db.execute(
                "INSERT INTO {trace_projection_source_coverage} VALUES(?,?,1) "
                "ON CONFLICT(node_id,source_epoch) DO UPDATE SET uncertain=1",
                (node, epoch),
            )
        return
    if kind != "coverage":
        return
    existing = db.execute(
        "SELECT unsupported_json FROM {trace_projection_run_coverage} WHERE trace_id=?",
        (trace_id,),
    ).fetchone()
    unsupported = set(json.loads(existing[0])) if existing else set()
    unsupported.update(attrs.get("unsupported_families", []))
    db.execute(
        "INSERT INTO {trace_projection_run_coverage} VALUES(?,?,?,?) ON CONFLICT(trace_id) DO UPDATE SET "
        "unknown=max(unknown,excluded.unknown),unpositioned_loss=max(unpositioned_loss,excluded.unpositioned_loss),"
        "unsupported_json=excluded.unsupported_json",
        (
            trace_id,
            int(phase == "unknown"),
            int(attrs.get("dropped_observations", 0) > 0),
            json.dumps(sorted(unsupported)),
        ),
    )
    db.executemany(
        "INSERT OR IGNORE INTO {trace_projection_run_losses} VALUES(?,?,?,?,?,?)",
        [
            (
                trace_id,
                node,
                epoch,
                attrs["export_generation"],
                item["first"],
                item["last"],
            )
            for item in attrs.get("lost_ranges", [])
        ],
    )


def _scope_update(db: ProjectionTables, scope: tuple) -> dict:
    row = db.execute(
        "SELECT through_seq FROM {trace_projection_scope_progress} WHERE node_id=? "
        "AND source_epoch=? AND export_generation=?",
        scope,
    ).fetchone()
    return dict(
        zip(
            ("node_id", "source_epoch", "export_generation", "export_seq"),
            (*scope, row[0] if row else 0),
        )
    )


def project_facts(
    db: ProjectionTables, seq: int, event: dict | None, *, expired: bool = False
) -> dict:
    """Record one committed ingestion position inside the projector transaction.

    A commit may have both a raw event and receipt, or only a duplicate/rejection/
    conflict receipt. Conflicts never supply a trace ID from their rejected body.
    """
    if not db.in_transaction:
        raise ValueError("projection_transaction_required")
    scopes: set[tuple[str, str, str]] = set()
    fact = None
    if event is not None:
        if event["trace_id"] is not None:
            raw_hash = db.execute(
                "SELECT event_sha256 FROM trace_raw_events WHERE ingest_seq=?", (seq,)
            ).fetchone()[0]
            db.execute(
                "INSERT INTO {trace_projection_run_events} VALUES(?,?,?,?,?,?,?)",
                (
                    event["node_id"],
                    event["source_epoch"],
                    event["event_id"],
                    raw_hash,
                    event["trace_id"],
                    seq,
                    event["agent_id"],
                ),
            )
        if event["kind"] in {"coverage", "source", "security"}:
            scope_epoch = event["source_epoch"]
            attrs = event["attributes"]
            if event["kind"] == "coverage":
                scope = coverage_scope(event)
                scope_epoch = scope[1]
                scopes.add(scope)
                if event["trace_id"] is not None:
                    _required(db, event["trace_id"], scope, attrs["through_export_seq"])
            elif event["kind"] == "source" and event["phase"] == "restored":
                scope_epoch = attrs.get("previous_source_epoch") or scope_epoch
            fact = (
                event["node_id"],
                scope_epoch,
                event["trace_id"],
                event["kind"],
                event["phase"],
                attrs,
            )
    elif expired:
        node, epoch = db.execute(
            "SELECT node_id,source_epoch FROM trace_raw_events WHERE ingest_seq=?",
            (seq,),
        ).fetchone()
        fact = (node, epoch, None, "payload", "expired", {})
    if fact:
        _record_fact(db, fact)

    receipt = db.execute(
        "SELECT node_id,source_epoch,export_generation,export_seq,outcome,event_id,event_sha256 "
        "FROM trace_ingest_positions WHERE ingest_seq=?",
        (seq,),
    ).fetchone()
    if receipt is None:
        receipt = db.execute(
            "SELECT node_id,source_epoch,export_generation,export_seq,'rejected',NULL,event_sha256 "
            "FROM trace_rejected_positions WHERE ingest_seq=? UNION ALL "
            "SELECT node_id,source_epoch,export_generation,export_seq,'conflict',NULL,received_sha256 "
            "FROM trace_ingest_conflicts WHERE ingest_seq=?",
            (seq, seq),
        ).fetchone()
    # Loss receipts survive payload expiry in the Core's immutable range ledger.
    # Only ranges whose marker is this consumed commit may enter the projection.
    losses = db.execute(
        "SELECT l.node_id,l.source_epoch,l.export_generation,l.first_seq,l.last_seq "
        "FROM trace_loss_ranges l JOIN trace_raw_events r ON r.node_id=l.node_id "
        "AND r.source_epoch=l.writer_epoch AND r.event_id=l.event_id WHERE r.ingest_seq=?",
        (seq,),
    ).fetchall()
    for node, epoch, generation, first, last in losses:
        scope = (node, epoch, generation)
        scopes.add(scope)
        _include(db, scope, first, last, "lost")
    trace_id = None
    detail = None
    if receipt is not None:
        scope = tuple(receipt[:3])
        scopes.add(scope)
        _include(db, scope, receipt[3], receipt[3], receipt[4])
        if receipt[4] in {"accepted", "duplicate"}:
            membership = db.execute(
                "SELECT trace_id FROM {trace_projection_run_events} WHERE node_id=? "
                "AND source_epoch=? AND event_id=? AND event_sha256=?",
                (receipt[0], receipt[1], receipt[5], receipt[6]),
            ).fetchone()
            if membership:
                trace_id = membership[0]
                _required(db, trace_id, scope, receipt[3])
        detail = dict(
            zip(
                (
                    "node_id",
                    "source_epoch",
                    "export_generation",
                    "export_seq",
                    "outcome",
                ),
                receipt[:5],
            )
        )
    return {
        "receipt_trace_id": trace_id,
        "receipt": detail,
        "loss_ranges": [
            dict(
                zip(
                    ("node_id", "source_epoch", "export_generation", "first", "last"),
                    row,
                )
            )
            for row in losses
        ],
        "scope_updates": [_scope_update(db, scope) for scope in sorted(scopes)],
        "source_fact": dict(
            zip(
                ("node_id", "source_epoch", "trace_id", "kind", "phase", "attributes"),
                fact,
            )
        )
        if fact
        else None,
    }


def run_coverage(db: ProjectionTables, trace_id: str, *, unresolved: bool) -> dict:
    """Read coverage from the caller's projection snapshot, with honest scope.

    There is no authoritative participant-set closure in event v1. Known-source
    reconciliation must therefore never be advertised as complete forever.
    """
    if not db.in_transaction:
        raise ValueError("projection_transaction_required")
    scopes = db.execute(
        "SELECT s.node_id,s.source_epoch,s.export_generation,s.required_through,COALESCE(p.through_seq,0),COALESCE(p.has_loss,0),COALESCE(p.has_rejection,0) "
        "FROM {trace_projection_run_scopes} s LEFT JOIN {trace_projection_scope_progress} p "
        "USING(node_id,source_epoch,export_generation) WHERE s.trace_id=? "
        "ORDER BY s.node_id,s.source_epoch,s.export_generation LIMIT 1001",
        (trace_id,),
    ).fetchall()
    if len(scopes) > 1000:
        raise ValueError("projection_coverage_expansion_required")
    saved = db.execute(
        "SELECT unknown,unpositioned_loss,unsupported_json FROM {trace_projection_run_coverage} WHERE trace_id=?",
        (trace_id,),
    ).fetchone()
    unknown = bool(saved and saved[0])
    unsupported = json.loads(saved[2]) if saved else []
    # A later accepted receipt can repair a previously declared export loss.
    # Unpositioned producer loss has no corresponding ledger repair proof.
    missing = db.execute(
        "SELECT 1 FROM {trace_projection_run_losses} l WHERE l.trace_id=? AND NOT EXISTS("
        "SELECT 1 FROM {trace_projection_intervals} a WHERE a.node_id=l.node_id "
        "AND a.source_epoch=l.source_epoch AND a.export_generation=l.export_generation "
        "AND a.disposition='accepted' AND a.first_seq<=l.first_seq AND a.last_seq>=l.last_seq) LIMIT 1",
        (trace_id,),
    ).fetchone()
    gap = bool(missing or (saved and saved[1]))
    source_uncertainty = (
        any(lost or rejected for *_, lost, rejected in scopes)
        or db.execute(
            "SELECT 1 FROM {trace_projection_source_coverage} f WHERE EXISTS("
            "SELECT 1 FROM {trace_projection_run_scopes} s WHERE s.trace_id=? "
            "AND s.node_id=f.node_id AND s.source_epoch=f.source_epoch) LIMIT 1",
            (trace_id,),
        ).fetchone()
        is not None
    )
    return {
        "coverage": {
            # Partial also includes the unclosed participant set. Successful task
            # outcome and a caught-up known source cannot remove that uncertainty.
            "partial": True,
            "catching_up": any(
                through < required for _, _, _, required, through, _, _ in scopes
            ),
            "gap": gap,
            "unknown_sources": True,
            "unsupported_families": sorted(unsupported),
            "reconciled_through": [
                dict(
                    zip(
                        ("node_id", "source_epoch", "export_generation", "export_seq"),
                        (node, epoch, generation, min(required, through)),
                    )
                )
                for node, epoch, generation, required, through, _, _ in scopes
            ],
        },
        "coverage_reasons": {
            "participant_set_unknown": True,
            "unresolved_ancestry": unresolved,
            "source_uncertainty": source_uncertainty,
            "run_coverage_unknown": unknown,
        },
    }
