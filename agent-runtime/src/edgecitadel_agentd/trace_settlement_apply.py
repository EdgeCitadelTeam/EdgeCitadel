"""Durable source-side v2 page application, independent of executable tasks."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from .trace_contract import TraceContractError, canonical_bytes
from .trace_settlement_pages import validate_page_reply, validate_page_request

if TYPE_CHECKING:
    from .store import AgentdStore

SETTLEMENT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trace_source_settlements (
    node_id TEXT NOT NULL,
    source_epoch TEXT NOT NULL,
    export_generation TEXT NOT NULL,
    collector_epoch TEXT NOT NULL,
    applied_through INTEGER NOT NULL CHECK(applied_through >= 0),
    last_page_json TEXT NOT NULL,
    PRIMARY KEY(node_id, source_epoch, export_generation),
    FOREIGN KEY(node_id, source_epoch, export_generation)
        REFERENCES trace_export_generations(node_id, source_epoch, export_generation)
);
"""


def page_request(store: AgentdStore, scope: tuple[str, str, str]) -> dict[str, Any]:
    """Build from durable applied progress, never a broker ACK or received reply."""
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("settlement_requires_committed_store")
        if (
            store._connection.execute(
                "SELECT 1 FROM trace_export_generations WHERE node_id=? AND source_epoch=? AND export_generation=?",
                scope,
            ).fetchone()
            is None
        ):
            raise TraceContractError("unknown_export_generation")
        row = store._connection.execute(
            "SELECT collector_epoch,applied_through FROM trace_source_settlements "
            "WHERE node_id=? AND source_epoch=? AND export_generation=?",
            scope,
        ).fetchone()
        recovery = store._connection.execute(
            "SELECT phase FROM trace_collector_recovery WHERE node_id=? AND source_epoch=? AND export_generation=?",
            scope,
        ).fetchone()
        if recovery and recovery[0] == "scanning":
            raise TraceContractError("collector_recovery_in_progress")
        if recovery and recovery[0] == "ready":
            row = None
    request = {
        "schema_version": 2,
        "request_id": str(uuid4()),
        "node_id": scope[0],
        "source_epoch": scope[1],
        "export_generation": scope[2],
        "collector_epoch": row[0] if row else None,
        "after_export_seq": row[1] if row else 0,
    }
    validate_page_request(request)
    return request


def apply_page(
    store: AgentdStore, request: dict[str, Any], reply: dict[str, Any]
) -> str:
    """Commit classifications, settlement marks and cursor in one transaction.

    No payload is deleted here. Existing journal retention owns physical removal.
    Collector changes need explicit recovery, not an implicit epoch/cursor reset.
    """
    request = json.loads(validate_page_request(request))
    reply = json.loads(validate_page_reply(reply, request=request))
    if reply["status"] == "error":
        return str(reply["code"])
    page = reply["page"]
    scope = tuple(page[key] for key in ("node_id", "source_epoch", "export_generation"))
    page_json = canonical_bytes(page, limit=18 * 1024).decode()
    after, through, epoch = (
        page["after_export_seq"],
        page["settled_export_seq"],
        page["collector_epoch"],
    )
    with store._lock:
        if store._connection.in_transaction:
            raise TraceContractError("settlement_requires_committed_store")
        with store._connection:
            db = store._connection
            db.execute("BEGIN IMMEDIATE")
            generation = db.execute(
                "SELECT next_export_seq FROM trace_export_generations "
                "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                scope,
            ).fetchone()
            if generation is None:
                raise TraceContractError("unknown_export_generation")
            if through >= generation[0]:
                raise TraceContractError("settlement_beyond_assigned_position")
            previous = db.execute(
                "SELECT collector_epoch,applied_through,last_page_json FROM trace_source_settlements "
                "WHERE node_id=? AND source_epoch=? AND export_generation=?",
                scope,
            ).fetchone()
            recovery = db.execute(
                "SELECT phase,blocked_epochs_json FROM trace_collector_recovery WHERE node_id=? AND source_epoch=? AND export_generation=?",
                scope,
            ).fetchone()
            if recovery:
                if epoch in json.loads(recovery[1]):
                    raise TraceContractError("retired_collector_epoch")
                if recovery[0] == "scanning":
                    raise TraceContractError("collector_recovery_in_progress")
                if recovery[0] == "ready":
                    previous = None
            if previous and previous[0] != epoch:
                raise TraceContractError("collector_epoch_changed")
            if previous and previous[2] == page_json:
                return "duplicate"
            if after != (previous[1] if previous else 0):
                raise TraceContractError("settlement_base_mismatch")
            db.execute(
                "UPDATE trace_spool SET state='core_settled',collector_epoch=?,core_outcome='accepted' "
                "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq>? AND export_seq<=?",
                (epoch, *scope, after, through),
            )
            for field, outcome in (
                ("rejected_ranges", "rejected"),
                ("lost_ranges", "lost"),
            ):
                for item in page[field]:
                    db.execute(
                        "UPDATE trace_spool SET core_outcome=? "
                        "WHERE node_id=? AND source_epoch=? AND export_generation=? AND export_seq BETWEEN ? AND ?",
                        (outcome, *scope, item["first"], item["last"]),
                    )
            db.execute(
                "INSERT INTO trace_source_settlements VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(node_id,source_epoch,export_generation) DO UPDATE SET "
                "collector_epoch=excluded.collector_epoch,applied_through=excluded.applied_through,last_page_json=excluded.last_page_json",
                (*scope, epoch, through, page_json),
            )
            if recovery:
                db.execute(
                    "UPDATE trace_collector_recovery SET phase='live' WHERE node_id=? AND source_epoch=? AND export_generation=?",
                    scope,
                )
    return "applied"
