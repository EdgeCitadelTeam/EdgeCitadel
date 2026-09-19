"""Daemon-only, bounded authentication rejection evidence without caller metadata."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from . import trace_capacity
from .trace_contract import TraceContractError, canonical_bytes
from .trace_journal import TraceJournal

if TYPE_CHECKING:
    from .store import AgentdStore

MAX_REJECTION_COUNT = 9007199254740991


def note_authentication_rejection(store: AgentdStore) -> None:
    """No request arguments or disk I/O; failure handling cannot echo credentials."""
    with store._lock:
        store._authentication_rejections = min(
            MAX_REJECTION_COUNT, store._authentication_rejections + 1
        )


def flush_authentication_rejections(
    store: AgentdStore, *, now_ms: int | None = None
) -> bool:
    """Coalesce at most one immutable event per source and UTC calendar minute.

    Protected security journal rows are the durable minute receipts, including
    after restart or clock rollback. A future retention policy must preserve that
    receipt before removing such rows. Pending counts are volatile and saturating;
    evidence is a lower bound, never an exact lifetime rejection total.
    """
    now = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    with store._lock:
        count = store._authentication_rejections
        if not count:
            return False
        try:
            with (store.state_directory.parent / "node.json").open("rb") as source:
                raw = source.read(65537)
            node_id = json.loads(raw)["agent_id"]
            if (
                len(raw) > 65536
                or not isinstance(node_id, str)
                or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", node_id) is None
            ):
                raise ValueError
        except (OSError, ValueError, KeyError, TypeError) as error:
            raise TraceContractError("storage_unavailable") from error
        db = store._connection
        with db:
            db.execute("BEGIN IMMEDIATE")
            journal = TraceJournal(db)
            epoch, _generation = journal.initialize(node_id)
            digest = hashlib.sha256(
                b"edgecitadel-authentication-rejection-minute-v1\0"
                + canonical_bytes([node_id, epoch, now // 60000])
            ).digest()
            event_id = str(UUID(bytes=digest[:16], version=4))
            if (
                db.execute(
                    "SELECT 1 FROM trace_journal WHERE node_id=? AND source_epoch=? AND event_id=?",
                    (node_id, epoch, event_id),
                ).fetchone()
                is not None
            ):
                return False
            # A new minute adds a protected durable identity. Keep the bounded
            # volatile count pending when storage cannot admit that identity.
            try:
                pressure = trace_capacity.physical_storage(db)["pressure_bytes"]
            except OSError as error:
                raise TraceContractError("storage_unavailable") from error
            if pressure >= trace_capacity.PHYSICAL_PRESSURE_BYTES:
                raise TraceContractError("quota_exceeded")
            event = {
                "schema_version": 1,
                "event_id": event_id,
                "agent_id": None,
                "trace_id": None,
                "context_id": None,
                "task_id": None,
                "parent_task_id": None,
                "parent_run_id": None,
                "execution_attempt_id": None,
                "span_id": None,
                "parent_span_id": None,
                "kind": "security",
                "phase": "authentication_rejected",
                "occurred_at": datetime.fromtimestamp(now / 1000, UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "duration_ms": None,
                "evidence_kind": "source_observed",
                "attributes": {"reason": "authentication_failed", "count": count},
                "causes": [],
                "supersedes_event_id": None,
            }
            journal.record(node_id, event, selected=True, reserve_capacity=True)
        # Commit failure leaves the volatile count intact. The lock also prevents
        # increments between taking the count and acknowledging its persistence.
        store._authentication_rejections = 0
        return True
