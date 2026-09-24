"""Optional communication evidence, isolated from task and broker transactions."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

from .trace_correlation import TaskTraceContext
from .trace_journal import TraceJournal

log = logging.getLogger(__name__)


class TransportTrace:
    def __init__(self, store, node_id: str):
        self.store = store
        self.node_id = node_id
        self.dropped = 0

    def record(
        self,
        phase,
        *,
        envelope=None,
        attributes=None,
        kind="transport",
        content=None,
        event_id=None,
    ):
        try:
            context = TaskTraceContext.from_envelope(envelope) if envelope else None
            attrs = {"provenance": "nats_client", **(attributes or {})}
            if envelope:
                attrs.update(
                    {
                        key: envelope[key]
                        for key in ("sender_id", "recipient_id")
                        if key in envelope
                    }
                )
                attrs.update(message_id=envelope["id"], message_type=envelope["type"])
            if self.dropped:
                attrs["dropped_observations"] = self.dropped
            event = {
                "schema_version": 1,
                "event_id": event_id or str(uuid4()),
                "agent_id": None,
                "trace_id": context.trace_id if context else None,
                "task_id": context.task_id if context else None,
                "context_id": context.context_id if context else None,
                "parent_task_id": context.parent_task_id if context else None,
                "parent_run_id": context.parent_run_id if context else None,
                "execution_attempt_id": None,
                "span_id": None,
                "parent_span_id": None,
                "kind": kind,
                "phase": phase,
                "occurred_at": datetime.now(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
                "duration_ms": None,
                "evidence_kind": "source_observed",
                "causes": [],
                "supersedes_event_id": None,
                "attributes": attrs,
            }
            if content is not None:
                from .trace_content import bounded_content

                event["content"] = bounded_content(content)
            db = self.store._connection
            with self.store._lock, db:
                db.execute("BEGIN IMMEDIATE")
                if (
                    event_id
                    and db.execute(
                        "SELECT 1 FROM trace_journal_all WHERE node_id=? AND event_id=? UNION ALL SELECT 1 FROM trace_spool WHERE node_id=? AND event_id=? LIMIT 1",
                        (self.node_id, event_id, self.node_id, event_id),
                    ).fetchone()
                ):
                    return
                TraceJournal(db).record(self.node_id, event, selected=True)
            self.dropped = 0
        except Exception:  # noqa: BLE001 - observation must never affect delivery
            self.dropped = min(self.dropped + 1, 2**31 - 1)
            log.warning("Communication trace observation unavailable")


def delivery_metadata(message):
    try:
        metadata = message.metadata
        return {
            "stream": metadata.stream,
            "consumer": metadata.consumer,
            "stream_sequence": metadata.sequence.stream,
            "consumer_sequence": metadata.sequence.consumer,
            "delivery_attempt": metadata.num_delivered,
            **({"domain": metadata.domain} if metadata.domain else {}),
        }
    except Exception:  # noqa: BLE001 - unsupported broker metadata is not delivery failure
        return {"coverage_reason": "delivery_metadata_unavailable"}


def publication_metadata(ack):
    try:
        return {
            "stream": ack.stream,
            "stream_sequence": ack.seq,
            "duplicate": bool(ack.duplicate),
            **({"domain": ack.domain} if ack.domain else {}),
        }
    except Exception:  # noqa: BLE001 - observation must not repeat a successful publish
        return {"coverage_reason": "publication_metadata_unavailable"}
