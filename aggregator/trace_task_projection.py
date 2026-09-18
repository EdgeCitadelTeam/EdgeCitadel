"""Reduce immutable task observations without acquiring execution authority.

This is the task-node part of the M5 projector. Inputs carry persisted Core
ingestion positions: source sequence determines a source's latest observation,
while ingestion position preserves first-observed terminal conflict presentation.
It does not infer run outcome or telemetry completeness from task state.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

from edgecitadel_agentd.trace_contract import (
    TASK_DISPLAY_STATES,
    TraceContractError,
    validate_event,
)

TERMINAL_STATES = frozenset(
    {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
)


@dataclass(frozen=True)
class TaskObservation:
    ingest_seq: int
    event: dict[str, Any]


@dataclass(frozen=True)
class TaskProjection:
    node: dict[str, Any]
    # All terminal evidence survives, including corrections and superseded
    # observations. A supersession reference alone cannot erase authority.
    outcomes: tuple[dict[str, Any], ...]
    perspectives: tuple[dict[str, Any], ...]
    ambiguous_live_state: bool


def reduce_task(observations: Iterable[TaskObservation]) -> TaskProjection:
    """Project exactly one task from accepted raw events, never conflict payloads.

    Input iteration order is irrelevant; persisted ingest_seq supplies observation
    history. Exact repeated event identities count once. Invalid/mixed identities
    are fixed diagnostics rather than silently merged nodes. The caller owns
    paging, the ingestion snapshot and the durable projection transaction.
    """
    events: dict[tuple[str, str, str], TaskObservation] = {}
    source_positions: dict[tuple[str, str, int], tuple[str, str, str]] = {}
    ingest_positions: dict[int, tuple[str, str, str]] = {}
    scope = None
    for observation in observations:
        if type(observation.ingest_seq) is not int or observation.ingest_seq < 1:
            raise ValueError("invalid_projection_ingest_sequence")
        # Validate and detach caller-owned mappings before selecting evidence.
        event = json.loads(validate_event(observation.event))
        if event["kind"] != "task":
            raise ValueError("task_projection_requires_task_events")
        candidate_scope = (event["trace_id"], event["task_id"])
        if scope is not None and candidate_scope != scope:
            raise ValueError("task_projection_scope_mismatch")
        scope = candidate_scope
        key = (event["node_id"], event["source_epoch"], event["event_id"])
        position = (*key[:2], event["source_seq"])
        if source_positions.setdefault(position, key) != key:
            raise TraceContractError("projection_source_position_conflict")
        if ingest_positions.setdefault(observation.ingest_seq, key) != key:
            raise ValueError("projection_ingest_position_conflict")
        previous = events.get(key)
        if previous is not None and previous.event != event:
            raise TraceContractError("projection_event_identity_conflict")
        if previous is None or observation.ingest_seq < previous.ingest_seq:
            events[key] = TaskObservation(observation.ingest_seq, event)
    if not events:
        raise ValueError("task_projection_requires_observations")

    ordered = sorted(events.values(), key=lambda item: item.ingest_seq)
    # Source epochs are independent namespaces, not clock-sortable incarnations.
    # Agent and role stay separate even when multiple agents share one daemon.
    latest: dict[tuple[str, str, str | None, str], TaskObservation] = {}
    for observation in ordered:
        event = observation.event
        source = (
            event["node_id"],
            event["source_epoch"],
            event["agent_id"],
            event["attributes"]["source_role"],
        )
        previous = latest.get(source)
        if previous is None or event["source_seq"] > previous.event["source_seq"]:
            latest[source] = observation

    terminals = [item for item in ordered if item.event["phase"] in TERMINAL_STATES]
    current = sorted(latest.values(), key=lambda item: item.ingest_seq)
    eligible = terminals or current
    # A sender's delivery/deadline view must not overwrite the executing
    # recipient's outcome. Within a role, contradictory terminals retain the
    # first committed presentation, with every candidate visible below.
    recipients = [
        item
        for item in eligible
        if item.event["attributes"]["source_role"] == "recipient"
    ]
    eligible = recipients or eligible
    ambiguous = not terminals and len({item.event["phase"] for item in eligible}) > 1
    selected = None if ambiguous else eligible[0]
    exemplar = ordered[0].event
    node = {
        "id": f"task:{exemplar['task_id']}",
        "kind": "task",
        "task_id": exemplar["task_id"],
        "agent_id": selected.event["agent_id"] if selected else None,
        "state": TASK_DISPLAY_STATES[selected.event["phase"]]
        if selected
        else "unknown",
        "original_state": selected.event["phase"] if selected else None,
        "operation": None,
        "evidence_kind": selected.event["evidence_kind"] if selected else None,
        "conflict": len({item.event["phase"] for item in terminals}) > 1,
        "outcome_candidate_count": len(terminals),
    }
    return TaskProjection(
        node=node,
        outcomes=tuple(_evidence(item) for item in terminals),
        perspectives=tuple(_evidence(item) for item in current),
        ambiguous_live_state=ambiguous,
    )


def _evidence(observation: TaskObservation) -> dict[str, Any]:
    event = observation.event
    return {
        "node_id": event["node_id"],
        "source_epoch": event["source_epoch"],
        "event_id": event["event_id"],
        "source_seq": event["source_seq"],
        "ingest_seq": observation.ingest_seq,
        "agent_id": event["agent_id"],
        "source_role": event["attributes"]["source_role"],
        "execution_attempt_id": event["execution_attempt_id"],
        "phase": event["phase"],
        "state": TASK_DISPLAY_STATES[event["phase"]],
        "reason": event["attributes"].get("reason"),
        "evidence_kind": event["evidence_kind"],
        "supersedes_event_id": event["supersedes_event_id"],
    }
