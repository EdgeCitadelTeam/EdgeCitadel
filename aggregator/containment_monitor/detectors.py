"""Deterministic alerts derived from trace events.

- ``admission_denied``: a dispatch denied as ``outside_authority_scope`` or by the
  grant (``permission_denied``, the RPC's ``not_authorized``). ``execution_held``
  denials are the monitor's own holds working and are not alerts.
- ``lease_violation``: an invocation or task ended because its session lease,
  deadline or quota ran out: run/task reasons ``session_unavailable`` (lease lost
  while executing), ``deadline_expired`` or ``quota_exceeded``, or task ``expired``.
  A requeue before execution (``session_closed``) is not a violation, and the
  sender's mirror of a remote task is skipped so one expiry yields one alert.
- ``revoked_version_consumed``: an invocation used a version that an operator or
  validator revoked earlier.

Alert identities derive from the triggering event, so replaying a batch after a
rollback produces the same alerts.
"""

from __future__ import annotations

from .model import Alert, detector_alert_id, inv, task
from .provenance import RUN_TERMINAL, TASK_TERMINAL, ProvenanceGraph, occurred_ms

DENIAL_REASONS = frozenset({"outside_authority_scope", "permission_denied"})
LEASE_REASONS = frozenset({"session_unavailable", "deadline_expired", "quota_exceeded"})


def detect(event: dict, graph: ProvenanceGraph) -> list[Alert]:
    kind, phase, attributes = event["kind"], event["phase"], event["attributes"]
    attempt = event.get("execution_attempt_id")
    found: list[tuple[str, str, dict]] = []
    if kind == "dispatch" and phase == "denied" and attempt is not None:
        reason = attributes.get("reason")
        if reason in DENIAL_REASONS:
            found.append(
                (
                    "admission_denied",
                    inv(attempt),
                    {
                        "reason": reason,
                        "recipient_id": attributes["recipient_id"],
                        "dispatch_id": attributes["dispatch_id"],
                    },
                )
            )
    elif kind == "run" and phase in RUN_TERMINAL and attempt is not None:
        if attributes.get("reason") in LEASE_REASONS:
            found.append(
                ("lease_violation", inv(attempt), {"reason": attributes["reason"]})
            )
    elif (
        kind == "task"
        and attributes.get("source_role") != "sender"
        and phase in TASK_TERMINAL
        and (phase == "expired" or attributes.get("reason") in LEASE_REASONS)
    ):
        attempts = graph.invocations_of_task(event["task_id"])
        subject = inv(attempts[-1]) if attempts else task(event["task_id"])
        found.append(
            (
                "lease_violation",
                subject,
                {"reason": attributes.get("reason") or "deadline_expired"},
            )
        )
    elif kind == "lineage" and phase == "used" and attempt is not None:
        version = attributes["version_id"]
        if version in graph.revoked:
            found.append(
                ("revoked_version_consumed", inv(attempt), {"version_id": version})
            )
    at = occurred_ms(event["occurred_at"])
    return [
        Alert(
            detector_alert_id(kind_, subject, event["event_id"]),
            kind_,
            subject,
            at,
            "detector",
            detail,
        )
        for kind_, subject, detail in found
    ]
