"""Pure in-memory state for the watchdog Plugin runtime.

No NATS, no asyncio — kept side-effect-free so tests can drive it
deterministically. The runtime wires NATS handlers around this class.

Design pinned in:
- docs/superpowers/specs/2026-04-29-phase-3-watchdog-and-registry-design.md
- docs/adr/0007-watchdog-trigger-model.md
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime


# Threshold floor + tolerance from the spec.
THRESHOLD_FLOOR_SEC = 20
TOLERANCE_SEC = 5
DEFAULT_INTERVAL_SEC = 30


@dataclass
class WatchdogState:
    """In-memory observation state.

    last_seen[X]          — datetime of most recent heartbeat from X
    declared_interval[X]  — heartbeat_interval_sec from X's card (if registered)
    pending_tasks[X]      — {task_id: original_sender} for in-flight commands TO X
    offline_agents        — sticky set; cleared on next heartbeat from X

    threshold_floor_sec / tolerance_sec / default_interval_sec are
    overridable so tests can drive sub-second cadences without changing
    production defaults. The Watchdog Plugin reads env (WATCHDOG_THRESHOLD_FLOOR_SEC,
    WATCHDOG_TOLERANCE_SEC) and passes through.
    """

    last_seen: dict[str, datetime] = field(default_factory=dict)
    declared_interval: dict[str, int] = field(default_factory=dict)
    pending_tasks: dict[str, dict[str, str]] = field(default_factory=dict)
    offline_agents: set[str] = field(default_factory=set)
    threshold_floor_sec: int = THRESHOLD_FLOOR_SEC
    tolerance_sec: int = TOLERANCE_SEC
    default_interval_sec: int = DEFAULT_INTERVAL_SEC

    # ---- Register ---------------------------------------------------------

    def record_register(self, agent_id: str, *, interval_sec: int) -> None:
        """Update the declared heartbeat interval for `agent_id`. Idempotent."""
        self.declared_interval[agent_id] = interval_sec

    # ---- Heartbeat --------------------------------------------------------

    def record_heartbeat(self, agent_id: str, ts: datetime) -> bool:
        """Record a heartbeat. Returns True if this clears an offline flag
        (meaning the caller should publish a recovery log envelope)."""
        self.last_seen[agent_id] = ts
        if agent_id in self.offline_agents:
            self.offline_agents.discard(agent_id)
            return True
        return False

    # ---- Outbox -----------------------------------------------------------

    def record_outbox_command(
        self, *, recipient_id: str, task_id: str, sender_id: str
    ) -> bool:
        """Observe a command/delegation envelope on `agents.{sender}.outbox`
        (mirrored by the sender per ADR-0006).

        Returns True iff the recipient is already flagged offline — caller
        should immediately synthesise a failure (sticky-offline path) and
        skip adding to pending_tasks.
        """
        if recipient_id in self.offline_agents:
            return True
        self.pending_tasks.setdefault(recipient_id, {})[task_id] = sender_id
        return False

    def record_outbox_result(self, *, worker_id: str, task_id: str) -> None:
        """Observe a result envelope on `agents.{worker}.outbox`. Clears the
        matching pending entry if any. The result's `sender_id` IS the
        worker (the entity publishing the result), and the corresponding
        pending entry was keyed by recipient — which is the same worker."""
        bucket = self.pending_tasks.get(worker_id)
        if bucket is not None:
            bucket.pop(task_id, None)

    # ---- Staleness check --------------------------------------------------

    def staleness_check(
        self, *, now: datetime
    ) -> list[tuple[str, list[tuple[str, str]]]]:
        """Identify agents whose last heartbeat is older than their
        threshold and have not yet been flagged offline. For each, mark
        offline + drain pending_tasks.

        Returns a list of (agent_id, [(task_id, original_sender), ...]).
        Caller publishes synthesised failures for each pending tuple.
        """
        transitions: list[tuple[str, list[tuple[str, str]]]] = []
        for agent_id, last_ts in list(self.last_seen.items()):
            if agent_id in self.offline_agents:
                continue
            interval = self.declared_interval.get(agent_id, self.default_interval_sec)
            threshold = max(2 * interval, self.threshold_floor_sec) + self.tolerance_sec
            if (now - last_ts).total_seconds() <= threshold:
                continue
            self.offline_agents.add(agent_id)
            pending = list(self.pending_tasks.pop(agent_id, {}).items())
            transitions.append((agent_id, pending))
        return transitions
