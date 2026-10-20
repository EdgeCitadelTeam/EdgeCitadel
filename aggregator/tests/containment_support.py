"""Scripted, schema-valid trace events and a Core + monitor harness.

``TraceBuilder`` emits the same event shapes agentd journals for task, run,
dispatch and lineage boundaries (validated with the trace contract), with explicit
times, so tests control the timeline exactly. Effect events (forthcoming, not yet
in the schema) are built separately and applied to the graph directly.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aggregator import trace_store
from aggregator.containment_monitor.config import MonitorConfig
from aggregator.containment_monitor.control import FakeControlClient
from aggregator.containment_monitor.service import ContainmentMonitor
from edgecitadel_agentd.trace_contract import canonical_bytes, validate_event

BASE_MS = 1_800_000_000_000


def iso(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class TraceBuilder:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.trace_id = uuid4().hex
        self._seq: dict[str, int] = {}
        self._epoch: dict[str, str] = {}
        self.tasks: dict[str, dict] = {}
        self.attempts: dict[str, dict] = {}
        self.versions: dict[tuple[str, str], str] = {}

    def _event(
        self,
        node,
        kind,
        phase,
        at,
        *,
        agent,
        task_id=None,
        parent_task_id=None,
        attempt=None,
        context=None,
        attributes=None,
        span=None,
        evidence="source_observed",
    ):
        self._seq[node] = self._seq.get(node, 0) + 1
        event = {
            "schema_version": 1,
            "event_id": str(uuid4()),
            "node_id": node,
            "source_epoch": self._epoch.setdefault(node, str(uuid4())),
            "source_seq": self._seq[node],
            "agent_id": agent,
            "trace_id": self.trace_id,
            "context_id": context,
            "task_id": task_id,
            "parent_task_id": parent_task_id,
            "parent_run_id": None,
            "execution_attempt_id": attempt,
            "span_id": span,
            "parent_span_id": None,
            "kind": kind,
            "phase": phase,
            "occurred_at": iso(BASE_MS + at),
            "duration_ms": None,
            "evidence_kind": evidence,
            "causes": [],
            "attributes": attributes or {},
            "supersedes_event_id": None,
        }
        validate_event(event)
        self.events.append(event)
        return event

    @staticmethod
    def version() -> str:
        return secrets.token_hex(32)

    # -- tasks and invocations ------------------------------------------------
    def root(
        self, agent: str, node: str, at: int, *, context: str | None = None
    ) -> str:
        task_id = str(uuid4())
        context = context or task_id
        self.tasks[task_id] = {
            "agent": agent,
            "node": node,
            "parent": None,
            "context": context,
            "sender": None,
        }
        common = {"task_id": task_id, "context": context}
        self._event(
            node,
            "task",
            "queued",
            at,
            agent="operator",
            attributes={"source_role": "sender"},
            **common,
        )
        self._event(
            node,
            "task",
            "offered",
            at,
            agent="edgecitadel-system",
            attributes={"source_role": "daemon"},
            **common,
        )
        self._event(
            node,
            "task",
            "accepted",
            at,
            agent=agent,
            attributes={"source_role": "recipient"},
            **common,
        )
        self.versions[("request", task_id)] = self.version()
        return task_id

    def start(self, task_id: str, at: int) -> str:
        task = self.tasks[task_id]
        attempt = str(uuid4())
        self.attempts[attempt] = {
            "task": task_id,
            "agent": task["agent"],
            "node": task["node"],
        }
        common = {
            "task_id": task_id,
            "parent_task_id": task["parent"],
            "context": task["context"],
            "attempt": attempt,
        }
        self._event(task["node"], "run", "started", at, agent=task["agent"], **common)
        self._event(
            task["node"],
            "lineage",
            "used",
            at,
            agent=task["agent"],
            attributes={
                "lineage_version": 1,
                "entity": "request",
                "entity_task_id": task_id,
                "version_id": self.versions[("request", task_id)],
                "external": task["parent"] is None,
            },
            **common,
        )
        self._event(
            task["node"],
            "task",
            "running",
            at,
            agent=task["agent"],
            attributes={"source_role": "recipient"},
            **common,
        )
        return attempt

    def dispatch(
        self, attempt: str, recipient: str, recipient_node: str, at: int
    ) -> str:
        sender = self.attempts[attempt]
        parent_task = self.tasks[sender["task"]]
        child = str(uuid4())
        self.tasks[child] = {
            "agent": recipient,
            "node": recipient_node,
            "parent": sender["task"],
            "context": parent_task["context"],
            "sender": attempt,
        }
        common = {
            "task_id": sender["task"],
            "parent_task_id": parent_task["parent"],
            "context": parent_task["context"],
            "attempt": attempt,
        }
        self._event(
            sender["node"],
            "dispatch",
            "allowed",
            at,
            agent=sender["agent"],
            attributes={
                "dispatch_id": str(uuid4()),
                "recipient_id": recipient,
                "skill_id": None,
                "grant_version": None,
                "reason": None,
                "child_task_id": child,
            },
            **common,
        )
        self._event(
            sender["node"],
            "task",
            "queued",
            at,
            agent=sender["agent"],
            task_id=child,
            parent_task_id=sender["task"],
            context=parent_task["context"],
            attributes={"source_role": "sender"},
        )
        version = self.versions[("request", child)] = self.version()
        self._event(
            sender["node"],
            "lineage",
            "generated",
            at,
            agent=sender["agent"],
            attributes={
                "lineage_version": 1,
                "entity": "request",
                "entity_task_id": child,
                "version_id": version,
                "external": False,
            },
            **common,
        )
        return child

    def arrive(self, task_id: str, at: int) -> None:
        task = self.tasks[task_id]
        sender = (
            self.attempts[task["sender"]]["agent"] if task["sender"] else "operator"
        )
        for phase in ("queued", "offered"):
            self._event(
                task["node"],
                "task",
                phase,
                at,
                agent=sender,
                task_id=task_id,
                parent_task_id=task["parent"],
                context=task["context"],
                attributes={"source_role": "daemon"},
            )
        self._event(
            task["node"],
            "task",
            "accepted",
            at,
            agent=task["agent"],
            task_id=task_id,
            parent_task_id=task["parent"],
            context=task["context"],
            attributes={"source_role": "recipient"},
        )

    def deny(self, attempt: str, recipient: str, reason: str, at: int) -> dict:
        sender = self.attempts[attempt]
        task = self.tasks[sender["task"]]
        return self._event(
            sender["node"],
            "dispatch",
            "denied",
            at,
            agent=sender["agent"],
            task_id=sender["task"],
            parent_task_id=task["parent"],
            context=task["context"],
            attempt=attempt,
            attributes={
                "dispatch_id": str(uuid4()),
                "recipient_id": recipient,
                "skill_id": None,
                "grant_version": None,
                "reason": reason,
            },
        )

    def complete(self, attempt: str, at: int) -> str:
        run = self.attempts[attempt]
        task = self.tasks[run["task"]]
        version = self.versions[("result", run["task"])] = self.version()
        common = {
            "task_id": run["task"],
            "parent_task_id": task["parent"],
            "context": task["context"],
            "attempt": attempt,
        }
        self._event(
            run["node"],
            "task",
            "completed",
            at,
            agent=run["agent"],
            attributes={"source_role": "recipient"},
            **common,
        )
        self._event(
            run["node"],
            "lineage",
            "generated",
            at,
            agent=run["agent"],
            attributes={
                "lineage_version": 1,
                "entity": "result",
                "entity_task_id": run["task"],
                "version_id": version,
                "external": False,
            },
            **common,
        )
        self._event(run["node"], "run", "completed", at, agent=run["agent"], **common)
        return version

    def read_result(self, attempt: str, child_task: str, at: int) -> None:
        run = self.attempts[attempt]
        task = self.tasks[run["task"]]
        self._event(
            run["node"],
            "lineage",
            "used",
            at,
            agent=run["agent"],
            task_id=run["task"],
            parent_task_id=task["parent"],
            context=task["context"],
            attempt=attempt,
            attributes={
                "lineage_version": 1,
                "entity": "result",
                "entity_task_id": child_task,
                "version_id": self.versions[("result", child_task)],
                "external": False,
            },
        )

    def end(
        self,
        attempt: str,
        at: int,
        phase: str = "interrupted",
        reason: str | None = None,
    ) -> dict:
        run = self.attempts[attempt]
        task = self.tasks[run["task"]]
        return self._event(
            run["node"],
            "run",
            phase,
            at,
            agent=run["agent"],
            task_id=run["task"],
            parent_task_id=task["parent"],
            context=task["context"],
            attempt=attempt,
            attributes={"reason": reason},
        )

    def effect(
        self,
        attempt: str,
        effect_class: str,
        phase: str,
        at: int,
        *,
        effect_id: str | None = None,
        scheduled: int | None = None,
        reason: str | None = None,
    ) -> dict:
        """agentd's effect gateway: requested, then committed or denied."""
        run = self.attempts[attempt]
        task = self.tasks[run["task"]]
        decided = phase != "requested"
        attributes = {
            "effect_version": 1,
            "effect_id": effect_id or str(uuid4()),
            "effect_class": effect_class,
            "target": "actuator/1",
            "tool_call_id": None,
            "scheduled_at_ms": None if scheduled is None else BASE_MS + scheduled,
            "policy": "managed_package_grant" if decided else None,
            "policy_version": "1" if decided else None,
            "reason": reason if phase == "denied" else None,
            "effects_remaining": None,
        }
        return self._event(
            run["node"],
            "effect",
            phase,
            at,
            agent=run["agent"],
            task_id=run["task"],
            parent_task_id=task["parent"],
            context=task["context"],
            attempt=attempt,
            attributes=attributes,
        )


def ingest(core: sqlite3.Connection, event: dict, export_seq: int = 1) -> int:
    return trace_store.ingest(
        core,
        f"edgecitadel.telemetry.v1.{event['node_id']}",
        {
            "schema_version": 1,
            "node_id": event["node_id"],
            "source_epoch": event["source_epoch"],
            "export_generation": str(uuid4()),
            "export_seq": export_seq,
            "event_sha256": hashlib.sha256(canonical_bytes(event)).hexdigest(),
            "event": event,
        },
        received_at_ms=1,
    ).ingest_seq


class Harness:
    """A Core database, a monitor database and an engine with a settable clock."""

    def __init__(self, root: Path, config: MonitorConfig | None = None, control=None):
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.core = sqlite3.connect(root / "core.db")
        self.core.execute("PRAGMA journal_mode=WAL")
        trace_store.initialize(self.core)
        self.config = config or MonitorConfig()
        self.control = control or FakeControlClient()
        self.now = BASE_MS
        self.monitor = self._open()

    def _open(self) -> ContainmentMonitor:
        self.db = sqlite3.connect(self.root / "monitor.db")
        return ContainmentMonitor(
            self.core, self.db, self.config, self.control, clock=lambda: self.now
        )

    def reopen(self) -> ContainmentMonitor:
        self.monitor.close()
        self.db.close()
        self.monitor = self._open()
        return self.monitor

    def at(self, ms: int) -> None:
        self.now = BASE_MS + ms

    def feed(self, events: list[dict]) -> None:
        """Through Core ingest and the follower."""
        for event in events:
            ingest(self.core, event)
        self.monitor.catch_up()

    def close(self) -> None:
        self.monitor.close()
        self.db.close()
        self.core.close()
