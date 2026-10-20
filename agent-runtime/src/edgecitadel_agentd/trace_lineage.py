"""Content-addressed request/result versions and their used/generated evidence.

A version names the exact content that crossed one execution boundary: the
request a task was created with, or the result it completed with. Correlation
and routing fields stamped by the daemon are excluded, so the dispatching and
the executing node derive the same version independently. Core joins lineage
across nodes by (trace_id, version_id). Lineage is evidence, never authority.

Boundaries recorded here (all on the node that observes them):
- dispatch: the parent invocation ``generated`` the child's request;
- bind: an invocation ``used`` its task's request, ``external`` for a root task;
- completion: an invocation ``generated`` its task's result;
- first read of a completed child by its sender: the parent invocation ``used``
  that result.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

from .trace_contract import TraceContractError
from .trace_journal import TraceJournal
from .trace_lifecycle import configured_node_id

if TYPE_CHECKING:
    from .store import AgentdStore

LINEAGE_VERSION = 1
Entity = Literal["request", "result"]
# Stamped by whichever daemon holds the task. They identify where content
# travelled, not what it was, and differ between sender and recipient copies.
_CORRELATION_FIELDS = frozenset(
    {"trace_id", "parent_task_id", "execution_context", "skill_id", "deadline_at_ms"}
)
_INVOCATION_FIELDS = (
    "agent_id",
    "trace_id",
    "context_id",
    "task_id",
    "parent_task_id",
    "parent_run_id",
    "execution_attempt_id",
)


def version_id(entity: Entity, task_id: str, content: Mapping[str, object]) -> str:
    material = {
        key: value for key, value in content.items() if key not in _CORRELATION_FIELDS
    }
    encoded = json.dumps(
        {"entity": entity, "task_id": task_id, "content": material},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def reference(event: Mapping[str, Any]) -> dict[str, str]:
    return {key: str(event[key]) for key in ("node_id", "source_epoch", "event_id")}


def _stable_event_id(*parts: str) -> str:
    # Repeated reads or retried completions observe one boundary crossing once.
    digest = hashlib.sha256(json.dumps(parts).encode()).digest()
    return str(UUID(bytes=digest[:16], version=4))


def _timestamp(now_ms: int) -> str:
    return (
        datetime.fromtimestamp(now_ms / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _recorded(db: sqlite3.Connection, trace_id: str, event_id: str) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM trace_journal_all WHERE trace_id=? AND event_id=?",
            (trace_id, event_id),
        ).fetchone()
        is not None
    )


def task_event(
    db: sqlite3.Connection, trace_id: str, task_id: str, *, phase: str | None = None
) -> dict[str, Any] | None:
    """The earliest local lifecycle event of a task, optionally of one phase."""
    row = db.execute(
        "SELECT event_json FROM trace_journal_all WHERE trace_id=? AND task_id=? "
        "AND json_extract(event_json,'$.kind')='task' "
        "AND (? IS NULL OR json_extract(event_json,'$.phase')=?) "
        "ORDER BY received_at_ms,source_seq LIMIT 1",
        (trace_id, task_id, phase, phase),
    ).fetchone()
    return json.loads(row[0]) if row else None


def _invocation(db: sqlite3.Connection, binding: sqlite3.Row) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT event_json FROM trace_journal_all WHERE trace_id=? "
        "AND json_extract(event_json,'$.execution_attempt_id')=? "
        "AND json_extract(event_json,'$.kind')='run' "
        "AND json_extract(event_json,'$.phase')='started'",
        (binding["trace_id"], binding["execution_attempt_id"]),
    ).fetchone()
    return json.loads(row[0]) if row else None


def record(
    db: sqlite3.Connection,
    node_id: str,
    invocation: Mapping[str, Any],
    *,
    phase: Literal["used", "generated"],
    entity: Entity,
    entity_task_id: str,
    version: str,
    external: bool,
    causes: Sequence[Mapping[str, str]],
    occurred_at: str,
    event_id: str | None = None,
) -> dict[str, Any]:
    """Caller holds the store lock and transaction, like every journal writer.

    ``invocation`` is the binding's run/started event; lineage shares its
    execution identity so the consuming or producing activity is explicit.
    """
    return TraceJournal(db).record(
        node_id,
        {
            **{key: invocation[key] for key in _INVOCATION_FIELDS},
            "schema_version": 1,
            "event_id": event_id or str(uuid4()),
            "span_id": None,
            "parent_span_id": None,
            "kind": "lineage",
            "phase": phase,
            "occurred_at": occurred_at,
            "duration_ms": None,
            "evidence_kind": "source_observed",
            "causes": [dict(cause) for cause in causes],
            "attributes": {
                "lineage_version": LINEAGE_VERSION,
                "entity": entity,
                "entity_task_id": entity_task_id,
                "version_id": version,
                "external": external,
            },
            "supersedes_event_id": None,
        },
        selected=True,
    )


# Both recorders decide under the store lock, which serializes every writer on
# this connection, so their checks still hold when the write transaction starts.
# Reads that find nothing to record take no write transaction.


def record_result_generated(
    store: AgentdStore, *, task_id: str, session_id: str, now_ms: int
) -> None:
    """Record, after the completion commit, that the executing invocation produced its result.

    A reserved terminal completion cannot share its transaction with ordinary
    trace writes, so this is a separate transaction. If it is lost (crash or
    trace quota), the result has no recorded producer and Core reports the
    run's lineage as open; it never invents one.
    """
    node_id = configured_node_id(store)
    if node_id is None:
        return
    db = store._connection
    with store._lock:
        task = db.execute(
            "SELECT trace_id,state,result_json FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        binding = db.execute(
            "SELECT trace_id,execution_attempt_id FROM trace_bindings_all "
            "WHERE task_id=? AND session_id=?",
            (task_id, session_id),
        ).fetchone()
        if (
            task is None
            or task["state"] != "completed"
            or task["result_json"] is None
            or binding is None
        ):
            return
        invocation = _invocation(db, binding)
        if invocation is None:
            return
        version = version_id(
            "result", task_id, store._decode_content(task["result_json"])
        )
        event_id = _stable_event_id(
            binding["execution_attempt_id"], "generated", version
        )
        if _recorded(db, binding["trace_id"], event_id):
            return
        completed = task_event(db, binding["trace_id"], task_id, phase="completed")
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                record(
                    db,
                    node_id,
                    invocation,
                    phase="generated",
                    entity="result",
                    entity_task_id=task_id,
                    version=version,
                    external=False,
                    causes=[reference(completed)] if completed else [],
                    occurred_at=_timestamp(now_ms),
                    event_id=event_id,
                )
        except TraceContractError as error:
            if error.code != "quota_exceeded":
                raise


def record_result_used(
    store: AgentdStore, *, task_id: str, reader_id: str, now_ms: int
) -> None:
    """Record the parent invocation's first read of a completed child result.

    The reader is the child's sender. The consuming invocation is the open
    binding of the child's parent task, or of its task-less native root run.
    Repeated polling reads take no write transaction once it is recorded.
    """
    node_id = configured_node_id(store)
    if node_id is None:
        return
    db = store._connection
    with store._lock:
        task = db.execute(
            "SELECT sender_id,trace_id,state,result_json FROM tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        saved = db.execute(
            "SELECT context_json FROM trace_task_contexts WHERE task_id=?", (task_id,)
        ).fetchone()
        if (
            task is None
            or task["sender_id"] != reader_id
            or task["state"] != "completed"
            or task["result_json"] is None
            or saved is None
        ):
            return
        context = json.loads(saved[0])
        if context["parent_task_id"] is not None:
            binding = db.execute(
                "SELECT trace_id,execution_attempt_id FROM trace_bindings_all "
                "WHERE task_id=? AND agent_id=? AND closed_at_ms IS NULL "
                "ORDER BY opened_at_ms DESC LIMIT 1",
                (context["parent_task_id"], reader_id),
            ).fetchone()
        elif context["parent_run_id"] is not None:
            binding = db.execute(
                "SELECT trace_id,execution_attempt_id FROM trace_bindings_all "
                "WHERE trace_id=? AND task_id IS NULL AND agent_id=? AND closed_at_ms IS NULL "
                "ORDER BY opened_at_ms DESC LIMIT 1",
                (context["parent_run_id"], reader_id),
            ).fetchone()
        else:
            return
        if binding is None:
            return
        invocation = _invocation(db, binding)
        if invocation is None:
            return
        version = version_id(
            "result", task_id, store._decode_content(task["result_json"])
        )
        event_id = _stable_event_id(binding["execution_attempt_id"], "used", version)
        if _recorded(db, binding["trace_id"], event_id):
            return
        # The local event that made this result available: the recipient's
        # completion here, or the remote result's ingestion.
        arrival = task_event(db, task["trace_id"], task_id, phase="completed")
        with db:
            db.execute("BEGIN IMMEDIATE")
            record(
                db,
                node_id,
                invocation,
                phase="used",
                entity="result",
                entity_task_id=task_id,
                version=version,
                external=False,
                causes=[reference(arrival)] if arrival else [],
                occurred_at=_timestamp(now_ms),
                event_id=event_id,
            )
