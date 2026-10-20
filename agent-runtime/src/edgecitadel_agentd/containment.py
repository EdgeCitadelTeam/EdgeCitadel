"""Owned, fenced holds on tasks and outbox messages, and parked inbound deliveries.

One ``holds`` table generalizes the former restore holds. A row is
``(kind, object_id, reason)`` with an explicit owner and policy version. An object
is held while any row names it, whatever the reason:

- ``task``: the task cannot be claimed or transitioned, the daemon's deadline and
  session-loss sweeps skip it, its invocations cannot dispatch, its unpublished
  outbound messages stay in the outbox, and inbound deliveries for it are
  acknowledged to the broker but parked in ``held_deliveries`` instead of applied.
  A task hold may be placed before the task exists on this node.
- ``transport``: that outbox message is not published.

``restore`` holds are owned by the restored source epoch and are only reviewed by
restore activation. ``containment`` holds are placed and released by one owner
(the monitor identity) whose policy version is a fencing token: per owner, agentd
keeps the highest version it has accepted and rejects any hold or release stamped
with an older version, so a delayed or replayed decision cannot undo a newer one.

A hold cannot recall a message that the outbox loop already selected for
publication; the outbox records that publication as it happened.
"""

from __future__ import annotations

import re
import sqlite3
import time
import uuid
from typing import TYPE_CHECKING

from .store import StoreError
from .trace_contract import TraceContractError

if TYPE_CHECKING:
    from .store import AgentdStore

HOLDS_SCHEMA_SQL = """
CREATE TABLE task_state.holds (
    kind TEXT NOT NULL CHECK(kind IN ('task', 'transport')),
    object_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN ('restore', 'containment')),
    owner TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK(policy_version >= 0),
    held_at_ms INTEGER NOT NULL,
    PRIMARY KEY(kind, object_id, reason)
);
CREATE TABLE task_state.containment_fences (
    owner TEXT PRIMARY KEY,
    policy_version INTEGER NOT NULL CHECK(policy_version >= 1)
);
CREATE TABLE task_state.held_deliveries (
    message_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL
);
CREATE INDEX task_state.held_deliveries_task ON held_deliveries(task_id, received_at_ms)
"""
HOLD_KINDS = frozenset({"task", "transport"})
OWNER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
MAX_POLICY_VERSION = 9_007_199_254_740_991
APPLY_BATCH = 64


class ContainmentError(StoreError):
    """A rejected containment decision; the code is stable for monitors."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validate(kind: object, object_id: object, owner: object, version: object) -> None:
    if kind not in HOLD_KINDS:
        raise ContainmentError("invalid_hold_kind")
    try:
        parsed = uuid.UUID(str(object_id))
    except ValueError as error:
        raise ContainmentError("invalid_hold_object") from error
    if parsed.version != 4 or str(parsed) != object_id:
        raise ContainmentError("invalid_hold_object")
    if not isinstance(owner, str) or OWNER_PATTERN.fullmatch(owner) is None:
        raise ContainmentError("invalid_hold_owner")
    if type(version) is not int or not 1 <= version <= MAX_POLICY_VERSION:
        raise ContainmentError("invalid_policy_version")


def _fence(db: sqlite3.Connection, owner: str, version: int) -> None:
    """Reject a decision older than the newest accepted one, then advance."""
    row = db.execute(
        "SELECT policy_version FROM containment_fences WHERE owner=?", (owner,)
    ).fetchone()
    if row is not None and version < int(row[0]):
        raise ContainmentError("stale_policy_version")
    db.execute(
        "INSERT INTO containment_fences(owner,policy_version) VALUES (?,?) "
        "ON CONFLICT(owner) DO UPDATE SET policy_version=excluded.policy_version",
        (owner, version),
    )


def _mapping(row: sqlite3.Row) -> dict[str, object]:
    return {
        key: row[key]
        for key in ("kind", "object_id", "reason", "owner", "policy_version")
    } | {"held_at_ms": row["held_at_ms"]}


def place_hold(
    store: AgentdStore,
    *,
    kind: str,
    object_id: str,
    owner: str,
    policy_version: int,
) -> dict[str, object]:
    """Hold an object for this owner; re-placing restamps its policy version."""
    _validate(kind, object_id, owner, policy_version)
    with store._task_transaction():
        db = store._connection
        _fence(db, owner, policy_version)
        existing = db.execute(
            "SELECT owner FROM holds WHERE kind=? AND object_id=? AND reason='containment'",
            (kind, object_id),
        ).fetchone()
        if existing is not None and existing["owner"] != owner:
            raise ContainmentError("hold_owned_by_another")
        db.execute(
            "INSERT INTO holds(kind,object_id,reason,owner,policy_version,held_at_ms) "
            "VALUES (?,?,'containment',?,?,?) "
            "ON CONFLICT(kind,object_id,reason) DO UPDATE SET policy_version=excluded.policy_version",
            (kind, object_id, owner, policy_version, time.time_ns() // 1_000_000),
        )
        return _mapping(
            db.execute(
                "SELECT * FROM holds WHERE kind=? AND object_id=? AND reason='containment'",
                (kind, object_id),
            ).fetchone()
        )


def release_hold(
    store: AgentdStore,
    *,
    kind: str,
    object_id: str,
    owner: str,
    policy_version: int,
) -> dict[str, object]:
    """Release this owner's hold, then apply deliveries it parked.

    The owner's fence is at least the hold's own version, so a release stamped
    before the latest hold (or any later decision) is rejected as stale.
    """
    _validate(kind, object_id, owner, policy_version)
    with store._task_transaction():
        db = store._connection
        hold = db.execute(
            "SELECT owner FROM holds WHERE kind=? AND object_id=? AND reason='containment'",
            (kind, object_id),
        ).fetchone()
        if hold is None:
            raise ContainmentError("hold_not_found")
        if hold["owner"] != owner:
            raise ContainmentError("hold_owned_by_another")
        _fence(db, owner, policy_version)
        db.execute(
            "DELETE FROM holds WHERE kind=? AND object_id=? AND reason='containment'",
            (kind, object_id),
        )
    applied = (
        apply_released_deliveries(store, task_id=object_id) if kind == "task" else 0
    )
    return {"released": True, "applied_deliveries": applied}


def list_holds(store: AgentdStore) -> list[dict[str, object]]:
    with store._lock:
        rows = store._connection.execute(
            "SELECT * FROM holds ORDER BY reason,kind,object_id"
        ).fetchall()
        parked = dict(
            store._connection.execute(
                "SELECT task_id,COUNT(*) FROM held_deliveries GROUP BY task_id"
            ).fetchall()
        )
    return [
        _mapping(row)
        | (
            {"parked_deliveries": int(parked.get(row["object_id"], 0))}
            if row["kind"] == "task"
            else {}
        )
        for row in rows
    ]


def park_delivery_locked(
    store: AgentdStore, envelope: dict[str, object], task_id: str
) -> None:
    """Caller holds the task transaction and has found the task held."""
    store._connection.execute(
        "INSERT OR IGNORE INTO held_deliveries(message_id,task_id,envelope_json,received_at_ms) "
        "VALUES (?,?,?,?)",
        (
            str(envelope["id"]),
            task_id,
            store._encode_content(envelope),
            time.time_ns() // 1_000_000,
        ),
    )


def apply_released_deliveries(store: AgentdStore, *, task_id: str | None = None) -> int:
    """Apply parked deliveries whose task is no longer held, in arrival order.

    Each delivery is ingested exactly as when it arrived, then removed. A crash
    between the two re-applies it on the next pass; ingestion of a command,
    result or cancel that already took effect is idempotent. Rejections end the
    delivery just as the broker would have terminated it; transient storage
    errors keep it for the next maintenance pass.
    """
    with store._lock:
        rows = store._connection.execute(
            "SELECT message_id,envelope_json FROM held_deliveries d "
            "WHERE (? IS NULL OR d.task_id=?) AND NOT EXISTS("
            "SELECT 1 FROM holds h WHERE h.kind='task' AND h.object_id=d.task_id) "
            "ORDER BY received_at_ms,message_id LIMIT ?",
            (task_id, task_id, APPLY_BATCH),
        ).fetchall()
    applied = 0
    for message_id, encoded in rows:
        envelope = store._decode_content(encoded)
        try:
            outcome = store.ingest_transport_envelope(envelope)
        except StoreError as error:
            cause = error.__cause__
            if isinstance(cause, TraceContractError) and cause.code in {
                "quota_exceeded",
                "storage_unavailable",
            }:
                break
            outcome = {}
        if outcome.get("held"):
            continue  # Held again meanwhile; the parked row is still the copy.
        with store._task_transaction():
            store._connection.execute(
                "DELETE FROM held_deliveries WHERE message_id=?", (message_id,)
            )
        applied += 1
    return applied
