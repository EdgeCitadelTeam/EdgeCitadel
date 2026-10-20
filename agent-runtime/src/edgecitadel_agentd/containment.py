"""Owned, fenced containment state and the gates that consult it.

Holds
-----
One ``holds`` table generalizes the former restore holds. A row is
``(kind, object_id, reason)`` with an explicit owner and policy version. An object
is held while any row names it, whatever the reason:

- ``task`` (execution hold, a pause): the task cannot be claimed or
  transitioned, the daemon's deadline and session-loss sweeps skip it, its
  invocations cannot dispatch or authorize effects, its outbound messages stay
  in the outbox, and inbound deliveries for it are acknowledged to the broker but
  parked in ``held_deliveries`` instead of applied.
- ``egress`` (egress block): the task keeps running and receiving, but nothing
  leaves it: no dispatch, no effect, no result (its recipient's terminal
  transitions wait), and none of its outbound messages is published. These
  include requests and cancels it already queued for its children. The object
  may also be the binding ID of a task-less native root.
- ``transport``: that outbox message is not published.

A task or egress hold may be placed before the task exists on this node.

Authority
---------
A delegated task acts under its own authority and its ancestors'
(``TaskTraceContext.authorities``). Revocations are irreversible and deny later
dispatches and effects on this node:

- ``authority`` (a task ID): of that task and every descendant;
- ``egress`` (a task ID, or the binding ID of a task-less native root): of that
  invocation only; it may still complete;
- ``credential`` (an agent ID): of every invocation of that agent, its
  ``task.create``, and the publication of every message it emits. An **ingress filter** ``(authority, target)`` denies only
traffic under that authority into one target: delegations into a recipient
agent (refused at dispatch, withheld in the outbox, parked at ingest) or
effects matching an effect rule. Other traffic into the target continues. The
authority is a delegation (a task ID, reaching its descendants through their
chains), a credential (an agent ID: everything that agent sends or does), or
any authority (admission control: no new task for the target agent). Filters
match authority keys ``task:<id>``, ``agent:<id>`` and ``any:*``.

``restore`` holds are owned by the restored source epoch and are only reviewed by
restore activation. Every containment decision is placed by one owner (the
monitor identity) whose policy version is a fencing token: per owner, agentd
keeps the highest version it has accepted and rejects any decision stamped with
an older version, so a delayed or replayed decision cannot undo a newer one.

A hold cannot recall a message that the outbox loop already selected for
publication; the outbox records that publication as it happened.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from .delegation_authority import EffectRule
from .store import StoreError
from .trace_contract import TraceContractError

if TYPE_CHECKING:
    from .store import AgentdStore

HOLDS_SCHEMA_SQL = """
CREATE TABLE task_state.holds (
    kind TEXT NOT NULL CHECK(kind IN ('task', 'transport', 'egress')),
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
AUTHORITY_SCHEMA_SQL = """
CREATE TABLE task_state.revocations (
    kind TEXT NOT NULL CHECK(kind IN ('authority', 'egress', 'credential')),
    object_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK(policy_version >= 1),
    revoked_at_ms INTEGER NOT NULL,
    PRIMARY KEY(kind, object_id)
);
CREATE TABLE task_state.ingress_filters (
    authority TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK(target_kind IN ('agent', 'effect')),
    effect_class TEXT NOT NULL,
    target TEXT NOT NULL,
    owner TEXT NOT NULL,
    policy_version INTEGER NOT NULL CHECK(policy_version >= 1),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(authority, target_kind, effect_class, target)
);
CREATE TABLE task_state.authority_ledger (
    task_id TEXT PRIMARY KEY,
    effects_used INTEGER NOT NULL DEFAULT 0 CHECK(effects_used >= 0),
    effects_reserved INTEGER NOT NULL DEFAULT 0 CHECK(effects_reserved >= 0)
);
CREATE TABLE task_state.control_commands (
    command_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    confirmation_json TEXT NOT NULL,
    expires_at_ms INTEGER NOT NULL
);
CREATE INDEX task_state.control_commands_expiry ON control_commands(expires_at_ms)
"""
HOLD_KINDS = frozenset({"task", "transport", "egress"})
OWNER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")
AGENT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
MAX_POLICY_VERSION = 9_007_199_254_740_991
APPLY_BATCH = 64

# Outbox messages (alias ``o``) that must not be published now. A message to a
# task's recipient is the task's request or cancel, which the sender emitted:
# it is the egress of the parent invocation and subject to its ingress filters.
PUBLICATION_BLOCKED_SQL = """(
    o.message_id IN (SELECT object_id FROM holds WHERE kind='transport')
    OR o.task_id IN (SELECT object_id FROM holds WHERE kind IN ('task','egress'))
    OR EXISTS (
        SELECT 1 FROM tasks t WHERE t.task_id=o.task_id AND EXISTS (
            SELECT 1 FROM revocations r WHERE r.kind='credential'
              AND r.object_id=CASE WHEN o.subject='agents.'||t.recipient_id||'.inbox'
                                   THEN t.sender_id ELSE t.recipient_id END))
    OR EXISTS (
        SELECT 1 FROM tasks t LEFT JOIN trace_task_contexts c ON c.task_id=t.task_id
        WHERE t.task_id=o.task_id AND o.subject='agents.'||t.recipient_id||'.inbox'
          AND (
            EXISTS (SELECT 1 FROM holds h WHERE h.kind IN ('task','egress')
                    AND h.object_id=json_extract(c.context_json,'$.parent_task_id'))
            OR EXISTS (SELECT 1 FROM holds h JOIN trace_bindings_all b
                       ON b.binding_id=h.object_id
                       WHERE h.kind='egress' AND b.task_id IS NULL
                         AND b.trace_id=json_extract(c.context_json,'$.parent_run_id'))
            OR EXISTS (SELECT 1 FROM ingress_filters f
                       WHERE f.target_kind='agent' AND f.target=t.recipient_id
                         AND (f.authority IN ('any:*', 'agent:'||t.sender_id)
                              OR f.authority IN (SELECT 'task:'||a.value FROM
                                 json_each(c.context_json,'$.authority_chain') a)))
          )
    )
)"""


class ContainmentError(StoreError):
    """A rejected containment decision; the code is stable for monitors."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _uuid4(value: object, code: str) -> str:
    try:
        parsed = uuid.UUID(str(value))
    except ValueError as error:
        raise ContainmentError(code) from error
    if parsed.version != 4 or str(parsed) != value:
        raise ContainmentError(code)
    return str(value)


def _validate_decision(owner: object, version: object) -> None:
    if not isinstance(owner, str) or OWNER_PATTERN.fullmatch(owner) is None:
        raise ContainmentError("invalid_hold_owner")
    if type(version) is not int or not 1 <= version <= MAX_POLICY_VERSION:
        raise ContainmentError("invalid_policy_version")


def _validate(kind: object, object_id: object, owner: object, version: object) -> None:
    if kind not in HOLD_KINDS:
        raise ContainmentError("invalid_hold_kind")
    _uuid4(object_id, "invalid_hold_object")
    _validate_decision(owner, version)


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


def check_fence(db: sqlite3.Connection, owner: str, version: int) -> None:
    """Fence a decision that changes no containment state (a lease renewal)."""
    _validate_decision(owner, version)
    _fence(db, owner, version)


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
            (kind, object_id, owner, policy_version, _now_ms()),
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
        if kind == "task":
            _recover_if_orphaned_locked(store, object_id)
    applied = (
        apply_released_deliveries(store, task_id=object_id) if kind == "task" else 0
    )
    return {"released": True, "applied_deliveries": applied}


def _recover_if_orphaned_locked(store: AgentdStore, task_id: str) -> None:
    """Recover a released task whose executing session closed while it was held.

    Session recovery skips held tasks, so a paused task whose runtime exited
    would otherwise stay claimed forever. Once nothing holds it, the ordinary
    session-loss rule applies: accepted work is requeued, and running work
    fails as not retry-safe because its handler may have had effects.
    """
    row = store._connection.execute(
        "SELECT t.claimed_session_id FROM tasks t JOIN sessions s "
        "ON s.session_id=t.claimed_session_id WHERE t.task_id=? "
        "AND t.state IN ('accepted','running') AND s.closed_at_ms IS NOT NULL",
        (task_id,),
    ).fetchone()
    if row is not None and store._task_hold_locked(task_id) is None:
        store._recover_session_tasks_locked(str(row[0]), _now_ms())


REVOCATION_KINDS = frozenset({"authority", "egress", "credential"})


def revoke(
    store: AgentdStore, *, kind: str, object_id: str, owner: str, policy_version: int
) -> dict[str, object]:
    """Revoke authority on this node; irreversible.

    ``authority``: a task and its descendants; the task need not exist here,
    because descendants that arrive later carry it in their chain. ``egress``:
    one invocation, by task ID or by a task-less native root's binding ID.
    ``credential``: an agent.
    """
    if kind not in REVOCATION_KINDS:
        raise ContainmentError("invalid_revocation_kind")
    if kind == "credential":
        if not isinstance(object_id, str) or AGENT_PATTERN.fullmatch(object_id) is None:
            raise ContainmentError("invalid_authority")
    else:
        _uuid4(object_id, "invalid_authority")
    _validate_decision(owner, policy_version)
    with store._task_transaction():
        db = store._connection
        _fence(db, owner, policy_version)
        db.execute(
            "INSERT OR IGNORE INTO revocations(kind,object_id,owner,policy_version,"
            "revoked_at_ms) VALUES (?,?,?,?,?)",
            (kind, object_id, owner, policy_version, _now_ms()),
        )
        row = db.execute(
            "SELECT revoked_at_ms FROM revocations WHERE kind=? AND object_id=?",
            (kind, object_id),
        ).fetchone()
        return {
            "revoked_at_ms": int(row[0]),
            "local_tasks": _tasks_affected_locked(db, kind, object_id),
        }


def revoke_authority(
    store: AgentdStore, *, task_id: str, owner: str, policy_version: int
) -> dict[str, object]:
    """Revoke a delegation's authority here, for the task and its descendants."""
    return revoke(
        store,
        kind="authority",
        object_id=task_id,
        owner=owner,
        policy_version=policy_version,
    )


def _tasks_affected_locked(db: sqlite3.Connection, kind: str, object_id: str) -> int:
    if kind == "authority":
        return _tasks_under_locked(db, object_id)
    terminal = "('completed','failed','rejected','cancelled','expired','undeliverable')"
    if kind == "credential":
        query = (
            f"SELECT COUNT(*) FROM tasks WHERE state NOT IN {terminal} "  # noqa: S608
            "AND (sender_id=? OR recipient_id=?)"
        )
        return int(db.execute(query, (object_id, object_id)).fetchone()[0])
    query = (
        f"SELECT COUNT(*) FROM tasks WHERE state NOT IN {terminal} AND "  # noqa: S608
        "(task_id=? OR task_id IN (SELECT task_id FROM trace_bindings_all "
        "WHERE binding_id=? AND task_id IS NOT NULL))"
    )
    count = int(db.execute(query, (object_id, object_id)).fetchone()[0])
    if (
        count == 0
        and db.execute(
            "SELECT 1 FROM trace_bindings_all WHERE binding_id=? AND closed_at_ms IS NULL",
            (object_id,),
        ).fetchone()
    ):
        return 1  # A live task-less native root.
    return count


def _tasks_under_locked(db: sqlite3.Connection, authority_id: str) -> int:
    """Local non-terminal tasks acting under this authority."""
    return int(
        db.execute(
            "SELECT COUNT(*) FROM tasks t LEFT JOIN trace_task_contexts c "
            "ON c.task_id=t.task_id WHERE t.state NOT IN "
            "('completed','failed','rejected','cancelled','expired','undeliverable') "
            "AND (t.task_id=? OR EXISTS (SELECT 1 FROM "
            "json_each(c.context_json,'$.authority_chain') a WHERE a.value=?))",
            (authority_id, authority_id),
        ).fetchone()[0]
    )


def authority_keys(task_ids: Iterable[str], agent_id: str | None) -> tuple[str, ...]:
    """The filter keys traffic acts under: its delegations, its credential, any."""
    return (
        *(f"task:{task_id}" for task_id in task_ids),
        *((f"agent:{agent_id}",) if agent_id else ()),
        "any:*",
    )


def filter_authority(authority: object) -> str:
    """``{"task_id"}`` (a delegation), ``{"agent_id"}`` (a credential) or
    ``{"any": true}`` (admission control) as a filter key."""
    if isinstance(authority, Mapping) and dict(authority) == {"any": True}:
        return "any:*"
    if isinstance(authority, Mapping) and set(authority) == {"task_id"}:
        return f"task:{_uuid4(authority['task_id'], 'invalid_authority')}"
    if isinstance(authority, Mapping) and set(authority) == {"agent_id"}:
        agent = authority["agent_id"]
        if isinstance(agent, str) and AGENT_PATTERN.fullmatch(agent):
            return f"agent:{agent}"
    raise ContainmentError("invalid_authority")


def filter_target(target: object) -> tuple[str, str, str]:
    """``{"agent_id"}`` or ``{"effect_class","target"}`` as a stored key."""
    if isinstance(target, Mapping) and set(target) == {"agent_id"}:
        agent = target["agent_id"]
        if not isinstance(agent, str) or AGENT_PATTERN.fullmatch(agent) is None:
            raise ContainmentError("invalid_filter_target")
        return "agent", "", agent
    if isinstance(target, Mapping) and set(target) == {"effect_class", "target"}:
        try:
            rule = EffectRule(target["effect_class"], target["target"])
        except TraceContractError as error:
            raise ContainmentError("invalid_filter_target") from error
        return "effect", rule.effect_class, rule.target
    raise ContainmentError("invalid_filter_target")


def place_filter(
    store: AgentdStore,
    *,
    authority: object,
    target: object,
    owner: str,
    policy_version: int,
) -> dict[str, object]:
    key = (filter_authority(authority), *filter_target(target))
    _validate_decision(owner, policy_version)
    with store._task_transaction():
        db = store._connection
        _fence(db, owner, policy_version)
        existing = db.execute(
            "SELECT owner FROM ingress_filters WHERE authority=? AND target_kind=? "
            "AND effect_class=? AND target=?",
            key,
        ).fetchone()
        if existing is not None and existing["owner"] != owner:
            raise ContainmentError("filter_owned_by_another")
        db.execute(
            "INSERT INTO ingress_filters(authority,target_kind,effect_class,target,"
            "owner,policy_version,created_at_ms) VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(authority,target_kind,effect_class,target) "
            "DO UPDATE SET policy_version=excluded.policy_version",
            (*key, owner, policy_version, _now_ms()),
        )
    return {"authority": authority, "target": target}


def remove_filter(
    store: AgentdStore,
    *,
    authority: object,
    target: object,
    owner: str,
    policy_version: int,
) -> dict[str, object]:
    key = (filter_authority(authority), *filter_target(target))
    _validate_decision(owner, policy_version)
    with store._task_transaction():
        db = store._connection
        existing = db.execute(
            "SELECT owner FROM ingress_filters WHERE authority=? AND target_kind=? "
            "AND effect_class=? AND target=?",
            key,
        ).fetchone()
        if existing is None:
            raise ContainmentError("filter_not_found")
        if existing["owner"] != owner:
            raise ContainmentError("filter_owned_by_another")
        _fence(db, owner, policy_version)
        db.execute(
            "DELETE FROM ingress_filters WHERE authority=? AND target_kind=? "
            "AND effect_class=? AND target=?",
            key,
        )
    return {"removed": True, "applied_deliveries": apply_released_deliveries(store)}


def cancel_task(
    store: AgentdStore, *, task_id: str, owner: str, policy_version: int
) -> dict[str, object]:
    """End a task as ``cancelled``, removing this owner's holds on it first.

    A cancel is terminal and wins over a pause. Another owner's hold, or a
    restore hold, keeps the task as it is. Cancelling a cancelled task is
    idempotent; cancelling another terminal state is refused.
    """
    _uuid4(task_id, "invalid_hold_object")
    _validate_decision(owner, policy_version)
    with store._task_transaction():
        db = store._connection
        _fence(db, owner, policy_version)
        row = db.execute(
            "SELECT state FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if row is None:
            raise ContainmentError("task_not_found")
        if row["state"] == "cancelled":
            return {"state": "cancelled", "released_holds": []}
        holds = db.execute(
            "SELECT kind,reason,owner FROM holds WHERE kind IN ('task','egress') "
            "AND object_id=?",
            (task_id,),
        ).fetchall()
        if any(hold["reason"] == "restore" for hold in holds):
            raise ContainmentError("restore_hold")
        if any(hold["owner"] != owner for hold in holds):
            raise ContainmentError("hold_owned_by_another")
        db.execute(
            "DELETE FROM holds WHERE kind IN ('task','egress') AND object_id=? "
            "AND reason='containment'",
            (task_id,),
        )
        try:
            store.transition_task(
                task_id=task_id,
                state="cancelled",
                actor_id="edgecitadel-system",
                reason="containment_cancelled",
                evidence={"containment_owner": owner},
            )
        except StoreError as error:
            if "illegal task transition" in str(error):
                raise ContainmentError("task_terminal") from error
            raise
    apply_released_deliveries(store, task_id=task_id)
    return {"state": "cancelled", "released_holds": sorted(h["kind"] for h in holds)}


def _authority_mapping(key: str) -> dict[str, object]:
    kind, _, value = key.partition(":")
    return {"any": True} if kind == "any" else {f"{kind}_id": value}


def list_containment(store: AgentdStore) -> dict[str, list[dict[str, object]]]:
    with store._lock:
        db = store._connection
        rows = db.execute(
            "SELECT * FROM holds ORDER BY reason,kind,object_id"
        ).fetchall()
        parked = dict(
            db.execute(
                "SELECT task_id,COUNT(*) FROM held_deliveries GROUP BY task_id"
            ).fetchall()
        )
        revocations = [
            dict(row)
            for row in db.execute("SELECT * FROM revocations ORDER BY kind,object_id")
        ]
        filters = [
            {
                "authority": _authority_mapping(row["authority"]),
                "target": {"agent_id": row["target"]}
                if row["target_kind"] == "agent"
                else {"effect_class": row["effect_class"], "target": row["target"]},
                "owner": row["owner"],
                "policy_version": row["policy_version"],
            }
            for row in db.execute(
                "SELECT * FROM ingress_filters ORDER BY authority,target_kind,target"
            )
        ]
    return {
        "holds": [
            _mapping(row)
            | (
                {"parked_deliveries": int(parked.get(row["object_id"], 0))}
                if row["kind"] == "task"
                else {}
            )
            for row in rows
        ],
        "revocations": revocations,
        "ingress_filters": filters,
    }


# Gates. Callers hold the store lock, normally inside the deciding transaction.


def egress_hold_locked(
    db: sqlite3.Connection, task_id: str | None, binding_id: str | None = None
) -> sqlite3.Row | None:
    """An egress hold on the invocation's task or on its task-less binding."""
    return db.execute(
        "SELECT reason,owner,policy_version FROM holds WHERE kind='egress' "
        "AND object_id IN (?,?) LIMIT 1",
        (task_id, binding_id),
    ).fetchone()


def revocation_locked(
    db: sqlite3.Connection,
    *,
    authorities: Iterable[str],
    invocation: Iterable[str | None],
    agent_id: str | None,
) -> sqlite3.Row | None:
    """The revocation denying this invocation, if any.

    ``authorities`` are its task and ancestors (``authority`` revocations),
    ``invocation`` its own task and binding IDs (``egress``), and ``agent_id``
    its credential.
    """
    authorities = tuple(authorities)
    invocation = tuple(value for value in invocation if value)
    clauses, values = [], []
    if authorities:
        clauses.append(
            f"(kind='authority' AND object_id IN ({','.join('?' * len(authorities))}))"
        )
        values.extend(authorities)
    if invocation:
        clauses.append(
            f"(kind='egress' AND object_id IN ({','.join('?' * len(invocation))}))"
        )
        values.extend(invocation)
    if agent_id:
        clauses.append("(kind='credential' AND object_id=?)")
        values.append(agent_id)
    if not clauses:
        return None
    return db.execute(
        "SELECT kind,object_id,owner,policy_version FROM revocations WHERE "  # noqa: S608
        + " OR ".join(clauses)
        + " ORDER BY revoked_at_ms LIMIT 1",
        values,
    ).fetchone()


def ingress_filter_locked(
    db: sqlite3.Connection,
    authorities: Iterable[str],
    *,
    agent_id: str | None = None,
    effect: EffectRule | None = None,
) -> sqlite3.Row | None:
    """The filter denying traffic under these authority keys into the target."""
    authorities = tuple(authorities)
    if not authorities:
        return None
    marks = ",".join("?" for _ in authorities)
    rows = db.execute(
        "SELECT authority,target_kind,effect_class,target,owner,policy_version "
        f"FROM ingress_filters WHERE authority IN ({marks}) "  # noqa: S608
        "ORDER BY created_at_ms",
        authorities,
    ).fetchall()
    for row in rows:
        if row["target_kind"] == "agent" and row["target"] == agent_id:
            return row
        if (
            row["target_kind"] == "effect"
            and effect is not None
            and EffectRule(row["effect_class"], row["target"]).covers(effect)
        ):
            return row
    return None


def record_alert_locked(
    db: sqlite3.Connection,
    node_id: str,
    invocation: Mapping[str, object],
    *,
    alert: str,
    request_id: str,
    lease_dimension: str | None,
    causes: list[dict[str, str]],
    occurred_at: str,
) -> None:
    """Record a containment alert in the invocation's run, for monitors."""
    from .trace_journal import TraceJournal

    TraceJournal(db).record(
        node_id,
        {
            **invocation,
            "event_id": str(uuid.uuid4()),
            "span_id": None,
            "parent_span_id": None,
            "kind": "containment",
            "phase": "alert",
            "occurred_at": occurred_at,
            "duration_ms": None,
            "causes": causes,
            "attributes": {
                "containment_version": 1,
                "alert": alert,
                "request_id": request_id,
                "lease_dimension": lease_dimension,
            },
            "supersedes_event_id": None,
        },
        selected=True,
    )


def park_delivery_locked(
    store: AgentdStore,
    envelope: dict[str, object],
    task_id: str,
    *,
    authorities: Iterable[str] = (),
) -> None:
    """Caller holds the task transaction and found the delivery blocked.

    ``authorities`` are the delivery's filter keys, kept in clear so a filter's
    removal can find what it parked.
    """
    store._connection.execute(
        "INSERT OR IGNORE INTO held_deliveries(message_id,task_id,envelope_json,"
        "received_at_ms,recipient_id,authorities_json) VALUES (?,?,?,?,?,?)",
        (
            str(envelope["id"]),
            task_id,
            store._encode_content(envelope),
            _now_ms(),
            str(envelope.get("recipient_id", "")),
            json.dumps(list(authorities)),
        ),
    )


def apply_released_deliveries(store: AgentdStore, *, task_id: str | None = None) -> int:
    """Apply parked deliveries no longer blocked, in arrival order.

    A delivery is blocked while its task is held or an ingress filter matches
    its authority chain and recipient. Each delivery is ingested exactly as
    when it arrived, then removed. A crash between the two re-applies it on
    the next pass; ingestion of a command, result or cancel that already took
    effect is idempotent. Rejections end the delivery just as the broker would
    have terminated it; transient storage errors keep it for the next pass.
    """
    with store._lock:
        rows = store._connection.execute(
            "SELECT message_id,envelope_json FROM held_deliveries d "
            "WHERE (? IS NULL OR d.task_id=?) AND NOT EXISTS("
            "SELECT 1 FROM holds h WHERE h.kind='task' AND h.object_id=d.task_id) "
            "AND NOT EXISTS(SELECT 1 FROM json_each(d.authorities_json) a "
            "JOIN ingress_filters f ON f.authority=a.value "
            "WHERE f.target_kind='agent' AND f.target=d.recipient_id) "
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
            continue  # Blocked again meanwhile; the parked row is still the copy.
        with store._task_transaction():
            store._connection.execute(
                "DELETE FROM held_deliveries WHERE message_id=?", (message_id,)
            )
        applied += 1
    return applied
