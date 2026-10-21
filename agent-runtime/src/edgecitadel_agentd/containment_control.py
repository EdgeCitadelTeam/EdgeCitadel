"""Containment control commands and their enforcement confirmations.

A controller (an owner such as ``core-monitor``) sends a versioned command to a
node, either on the NATS request subject ``edgecitadel.control.v1.<node_id>`` or
through the local admin RPC ``containment.control``. The node applies it with the
same fenced primitives as every other containment decision and replies with a
confirmation of what it actually enforced (``schemas/containment-control.v1.json``).

Authentication on NATS: the fleet broker has no per-subject permissions, so a
command carries ``mac``, an HMAC-SHA256 over its canonical JSON under the owner's
key, read from ``<agentd state>/control-keys/<owner>.key``. The owner named in the
command is therefore authenticated, which makes its fencing token meaningful.
The admin RPC is authenticated by the local management credential instead.

Replay: a command is accepted only before its ``expires_at_ms`` (at most five
minutes after ``issued_at_ms``, judged by this node's clock). Its confirmation is
stored under ``command_id`` until then, so a retried or replayed command gets the
original confirmation and is never applied twice.

A confirmation's ``outbound`` counts the object's unpublished outbound messages
at enforcement time: ``held`` will not be published while the decision stands,
``in_flight`` lists messages the outbox had already handed to the broker (they
cannot be recalled and will be recorded as published), and ``published`` were
sent before. ``effects_in_flight`` lists effects the gateway had committed and
was still performing. ``applied_at_ms`` is when the gates took effect; a paused
handler is interrupted by its runtime within its watch interval after that.

``journal`` is this node's trace journal position once the decision took
effect: its source epoch and the sequence of the latest event journaled by then
(0 before the first). Every event that passed one of the decision's gates before
it took effect (a dispatch, a committed effect, a publication) is journaled at
or below it, so a controller that has ingested this node's events up to the
position has seen everything the decision did not stop.

Commands serialize on ``store.control_lock`` with the outbox's last check before
publishing a message and with the effect gateway's commit step, so a command
applied before either wins, and anything that passed before it is reported.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

from jsonschema import Draft202012Validator

from . import containment
from .containment import ContainmentError
from .trace_contract import TraceContractError, canonical_bytes

if TYPE_CHECKING:
    from .store import AgentdStore

SUBJECT_PREFIX = "edgecitadel.control.v1"
# Whether the outbox message ``o`` is currently barred from publication.
_BLOCKED = f"{containment.PUBLICATION_BLOCKED_SQL} AS blocked"
MAC_CONTEXT = b"edgecitadel.containment.control.v1\x00"
MAX_COMMAND_LIFETIME_MS = 5 * 60 * 1000
MAX_COMMAND_BYTES = 4096
KEY_DIRECTORY = "control-keys"
TERMINAL_STATES = frozenset(
    {"completed", "failed", "rejected", "cancelled", "expired", "undeliverable"}
)
_SCHEMA = json.loads(
    (
        Path(__file__).resolve().parents[3] / "schemas/containment-control.v1.json"
    ).read_text()
)
_COMMAND = Draft202012Validator(
    {**_SCHEMA["$defs"]["command"], "$defs": _SCHEMA["$defs"]}
)
_CONFIRMATION = Draft202012Validator(
    {**_SCHEMA["$defs"]["confirmation"], "$defs": _SCHEMA["$defs"]}
)


class PublicationGate(Protocol):
    """The outbox's publication boundary, owned by the NATS transport."""

    def in_flight(self) -> frozenset[str]: ...


def subject_for(node_id: str) -> str:
    return f"{SUBJECT_PREFIX}.{node_id}"


def valid_confirmation(value: object) -> bool:
    """Whether a reply is a schema-valid confirmation; controllers check replies."""
    return _CONFIRMATION.is_valid(value)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def command_mac(key: bytes, command: Mapping[str, Any]) -> str:
    body = {name: value for name, value in command.items() if name != "mac"}
    return hmac.digest(key, MAC_CONTEXT + canonical_bytes(body), "sha256").hex()


def _owner_key(store: AgentdStore, owner: str) -> bytes | None:
    path = store.state_directory / KEY_DIRECTORY / f"{owner}.key"
    try:
        if path.is_symlink() or not path.is_file():
            return None
        key = path.read_bytes().strip()
    except OSError:
        return None
    return key if len(key) >= 32 else None


def _rejected(
    node_id: str, command: Mapping[str, Any], code: str, received_at_ms: int
) -> dict[str, Any]:
    command_id = command.get("command_id")
    action = command.get("action")
    return {
        "schema_version": 1,
        "command_id": command_id if isinstance(command_id, str) else None,
        "node_id": node_id,
        "action": action if isinstance(action, str) else None,
        "status": "rejected",
        "code": code,
        "received_at_ms": received_at_ms,
        "applied_at_ms": None,
        "enforcement": None,
        "journal": None,
    }


def _journal_position(store: AgentdStore, node_id: str) -> dict[str, Any] | None:
    """This node's active source epoch and its latest journaled sequence."""
    with store._lock:
        row = store._connection.execute(
            "SELECT source_epoch,next_source_seq FROM trace_sources "
            "WHERE node_id=? AND active=1",
            (node_id,),
        ).fetchone()
    if row is None:
        return None  # nothing journaled under this identity yet
    return {"source_epoch": str(row[0]), "source_seq": int(row[1]) - 1}


def handle_wire(
    store: AgentdStore,
    data: bytes,
    *,
    node_id: str,
    gate: PublicationGate | None,
) -> dict[str, Any]:
    """Authenticate one NATS command and execute it; always return a reply."""
    from .trace_security import note_authentication_rejection

    received = _now_ms()
    try:
        if len(data) > MAX_COMMAND_BYTES:
            raise ValueError
        command = json.loads(data)
        if not isinstance(command, dict) or not _COMMAND.is_valid(command):
            raise ValueError
        # Also rejects JSON the canonical form cannot carry, such as 1.0.
        canonical_bytes(command)
    except (ValueError, UnicodeDecodeError):
        return _rejected(node_id, {}, "invalid_command", received)
    key = _owner_key(store, command["owner"])
    if (
        key is None
        or "mac" not in command
        or not hmac.compare_digest(command["mac"], command_mac(key, command))
    ):
        note_authentication_rejection(store)
        return _rejected(node_id, command, "not_authenticated", received)
    body = {name: value for name, value in command.items() if name != "mac"}
    return execute(store, body, node_id=node_id, gate=gate, received_at_ms=received)


def local_command(params: Mapping[str, Any], *, node_id: str) -> dict[str, Any]:
    """An admin RPC command: same document, issued now by the local operator."""
    now = _now_ms()
    return {
        "schema_version": 1,
        "command_id": params.get("command_id") or str(uuid4()),
        "node_id": node_id,
        "owner": params.get("owner"),
        "policy_version": params.get("policy_version"),
        "issued_at_ms": now,
        "expires_at_ms": now + MAX_COMMAND_LIFETIME_MS,
        "action": params.get("action"),
        "target": params.get("target"),
    }


def execute(
    store: AgentdStore,
    command: dict[str, Any],
    *,
    node_id: str,
    gate: PublicationGate | None,
    received_at_ms: int | None = None,
) -> dict[str, Any]:
    """Apply an authenticated command once and confirm what was enforced."""
    received = received_at_ms if received_at_ms is not None else _now_ms()
    try:
        digest = hashlib.sha256(canonical_bytes(command)).hexdigest()
    except TraceContractError:
        return _rejected(node_id, {}, "invalid_command", received)
    if not _COMMAND.is_valid(command) or "mac" in command:
        return _rejected(node_id, command, "invalid_command", received)
    if command["node_id"] != node_id:
        return _rejected(node_id, command, "wrong_node", received)
    lifetime = command["expires_at_ms"] - command["issued_at_ms"]
    if not 0 < lifetime <= MAX_COMMAND_LIFETIME_MS:
        return _rejected(node_id, command, "invalid_command", received)
    # Commands serialize on the control lock. The outbox registers a message as
    # in flight, and the effect gateway an effect as committed, under the same
    # lock after rechecking them, so the snapshot is exact for both boundaries.
    with store.control_lock:
        with store._lock:
            stored = store._connection.execute(
                "SELECT request_sha256,confirmation_json FROM control_commands "
                "WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()
        if stored is not None:
            if stored[0] != digest:
                return _rejected(node_id, command, "idempotency_conflict", received)
            return json.loads(stored[1])
        if received >= command["expires_at_ms"]:
            return _rejected(node_id, command, "command_expired", received)
        try:
            enforcement = _APPLY[command["action"]](store, command)
            applied = _now_ms()
            enforcement = _snapshot(
                store, command, enforcement, gate.in_flight() if gate else frozenset()
            )
            effects = store.effects.in_flight()
            if effects and command["action"] not in {"renew", "describe"}:
                enforcement["effects_in_flight"] = sorted(effects)
            confirmation = {
                "schema_version": 1,
                "command_id": command["command_id"],
                "node_id": node_id,
                "action": command["action"],
                "status": "applied",
                "code": None,
                "received_at_ms": received,
                "applied_at_ms": applied,
                "enforcement": enforcement,
                # Read after the decision committed, still under the control
                # lock: at least the position of everything that preceded it.
                "journal": _journal_position(store, node_id),
            }
        except ContainmentError as error:
            confirmation = _rejected(node_id, command, error.code, received)
        if not _CONFIRMATION.is_valid(confirmation):
            raise TraceContractError("invalid_control_confirmation")
        with store._task_transaction():
            store._connection.execute(
                "INSERT INTO control_commands(command_id,owner,request_sha256,"
                "confirmation_json,expires_at_ms) VALUES (?,?,?,?,?)",
                (
                    command["command_id"],
                    command["owner"],
                    digest,
                    json.dumps(confirmation, sort_keys=True, separators=(",", ":")),
                    command["expires_at_ms"],
                ),
            )
    return confirmation


def _decision(command: Mapping[str, Any]) -> dict[str, Any]:
    return {"owner": command["owner"], "policy_version": command["policy_version"]}


# The op:kind pairs this node enforces, in the monitor's vocabulary.
CAPABILITIES = (
    "hold:task",
    "hold:egress",
    "hold:transport",
    "hold:effect",
    # Held events of labels (released only; agentd places them).
    "hold:event",
    "revoke:authority",
    "revoke:egress",
    "revoke:credential",
    "revoke:grant",
    "revoke:admission",
    "filter:agent",
    "filter:effect",
    "cancel:task",
    "renew:controller",
)


def _hold(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    target = command["target"]
    containment.place_hold(
        store, kind=target["kind"], object_id=target["object_id"], **_decision(command)
    )
    return {}


def _release(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    target = command["target"]
    released = containment.release_hold(
        store, kind=target["kind"], object_id=target["object_id"], **_decision(command)
    )
    return {"applied_deliveries": released["applied_deliveries"]}


def _revocation_filter(
    kind: str, object_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """``grant`` and ``admission`` revocations are filters with no lifted state.

    ``grant``: ``<agent>><target>``, the target an agent ID or ``fx:<class>``
    (every effect of that class). ``admission``: an agent ID, which admits no
    new task under any authority.
    """
    if kind == "admission":
        return {"any": True}, {"agent_id": object_id}
    agent, separator, target = object_id.partition(">")
    if not separator:
        raise ContainmentError("invalid_authority")
    if target.startswith("fx:"):
        return {"agent_id": agent}, {"effect_class": target[3:], "target": "*"}
    return {"agent_id": agent}, {"agent_id": target}


def _revoke(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    kind, object_id = command["target"]["kind"], command["target"]["object_id"]
    if kind in {"grant", "admission"}:
        authority, into = _revocation_filter(kind, object_id)
        containment.place_filter(
            store, authority=authority, target=into, **_decision(command)
        )
        return {}
    revoked = containment.revoke(
        store, kind=kind, object_id=object_id, **_decision(command)
    )
    return {"local_tasks": revoked["local_tasks"]}


def _filter(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    target = command["target"]
    containment.place_filter(
        store,
        authority=target["authority"],
        target=target["into"],
        **_decision(command),
    )
    return {}


def _unfilter(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    target = command["target"]
    removed = containment.remove_filter(
        store,
        authority=target["authority"],
        target=target["into"],
        **_decision(command),
    )
    return {"applied_deliveries": removed["applied_deliveries"]}


def _cancel(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    cancelled = containment.cancel_task(
        store, task_id=command["target"]["task_id"], **_decision(command)
    )
    return {"released_holds": cancelled["released_holds"]}


def _renew(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    with store._task_transaction():
        containment.check_fence(
            store._connection, command["owner"], command["policy_version"]
        )
    until = store.renew_controller_lease(_now_ms(), command["expires_at_ms"])
    return {
        "controller_lease_until_ms": until,
        "controller_lease_enforced": store.controller_lease_ms is not None,
    }


def _describe(store: AgentdStore, command: Mapping[str, Any]) -> dict[str, Any]:
    del store, command
    return {"capabilities": list(CAPABILITIES)}


_APPLY: dict[str, Callable[[AgentdStore, Mapping[str, Any]], dict[str, Any]]] = {
    "hold": _hold,
    "release": _release,
    "revoke": _revoke,
    "filter": _filter,
    "unfilter": _unfilter,
    "cancel": _cancel,
    "renew": _renew,
    "describe": _describe,
}

# What each decision closes at this node.
_GATES = {
    "hold:task": [
        "claim",
        "transition",
        "dispatch",
        "effect",
        "publication",
        "inbound",
    ],
    "hold:egress": ["result", "dispatch", "effect", "publication"],
    "hold:transport": ["publication"],
    "hold:effect": ["effect"],
    "revoke:authority": ["dispatch", "effect"],
    "revoke:egress": ["dispatch", "effect"],
    "revoke:credential": ["dispatch", "effect", "task_create", "publication"],
    "filter:agent": ["dispatch", "task_create", "publication", "inbound"],
    "filter:effect": ["effect"],
}


def _snapshot(
    store: AgentdStore,
    command: Mapping[str, Any],
    enforcement: dict[str, Any],
    in_flight: frozenset[str],
) -> dict[str, Any]:
    """Describe the enforced state of the command's object at this instant."""
    action, target = command["action"], command["target"]
    if action == "revoke" and target["kind"] in {"grant", "admission"}:
        authority, into = _revocation_filter(target["kind"], target["object_id"])
        action, target = "filter", {"authority": authority, "into": into}
    db = store._connection
    with store._lock:
        if action in {"hold", "release"}:
            kind, object_id = target["kind"], target["object_id"]
            if action == "hold":
                enforcement["gates"] = _GATES[f"hold:{kind}"]
            if kind == "event":
                row = db.execute(
                    "SELECT kind,state FROM held_events WHERE event_id=?",
                    (object_id,),
                ).fetchone()
                if row is not None:
                    enforcement["held_event"] = {
                        "event": row["kind"],
                        "state": row["state"],
                    }
                return enforcement
            if kind == "effect":
                row = db.execute(
                    "SELECT state FROM scheduled_effects WHERE effect_id=?",
                    (object_id,),
                ).fetchone()
                enforcement["effect_state"] = row["state"] if row else None
                return enforcement
            if kind == "transport":
                messages = db.execute(
                    f"SELECT o.message_id,o.published_at_ms,{_BLOCKED} "
                    "FROM transport_outbox o WHERE o.message_id=?",
                    (object_id,),
                ).fetchall()
            else:
                messages = _egress_messages(db, object_id)
                enforcement["task"] = _task(db, object_id)
                if kind == "task":
                    enforcement["parked_deliveries"] = int(
                        db.execute(
                            "SELECT COUNT(*) FROM held_deliveries WHERE task_id=?",
                            (object_id,),
                        ).fetchone()[0]
                    )
            enforcement["outbound"] = _outbound(messages, in_flight)
        elif action == "revoke":
            enforcement["gates"] = _GATES[f"revoke:{target['kind']}"]
            if target["kind"] == "credential":
                messages = db.execute(
                    f"SELECT o.message_id,o.published_at_ms,{_BLOCKED} "
                    "FROM transport_outbox o JOIN tasks t ON t.task_id=o.task_id "
                    "WHERE ?=CASE WHEN o.subject='agents.'||t.recipient_id||'.inbox' "
                    "THEN t.sender_id ELSE t.recipient_id END",
                    (target["object_id"],),
                ).fetchall()
                enforcement["outbound"] = _outbound(messages, in_flight)
        elif action in {"filter", "unfilter"}:
            into = target["into"]
            kind = "agent" if "agent_id" in into else "effect"
            if action == "filter":
                enforcement["gates"] = _GATES[f"filter:{kind}"]
            if kind == "agent":
                key = containment.filter_authority(target["authority"])
                messages = db.execute(
                    f"SELECT o.message_id,o.published_at_ms,{_BLOCKED} "
                    "FROM transport_outbox o JOIN tasks t ON t.task_id=o.task_id "
                    "LEFT JOIN trace_task_contexts c ON c.task_id=t.task_id "
                    "WHERE t.recipient_id=? AND o.subject='agents.'||t.recipient_id||'.inbox' "
                    "AND (?='any:*' OR ?='agent:'||t.sender_id OR ? IN (SELECT "
                    "'task:'||a.value FROM json_each(c.context_json,'$.authority_chain') a))",
                    (into["agent_id"], key, key, key),
                ).fetchall()
                enforcement["outbound"] = _outbound(messages, in_flight)
                enforcement["parked_deliveries"] = int(
                    db.execute(
                        "SELECT COUNT(*) FROM held_deliveries d WHERE d.recipient_id=? "
                        "AND EXISTS (SELECT 1 FROM json_each(d.authorities_json) a "
                        "WHERE a.value=?)",
                        (into["agent_id"], key),
                    ).fetchone()[0]
                )
        elif action == "cancel":
            enforcement["task"] = _task(db, target["task_id"])
            enforcement["outbound"] = _outbound(
                _egress_messages(db, target["task_id"]), in_flight
            )
    return enforcement


def _egress_messages(db: Any, object_id: str) -> list[Any]:
    """An invocation's own messages and the requests or cancels it sent children.

    ``object_id`` is a task, or the binding of a task-less native root whose
    children name its run as their parent.
    """
    return db.execute(
        f"SELECT o.message_id,o.published_at_ms,{_BLOCKED} FROM transport_outbox o "
        "WHERE o.task_id=? OR EXISTS (SELECT 1 FROM tasks t "
        "JOIN trace_task_contexts c ON c.task_id=t.task_id WHERE t.task_id=o.task_id "
        "AND o.subject='agents.'||t.recipient_id||'.inbox' "
        "AND (json_extract(c.context_json,'$.parent_task_id')=? "
        "OR json_extract(c.context_json,'$.parent_run_id')=(SELECT trace_id FROM "
        "trace_bindings_all WHERE binding_id=? AND task_id IS NULL)))",
        (object_id, object_id, object_id),
    ).fetchall()


def _outbound(messages: list[Any], in_flight: frozenset[str]) -> dict[str, Any]:
    """Classify the object's outbound messages at the enforcement instant."""
    waiting = [row for row in messages if row[1] is None and row[0] not in in_flight]
    return {
        "held": sum(1 for row in waiting if row[2]),
        "pending": sum(1 for row in waiting if not row[2]),
        "in_flight": sorted(
            row[0] for row in messages if row[1] is None and row[0] in in_flight
        ),
        "published": sum(1 for row in messages if row[1] is not None),
    }


def _task(db: Any, task_id: str) -> dict[str, Any] | None:
    row = db.execute(
        "SELECT state,claimed_session_id FROM tasks WHERE task_id=?", (task_id,)
    ).fetchone()
    if row is None:
        return None
    return {
        "state": row["state"],
        "claimed": row["claimed_session_id"] is not None
        and row["state"] not in TERMINAL_STATES,
    }
