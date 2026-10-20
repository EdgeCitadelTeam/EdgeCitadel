"""Delegation grants and the effective authority of an invocation.

A grant is administrator state, never a caller claim. For a Managed Agent it is
the installed package's ``outboundAgents`` (recipients) and ``effects``. For a
native connector (Pi, Claude Code, Codex) it is the ``outbound_agents`` and
``effects`` lists set by ``connector.grant``, both empty until an administrator
grants them.

An invocation's authority is its connector's grant intersected with the
authority its task inherited from the delegating invocation, so a child never
reaches further than its parent:

- recipients it may dispatch to (``authority_scope``);
- effect rules it may authorize (``effect_scope``), each an effect class and a
  target that is exact or a prefix ending in ``*``;
- a resource lease: a wall-clock expiry and an effect budget (hard limits), plus
  an optional token budget that is carried and attenuated but not metered.

A child's lease never exceeds what its parent has left. Effects delegated to a
child are reserved from the parent's budget at dispatch, so the effects of a
whole delegation tree stay within the root's budget.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, cast

from .trace_contract import TraceContractError, canonical_bytes

if TYPE_CHECKING:
    from .store import AgentdStore
    from .trace_correlation import TaskTraceContext

# Version 3: effect rules and leases join recipients in the delegated authority.
GRANT_POLICY_VERSION = "3"
EFFECT_CLASSES = frozenset(
    {"fs.write", "process.exec", "network.http", "device.command"}
)
MAX_EFFECT_RULES = 64
# run/started lists an invocation's recipients and effect rules up to this
# size; a larger scope is exported as null (consult the grant instead).
MAX_EXPORTED_SCOPE_BYTES = 8 * 1024
MAX_INTEGER = 9_007_199_254_740_991
_TARGET = re.compile(r"[^\x00-\x1f\x7f]{1,512}")


class LeaseExceeded(Exception):
    """A requested or used amount exceeds the remaining lease."""

    def __init__(self, dimension: str) -> None:
        self.dimension = dimension
        super().__init__(dimension)


@dataclass(frozen=True, order=True)
class EffectRule:
    """An effect class and an exact target, or a target prefix ending in ``*``."""

    effect_class: str
    target: str

    def __post_init__(self) -> None:
        if (
            self.effect_class not in EFFECT_CLASSES
            or not isinstance(self.target, str)
            or _TARGET.fullmatch(self.target) is None
        ):
            raise TraceContractError("invalid_effect_rule")

    def covers(self, other: EffectRule) -> bool:
        if self.effect_class != other.effect_class:
            return False
        if self.target.endswith("*"):
            return other.target.startswith(self.target[:-1])
        return self.target == other.target


def effect_rules(value: object) -> frozenset[EffectRule]:
    """Parse rules from their JSON form, ``[{"effect_class", "target"}]``."""
    if isinstance(value, frozenset):
        return cast(frozenset[EffectRule], value)
    if not isinstance(value, (list, tuple)) or len(value) > MAX_EFFECT_RULES:
        raise TraceContractError("invalid_effect_rule")
    rules = []
    for item in value:
        if isinstance(item, EffectRule):
            rules.append(item)
            continue
        if not isinstance(item, Mapping) or set(item) != {"effect_class", "target"}:
            raise TraceContractError("invalid_effect_rule")
        rules.append(EffectRule(item["effect_class"], item["target"]))
    return frozenset(rules)


def intersect_effects(
    left: Iterable[EffectRule], right: Iterable[EffectRule]
) -> frozenset[EffectRule]:
    """Rules allowing exactly the effects both sides allow (prefixes nest)."""
    right = tuple(right)
    common: set[EffectRule] = set()
    for a in left:
        for b in right:
            if a.covers(b):
                common.add(b)
            elif b.covers(a):
                common.add(a)
    return frozenset(
        rule
        for rule in common
        if not any(other != rule and other.covers(rule) for other in common)
    )


def effect_allowed(rules: Iterable[EffectRule], effect: EffectRule) -> bool:
    return any(rule.covers(effect) for rule in rules)


@dataclass(frozen=True)
class Lease:
    """Hard limits carried by a delegated task; None means unlimited."""

    expires_at_ms: int | None = None
    max_effects: int | None = None
    max_tokens: int | None = None

    def __post_init__(self) -> None:
        for value in (self.expires_at_ms, self.max_effects, self.max_tokens):
            if value is not None and (
                type(value) is not int or not 0 <= value <= MAX_INTEGER
            ):
                raise TraceContractError("invalid_lease")

    @classmethod
    def parse(cls, value: object) -> Lease | None:
        if value is None or isinstance(value, Lease):
            return value
        if not isinstance(value, Mapping) or set(value) != {
            "expires_at_ms",
            "max_effects",
            "max_tokens",
        }:
            raise TraceContractError("invalid_lease")
        return cls(**cast(dict[str, int | None], dict(value)))


@dataclass(frozen=True)
class LeaseRequest:
    """A delegator's request for a child lease, relative to dispatch time."""

    wall_time_ms: int | None = None
    max_effects: int | None = None
    max_tokens: int | None = None


def remaining_effects(
    db: sqlite3.Connection, task_id: str | None, lease: Lease | None
) -> int | None:
    if task_id is None or lease is None or lease.max_effects is None:
        return None
    row = db.execute(
        "SELECT effects_used,effects_reserved FROM authority_ledger WHERE task_id=?",
        (task_id,),
    ).fetchone()
    spent = int(row[0]) + int(row[1]) if row else 0
    return max(0, lease.max_effects - spent)


def charge(
    db: sqlite3.Connection, task_id: str, *, used: int = 0, reserved: int = 0
) -> None:
    db.execute(
        "INSERT INTO authority_ledger(task_id,effects_used,effects_reserved) VALUES (?,?,?) "
        "ON CONFLICT(task_id) DO UPDATE SET effects_used=effects_used+excluded.effects_used, "
        "effects_reserved=effects_reserved+excluded.effects_reserved",
        (task_id, used, reserved),
    )


def child_lease(
    parent: Lease | None,
    parent_remaining_effects: int | None,
    requested: LeaseRequest,
    now_ms: int,
) -> Lease | None:
    """The largest lease the request asks for that fits the parent's remainder.

    Raises LeaseExceeded when the parent's lease has expired or the request
    asks for more than remains. An omitted limit inherits the parent's
    remainder.
    """
    if parent is not None and parent.expires_at_ms is not None:
        if now_ms >= parent.expires_at_ms:
            raise LeaseExceeded("wall_time")
    expiries = [
        value
        for value in (
            parent.expires_at_ms if parent else None,
            now_ms + requested.wall_time_ms
            if requested.wall_time_ms is not None
            else None,
        )
        if value is not None
    ]
    effects = requested.max_effects
    if effects is None:
        effects = parent_remaining_effects
    elif parent_remaining_effects is not None and effects > parent_remaining_effects:
        raise LeaseExceeded("effects")
    parent_tokens = parent.max_tokens if parent else None
    tokens = requested.max_tokens
    if tokens is None:
        tokens = parent_tokens
    elif parent_tokens is not None and tokens > parent_tokens:
        raise LeaseExceeded("tokens")
    lease = Lease(min(expiries) if expiries else None, effects, tokens)
    return None if lease == Lease() else lease


@dataclass(frozen=True)
class DelegationGrant:
    policy: str
    version: str
    usable: bool
    recipients: frozenset[str]
    effects: frozenset[EffectRule]

    def scope(self, inherited: tuple[str, ...] | None) -> frozenset[str]:
        if not self.usable:
            return frozenset()
        return (
            self.recipients if inherited is None else self.recipients & set(inherited)
        )

    def effect_scope(
        self, inherited: Iterable[EffectRule] | None
    ) -> frozenset[EffectRule]:
        # ``effects`` is already empty for an unusable package grant; a native
        # connector's effects do not depend on its delegation capability.
        return (
            self.effects
            if inherited is None
            else intersect_effects(self.effects, inherited)
        )


def delegation_grant(store: AgentdStore, connector: sqlite3.Row) -> DelegationGrant:
    agent_id = str(connector["agent_id"])
    if connector["host_type"] == "managed-agent":
        owners = [
            record
            for record in store.list_managed_agents()
            if agent_id in cast(list[str], record.get("agent_ids", []))
        ]
        usable = (
            connector["connector_id"] == f"managed-{agent_id}"
            and len(owners) == 1
            and owners[0].get("desired_state") == "running"
        )
        return DelegationGrant(
            "managed_package_grant",
            hashlib.sha256(
                json.dumps(owners, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            usable,
            frozenset(
                cast(list[str], owners[0].get("outbound_agents", []))
                if len(owners) == 1
                else ()
            ),
            effect_rules(owners[0].get("effect_grant", [])) if usable else frozenset(),
        )
    capabilities = sorted(json.loads(connector["capabilities_json"])["items"])
    recipients = sorted(json.loads(connector["outbound_agents_json"]))
    effects = json.loads(connector["effect_grant_json"])
    return DelegationGrant(
        "native_connector_grant",
        hashlib.sha256(
            canonical_bytes(
                {
                    "capabilities": capabilities,
                    "outbound_agents": recipients,
                    "effects": effects,
                }
            )
        ).hexdigest(),
        "edgecitadel_delegate" in capabilities,
        frozenset(recipients),
        effect_rules(effects),
    )


def exported_authority(
    grant: DelegationGrant, inherited: TaskTraceContext | None
) -> dict[str, Any]:
    """An invocation's effective authority when it starts, as run/started exports it.

    It is the grant intersected with what the task inherited, exactly as
    dispatch and effect admission compute it. Holds, revocations and filters
    are separate controller decisions and are not folded in; a grant changed
    during the run is not exported again.
    """
    recipients = sorted(
        grant.scope(inherited.authority_scope if inherited is not None else None)
    )
    effects = [
        asdict(rule)
        for rule in sorted(
            grant.effect_scope(
                inherited.effect_scope if inherited is not None else None
            )
        )
    ]
    if len(canonical_bytes([recipients, effects])) > MAX_EXPORTED_SCOPE_BYTES:
        recipients_value: list[str] | None = None
        effects_value: list[dict[str, str]] | None = None
    else:
        recipients_value, effects_value = recipients, effects
    lease = inherited.lease if inherited is not None else None
    return {
        "authority_version": 1,
        "policy": grant.policy,
        "policy_version": GRANT_POLICY_VERSION,
        "grant_version": grant.version,
        "recipients": recipients_value,
        "effects": effects_value,
        "lease": asdict(lease) if lease is not None else None,
        "authority_chain": list(inherited.authority_chain)
        if inherited is not None
        else [],
    }
