"""Delegation grants and the effective authority scope of an invocation.

A grant is administrator state, never a caller claim. For a Managed Agent it is
the installed package's ``outboundAgents``. For a native connector (Pi, Claude
Code, Codex) it is the ``outbound_agents`` list set by ``connector.grant``, and
it is empty until an administrator grants recipients. An invocation's authority
scope is its connector's grant intersected with the scope its task inherited
from the delegating invocation, so a child never reaches further than its parent.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from .trace_contract import canonical_bytes

if TYPE_CHECKING:
    from .store import AgentdStore

# Version 2: recipients are checked against the grant and the inherited scope.
GRANT_POLICY_VERSION = "2"


@dataclass(frozen=True)
class DelegationGrant:
    policy: str
    version: str
    usable: bool
    recipients: frozenset[str]

    def scope(self, inherited: tuple[str, ...] | None) -> frozenset[str]:
        if not self.usable:
            return frozenset()
        return (
            self.recipients if inherited is None else self.recipients & set(inherited)
        )


def delegation_grant(store: AgentdStore, connector: sqlite3.Row) -> DelegationGrant:
    agent_id = str(connector["agent_id"])
    if connector["host_type"] == "managed-agent":
        owners = [
            record
            for record in store.list_managed_agents()
            if agent_id in cast(list[str], record.get("agent_ids", []))
        ]
        return DelegationGrant(
            "managed_package_grant",
            hashlib.sha256(
                json.dumps(owners, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            connector["connector_id"] == f"managed-{agent_id}"
            and len(owners) == 1
            and owners[0].get("desired_state") == "running",
            frozenset(
                cast(list[str], owners[0].get("outbound_agents", []))
                if len(owners) == 1
                else ()
            ),
        )
    capabilities = sorted(json.loads(connector["capabilities_json"])["items"])
    recipients = sorted(json.loads(connector["outbound_agents_json"]))
    return DelegationGrant(
        "native_connector_grant",
        hashlib.sha256(
            canonical_bytes(
                {"capabilities": capabilities, "outbound_agents": recipients}
            )
        ).hexdigest(),
        "edgecitadel_delegate" in capabilities,
        frozenset(recipients),
    )
