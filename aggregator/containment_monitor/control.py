"""ControlClient: the narrow interface between the monitor and enforcement nodes.

Three operations, each fenced by the owner's policy version (monotone per owner;
a node rejects anything older than the newest version it accepted):

- ``place_hold(node_id, kind, object_id)``: hold a task, transport message or effect;
- ``release_hold(node_id, kind, object_id)``;
- ``revoke(node_id, kind, object_id)``: narrow authority (grant, credential, admission).

Each call returns only after the node confirmed the decision, with the node's own
application time when it reports one. ``capabilities`` lists the ``op:kind`` pairs
a deployment supports; the projection treats anything else as uncuttable.

``LocalAgentdControlClient`` calls agentd's admin RPC ``containment.control`` on
co-located agentd sockets for task and transport holds; revocations are not
mapped yet. The remote NATS control subject can implement the same
three methods.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .model import Action


class ControlError(RuntimeError):
    """A node refused or could not confirm a decision; ``code`` is stable."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Confirmation:
    node_id: str
    applied_at_ms: int | None  # node clock, when reported


class ControlClient(Protocol):
    capabilities: frozenset[str]

    def place_hold(
        self,
        node_id: str,
        kind: str,
        object_id: str,
        *,
        owner: str,
        policy_version: int,
    ) -> Confirmation: ...

    def release_hold(
        self,
        node_id: str,
        kind: str,
        object_id: str,
        *,
        owner: str,
        policy_version: int,
    ) -> Confirmation: ...

    def revoke(
        self,
        node_id: str,
        kind: str,
        object_id: str,
        *,
        owner: str,
        policy_version: int,
    ) -> Confirmation: ...


def execute(
    client: ControlClient, action: Action, *, owner: str, policy_version: int
) -> Confirmation:
    if action.capability not in client.capabilities:
        raise ControlError("unsupported_action")
    call = client.place_hold if action.op == "hold" else client.revoke
    return call(
        action.node_id,
        action.kind,
        action.object_id,
        owner=owner,
        policy_version=policy_version,
    )


def release(
    client: ControlClient, action: Action, *, owner: str, policy_version: int
) -> Confirmation:
    if action.op != "hold":
        # Revocations are irreversible here; phase 1 uses holds only.
        raise ControlError("irreversible_action")
    return client.release_hold(
        action.node_id,
        action.kind,
        action.object_id,
        owner=owner,
        policy_version=policy_version,
    )


# ---------------------------------------------------------------------------
# In-process fake (tests and offline experiments)
# ---------------------------------------------------------------------------
@dataclass
class FakeNode:
    holds: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)
    revoked: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)
    fences: dict[str, int] = field(default_factory=dict)


class FakeControlClient:
    """agentd's fencing semantics in memory, with per-node delay and failure hooks.

    ``timeline`` records (monotonic time, event, action key) after each node
    applied a decision, so tests can check what was in force at every instant.
    """

    def __init__(
        self,
        capabilities: frozenset[str] = frozenset(
            {
                "hold:task",
                "hold:transport",
                "hold:effect",
                "revoke:egress",
                "revoke:grant",
                "revoke:credential",
                "revoke:admission",
            }
        ),
        *,
        delay: Callable[[str, str], float] | None = None,
        fail: Callable[[str, str, str], str | None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.capabilities = frozenset(capabilities)
        self.nodes: dict[str, FakeNode] = {}
        self.delay = delay or (lambda node_id, op: 0.0)
        self.fail = fail or (lambda node_id, op, object_id: None)
        self.clock = clock
        self.timeline: list[tuple[float, str, tuple[str, str, str, str]]] = []
        self._lock = threading.Lock()

    def _apply(self, op, node_id, kind, object_id, owner, version) -> Confirmation:
        wait = self.delay(node_id, op)
        if wait:
            time.sleep(wait)
        code = self.fail(node_id, op, object_id)
        if code:
            raise ControlError(code)
        with self._lock:
            node = self.nodes.setdefault(node_id, FakeNode())
            if version < node.fences.get(owner, 0):
                raise ControlError("stale_policy_version")
            key = (kind, object_id)
            if op == "release":
                held = node.holds.get(key)
                if held is None:
                    raise ControlError("hold_not_found")
                if held[0] != owner:
                    raise ControlError("hold_owned_by_another")
            elif op == "hold" and key in node.holds and node.holds[key][0] != owner:
                raise ControlError("hold_owned_by_another")
            node.fences[owner] = version
            if op == "hold":
                node.holds[key] = (owner, version)
            elif op == "release":
                del node.holds[key]
            else:
                node.revoked[key] = (owner, version)
            self.timeline.append((self.clock(), op, (node_id, kind, object_id, owner)))
        return Confirmation(node_id, int(time.time() * 1000))

    def place_hold(self, node_id, kind, object_id, *, owner, policy_version):
        return self._apply("hold", node_id, kind, object_id, owner, policy_version)

    def release_hold(self, node_id, kind, object_id, *, owner, policy_version):
        return self._apply("release", node_id, kind, object_id, owner, policy_version)

    def revoke(self, node_id, kind, object_id, *, owner, policy_version):
        return self._apply("revoke", node_id, kind, object_id, owner, policy_version)

    def in_force(self) -> set[tuple[str, str, str]]:
        with self._lock:
            return {
                (node_id, kind, object_id)
                for node_id, node in self.nodes.items()
                for (kind, object_id) in [*node.holds, *node.revoked]
            }


# ---------------------------------------------------------------------------
# Co-located agentd over its admin RPC
# ---------------------------------------------------------------------------
Transport = Callable[[str, dict], object]


class LocalAgentdControlClient:
    """Task and transport holds through ``containment.control`` on agentd admin sockets.

    ``endpoints`` maps node_id to a transport ``(operation, params) -> result``.
    ``from_sockets`` builds transports from the private socket and admin token of
    each node's agentd state directory.
    """

    capabilities = frozenset({"hold:task", "hold:transport"})

    def __init__(self, endpoints: Mapping[str, Transport]):
        self.endpoints = dict(endpoints)

    @classmethod
    def from_sockets(
        cls, nodes: Mapping[str, tuple[Path, Path]], timeout: float = 5.0
    ) -> LocalAgentdControlClient:
        from edgecitadel_agentd.client import AgentdClient, AgentdClientError

        def transport(socket_path: Path, token_path: Path) -> Transport:
            def call(operation: str, params: dict) -> object:
                client = AgentdClient(
                    socket_path,
                    admin_token=token_path.read_text().strip(),
                    timeout=timeout,
                )
                try:
                    return client.call(operation, **params)
                except AgentdClientError as error:
                    raise ControlError(_code(str(error))) from None

            return call

        return cls({node: transport(*paths) for node, paths in nodes.items()})

    def _call(self, node_id: str, operation: str, params: dict) -> object:
        endpoint = self.endpoints.get(node_id)
        if endpoint is None:
            raise ControlError("node_unreachable")
        try:
            return endpoint(operation, params)
        except ControlError:
            raise
        except Exception as error:  # noqa: BLE001 - map to a stable code, no payload text
            raise ControlError(_code(str(error))) from None

    def _control(self, node_id, action, kind, object_id, owner, policy_version):
        # m2/agentd replaced containment.hold/release with containment.control,
        # which answers with an enforcement confirmation instead of raising.
        reply = self._call(
            node_id,
            "containment.control",
            {
                "action": action,
                "target": {"kind": kind, "object_id": object_id},
                "owner": owner,
                "policy_version": policy_version,
            },
        )
        if not isinstance(reply, dict) or reply.get("status") != "applied":
            code = reply.get("code") if isinstance(reply, dict) else None
            raise ControlError(code if isinstance(code, str) else "control_failed")
        return Confirmation(node_id, reply.get("applied_at_ms"))

    def place_hold(self, node_id, kind, object_id, *, owner, policy_version):
        if kind not in {"task", "transport"}:
            raise ControlError("unsupported_action")
        return self._control(node_id, "hold", kind, object_id, owner, policy_version)

    def release_hold(self, node_id, kind, object_id, *, owner, policy_version):
        return self._control(node_id, "release", kind, object_id, owner, policy_version)

    def revoke(self, node_id, kind, object_id, *, owner, policy_version):
        raise ControlError("unsupported_action")


KNOWN_CODES = frozenset(
    {
        "stale_policy_version",
        "hold_owned_by_another",
        "hold_not_found",
        "invalid_hold_kind",
        "invalid_hold_object",
        "invalid_hold_owner",
        "invalid_policy_version",
    }
)


def _code(message: str) -> str:
    return message if message in KNOWN_CODES else "control_failed"


class UnconfiguredControlClient:
    """No enforcement endpoints: decisions are computed and logged, never applied."""

    capabilities = frozenset({"hold:task", "hold:transport"})

    def place_hold(self, node_id, kind, object_id, *, owner, policy_version):
        raise ControlError("node_unreachable")

    def release_hold(self, node_id, kind, object_id, *, owner, policy_version):
        raise ControlError("node_unreachable")

    def revoke(self, node_id, kind, object_id, *, owner, policy_version):
        raise ControlError("node_unreachable")
