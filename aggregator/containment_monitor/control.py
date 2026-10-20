"""The monitor's side of agentd's containment control protocol.

The monitor drives agentd nodes with versioned, owner-fenced commands
(``schemas/containment-control.v1.json``) and trusts only their confirmations.
``command_for`` maps the monitor's action vocabulary onto agentd commands:

=================================  =============================================
monitor action                     agentd command
=================================  =============================================
hold:task|egress|transport|effect  ``hold`` (``release``) ``{kind, object_id}``
revoke:egress|authority            ``revoke {kind, object_id: <task>}``
revoke:credential|admission        ``revoke {kind, object_id: <agent>}``
revoke:grant                       ``revoke {kind: grant, object_id: a>b|a>fx:c}``
cancel:task                        ``cancel {task_id}``
=================================  =============================================

A send has exactly one of three outcomes:

- a ``Confirmation``: the node applied the command at ``applied_at_ms`` (its
  clock) and reports what it enforced (``enforcement``);
- ``ControlError(code)``: definitely not applied: the node rejected it with a
  stable code (``stale_policy_version``, ``hold_owned_by_another``...), or it
  could not be delivered (``node_unreachable``);
- ``ControlUnconfirmed``: sent, but no confirmation arrived in time. The node may
  or may not have applied it, so callers treat it neither as in force nor as
  absent.

Capabilities are each node's answer to ``describe`` (``op:kind`` pairs), cached
per node by ``refresh``; a node that has never answered supports nothing, so the
projection offers no action there.

Transports: ``LocalAgentdControlClient`` (admin RPC ``containment.control`` on
co-located sockets) and ``NatsControlClient`` (HMAC-signed request/reply on
``edgecitadel.control.v1.<node_id>``). ``FakeControlClient`` reproduces agentd's
fencing in memory; ``UnconfiguredControlClient`` plans with agentd's vocabulary
and applies nothing.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from edgecitadel_agentd.containment_control import (
    CAPABILITIES,
    MAX_COMMAND_LIFETIME_MS,
    command_mac,
    subject_for,
    valid_confirmation,
)

from .model import CANCEL, HOLD, REVOKE, Action

AGENTD_CAPABILITIES = frozenset(CAPABILITIES)
# describe is not a containment decision: agentd checks no fence for it, but the
# command schema requires a positive version.
DESCRIBE_POLICY_VERSION = 1
DESCRIBE_RETRY_SECONDS = 5.0
DESCRIBE_TTL_SECONDS = 300.0


class ControlError(RuntimeError):
    """The node did not apply the command; ``code`` is stable."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class ControlUnconfirmed(ControlError):
    """Sent without a confirmation: whether the node applied it is unknown."""


@dataclass(frozen=True)
class Confirmation:
    node_id: str
    command_id: str
    applied_at_ms: int  # node clock
    enforcement: Mapping[str, Any]


def command_for(action: Action, *, release: bool = False) -> tuple[str, dict]:
    """The agentd command (action name, target) that applies or releases ``action``."""
    if action.op == HOLD:
        target = {"kind": action.kind, "object_id": action.object_id}
        return ("release" if release else "hold"), target
    if release:
        raise ControlError("irreversible_action")
    if action.op == REVOKE:
        return "revoke", {"kind": action.kind, "object_id": action.object_id}
    if action.op == CANCEL:
        return "cancel", {"task_id": action.object_id}
    raise ControlError("unsupported_action")


class ControlClient(Protocol):
    owner: str

    def capabilities(self, node_id: str) -> frozenset[str]: ...

    def refresh(self, nodes: Iterable[str]) -> None: ...

    def send(
        self,
        node_id: str,
        action: str,
        target: Mapping[str, object],
        *,
        policy_version: int,
    ) -> Confirmation: ...

    def close(self) -> None: ...


def execute(
    client: ControlClient, action: Action, *, node_id: str, policy_version: int
) -> Confirmation:
    """Apply ``action`` at one of its sites (``node_id``)."""
    if action.capability not in client.capabilities(node_id):
        raise ControlError("unsupported_action")
    name, target = command_for(action)
    return client.send(node_id, name, target, policy_version=policy_version)


def release(
    client: ControlClient, action: Action, *, policy_version: int
) -> Confirmation:
    name, target = command_for(action, release=True)
    return client.send(action.node_id, name, target, policy_version=policy_version)


def confirmation_from(
    reply: object, *, node_id: str, command_id: str, action: str
) -> Confirmation:
    """Check a reply against the command it answers; raise for a rejection.

    A reply that is not a valid confirmation of this command proves nothing about
    what the node did, so it is unconfirmed rather than failed.
    """
    if not valid_confirmation(reply) or reply["node_id"] != node_id:
        raise ControlUnconfirmed("invalid_confirmation")
    if reply["status"] == "rejected":
        # invalid_command rejections cannot name the command they could not parse.
        if reply["command_id"] not in {command_id, None}:
            raise ControlUnconfirmed("invalid_confirmation")
        raise ControlError(reply["code"] or "rejected")
    if reply["command_id"] != command_id or reply["action"] != action:
        raise ControlUnconfirmed("invalid_confirmation")
    return Confirmation(
        node_id, command_id, reply["applied_at_ms"], reply["enforcement"] or {}
    )


class AgentdControl:
    """Describe-based capabilities, shared by the real agentd transports."""

    def __init__(self, owner: str, *, clock: Callable[[], float] = time.monotonic):
        self.owner = owner
        self._clock = clock
        self._lock = threading.Lock()
        # node -> (capabilities, monotonic time of the last describe attempt)
        self._described: dict[str, tuple[frozenset[str], float]] = {}

    def send(
        self,
        node_id: str,
        action: str,
        target: Mapping[str, object],
        *,
        policy_version: int,
    ) -> Confirmation:
        raise NotImplementedError

    def capabilities(self, node_id: str) -> frozenset[str]:
        with self._lock:
            cached = self._described.get(node_id)
        return cached[0] if cached else frozenset()

    def known_capabilities(self) -> dict[str, list[str]]:
        with self._lock:
            return {node: sorted(caps) for node, (caps, _) in self._described.items()}

    def refresh(self, nodes: Iterable[str]) -> None:
        """Describe nodes never described, failed a while ago, or described long ago.

        A failed refresh keeps the last answer: what a node supports does not
        change because it is unreachable now.
        """
        now = self._clock()
        for node_id in sorted(set(nodes)):
            with self._lock:
                cached = self._described.get(node_id)
            if cached is not None:
                caps, at = cached
                wait = DESCRIBE_TTL_SECONDS if caps else DESCRIBE_RETRY_SECONDS
                if now - at < wait:
                    continue
            try:
                confirmation = self.send(
                    node_id, "describe", {}, policy_version=DESCRIBE_POLICY_VERSION
                )
                caps = frozenset(confirmation.enforcement.get("capabilities", ()))
            except ControlError:
                caps = cached[0] if cached else frozenset()
            with self._lock:
                self._described[node_id] = (caps, now)

    def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Co-located agentd over its admin RPC
# ---------------------------------------------------------------------------
Transport = Callable[[str, dict], object]


class LocalAgentdControlClient(AgentdControl):
    """``containment.control`` on agentd admin sockets of co-located nodes.

    ``endpoints`` maps node_id to a transport ``(operation, params) -> result``;
    a transport raises ``ControlError`` when the request was not delivered and
    ``ControlUnconfirmed`` when it was sent but not answered. agentd stamps an
    admin command when it receives it, so a retry would conflict with the first
    attempt: an unanswered command stays unconfirmed.
    """

    def __init__(self, owner: str, endpoints: Mapping[str, Transport], **kwargs):
        super().__init__(owner, **kwargs)
        self.endpoints = dict(endpoints)

    @classmethod
    def from_sockets(
        cls, owner: str, nodes: Mapping[str, tuple[Path, Path]], timeout: float = 5.0
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
                    if isinstance(error.__cause__, TimeoutError):
                        raise ControlUnconfirmed("confirmation_timeout") from None
                    if isinstance(error.__cause__, OSError):
                        raise ControlError("node_unreachable") from None
                    raise ControlError("control_failed") from None

            return call

        return cls(owner, {node: transport(*paths) for node, paths in nodes.items()})

    def send(self, node_id, action, target, *, policy_version):
        endpoint = self.endpoints.get(node_id)
        if endpoint is None:
            raise ControlError("node_unreachable")
        command_id = str(uuid4())
        reply = endpoint(
            "containment.control",
            {
                "action": action,
                "target": dict(target),
                "owner": self.owner,
                "policy_version": policy_version,
                "command_id": command_id,
            },
        )
        return confirmation_from(
            reply, node_id=node_id, command_id=command_id, action=action
        )


# ---------------------------------------------------------------------------
# Remote agentd over the signed NATS control subject
# ---------------------------------------------------------------------------
class NatsControlClient(AgentdControl):
    """Signed commands on ``edgecitadel.control.v1.<node_id>``.

    The command is authenticated by ``mac = HMAC-SHA256(owner key, context ||
    canonical JSON)`` (agentd's ``command_mac``). One command is retried with
    the same ``command_id`` and body until ``confirmation_timeout_s`` (and never
    past its expiry), so a node that received an earlier copy answers with its
    stored confirmation instead of applying it twice. If every attempt found no
    subscriber, nothing was delivered (``node_unreachable``); if any attempt may
    have been delivered and none was answered, the outcome is unconfirmed.
    """

    def __init__(
        self,
        url: str,
        token: str | None,
        owner: str,
        key: bytes,
        *,
        confirmation_timeout_s: float = 5.0,
        attempt_timeout_s: float = 1.0,
        command_lifetime_ms: int = 30_000,
        **kwargs,
    ):
        super().__init__(owner, **kwargs)
        if len(key) < 32:
            raise ValueError("control_key_too_short")
        if not 0 < command_lifetime_ms <= MAX_COMMAND_LIFETIME_MS:
            raise ValueError("invalid_command_lifetime")
        if confirmation_timeout_s * 1000 >= command_lifetime_ms:
            raise ValueError("confirmation_timeout_exceeds_command_lifetime")
        self.url, self.token, self.key = url, token, key
        self.confirmation_timeout_s = confirmation_timeout_s
        self.attempt_timeout_s = attempt_timeout_s
        self.command_lifetime_ms = command_lifetime_ms
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="containment-nats", daemon=True
        )
        self._thread.start()
        self._nc = None
        self._connecting = asyncio.Lock()

    @classmethod
    def from_key_file(
        cls, url: str, token: str | None, owner: str, key_file: Path, **kwargs
    ) -> NatsControlClient:
        # agentd strips surrounding whitespace from its copy of the key; so do we.
        return cls(url, token, owner, key_file.read_bytes().strip(), **kwargs)

    def close(self) -> None:
        async def disconnect() -> None:
            if self._nc is not None and not self._nc.is_closed:
                await self._nc.close()

        if self._thread.is_alive():
            asyncio.run_coroutine_threadsafe(disconnect(), self._loop).result(5)
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)
        self._loop.close()

    async def _connection(self):
        from nats.aio.client import Client as NATS

        async with self._connecting:
            if self._nc is None or self._nc.is_closed:
                nc = NATS()
                await nc.connect(
                    servers=[self.url],
                    token=self.token,
                    connect_timeout=2,
                    reconnect_time_wait=0.5,
                    max_reconnect_attempts=-1,
                )
                self._nc = nc
            return self._nc

    def signed(
        self,
        node_id: str,
        action: str,
        target: Mapping[str, object],
        policy_version: int,
    ) -> dict[str, Any]:
        issued = time.time_ns() // 1_000_000
        command: dict[str, Any] = {
            "schema_version": 1,
            "command_id": str(uuid4()),
            "node_id": node_id,
            "owner": self.owner,
            "policy_version": policy_version,
            "issued_at_ms": issued,
            "expires_at_ms": issued + self.command_lifetime_ms,
            "action": action,
            "target": dict(target),
        }
        command["mac"] = command_mac(self.key, command)
        return command

    def send(self, node_id, action, target, *, policy_version):
        command = self.signed(node_id, action, target, policy_version)
        future = asyncio.run_coroutine_threadsafe(self._request(command), self._loop)
        return future.result()

    async def _request(self, command: dict[str, Any]) -> Confirmation:
        from nats import errors

        node_id, command_id = command["node_id"], command["command_id"]
        payload = json.dumps(command, separators=(",", ":")).encode()
        # Stop retrying a little before expiry: a copy that arrives after it is
        # rejected as expired even if an earlier copy was applied.
        expiry_budget = (
            command["expires_at_ms"] - time.time_ns() // 1_000_000 - 1000
        ) / 1000
        deadline = time.monotonic() + min(self.confirmation_timeout_s, expiry_budget)
        maybe_delivered = False
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                nc = await asyncio.wait_for(self._connection(), remaining)
                reply = await nc.request(
                    subject_for(node_id),
                    payload,
                    timeout=min(self.attempt_timeout_s, remaining),
                )
            except errors.NoRespondersError:
                await asyncio.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
                continue
            except (errors.TimeoutError, TimeoutError):
                maybe_delivered = True
                continue
            except (errors.Error, OSError):
                # Not connected: nothing was handed to the broker on this attempt.
                await asyncio.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
                continue
            try:
                document = json.loads(reply.data)
                return confirmation_from(
                    document,
                    node_id=node_id,
                    command_id=command_id,
                    action=command["action"],
                )
            except ValueError:
                maybe_delivered = True  # an unreadable reply from someone
            except ControlUnconfirmed:
                maybe_delivered = True
        if maybe_delivered:
            raise ControlUnconfirmed("confirmation_timeout")
        raise ControlError("node_unreachable")


# ---------------------------------------------------------------------------
# In-process fake (tests and offline experiments)
# ---------------------------------------------------------------------------
@dataclass
class FakeNode:
    holds: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)
    revoked: dict[tuple[str, str], tuple[str, int]] = field(default_factory=dict)
    fences: dict[str, int] = field(default_factory=dict)


class FakeControlClient:
    """agentd's fencing semantics in memory, with per-node delay and fault hooks.

    ``capabilities`` is one set for every node or a per-node mapping. ``fail``
    returns a rejection code for a command; ``lose`` applies a command but drops
    its confirmation (the worst case for an unconfirmed action). ``timeline``
    records (monotonic time, command, (node, kind, object, owner)) after a node
    applied a decision, so tests can check what was in force at every instant.
    """

    def __init__(
        self,
        capabilities: frozenset[str]
        | Mapping[str, frozenset[str]] = AGENTD_CAPABILITIES,
        *,
        owner: str = "core-monitor",
        delay: Callable[[str, str], float] | None = None,
        fail: Callable[[str, str, str], str | None] | None = None,
        lose: Callable[[str, str, str], bool] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.owner = owner
        self._capabilities = capabilities
        self.nodes: dict[str, FakeNode] = {}
        self.delay = delay or (lambda node_id, op: 0.0)
        self.fail = fail or (lambda node_id, op, object_id: None)
        self.lose = lose or (lambda node_id, op, object_id: False)
        self.clock = clock
        self.timeline: list[tuple[float, str, tuple[str, str, str, str]]] = []
        self._lock = threading.Lock()

    def capabilities(self, node_id: str) -> frozenset[str]:
        if isinstance(self._capabilities, Mapping):
            return frozenset(self._capabilities.get(node_id, ()))
        return frozenset(self._capabilities)

    def refresh(self, nodes: Iterable[str]) -> None:
        return None

    def close(self) -> None:
        return None

    def send(self, node_id, action, target, *, policy_version):
        command_id = str(uuid4())
        if action == "describe":
            caps = sorted(self.capabilities(node_id))
            return Confirmation(node_id, command_id, _now_ms(), {"capabilities": caps})
        if action == "renew":
            with self._lock:
                fences = self.nodes.setdefault(node_id, FakeNode()).fences
                if policy_version < fences.get(self.owner, 0):
                    raise ControlError("stale_policy_version")
                fences[self.owner] = policy_version
            return Confirmation(
                node_id,
                command_id,
                _now_ms(),
                {"controller_lease_until_ms": None, "controller_lease_enforced": False},
            )
        kind = target.get("kind", "task")
        object_id = target.get("object_id") or target["task_id"]
        wait = self.delay(node_id, action)
        if wait:
            time.sleep(wait)
        code = self.fail(node_id, action, object_id)
        if code:
            raise ControlError(code)
        with self._lock:
            node = self.nodes.setdefault(node_id, FakeNode())
            if policy_version < node.fences.get(self.owner, 0):
                raise ControlError("stale_policy_version")
            key = (kind, object_id)
            held = node.holds.get(key)
            if action in {"release", "cancel"} and held and held[0] != self.owner:
                raise ControlError("hold_owned_by_another")
            if action == "release" and held is None:
                raise ControlError("hold_not_found")
            if action == "hold" and held and held[0] != self.owner:
                raise ControlError("hold_owned_by_another")
            node.fences[self.owner] = policy_version
            if action == "hold":
                node.holds[key] = (self.owner, policy_version)
            elif action in {"release", "cancel"}:
                node.holds.pop(key, None)
            if action in {"revoke", "cancel"}:
                node.revoked[key] = (self.owner, policy_version)
            self.timeline.append(
                (self.clock(), action, (node_id, kind, object_id, self.owner))
            )
        if self.lose(node_id, action, object_id):
            raise ControlUnconfirmed("confirmation_timeout")
        return Confirmation(node_id, command_id, _now_ms(), {})

    def in_force(self) -> set[tuple[str, str, str]]:
        with self._lock:
            return {
                (node_id, kind, object_id)
                for node_id, node in self.nodes.items()
                for (kind, object_id) in [*node.holds, *node.revoked]
            }


class UnconfiguredControlClient:
    """No enforcement endpoints: plan with agentd's vocabulary, apply nothing."""

    def __init__(self, owner: str = "core-monitor"):
        self.owner = owner

    def capabilities(self, node_id: str) -> frozenset[str]:
        return AGENTD_CAPABILITIES

    def refresh(self, nodes: Iterable[str]) -> None:
        return None

    def close(self) -> None:
        return None

    def send(self, node_id, action, target, *, policy_version):
        raise ControlError("node_unreachable")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000
