"""Shared vocabulary: provenance identities, control actions and alerts.

G_P node identities are typed strings so persisted edges and in-memory indexes
share one key space:

- ``inv:<execution_attempt_id>``: one invocation (an execution attempt);
- ``ver:<version_id>``: one request or result version (lineage ``version_id``);
- ``task:<task_id>``: a task identity, used only for authority edges and alerts;
- ``fx:<effect_id>``: one effect (forthcoming effect events, or an observed tool call).

Edges point in influence direction. ``data`` and ``session`` edges carry taint;
``authority`` (dispatch grants) and ``resource`` (leases) edges carry permission
and are followed only by root-cause slices of the matching alert type.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Any

DATA, SESSION, AUTHORITY, RESOURCE = "data", "session", "authority", "resource"
EDGE_TYPES = (DATA, SESSION, AUTHORITY, RESOURCE)
TAINT_EDGE_TYPES = (DATA, SESSION)

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
VERSION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
AGENT_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
NODE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


def inv(attempt_id: str) -> str:
    return f"inv:{attempt_id}"


def ver(version_id: str) -> str:
    return f"ver:{version_id}"


def task(task_id: str) -> str:
    return f"task:{task_id}"


def fx(effect_id: str) -> str:
    return f"fx:{effect_id}"


def split(node: str) -> tuple[str, str]:
    kind, _, value = node.partition(":")
    return kind, value


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------
HOLD, REVOKE, CANCEL = "hold", "revoke", "cancel"
# Reversible (released by the monitor): task (a pause), egress (an admission
# label: the invocation runs and receives, and agentd holds each new event it
# produces, its result included, until admitted), transport (one outbox
# message), effect (a scheduled effect stays uncommitted), event (one held
# event of a label: placed by agentd, released by the monitor to admit it).
HOLD_KINDS = frozenset({"task", "egress", "transport", "effect", "event"})
# Irreversible. egress: new dispatches and effects of one invocation (object: its
#   task id), which may still complete; authority: a delegation (task id) and all
#   its descendants; grant: one (agent, recipient-or-effect-class) pair of the
#   agent's delegation grant; credential: every outbound action of an agent;
#   admission: new tasks of an agent under any authority.
REVOKE_KINDS = frozenset({"egress", "authority", "grant", "credential", "admission"})
# Terminal: the task is cancelled and this owner's holds on it are removed.
CANCEL_KINDS = frozenset({"task"})
OP_KINDS = {HOLD: HOLD_KINDS, REVOKE: REVOKE_KINDS, CANCEL: CANCEL_KINDS}


@dataclass(frozen=True, order=True)
class TaintedOutputs:
    """Not a control action: accepting that a service agent's future
    unprotected outputs come from tainted memory (``tainted_output_value``).
    The cut solver may remove the edges into that agent's ``T:`` sink at this
    loss instead of stopping the taint upstream; nothing is sent or enforced."""

    agent: str

    def label(self) -> str:
        return f"accept:tainted_outputs:{self.agent}"


@dataclass(frozen=True, order=True)
class Action:
    """One enforceable control decision, sited at the node where it must land.

    ``hold``: ``kind`` is task/egress/transport/effect and ``object_id`` its UUID.
    ``revoke``: ``kind`` is egress/authority (a task UUID), credential/admission
    (``<agent>``) or grant (``<agent>><target>``, target an agent or
    ``fx:<class>``). ``cancel``: kind task. Every strategy produces this
    vocabulary; ``control.command_for`` maps it onto agentd's control commands.
    Revocations whose agentd state is node-local are also sent to the other nodes
    that may hold affected work (``projection.ScopeMirror.sites``); ``node_id`` is
    the site whose landing time decides feasibility.
    """

    op: str
    node_id: str
    kind: str
    object_id: str

    def __post_init__(self) -> None:
        if self.kind not in OP_KINDS.get(self.op, ()):
            raise ValueError("invalid_action")

    @property
    def capability(self) -> str:
        return f"{self.op}:{self.kind}"

    @property
    def reversible(self) -> bool:
        return self.op == HOLD

    def to_json(self) -> dict[str, str]:
        return {
            "op": self.op,
            "node_id": self.node_id,
            "kind": self.kind,
            "object_id": self.object_id,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Action:
        return cls(value["op"], value["node_id"], value["kind"], value["object_id"])

    def label(self) -> str:
        return f"{self.op}:{self.kind}:{self.object_id}@{self.node_id}"


def hold_task(node_id: str, task_id: str) -> Action:
    return Action(HOLD, node_id, "task", task_id)


def hold_effect(node_id: str, effect_id: str) -> Action:
    return Action(HOLD, node_id, "effect", effect_id)


def hold_egress(node_id: str, task_id: str) -> Action:
    return Action(HOLD, node_id, "egress", task_id)


def hold_event(node_id: str, event_id: str) -> Action:
    return Action(HOLD, node_id, "event", event_id)


def revoke_egress(node_id: str, task_id: str) -> Action:
    return Action(REVOKE, node_id, "egress", task_id)


def revoke_grant(node_id: str, agent_id: str, target: str) -> Action:
    return Action(REVOKE, node_id, "grant", f"{agent_id}>{target}")


def revoke_credential(node_id: str, agent_id: str) -> Action:
    return Action(REVOKE, node_id, "credential", agent_id)


def revoke_admission(node_id: str, agent_id: str) -> Action:
    return Action(REVOKE, node_id, "admission", agent_id)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
# External alert kinds accepted by intake; detectors emit the second group.
EXTERNAL_KINDS = frozenset(
    {"validator_failure", "validator_pass", "version_revoked", "operator"}
)
DETECTOR_KINDS = frozenset(
    {"admission_denied", "lease_violation", "revoked_version_consumed"}
)
# Kinds that are failures for spectrum ranking and trigger a response.
FAILURE_KINDS = frozenset({"validator_failure", "operator", *DETECTOR_KINDS})
# Influence edges the backward slice follows, per alert class.
SLICE_EDGES = {
    "validator_failure": (DATA, SESSION),
    "revoked_version_consumed": (DATA, SESSION),
    "admission_denied": (DATA, SESSION, AUTHORITY),
    "lease_violation": (DATA, SESSION, RESOURCE),
    "operator": EDGE_TYPES,
}
# The edge type that characterizes an alert class (type affinity in ranking).
PRIMARY_EDGE = {
    "validator_failure": DATA,
    "revoked_version_consumed": DATA,
    "admission_denied": AUTHORITY,
    "lease_violation": RESOURCE,
    "operator": DATA,
}
STRATEGY_NAMES = (
    "reject_source",
    "stop_team",
    "stop_downstream",
    "trace_and_stop",
    "trace_and_quarantine",
    "sink_guard",
    "gsafeguard",
    "exit_guard",
    "greedy_cut",
    "cut_without_deadlines",
    "firebreak",
)
# Strategies whose flag on the root's agent outlives the alert: a new unit of
# a flagged agent is a frontier advance, so the re-solve covers it.
STICKY_FLAG_STRATEGIES = frozenset({"gsafeguard"})
MAX_DETAIL_KEYS = 16
MAX_DETAIL_TEXT = 256


@dataclass(frozen=True)
class Alert:
    alert_id: str
    kind: str
    subject: str  # a G_P node identity
    observed_at_ms: int
    origin: str  # "external" | "detector"
    detail: dict[str, Any] = field(default_factory=dict)
    root: str | None = None  # operator-designated root cause (skips ranking choice)
    strategy: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "kind": self.kind,
            "subject": self.subject,
            "observed_at_ms": self.observed_at_ms,
            "origin": self.origin,
            "detail": self.detail,
            "root": self.root,
            "strategy": self.strategy,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Alert:
        return cls(**value)


class AlertError(ValueError):
    """Invalid external alert; the code is safe to return to the caller."""


def _subject(value: dict[str, Any]) -> str:
    given = [key for key in ("version_id", "task_id", "attempt_id") if key in value]
    if len(given) != 1:
        raise AlertError("alert_subject_required")
    key = given[0]
    identity = value[key]
    if not isinstance(identity, str):
        raise AlertError("invalid_alert_subject")
    if key == "version_id":
        if not VERSION_PATTERN.fullmatch(identity):
            raise AlertError("invalid_alert_subject")
        return ver(identity)
    if not UUID_PATTERN.fullmatch(identity):
        raise AlertError("invalid_alert_subject")
    return task(identity) if key == "task_id" else inv(identity)


def _detail(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict) or len(value) > MAX_DETAIL_KEYS:
        raise AlertError("invalid_alert_detail")
    for key, item in value.items():
        if not isinstance(key, str) or len(key) > 64:
            raise AlertError("invalid_alert_detail")
        if isinstance(item, str) and len(item) > MAX_DETAIL_TEXT:
            raise AlertError("invalid_alert_detail")
        if not isinstance(item, (str, int, float, bool)) and item is not None:
            raise AlertError("invalid_alert_detail")
    return dict(value)


def parse_external_alert(value: object, *, now_ms: int) -> Alert:
    """Validate an intake payload: exactly one subject, bounded detail, known kind.

    ``{"kind": "validator_failure", "version_id": "<64 hex>"}``;
    ``{"kind": "operator", "task_id"|"attempt_id": "<uuid>"}``;
    ``validator_pass`` records healthy evidence; ``version_revoked`` marks a
    version whose later consumption the detector reports.
    """
    if not isinstance(value, dict):
        raise AlertError("invalid_alert")
    allowed = {
        "kind",
        "version_id",
        "task_id",
        "attempt_id",
        "observed_at_ms",
        "detail",
        "root",
        "strategy",
        "alert_id",
    }
    if set(value) - allowed:
        raise AlertError("unknown_alert_field")
    kind = value.get("kind")
    if kind not in EXTERNAL_KINDS:
        raise AlertError("invalid_alert_kind")
    subject = _subject(value)
    if kind in {
        "validator_failure",
        "validator_pass",
        "version_revoked",
    } and not subject.startswith("ver:"):
        raise AlertError("version_subject_required")
    observed = value.get("observed_at_ms", now_ms)
    if type(observed) is not int or observed < 0:
        raise AlertError("invalid_alert_time")
    root = value.get("root")
    if root is not None:
        root = _subject(root) if isinstance(root, dict) else None
        if root is None:
            raise AlertError("invalid_alert_root")
    strategy = value.get("strategy")
    if strategy is not None and strategy not in STRATEGY_NAMES:
        raise AlertError("invalid_alert_strategy")
    alert_id = value.get("alert_id") or str(uuid.uuid4())
    if not isinstance(alert_id, str) or not UUID_PATTERN.fullmatch(alert_id):
        raise AlertError("invalid_alert_id")
    return Alert(
        alert_id,
        kind,
        subject,
        observed,
        "external",
        _detail(value.get("detail")),
        root,
        strategy,
    )


def detector_alert_id(kind: str, subject: str, event_id: str) -> str:
    """Deterministic, so reprocessing an event cannot duplicate its alert."""
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL, f"edgecitadel-containment:{kind}:{subject}:{event_id}"
        )
    )
