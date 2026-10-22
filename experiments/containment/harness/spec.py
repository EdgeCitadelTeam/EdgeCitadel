"""Workload specification: one containment experiment instance.

A spec is a JSON (or YAML) document. It states the declared workflow in the
simulator's vocabulary so the same instance runs in the simulator and on the
runtime; ``simexport`` converts in both directions.

```json
{
  "spec_version": 1,
  "name": "c2-late",
  "agents": [{"id": "kipp", "parent": null, "site": "core"},
             {"id": "tars", "parent": "kipp", "site": "site-a"}],
  "placement": {"core": "core", "site-a": "edge-1"},
  "effect_classes": [{"id": "final", "site": "core", "min_delay": 0.2}],
  "tasks": [{"id": "K.plan", "agent": "kipp", "session": "sK", "compute": 1.0,
             "reads": ["task"]},
            {"id": "T.plan", "agent": "tars", "session": "sT", "compute": 1.0}],
  "external_inputs": ["task"],
  "artifacts": [{"id": "A@v1", "producer": "T.plan"}],
  "messages": [{"id": "dT", "sender": "K.plan", "receiver": "T.plan", "latency": 0.3}],
  "effects": [{"id": "FINAL", "issuer": "K.agg", "class": "final", "delay": 0.5}],
  "fault": {"class": "wrong_value", "item": "A@v1"},
  "alerts": {"early": {"event": "produce", "item": "A@v1", "delay_s": 0.4},
             "late": {"event": "consume", "item": "A@v1", "count": 1, "delay_s": 0.4}},
  "rogue": {"rate": 0.0, "seed": 0},
  "shared_memory_agents": [],
  "slots": {},
  "service": {}
}
```

``shared_memory_agents``: agents whose invocations share one memory (an
inbox, a store): each continues the previous one, so an invocation that
took tainted input taints every later one of its agent (the monitor's
``shared_memory_agents``). ``slots``: concurrent invocations per agent,
where the session count would overstate them. ``service``: the
service-continuity roles and metrics of a scenario (``harness.svc``).

Semantics (as in the simulator): a task starts when every declared input is
available (its creating message or session predecessor, the artifacts it reads,
its other received messages) and not before ``release``; it computes for
``compute`` seconds with step boundaries every ``step_len``; at its end it
outputs its artifacts and messages, and issues its effects, which commit
``delay`` seconds later. Turns of one session run in list order. Every effect
class is a protected sink unless ``protected`` is false. ``rogue`` is the hidden
malfunction: at each step boundary a tainted invocation takes, with probability
``rate``, an undeclared action inside its authority scope. A task nobody
creates (no creating message, no session predecessor) is an external request:
the harness submits it to its agent at ``release``.

Runtime realization (``compile_plan``), fixed by what the runtime's lineage
records: a task is created through its *dispatcher* (its session predecessor,
else its creating message's sender, else the harness for a root task, at its
``release``), which
hands the creating input to the task's agent as a pending creation; every other
input is *pushed* by its producer as a delivery task to the consumer's agent.
The pending creation dispatches the task once its deliveries have landed, so
the task's invocation starts after every input arrived (the monitor links an
agent's invocations in start order).
Agent identities, grants and effect rules derive from the declared flows: an
agent's scope covers the agents it delivers or dispatches to and the effect
classes it issues, closed upward along the agent tree like the simulator's
witness scopes; every agent may also dispatch to itself (session successors).
A protected effect class is a ``device.command`` at the runtime, an
unprotected one (a reply to a user) a ``network.http``.
"""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any

SPEC_VERSION = 1
AGENT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
NODE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
FAULT_CLASSES = (
    "none",
    "wrong_value",
    "stale_version",
    "out_of_scope_effect",
    "lease_runaway",
    "message_delay",
    "message_duplicate",
)
# Faults on an item name its producer as the origin; the others name a task.
ITEM_FAULTS = frozenset(
    {"wrong_value", "stale_version", "message_delay", "message_duplicate"}
)
ALERT_EVENTS = ("produce", "consume", "start", "end", "fault", "at", "none")
# The trigger of a detector-triggered run: the harness raises no alert.
NO_ALERT = "none"
# Runtime effect class of a workload effect class; the spec class is the target.
RUNTIME_EFFECT_CLASS = "device.command"  # protected: an actuation
UNPROTECTED_EFFECT_CLASS = "network.http"  # unprotected: e.g. a reply to a user


def runtime_class(protected: bool) -> str:
    return RUNTIME_EFFECT_CLASS if protected else UNPROTECTED_EFFECT_CLASS


class SpecError(ValueError):
    """The document is not a valid workload spec; the message names the field."""


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Agent:
    id: str
    parent: str | None
    site: str
    label: str | None = None  # display name (e.g. the simulator's agent id)


@dataclass(frozen=True)
class EffectClass:
    id: str
    site: str
    min_delay: float
    protected: bool = True


@dataclass(frozen=True)
class Task:
    id: str
    agent: str
    session: str
    compute: float
    release: float = 0.0
    reads: tuple[str, ...] = ()
    step_len: float = 2.0
    gpu: float = 0.0  # GPU share while computing (GPU-seconds = compute * gpu)
    tokens: int = 0  # synthetic token count reported when the task completes

    @property
    def n_steps(self) -> int:
        return max(1, int(math.ceil(self.compute / self.step_len - 1e-9)))


@dataclass(frozen=True)
class Artifact:
    id: str
    producer: str


@dataclass(frozen=True)
class Message:
    id: str
    sender: str
    receiver: str
    latency: float = 0.0


@dataclass(frozen=True)
class Effect:
    id: str
    issuer: str
    cls: str
    delay: float


@dataclass(frozen=True)
class Fault:
    """``cls`` in FAULT_CLASSES; ``item`` for item faults, ``origin`` otherwise.

    ``params``: ``delay_s``/``stale_after_s`` (message_delay), ``rate_hz`` and
    ``max_effects`` (lease_runaway), ``target_class`` (out_of_scope_effect).
    """

    cls: str
    item: str | None = None
    origin: str | None = None
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AlertTrigger:
    """When the harness raises the alert, identically for every strategy.

    ``event`` ``produce``/``consume`` of ``item``, ``start``/``end`` of ``task``,
    ``fault`` (the fault's first visible action) or ``at`` (``delay_s`` after
    run start); fires ``delay_s`` (spec seconds) after the ``count``-th match.
    ``consume`` counts reader tasks that started computing with the item.
    ``none``: the harness raises no alert at all; the monitor's own detectors
    are the only alert source (a detector-triggered run, scored from the
    first detector alert on the fault's lineage: ``metrics.detection``).
    ``sim_t`` is the simulator's alert time for this instance, when known.
    ``posted_on_receipt`` (``produce`` with no delay only): the run's services
    post the alert themselves when they receive the matching produce event,
    instead of the controller after its next poll of their feed, so the alert
    does not lag the event by the controller's polling and Core lookups.
    The alert is an operator alert. Whether it also hands the monitor the
    faulty source as the root cause is the experiment's root mode (see
    ``runner``), not this field: ``designate_root`` is read for older specs
    and otherwise ignored.
    """

    event: str
    item: str | None = None
    task: str | None = None
    count: int = 1
    delay_s: float = 0.0
    designate_root: bool = True
    sim_t: float | None = None
    posted_on_receipt: bool = False

    @property
    def by_detector(self) -> bool:
        """No harness alert: the monitor's detectors raise it, if anything does."""
        return self.event == NO_ALERT


@dataclass(frozen=True)
class Rogue:
    rate: float = 0.0
    p_eff: float = 0.5
    extra_delay: float = 1.5
    p_spawn: float = 1.0
    seed: int = 0
    dyn_steps: int = 3
    dyn_step_len: float = 2.0
    new_inv_step: float = 0.5
    max_dyn: int = 200


@dataclass(frozen=True)
class Spec:
    name: str
    agents: tuple[Agent, ...]
    effect_classes: tuple[EffectClass, ...]
    tasks: tuple[Task, ...]
    external_inputs: tuple[str, ...]
    artifacts: tuple[Artifact, ...]
    messages: tuple[Message, ...]
    effects: tuple[Effect, ...]
    fault: Fault
    alerts: Mapping[str, AlertTrigger]
    rogue: Rogue = Rogue()
    placement: Mapping[str, str] = field(default_factory=dict)
    scope_extra: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    read_lat: float = 0.0
    shared_memory_agents: tuple[str, ...] = ()
    slots: Mapping[str, int] = field(default_factory=dict)
    service: Mapping[str, Any] = field(default_factory=dict)
    # Simulator-only parameters kept for sim-to-real (site control latency, ...).
    sim: Mapping[str, Any] = field(default_factory=dict)
    source: Mapping[str, Any] = field(default_factory=dict)

    # -- indexes --------------------------------------------------------------
    def task(self, task_id: str) -> Task:
        return self._index["tasks"][task_id]

    def agent(self, agent_id: str) -> Agent:
        return self._index["agents"][agent_id]

    @property
    def _index(self) -> dict[str, dict[str, Any]]:
        cached = self.__dict__.get("_cached_index")
        if cached is None:
            cached = {
                "tasks": {t.id: t for t in self.tasks},
                "agents": {a.id: a for a in self.agents},
                "artifacts": {a.id: a for a in self.artifacts},
                "messages": {m.id: m for m in self.messages},
                "effects": {e.id: e for e in self.effects},
                "classes": {c.id: c for c in self.effect_classes},
            }
            object.__setattr__(self, "_cached_index", cached)
        return cached

    def artifact(self, artifact_id: str) -> Artifact:
        return self._index["artifacts"][artifact_id]

    def message(self, message_id: str) -> Message:
        return self._index["messages"][message_id]

    def effect_class(self, class_id: str) -> EffectClass:
        return self._index["classes"][class_id]

    def with_fault(self, fault: Fault) -> Spec:
        return replace(self, fault=fault)


# ---------------------------------------------------------------------------
# Loading and saving
# ---------------------------------------------------------------------------
def _require(value: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in value:
        raise SpecError(f"{where}: missing '{key}'")
    return value[key]


def _number(value: Any, where: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SpecError(f"{where}: expected a number")
    if value < 0 or (positive and value <= 0) or math.isnan(value):
        raise SpecError(f"{where}: out of range")
    return float(value)


def _known(value: Mapping[str, Any], allowed: Iterable[str], where: str) -> None:
    unknown = set(value) - set(allowed)
    if unknown:
        raise SpecError(f"{where}: unknown fields {sorted(unknown)}")


def from_dict(document: Mapping[str, Any]) -> Spec:
    if not isinstance(document, Mapping):
        raise SpecError("spec: expected an object")
    if document.get("spec_version") != SPEC_VERSION:
        raise SpecError(f"spec_version must be {SPEC_VERSION}")
    _known(
        document,
        {
            "spec_version",
            "name",
            "agents",
            "effect_classes",
            "tasks",
            "external_inputs",
            "artifacts",
            "messages",
            "effects",
            "fault",
            "alerts",
            "rogue",
            "placement",
            "scope_extra",
            "read_lat",
            "sim",
            "source",
            "shared_memory_agents",
            "slots",
            "service",
        },
        "spec",
    )

    def items(key: str) -> list[Mapping[str, Any]]:
        value = document.get(key, [])
        if not isinstance(value, list) or not all(
            isinstance(v, Mapping) for v in value
        ):
            raise SpecError(f"{key}: expected a list of objects")
        return value

    agents = []
    for i, a in enumerate(items("agents")):
        where = f"agents[{i}]"
        _known(a, {"id", "parent", "site", "label"}, where)
        agents.append(
            Agent(
                str(_require(a, "id", where)),
                a.get("parent"),
                str(_require(a, "site", where)),
                a.get("label"),
            )
        )
    classes = []
    for i, c in enumerate(items("effect_classes")):
        where = f"effect_classes[{i}]"
        _known(c, {"id", "site", "min_delay", "protected"}, where)
        classes.append(
            EffectClass(
                str(_require(c, "id", where)),
                str(_require(c, "site", where)),
                _number(c.get("min_delay", 0.0), f"{where}.min_delay"),
                bool(c.get("protected", True)),
            )
        )
    tasks = []
    for i, t in enumerate(items("tasks")):
        where = f"tasks[{i}]"
        _known(
            t,
            {
                "id",
                "agent",
                "session",
                "compute",
                "release",
                "reads",
                "step_len",
                "gpu",
                "tokens",
            },
            where,
        )
        reads = t.get("reads", [])
        if not isinstance(reads, list) or not all(isinstance(r, str) for r in reads):
            raise SpecError(f"{where}.reads: expected a list of ids")
        tasks.append(
            Task(
                str(_require(t, "id", where)),
                str(_require(t, "agent", where)),
                str(_require(t, "session", where)),
                _number(
                    _require(t, "compute", where), f"{where}.compute", positive=True
                ),
                _number(t.get("release", 0.0), f"{where}.release"),
                tuple(reads),
                _number(t.get("step_len", 2.0), f"{where}.step_len", positive=True),
                _number(t.get("gpu", 0.0), f"{where}.gpu"),
                int(t.get("tokens", 0)),
            )
        )
    externals = document.get("external_inputs", [])
    if not isinstance(externals, list) or not all(
        isinstance(e, str) for e in externals
    ):
        raise SpecError("external_inputs: expected a list of ids")
    artifacts = []
    for i, a in enumerate(items("artifacts")):
        where = f"artifacts[{i}]"
        _known(a, {"id", "producer"}, where)
        artifacts.append(
            Artifact(str(_require(a, "id", where)), str(_require(a, "producer", where)))
        )
    messages = []
    for i, m in enumerate(items("messages")):
        where = f"messages[{i}]"
        _known(m, {"id", "sender", "receiver", "latency"}, where)
        messages.append(
            Message(
                str(_require(m, "id", where)),
                str(_require(m, "sender", where)),
                str(_require(m, "receiver", where)),
                _number(m.get("latency", 0.0), f"{where}.latency"),
            )
        )
    effects = []
    for i, e in enumerate(items("effects")):
        where = f"effects[{i}]"
        _known(e, {"id", "issuer", "class", "delay"}, where)
        effects.append(
            Effect(
                str(_require(e, "id", where)),
                str(_require(e, "issuer", where)),
                str(_require(e, "class", where)),
                _number(e.get("delay", 0.0), f"{where}.delay"),
            )
        )
    raw_fault = document.get("fault", {"class": "none"})
    if not isinstance(raw_fault, Mapping):
        raise SpecError("fault: expected an object")
    _known(raw_fault, {"class", "item", "origin", "params"}, "fault")
    params = raw_fault.get("params", {})
    if not isinstance(params, Mapping):
        raise SpecError("fault.params: expected an object")
    fault = Fault(
        str(raw_fault.get("class", "none")),
        raw_fault.get("item"),
        raw_fault.get("origin"),
        dict(params),
    )
    raw_alerts = document.get("alerts", {})
    if not isinstance(raw_alerts, Mapping):
        raise SpecError("alerts: expected an object of named triggers")
    alerts = {}
    for name, a in raw_alerts.items():
        where = f"alerts.{name}"
        if not isinstance(a, Mapping):
            raise SpecError(f"{where}: expected an object")
        _known(
            a,
            {
                "event",
                "item",
                "task",
                "count",
                "delay_s",
                "designate_root",
                "sim_t",
                "posted_on_receipt",
            },
            where,
        )
        count = a.get("count", 1)
        if type(count) is not int or count < 0:
            raise SpecError(f"{where}.count: expected a non-negative integer")
        alerts[str(name)] = AlertTrigger(
            str(_require(a, "event", where)),
            a.get("item"),
            a.get("task"),
            count,
            _number(a.get("delay_s", 0.0), f"{where}.delay_s"),
            bool(a.get("designate_root", True)),
            None if a.get("sim_t") is None else _number(a["sim_t"], f"{where}.sim_t"),
            bool(a.get("posted_on_receipt", False)),
        )
    raw_rogue = document.get("rogue", {})
    if not isinstance(raw_rogue, Mapping):
        raise SpecError("rogue: expected an object")
    _known(raw_rogue, Rogue.__dataclass_fields__, "rogue")
    rogue = Rogue(**dict(raw_rogue))
    for name in ("rate", "p_eff", "p_spawn"):
        value = getattr(rogue, name)
        if _number(value, f"rogue.{name}") > 1:
            raise SpecError(f"rogue.{name}: must be a probability")
    placement = document.get("placement", {})
    scope_extra = document.get("scope_extra", {})
    if not isinstance(placement, Mapping) or not isinstance(scope_extra, Mapping):
        raise SpecError("placement and scope_extra must be objects")
    shared = document.get("shared_memory_agents", [])
    if not isinstance(shared, list) or not all(isinstance(a, str) for a in shared):
        raise SpecError("shared_memory_agents: expected a list of agent ids")
    slots = document.get("slots", {})
    if not isinstance(slots, Mapping) or not all(
        type(n) is int and n >= 1 for n in slots.values()
    ):
        raise SpecError("slots: expected positive integers per agent")
    service = document.get("service", {})
    if not isinstance(service, Mapping):
        raise SpecError("service: expected an object")
    spec = Spec(
        name=str(_require(document, "name", "spec")),
        agents=tuple(agents),
        effect_classes=tuple(classes),
        tasks=tuple(tasks),
        external_inputs=tuple(externals),
        artifacts=tuple(artifacts),
        messages=tuple(messages),
        effects=tuple(effects),
        fault=fault,
        alerts=alerts,
        rogue=rogue,
        placement=dict(placement),
        scope_extra={k: tuple(v) for k, v in scope_extra.items()},
        read_lat=_number(document.get("read_lat", 0.0), "read_lat"),
        sim=dict(document.get("sim", {})),
        source=dict(document.get("source", {})),
        shared_memory_agents=tuple(shared),
        slots=dict(slots),
        service=dict(service),
    )
    validate(spec)
    return spec


def to_dict(spec: Spec) -> dict[str, Any]:
    def task(t: Task) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": t.id,
            "agent": t.agent,
            "session": t.session,
            "compute": t.compute,
        }
        if t.release:
            out["release"] = t.release
        if t.reads:
            out["reads"] = list(t.reads)
        if t.step_len != 2.0:
            out["step_len"] = t.step_len
        if t.gpu:
            out["gpu"] = t.gpu
        if t.tokens:
            out["tokens"] = t.tokens
        return out

    def alert(a: AlertTrigger) -> dict[str, Any]:
        if a.by_detector:
            return {"event": a.event}
        out = {k: v for k, v in asdict(a).items() if v is not None}
        if out.get("count") == 1:
            del out["count"]
        if not out.get("posted_on_receipt"):
            out.pop("posted_on_receipt", None)
        return out

    fault: dict[str, Any] = {"class": spec.fault.cls}
    if spec.fault.item is not None:
        fault["item"] = spec.fault.item
    if spec.fault.origin is not None:
        fault["origin"] = spec.fault.origin
    if spec.fault.params:
        fault["params"] = dict(spec.fault.params)
    document: dict[str, Any] = {
        "spec_version": SPEC_VERSION,
        "name": spec.name,
        "agents": [
            {k: v for k, v in asdict(a).items() if k != "label" or v is not None}
            for a in spec.agents
        ],
        "placement": dict(spec.placement),
        "effect_classes": [asdict(c) for c in spec.effect_classes],
        "tasks": [task(t) for t in spec.tasks],
        "external_inputs": list(spec.external_inputs),
        "artifacts": [asdict(a) for a in spec.artifacts],
        "messages": [asdict(m) for m in spec.messages],
        "effects": [
            {"id": e.id, "issuer": e.issuer, "class": e.cls, "delay": e.delay}
            for e in spec.effects
        ],
        "fault": fault,
        "alerts": {name: alert(a) for name, a in spec.alerts.items()},
        "rogue": asdict(spec.rogue),
    }
    if spec.scope_extra:
        document["scope_extra"] = {k: list(v) for k, v in spec.scope_extra.items()}
    if spec.read_lat:
        document["read_lat"] = spec.read_lat
    if spec.sim:
        document["sim"] = dict(spec.sim)
    if spec.source:
        document["source"] = dict(spec.source)
    if spec.shared_memory_agents:
        document["shared_memory_agents"] = list(spec.shared_memory_agents)
    if spec.slots:
        document["slots"] = dict(spec.slots)
    if spec.service:
        document["service"] = dict(spec.service)
    return document


def load(path: str | Path) -> Spec:
    path = Path(path)
    text = path.read_text()
    if path.suffix in {".yaml", ".yml"}:
        import yaml  # optional dependency, only for YAML specs

        document = yaml.safe_load(text)
    else:
        document = json.loads(text)
    return from_dict(document)


def save(spec: Spec, path: str | Path) -> None:
    Path(path).write_text(json.dumps(to_dict(spec), indent=1) + "\n")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate(spec: Spec) -> None:
    def unique(ids: Iterable[str], what: str) -> set[str]:
        seen: set[str] = set()
        for x in ids:
            if x in seen:
                raise SpecError(f"duplicate {what} id '{x}'")
            seen.add(x)
        return seen

    agents = unique((a.id for a in spec.agents), "agent")
    for a in spec.agents:
        if not AGENT_ID.fullmatch(a.id):
            raise SpecError(f"agent id '{a.id}' is not a runtime agent identity")
        if a.parent is not None and a.parent not in agents:
            raise SpecError(f"agent '{a.id}': unknown parent '{a.parent}'")
    roots = [a.id for a in spec.agents if a.parent is None]
    if len(roots) != 1:
        raise SpecError("the agent tree needs exactly one root")
    for a in spec.agents:  # the parent chain must reach the root (no cycles)
        seen, cur = set(), a.id
        while cur is not None:
            if cur in seen:
                raise SpecError(f"agent tree has a cycle at '{cur}'")
            seen.add(cur)
            cur = spec.agent(cur).parent
    sites = {a.site for a in spec.agents} | {c.site for c in spec.effect_classes}
    if spec.placement:
        missing = sites - set(spec.placement)
        if missing:
            raise SpecError(f"placement: no node for sites {sorted(missing)}")
        for node in spec.placement.values():
            if not NODE_ID.fullmatch(str(node)):
                raise SpecError(f"placement: invalid node id '{node}'")
    classes = unique((c.id for c in spec.effect_classes), "effect class")
    tasks = unique((t.id for t in spec.tasks), "task")
    for t in spec.tasks:
        if t.agent not in agents:
            raise SpecError(f"task '{t.id}': unknown agent '{t.agent}'")
    externals = unique(spec.external_inputs, "external input")
    artifacts = unique((a.id for a in spec.artifacts), "artifact")
    if externals & artifacts:
        raise SpecError(
            f"ids used as artifact and external input: {externals & artifacts}"
        )
    for a in spec.artifacts:
        if a.producer not in tasks:
            raise SpecError(f"artifact '{a.id}': unknown producer '{a.producer}'")
    for t in spec.tasks:
        for r in t.reads:
            if r not in artifacts and r not in externals:
                raise SpecError(f"task '{t.id}': reads unknown item '{r}'")
            if r in artifacts and spec.artifact(r).producer == t.id:
                raise SpecError(f"task '{t.id}' reads its own artifact '{r}'")
    messages = unique((m.id for m in spec.messages), "message")
    if messages & (artifacts | externals):
        raise SpecError("message ids must differ from artifact ids")
    for m in spec.messages:
        for end in (m.sender, m.receiver):
            if end not in tasks:
                raise SpecError(f"message '{m.id}': unknown task '{end}'")
        if m.sender == m.receiver:
            raise SpecError(f"message '{m.id}' is sent to its own sender")
    unique((e.id for e in spec.effects), "effect")
    for e in spec.effects:
        if e.issuer not in tasks:
            raise SpecError(f"effect '{e.id}': unknown issuer '{e.issuer}'")
        if e.cls not in classes:
            raise SpecError(f"effect '{e.id}': unknown class '{e.cls}'")
        if e.delay < spec.effect_class(e.cls).min_delay - 1e-12:
            raise SpecError(f"effect '{e.id}': delay below its class minimum")
    for agent in [*spec.shared_memory_agents, *spec.slots]:
        if agent not in agents:
            raise SpecError(f"unknown agent '{agent}' in shared_memory_agents or slots")
    for agent, extra in spec.scope_extra.items():
        if agent not in agents:
            raise SpecError(f"scope_extra: unknown agent '{agent}'")
        for target in extra:
            if target not in agents and target not in classes:
                raise SpecError(f"scope_extra.{agent}: unknown target '{target}'")
    derived = derive(spec)  # dispatchers exist and the dependency graph is acyclic
    _validate_fault(spec, derived)
    for name, alert in spec.alerts.items():
        where = f"alerts.{name}"
        if alert.event not in ALERT_EVENTS:
            raise SpecError(f"{where}.event: one of {ALERT_EVENTS}")
        if alert.event in {"produce", "consume"} and alert.item not in (
            artifacts | messages
        ):
            raise SpecError(f"{where}.item: unknown item")
        if alert.event in {"start", "end"} and alert.task not in tasks:
            raise SpecError(f"{where}.task: unknown task")
        if alert.posted_on_receipt and (alert.event != "produce" or alert.delay_s):
            raise SpecError(
                f"{where}.posted_on_receipt: only for a produce event with no delay"
            )
        if alert.by_detector and (
            alert.item or alert.task or alert.delay_s or alert.count != 1
        ):
            raise SpecError(f"{where}: an alert left to the detectors has no trigger")


def _validate_fault(spec: Spec, derived: Derived) -> None:
    fault = spec.fault
    if fault.cls not in FAULT_CLASSES:
        raise SpecError(f"fault.class: one of {FAULT_CLASSES}")
    if fault.cls == "none":
        return
    if fault.cls in ITEM_FAULTS:
        if fault.item not in derived.producer:
            raise SpecError("fault.item: must name an artifact or message")
        if fault.cls in {"message_delay", "message_duplicate"} and not any(
            d.item == fault.item for d in derived.deliveries
        ):
            raise SpecError("fault.item: a transport fault needs a pushed item")
    elif fault.origin not in {t.id for t in spec.tasks}:
        raise SpecError("fault.origin: must name a task")
    # The boundary faults must reach their boundary, or the run has no fault
    # behaviour to detect: an attempted class outside the origin's grant, and
    # a lease (only a created task carries one) on an agent with a protected
    # class to actuate.
    if fault.cls == "out_of_scope_effect":
        agent = spec.task(str(fault.origin)).agent
        if fault.params.get("target_class") in derived.effect_grant[agent]:
            raise SpecError("fault.params.target_class: inside the origin's scope")
    if fault.cls == "lease_runaway":
        origin = str(fault.origin)
        if derived.creation[origin].dispatcher is None:
            raise SpecError("fault.origin: a lease runaway needs a created task")
        if not derived.scope_classes[spec.task(origin).agent]:
            raise SpecError("fault.origin: its agent has no protected class to actuate")
    if fault.cls == "message_delay":
        _number(fault.params.get("delay_s"), "fault.params.delay_s", positive=True)


# ---------------------------------------------------------------------------
# Derived relations (the single owner of the realization rules)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Delivery:
    """A non-creating input pushed by its producer task to a consumer task."""

    item: str
    source: str  # producer task
    target: str  # consumer task


@dataclass(frozen=True)
class Creation:
    """How a task is created: by ``dispatcher`` (None: submitted by the harness).

    ``via`` is ``session`` (the predecessor hands over its session state),
    ``message`` (the creating message ``item``) or ``root``.
    """

    task: str
    dispatcher: str | None
    via: str
    item: str | None


@dataclass(frozen=True)
class Derived:
    creation: Mapping[str, Creation]
    deliveries: tuple[Delivery, ...]
    deliveries_in: Mapping[str, tuple[Delivery, ...]]
    deliveries_out: Mapping[str, tuple[Delivery, ...]]
    creates: Mapping[str, tuple[str, ...]]  # dispatcher -> tasks it creates
    session_pred: Mapping[str, str | None]
    session_succ: Mapping[str, str | None]
    producer: Mapping[str, str]  # artifact or message id -> producing task
    outputs: Mapping[str, tuple[str, ...]]  # task -> artifacts and messages it outputs
    readers: Mapping[str, tuple[str, ...]]  # item -> tasks that take it as an input
    effects_of: Mapping[str, tuple[Effect, ...]]
    scope_agents: Mapping[str, tuple[str, ...]]  # simulator witness scope (consumers)
    scope_classes: Mapping[str, tuple[str, ...]]  # protected effect classes in scope
    grants: Mapping[str, tuple[str, ...]]  # runtime outbound agents
    effect_grant: Mapping[str, tuple[str, ...]]  # all effect classes in scope
    roots: tuple[str, ...]  # tasks the harness submits


def session_item(task_id: str) -> str:
    return f"session:{task_id}"


def derive(spec: Spec) -> Derived:
    cached = spec.__dict__.get("_cached_derived")
    if cached is not None:
        return cached
    sessions: dict[str, list[str]] = defaultdict(list)
    for t in spec.tasks:
        sessions[t.session].append(t.id)
    pred: dict[str, str | None] = {}
    succ: dict[str, str | None] = {}
    for ids in sessions.values():
        for i, tid in enumerate(ids):
            pred[tid] = ids[i - 1] if i else None
            succ[tid] = ids[i + 1] if i + 1 < len(ids) else None
        if len({spec.task(t).agent for t in ids}) != 1:
            raise SpecError(f"session of '{ids[0]}' spans several agents")
    received: dict[str, list[Message]] = defaultdict(list)
    for m in spec.messages:
        received[m.receiver].append(m)
    creation: dict[str, Creation] = {}
    for t in spec.tasks:
        if pred[t.id] is not None:
            creation[t.id] = Creation(
                t.id, pred[t.id], "session", session_item(pred[t.id])
            )
            continue
        if received[t.id]:
            parent = spec.agent(t.agent).parent
            creating = next(
                (m for m in received[t.id] if spec.task(m.sender).agent == parent),
                received[t.id][0],
            )
            creation[t.id] = Creation(t.id, creating.sender, "message", creating.id)
            continue
        creation[t.id] = Creation(t.id, None, "root", None)
    # Tasks nobody creates: external requests the harness submits.
    roots = tuple(t for t, c in creation.items() if c.dispatcher is None)
    producer: dict[str, str] = {a.id: a.producer for a in spec.artifacts}
    producer.update({m.id: m.sender for m in spec.messages})
    deliveries: list[Delivery] = []
    readers: dict[str, list[str]] = defaultdict(list)
    for t in spec.tasks:
        for r in t.reads:
            readers[r].append(t.id)
            if r in producer:
                deliveries.append(Delivery(r, producer[r], t.id))
        for m in received[t.id]:
            readers[m.id].append(t.id)
            if creation[t.id].item != m.id:
                deliveries.append(Delivery(m.id, m.sender, t.id))
    outputs: dict[str, list[str]] = defaultdict(list)
    for a in spec.artifacts:
        outputs[a.producer].append(a.id)
    for m in spec.messages:
        outputs[m.sender].append(m.id)
    creates: dict[str, list[str]] = defaultdict(list)
    for c in creation.values():
        if c.dispatcher is not None:
            creates[c.dispatcher].append(c.task)
    effects_of: dict[str, list[Effect]] = defaultdict(list)
    for e in spec.effects:
        effects_of[e.issuer].append(e)
    d_in: dict[str, list[Delivery]] = defaultdict(list)
    d_out: dict[str, list[Delivery]] = defaultdict(list)
    for d in deliveries:
        d_in[d.target].append(d)
        d_out[d.source].append(d)
    _check_acyclic(spec, creation, deliveries)
    scope_agents, scope_classes, grants, effect_grant = _scopes(
        spec, creation, deliveries, effects_of
    )
    derived = Derived(
        creation=creation,
        deliveries=tuple(deliveries),
        deliveries_in={k: tuple(v) for k, v in d_in.items()},
        deliveries_out={k: tuple(v) for k, v in d_out.items()},
        creates={k: tuple(v) for k, v in creates.items()},
        session_pred=pred,
        session_succ=succ,
        producer=producer,
        outputs={k: tuple(v) for k, v in outputs.items()},
        readers={k: tuple(v) for k, v in readers.items()},
        effects_of={k: tuple(v) for k, v in effects_of.items()},
        scope_agents=scope_agents,
        scope_classes=scope_classes,
        grants=grants,
        effect_grant=effect_grant,
        roots=roots,
    )
    object.__setattr__(spec, "_cached_derived", derived)
    return derived


def _check_acyclic(
    spec: Spec, creation: Mapping[str, Creation], deliveries: Iterable[Delivery]
) -> None:
    deps: dict[str, set[str]] = {t.id: set() for t in spec.tasks}
    for c in creation.values():
        if c.dispatcher is not None:
            deps[c.task].add(c.dispatcher)
    for d in deliveries:
        deps[d.target].add(d.source)
    indegree = {t: len(v) for t, v in deps.items()}
    users: dict[str, list[str]] = defaultdict(list)
    for t, v in deps.items():
        for s in v:
            users[s].append(t)
    queue = deque(t for t, n in indegree.items() if n == 0)
    seen = 0
    while queue:
        t = queue.popleft()
        seen += 1
        for u in users[t]:
            indegree[u] -= 1
            if indegree[u] == 0:
                queue.append(u)
    if seen != len(deps):
        raise SpecError("task dependencies contain a cycle")


def _scopes(
    spec: Spec,
    creation: Mapping[str, Creation],
    deliveries: Iterable[Delivery],
    effects_of: Mapping[str, list[Effect]],
) -> tuple[dict, dict, dict, dict]:
    """Least-privilege scopes from declared flows, closed upward (simulator rule).

    Returns (consumer agents, protected classes, runtime recipients, effect
    classes) per agent. Consumer agents and classes are ordered as the
    simulator orders its witness scope (by its own agent ids, the labels), so
    the malfunction draws pick the same targets on both sides.
    """
    artifact_ids = {a.id for a in spec.artifacts}
    class_ids = {c.id for c in spec.effect_classes}
    own_agents: dict[str, set[str]] = {a.id: set() for a in spec.agents}
    own_classes: dict[str, set[str]] = {a.id: set() for a in spec.agents}
    for m in spec.messages:
        s, r = spec.task(m.sender).agent, spec.task(m.receiver).agent
        if s != r:
            own_agents[s].add(r)
    for t in spec.tasks:
        for r in t.reads:
            if r in artifact_ids:
                p = spec.task(spec.artifact(r).producer).agent
                if p != t.agent:
                    own_agents[p].add(t.agent)
    for issuer, effects in effects_of.items():
        for e in effects:
            own_classes[spec.task(issuer).agent].add(e.cls)
    for agent, extra in spec.scope_extra.items():
        for target in extra:
            (own_classes if target in class_ids else own_agents)[agent].add(target)
    children: dict[str, list[str]] = defaultdict(list)
    for a in spec.agents:
        if a.parent is not None:
            children[a.parent].append(a.id)
    agents_in: dict[str, set[str]] = {}
    classes_in: dict[str, set[str]] = {}

    def close(agent: str) -> None:
        agents_in[agent] = set(own_agents[agent])
        classes_in[agent] = set(own_classes[agent])
        for child in children[agent]:
            close(child)
            agents_in[agent] |= agents_in[child]
            classes_in[agent] |= classes_in[child]

    close(next(a.id for a in spec.agents if a.parent is None))

    def label(agent: str) -> str:
        return spec.agent(agent).label or agent

    protected = {c.id for c in spec.effect_classes if c.protected}
    scope_agents = {a: tuple(sorted(v, key=label)) for a, v in agents_in.items()}
    scope_protected = {
        a: tuple(sorted(c for c in v if c in protected)) for a, v in classes_in.items()
    }
    effect_classes = {a: tuple(sorted(v)) for a, v in classes_in.items()}
    # The runtime additionally needs the session self-dispatch; delegation and
    # delivery targets are message and read consumers, so already in scope.
    grants = {a: tuple(sorted({*v, a})) for a, v in agents_in.items()}
    for c in creation.values():
        if c.dispatcher is not None:
            s, r = spec.task(c.dispatcher).agent, spec.task(c.task).agent
            if r not in grants[s]:
                raise SpecError(f"dispatch {s}->{r} is outside the derived scope")
    for d in deliveries:
        s, r = spec.task(d.source).agent, spec.task(d.target).agent
        if r not in grants[s]:
            raise SpecError(f"delivery {s}->{r} is outside the derived scope")
    return scope_agents, scope_protected, grants, effect_classes


# ---------------------------------------------------------------------------
# Nominal schedule (dependency semantics, zero runtime overhead)
# ---------------------------------------------------------------------------
def nominal_schedule(spec: Spec) -> tuple[dict[str, float], dict[str, float]]:
    """Start and end of every task with message latencies and read latency.

    Same rule as the simulator's ``nominal_schedule``; used for alert times on
    the simulator side, timeouts and the expected makespan.
    """
    derived = derive(spec)
    latency = {m.id: m.latency for m in spec.messages}
    start: dict[str, float] = {}
    end: dict[str, float] = {}
    order = _topological(spec, derived)
    for tid in order:
        t = spec.task(tid)
        s = t.release
        c = derived.creation[tid]
        if c.via == "session":
            s = max(s, end[c.dispatcher])
        elif c.via == "message":
            s = max(s, end[c.dispatcher] + latency[c.item])
        for d in derived.deliveries_in.get(tid, ()):
            extra = latency[d.item] if d.item in latency else spec.read_lat
            s = max(s, end[d.source] + extra)
        start[tid], end[tid] = s, s + t.compute
    return start, end


def makespan(spec: Spec) -> float:
    start, end = nominal_schedule(spec)
    finish = 0.0
    for t in spec.tasks:
        finish = max(finish, end[t.id])
        for e in derive(spec).effects_of.get(t.id, ()):
            finish = max(finish, end[t.id] + e.delay)
    return finish


def _topological(spec: Spec, derived: Derived) -> list[str]:
    deps: dict[str, set[str]] = {t.id: set() for t in spec.tasks}
    for c in derived.creation.values():
        if c.dispatcher is not None:
            deps[c.task].add(c.dispatcher)
    for d in derived.deliveries:
        deps[d.target].add(d.source)
    out: list[str] = []
    done: set[str] = set()
    for t in spec.tasks:  # stable: list order among ready tasks
        stack = [(t.id, False)]
        while stack:
            node, expanded = stack.pop()
            if node in done:
                continue
            if expanded:
                done.add(node)
                out.append(node)
                continue
            stack.append((node, True))
            stack.extend((d, False) for d in sorted(deps[node]) if d not in done)
    return out


def node_of_agent(spec: Spec, agent_id: str) -> str:
    site = spec.agent(agent_id).site
    return str(spec.placement.get(site, site))


# ---------------------------------------------------------------------------
# Worker plan: everything a det-worker needs, compiled once per run
# ---------------------------------------------------------------------------
def effect_target(cls: str, effect_id: str) -> str:
    return f"actuator://{cls}/{effect_id}"


def compile_plan(
    spec: Spec,
    *,
    run_id: str,
    time_scale: float,
    effect_sink: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The JSON plan workers execute; derived relations are resolved here only.

    ``effect_sink`` is where agentd's gateway performs the workers' effects
    (``det_worker.effects``; default the run's HTTP ledger).
    """
    derived = derive(spec)
    fault = spec.fault
    origin = fault_origin(spec)
    tasks: dict[str, Any] = {}
    for t in spec.tasks:
        c = derived.creation[t.id]
        creates = []
        for child in derived.creates.get(t.id, ()):
            cc = derived.creation[child]
            entry: dict[str, Any] = {
                "task": child,
                "agent": spec.task(child).agent,
                "via": cc.via,
                "item": cc.item,
            }
            if child == origin and fault.cls == "lease_runaway":
                # The faulty task's lease fits exactly the declared effects of
                # the work it creates; its children inherit the remainder.
                budget = fault.params.get(
                    "max_effects", _subtree_effects(derived, child)
                )
                entry["lease"] = {"max_effects": int(budget)}
            creates.append(entry)
        tasks[t.id] = {
            "agent": t.agent,
            "session": t.session,
            "compute": t.compute,
            "release": t.release,
            "n_steps": t.n_steps,
            "gpu": t.gpu,
            "tokens": t.tokens,
            "creation": {"via": c.via, "item": c.item, "dispatcher": c.dispatcher},
            "deliveries_in": [
                {"item": d.item, "source": d.source}
                for d in derived.deliveries_in.get(t.id, ())
            ],
            "seeded": [r for r in t.reads if r in spec.external_inputs],
            "outputs": list(derived.outputs.get(t.id, ())),
            "deliveries_out": [
                {"item": d.item, "target": d.target, "agent": spec.task(d.target).agent}
                for d in derived.deliveries_out.get(t.id, ())
            ],
            "creates": creates,
            "effects": [
                {
                    "id": e.id,
                    "class": e.cls,
                    "delay": e.delay,
                    "target": effect_target(e.cls, e.id),
                }
                for e in derived.effects_of.get(t.id, ())
            ],
        }
    shared = set(spec.shared_memory_agents)
    return {
        "plan_version": 1,
        "run_id": run_id,
        "effect_sink": dict(effect_sink or {"adapter": "http"}),
        "spec": spec.name,
        "time_scale": time_scale,
        "root_agent": next(a.id for a in spec.agents if a.parent is None),
        "roots": list(derived.roots),
        "agents": {
            a.id: {
                "label": a.label or a.id,
                "node": node_of_agent(spec, a.id),
                "tasks": [t.id for t in spec.tasks if t.agent == a.id],
                "scope_agents": list(derived.scope_agents[a.id]),
                "scope_classes": list(derived.scope_classes[a.id]),
                "shared_memory": a.id in shared,
            }
            for a in spec.agents
        },
        "tasks": tasks,
        "effect_classes": {
            c.id: {
                "min_delay": c.min_delay,
                "protected": c.protected,
                "runtime_class": runtime_class(c.protected),
            }
            for c in spec.effect_classes
        },
        "external_inputs": list(spec.external_inputs),
        "fault": {
            "class": fault.cls,
            "item": fault.item,
            "origin": origin,
            "params": dict(fault.params),
        },
        "rogue": asdict(spec.rogue),
    }


def _subtree_effects(derived: Derived, task_id: str) -> int:
    count, stack = 0, [task_id]
    while stack:
        current = stack.pop()
        count += len(derived.effects_of.get(current, ()))
        stack.extend(derived.creates.get(current, ()))
    return count


def fault_origin(spec: Spec) -> str | None:
    """The faulty invocation: the item's producer, or the named task."""
    fault = spec.fault
    if fault.cls == "none":
        return None
    if fault.cls in ITEM_FAULTS:
        return derive(spec).producer[fault.item]
    return fault.origin
