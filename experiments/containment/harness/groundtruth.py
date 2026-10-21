"""Ground truth computed from the workload and the run's own records.

Nothing here reads the tracer or the monitor. Two sources:

- the spec and its fault: fault-free contents of every item (what a correct
  run outputs), and the declared taint closure (every task that depends on the
  fault's seeds under the declared plan, as the simulator's ``declared_taint``);
- the services' records of the run: worker events (what each invocation
  consumed, produced and dispatched, its fault labels, pauses) and the effect
  ledger: the worker's attempted / scheduled / denied / sink_failed records and
  the sink's committed ones (the actuator, reached only through agentd's effect
  gateway), with content. An effect the gateway scheduled and later refused
  stays ``pending`` here; the gateway's own decisions are in
  ``gateway_effects.jsonl``.

Whether a committed effect is tainted is decided by its lineage
(``commit_lineage``), not by the labels the sink copies from the worker's
``attempted`` record: the sink entry names the runtime task that issued it
(the gateway's post, or Core's gateway record of the same commit), the run's
records map that task to its planned task, and the fault spec with the
observed contamination says whether that task is tainted. A commit with no
``attempted`` record is still classified, and counted apart.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ._worker import content as det_content
from .spec import Spec, derive, fault_origin, session_item


# ---------------------------------------------------------------------------
# From the spec
# ---------------------------------------------------------------------------
def expected_contents(spec: Spec) -> dict[str, dict[str, Any]]:
    """Fault-free content of every output item, session state and effect command."""
    derived = derive(spec)
    contents: dict[str, dict[str, Any]] = {
        x: det_content.external_content(x) for x in spec.external_inputs
    }
    for tid in _order(spec):
        t = spec.task(tid)
        creation = derived.creation[tid]
        inputs = []
        if creation.item is not None:
            inputs.append(contents[creation.item])
        inputs += [contents[d.item] for d in derived.deliveries_in.get(tid, ())]
        inputs += [contents[r] for r in t.reads if r in spec.external_inputs]
        for item in [*derived.outputs.get(tid, ()), session_item(tid)]:
            contents[item] = det_content.item_content(item, inputs, ())
        for e in derived.effects_of.get(tid, ()):
            contents[e.id] = det_content.item_content(
                e.id, [contents[session_item(tid)]], ()
            )
    return contents


def _order(spec: Spec) -> list[str]:
    derived = derive(spec)
    deps: dict[str, set[str]] = {t.id: set() for t in spec.tasks}
    for c in derived.creation.values():
        if c.dispatcher is not None:
            deps[c.task].add(c.dispatcher)
    for d in derived.deliveries:
        deps[d.target].add(d.source)
    out, done = [], set()
    pending = [t.id for t in spec.tasks]
    while pending:
        ready = [t for t in pending if deps[t] <= done]
        for t in ready:
            out.append(t)
            done.add(t)
        pending = [t for t in pending if t not in done]
    return out


def dependents(spec: Spec) -> dict[str, set[str]]:
    """Task -> tasks that take one of its outputs (creation, push or session)."""
    derived = derive(spec)
    users: dict[str, set[str]] = defaultdict(set)
    for c in derived.creation.values():
        if c.dispatcher is not None:
            users[c.dispatcher].add(c.task)
    for d in derived.deliveries:
        users[d.source].add(d.target)
    return users


def declared_closure(spec: Spec, seeds: Iterable[str]) -> set[str]:
    users = dependents(spec)
    out = set(seeds)
    queue = deque(out)
    while queue:
        for user in users.get(queue.popleft(), ()):
            if user not in out:
                out.add(user)
                queue.append(user)
    return out


def fault_seeds(spec: Spec) -> set[str]:
    """Tasks the fault taints directly.

    Content faults taint the faulty invocation; transport faults (a delayed or
    duplicated item) taint the readers that consume the corrupted copy.
    """
    fault = spec.fault
    if fault.cls == "none":
        return set()
    if fault.cls in {"message_delay", "message_duplicate"}:
        readers = derive(spec).readers.get(fault.item, ())
        if fault.cls == "message_delay" and fault.params.get("stale_after_s") is None:
            return set()  # a pure delay changes timing, not content
        return set(readers)
    origin = fault_origin(spec)
    return {origin} if origin else set()


def protected_classes(spec: Spec) -> set[str]:
    return {c.id for c in spec.effect_classes if c.protected}


def tainted_tasks(spec: Spec, records: RunRecords) -> set[str]:
    """Planned tasks whose work carries the fault, by lineage: the fault's
    seeds, every task the worker events show computing on fault-labelled
    content (or polluted), and their declared dependents."""
    found = fault_seeds(spec) | {
        t for t, rec in records.tasks.items() if rec.tainted_at is not None
    }
    return declared_closure(spec, found) if found else set()


def runtime_tasks(
    events: Iterable[Mapping[str, Any]], marks: Iterable[Mapping[str, Any]] = ()
) -> dict[str, str]:
    """Runtime task id -> the planned task (or spawned invocation) it ran, from
    the worker events and the harness's request marks."""
    out: dict[str, str] = {}
    for record in [*marks, *events]:
        rt, task = record.get("rt"), record.get("task")
        if rt and task and record.get("inv_kind", "task") in {"task", "rogue"}:
            out.setdefault(str(rt), str(task))
    return out


def _effect_class(spec: Spec, effect_id: str, target: Any) -> str | None:
    """The workload class of an effect: the spec's, else its ledger target
    (``actuator://<class>/<effect>``)."""
    declared = {e.id: e.cls for e in spec.effects}
    if effect_id in declared:
        return declared[effect_id]
    if isinstance(target, str) and target.startswith("actuator://"):
        return target.removeprefix("actuator://").split("/", 1)[0] or None
    return None


def commit_lineage(
    spec: Spec,
    records: RunRecords,
    gateway: Iterable[Mapping[str, Any]] = (),
    rt_tasks: Mapping[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Every committed sink entry classified by lineage.

    The issuer is the planned task of the runtime task the gateway committed
    for (the sink entry's gateway post, else Core's gateway record of the same
    commit); only when neither names one, the worker's identity the effect
    carried to the sink. ``tainted`` is None when no issuer is known.
    """
    rt_tasks = runtime_tasks(records.events) if rt_tasks is None else rt_tasks
    core = {str(r["effect_id"]): r for r in gateway if r.get("phase") == "committed"}
    tainted = tainted_tasks(spec, records)
    declared = {e.id for e in spec.effects}
    out: dict[str, dict[str, Any]] = {}
    for r in records.ledger:
        if r.get("phase") != "committed" or r["effect_id"] in out:
            continue
        post = r.get("gateway") or {}
        joined = core.get(str(post.get("effect_id")), {})
        runtime = post.get("task_id") or joined.get("task_id")
        issuer = rt_tasks.get(str(runtime)) if runtime else None
        source = "gateway" if issuer is not None else None
        if issuer is None and r.get("issuer"):
            issuer, source = str(r["issuer"]), "sink_data"
        out[r["effect_id"]] = {
            "issuer": issuer,
            "source": source,
            "class": r.get("class")
            or _effect_class(
                spec, str(r["effect_id"]), post.get("target") or joined.get("target")
            ),
            "declared": r["effect_id"] in declared,
            "tainted": None
            if issuer is None
            else issuer in tainted or issuer in records.spawned,
            "attributed": r.get("attributed", True),
        }
    return out


# ---------------------------------------------------------------------------
# From the run's records
# ---------------------------------------------------------------------------
@dataclass
class TaskRecord:
    """What the worker events say about one planned task."""

    starts: list[int] = field(default_factory=list)
    compute_start: int | None = None
    labels_at_start: tuple[str, ...] = ()
    polluted_at: int | None = None
    produced: dict[str, tuple[int, str, tuple[str, ...]]] = field(default_factory=dict)
    produced_at: int | None = None  # all outputs out (also for a task without any)
    end: int | None = None
    pauses: list[int] = field(default_factory=list)
    resumes: list[int] = field(default_factory=list)
    cancelled: int | None = None
    dispatches: dict[tuple[str, str], tuple[int, str | None]] = field(
        default_factory=dict
    )
    wasted_s: float = 0.0

    @property
    def tainted_at(self) -> int | None:
        times = []
        if self.compute_start is not None and self.labels_at_start:
            times.append(self.compute_start)
        if self.polluted_at is not None:
            times.append(self.polluted_at)
        return min(times) if times else None

    @property
    def held_at_end(self) -> bool:
        return bool(self.pauses) and (
            not self.resumes or self.pauses[-1] > self.resumes[-1]
        )


@dataclass
class RunRecords:
    tasks: dict[str, TaskRecord]
    ledger: list[dict[str, Any]]
    spawned: dict[str, dict[str, Any]]
    consumed: dict[str, list[tuple[int, str]]]  # item -> [(t, reader task)]
    events: list[dict[str, Any]]


def read_records(
    spec: Spec, events: Iterable[Mapping[str, Any]], ledger: Iterable[Mapping[str, Any]]
) -> RunRecords:
    """Index the services' records; times are the services' ``t_ms``."""
    tasks = {t.id: TaskRecord() for t in spec.tasks}
    spawned: dict[str, dict[str, Any]] = {}
    consumed: dict[str, list[tuple[int, str]]] = defaultdict(list)
    kept_events = []
    for e in events:
        kept_events.append(dict(e))
        kind, t = e["kind"], int(e["t_ms"])
        if e.get("inv_kind") == "rogue":
            entry = spawned.setdefault(e["task"], {"agent": e["agent"]})
            entry.setdefault(kind, t)
            continue
        if e.get("inv_kind") != "task" or e["task"] not in tasks:
            continue
        rec = tasks[e["task"]]
        if kind == "start":
            rec.starts.append(t)
        elif kind == "resumed":
            rec.resumes.append(t)
        elif kind == "compute_start" and rec.compute_start is None:
            rec.compute_start, rec.labels_at_start = t, tuple(e.get("labels", ()))
        elif kind == "consume":
            consumed[e["item"]].append((t, e["task"]))
        elif kind == "polluted" and rec.polluted_at is None:
            rec.polluted_at = t
        elif kind == "produce" and e["item"] not in rec.produced:
            rec.produced[e["item"]] = (t, e["value"], tuple(e.get("labels", ())))
        elif kind == "produced" and rec.produced_at is None:
            rec.produced_at = t
        elif kind == "end":
            rec.end = t
        elif kind == "paused":
            rec.pauses.append(t)
            rec.wasted_s = max(rec.wasted_s, float(e.get("wasted_s", 0.0)))
        elif kind == "cancelled":
            rec.cancelled = t
        elif kind == "dispatch" and e.get("purpose") in {"deliver", "create"}:
            key = (e["purpose"], f"{e.get('item')}->{e['target']}")
            rec.dispatches.setdefault(key, (t, e.get("code")))
    return RunRecords(
        tasks, [dict(r) for r in ledger], spawned, dict(consumed), kept_events
    )


def ledger_effects(
    records: RunRecords, lineage: Mapping[str, Mapping[str, Any]] | None = None
) -> dict[str, dict[str, Any]]:
    """Effect id -> its final ledger state and times (the actuator's view).

    With ``lineage`` (``commit_lineage``), a committed effect's ``tainted`` is
    its lineage verdict (None: unknown); otherwise, and for effects never
    committed, whether the worker's record carried fault labels.
    """
    out: dict[str, dict[str, Any]] = {}
    for r in records.ledger:
        known = (lineage or {}).get(r["effect_id"], {})
        entry = out.setdefault(
            r["effect_id"],
            {
                "issuer": r.get("issuer", known.get("issuer")),
                "class": r.get("class", known.get("class")),
                "declared": r.get("declared", known.get("declared", False)),
                "origin": r.get(
                    "origin", "declared" if known.get("declared") else None
                ),
                "labels": tuple(r.get("labels", ())),
                "value": r.get("value"),
                "attributed": True,
            },
        )
        entry.setdefault(f"t_{r['phase']}", int(r["t_ms"]))
        if r["phase"] in {"committed", "denied", "sink_failed"}:
            entry["state"] = r["phase"]
            entry["code"] = r.get("code")
        if r["phase"] == "committed":
            entry["attributed"] = bool(r.get("attributed", True))
    for effect_id, entry in out.items():
        entry.setdefault("state", "pending")
        if lineage is not None and effect_id in lineage:
            entry["tainted"] = lineage[effect_id]["tainted"]
            entry["lineage"] = lineage[effect_id]["source"]
            if entry["class"] is None:
                entry["class"] = lineage[effect_id]["class"]
        else:
            entry["tainted"] = bool(entry["labels"])
            entry["lineage"] = None
    return out
