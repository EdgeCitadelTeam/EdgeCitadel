"""Per-event admission of labelled units: theory (AI) with the guarded release (R).

A unit under an admission label (``hold:egress``) keeps running. agentd holds
each new event it produces that passes every other gate (a dispatch, a
registered effect, its result) under an event hold of its own, journaled with
``containment/held`` in the same transaction, so the event enters G already
removed. The monitor releases an event's hold, which admits that event alone,
only when the actions in force without that hold still separate every
protected sink (R), the event's head included:

- an effect of an unprotected class reaches no protected sink: admitted;
- an effect of a protected class is a protected sink itself: kept held while
  its invocation is on the source side (tainted, or reachable from the taint
  past the actions in force), whatever else blocks its class; admitted when a
  label over-approximated the source side;
- a dispatch (head: the new task) or a result (head: the live parent that
  would consume it; none, admitted): the units the head reaches past the
  actions in force are labelled first, so the labels still cover the source
  side the admission enlarges, and confirmed (fenced) before the release;
  then the head must reach no avoidable or protected sink.

``ContainmentMonitor.admit_held`` applies the verdicts and logs one ``admission``
record per decision, with the latency from the hold (node clock) to the
release applied at the node (node clock).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field, replace

from . import solver
from .enforcement import protected_sinks
from .graph import ControlGraph
from .model import Action, hold_egress, inv, revoke_egress
from .provenance import HeldEvent, ProvenanceGraph


@dataclass
class Verdict:
    admit: bool
    reason: str
    # Labels to confirm before the release (dispatch and result heads).
    labels: set[Action] = field(default_factory=set)
    blocking: list[str] = field(default_factory=list)


def heads(
    held: HeldEvent, cg: ControlGraph, graph: ProvenanceGraph
) -> list[str] | None:
    """The G_C vertices an admitted event flows into (None: not known yet)."""
    if held.event == "dispatch":
        key = f"task:{held.subject_id}"
        return [f"in:{key}"] if key in cg.units else None
    if held.event == "result":
        parent = graph.parent_attempt(held.subject_id)
        return [f"in:{parent}"] if parent in cg.units else []
    return []


def on_source_side(
    held: HeldEvent, cg: ControlGraph, graph: ProvenanceGraph, in_force
) -> bool:
    """Whether the event's invocation is on the source side of the actions in
    force: tainted, or reachable from the taint past them."""
    attempt = held.attempt_id
    if attempt is not None and inv(attempt) in graph.taint:
        return True
    if attempt is None or attempt not in cg.units:
        return False
    return f"out:{attempt}" in cg.closure(removed=set(in_force))


def reach(
    cg: ControlGraph, starts: Iterable[str], in_force: Iterable[Action]
) -> set[str]:
    """Vertices reached from ``starts`` over edges the actions in force keep."""
    removed = set(in_force)
    seen = set(starts)
    queue = deque(seen)
    while queue:
        node = queue.popleft()
        for edge in cg.successors(node):
            if edge.action is not None and edge.action in removed:
                continue
            if edge.head not in seen:
                seen.add(edge.head)
                queue.append(edge.head)
    return seen


def labels_for(
    cg: ControlGraph, reached: set[str], in_force
) -> tuple[set[Action], list[str]]:
    """Egress labels of every unit the head reaches whose egress is still
    open, and the units reached that cannot be labelled (no node or task)."""
    found, unlabelled = set(), []
    for key, unit in cg.units.items():
        if f"out:{key}" not in reached:
            continue
        if not (unit.node and unit.task_id):
            unlabelled.append(key)
            continue
        label = hold_egress(unit.node, unit.task_id)
        if label not in in_force and revoke_egress(unit.node, unit.task_id) not in (
            in_force
        ):
            found.add(label)
    return found, sorted(unlabelled)


def blocking(cg: ControlGraph, starts: list[str], in_force) -> list[str]:
    """The avoidable or protected sinks the heads still reach (cut_holds)."""
    instance = cg.instance()
    targets = (instance.sinks - solver.unavoidable_sinks(instance)) | protected_sinks(
        cg
    )
    from_heads = replace(instance, sources=frozenset(starts))
    return sorted(solver.reached_sinks(from_heads, in_force, targets))


def verdict(
    held: HeldEvent,
    cg: ControlGraph,
    graph: ProvenanceGraph,
    in_force: frozenset[Action],
    protected_classes: frozenset[str],
) -> Verdict:
    """The guarded release of one held event on ``cg`` (built with ``in_force``)."""
    if held.event == "effect":
        if held.effect_class not in protected_classes:
            return Verdict(True, "unprotected_effect")
        if on_source_side(held, cg, graph, in_force):
            return Verdict(False, "protected_sink", blocking=[f"fx:{held.subject_id}"])
        return Verdict(True, "not_on_source_side")
    if not on_source_side(held, cg, graph, in_force):
        return Verdict(True, "not_on_source_side")
    starts = heads(held, cg, graph)
    if starts is None:
        return Verdict(False, "head_unknown")
    if not starts:
        return Verdict(True, "no_live_consumer")
    labels, unlabelled = labels_for(cg, reach(cg, starts, in_force), in_force)
    if unlabelled:
        # (AI) cannot cover them: the event stays held.
        return Verdict(False, "head_unlabelled", blocking=unlabelled)
    if labels:
        return Verdict(False, "labels_needed", labels=labels)
    reached = blocking(cg, starts, in_force)
    if reached:
        return Verdict(False, "head_reaches_sink", blocking=reached)
    return Verdict(True, "head_separated")
