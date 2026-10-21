"""Convert between the offline simulator's workflows and workload specs.

``export`` turns a simulator ``Workflow`` + ``Fault`` (the C2/C3/N2/N3 builders or
``random_workflow`` + ``random_fault``) into a spec; ``to_sim`` builds the
simulator objects back from any spec, so the same instance runs in the
simulator (``simbackend``) and on the runtime. A round trip reproduces the
simulator's workflow: task, artifact, message and effect ids are kept, agents
get runtime identities (lower case) and keep the simulator id as ``label``.

The simulator package (``contain``) is found through ``CONTAIN_SIM_ROOT`` or the
``sim_root`` argument; nothing else in the harness needs it.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from .spec import (
    Agent,
    AlertTrigger,
    Artifact,
    Effect,
    EffectClass,
    Fault,
    Message,
    Rogue,
    Spec,
    SpecError,
    Task,
    derive,
    nominal_schedule,
    validate,
)

DEFAULT_NODES = ("core", "edge-1", "edge-2")
# Small offset after the triggering event for the exported early/late alerts.
ALERT_SLACK_S = 0.4


def import_contain(sim_root: str | Path | None = None) -> Any:
    """The simulator package; raises with a clear message when it is absent."""
    root = sim_root or os.environ.get("CONTAIN_SIM_ROOT")
    if root is not None and str(root) not in sys.path:
        sys.path.insert(0, str(root))
    try:
        import contain.scenarios as scenarios  # noqa: F401
        import contain.simulator  # noqa: F401
    except ImportError as error:
        raise RuntimeError(
            "the simulator package 'contain' is not importable; set CONTAIN_SIM_ROOT"
        ) from error
    import contain

    return contain


def runtime_agent_id(sim_id: str) -> str:
    value = re.sub(r"[^a-z0-9_-]+", "-", sim_id.lower()).strip("-_")
    if not value:
        raise SpecError(f"agent id '{sim_id}' has no runtime form")
    return value[:64]


def default_placement(wf: Any, nodes: Sequence[str] = DEFAULT_NODES) -> dict[str, str]:
    """Root's site on the first node; each top-level subtree on the next node.

    Matches the planned lab mapping (root on Core, one team per edge site);
    with more teams than edge nodes the teams wrap around.
    """
    root = wf.root_agent
    placement = {wf.agents[root].site: nodes[0]}
    edges = list(nodes[1:]) or [nodes[0]]
    for i, team in enumerate(sorted(wf.children[root])):
        node = edges[i % len(edges)]
        for agent in wf.agent_subtree(team):
            placement.setdefault(wf.agents[agent].site, node)
    for cls in wf.effect_classes.values():
        placement.setdefault(cls.site, nodes[0])
    return placement


def export(
    wf: Any,
    fault: Any,
    *,
    name: str | None = None,
    rogue: Rogue = Rogue(),
    nodes: Sequence[str] = DEFAULT_NODES,
    source: dict[str, Any] | None = None,
    sim_alerts: Mapping[str, float] | None = None,
) -> Spec:
    """A spec for one simulator instance (workflow, fault, malfunction draws).

    ``sim_alerts`` names simulator alert times to keep as alert triggers
    (default: the fault's own time as ``sim``); generic ``early`` and ``late``
    triggers are added unless a simulator time already uses the name.
    """
    ids = {a: runtime_agent_id(a) for a in wf.agents}
    if len(set(ids.values())) != len(ids):
        raise SpecError("simulator agent ids collide in their runtime form")
    agents = tuple(
        Agent(ids[a.id], ids[a.parent] if a.parent else None, a.site, a.id)
        for a in wf.agents.values()
    )
    classes = tuple(
        EffectClass(c.id, c.site, c.min_delay, c.protected)
        for c in wf.effect_classes.values()
    )
    tasks = tuple(
        Task(
            t.id,
            ids[t.agent],
            t.session,
            t.compute,
            t.release,
            tuple(t.reads),
            t.step_len,
        )
        for t in wf.tasks.values()
    )
    externals = tuple(a.id for a in wf.artifacts.values() if a.producer is None)
    untrusted = [
        a.id for a in wf.artifacts.values() if a.producer is None and not a.trusted
    ]
    if untrusted:
        raise SpecError(f"untrusted external inputs are not modelled: {untrusted}")
    artifacts = tuple(
        Artifact(a.id, a.producer) for a in wf.artifacts.values() if a.producer
    )
    messages = tuple(
        Message(m.id, m.sender, m.receiver, m.latency) for m in wf.messages.values()
    )
    effects = tuple(Effect(e.id, e.issuer, e.cls, e.delay) for e in wf.effects.values())
    kind = {"wrong_value": "wrong_value", "stale_version": "stale_version"}.get(
        fault.kind
    )
    if kind is None:
        raise SpecError(f"simulator fault kind '{fault.kind}' has no spec class")
    sim_meta: dict[str, Any] = {
        "site_L": dict(wf.site_L),
        "net_lat": _used_latencies(wf, ids),
        "new_inv_step": wf.new_inv_step,
        "t_alert": fault.t_alert,
        "workflow": wf.name,
    }
    draft = Spec(
        name=name or f"{wf.name}:{fault.note or fault.z}",
        agents=agents,
        effect_classes=classes,
        tasks=tasks,
        external_inputs=externals,
        artifacts=artifacts,
        messages=messages,
        effects=effects,
        fault=Fault(kind, item=fault.z),
        alerts={},
        rogue=replace(rogue, new_inv_step=wf.new_inv_step),
        placement=default_placement(wf, nodes),
        scope_extra=_scope_extra(wf, ids),
        read_lat=wf.read_lat,
        sim=sim_meta,
        source=source or {"kind": "sim", "workflow": wf.name},
    )
    alerts = exported_alerts(draft)
    for alert_name, t in (sim_alerts or {"sim": fault.t_alert}).items():
        alerts[alert_name] = sim_alert(draft, t)
    spec = replace(draft, alerts=alerts)
    validate(spec)
    return spec


def _used_latencies(wf: Any, ids: dict[str, str]) -> list[list[Any]]:
    """Agent-to-agent latencies the simulator uses for undeclared messages."""
    pairs = []
    for a in wf.agents.values():
        for target in wf.consumers_in(wf.witnesses[a.witness].scope):
            pairs.append([ids[a.id], ids[target], wf.lat(a.id, target)])
    return pairs


def _scope_extra(wf: Any, ids: dict[str, str]) -> dict[str, tuple[str, ...]]:
    """Scope beyond the declared flows (the generator's ``scope_slack``)."""
    from_flows: dict[str, set[str]] = {a: set() for a in wf.agents}
    for m in wf.messages.values():
        s, r = wf.tasks[m.sender].agent, wf.tasks[m.receiver].agent
        if s != r:
            from_flows[s].add(f"to:{r}")
    for art in wf.artifacts.values():
        if art.producer is None:
            continue
        p = wf.tasks[art.producer].agent
        for reader in wf.readers.get(art.id, []):
            if wf.tasks[reader].agent != p:
                from_flows[p].add(f"to:{wf.tasks[reader].agent}")
    for e in wf.effects.values():
        from_flows[wf.tasks[e.issuer].agent].add(f"fx:{e.cls}")
    extra: dict[str, tuple[str, ...]] = {}
    for a in wf.agents.values():
        closed = set(from_flows[a.id])
        for d in wf.agent_subtree(a.id):
            closed |= from_flows[d]
        own = wf.witnesses[a.witness].scope - closed
        if own:
            extra[ids[a.id]] = tuple(
                sorted(ids[s[3:]] if s.startswith("to:") else s[3:] for s in own)
            )
    return extra


def exported_alerts(spec: Spec) -> dict[str, AlertTrigger]:
    """``early``: the fault item is produced, nobody consumed it; ``late``:
    every reader consumed it. Both fire shortly after the triggering event."""
    item = spec.fault.item
    if item is None:
        return {}
    alerts = {"early": AlertTrigger("produce", item=item, delay_s=ALERT_SLACK_S)}
    readers = derive(spec).readers.get(item, ())
    if readers:
        alerts["late"] = AlertTrigger(
            "consume", item=item, count=len(readers), delay_s=ALERT_SLACK_S
        )
    return alerts


def sim_alert(spec: Spec, sim_t: float) -> AlertTrigger:
    """A simulator alert time as a trigger that preserves the spread at alert.

    The runtime is slower than the nominal schedule, so a fixed clock time would
    land at a different point of the workflow; counting the reads of the fault
    item before ``sim_t`` and keeping the remaining offset reproduces the state.
    """
    item = spec.fault.item
    derived = derive(spec)
    start, end = nominal_schedule(spec)
    reads = sorted(start[r] for r in derived.readers.get(item, ()))
    consumed = [r for r in reads if r <= sim_t]
    if consumed:
        return AlertTrigger(
            "consume",
            item=item,
            count=len(consumed),
            delay_s=max(0.0, sim_t - consumed[-1]),
            sim_t=sim_t,
        )
    produced = end[derived.producer[item]]
    return AlertTrigger(
        "produce", item=item, delay_s=max(0.0, sim_t - produced), sim_t=sim_t
    )


def alert_time(spec: Spec, trigger: AlertTrigger) -> float:
    """The trigger's time on the nominal schedule (simulator seconds)."""
    if trigger.sim_t is not None:
        return trigger.sim_t
    derived = derive(spec)
    start, end = nominal_schedule(spec)
    if trigger.event == "at":
        return trigger.delay_s
    if trigger.event == "produce":
        return end[derived.producer[trigger.item]] + trigger.delay_s
    if trigger.event == "consume":
        reads = sorted(start[r] for r in derived.readers.get(trigger.item, ()))
        if trigger.count == 0 or not reads:
            return end[derived.producer[trigger.item]] + trigger.delay_s
        return reads[min(trigger.count, len(reads)) - 1] + trigger.delay_s
    if trigger.event == "start":
        return start[trigger.task] + trigger.delay_s
    if trigger.event == "end":
        return end[trigger.task] + trigger.delay_s
    raise SpecError(f"alert event '{trigger.event}' has no simulator time")


def to_sim(
    spec: Spec,
    alert: str,
    *,
    sim_root: str | Path | None = None,
    control_latency: float | None = None,
) -> tuple[Any, Any, Any]:
    """Simulator ``(Workflow, Fault, Overreach)`` for one spec and alert timing.

    ``control_latency`` replaces every site's control latency (the experiment's
    latency setting); otherwise the exported per-site values are used.
    """
    import_contain(sim_root)
    from contain.scenarios import WFBuilder
    from contain.schema import Fault as SimFault
    from contain.simulator import Overreach

    if spec.fault.cls not in {"wrong_value", "stale_version"}:
        raise SpecError(f"the simulator has no '{spec.fault.cls}' fault")
    label = {a.id: a.label or a.id for a in spec.agents}
    builder = WFBuilder(spec.name)
    for a in spec.agents:
        builder.agent(label[a.id], label[a.parent] if a.parent else None, a.site)
    for c in spec.effect_classes:
        builder.eclass(c.id, c.protected, c.site, c.min_delay)
    for x in spec.external_inputs:
        builder.ext(x)
    for t in spec.tasks:
        builder.task(
            t.id, label[t.agent], t.session, t.compute, t.release, t.reads, t.step_len
        )
    for art in spec.artifacts:
        name, _, version = art.id.partition("@")
        number = int(version[1:]) if re.fullmatch(r"v\d+", version) else 1
        builder.art(art.id, art.producer, name, number)
    for m in spec.messages:
        builder.msg(m.id, m.sender, m.receiver, m.latency)
    for e in spec.effects:
        builder.eff(e.id, e.issuer, e.cls, e.delay)
    classes = {c.id for c in spec.effect_classes}
    for agent, extra in spec.scope_extra.items():
        builder.scope(
            label[agent],
            consumers=[label[x] for x in extra if x not in classes],
            classes=[x for x in extra if x in classes],
        )
    sites = {a.site for a in spec.agents} | {c.site for c in spec.effect_classes}
    if control_latency is not None:
        site_L = {s: control_latency for s in sites}
    else:
        site_L = {s: float(spec.sim.get("site_L", {}).get(s, 0.5)) for s in sites}
    wf = builder.build(site_L=site_L, read_lat=spec.read_lat)
    for a, b, lat in spec.sim.get("net_lat", ()):
        key = (label[a], label[b]) if label[a] < label[b] else (label[b], label[a])
        wf.net_lat[key] = lat
    wf.new_inv_step = spec.rogue.new_inv_step
    trigger = spec.alerts[alert]
    item = spec.fault.item
    fault = SimFault(
        item,
        derive(spec).producer[item],
        alert_time(spec, trigger),
        spec.fault.cls,
        alert,
    )
    r = spec.rogue
    over = Overreach(
        p_over=r.rate,
        p_eff=r.p_eff,
        extra_delay=r.extra_delay,
        seed=r.seed,
        p_spawn=r.p_spawn,
        dyn_steps=r.dyn_steps,
        dyn_step_len=r.dyn_step_len,
        max_dyn=r.max_dyn,
    )
    return wf, fault, over


# ---------------------------------------------------------------------------
# Scenario catalogue (simulator builders -> specs)
# ---------------------------------------------------------------------------
def scenario(
    which: str,
    *,
    sim_root: str | Path | None = None,
    rogue: Rogue = Rogue(),
    nodes: Sequence[str] = DEFAULT_NODES,
    **params: Any,
) -> Spec:
    """``c2``, ``c3``, ``n2``, ``n3`` or ``random`` as a spec.

    ``random`` takes ``random_workflow`` parameters plus ``consumed`` (spread
    target) and ``fault_seed``; the simulator's own alert time is kept as the
    ``sim`` alert next to ``early`` and ``late``.
    """
    contain = import_contain(sim_root)
    sc = contain.scenarios
    sim_alerts = None
    if which == "c2":
        # One instance, both of the simulator's alert times (A unread / R1,R2 in flight).
        wf, fault = sc.c2("late")
        _, early = sc.c2("early")
        sim_alerts = {"early": early.t_alert, "late": fault.t_alert}
        name = "c2"
    elif which in {"n2", "n3", "c3"}:
        wf, fault = getattr(sc, which)()
        name = which
    elif which == "random":
        consumed = float(params.pop("consumed", 0.5))
        fault_seed = int(params.pop("fault_seed", params.get("seed", 0)))
        wf = sc.random_workflow(**params)
        fault = sc.random_fault(wf, consumed, seed=fault_seed)
        if fault is None:
            raise SpecError("the generated workflow has no faultable artifact")
        name = (
            f"rand-n{params.get('n_agents', 15)}-s{params.get('seed', 0)}-c{consumed}"
        )
        params = {**params, "consumed": consumed, "fault_seed": fault_seed}
    else:
        raise SpecError(f"unknown scenario '{which}'")
    return export(
        wf,
        fault,
        name=name,
        rogue=rogue,
        nodes=nodes,
        source={"kind": "sim", "scenario": which, "params": params},
        sim_alerts=sim_alerts,
    )
