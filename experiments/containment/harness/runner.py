"""Controller: run a paired grid of experiment runs and collect their records.

An experiment file names instances (spec files), strategies, alert timings,
latency settings, repetitions, the monitor's mechanism ``profiles``
(``deployed``: each strategy as deployed, the default; ``shared``: every
mechanism for every strategy), ``phase1_modes`` (unset: the profile's,
``adaptive`` for the deployed cut strategies and ``freeze`` in the shared
ablation; or an ablation setting ``freeze``, ``freeze_plus_cut``),
``phase1_labels`` (``frontier``, the default, or ``forward_closure``) and
``receding_during_waits`` settings. ``parts``
adds sub-grids that override any of those (for example one instance under the
shared profile). The grid is paired: every strategy runs the same instance
under the same alert trigger, latency setting, repetition, profile and option,
consecutively, so slow drift of the testbed affects all strategies alike. Each
instance also gets a no-fault reference run per latency setting and repetition
(strategy ``reference``); strategy ``none`` runs the fault with no alert. Runs
without containment (``none``, ``reference``) keep the monitor running with
``enforce: false``, so the nodes' controller leases are renewed in every run.

One run: fresh stack, root task(s) submitted at ``t0``, the alert posted when
its trigger fires on the workers' events (same trigger for every strategy),
then wait until every task ended, the run is quiescent, or it times out.

Root modes decouple containment from root-cause analysis: in the main runs
(``given``) the alert names the faulty invocation and hands it to the monitor
as the root; in the smaller ``ranked`` mode the alert names only the symptom
(the reader whose consumption tripped the trigger, or the faulty invocation
for a trigger without a reader) and the monitor's root-cause ranking picks.

Latency settings that inject more than the measured LAN round trip between
sites (``measured_rtt_ms``, 4 ms on the lab testbed) are stress settings and
must say so in their name; ``latency_regime`` records it per run.
Collected into ``<out>/runs/<run_id>/``: ``run.json`` (identity, times,
status, alert), ``spec.json``, ``plan.json``, ``events.jsonl``,
``ledger.jsonl``, ``marks.jsonl``, ``decisions.jsonl`` (the monitor's
decision log), ``status.json`` and ``status_at_decision.json`` (monitor
snapshots), and ``logs/``. A run whose ``run.json`` exists is not repeated.
"""

from __future__ import annotations

import itertools
import json
import re
import shutil
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from . import deploy, netem
from .spec import (
    AlertTrigger,
    Fault,
    Spec,
    compile_plan,
    derive,
    fault_origin,
    load,
    makespan,
    to_dict,
)
from .stack import RunStack, StackConfig, StackError

POLL_S = 0.2
START_MARGIN_MS = 1500
# Core ingests the faulty task's run/started about a second after it happened.
ATTEMPT_LOOKUP_S = 1.0
# How long a ranked-mode alert waits for Core to know the symptom invocation.
SYMPTOM_LOOKUP_S = 5.0
ROOT_MODES = ("given", "ranked")
MEASURED_RTT_MS = netem.MEASURED_RTT_MS


@dataclass(frozen=True)
class Latency:
    """A latency setting: what the monitor assumes and what is injected.

    ``control_latency_ms``: per node (``*`` for all) for the monitor's deadline
    model; ``control_delay_ms``: added to every control call to that node;
    ``netem``: a lab network profile name (installed around the run when the
    stack file sets ``netem.apply``; see ``harness.netem``).
    """

    name: str  # unique within an experiment; identifies the setting
    control_latency_ms: Mapping[str, float] = field(default_factory=dict, compare=False)
    control_delay_ms: Mapping[str, float] = field(default_factory=dict, compare=False)
    netem: str | None = None

    def per_node(self, nodes: Iterable[str]) -> dict[str, float]:
        default = float(self.control_latency_ms.get("*", MEASURED_RTT_MS))
        return {n: float(self.control_latency_ms.get(n, default)) for n in nodes}

    def regime(self, measured_rtt_ms: float, netem_rtt_ms: float = 0.0) -> str:
        """``stress`` when the setting assumes or injects more control latency
        than the measured round trip, or its netem profile adds more."""
        values = [
            *map(float, self.control_latency_ms.values()),
            *map(float, self.control_delay_ms.values()),
            float(netem_rtt_ms),
        ]
        return "stress" if max(values, default=0.0) > measured_rtt_ms else "measured"


@dataclass(frozen=True)
class Experiment:
    name: str
    instances: tuple[Path, ...]
    strategies: tuple[str, ...]
    alerts: tuple[str, ...]
    latencies: tuple[Latency, ...]
    repetitions: int = 1
    time_scale: float = 1.0
    rogue_rates: tuple[float, ...] | None = None
    reference: bool = True
    quiet_s: float = 6.0
    timeout_factor: float = 3.0
    timeout_extra_s: float = 60.0
    keep_state: bool = False
    # Repeat a run whose testbed failed (a worker slot lost agentd, a process
    # died, a service never came up): such a run measures the machine, not the
    # strategy. The failed attempt is kept next to it as <run_id>.infra<n>.
    infra_retries: int = 1
    root_modes: tuple[str, ...] = ("given",)
    measured_rtt_ms: float = MEASURED_RTT_MS
    profiles: tuple[str, ...] = ("deployed",)
    # None: the profile's phase-1 mode (PROFILE_PHASE1_MODES).
    phase1_modes: tuple[str | None, ...] = (None,)
    phase1_labels: tuple[str, ...] = ("frontier",)
    receding_during_waits: tuple[bool, ...] = (False,)
    # Sub-grids run after this one, each overriding some of its dimensions.
    parts: tuple[Experiment, ...] = ()

    @classmethod
    def load(cls, path: str | Path) -> Experiment:
        path = Path(path)
        document = json.loads(path.read_text())
        fields = _experiment_fields(document, path.parent)
        parts = []
        for index, part in enumerate(document.get("parts", [])):
            if set(part) & {"name", "parts", "latencies", "measured_rtt_ms"}:
                raise ValueError(f"part {index}: only grid dimensions can change")
            parts.append(cls(**{**fields, **_experiment_fields(part, path.parent)}))
        return cls(**fields, parts=tuple(parts))


PROFILES = ("deployed", "shared")
PHASE1_MODES = ("freeze", "freeze_plus_cut", "adaptive")
PHASE1_LABELS = ("frontier", "forward_closure")
# The monitor's phase-1 mode per profile when a run sets none
# (MonitorConfig.phase1_mode_for): the main design of each profile.
PROFILE_PHASE1_MODES = {"deployed": "adaptive", "shared": "freeze"}


def _experiment_fields(document: Mapping[str, Any], base: Path) -> dict[str, Any]:
    """The fields an experiment document (or one of its parts) sets."""
    out: dict[str, Any] = {}
    if "name" in document:
        out["name"] = document["name"]
    if "strategies" in document:
        out["strategies"] = tuple(document["strategies"])
        for s in out["strategies"]:
            if s not in deploy.STRATEGIES and s != "none":
                raise ValueError(f"unknown strategy '{s}'")
    if "root_modes" in document:
        out["root_modes"] = tuple(document["root_modes"])
        for mode in out["root_modes"]:
            if mode not in ROOT_MODES:
                raise ValueError(f"unknown root mode '{mode}'")
    if "profiles" in document:
        out["profiles"] = tuple(document["profiles"])
        for profile in out["profiles"]:
            if profile not in PROFILES:
                raise ValueError(f"unknown profile '{profile}'")
    if "phase1_modes" in document:
        out["phase1_modes"] = tuple(document["phase1_modes"])
        for mode in out["phase1_modes"]:
            if mode not in PHASE1_MODES:
                raise ValueError(f"unknown phase-1 mode '{mode}'")
    if "phase1_labels" in document:
        out["phase1_labels"] = tuple(document["phase1_labels"])
        for labels in out["phase1_labels"]:
            if labels not in PHASE1_LABELS:
                raise ValueError(f"unknown phase-1 labels '{labels}'")
    if "receding_during_waits" in document:
        out["receding_during_waits"] = tuple(document["receding_during_waits"])
        if not all(isinstance(v, bool) for v in out["receding_during_waits"]):
            raise ValueError("receding_during_waits lists booleans")
    if "measured_rtt_ms" in document:
        out["measured_rtt_ms"] = float(document["measured_rtt_ms"])
    if "latencies" in document:
        measured = out.get("measured_rtt_ms", MEASURED_RTT_MS)
        out["latencies"] = tuple(
            Latency(**latency) for latency in document["latencies"]
        )
        for latency in out["latencies"]:
            check_stress_label(latency.name, latency.regime(measured), measured)
    if "instances" in document:
        out["instances"] = tuple((base / p).resolve() for p in document["instances"])
    if "alerts" in document:
        out["alerts"] = tuple(document["alerts"])
    if "rogue_rates" in document:
        out["rogue_rates"] = tuple(document["rogue_rates"])
    for key, kind in (
        ("repetitions", int),
        ("time_scale", float),
        ("reference", bool),
        ("quiet_s", float),
        ("timeout_factor", float),
        ("timeout_extra_s", float),
        ("keep_state", bool),
        ("infra_retries", int),
    ):
        if key in document:
            out[key] = kind(document[key])
    return out


def check_stress_label(name: str, regime: str, measured_rtt_ms: float) -> None:
    """A latency setting above the measured round trip must be labelled stress."""
    if regime == "stress" and "stress" not in name.lower():
        raise ValueError(
            f"latency '{name}' is above the measured LAN round trip "
            f"({measured_rtt_ms:g} ms): label it as a stress setting (e.g. 'stress-{name}')"
        )


@dataclass(frozen=True)
class RunCell:
    instance: Path
    rogue_rate: float | None
    strategy: str  # monitor strategy code, "none" (no alert) or "reference" (no fault)
    alert: str | None
    latency: Latency
    rep: int
    root_mode: str = "given"
    profile: str = "deployed"
    phase1_mode: str | None = None  # None: the profile's
    receding: bool = False
    phase1_labels: str = "frontier"

    @property
    def mode(self) -> str:
        """The phase-1 mode of the run's two-phase strategies."""
        return self.phase1_mode or PROFILE_PHASE1_MODES[self.profile]

    @property
    def main_design(self) -> bool:
        """The profile's own phase 1 with frontier labels and no additions."""
        return (
            self.mode == PROFILE_PHASE1_MODES[self.profile]
            and self.phase1_labels == "frontier"
            and not self.receding
        )

    @property
    def run_id(self) -> str:
        rogue = "" if self.rogue_rate is None else f"__p{self.rogue_rate:g}"
        alert = self.alert or "nofault"
        if self.alert and self.root_mode != "given":
            alert = f"{alert}.{self.root_mode}"
        # The main design (deployed profile, its phase 1) keeps plain ids.
        variant = [] if self.profile == "deployed" else [self.profile]
        if self.mode != PROFILE_PHASE1_MODES[self.profile]:
            variant.append(self.mode)
        variant += [] if self.phase1_labels == "frontier" else [self.phase1_labels]
        variant += ["receding"] if self.receding else []
        parts = [
            self.instance.stem + rogue,
            alert,
            self.latency.name,
            f"r{self.rep}",
            *variant,
            self.strategy,
        ]
        return re.sub(r"[^A-Za-z0-9_.=-]+", "-", "__".join(parts))


def grid(experiment: Experiment) -> list[RunCell]:
    """Paired cells: strategies adjacent within each (instance, alert, latency,
    rep, profile, option); then the cells of each part."""
    cells = []
    rates: tuple[float | None, ...] = experiment.rogue_rates or (None,)
    for instance in experiment.instances:
        for rate in rates:
            for latency in experiment.latencies:
                for rep in range(experiment.repetitions):
                    if experiment.reference:
                        cells.append(
                            RunCell(instance, rate, "reference", None, latency, rep)
                        )
                    for profile in experiment.profiles:
                        for phase1, receding, labels in itertools.product(
                            experiment.phase1_modes,
                            experiment.receding_during_waits,
                            experiment.phase1_labels,
                        ):
                            for alert in experiment.alerts:
                                for mode in experiment.root_modes:
                                    for strategy in experiment.strategies:
                                        cells.append(
                                            RunCell(
                                                instance,
                                                rate,
                                                strategy,
                                                alert,
                                                latency,
                                                rep,
                                                mode,
                                                profile,
                                                phase1,
                                                receding,
                                                labels,
                                            )
                                        )
    for part in experiment.parts:
        cells += grid(part)
    ids = [cell.run_id for cell in cells]
    duplicated = sorted({i for i in ids if ids.count(i) > 1})
    if duplicated:
        raise ValueError(f"the grid repeats runs: {duplicated[:3]}")
    return cells


def cell_spec(cell: RunCell) -> Spec:
    spec = load(cell.instance)
    if cell.rogue_rate is not None:
        spec = replace(spec, rogue=replace(spec.rogue, rate=cell.rogue_rate))
    if cell.strategy == "reference":
        spec = replace(spec, fault=Fault("none"), alerts={})
    return spec


# ---------------------------------------------------------------------------
# Alert triggers on the workers' events
# ---------------------------------------------------------------------------
class TriggerWatch:
    """Find when an alert trigger fires, from the services' event feed."""

    def __init__(
        self, spec: Spec, trigger: AlertTrigger, t0_ms: int, time_scale: float
    ) -> None:
        self.spec = spec
        self.trigger = trigger
        self.t0 = t0_ms
        self.scale = time_scale
        self.readers = set(derive(spec).readers.get(trigger.item or "", ()))
        self.consumers: dict[str, int] = {}
        self.consumer_rt: dict[str, str] = {}
        self.matched: int | None = None

    def observe(self, event: Mapping[str, Any]) -> None:
        trigger, kind = self.trigger, event["kind"]
        if self.matched is not None or event.get("inv_kind") != "task":
            return
        if (
            trigger.event == "produce"
            and kind == "produce"
            and event.get("item") == trigger.item
        ):
            self.matched = int(event["t_ms"])
        elif trigger.event == "consume":
            if (
                trigger.count == 0
                and kind == "produce"
                and event.get("item") == trigger.item
            ):
                self.matched = int(event["t_ms"])
            elif (
                kind == "consume"
                and event.get("item") == trigger.item
                and event["task"] in self.readers
            ):
                self.consumers.setdefault(event["task"], int(event["t_ms"]))
                self.consumer_rt.setdefault(event["task"], str(event.get("rt", "")))
                if trigger.count and len(self.consumers) >= trigger.count:
                    self.matched = int(event["t_ms"])
        elif (
            trigger.event in {"start", "end"}
            and kind == trigger.event
            and event["task"] == trigger.task
        ):
            self.matched = int(event["t_ms"])
        elif trigger.event == "fault" and kind == "fault":
            self.matched = int(event["t_ms"])

    def symptom(self) -> str | None:
        """The runtime task of the latest reader that consumed the item, if any:
        where an operator would first see the fault's symptom."""
        if not self.consumers:
            return None
        task = max(self.consumers, key=lambda t: (self.consumers[t], t))
        return self.consumer_rt.get(task) or None

    def fire_at(self) -> int | None:
        delay = int(self.trigger.delay_s * self.scale * 1000)
        if self.trigger.event == "at":
            return self.t0 + delay
        return None if self.matched is None else self.matched + delay


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------
def run_cell(
    stack_config: StackConfig, experiment: Experiment, cell: RunCell, out: Path
) -> Path:
    run_dir = out / "runs" / cell.run_id
    if (run_dir / "run.json").exists():
        return run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    spec = cell_spec(cell)
    scale = experiment.time_scale
    plan = compile_plan(
        spec,
        run_id=cell.run_id,
        time_scale=scale,
        effect_sink=stack_config.plan_sink(),
    )
    (run_dir / "spec.json").write_text(json.dumps(to_dict(spec), indent=1))
    (run_dir / "plan.json").write_text(json.dumps(plan))
    nodes = deploy.nodes(spec)
    meta: dict[str, Any] = {
        "run_id": cell.run_id,
        "experiment": experiment.name,
        "mode": stack_config.mode,
        "instance": cell.instance.stem,
        "spec_name": spec.name,
        "fault": spec.fault.cls,
        "rogue_rate": spec.rogue.rate,
        "strategy": cell.strategy,
        "alert": cell.alert,
        "root_mode": cell.root_mode if cell.alert else None,
        "latency": cell.latency.name,
        "latency_regime": cell.latency.regime(
            experiment.measured_rtt_ms, stack_config.netem_rtt_ms(cell.latency.netem)
        ),
        "effect_sink": stack_config.effect_sink["kind"],
        "controller_lease_ms": stack_config.controller_lease_ms,
        "profile": cell.profile,
        # The run's setting; phase1_modes says what each strategy ran (none
        # for a rule applied directly), from the monitor's status.
        "phase1_mode": cell.mode,
        "phase1_modes": None,
        "phase1_labels": cell.phase1_labels,
        "receding_during_waits": cell.receding,
        "mechanisms": None,  # the monitor's, for this strategy (from its status)
        "control_latency_ms": cell.latency.per_node(nodes),
        "control_delay_ms": dict(cell.latency.control_delay_ms),
        "rep": cell.rep,
        "time_scale": scale,
        "t0_ms": None,
        "t_alert_ms": None,
        "status": "failed_to_start",
        "errors": [],
    }
    stack = RunStack(stack_config, cell.run_id, run_dir)
    strategy = cell.strategy if cell.strategy in deploy.STRATEGIES else "firebreak"
    netem = False
    try:
        netem = stack.apply_netem(cell.latency.netem)
        meta["netem_applied"] = netem
        stack.up(
            spec,
            plan,
            strategy=strategy,
            control_latency_ms=meta["control_latency_ms"],
            control_delay_ms=cell.latency.control_delay_ms,
            enforce=cell.strategy in deploy.STRATEGIES,
            profile=cell.profile,
            phase1_mode=cell.phase1_mode,
            phase1_labels=cell.phase1_labels,
            receding_during_waits=cell.receding,
        )
        if cell.strategy in deploy.STRATEGIES:
            status = stack.monitor_status()
            meta["mechanisms"] = status["mechanisms"][cell.strategy]
            meta["phase1_modes"] = status["phase1_mode"]
        _drive(stack, spec, plan, cell, experiment, meta, run_dir)
    except (StackError, OSError, ValueError, KeyError) as error:
        meta["errors"].append(f"{type(error).__name__}: {error}"[:2000])
    finally:
        died = stack.down()
        if died:
            meta["errors"].append(f"processes exited early: {died}")
        if netem:
            stack.clear_netem()
        _collect(stack, stack_config, run_dir, keep_state=experiment.keep_state)
        meta.update(testbed_counts(run_dir))
        (run_dir / "run.json").write_text(json.dumps(meta, indent=1))
    return run_dir


TRANSPORT_CODES = ("rpc:agentd is unavailable", "rpc:agentd closed the connection")


def testbed_counts(run_dir: Path) -> dict[str, int]:
    """Worker slots that lost agentd, RPCs retried, and RPCs that never got
    through (a dispatch or effect lost to the testbed, not refused by it)."""
    counts = {"slot_restarts": 0, "rpc_retries": 0, "rpc_failures": 0}
    for name in ("events.jsonl", "ledger.jsonl"):
        path = run_dir / name
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            record = json.loads(line)
            kind = record.get("kind")
            if kind == "slot_restart":
                counts["slot_restarts"] += 1
            elif kind == "rpc_retry":
                counts["rpc_retries"] += 1
            elif str(record.get("code") or "").startswith(TRANSPORT_CODES):
                counts["rpc_failures"] += 1
    return counts


def _drive(
    stack: RunStack,
    spec: Spec,
    plan: Mapping[str, Any],
    cell: RunCell,
    experiment: Experiment,
    meta: dict[str, Any],
    run_dir: Path,
) -> None:
    scale = experiment.time_scale
    t0 = int(stack.services("GET", "/clock")["t_ms"]) + START_MARGIN_MS
    meta["t0_ms"] = t0
    stack.services("POST", "/mark", {"name": "t0", "t0_ms": t0})
    root_agent = plan["root_agent"]
    root_node = plan["agents"][root_agent]["node"]
    meta["roots"] = {
        root: stack.submit(
            root_node,
            root_agent,
            {
                "protocol": "det-worker.v1",
                "run": cell.run_id,
                "t0_ms": t0,
                "kind": "task",
                "task": root,
                "input": None,
            },
        )
        for root in derive(spec).roots
    }
    trigger = (
        spec.alerts.get(cell.alert)
        if cell.alert and cell.strategy != "reference"
        else None
    )
    if cell.alert and cell.strategy != "reference" and trigger is None:
        raise ValueError(f"instance has no alert '{cell.alert}'")
    watch = TriggerWatch(spec, trigger, t0, scale) if trigger else None
    origin = fault_origin(spec)
    deadline = (
        time.monotonic()
        + (START_MARGIN_MS / 1000)
        + makespan(spec) * scale * experiment.timeout_factor
        + experiment.timeout_extra_s
    )
    quiet = max(experiment.quiet_s, 2.0) * max(1.0, scale)
    after, last_change = 0, time.monotonic()
    ended: set[str] = set()
    origin_rt = origin_attempt = None
    next_lookup = next_status = 0.0
    alert_due = cell.strategy not in {"reference", "none"}
    decision_seen = False
    while True:
        feed = stack.services("GET", f"/events?after={after}")
        events = feed["events"]
        if events:
            after, last_change = feed["next"], time.monotonic()
        for event in events:
            if event.get("inv_kind") == "task" and event["kind"] == "end":
                ended.add(event["task"])
            if (
                event.get("inv_kind") == "task"
                and event["task"] == origin
                and origin_rt is None
            ):
                origin_rt = event["rt"]
                next_lookup = time.monotonic() + ATTEMPT_LOOKUP_S
            if watch:
                watch.observe(event)
        if (
            alert_due
            and origin_rt
            and origin_attempt is None
            and time.monotonic() >= next_lookup
        ):
            # Resolve the faulty invocation ahead of the trigger (Core lags the run).
            origin_attempt = stack.attempt_of(origin_rt)
            next_lookup = time.monotonic() + ATTEMPT_LOOKUP_S
        now_ms = int(feed["t_ms"])
        if (
            watch
            and meta["t_alert_ms"] is None
            and watch.fire_at() is not None
            and now_ms >= watch.fire_at()
        ):
            meta["alert_trigger_ms"] = watch.fire_at()
            if alert_due and origin_rt is not None:
                _post_alert(stack, spec, cell, watch, origin_rt, origin_attempt, meta)
            elif not alert_due:
                # Uncontained baseline: the alert time is still the reference point.
                meta["t_alert_ms"] = int(
                    stack.services("POST", "/mark", {"name": "alert", "posted": False})[
                        "t_ms"
                    ]
                )
        if (
            meta["t_alert_ms"] is not None
            and alert_due
            and not decision_seen
            and time.monotonic() >= next_status
        ):
            next_status = time.monotonic() + 0.5
            status = stack.monitor_status()
            if status.get("last_decision"):
                decision_seen = True
                (run_dir / "status_at_decision.json").write_text(json.dumps(status))
        if len(ended) == len(spec.tasks) and time.monotonic() - last_change > 1.0:
            meta["status"] = "complete"
            break
        if (
            time.monotonic() - last_change > quiet
            and (now_ms - t0) / 1000 > makespan(spec) * scale
        ):
            meta["status"] = "quiescent"
            break
        if time.monotonic() > deadline:
            meta["status"] = "timeout"
            break
        time.sleep(POLL_S)
    meta["n_ended"] = len(ended)
    if trigger is not None and meta["t_alert_ms"] is None:
        meta["errors"].append("alert trigger never fired")
    if alert_due and meta["t_alert_ms"] is not None and "alert_payload" not in meta:
        meta["errors"].append("faulty task never started; no alert posted")
    try:
        (run_dir / "status.json").write_text(json.dumps(stack.monitor_status()))
    except OSError as error:
        meta["errors"].append(f"final monitor status unavailable: {error}")


def _post_alert(
    stack: RunStack,
    spec: Spec,
    cell: RunCell,
    watch: TriggerWatch,
    origin_rt: str,
    origin_attempt: str | None,
    meta: dict[str, Any],
) -> None:
    """The operator alert of the run's root mode.

    ``given``: the alert names the faulty invocation and designates it as the
    root. ``ranked``: it names the symptom (the latest reader that consumed
    the faulty item, else the faulty invocation) and no root, so the monitor's
    root-cause ranking chooses. The monitor's taint index starts from
    invocations, so attempts are preferred; a task subject is the fallback and
    is recorded as an error of the run.
    """
    if origin_attempt is None:
        origin_attempt = stack.attempt_of(origin_rt)
    meta["origin_attempt"] = origin_attempt
    subject_rt, subject_attempt = origin_rt, origin_attempt
    if cell.root_mode == "ranked":
        symptom = watch.symptom()
        if symptom is not None and symptom != origin_rt:
            subject_rt, subject_attempt = symptom, None
            deadline = time.monotonic() + SYMPTOM_LOOKUP_S
            while subject_attempt is None and time.monotonic() < deadline:
                subject_attempt = stack.attempt_of(symptom)
                if subject_attempt is None:
                    time.sleep(0.25)
        meta["symptom_rt"] = subject_rt
    subject = (
        {"attempt_id": subject_attempt} if subject_attempt else {"task_id": subject_rt}
    )
    if subject_attempt is None:
        meta["errors"].append("alert subject not yet in Core; alert names its task")
    alert: dict[str, Any] = {
        "kind": "operator",
        **subject,
        "strategy": cell.strategy,
        "detail": {
            "fault": spec.fault.cls,
            "item": spec.fault.item or "",
            "run": cell.run_id,
        },
    }
    if cell.root_mode == "given":
        alert["root"] = dict(subject)
    t_posted = int(
        stack.services("POST", "/mark", {"name": "alert", "posted": True})["t_ms"]
    )
    reply = stack.post_alert(alert)
    meta["t_alert_ms"] = t_posted
    meta["alert_payload"] = {**alert, "reply": reply}


def _collect(
    stack: RunStack, config: StackConfig, run_dir: Path, *, keep_state: bool
) -> None:
    controller = config.controller
    base = stack.host_dir(controller)
    for name, target in (
        ("services/events.jsonl", "events.jsonl"),
        ("services/ledger.jsonl", "ledger.jsonl"),
        ("services/marks.jsonl", "marks.jsonl"),
        ("core/containment-decisions.jsonl", "decisions.jsonl"),
    ):
        data = stack.fetch(controller, f"{base}/{name}")
        (run_dir / target).write_bytes(data or b"")
    try:  # the gateway's decisions, as Core collected them
        effects = stack.hosts[controller].run(
            [
                config.hosts[controller].python,
                "-m",
                "harness.coreq",
                "effects",
                "--db",
                f"{base}/core/openclaw.db",
            ],
            cwd=config.hosts[controller].runtime_root,
        )
    except (StackError, OSError):
        effects = ""
    (run_dir / "gateway_effects.jsonl").write_text(effects)
    logs = run_dir / "logs"
    logs.mkdir(exist_ok=True)
    for host in {controller, *config.node_hosts.values()}:
        directory = stack.host_dir(host)
        for proc_log in _remote_logs(stack, host, directory):
            data = stack.fetch(host, proc_log)
            if data is not None:
                (logs / f"{host}__{Path(proc_log).name}").write_bytes(data[-2_000_000:])
    if not keep_state:
        for host in {controller, *config.node_hosts.values()}:
            if config.hosts[host].ssh is None:
                shutil.rmtree(stack.host_dir(host), ignore_errors=True)
            else:
                stack.hosts[host].run(["rm", "-rf", stack.host_dir(host)])


def _remote_logs(stack: RunStack, host: str, directory: str) -> list[str]:
    names = []
    for proc_dir in ("logs",):
        listing = stack.hosts[host].run(
            [
                "sh",
                "-c",
                f"ls {directory}/{proc_dir}/*.log {directory}/workers/*/*.events.jsonl 2>/dev/null || true",
            ]
        )
        names += [line for line in listing.split() if line]
    return names


def infrastructure_failure(meta: Mapping[str, Any]) -> str | None:
    """Why a run measured the testbed rather than the strategy, if it did."""
    if meta.get("slot_restarts"):
        return f"{meta['slot_restarts']} worker slot(s) lost agentd"
    if meta.get("rpc_failures"):
        return f"{meta['rpc_failures']} runtime call(s) never reached agentd"
    for error in meta.get("errors", []):
        if (
            "exited early" in error
            or "did not become ready" in error
            or "cannot reach" in error
            # Nothing strategy-specific runs before the alert, so a run that
            # never reached its alert point stalled on the testbed.
            or "never fired" in error
        ):
            return error
    if meta.get("t0_ms") is None:
        return "the run did not start"
    return None


def run_experiment(
    stack_config: StackConfig,
    experiment: Experiment,
    out: Path,
    *,
    limit: int | None = None,
) -> list[Path]:
    out.mkdir(parents=True, exist_ok=True)
    done = []
    for i, cell in enumerate(grid(experiment)):
        if limit is not None and i >= limit:
            break
        if (out / "runs" / cell.run_id / "run.json").exists():
            # Finished by an earlier invocation, its retries included.
            done.append(out / "runs" / cell.run_id)
            continue
        started = time.monotonic()
        for attempt in range(experiment.infra_retries + 1):
            run_dir = run_cell(stack_config, experiment, cell, out)
            meta = json.loads((run_dir / "run.json").read_text())
            failure = infrastructure_failure(meta)
            if failure is None or attempt == experiment.infra_retries:
                break
            print(
                f"[{i + 1}] {cell.run_id}: testbed failure ({failure}); repeating",
                flush=True,
            )
            run_dir.rename(run_dir.with_name(f"{run_dir.name}.infra{attempt}"))
        meta["attempts"] = attempt + 1
        (run_dir / "run.json").write_text(json.dumps(meta, indent=1))
        print(
            f"[{i + 1}] {cell.run_id}: {meta['status']} in {time.monotonic() - started:.0f}s"
            + (f" errors={meta['errors']}" if meta["errors"] else ""),
            flush=True,
        )
        done.append(run_dir)
    return done
