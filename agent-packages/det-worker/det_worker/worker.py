"""Deterministic worker: executes one agent's tasks of a compiled workload plan.

Requests are JSON bodies with ``protocol: det-worker.v1``; ``kind`` selects:

- ``task``: run a planned task. Inputs: the creating input carried by the
  request (the dispatcher's message or session state), items their producers
  pushed to this agent, and seeded external inputs. Then compute in steps,
  output items, push deliveries, hand each task it creates its creating input
  (a ``create`` request to that task's agent), and perform its effects.
- ``create``: a created task's creating input waiting at its agent, like a
  delivered message in an inbox: once the task's pushed inputs have landed it
  dispatches the task itself. The task therefore starts after every input
  arrived, which is the order the runtime's lineage records (an agent's later
  invocations follow its earlier ones), and its creator need not wait.
- ``deliver``: store a pushed item for a later task of this agent.
- ``rogue``: an undeclared new invocation spawned by a malfunctioning task.
- ``rogue_msg``: an undeclared message that lands in a live task of this agent.

Interruption contract (``managed_runtime``): a containment hold cancels the
handler with ``context.interruption == "pause"``; once released the runtime
calls it again for the same task and binding with ``context.resumed``. Every
side effect of an invocation is an *op* recorded before it starts and keyed by
stable request ids, so a resumed invocation continues where it stopped:
completed steps, outputs, pushes, dispatches and effects are never repeated
(agentd returns the stored receipt for a repeated request id) and only the
interrupted compute step is redone. A cancel ends the invocation; agentd itself
refuses dispatches and effects of a held or ended task.

Effects go through ``effect.authorize`` (scheduled ones are committed with
``effect.commit`` when due) and, once the gateway allows them, to the ledger,
which is the actuator. A tainted invocation (one whose inputs carry a fault
label) takes the workload's undeclared in-scope actions at its step boundaries,
drawn exactly as the simulator draws them.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from edgecitadel_agentd.client import AgentdClientError

from .content import (
    Content,
    external_content,
    fault_label,
    item_content,
    labels_of,
    rogue_draw,
)
from .services import ServicesClient, ServicesUnavailable

log = logging.getLogger(__name__)
PROTOCOL = "det-worker.v1"
SKILL_ID = "det.task"
RUNTIME_EFFECT_CLASS = "device.command"
READY_POLL_SECONDS = 0.05
HELD_RETRY_SECONDS = 0.1
# A scheduled effect due sooner than this is authorized as an immediate one.
IMMEDIATE_EFFECT_MS = 20
# Transport failures of one RPC (agentd's client messages), retried in place.
TRANSPORT_ERRORS = ("agentd is unavailable", "agentd closed the connection")
RPC_ATTEMPTS = 4
RPC_RETRY_SECONDS = 0.5
CONTENT_FAULTS = frozenset(
    {"wrong_value", "stale_version", "out_of_scope_effect", "lease_runaway"}
)


def stable_uuid(*parts: object) -> str:
    """A UUIDv4-shaped id derived from ``parts``: a repeated op keeps its id."""
    digest = hashlib.sha256(json.dumps([str(p) for p in parts]).encode()).digest()
    return str(uuid.UUID(bytes=digest[:16], version=4))


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class Op:
    """One side effect of an invocation; ``params`` also records its progress."""

    key: str
    kind: str  # dispatch | effect | runaway
    params: dict[str, Any]
    state: str = "pending"  # pending | done | denied


@dataclass
class Invocation:
    rt_task: str
    kind: str
    task: str  # spec task id, spawned invocation id, or a delivery label
    started: bool = False
    started_ms: int = 0
    inputs: list[Content] | None = None
    labels: set[str] = field(default_factory=set)
    steps_done: int = 0
    wasted_s: float = 0.0
    produced: bool = False
    outputs: dict[str, Content] = field(default_factory=dict)
    ops: dict[str, Op] = field(default_factory=dict)
    running: dict[str, asyncio.Task[None]] = field(default_factory=dict)


@dataclass
class AgentState:
    """This agent's memory, shared by its slots; workload content lives here."""

    delivered: dict[str, dict[str, list[Content]]] = field(default_factory=dict)
    pollution: dict[str, set[str]] = field(default_factory=dict)
    computing: set[str] = field(default_factory=set)
    finished: set[str] = field(default_factory=set)
    invocations: dict[str, Invocation] = field(default_factory=dict)


def _invocation_name(kind: str, request: Mapping[str, Any]) -> str:
    if kind == "task":
        return str(request["task"])
    if kind == "create":
        return f"create:{request['task']}"
    if kind == "rogue":
        return str(request["id"])
    if kind == "rogue_msg":
        return str(request["msg"])
    return f"deliver:{request['item']}->{request['target']}"


class DetWorker:
    def __init__(
        self,
        plan: Mapping[str, Any],
        agent: str,
        services: ServicesClient,
        *,
        node: str,
        event_log: Path | None = None,
    ) -> None:
        self.plan = plan
        self.agent = agent
        self.services = services
        self.node = node
        self.event_log = event_log
        self.state = AgentState()
        self.scale = float(plan["time_scale"])
        self.run_id = str(plan["run_id"])
        self.fault = plan["fault"]
        self.rogue = plan["rogue"]
        self.scope = plan["agents"][agent]

    # -- entry point ----------------------------------------------------------
    async def handle(
        self, envelope: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        payload = envelope.get("payload") or {}
        try:
            request = json.loads(payload.get("body", ""))
        except (TypeError, ValueError):
            return {"error": "invalid_request"}, "rejected"
        if (
            not isinstance(request, dict)
            or request.get("protocol") != PROTOCOL
            or request.get("run") != self.run_id
        ):
            return {"error": "not_a_workload_request"}, "rejected"
        handlers = {
            "task": self._task,
            "create": self._pending_create,
            "deliver": self._deliver,
            "rogue": self._spawned,
            "rogue_msg": self._rogue_message,
        }
        kind = request.get("kind")
        if kind not in handlers:
            return {"error": "unknown_kind"}, "rejected"
        rt_task = str(envelope["task_id"])
        inv = self.state.invocations.get(rt_task)
        if inv is None:
            inv = Invocation(rt_task, kind, _invocation_name(kind, request))
            self.state.invocations[rt_task] = inv
        try:
            return await handlers[kind](inv, request, context)
        except asyncio.CancelledError:
            await self._interrupted(inv, context)
            raise

    async def _interrupted(self, inv: Invocation, context: Any) -> None:
        for running in inv.running.values():
            running.cancel()
        await asyncio.gather(*inv.running.values(), return_exceptions=True)
        inv.running.clear()
        reason = getattr(context, "interruption", None)
        await self.emit(
            "paused" if reason == "pause" else "cancelled",
            inv,
            reason=reason,
            steps_done=inv.steps_done,
            produced=inv.produced,
            wasted_s=round(inv.wasted_s, 3),
        )

    async def _begin(self, inv: Invocation, **data: Any) -> None:
        await self.emit("resumed" if inv.started else "start", inv, **data)
        if not inv.started:
            inv.started, inv.started_ms = True, now_ms()

    # -- planned task -----------------------------------------------------------
    async def _task(
        self, inv: Invocation, request: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        spec = self.plan["tasks"][inv.task]
        if spec["agent"] != self.agent:
            return {"error": "wrong_agent"}, "rejected"
        t0 = int(request["t0_ms"])
        await self._begin(inv)
        await self._sleep_until(t0 + spec["release"] * self.scale * 1000)
        if inv.inputs is None:
            await self._take_inputs(inv, request)
        origin = self.fault["origin"] == inv.task
        if origin and self.fault["class"] == "lease_runaway" and not inv.produced:
            self._add_op(inv, "runaway", "runaway", {})
        self._start_ops(inv, context)
        step = spec["compute"] * self.scale / spec["n_steps"]
        while inv.steps_done < spec["n_steps"]:
            k = inv.steps_done + 1
            began = time.monotonic()
            try:
                await asyncio.sleep(step)
            except asyncio.CancelledError:
                inv.wasted_s += time.monotonic() - began
                raise
            await self._absorb_pollution(inv)
            inv.steps_done = k
            if inv.labels:
                await self._malfunction(inv, inv.task, k)
            if origin and self.fault["class"] == "out_of_scope_effect":
                self._out_of_scope_attempt(inv, k)
            self._start_ops(inv, context)
        if not inv.produced:
            await self._produce(inv, spec, t0)
        await self._finish_ops(inv, context)
        self.state.finished.add(inv.task)
        await self.emit("end", inv, wasted_s=round(inv.wasted_s, 3))
        return {
            "det": 1,
            "task": inv.task,
            "outputs": {
                item: c["value"]
                for item, c in inv.outputs.items()
                if not item.startswith("session:")
            },
        }, "completed"

    async def _take_inputs(self, inv: Invocation, request: Mapping[str, Any]) -> None:
        spec = self.plan["tasks"][inv.task]
        consumed: list[tuple[str, Content]] = []
        given = request.get("input")
        if given is not None:
            consumed.append((str(given["item"]), dict(given["content"])))
        delivered = self.state.delivered.get(inv.task, {})
        extra: set[str] = set()
        for d in spec["deliveries_in"]:
            item = d["item"]
            copies = delivered.get(item, [])
            if not copies:
                if not self._transport_fault(item, "message_delay"):
                    raise RuntimeError(f"input {item} of {inv.task} has not landed")
                # The dispatcher stopped waiting; the task works on its stale copy.
                label = fault_label("message_delay", item)
                consumed.append((item, item_content(f"{item}@stale", (), [label])))
                continue
            if len(copies) > 1:
                extra.add(fault_label("message_duplicate", item))
            consumed.append((item, copies[0]))
        for item in spec["seeded"]:
            consumed.append((item, external_content(item)))
        inv.inputs = [content for _, content in consumed]
        inv.labels = set(labels_of(inv.inputs)) | extra
        inv.labels |= self.state.pollution.pop(inv.task, set())
        if self.fault["origin"] == inv.task and self.fault["class"] in CONTENT_FAULTS:
            subject = self.fault["item"] or self.fault["origin"]
            inv.labels.add(fault_label(self.fault["class"], str(subject)))
            await self.emit("fault", inv, fault=self.fault["class"])
        self.state.computing.add(inv.task)
        for item, content in consumed:
            await self.emit("consume", inv, item=item, labels=list(content["labels"]))
        await self.emit("compute_start", inv, labels=sorted(inv.labels))

    def _transport_fault(self, item: str, cls: str) -> bool:
        return self.fault["class"] == cls and self.fault["item"] == item

    async def _absorb_pollution(self, inv: Invocation) -> None:
        landed = self.state.pollution.pop(inv.task, set())
        if landed - inv.labels:
            inv.labels |= landed
            await self.emit("polluted", inv, labels=sorted(inv.labels))

    async def _produce(self, inv: Invocation, spec: Mapping[str, Any], t0: int) -> None:
        """Output items, then record pushes, dispatches and effects as ops."""
        assert inv.inputs is not None
        session_item = f"session:{inv.task}"
        for item in [*spec["outputs"], session_item]:
            inv.outputs[item] = item_content(item, inv.inputs, inv.labels)
        for item in spec["outputs"]:
            content = inv.outputs[item]
            await self.emit(
                "produce",
                inv,
                item=item,
                value=content["value"],
                labels=content["labels"],
            )
        base = {"protocol": PROTOCOL, "run": self.run_id, "t0_ms": t0}
        for d in spec["deliveries_out"]:
            item = d["item"]
            copies = 2 if self._transport_fault(item, "message_duplicate") else 1
            delay_ms = 0.0
            if self._transport_fault(item, "message_delay"):
                delay_ms = float(self.fault["params"]["delay_s"]) * self.scale * 1000
                await self.emit("fault", inv, fault="message_delay", item=item)
            for copy in range(copies):
                self._add_op(
                    inv,
                    f"deliver:{item}:{d['target']}:{copy}",
                    "dispatch",
                    {
                        "recipient": d["agent"],
                        "purpose": "deliver",
                        "item": item,
                        "target": d["target"],
                        "not_before_ms": now_ms() + delay_ms,
                        "request": {
                            **base,
                            "kind": "deliver",
                            "item": item,
                            "target": d["target"],
                            "copy": copy,
                            "content": inv.outputs[item],
                        },
                    },
                )
        for child in spec["creates"]:
            self._add_op(
                inv,
                f"create:{child['task']}",
                "dispatch",
                {
                    "recipient": child["agent"],
                    "purpose": "create",
                    "item": child["item"],
                    "target": child["task"],
                    "request": {
                        **base,
                        "kind": "create",
                        "task": child["task"],
                        "lease": child.get("lease"),
                        "input": {
                            "item": child["item"],
                            "content": inv.outputs[child["item"]],
                        },
                    },
                },
            )
        for effect in spec["effects"]:
            self._add_op(
                inv,
                f"effect:{effect['id']}",
                "effect",
                {
                    "effect_id": effect["id"],
                    "class": effect["class"],
                    "target": effect["target"],
                    "delay": effect["delay"],
                    "declared": True,
                    "origin": "declared",
                    "content": item_content(
                        effect["id"], [inv.outputs[session_item]], inv.labels
                    ),
                },
            )
        inv.produced = True
        self.state.computing.discard(inv.task)
        await self.emit("produced", inv, outputs=list(spec["outputs"]))
        runaway = inv.ops.get("runaway")
        if runaway is not None:
            runaway.state = "done"  # a runaway stops with the compute
            running = inv.running.pop("runaway", None)
            if running is not None:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)

    # -- undeclared behaviour -------------------------------------------------------
    async def _malfunction(self, inv: Invocation, emitter: str, k: int) -> None:
        """The simulator's hidden malfunction at step boundary k of a tainted run."""
        rogue = self.rogue
        draw = rogue_draw(rogue["seed"], emitter, k, rogue["rate"], rogue["p_eff"])
        if draw is None:
            return
        is_effect, u1, u2, u3, u4 = draw
        classes = self.scope["scope_classes"]
        consumers = self.scope["scope_agents"]
        labels = sorted(inv.labels)
        if is_effect and classes:
            cls = classes[int(u1 * len(classes)) % len(classes)]
            delay = (
                self.plan["effect_classes"][cls]["min_delay"]
                + u2 * rogue["extra_delay"]
            )
            effect_id = f"ovE:{emitter}:{k}"
            self._add_op(
                inv,
                f"rogue:{k}",
                "effect",
                {
                    "effect_id": effect_id,
                    "class": cls,
                    "target": f"actuator://{cls}/{effect_id}",
                    "delay": delay,
                    "declared": False,
                    "origin": "rogue",
                    "content": item_content(effect_id, inv.inputs or (), labels),
                },
            )
            await self.emit(
                "rogue", inv, k=k, action="effect", cls=cls, effect_id=effect_id
            )
        elif consumers:
            target = consumers[int(u1 * len(consumers)) % len(consumers)]
            message = f"ovM:{emitter}:{k}"
            spawn = u4 < rogue["p_spawn"]
            body: dict[str, Any] = {
                "protocol": PROTOCOL,
                "run": self.run_id,
                "kind": "rogue" if spawn else "rogue_msg",
                "msg": message,
                "labels": labels,
                "choice": u3,
            }
            if spawn:
                # The simulator names a spawned invocation after the agent's label.
                body["id"] = f"nv|{self.plan['agents'][target]['label']}|{message}"
            self._add_op(
                inv,
                f"rogue:{k}",
                "dispatch",
                {
                    "recipient": target,
                    "purpose": "spawn" if spawn else "rogue_msg",
                    "target": body.get("id", target),
                    "request": body,
                },
            )
            await self.emit(
                "rogue", inv, k=k, action="spawn" if spawn else "message", to=target
            )

    def _out_of_scope_attempt(self, inv: Invocation, k: int) -> None:
        target = str(self.fault["params"].get("target_class", "unauthorized"))
        effect_id = f"oos:{inv.task}:{k}"
        self._add_op(
            inv,
            effect_id,
            "effect",
            {
                "effect_id": effect_id,
                "class": target,
                "target": f"actuator://{target}/{effect_id}",
                "delay": 0.0,
                "declared": False,
                "origin": "out_of_scope",
                "content": item_content(
                    effect_id, inv.inputs or (), sorted(inv.labels)
                ),
            },
        )

    async def _spawned(
        self, inv: Invocation, request: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        """An undeclared invocation: tainted, emits ``dyn_steps`` times, no outputs."""
        rogue = self.rogue
        if not inv.started:
            inv.labels = set(request["labels"])
            inv.inputs = [item_content(str(request["msg"]), (), inv.labels)]
        await self._begin(inv, spawned=True)
        self._start_ops(inv, context)
        while inv.steps_done < rogue["dyn_steps"]:
            k = inv.steps_done + 1
            offset = rogue["new_inv_step"] + rogue["dyn_step_len"] * (k - 1)
            await self._sleep_until(inv.started_ms + offset * self.scale * 1000)
            inv.steps_done = k
            await self._malfunction(inv, inv.task, k)
            self._start_ops(inv, context)
        await self._finish_ops(inv, context)
        await self.emit("end", inv, spawned=True)
        return {"det": 1, "spawned": inv.task}, "completed"

    async def _rogue_message(
        self, inv: Invocation, request: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        """An undeclared message lands in a live task of this agent (simulator rule)."""
        receiver = None
        if not inv.started:
            live = [t for t in self.scope["tasks"] if t not in self.state.finished]
            if live:
                receiver = live[int(float(request["choice"]) * len(live)) % len(live)]
                self.state.pollution.setdefault(receiver, set()).update(
                    request["labels"]
                )
        await self._begin(inv)
        await self.emit("rogue_landed", inv, msg=request["msg"], receiver=receiver)
        return {"det": 1, "landed": receiver}, "completed"

    async def _pending_create(
        self, inv: Invocation, request: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        """Dispatch the created task to this agent once its pushed inputs landed."""
        task = str(request["task"])
        if self.plan["tasks"][task]["agent"] != self.agent:
            return {"error": "wrong_agent"}, "rejected"
        await self._begin(inv)
        await self._await_ready(task)
        body = {
            key: request[key] for key in ("protocol", "run", "t0_ms", "task", "input")
        }
        self._add_op(
            inv,
            f"start:{task}",
            "dispatch",
            {
                "recipient": self.agent,
                "purpose": "start",
                "item": request["input"]["item"],
                "target": task,
                "lease": request.get("lease"),
                "request": {**body, "kind": "task"},
            },
        )
        await self._finish_ops(inv, context)
        await self.emit("end", inv)
        return {"det": 1, "started": task}, "completed"

    async def _deliver(
        self, inv: Invocation, request: Mapping[str, Any], context: Any
    ) -> tuple[dict[str, Any], str]:
        target, item = str(request["target"]), str(request["item"])
        if not inv.started:
            copies = self.state.delivered.setdefault(target, {}).setdefault(item, [])
            copies.append(dict(request["content"]))
        await self._begin(inv)
        await self.emit(
            "delivered",
            inv,
            target=target,
            item=item,
            copy=int(request.get("copy", 0)),
            labels=list(request["content"]["labels"]),
            late=target in self.state.computing or target in self.state.finished,
        )
        return {"det": 1, "delivered": item}, "completed"

    # -- ops ------------------------------------------------------------------------
    def _add_op(
        self, inv: Invocation, key: str, kind: str, params: dict[str, Any]
    ) -> None:
        if key not in inv.ops:
            inv.ops[key] = Op(key, kind, params)

    def _start_ops(self, inv: Invocation, context: Any) -> None:
        runners = {
            "dispatch": self._dispatch_op,
            "effect": self._effect_op,
            "runaway": self._runaway_op,
        }
        for key, op in list(inv.ops.items()):
            if op.state == "pending" and key not in inv.running:
                inv.running[key] = asyncio.create_task(
                    runners[op.kind](inv, op, context)
                )

    async def _finish_ops(self, inv: Invocation, context: Any) -> None:
        while True:
            self._start_ops(inv, context)
            pending = [t for t in inv.running.values() if not t.done()]
            if not pending:
                break
            await asyncio.gather(*pending)
        for running in inv.running.values():
            running.result()  # an unexpected op failure fails the task visibly
        inv.running.clear()

    async def _call(
        self, context: Any, operation: str, **params: Any
    ) -> dict[str, Any]:
        """One runtime RPC; a refused call becomes an error reply with its code.

        A transport failure (agentd busy or unreachable) is retried with the
        same request id: agentd stores a receipt per request id, so a request
        it already applied returns that receipt instead of acting twice. Only
        a refusal or a persistent outage reaches the caller as an error.
        """
        for attempt in range(RPC_ATTEMPTS):
            try:
                reply = await asyncio.to_thread(
                    context.client.call, operation, **params
                )
                break
            except AgentdClientError as error:
                code = f"rpc:{error}"[:200]
                if not str(error).startswith(TRANSPORT_ERRORS):
                    return {"status": "error", "code": code}
                if attempt + 1 == RPC_ATTEMPTS:
                    return {"status": "error", "code": code}
                await self.note("rpc_retry", operation=operation, code=code)
                await asyncio.sleep(RPC_RETRY_SECONDS * (attempt + 1))
        return (
            reply
            if isinstance(reply, dict)
            else {"status": "error", "code": "invalid_reply"}
        )

    async def _dispatch_op(self, inv: Invocation, op: Op, context: Any) -> None:
        p = op.params
        if p.get("not_before_ms"):
            await self._sleep_until(p["not_before_ms"])
        if p["purpose"] == "spawn" and not p.get("reserved"):
            allowed = await asyncio.to_thread(
                self.services.spawn, {"run": self.run_id, "id": p["target"]}
            )
            if not allowed:
                op.state = "denied"
                await self.emit(
                    "dispatch",
                    inv,
                    purpose="spawn",
                    target=p["target"],
                    code="spawn_cap",
                )
                return
            p["reserved"] = True
        child = stable_uuid(self.run_id, inv.rt_task, op.key, "child")
        params: dict[str, Any] = {
            "schema_version": 1,
            "request_id": stable_uuid(self.run_id, inv.rt_task, op.key, "dispatch"),
            "binding_id": context.trace.binding_id,
            "child_task_id": child,
            "recipient_id": p["recipient"],
            "request": json.dumps(p["request"], separators=(",", ":")),
            "skill_id": SKILL_ID,
            "deadline_at_ms": None,
        }
        if p.get("lease"):
            params["lease"] = p["lease"]
        reply = await self._call(context, "trace.dispatch", **params)
        code = None if reply.get("status") == "ok" else str(reply.get("code"))
        op.state = "done" if code is None else "denied"
        await self.emit(
            "dispatch",
            inv,
            purpose=p["purpose"],
            item=p.get("item"),
            target=p["target"],
            recipient=p["recipient"],
            child=child,
            code=code,
        )

    async def _await_ready(self, task: str) -> None:
        """Wait until the pushed inputs of ``task`` landed (or a delayed one is stale)."""
        expected: dict[str, int] = {}
        for d in self.plan["tasks"][task]["deliveries_in"]:
            expected[d["item"]] = (
                2 if self._transport_fault(d["item"], "message_duplicate") else 1
            )
        if not expected:
            return
        stale_after = self.fault["params"].get("stale_after_s")
        give_up = None
        if self.fault["class"] == "message_delay" and stale_after is not None:
            give_up = now_ms() + float(stale_after) * self.scale * 1000
        while True:
            landed = await asyncio.to_thread(self.services.ready, task)
            missing = {i for i, n in expected.items() if landed.get(i, 0) < n}
            if not missing:
                return
            if (
                give_up is not None
                and now_ms() >= give_up
                and missing == {self.fault["item"]}
            ):
                return
            await asyncio.sleep(READY_POLL_SECONDS)

    async def _effect_op(self, inv: Invocation, op: Op, context: Any) -> None:
        p = op.params
        effect_request = stable_uuid(self.run_id, inv.rt_task, op.key, "effect")
        record = {
            "run": self.run_id,
            "effect_id": p["effect_id"],
            "class": p["class"],
            "target": p["target"],
            "issuer": inv.task,
            "rt_task": inv.rt_task,
            "agent": self.agent,
            "declared": p["declared"],
            "origin": p["origin"],
            "value": p["content"]["value"],
            "labels": list(p["content"]["labels"]),
        }
        p.setdefault("due_ms", now_ms() + p["delay"] * self.scale * 1000)
        if "requested" not in p:
            await self._ledger({**record, "phase": "attempted"})
            scheduled = p["due_ms"] - now_ms() > IMMEDIATE_EFFECT_MS
            reply = await self._call(
                context,
                "effect.authorize",
                schema_version=1,
                request_id=effect_request,
                binding_id=context.trace.binding_id,
                effect_class=RUNTIME_EFFECT_CLASS,
                target=p["target"],
                tool_call_id=None,
                scheduled_at_ms=int(p["due_ms"]) if scheduled else None,
            )
            if reply.get("status") != "ok":
                op.state = "denied"
                await self._ledger(
                    {**record, "phase": "denied", "code": reply.get("code")}
                )
                return
            p["requested"] = reply["result"]["state"]
        if p["requested"] == "committed":
            await self._ledger({**record, "phase": "committed"})
            op.state = "done"
            return
        await self._sleep_until(p["due_ms"])
        held = False
        while True:
            reply = await self._call(
                context,
                "effect.commit",
                schema_version=1,
                request_id=stable_uuid(self.run_id, inv.rt_task, op.key, "commit"),
                binding_id=context.trace.binding_id,
                effect_id=effect_request,
            )
            if reply.get("status") == "ok":
                await self._ledger({**record, "phase": "committed"})
                op.state = "done"
                return
            if reply.get("code") != "effect_held":
                op.state = "denied"
                await self._ledger(
                    {**record, "phase": "denied", "code": reply.get("code")}
                )
                return
            if not held:
                held = True
                await self._ledger({**record, "phase": "held", "code": "effect_held"})
            await asyncio.sleep(HELD_RETRY_SECONDS)

    async def _runaway_op(self, inv: Invocation, op: Op, context: Any) -> None:
        """Lease runaway: undeclared in-scope effects at a fixed rate while computing."""
        params = self.fault["params"]
        classes = self.scope["scope_classes"]
        if not classes:
            op.state = "done"
            return
        interval = self.scale / float(params.get("rate_hz", 2.0))
        while int(op.params.get("attempts", 0)) < int(params.get("attempts", 20)):
            await asyncio.sleep(interval)
            attempt = int(op.params.get("attempts", 0)) + 1
            op.params["attempts"] = attempt
            effect_id = f"runaway:{inv.task}:{attempt}"
            self._add_op(
                inv,
                effect_id,
                "effect",
                {
                    "effect_id": effect_id,
                    "class": classes[0],
                    "target": f"actuator://{classes[0]}/{effect_id}",
                    "delay": 0.0,
                    "declared": False,
                    "origin": "runaway",
                    "content": item_content(
                        effect_id, inv.inputs or (), sorted(inv.labels)
                    ),
                },
            )
            self._start_ops(inv, context)
        op.state = "done"

    # -- plumbing -------------------------------------------------------------------
    async def _ledger(self, record: Mapping[str, Any]) -> None:
        await asyncio.to_thread(self.services.ledger, record)

    async def emit(self, kind: str, inv: Invocation, **data: Any) -> None:
        record = {
            "run": self.run_id,
            "agent": self.agent,
            "node": self.node,
            "kind": kind,
            "task": inv.task,
            "rt": inv.rt_task,
            "inv_kind": inv.kind,
            "t_local_ms": now_ms(),
            **data,
        }
        if self.event_log is not None:
            with self.event_log.open("a") as handle:
                handle.write(json.dumps(record) + "\n")
        try:
            await asyncio.to_thread(self.services.event, record)
        except ServicesUnavailable:
            # The local event log keeps the record; the run is flagged when the
            # harness finds fewer service events than worker log lines.
            log.warning("event %s of %s not recorded by the services", kind, inv.task)

    async def note(self, kind: str, **data: Any) -> None:
        """A worker-level event (not about one invocation)."""
        await self.emit(kind, Invocation("", "worker", self.agent), **data)

    async def _sleep_until(self, t_ms: float) -> None:
        delay = (t_ms - now_ms()) / 1000
        if delay > 0:
            await asyncio.sleep(delay)
