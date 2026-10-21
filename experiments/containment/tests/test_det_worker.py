"""The det-worker's execution and interruption contract against a scripted runtime.

The runtime side is a fake that keeps agentd's idempotency rule (a repeated
request id returns the stored receipt); the services are the real in-process
ledger. ``test_det_worker_runtime`` runs the same worker on a real agentd.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import mini_document
from det_worker.content import fault_label, rogue_draw

pytest.importorskip(
    "edgecitadel_agentd.client", reason="the worker needs agentd's client"
)
from det_worker import worker as worker_module  # noqa: E402
from det_worker.worker import PROTOCOL, DetWorker  # noqa: E402
from edgecitadel_agentd.client import AgentdClientError  # noqa: E402

from harness import groundtruth as G
from harness import spec as S
from harness.services import RunServices

SCALE = 0.05


class LocalServices:
    """The real services state, called in-process instead of over HTTP."""

    def __init__(self, directory: Path) -> None:
        self.services = RunServices(directory, max_spawn=10)

    def event(self, record: dict[str, Any]) -> None:
        self.services.event(dict(record))

    def ledger(self, record: dict[str, Any]) -> dict[str, Any]:
        return self.services.ledger(dict(record))

    def ready(self, task: str) -> dict[str, int]:
        return self.services.ready(task)["items"]

    def spawn(self, record: dict[str, Any]) -> bool:
        return self.services.spawn(dict(record))["allowed"]

    def events(self, kind: str | None = None) -> list[dict[str, Any]]:
        out = self.services.events_after(0, 100000)["events"]
        return [e for e in out if kind is None or e["kind"] == kind]

    def ledger_records(self) -> list[dict[str, Any]]:
        lines = (self.services.directory / "ledger.jsonl").read_text().splitlines()
        return [json.loads(line) for line in lines]


class FakeRuntime:
    """agentd's dispatch, trace.use and effect gateway, with stored receipts per
    request id. The gateway performs each committed effect on the run's sink
    (the services), a scheduled one when due, as agentd's scheduler does."""

    def __init__(self, sink: LocalServices | None = None) -> None:
        self.sink = sink
        self.dispatches: dict[str, dict[str, Any]] = {}
        self.uses: dict[str, str] = {}  # request id -> delivery task
        self.executed: dict[str, dict[str, Any]] = {}  # request id -> reply
        self.requests: dict[str, dict[str, Any]] = {}  # request id -> request
        self.held = False  # the issuing task is held: effects are denied
        self._timers: list[threading.Timer] = []

    def call(self, operation: str, **params: Any) -> dict[str, Any]:
        if operation == "trace.dispatch":
            previous = self.dispatches.setdefault(params["request_id"], params)
            assert previous == params, (
                "a repeated request id must carry the same request"
            )
            return {"status": "ok", "result": {"task_id": params["child_task_id"]}}
        if operation == "trace.use":
            self.uses.setdefault(params["request_id"], params["task_id"])
            return {"status": "ok", "result": {"event_id": "e", "version_id": "v"}}
        if operation == "effect.execute":
            return self._execute(params)
        raise AssertionError(operation)

    def _execute(self, params: dict[str, Any]) -> dict[str, Any]:
        if params["request_id"] in self.executed:
            assert self.requests[params["request_id"]] == params
            return self.executed[params["request_id"]]
        self.requests[params["request_id"]] = params
        if self.held:
            reply = {"status": "error", "code": "execution_held"}
        elif params["scheduled_at_ms"] is not None:
            delay = max(0.0, (params["scheduled_at_ms"] - time.time() * 1000) / 1000)
            timer = threading.Timer(delay, self._perform, (params,))
            self._timers.append(timer)
            timer.start()
            reply = {"status": "ok", "result": {"state": "scheduled"}}
        else:
            self._perform(params)
            reply = {
                "status": "ok",
                "result": {"state": "committed", "outcome": {"performed": True}},
            }
        self.executed[params["request_id"]] = reply
        return reply

    def _perform(self, params: dict[str, Any]) -> None:
        if self.sink is not None:
            gateway = {"effect_id": params["request_id"], "target": params["target"]}
            self.sink.services.committed(params["data"], "http", gateway)

    def drain(self) -> None:
        """Wait until every scheduled effect was performed."""
        for timer in self._timers:
            timer.join()


def context(runtime: FakeRuntime) -> SimpleNamespace:
    return SimpleNamespace(
        client=runtime,
        trace=SimpleNamespace(binding_id=str(uuid.uuid4())),
        interruption=None,
        resumed=False,
        agent_id="b1",
    )


def envelope(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return {"task_id": task_id, "payload": {"body": json.dumps(body)}}


def worker_for(spec: S.Spec, agent: str, services: LocalServices) -> DetWorker:
    plan = S.compile_plan(spec, run_id="run-1", time_scale=SCALE)
    return DetWorker(plan, agent, services, node=plan["agents"][agent]["node"])  # type: ignore[arg-type]


def request(kind: str, **fields: Any) -> dict[str, Any]:
    return {"protocol": PROTOCOL, "run": "run-1", "t0_ms": 0, "kind": kind, **fields}


async def land_x(worker: DetWorker, content: dict[str, Any]) -> str:
    """Deliver X@v1 to B.t0's agent; returns the delivery's task."""
    delivery = str(uuid.uuid4())
    body = request("deliver", item="X@v1", target="B.t0", copy=0, content=content)
    result, state = await worker.handle(
        envelope(delivery, body), context(FakeRuntime())
    )
    assert state == "completed", result
    return delivery


def b_request(expected: dict[str, Any]) -> dict[str, Any]:
    return request("task", task="B.t0", input={"item": "dB", "content": expected["dB"]})


async def wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached"
        await asyncio.sleep(0.005)


@pytest.mark.asyncio
async def test_task_consumes_inputs_and_outputs_the_expected_content(
    mini_spec, tmp_path
):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    delivery = await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    result, state = await worker.handle(
        envelope("t-b", b_request(expected)), context(runtime)
    )
    runtime.drain()

    assert state == "completed"
    # X@v1 came through the agent's memory: the task reports using it.
    assert list(runtime.uses.values()) == [delivery]
    [use] = services.events("use")
    assert (use["item"], use["delivery"], use["code"]) == ("X@v1", delivery, None)
    assert result["outputs"] == {"rB": expected["rB"]["value"]}
    pushes = {json.loads(d["request"])["item"] for d in runtime.dispatches.values()}
    assert pushes == {"rB"}  # rB is pushed to R.agg (R.agg is created by R.plan)
    committed = [r for r in services.ledger_records() if r["phase"] == "committed"]
    assert [(r["effect_id"], r["value"], r["labels"]) for r in committed] == [
        ("E_B", expected["E_B"]["value"], [])
    ]
    consumed = {e["item"] for e in services.events("consume")}
    assert consumed == {"dB", "X@v1"}


@pytest.mark.asyncio
async def test_faulty_input_labels_every_output_and_effect(mini_spec, tmp_path):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    label = fault_label("wrong_value", "X@v1")
    await land_x(worker, {"value": "0" * 16, "labels": [label]})
    runtime = FakeRuntime(services)
    await worker.handle(envelope("t-b", b_request(expected)), context(runtime))
    runtime.drain()

    [produced] = services.events("produce")
    assert produced["labels"] == [label]
    assert produced["value"] != expected["rB"]["value"]
    [effect] = [r for r in services.ledger_records() if r["phase"] == "committed"]
    assert effect["labels"] == [label]


@pytest.mark.asyncio
async def test_pause_mid_compute_resumes_without_repeating_work(mini_spec, tmp_path):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    ctx = context(runtime)
    running = asyncio.create_task(
        worker.handle(envelope("t-b", b_request(expected)), ctx)
    )
    await wait_for(
        lambda: "t-b" in worker.state.invocations
        and worker.state.invocations["t-b"].steps_done == 1
    )
    await asyncio.sleep(0.25 * 2 * SCALE)  # inside step 2 of 2
    ctx.interruption = "pause"
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    [paused] = services.events("paused")
    assert paused["steps_done"] == 1 and not paused["produced"]
    assert not services.events("produce") and not runtime.dispatches

    ctx.interruption, ctx.resumed = None, True
    result, state = await worker.handle(envelope("t-b", b_request(expected)), ctx)
    assert state == "completed"
    assert result["outputs"] == {"rB": expected["rB"]["value"]}
    assert len(services.events("resumed")) == 1
    assert len(services.events("consume")) == 2  # inputs were taken once
    assert len(services.events("produce")) == 1
    assert len(runtime.dispatches) == 1
    runtime.drain()
    committed = [r for r in services.ledger_records() if r["phase"] == "committed"]
    assert len(committed) == 1


@pytest.mark.asyncio
async def test_a_scheduled_effect_is_left_to_the_gateway(mini_spec, tmp_path):
    """E_B is due after its delay: the gateway keeps it and performs it when
    due; the worker does not wait for it and never asks twice."""
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    _, state = await worker.handle(
        envelope("t-b", b_request(expected)), context(runtime)
    )
    assert state == "completed"
    [sent] = runtime.requests.values()
    assert sent["scheduled_at_ms"] is not None
    assert [r["phase"] for r in services.ledger_records()] == ["attempted", "scheduled"]
    runtime.drain()
    [committed] = [r for r in services.ledger_records() if r["phase"] == "committed"]
    assert committed["attributed"] and committed["sink"] == "http"
    assert committed["value"] == expected["E_B"]["value"]


@pytest.mark.asyncio
async def test_an_effect_the_gateway_refuses_is_recorded_denied_once(
    mini_spec, tmp_path
):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    runtime.held = True  # the task is held when it asks
    _, state = await worker.handle(
        envelope("t-b", b_request(expected)), context(runtime)
    )
    assert state == "completed"
    records = services.ledger_records()
    assert [(r["phase"], r.get("code")) for r in records] == [
        ("attempted", None),
        ("denied", "execution_held"),
    ]


@pytest.mark.asyncio
async def test_cancel_ends_the_invocation_without_outputs(mini_spec, tmp_path):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    ctx = context(runtime)
    running = asyncio.create_task(
        worker.handle(envelope("t-b", b_request(expected)), ctx)
    )
    await wait_for(lambda: services.events("compute_start"))
    ctx.interruption = "cancel"
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert [e["kind"] for e in services.events()][-1] == "cancelled"
    assert not services.events("produce")
    assert not runtime.dispatches and not services.ledger_records()


class FlakyRuntime(FakeRuntime):
    """Drops the first calls of one operation at the transport, or refuses them."""

    def __init__(
        self, operation: str, failures: int, message: str, sink: LocalServices
    ) -> None:
        super().__init__(sink)
        self.operation, self.failures, self.message = operation, failures, message
        self.attempts = 0

    def call(self, operation: str, **params: Any) -> dict[str, Any]:
        if operation == self.operation:
            self.attempts += 1
            if self.attempts <= self.failures:
                # The first attempt may still have been applied before the
                # reply was lost; agentd then answers the retry from its receipt.
                super().call(operation, **params)
                raise AgentdClientError(self.message)
        return super().call(operation, **params)


@pytest.mark.asyncio
async def test_transport_failures_are_retried_with_the_same_request(
    mini_spec, tmp_path, monkeypatch
):
    monkeypatch.setattr(worker_module, "RPC_RETRY_SECONDS", 0.01)
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FlakyRuntime(
        "effect.execute", 2, "agentd is unavailable: timed out", services
    )
    _, state = await worker.handle(
        envelope("t-b", b_request(expected)), context(runtime)
    )
    runtime.drain()
    assert state == "completed"
    assert runtime.attempts == 3 and len(runtime.executed) == 1
    assert [r["phase"] for r in services.ledger_records()] == [
        "attempted",
        "scheduled",
        "committed",
    ]
    assert len(services.events("rpc_retry")) == 2


@pytest.mark.asyncio
async def test_a_refusal_is_not_retried(mini_spec, tmp_path):
    services = LocalServices(tmp_path)
    expected = G.expected_contents(mini_spec)
    worker = worker_for(mini_spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FlakyRuntime("effect.execute", 1, "effect_scope_denied", services)
    await worker.handle(envelope("t-b", b_request(expected)), context(runtime))
    assert runtime.attempts == 1
    denied = [r for r in services.ledger_records() if r["phase"] == "denied"]
    assert [r["code"] for r in denied] == ["rpc:effect_scope_denied"]
    assert not services.events("rpc_retry")


@pytest.mark.asyncio
async def test_creator_hands_creating_inputs_to_the_children_agents(tmp_path):
    spec = S.from_dict(mini_document())
    services = LocalServices(tmp_path)
    expected = G.expected_contents(spec)
    worker = worker_for(spec, "r", services)
    runtime = FakeRuntime(services)
    body = request("task", task="R.plan", input=None)
    _, state = await worker.handle(envelope("t-r", body), context(runtime))
    assert state == "completed"  # the creator does not wait for its children's inputs
    sent = {json.loads(d["request"])["task"]: d for d in runtime.dispatches.values()}
    assert set(sent) == {"A.t0", "B.t0", "B.n", "R.agg"}
    created = json.loads(sent["B.t0"]["request"])
    assert sent["B.t0"]["recipient_id"] == "b1" and created["kind"] == "create"
    assert created["input"] == {"item": "dB", "content": expected["dB"]}


@pytest.mark.asyncio
async def test_pending_creation_starts_the_task_after_its_pushed_inputs_landed(
    tmp_path,
):
    spec = S.from_dict(mini_document())
    services = LocalServices(tmp_path)
    expected = G.expected_contents(spec)
    worker = worker_for(spec, "b1", services)
    runtime = FakeRuntime(services)
    given = {"item": "dB", "content": expected["dB"]}
    body = request("create", task="B.t0", input=given, lease=None)
    running = asyncio.create_task(
        worker.handle(envelope("p-b", body), context(runtime))
    )
    await asyncio.sleep(0.2)
    assert not runtime.dispatches and not running.done()
    services.event({"kind": "delivered", "target": "B.t0", "item": "X@v1", "rt": "d-1"})
    _, state = await running
    assert state == "completed"
    [started] = runtime.dispatches.values()
    assert started["recipient_id"] == "b1"
    assert json.loads(started["request"]) == {
        "protocol": PROTOCOL,
        "run": "run-1",
        "t0_ms": 0,
        "task": "B.t0",
        "input": given,
        "kind": "task",
    }


@pytest.mark.asyncio
async def test_tainted_task_takes_the_simulators_undeclared_actions(tmp_path):
    document = mini_document()
    document["rogue"] = {"rate": 1.0, "p_eff": 0.5, "seed": 11}
    spec = S.from_dict(document)
    services = LocalServices(tmp_path)
    worker = worker_for(spec, "a1", services)
    runtime = FakeRuntime(services)
    body = request(
        "task",
        task="A.t0",
        input={"item": "dA", "content": G.expected_contents(spec)["dA"]},
    )
    _, state = await worker.handle(envelope("t-a", body), context(runtime))
    assert state == "completed"

    rogue = services.events("rogue")
    assert [e["k"] for e in rogue] == [1, 2]  # 3 s compute: boundaries at 2 s and 3 s
    for event in rogue:
        is_effect, *_ = rogue_draw(11, "A.t0", event["k"], 1.0, 0.5)
        scope = worker.scope
        expected_action = "effect" if is_effect and scope["scope_classes"] else "spawn"
        assert event["action"] == expected_action


def spec_with(**fault) -> S.Spec:
    return S.from_dict(mini_document(**fault))


@pytest.mark.asyncio
async def test_duplicated_item_is_pushed_twice_and_taints_its_reader(tmp_path):
    spec = spec_with(**{"class": "message_duplicate", "item": "rB"})
    expected = G.expected_contents(spec)
    services = LocalServices(tmp_path)
    producer = worker_for(spec, "b1", services)
    await land_x(producer, expected["X@v1"])
    runtime = FakeRuntime(services)
    await producer.handle(envelope("t-b", b_request(expected)), context(runtime))
    pushed = [json.loads(d["request"]) for d in runtime.dispatches.values()]
    assert sorted(p["copy"] for p in pushed if p["item"] == "rB") == [0, 1]

    consumer = worker_for(spec, "r", services)
    for i, body in enumerate(p for p in pushed if p["item"] == "rB"):
        await consumer.handle(envelope(f"d-{i}", body), context(FakeRuntime(services)))
    for item in ("rA", "rBn"):
        body = request(
            "deliver", item=item, target="R.agg", copy=0, content=expected[item]
        )
        await consumer.handle(
            envelope(f"d-{item}", body), context(FakeRuntime(services))
        )
    given = {"item": "session:R.plan", "content": expected["session:R.plan"]}
    create = request("create", task="R.agg", input=given, lease=None)
    await consumer.handle(envelope("p-agg", create), context(FakeRuntime(services)))
    agg = request("task", task="R.agg", input=given)
    await consumer.handle(envelope("t-agg", agg), context(FakeRuntime(services)))
    [start] = services.events("compute_start")[-1:]
    assert start["task"] == "R.agg"
    assert start["labels"] == [fault_label("message_duplicate", "rB")]


@pytest.mark.asyncio
async def test_delayed_item_goes_stale_when_the_reader_stops_waiting(tmp_path):
    spec = spec_with(
        **{
            "class": "message_delay",
            "item": "rB",
            "params": {"delay_s": 4.0, "stale_after_s": 1.0},
        }
    )
    expected = G.expected_contents(spec)
    services = LocalServices(tmp_path)
    consumer = worker_for(spec, "r", services)
    for item in ("rA", "rBn"):  # rB is late
        body = request(
            "deliver", item=item, target="R.agg", copy=0, content=expected[item]
        )
        await consumer.handle(
            envelope(f"d-{item}", body), context(FakeRuntime(services))
        )
    given = {"item": "session:R.plan", "content": expected["session:R.plan"]}
    runtime = FakeRuntime(services)
    started = asyncio.get_running_loop().time()
    await consumer.handle(
        envelope("p-agg", request("create", task="R.agg", input=given, lease=None)),
        context(runtime),
    )
    waited = asyncio.get_running_loop().time() - started
    assert 1.0 * SCALE <= waited < 4.0 * SCALE  # gave up after stale_after_s
    await consumer.handle(
        envelope("t-agg", request("task", task="R.agg", input=given)),
        context(FakeRuntime(services)),
    )
    [start] = services.events("compute_start")
    assert start["labels"] == [fault_label("message_delay", "rB")]


@pytest.mark.asyncio
async def test_runaway_task_keeps_actuating_while_it_computes(tmp_path):
    spec = spec_with(
        **{
            "class": "lease_runaway",
            "origin": "B.t0",
            "params": {"rate_hz": 4.0, "attempts": 50},
        }
    )
    expected = G.expected_contents(spec)
    services = LocalServices(tmp_path)
    worker = worker_for(spec, "b1", services)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    await worker.handle(envelope("t-b", b_request(expected)), context(runtime))
    runtime.drain()
    runaway = [
        r
        for r in services.ledger_records()
        if r["origin"] == "runaway" and r["phase"] == "committed"
    ]
    # 4 spec seconds of compute at 4 attempts per spec second; it stops with the compute.
    assert 12 <= len(runaway) <= 17
    assert {r["class"] for r in runaway} == {"act_b"}
    assert all(r["labels"] == [fault_label("lease_runaway", "B.t0")] for r in runaway)


@pytest.mark.asyncio
async def test_effects_go_to_the_plans_sink(mini_spec, tmp_path):
    services = LocalServices(tmp_path)
    plan = S.compile_plan(mini_spec, run_id="run-1", time_scale=SCALE)
    assert plan["effect_sink"] == {"adapter": "http"}
    target = "input_boolean.turn_on/input_boolean.lamp"
    physical = S.compile_plan(
        mini_spec,
        run_id="run-1",
        time_scale=SCALE,
        effect_sink={"adapter": "homeassistant", "targets": {"*": target}},
    )
    worker = DetWorker(physical, "b1", services, node="edge-2")  # type: ignore[arg-type]
    expected = G.expected_contents(mini_spec)
    await land_x(worker, expected["X@v1"])
    runtime = FakeRuntime(services)
    await worker.handle(envelope("t-b", b_request(expected)), context(runtime))
    runtime.drain()
    [sent] = runtime.requests.values()
    assert (sent["adapter"], sent["effect_class"], sent["target"]) == (
        "homeassistant",
        "device.command",
        target,
    )
    assert sent["data"] == {"ec_run": "run-1", "ec_effect": "E_B", "ec_issuer": "B.t0"}
    # The ledger keeps the workload's own target; the gateway acts on Home Assistant's.
    [attempted] = [r for r in services.ledger_records() if r["phase"] == "attempted"]
    assert attempted["target"] == "actuator://act_b/E_B"
    with pytest.raises(ValueError, match="targets"):
        DetWorker(
            {**physical, "effect_sink": {"adapter": "homeassistant"}},
            "b1",
            services,  # type: ignore[arg-type]
            node="edge-2",
        )
