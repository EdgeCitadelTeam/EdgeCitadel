"""The det-worker on a real agentd: the managed runtime, dispatch, effect gateway
and containment holds are production code; only the store is an ordinary file.

Needs the integrated runtime (effect gateway and containment control, m2/agentd)
on ``PYTHONPATH``; skipped otherwise. One node, no NATS: every agent is local.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest
from conftest import mini_document

from harness import deploy, groundtruth, nodectl
from harness import spec as S
from harness.services import RunServices, make_handler


def _has_effect_gateway() -> bool:
    try:  # a submodule lookup imports its parent package, which may be absent
        return importlib.util.find_spec("edgecitadel_agentd.effect_gateway") is not None
    except ModuleNotFoundError:
        return False


pytestmark = pytest.mark.skipif(
    not _has_effect_gateway(),
    reason="needs the integrated runtime (m2/agentd effect gateway) on PYTHONPATH",
)
SCALE = 0.4
WORKER_ROOT = Path(__file__).resolve().parents[3] / "agent-packages" / "det-worker"


def wait_for(predicate, what: str, timeout: float = 60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"{what} not reached")


@pytest.fixture
def node(tmp_path, services, monkeypatch):
    """One agentd whose effect gateway performs effects on the run's sink."""
    _, url = services
    state = tmp_path / "node"
    nodectl.configure(state, "core", None, None)
    adapters = tmp_path / "effect-adapters.json"
    adapters.write_text(json.dumps({"http": {"ledger_url": f"{url}/sink/sink"}}))
    monkeypatch.setenv("EDGECITADEL_EFFECT_ADAPTERS", str(adapters))
    stop = threading.Event()
    daemon = threading.Thread(target=nodectl.serve, args=(state, stop), daemon=True)
    daemon.start()
    from edgecitadel_agentd.service import socket_path_for

    # The admin token is written before the socket listens: wait for both.
    wait_for(lambda: (state / "agentd" / "admin.token").exists(), "agentd")
    wait_for(lambda: socket_path_for(state / "agentd").exists(), "agentd socket")
    yield state
    stop.set()
    daemon.join(10)


@pytest.fixture
def services(tmp_path):
    run = RunServices(tmp_path / "services", max_spawn=10)
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), make_handler(run, "token", sink_token="sink")
    )
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield run, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    run.close()


def start_workers(
    spec: S.Spec, plan: dict, state: Path, url: str, work: Path
) -> list[subprocess.Popen]:
    work.mkdir(parents=True, exist_ok=True)
    plan_path = work / "plan.json"
    plan_path.write_text(json.dumps(plan))
    procs = []
    for agent in spec.agents:
        env = {
            **os.environ,
            "EDGECITADEL_STATE_DIR": str(state),
            "EDGECITADEL_CONNECTOR_ID": f"managed-{agent.id}",
            "DET_WORKER_SERVICES_TOKEN": "token",
            "PYTHONPATH": os.pathsep.join(sys.path),
        }
        directory = work / agent.id
        procs.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "det_worker",
                    "--plan",
                    str(plan_path),
                    "--agent",
                    agent.id,
                    "--services",
                    url,
                    "--slots",
                    str(deploy.slots(spec, agent.id)),
                    "--work-dir",
                    str(directory),
                ],
                cwd=WORKER_ROOT,
                env=env,
                stdout=(work / f"{agent.id}.log").open("wb"),
                stderr=subprocess.STDOUT,
            )
        )
    for agent in spec.agents:
        wait_for(lambda a=agent.id: (work / a / "ready").exists(), f"worker {agent.id}")
    return procs


def performed(run: RunServices) -> set[str]:
    """Effects the sink performed so far (the gateway performs a scheduled one
    when it is due, possibly after its issuer ended)."""
    path = run.directory / "ledger.jsonl"
    lines = path.read_text().splitlines() if path.exists() else []
    return {r["effect_id"] for r in map(json.loads, lines) if r["phase"] == "committed"}


def events(run: RunServices, kind: str, task: str | None = None) -> list[dict]:
    """Events of planned tasks (not of deliveries or pending creations)."""
    feed = run.events_after(0, 100000)["events"]
    return [
        e
        for e in feed
        if e["kind"] == kind
        and e["inv_kind"] == "task"
        and (task is None or e["task"] == task)
    ]


def submit(node, run: RunServices, body: dict) -> None:
    """The harness's external request, now (``nodectl schedule``)."""
    nodectl.schedule(
        node, [{"at_ms": 0, "recipient": "r", "task": body["task"], "body": body}], run
    )
    [mark] = [m for m in marks(run) if m.get("task") == body["task"]]
    assert mark["name"] == "submitted" and mark["rt"]


def marks(run: RunServices) -> list[dict]:
    path = run.directory / "marks.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_held_task_pauses_resumes_and_the_workload_completes_once(
    tmp_path, node, services
):
    run, url = services
    spec = S.from_dict(mini_document(**{"class": "none"}))
    spec = replace(spec, placement={site: "core" for site in spec.placement})
    plan = S.compile_plan(spec, run_id="it-1", time_scale=SCALE)
    nodectl.install(node, deploy.agent_records(spec)["core"])
    nodectl.operator(node, ["r"])
    workers = start_workers(spec, plan, node, url, tmp_path / "workers")
    admin = nodectl.admin_client(node)
    try:
        t0 = int(time.time() * 1000)
        body = {
            "protocol": "det-worker.v1",
            "run": "it-1",
            "t0_ms": t0,
            "kind": "task",
            "task": "R.plan",
            "input": None,
        }
        submit(node, run, body)
        [started] = wait_for(
            lambda: events(run, "compute_start", "B.t0"), "B.t0 computing"
        )

        def control(action: str) -> dict:
            return admin.call(
                "containment.control",
                action=action,
                target={"kind": "task", "object_id": started["rt"]},
                owner="core-monitor",
                policy_version=1 if action == "hold" else 2,
            )

        assert control("hold")["status"] == "applied"
        wait_for(lambda: events(run, "paused", "B.t0"), "B.t0 paused")
        time.sleep(1.0)
        assert not events(run, "produce", "B.t0"), "a held task must not output"
        assert control("release")["status"] == "applied"
        wait_for(lambda: events(run, "resumed", "B.t0"), "B.t0 resumed")
        wait_for(
            lambda: len(events(run, "end")) == len(spec.tasks), "every task ended", 90
        )
        wait_for(
            lambda: performed(run) >= {"E_B", "E_Bn", "FINAL"}, "effects performed", 30
        )
    finally:
        for proc in workers:
            proc.terminate()
        for proc in workers:
            proc.wait(10)

    ended = {e["task"] for e in events(run, "end")}
    assert ended == {t.id for t in spec.tasks}
    expected = groundtruth.expected_contents(spec)
    for produced in events(run, "produce"):
        assert produced["value"] == expected[produced["item"]]["value"], produced[
            "item"
        ]
    assert len(events(run, "produce", "B.t0")) == 1  # outputs once, after the resume
    ledger = [
        json.loads(line)
        for line in (tmp_path / "services" / "ledger.jsonl").read_text().splitlines()
    ]
    committed = [r["effect_id"] for r in ledger if r["phase"] == "committed"]
    assert sorted(committed) == ["E_B", "E_Bn", "FINAL"]
    for record in ledger:
        if record["phase"] == "committed":
            # Performed by the gateway, attributed to the worker's attempt.
            assert record["sink"] == "http" and record["attributed"]
            assert record["gateway"]["node_id"] == "core"
            assert record["value"] == expected[record["effect_id"]]["value"]
    # Every pushed input reached its reader through the agent's memory, and
    # the reader reported its use to agentd (B.t0's X@v1, R.agg's results).
    uses = [e for e in run.events_after(0, 100000)["events"] if e["kind"] == "use"]
    pushed = {(d.target, d.item) for d in S.derive(spec).deliveries}
    assert {(e["task"], e["item"]) for e in uses} == pushed
    assert ("B.t0", "X@v1") in pushed and {e["code"] for e in uses} == {None}


def test_gateway_refuses_effects_outside_the_derived_scope(tmp_path, node, services):
    run, url = services
    spec = S.from_dict(
        mini_document(
            **{
                "class": "out_of_scope_effect",
                "origin": "B.n",
                "params": {"target_class": "act_root"},
            }
        )
    )
    spec = replace(spec, placement={site: "core" for site in spec.placement})
    plan = S.compile_plan(spec, run_id="it-2", time_scale=SCALE)
    nodectl.install(node, deploy.agent_records(spec)["core"])
    nodectl.operator(node, ["r"])
    workers = start_workers(spec, plan, node, url, tmp_path / "workers")
    try:
        body = {
            "protocol": "det-worker.v1",
            "run": "it-2",
            "t0_ms": int(time.time() * 1000),
            "kind": "task",
            "task": "R.plan",
            "input": None,
        }
        submit(node, run, body)
        wait_for(lambda: events(run, "end", "B.n"), "B.n ended", 60)
        wait_for(lambda: "E_Bn" in performed(run), "E_Bn performed", 30)
    finally:
        for proc in workers:
            proc.terminate()
        for proc in workers:
            proc.wait(10)
    ledger = [
        json.loads(line)
        for line in (tmp_path / "services" / "ledger.jsonl").read_text().splitlines()
    ]
    out_of_scope = [r for r in ledger if r["origin"] == "out_of_scope"]
    assert out_of_scope and {r["phase"] for r in out_of_scope} == {
        "attempted",
        "denied",
    }
    assert {r["code"] for r in out_of_scope if r["phase"] == "denied"} == {
        "outside_effect_scope"
    }
    tainted_declared = [
        r for r in ledger if r["effect_id"] == "E_Bn" and r["phase"] == "committed"
    ]
    assert tainted_declared and tainted_declared[0]["labels"] == [
        "fault:out_of_scope_effect:B.n"
    ]


def run_mini(tmp_path, node, services, fault: dict, run_id: str, until) -> list:
    """The mini workload on one real agentd with ``fault``, until ``until(run)``."""
    run, url = services
    spec = S.from_dict(mini_document(**fault))
    spec = replace(spec, placement={site: "core" for site in spec.placement})
    plan = S.compile_plan(spec, run_id=run_id, time_scale=SCALE)
    nodectl.install(node, deploy.agent_records(spec)["core"])
    nodectl.operator(node, ["r"])
    workers = start_workers(spec, plan, node, url, tmp_path / "workers")
    try:
        body = {
            "protocol": "det-worker.v1",
            "run": run_id,
            "t0_ms": int(time.time() * 1000),
            "kind": "task",
            "task": "R.plan",
            "input": None,
        }
        submit(node, run, body)
        until(run)
    finally:
        for proc in workers:
            proc.terminate()
        for proc in workers:
            proc.wait(10)
    return run.events_after(0, 100000)["events"]


def test_agentd_denies_a_hand_off_outside_the_grant(tmp_path, node, services):
    """out_of_scope_dispatch: B.t0 (agent b1, granted r only) also hands its
    result to a1; agentd refuses it at admission (``permission_denied``,
    which the monitor's admission_denied detector reads), and the declared
    hand-offs still go out."""
    fault = {
        "class": "out_of_scope_dispatch",
        "origin": "B.t0",
        "params": {"target_agent": "a1"},
    }
    feed = run_mini(
        tmp_path,
        node,
        services,
        fault,
        "it-3",
        lambda run: wait_for(lambda: events(run, "end", "B.t0"), "B.t0 ended", 60),
    )
    sent = [e for e in feed if e["kind"] == "dispatch" and e["task"] == "B.t0"]
    [refused] = [e for e in sent if e["purpose"] == "out_of_scope"]
    assert (refused["recipient"], refused["code"]) == ("a1", "not_authorized")
    assert all(e["code"] is None for e in sent if e["purpose"] != "out_of_scope")


def test_agentd_expires_a_deadline_runaway_and_cancels_it(tmp_path, node, services):
    """deadline_runaway: B.t0 outputs and hands on its work within the 1.5 x
    compute deadline its dispatcher set, then does not return; agentd expires
    it at the deadline (``deadline_expired``, which the monitor's
    lease_violation detector reads) and the runtime cancels the handler."""
    fault = {"class": "deadline_runaway", "origin": "B.t0"}
    feed = run_mini(
        tmp_path,
        node,
        services,
        fault,
        "it-4",
        lambda run: wait_for(
            lambda: events(run, "cancelled", "B.t0"), "B.t0 cancelled", 60
        ),
    )
    by_kind = {e["kind"]: e for e in feed if e["task"] == "B.t0"}
    assert {"produce", "overrun", "cancelled"} <= set(by_kind)
    assert "end" not in by_kind
    assert (
        by_kind["cancelled"]["reason"] == "cancel" and by_kind["cancelled"]["produced"]
    )
    # Cancelled after its deadline (1.5 x 4 spec s), well before its runaway's end.
    ran = (by_kind["cancelled"]["t_local_ms"] - by_kind["start"]["t_local_ms"]) / 1000
    assert 1.5 * 4 * SCALE <= ran < 3 * 4 * SCALE
