"""Coordinator acceptance at its durable journal and agentd dispatch boundaries."""

import asyncio
import importlib.util
from contextlib import asynccontextmanager, closing
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from edgecitadel_agentd.client import AgentdClientError
from edgecitadel_agentd.store import AgentdStore

SOURCE = (
    Path(__file__).resolve().parents[3] / "agent-packages/jev/runtime/coordinator.py"
)
spec = importlib.util.spec_from_file_location("jev_coordinator", SOURCE)
jev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(jev)


def envelope(body="Explain why the sky is blue", **args):
    return {
        "task_id": str(uuid4()),
        "sender_id": "operator",
        "payload": {"body": body, "args": args or {"request_id": str(uuid4())}},
    }


class Client:
    def __init__(self):
        self.agents = [
            {
                "agent_id": "jim-eq-hermes",
                "state": "online",
                "capabilities": ["reasoning.chat"],
            }
        ]
        self.tasks, self.dispatches = {}, []
        self.mode = "completed"

    def call(self, op, **params):
        if op == "agent.list":
            return self.agents
        if op == "task.get":
            if params["task_id"] not in self.tasks:
                raise AgentdClientError("task was not found")
            return self.tasks[params["task_id"]]
        assert op == "trace.dispatch"
        self.dispatches.append(params)
        if self.mode == "denied":
            return {"status": "error", "code": "not_authorized"}
        if self.mode == "crash_before":
            raise SystemExit("before dispatch commit")
        self.tasks[params["child_task_id"]] = {
            "state": self.mode,
            "result": {"body": "Answer"},
        }
        if self.mode == "crash_after":
            self.tasks[params["child_task_id"]]["state"] = "completed"
            raise SystemExit("after dispatch commit")
        return {"status": "ok", "result": {"task_id": params["child_task_id"]}}


class Context:
    def __init__(self, client):
        self.client, self.progress = client, []
        self.trace = SimpleNamespace(binding_id=str(uuid4()), operation=self.operation)

    @asynccontextmanager
    async def operation(self, *_):
        yield

    async def publish_progress(self, task_id, **payload):
        self.progress.append(payload)


@pytest.fixture
def setup(tmp_path):
    journal = jev.Journal(tmp_path / "jev.sqlite3")
    client = Client()
    context = Context(client)
    coordinator = jev.Coordinator(
        journal,
        lambda *_: "review:jim-eq-hermes:jim-eq-hermes",
        wait_seconds=0,
        poll_seconds=0,
    )
    yield coordinator, context
    journal.db.close()


def invoke(coordinator, context, request=None):
    return asyncio.run(coordinator.handle(request or envelope(), context))[0]


def resume(run):
    e = envelope(run_id=run["run_id"])
    e["payload"]["skill_id"] = "jev.resume"
    return e


def test_candidates_require_all_three_constraints():
    rows = [
        {
            "agent_id": "jim-eq-hermes",
            "state": "online",
            "capabilities": ["reasoning.chat"],
        },
        {
            "agent_id": "untrusted",
            "state": "online",
            "capabilities": ["reasoning.chat"],
        },
    ]
    assert jev.candidates(rows) == ["jim-eq-hermes"]
    rows[0]["state"] = "offline"
    assert jev.candidates(rows) == []
    rows[0].update(state="online", capabilities=["other"])
    assert jev.candidates(rows) == []


@pytest.mark.parametrize(
    "route,count",
    [("single:jim-eq-hermes", 1), ("review:jim-eq-hermes:jim-eq-hermes", 2)],
)
def test_workflows_and_duplicate_run(setup, route, count):
    coordinator, context = setup
    coordinator.selector = lambda *_: route
    request = envelope()
    output = invoke(coordinator, context, request)
    assert output["outcome"] == "completed"
    assert output["body"] == "Answer"
    assert len(context.client.dispatches) == count
    if count == 2:
        second = context.client.dispatches[1]["request"]
        assert "first_step_answer" in second and request["payload"]["body"] in second
    assert invoke(coordinator, context, request) == output
    assert invoke(coordinator, context, resume(output)) == output
    assert len(context.client.dispatches) == count
    request["payload"]["body"] = "different"
    assert invoke(coordinator, context, request)["outcome"] == "rejected"


@pytest.mark.parametrize(
    "route", ["clarify", "unsupported", "single:untrusted", {}, None]
)
def test_selection_refuses_without_dispatch(setup, route):
    coordinator, context = setup
    coordinator.selector = lambda *_: route
    output = invoke(coordinator, context)
    assert output["outcome"] == "rejected"
    assert not context.client.dispatches


@pytest.mark.parametrize(
    "payload",
    [
        {"body": "", "args": {"request_id": str(uuid4())}},
        {"body": ["image"], "args": {}},
        {"body": "goal", "args": {"request_id": "bad"}},
        {"body": "goal", "args": []},
    ],
)
def test_bad_contract(setup, payload):
    coordinator, context = setup
    request = envelope()
    request["payload"] = payload
    assert invoke(coordinator, context, request)["outcome"] == "rejected"
    assert not context.client.dispatches


def test_no_candidate(setup):
    coordinator, context = setup
    context.client.agents = []
    assert invoke(coordinator, context)["outcome"] == "rejected"
    assert not context.client.dispatches


@pytest.mark.parametrize(
    "state", ["running", "completed", "failed", "expired", "undeliverable", "unknown"]
)
def test_restart_observes_only_explicit_resume_advances(setup, state):
    coordinator, context = setup
    context.client.mode = "running"
    request = envelope()
    output = invoke(coordinator, context, request)
    first = output["steps"][0]["task_id"]
    context.client.tasks[first]["state"] = state
    if state == "unknown":
        del context.client.tasks[first]
    asyncio.run(coordinator.reconcile(context.client))
    assert len(context.client.dispatches) == 1
    assert invoke(coordinator, context, request)["outcome"] == "interrupted"
    assert len(context.client.dispatches) == 1
    context.client.mode = "completed"
    resumed = invoke(coordinator, context, resume(output))
    assert len(context.client.dispatches) == (2 if state == "completed" else 1)
    assert resumed["outcome"] == (
        "completed"
        if state == "completed"
        else "failed"
        if state in {"failed", "expired", "undeliverable"}
        else "interrupted"
    )


@pytest.mark.parametrize("mode", ["crash_before", "crash_after"])
def test_crash_at_dispatch_boundary_never_redispatches(setup, mode):
    coordinator, context = setup
    context.client.mode = mode
    request = envelope()
    with pytest.raises(SystemExit):
        invoke(coordinator, context, request)
    run = coordinator.journal.request(
        "operator", request["payload"]["args"]["request_id"]
    )
    first_id = run["steps"][0]["task_id"]
    asyncio.run(coordinator.reconcile(context.client))
    context.client.mode = "completed"
    output = invoke(coordinator, context, resume(run))
    assert sum(d["child_task_id"] == first_id for d in context.client.dispatches) == 1
    assert output["outcome"] == (
        "completed" if mode == "crash_after" else "interrupted"
    )


def test_permission_denial_is_terminal(setup):
    coordinator, context = setup
    context.client.mode = "denied"
    output = invoke(coordinator, context)
    assert output["outcome"] == "failed"
    assert not context.client.tasks
    invoke(coordinator, context, resume(output))
    assert len(context.client.dispatches) == 1


def test_resume_owner_and_output_contract(setup):
    coordinator, context = setup
    context.client.mode = "running"
    output = invoke(coordinator, context)
    request = resume(output)
    request["sender_id"] = "stranger"
    assert invoke(coordinator, context, request)["outcome"] == "rejected"
    context.client.tasks[output["steps"][0]["task_id"]] = {
        "state": "completed",
        "result": {"image": "x"},
    }
    assert invoke(coordinator, context, resume(output))["outcome"] == "failed"


@pytest.mark.parametrize(
    "response", [b"no json", b"{}", b'{"answers":{"route":{"choice":"invalid"}}}']
)
def test_model_response_validation(monkeypatch, response):
    from io import BytesIO

    monkeypatch.setenv("TYPESAFE_API_KEY", "test")
    monkeypatch.setattr(
        jev.urllib.request, "urlopen", lambda *a, **k: BytesIO(response)
    )
    with pytest.raises(jev.CoordinationError, match="无效"):
        jev.select("goal", ["jim-eq-hermes"])


def test_model_timeout_and_missing_secret(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(jev.CoordinationError, match="TYPESAFE_API_KEY"):
        jev.select("goal", ["jim-eq-hermes"])
    monkeypatch.setenv("TYPESAFE_API_KEY", "test")

    def timeout(*args, **kwargs):
        assert kwargs["timeout"] == 30
        raise TimeoutError

    monkeypatch.setattr(jev.urllib.request, "urlopen", timeout)
    with pytest.raises(jev.CoordinationError, match="超时"):
        jev.select("goal", ["jim-eq-hermes"])


def test_coordinator_real_agentd_dispatch_and_trace(tmp_path):
    """Real SQLite permission/dispatch transaction and child-result reads, no broker mock."""
    from edgecitadel_agentd.service import dispatch

    (tmp_path / "node.json").write_text('{"agent_id":"node"}')
    with closing(AgentdStore(tmp_path / "agentd" / "agentd.sqlite3")) as store:
        token = store.register_connector(
            connector_id="managed-jev",
            host_type="managed-agent",
            agent_id="jev",
            capabilities=["jev.run", "jev.resume"],
        )
        session = store.open_session(connector_id="managed-jev", token=token)[
            "session_id"
        ]
        store.reconcile_managed_agents(
            [
                {
                    "package_id": "edgecitadel.jev",
                    "desired_state": "running",
                    "agent_ids": ["jev"],
                    "outbound_agents": ["jim-eq-hermes"],
                }
            ]
        )
        worker_token = store.register_connector(
            connector_id="worker",
            host_type="managed-agent",
            agent_id="jim-eq-hermes",
            capabilities=["reasoning.chat"],
        )
        worker_session = store.open_session(connector_id="worker", token=worker_token)[
            "session_id"
        ]

        def rpc(op, params):
            return dispatch(
                store,
                {
                    "version": 1,
                    "operation": op,
                    "connector_id": "managed-jev",
                    "token": token,
                    "params": params,
                },
            )

        parent = store.create_task(
            sender_id="operator", recipient_id="jev", payload={"body": "goal"}
        )
        store.transition_task(
            task_id=parent["task_id"], state="offered", actor_id="edgecitadel-system"
        )
        store.transition_task(
            task_id=parent["task_id"],
            state="accepted",
            actor_id="jev",
            session_id=session,
        )
        store.transition_task(
            task_id=parent["task_id"],
            state="running",
            actor_id="jev",
            session_id=session,
        )
        binding = store.bind_trace(
            node_id="node",
            connector_id="managed-jev",
            token=token,
            params={
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session,
                "task_id": parent["task_id"],
                "context_id": None,
            },
        )["result"]

        class StoreClient(Client):
            def call(self, op, **params):
                if op == "trace.dispatch":
                    self.dispatches.append(params)
                    reply = rpc(op, params)
                    task_id = reply["result"]["task_id"]
                    store.transition_task(
                        task_id=task_id, state="offered", actor_id="edgecitadel-system"
                    )
                    store.transition_task(
                        task_id=task_id,
                        state="accepted",
                        actor_id="jim-eq-hermes",
                        session_id=worker_session,
                    )
                    store.transition_task(
                        task_id=task_id,
                        state="running",
                        actor_id="jim-eq-hermes",
                        session_id=worker_session,
                    )
                    store.transition_task(
                        task_id=task_id,
                        state="completed",
                        actor_id="jim-eq-hermes",
                        session_id=worker_session,
                        result={"body": "verified"},
                    )
                    return reply
                return rpc(op, params)

        journal = jev.Journal(tmp_path / "jev.sqlite3")
        try:
            context = Context(StoreClient())
            context.trace.binding_id = binding["binding_id"]
            coordinator = jev.Coordinator(
                journal, lambda *_: "review:jim-eq-hermes:jim-eq-hermes"
            )
            output = invoke(coordinator, context)
            assert output["body"] == "verified"
            for step in output["steps"]:
                child = store.get_task(step["task_id"])
                assert child["payload"]["parent_task_id"] == parent["task_id"]
                assert child["skill_id"] == "reasoning.chat"
        finally:
            journal.db.close()


def test_model_timeout_rejects_without_work(setup):
    coordinator, context = setup

    def timeout(*args):
        raise TimeoutError

    coordinator.selector = timeout
    output = invoke(coordinator, context)
    assert output["outcome"] == "rejected"
    assert "TypeSafe" in output["body"]
    assert not context.client.dispatches


@pytest.mark.parametrize("boundary", ["before", "after"])
def test_process_death_preserves_journal_and_remote_task(tmp_path, boundary):
    import os
    import subprocess
    import sys

    # Separate process and real SQLite commits: no finally handlers or in-memory state survive.
    script = tmp_path / "crash.py"
    script.write_text("""
import asyncio, importlib.util, os, sys
from pathlib import Path
from types import SimpleNamespace
from contextlib import asynccontextmanager
from edgecitadel_agentd.store import AgentdStore
spec = importlib.util.spec_from_file_location('jev', sys.argv[1])
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
root = Path(sys.argv[2]); boundary = sys.argv[3]
store = AgentdStore(root / 'remote.sqlite3')
journal = m.Journal(root / 'jev.sqlite3')
class Client:
 def call(self, op, **p):
  if op == 'agent.list': return [{'agent_id':'jim-eq-hermes','state':'online','capabilities':['reasoning.chat']}]
  if op == 'trace.dispatch':
   if boundary == 'before': os._exit(73)
   store.create_task(sender_id='jev', recipient_id='jim-eq-hermes', payload={'body':p['request']}, task_id=p['child_task_id'])
   os._exit(73)
@asynccontextmanager
async def operation(*args): yield
async def progress(*args, **kwargs): pass
context = SimpleNamespace(client=Client(), trace=SimpleNamespace(binding_id='binding',operation=operation),publish_progress=progress)
c = m.Coordinator(journal, lambda *_:'single:jim-eq-hermes')
asyncio.run(c.handle({'task_id':'parent','sender_id':'operator','payload':{'body':'goal','args':{'request_id':'baf8810c-0ea3-4f06-b189-bbcc6d37bfa2'}}},context))
""")
    completed = subprocess.run(
        [sys.executable, str(script), str(SOURCE), str(tmp_path), boundary],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        timeout=15,
    )
    assert completed.returncode == 73
    journal = jev.Journal(tmp_path / "jev.sqlite3")
    with closing(AgentdStore(tmp_path / "remote.sqlite3")) as store:

        class Reader(Client):
            def call(self, op, **params):
                if op == "task.get":
                    from edgecitadel_agentd.store import StoreError

                    try:
                        return store.get_task_for(params["task_id"], "jev")
                    except StoreError as error:
                        raise AgentdClientError(str(error)) from error
                assert op != "trace.dispatch", (
                    "recovery must never repeat uncertain work"
                )
                return super().call(op, **params)

        coordinator = jev.Coordinator(journal, wait_seconds=0)
        context = Context(Reader())
        try:
            asyncio.run(coordinator.reconcile(context.client))
            run = journal.unfinished()[0]
            assert invoke(coordinator, context, resume(run))["outcome"] == "interrupted"
            assert len(store.list_tasks(actor_id="jev")) == (
                1 if boundary == "after" else 0
            )
        finally:
            journal.db.close()


def test_managed_trace_metadata_is_allowed_but_attachments_are_not(setup):
    from edgecitadel_agentd.trace_correlation import TaskTraceContext
    from datetime import datetime, UTC

    coordinator, context = setup
    request = envelope()
    request.update(
        v=1,
        id=str(uuid4()),
        type="command",
        recipient_id="jev",
        hop_count=0,
        timestamp=datetime.now(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    )
    stamped = TaskTraceContext(request["task_id"], str(uuid4()), uuid4().hex).apply(
        request
    )
    assert invoke(coordinator, context, stamped)["outcome"] == "completed"
    bad = envelope()
    bad["payload"]["attachments"] = ["file.pdf"]
    assert invoke(coordinator, context, bad)["outcome"] == "rejected"
