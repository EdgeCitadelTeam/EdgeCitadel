import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

from edgecitadel_hermes_plugin.delegation import (
    bind_agent_execution,
    scoped_delegate_handler,
)


def test_parallel_execution_scope_stays_outside_model_arguments():
    calls = []
    barrier = Barrier(2)

    def transport(name, arguments, metadata):
        calls.append((name, arguments, metadata))
        return {"task_id": str(uuid4())}

    delegate = scoped_delegate_handler(transport)

    class Agent:
        def run_conversation(self, **kwargs):
            barrier.wait(timeout=2)
            result = delegate({"recipient_id": "worker", "request": "owned child"})
            assert (
                json.loads(
                    delegate(
                        {
                            "recipient_id": "worker",
                            "request": "owned child",
                            "binding_id": "forged",
                        }
                    )
                )["error"]
                == "invalid_delegation_arguments"
            )
            return kwargs["task_id"], result

    traces = [
        SimpleNamespace(binding_id=str(uuid4()), task_id=str(uuid4())) for _ in range(2)
    ]
    agents = [Agent(), Agent()]
    for agent, trace in zip(agents, traces):
        bind_agent_execution(agent, trace)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda agent: agent.run_conversation(task_id="conversation"), agents
            )
        )
    assert {r[0] for r in results} == {t.task_id for t in traces}
    assert {c[2]["edgecitadel_execution"]["binding_id"] for c in calls} == {
        t.binding_id for t in traces
    }
    assert len({c[2]["edgecitadel_execution"]["request_id"] for c in calls}) == 2
    assert all(set(c[1]) == {"recipient_id", "request"} for c in calls)
    assert (
        json.loads(delegate({"recipient_id": "worker", "request": "outside run"}))[
            "error"
        ]
        == "execution_binding_unavailable"
    )
    assert len(calls) == 2
