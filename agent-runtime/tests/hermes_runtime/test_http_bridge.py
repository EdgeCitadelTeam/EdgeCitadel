import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from aiohttp import web
from edgecitadel_hermes_plugin.http_bridge import bound_api_adapter


@pytest.mark.asyncio
async def test_http_binding_gate_and_parallel_callback_scope():
    context, session = str(uuid4()), str(uuid4())
    tasks = {str(uuid4()): str(uuid4()) for _ in range(2)}
    observations = []
    binds = []

    class Client:
        def call(self, operation, **params):
            if operation == "trace.bind":
                binds.append(params)
                return {
                    "schema_version": 1,
                    "operation": "bind",
                    "request_id": params["request_id"],
                    "status": "ok",
                    "result": {
                        "binding_id": tasks[params["task_id"]],
                        "task_id": params["task_id"],
                        "trace_id": "a" * 32,
                        "context_id": context,
                        "execution_attempt_id": str(uuid4()),
                    },
                }
            observations.append(params)
            return {
                "schema_version": 1,
                "operation": "append",
                "request_id": params["observation_id"],
                "status": "ok",
                "result": {
                    "event_id": str(uuid4()),
                    "source_epoch": str(uuid4()),
                    "source_seq": len(observations),
                },
            }

    class Base:
        def _check_auth(self, request):
            return (
                None
                if request.headers.get("Authorization") == "owned"
                else web.Response(status=401)
            )

        async def _handle_chat_completions(self, request):
            await asyncio.sleep(0)
            return await self._run_agent()

        async def _run_agent(self, **kwargs):
            if kwargs:
                await asyncio.to_thread(
                    kwargs["tool_start_callback"],
                    "same-id",
                    "owned-tool",
                    {"secret": "private"},
                )
                await asyncio.sleep(0)
                await asyncio.to_thread(
                    kwargs["tool_complete_callback"],
                    "same-id",
                    "owned-tool",
                    {},
                    "private",
                )
            return "executed"

    bridge = bound_api_adapter(Base)(trace_client=Client())

    def request(task, **extra):
        return SimpleNamespace(
            headers={
                "Authorization": "owned",
                "X-Hermes-Session-Id": context,
                "X-EdgeCitadel-Run-Binding": tasks[task],
                "X-EdgeCitadel-Task-Id": task,
                "X-EdgeCitadel-Session-Id": session,
                **extra,
            }
        )

    results = await asyncio.gather(
        *(bridge._handle_chat_completions(request(task)) for task in tasks)
    )
    assert results == ["executed", "executed"]
    assert len(observations) == 4
    for binding in tasks.values():
        pair = [p["observation"] for p in observations if p["binding_id"] == binding]
        assert [p["phase"] for p in pair] == ["started", "finished"]
        assert pair[0]["span_id"] == pair[1]["span_id"]
    assert "private" not in str(observations)
    task = next(iter(tasks))
    for extra, status in [
        ({"Authorization": "bad"}, 401),
        ({"X-Hermes-Session-Id": str(uuid4())}, 403),
        ({"X-EdgeCitadel-Run-Binding": str(uuid4())}, 403),
        ({"X-EdgeCitadel-Session-Id": ""}, 400),
    ]:
        reply = await bridge._handle_chat_completions(request(task, **extra))
        assert reply.status == status
    assert len(observations) == 4
    assert (
        await bridge._handle_chat_completions(
            SimpleNamespace(headers={"Authorization": "owned"})
        )
        == "executed"
    )
    assert len(observations) == 4
