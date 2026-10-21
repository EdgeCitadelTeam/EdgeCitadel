"""Pieces the A2A baseline's agents, stack and native controller share.

The baseline speaks Google's A2A protocol through a2a-sdk (JSON-RPC over HTTP):
every workload agent is an A2A server, every runtime dispatch is a
``SendMessage`` with ``return_immediately`` whose single data part is the
det-worker.v1 request, and the operator's controls are ``ListTasks`` and
``CancelTask``. Cards are built from the stack's known addresses rather than
fetched, so no request depends on card discovery.

Only the A2A stack imports this module; EdgeCitadel runs never need a2a-sdk.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, TypeVar

import httpx
from a2a.client import A2AClientError, Client, ClientConfig, create_client
from a2a.types import a2a_pb2 as pb
from google.protobuf import json_format, struct_pb2

from .native import LoopThread, now_ms  # noqa: F401 (re-exported)

T = TypeVar("T")
SKILL_ID = "det.task"
PROTOCOL_BINDING = "JSONRPC"
# States an operator's sweep cancels: work that is queued or running.
LIVE_STATES = (pb.TaskState.TASK_STATE_SUBMITTED, pb.TaskState.TASK_STATE_WORKING)
LIST_PAGE_SIZE = 100


def agent_card(agent: str, url: str) -> pb.AgentCard:
    return pb.AgentCard(
        name=agent,
        description="Deterministic workload worker (containment experiment baseline).",
        version="0.1.0",
        supported_interfaces=[
            pb.AgentInterface(
                url=url, protocol_binding=PROTOCOL_BINDING, protocol_version="1.0"
            )
        ],
        capabilities=pb.AgentCapabilities(streaming=False),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=[
            pb.AgentSkill(
                id=SKILL_ID,
                name="det-task",
                description="Execute one planned deterministic workload task.",
                tags=["workload"],
            )
        ],
    )


def data_message(
    body: Mapping[str, Any], *, message_id: str, context_id: str | None = None
) -> pb.SendMessageRequest:
    """One det-worker.v1 request as an A2A message, returned at submission."""
    value = struct_pb2.Value()
    json_format.ParseDict(dict(body), value)
    return pb.SendMessageRequest(
        message=pb.Message(
            message_id=message_id,
            context_id=context_id or "",
            role=pb.Role.ROLE_USER,
            parts=[pb.Part(data=value, media_type="application/json")],
        ),
        configuration=pb.SendMessageConfiguration(return_immediately=True),
    )


def request_body(message: pb.Message | None) -> dict[str, Any] | None:
    """The det-worker.v1 request a message carries, or None."""
    if message is None:
        return None
    for part in message.parts:
        if part.HasField("data"):
            body = json_format.MessageToDict(part.data)
            return body if isinstance(body, dict) else None
    return None


def is_transport_error(error: BaseException) -> bool:
    """A request that may not have reached the agent (connection, timeout)."""
    if not isinstance(error, A2AClientError):
        return False
    cause = error.__cause__
    return isinstance(cause, httpx.RequestError) or "timed out" in str(error)


class Peers:
    """One a2a-sdk client per agent server, over one shared HTTP connection pool."""

    def __init__(self, urls: Mapping[str, str], *, timeout: float = 30.0) -> None:
        self.urls = dict(urls)
        self._http = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
            trust_env=False,  # never route the experiment through an egress proxy
        )
        self._clients: dict[str, Client] = {}

    async def client(self, agent: str) -> Client:
        if agent not in self._clients:
            self._clients[agent] = await create_client(
                agent_card(agent, self.urls[agent]),
                ClientConfig(streaming=False, httpx_client=self._http),
            )
        return self._clients[agent]

    async def send(
        self, agent: str, body: Mapping[str, Any], *, message_id: str, context_id: str
    ) -> pb.Task:
        client = await self.client(agent)
        request = data_message(body, message_id=message_id, context_id=context_id)
        task: pb.Task | None = None
        async for response in client.send_message(request):
            if response.HasField("task"):
                task = response.task
        if task is None:
            raise A2AClientError("the agent answered without a task")
        return task

    async def live_tasks(self, agent: str) -> list[pb.Task]:
        """Every queued or running task on one agent server (ListTasks, paged)."""
        client = await self.client(agent)
        tasks: list[pb.Task] = []
        for state in LIVE_STATES:
            token = ""
            while True:
                page = await client.list_tasks(
                    pb.ListTasksRequest(
                        status=state, page_size=LIST_PAGE_SIZE, page_token=token
                    )
                )
                tasks.extend(page.tasks)
                token = page.next_page_token
                if not token:
                    break
        return tasks

    async def cancel(self, agent: str, task_id: str) -> pb.Task:
        client = await self.client(agent)
        return await client.cancel_task(pb.CancelTaskRequest(id=task_id))

    async def post(
        self, url: str, body: Mapping[str, Any], headers: Mapping[str, str]
    ) -> httpx.Response:
        return await self._http.post(url, json=dict(body), headers=dict(headers))

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()
        await self._http.aclose()


def unique(items: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(items))
