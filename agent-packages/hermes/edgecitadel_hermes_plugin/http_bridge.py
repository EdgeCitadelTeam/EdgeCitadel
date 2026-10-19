"""Opt-in Hermes API adapter binding; upstream installation stays unchanged."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
from typing import Any, cast

from aiohttp import web

from edgecitadel_agentd.trace_producer import RuntimeTrace

from .delegation import bind_agent_execution
from .trace_hooks import HermesModelObserver, HermesToolObserver


def bound_api_adapter(base_adapter: type) -> type:
    """Wrap the installed APIServerAdapter with an explicit agentd client.

    The server's connector credential authenticates binding lookup. HTTP metadata
    supplies correlation only and cannot grant access to a different connector.
    """

    class BoundAdapter(base_adapter):
        def __init__(self, *args: Any, trace_client: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._trace_client = trace_client
            self._request_trace: ContextVar[RuntimeTrace | None] = ContextVar(
                "hermes_request_trace", default=None
            )

        async def _handle_chat_completions(self, request: Any) -> Any:
            auth_error = self._check_auth(request)
            if auth_error is not None:
                return auth_error
            names = (
                "X-EdgeCitadel-Run-Binding",
                "X-EdgeCitadel-Session-Id",
                "X-EdgeCitadel-Task-Id",
            )
            supplied = [request.headers.get(name) for name in names]
            if not any(supplied):
                return await super()._handle_chat_completions(request)
            if not all(supplied):
                return web.json_response(
                    {"error": "incomplete_execution_binding"}, status=400
                )
            trace = RuntimeTrace(self._trace_client)
            await trace.bind(session_id=supplied[1], task_id=supplied[2])
            if trace.binding_id != supplied[
                0
            ] or trace.context_id != request.headers.get("X-Hermes-Session-Id"):
                return web.json_response(
                    {"error": "execution_binding_unavailable"}, status=403
                )
            scope = self._request_trace.set(trace)
            try:
                return await super()._handle_chat_completions(request)
            finally:
                await trace.report_loss()
                self._request_trace.reset(scope)

        async def _run_agent(self, *args: Any, **kwargs: Any) -> Any:
            trace = self._request_trace.get()
            if trace is not None:
                observer = HermesToolObserver(trace, asyncio.get_running_loop())
                for name, callback in (
                    ("tool_start_callback", observer.started),
                    ("tool_complete_callback", observer.completed),
                ):
                    previous = kwargs.get(name)

                    def chained(
                        *values: Any, callback: Any = callback, previous: Any = previous
                    ) -> None:
                        callback(*values)
                        if previous is not None:
                            previous(*values)

                    cast(Any, chained)._edgecitadel_trace = (
                        trace,
                        asyncio.get_running_loop(),
                        observer,
                    )
                    kwargs[name] = chained
            return await super()._run_agent(*args, **kwargs)

        def _create_agent(self, *args: Any, **kwargs: Any) -> Any:
            agent = super()._create_agent(*args, **kwargs)
            scope = getattr(
                kwargs.get("tool_start_callback"), "_edgecitadel_trace", None
            )
            if scope is not None:
                HermesModelObserver(*scope).attach(agent)
                bind_agent_execution(agent, scope[0])
            return agent

    return BoundAdapter
