"""Explicitly mounted fleet-authenticated HTTP/WS reads; no default app wiring."""

from __future__ import annotations

import asyncio
import json
import re
import time
from contextlib import asynccontextmanager, suppress

from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from starlette.websockets import WebSocketState

from edgecitadel_agentd.trace_contract import validate_read_response

from .trace_event_pages import TraceReadError
from .trace_read_service import TraceReadService

AUTH_TIMEOUT_SECONDS = 5.0
SEND_TIMEOUT_SECONDS = 2.0
POLL_SECONDS = 0.25
HEARTBEAT_SECONDS = 15.0
MAX_SOCKETS = 8


def _parameters(query, allowed: set[str], required: set[str] = frozenset()) -> dict:
    if (
        set(query) - allowed
        or any(len(query.getlist(key)) != 1 for key in query)
        or required - set(query)
    ):
        raise TraceReadError("invalid_request")
    result = dict(query)
    if any(len(value) > 4096 for value in result.values()):
        raise TraceReadError("invalid_request")
    if "limit" in result:
        if re.fullmatch(r"[0-9]{1,3}", result["limit"]) is None:
            raise TraceReadError("invalid_request")
        result["limit"] = int(result["limit"])
    return result


def _response(body: dict, status: int = 200) -> Response:
    return Response(
        validate_read_response(body),
        status_code=status,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store",
            "X-EdgeCitadel-Access-Mode": "trusted-fleet",
        },
    )


async def _send(socket: WebSocket, body: dict) -> None:
    await asyncio.wait_for(
        socket.send_text(validate_read_response(body).decode()), SEND_TIMEOUT_SECONDS
    )


def make_trace_router(service: TraceReadService) -> APIRouter:
    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await shutdown()

    router = APIRouter(lifespan=lifespan)
    sockets = asyncio.Semaphore(MAX_SOCKETS)
    stopping = asyncio.Event()
    sessions: set[asyncio.Task] = set()

    async def http(
        request: Request, kind: str, trace_id: str | None = None
    ) -> Response:
        credentials = request.headers.getlist("authorization")
        authorization = credentials[0] if len(credentials) == 1 else ""
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not service.authorized(token):
            return _response(TraceReadError("not_authorized").response, 401)
        origins = request.headers.getlist("origin")
        if len(origins) > 1 or not service.origin_allowed(
            origins[0] if origins else None
        ):
            return _response(TraceReadError("not_authorized").response, 403)
        try:
            allowed = {
                "list": {"cursor", "agent_id", "outcome", "task_id", "limit"},
                "graph": {"at", "expand"},
                "events": {"as_of", "after", "node_id", "limit"},
                "changes": {"after", "limit"},
            }[kind]
            required = (
                {"as_of"}
                if kind == "events"
                else {"after"}
                if kind == "changes"
                else set()
            )
            parameters = _parameters(request.query_params, allowed, required)
            if trace_id is not None:
                parameters["trace_id"] = trace_id
            return _response(await service.query(kind, **parameters))
        except TraceReadError as error:
            return _response(error.response, error.status_code)

    @router.get("/api/traces")
    async def trace_list(request: Request):
        return await http(request, "list")

    @router.get("/api/traces/{trace_id}")
    async def trace_graph(request: Request, trace_id: str):
        return await http(request, "graph", trace_id)

    @router.get("/api/traces/{trace_id}/events")
    async def trace_events(request: Request, trace_id: str):
        return await http(request, "events", trace_id)

    @router.get("/api/traces/{trace_id}/changes")
    async def trace_changes(request: Request, trace_id: str):
        return await http(request, "changes", trace_id)

    @router.websocket("/ws/traces/{trace_id}")
    async def trace_socket(socket: WebSocket, trace_id: str):
        origins = socket.headers.getlist("origin")
        if len(origins) > 1 or not service.origin_allowed(
            origins[0] if origins else None
        ):
            await socket.close(code=4403)
            return
        if stopping.is_set() or sockets.locked():
            await socket.close(code=1013)
            return
        await sockets.acquire()
        task = asyncio.current_task()
        sessions.add(task)
        disconnected = None
        stopped = None
        close_code = 1000
        try:
            await socket.accept()
            # Browser WebSockets cannot set Authorization. The opening frame is
            # the only credential transport; credentials never enter URLs/cursors.
            frame = await asyncio.wait_for(socket.receive(), AUTH_TIMEOUT_SECONDS)
            text = frame.get("text")
            if frame["type"] == "websocket.disconnect":
                return
            try:
                credentials = (
                    json.loads(text)
                    if isinstance(text, str) and len(text) <= 512
                    else None
                )
            except json.JSONDecodeError:
                credentials = None
            if (
                not isinstance(credentials, dict)
                or set(credentials) != {"type", "token"}
                or credentials["type"] != "authenticate"
                or not service.authorized(credentials["token"])
            ):
                await _send(socket, TraceReadError("not_authorized").response)
                close_code = 4401
                return
            parameters = _parameters(socket.query_params, {"after"}, {"after"})
            after = parameters["after"]
            disconnected = asyncio.create_task(socket.receive())
            stopped = asyncio.create_task(stopping.wait())

            async def while_connected(operation):
                running = asyncio.create_task(operation)
                try:
                    done, _ = await asyncio.wait(
                        {running, disconnected, stopped},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if disconnected in done or stopped in done:
                        raise WebSocketDisconnect(1000)
                    return await running
                finally:
                    if not running.done():
                        running.cancel()
                    await asyncio.gather(running, return_exceptions=True)

            last_heartbeat = 0.0
            while not stopping.is_set():
                page = await while_connected(
                    service.query("changes", trace_id=trace_id, after=after)
                )
                for change in page["changes"]:
                    await _send(
                        socket,
                        {
                            "schema_version": 1,
                            "kind": "trace_change",
                            "trace_id": trace_id,
                            "projection_generation": page["projection_generation"],
                            "change": change,
                        },
                    )
                # This is server delivery position, never an application ACK.
                # Clients reconnect with their own last fully applied cursor.
                after = page["through_cursor"]
                if page["next_cursor"]:
                    continue
                if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                    status = await while_connected(service.query("status"))
                    if status["generation"] != page["projection_generation"]:
                        raise TraceReadError("generation_changed")
                    await _send(
                        socket,
                        {
                            "schema_version": 1,
                            "kind": "trace_heartbeat",
                            "trace_id": trace_id,
                            "projection_generation": page["projection_generation"],
                            "through_cursor": after,
                            "freshness": status["freshness"],
                        },
                    )
                    last_heartbeat = time.monotonic()
                await while_connected(asyncio.sleep(POLL_SECONDS))
        except TraceReadError as error:
            close_code = 1013 if error.response["retryable"] else 1008
            with suppress(WebSocketDisconnect, TimeoutError):
                await _send(socket, error.response)
        except TimeoutError:
            close_code = 1013
        except WebSocketDisconnect:
            pass
        finally:
            for pending in (disconnected, stopped):
                if pending is not None:
                    pending.cancel()
                    with suppress(asyncio.CancelledError):
                        await pending
            sessions.discard(task)
            sockets.release()
            if socket.application_state != WebSocketState.DISCONNECTED:
                with suppress(WebSocketDisconnect, TimeoutError):
                    await asyncio.wait_for(
                        socket.close(code=close_code), SEND_TIMEOUT_SECONDS
                    )

    async def shutdown():
        stopping.set()
        await asyncio.to_thread(service.close)
        if sessions:
            _, pending = await asyncio.wait(
                tuple(sessions), timeout=AUTH_TIMEOUT_SECONDS + SEND_TIMEOUT_SECONDS
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    return router
