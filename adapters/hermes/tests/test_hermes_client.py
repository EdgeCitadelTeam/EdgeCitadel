"""Unit tests for the Hermes SSE streaming client (respx-mocked)."""
from __future__ import annotations
import json

import httpx
import pytest
import respx


# Helper: format an SSE chunk in the OpenAI Chat Completions delta shape
def _sse_chunk(content: str) -> bytes:
    obj = {"choices": [{"delta": {"content": content}}]}
    return f"data: {json.dumps(obj)}\n\n".encode()


def _sse_done() -> bytes:
    return b"data: [DONE]\n\n"


@pytest.mark.asyncio
@respx.mock
async def test_streaming_aggregates_deltas():
    from adapters.hermes.hermes_client import call_hermes_streaming

    # Stream "Hello world!" as three chunks then DONE
    body = b"".join([_sse_chunk("Hello"), _sse_chunk(" world"),
                     _sse_chunk("!"), _sse_done()])
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    captured: list[str] = []

    async def publish(delta: str) -> None:
        captured.append(delta)

    full = await call_hermes_streaming(
        prompt="hi", session_id=None, publish_progress=publish)

    assert full == "Hello world!"
    # Final flush coalesces remaining buffer; full text must be reconstructable
    assert "".join(captured) == "Hello world!"


@pytest.mark.asyncio
@respx.mock
async def test_streaming_flushes_on_token_threshold():
    """8 deltas in quick succession must trigger exactly one flush."""
    from adapters.hermes.hermes_client import call_hermes_streaming

    body = b"".join([_sse_chunk(f"t{i}") for i in range(8)] + [_sse_done()])
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    flushes: list[str] = []

    async def publish(delta: str) -> None:
        flushes.append(delta)

    full = await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    # Exactly one flush at the 8th token; no trailing flush because buffer is empty
    assert len(flushes) == 1
    assert flushes[0] == "t0t1t2t3t4t5t6t7"
    assert full == "t0t1t2t3t4t5t6t7"


@pytest.mark.asyncio
@respx.mock
async def test_streaming_final_flush_drains_buffer():
    """If stream ends with <8 tokens buffered, the trailing flush carries them."""
    from adapters.hermes.hermes_client import call_hermes_streaming

    body = b"".join([_sse_chunk("a"), _sse_chunk("b"), _sse_done()])
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    flushes: list[str] = []

    async def publish(delta: str) -> None:
        flushes.append(delta)

    full = await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    assert flushes == ["ab"]
    assert full == "ab"


@pytest.mark.asyncio
@respx.mock
async def test_done_marker_terminates_stream():
    """[DONE] mid-stream halts iteration; later chunks are ignored."""
    from adapters.hermes.hermes_client import call_hermes_streaming

    body = b"".join([_sse_chunk("first"),
                     _sse_done(),
                     _sse_chunk("ignored")])  # this should never be read
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    flushes: list[str] = []

    async def publish(delta: str) -> None:
        flushes.append(delta)

    full = await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    assert full == "first"


@pytest.mark.asyncio
@respx.mock
async def test_unknown_sse_event_lines_ignored():
    """Hermes' custom `event: hermes.tool.progress` lines and other non-data
    lines are tolerated without error or contribution to text."""
    from adapters.hermes.hermes_client import call_hermes_streaming

    body = (b"event: hermes.tool.progress\n"
            b"data: {\"tool\":\"web_search\",\"progress\":0.5}\n\n"
            + _sse_chunk("ok")
            + _sse_done())
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    flushes: list[str] = []

    async def publish(delta: str) -> None:
        flushes.append(delta)

    full = await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    # The tool-progress JSON has no `choices[0].delta.content` so it
    # contributes nothing. The "ok" chunk is the only text.
    assert full == "ok"


@pytest.mark.asyncio
@respx.mock
async def test_malformed_data_line_skipped():
    """Bad JSON in a data: line is logged-and-skipped, not raised."""
    from adapters.hermes.hermes_client import call_hermes_streaming

    body = (b"data: {not json}\n\n"
            + _sse_chunk("good")
            + _sse_done())
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(
            200, content=body,
            headers={"Content-Type": "text/event-stream"}))

    flushes: list[str] = []

    async def publish(delta: str) -> None:
        flushes.append(delta)

    full = await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    assert full == "good"


@pytest.mark.asyncio
@respx.mock
async def test_http_400_raises_hermes_error():
    from adapters.hermes.hermes_client import (
        call_hermes_streaming, HermesError)
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(400, json={"error": "bad request"}))

    async def publish(_): pass

    with pytest.raises(HermesError, match="http_400"):
        await call_hermes_streaming(
            prompt="x", session_id=None, publish_progress=publish)


@pytest.mark.asyncio
@respx.mock
async def test_http_401_raises_hermes_error():
    from adapters.hermes.hermes_client import (
        call_hermes_streaming, HermesError)
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized"}))

    async def publish(_): pass

    with pytest.raises(HermesError, match="http_401"):
        await call_hermes_streaming(
            prompt="x", session_id=None, publish_progress=publish)


@pytest.mark.asyncio
@respx.mock
async def test_http_500_raises_hermes_error():
    from adapters.hermes.hermes_client import (
        call_hermes_streaming, HermesError)
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        return_value=httpx.Response(500))

    async def publish(_): pass

    with pytest.raises(HermesError, match="http_500"):
        await call_hermes_streaming(
            prompt="x", session_id=None, publish_progress=publish)


@pytest.mark.asyncio
@respx.mock
async def test_connect_error_raises_hermes_error():
    from adapters.hermes.hermes_client import (
        call_hermes_streaming, HermesError)
    respx.post("http://localhost:8642/v1/chat/completions").mock(
        side_effect=httpx.ConnectError("connection refused"))

    async def publish(_): pass

    with pytest.raises(HermesError):
        await call_hermes_streaming(
            prompt="x", session_id=None, publish_progress=publish)


@pytest.mark.asyncio
@respx.mock
async def test_session_id_header_set_when_context_id_present():
    from adapters.hermes.hermes_client import call_hermes_streaming

    captured_request = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured_request["headers"] = dict(request.headers)
        return httpx.Response(
            200, content=_sse_chunk("ok") + _sse_done(),
            headers={"Content-Type": "text/event-stream"})

    respx.post("http://localhost:8642/v1/chat/completions").mock(
        side_effect=_capture)

    async def publish(_): pass

    await call_hermes_streaming(
        prompt="x", session_id="ctx-abc-123", publish_progress=publish)

    assert captured_request["headers"].get("x-hermes-session-id") == "ctx-abc-123"


@pytest.mark.asyncio
@respx.mock
async def test_session_id_header_omitted_when_none():
    from adapters.hermes.hermes_client import call_hermes_streaming

    captured_request = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured_request["headers"] = dict(request.headers)
        return httpx.Response(
            200, content=_sse_chunk("ok") + _sse_done(),
            headers={"Content-Type": "text/event-stream"})

    respx.post("http://localhost:8642/v1/chat/completions").mock(
        side_effect=_capture)

    async def publish(_): pass

    await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    assert "x-hermes-session-id" not in captured_request["headers"]


@pytest.mark.asyncio
@respx.mock
async def test_authorization_bearer_header_set():
    from adapters.hermes.hermes_client import call_hermes_streaming

    captured_request = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured_request["auth"] = request.headers.get("authorization")
        return httpx.Response(
            200, content=_sse_chunk("ok") + _sse_done(),
            headers={"Content-Type": "text/event-stream"})

    respx.post("http://localhost:8642/v1/chat/completions").mock(
        side_effect=_capture)

    async def publish(_): pass

    await call_hermes_streaming(
        prompt="x", session_id=None, publish_progress=publish)

    assert captured_request["auth"] == "Bearer test-token"
