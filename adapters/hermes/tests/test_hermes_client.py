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
