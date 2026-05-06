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
