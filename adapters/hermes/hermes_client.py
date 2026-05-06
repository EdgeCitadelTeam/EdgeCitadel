"""HTTP/SSE client for Hermes Agent's OpenAI-compatible Chat Completions API.

Pure transport: no NATS awareness, no envelope construction. The adapter
glue calls `call_hermes_streaming(...)` and supplies a `publish_progress`
async callback that turns deltas into `task.progress` envelopes.

Memory ownership stays upstream — `session_id` is forwarded as
`X-Hermes-Session-Id` and Hermes honors it iff the Runs API is enabled.
"""
from __future__ import annotations
import json
import os
import time
from typing import Awaitable, Callable

import httpx

HERMES_BASE_URL = os.environ.get("HERMES_BASE_URL", "http://localhost:8642")
HERMES_MODEL = os.environ.get("HERMES_MODEL", "hermes")
HERMES_TIMEOUT_SEC = int(os.environ.get("HERMES_TIMEOUT_SEC", "300"))
# Bearer token; required at call time (not import time so tests can monkeypatch)
def _token() -> str:
    return os.environ["HERMES_TOKEN"]


FLUSH_TOKENS = 8
FLUSH_MS = 100


class HermesError(RuntimeError):
    """Network or HTTP failure talking to Hermes Agent."""


ProgressFn = Callable[[str], Awaitable[None]]


async def call_hermes_streaming(*, prompt: str, session_id: str | None,
                                 publish_progress: ProgressFn) -> str:
    """POST /v1/chat/completions with stream=true. Aggregate SSE deltas,
    flush to publish_progress on hybrid 8-token / 100ms cadence, return
    full joined text on completion. Raises HermesError on transport/HTTP
    failure."""
    headers = {
        "Authorization": f"Bearer {_token()}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    if session_id:
        headers["X-Hermes-Session-Id"] = session_id

    body = {
        "model": HERMES_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
    }

    full_parts: list[str] = []
    delta_buffer: list[str] = []
    last_flush = time.monotonic()

    async with httpx.AsyncClient(timeout=HERMES_TIMEOUT_SEC) as client:
        try:
            async with client.stream("POST",
                                     f"{HERMES_BASE_URL}/v1/chat/completions",
                                     headers=headers, json=body) as resp:
                if resp.status_code >= 400:
                    raise HermesError(f"http_{resp.status_code}")
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    delta = (chunk.get("choices", [{}])[0]
                                  .get("delta", {})
                                  .get("content", "") or "")
                    if delta:
                        full_parts.append(delta)
                        delta_buffer.append(delta)
                    elapsed_ms = (time.monotonic() - last_flush) * 1000
                    if (len(delta_buffer) >= FLUSH_TOKENS
                            or elapsed_ms >= FLUSH_MS):
                        if delta_buffer:
                            await publish_progress("".join(delta_buffer))
                            delta_buffer.clear()
                        last_flush = time.monotonic()
        except httpx.HTTPError as e:
            raise HermesError(str(e)) from e

    if delta_buffer:
        await publish_progress("".join(delta_buffer))
    return "".join(full_parts)
