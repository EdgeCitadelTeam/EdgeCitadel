"""Streaming Ollama /api/chat client with hybrid 8-tokens-or-100ms flush."""
from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from typing import Awaitable, Callable

import httpx

log = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "localhost")
OLLAMA_PORT = int(os.environ.get("OLLAMA_PORT", "11434"))
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gemma3:4b")
OLLAMA_TIMEOUT_SEC = int(os.environ.get("OLLAMA_TIMEOUT_SEC", "120"))

FLUSH_TOKENS = 8
FLUSH_MS = 100


def _ollama_url(path: str) -> str:
    return f"http://{OLLAMA_HOST}:{OLLAMA_PORT}{path}"


PublishProgress = Callable[[str], Awaitable[None]]


async def call_ollama_streaming(*, messages: list[dict], skill: dict,
                                 args: dict, use_json: bool,
                                 publish_progress: PublishProgress) -> str:
    """Stream tokens from Ollama /api/chat; publish via hybrid flush.

    Hybrid: flush on 8 tokens accumulated OR 100ms elapsed since last
    flush, whichever first. Returns the full joined text after the
    stream completes."""
    body: dict = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": True,
        "options": {**skill.get("defaults", {}), **(args or {})},
    }
    if use_json:
        body["format"] = "json"

    full: list[str] = []
    buf: list[str] = []
    last_flush = time.monotonic()

    async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT_SEC) as client:
        async with client.stream("POST", _ollama_url("/api/chat"),
                                 json=body) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError:
                    continue
                delta = (chunk.get("message") or {}).get("content", "")
                if delta:
                    full.append(delta)
                    buf.append(delta)
                elapsed_ms = (time.monotonic() - last_flush) * 1000
                if len(buf) >= FLUSH_TOKENS or elapsed_ms >= FLUSH_MS:
                    if buf:
                        await publish_progress("".join(buf))
                        buf.clear()
                    last_flush = time.monotonic()
                if chunk.get("done"):
                    break

    if buf:
        await publish_progress("".join(buf))

    return "".join(full)
