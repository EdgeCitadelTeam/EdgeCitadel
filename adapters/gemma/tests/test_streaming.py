"""Tests for streaming Ollama client + hybrid flush + cancellation."""
import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from adapters.gemma.ollama_client import call_ollama_streaming


class FakeStreamResponse:
    def __init__(self, lines):
        self._lines = lines

    def raise_for_status(self):
        pass

    async def aiter_lines(self):
        for line in self._lines:
            yield line


class FakeStreamCM:
    def __init__(self, lines):
        self._lines = lines

    async def __aenter__(self):
        return FakeStreamResponse(self._lines)

    async def __aexit__(self, *exc):
        return False


class FakeAsyncClient:
    def __init__(self, lines):
        self._lines = lines

    def stream(self, method, url, **kw):
        self._last_kw = kw
        return FakeStreamCM(self._lines)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _ollama_lines(tokens, done_after=True):
    """Mimic Ollama /api/chat streaming response (one JSON object per line)."""
    out = []
    for t in tokens:
        out.append(json.dumps({"message": {"content": t}, "done": False}))
    if done_after:
        out.append(json.dumps({"message": {"content": ""}, "done": True}))
    return out


@pytest.mark.asyncio
async def test_stream_flushes_on_token_threshold(monkeypatch):
    tokens = [chr(ord("A") + i) for i in range(16)]   # 16 tokens
    fake_client = FakeAsyncClient(_ollama_lines(tokens))
    monkeypatch.setattr("adapters.gemma.ollama_client.httpx.AsyncClient",
                        lambda **kw: fake_client)

    publishes = []
    async def publish(delta):
        publishes.append(delta)

    text = await call_ollama_streaming(
        messages=[{"role": "user", "content": "x"}],
        skill={"defaults": {}}, args={}, use_json=False,
        publish_progress=publish)

    assert text == "".join(tokens)
    # 16 tokens flushed in batches of 8 → 2 publishes
    assert len(publishes) == 2
    assert publishes[0] == "".join(tokens[:8])
    assert publishes[1] == "".join(tokens[8:])


@pytest.mark.asyncio
async def test_stream_final_result_carries_full_text(monkeypatch):
    tokens = ["Hello, ", "world", "!"]
    fake_client = FakeAsyncClient(_ollama_lines(tokens))
    monkeypatch.setattr("adapters.gemma.ollama_client.httpx.AsyncClient",
                        lambda **kw: fake_client)

    text = await call_ollama_streaming(
        messages=[{"role": "user", "content": "x"}],
        skill={"defaults": {}}, args={}, use_json=False,
        publish_progress=AsyncMock())

    assert text == "Hello, world!"


@pytest.mark.asyncio
async def test_stream_uses_json_format_when_requested(monkeypatch):
    fake_client = FakeAsyncClient(_ollama_lines(['{"label":"x"}']))
    monkeypatch.setattr("adapters.gemma.ollama_client.httpx.AsyncClient",
                        lambda **kw: fake_client)

    await call_ollama_streaming(
        messages=[{"role": "user", "content": "x"}],
        skill={"defaults": {"temperature": 0.1}}, args={}, use_json=True,
        publish_progress=AsyncMock())

    body = fake_client._last_kw["json"]
    assert body["format"] == "json"
    assert body["stream"] is True
    assert body["options"]["temperature"] == 0.1


@pytest.mark.asyncio
async def test_stream_merges_skill_defaults_with_call_args(monkeypatch):
    fake_client = FakeAsyncClient(_ollama_lines(["x"]))
    monkeypatch.setattr("adapters.gemma.ollama_client.httpx.AsyncClient",
                        lambda **kw: fake_client)

    await call_ollama_streaming(
        messages=[{"role": "user", "content": "x"}],
        skill={"defaults": {"temperature": 0.3}},
        args={"temperature": 0.9},   # caller override wins
        use_json=False,
        publish_progress=AsyncMock())

    body = fake_client._last_kw["json"]
    assert body["options"]["temperature"] == 0.9
