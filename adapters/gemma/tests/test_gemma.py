"""Unit tests for the Gemma adapter handle() — no live Ollama."""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from adapters.gemma.adapter import handle
from adapters._common.pull_consumer import Context


def _ctx():
    ctx = Context(agent_id="gemma-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    return ctx


def _cmd(body="What is 2+2?", args=None, type="command",
         context_id=None):
    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": type,
           "sender_id": "tester", "recipient_id": "gemma-1",
           "task_id": "22222222-3333-4444-8555-666666666666",
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": body, **({"args": args} if args else {})}}
    if context_id is not None:
        env["context_id"] = context_id
    return env


class _FakeResp:
    def __init__(self, status_code, json_body=None, text=""):
        self.status_code = status_code
        self._json_body = json_body
        self.text = text

    def json(self):
        if self._json_body is None:
            raise ValueError("no JSON body")
        return self._json_body


@pytest.mark.asyncio
async def test_handle_command_calls_ollama(monkeypatch):
    captured = {}

    async def fake_post(self, url, json=None, timeout=None, **kw):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return _FakeResp(200, json_body={"response": "4", "model": "gemma3:4b"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())

    assert state == "completed"
    assert payload["body"] == "4"
    assert payload["model"] == "gemma3:4b"
    assert "duration_ms" in payload
    assert captured["url"].endswith("/api/generate")
    assert captured["json"]["model"] == "gemma3:4b"
    assert captured["json"]["prompt"] == "What is 2+2?"
    assert captured["json"]["stream"] is False


@pytest.mark.asyncio
async def test_handle_rejects_non_command():
    env = {"v": 1, "id": "x", "type": "delegation",
           "sender_id": "planner", "recipient_id": "gemma-1",
           "task_id": "t", "context_id": "c", "hop_count": 0,
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": "ignored"}}
    payload, state = await handle(env, _ctx())
    assert state == "rejected"
    assert payload["error"] == "unsupported_type"


@pytest.mark.asyncio
async def test_handle_rejects_empty_prompt():
    payload, state = await handle(_cmd(body="   "), _ctx())
    assert state == "rejected"
    assert payload["error"] == "empty_prompt"


@pytest.mark.asyncio
async def test_handle_ollama_unreachable_returns_failed(monkeypatch):
    import httpx

    async def fake_post(self, url, **kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_unreachable"


@pytest.mark.asyncio
async def test_handle_ollama_timeout_returns_failed(monkeypatch):
    import httpx

    async def fake_post(self, url, **kw):
        raise httpx.ReadTimeout("read timed out")

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(args={"timeout_sec": 1}), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_timeout"


@pytest.mark.asyncio
async def test_handle_model_not_loaded_returns_failed(monkeypatch):
    async def fake_post(self, url, **kw):
        return _FakeResp(404,
                         json_body={"error": "model 'fake' not found, try pulling it first"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "model_not_loaded"


@pytest.mark.asyncio
async def test_handle_ollama_5xx_returns_failed(monkeypatch):
    async def fake_post(self, url, **kw):
        return _FakeResp(500, json_body={"error": "internal"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(_cmd(), _ctx())
    assert state == "failed"
    assert payload["error"] == "ollama_inference_error"


@pytest.mark.asyncio
async def test_handle_args_override_model_and_temperature(monkeypatch):
    captured = {}

    async def fake_post(self, url, json=None, **kw):
        captured["json"] = json
        return _FakeResp(200, json_body={"response": "ok", "model": "gemma3:12b"})

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)
    payload, state = await handle(
        _cmd(args={"model": "gemma3:12b", "temperature": 0.2,
                   "max_tokens": 512}), _ctx())
    assert state == "completed"
    assert captured["json"]["model"] == "gemma3:12b"
    assert captured["json"]["options"]["temperature"] == 0.2
    assert captured["json"]["options"]["num_predict"] == 512


@pytest.mark.asyncio
async def test_preflight_passes_when_model_listed(monkeypatch):
    from adapters.gemma.adapter import preflight

    async def fake_get(self, url, **kw):
        return _FakeResp(200, json_body={
            "models": [{"name": "gemma3:4b"}, {"name": "llama3.2:3b"}]
        })

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    monkeypatch.setenv("OLLAMA_MODEL", "gemma3:4b")
    # Should not raise
    await preflight()


@pytest.mark.asyncio
async def test_preflight_raises_when_unreachable(monkeypatch):
    from adapters.gemma.adapter import preflight, PreflightError
    import httpx

    async def fake_get(self, url, **kw):
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    with pytest.raises(PreflightError, match="ollama_unreachable"):
        await preflight()


@pytest.mark.asyncio
async def test_preflight_raises_when_model_not_found(monkeypatch):
    from adapters.gemma.adapter import preflight, PreflightError

    async def fake_get(self, url, **kw):
        return _FakeResp(200, json_body={
            "models": [{"name": "llama3.2:3b"}]   # gemma3:4b absent
        })

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    monkeypatch.setenv("OLLAMA_MODEL", "gemma3:4b")
    with pytest.raises(PreflightError, match="model_not_loaded"):
        await preflight()


@pytest.mark.asyncio
async def test_preflight_accepts_bare_name_when_only_latest_tag_listed(monkeypatch):
    """Operators commonly set OLLAMA_MODEL=gemma4 expecting an implicit
    :latest tag. Ollama lists pulled models as 'name:tag' (e.g.
    'gemma4:latest'), so the bare form must match the :latest entry."""
    from adapters.gemma.adapter import preflight

    async def fake_get(self, url, **kw):
        return _FakeResp(200, json_body={
            "models": [{"name": "gemma4:latest"}, {"name": "llama3.1:8b"}]
        })

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)
    monkeypatch.setenv("OLLAMA_MODEL", "gemma4")
    await preflight()  # should not raise
