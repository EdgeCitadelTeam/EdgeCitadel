import json
from unittest.mock import MagicMock

import pytest
from edgecitadel_gemma_plugin import adapter


def test_chat_calls_basic_non_streaming_ollama_api(monkeypatch):
    response = MagicMock()
    response.__enter__.return_value.read.return_value = json.dumps(
        {"message": {"content": "hello from Gemma"}}
    ).encode()
    urlopen = MagicMock(return_value=response)
    monkeypatch.setattr(adapter.urllib.request, "urlopen", urlopen)

    body, model = adapter._chat("hello")

    request = urlopen.call_args.args[0]
    payload = json.loads(request.data)
    assert request.full_url == "http://127.0.0.1:11434/api/chat"
    assert payload == {
        "model": "gemma3:1b",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": False,
    }
    assert (body, model) == ("hello from Gemma", "gemma3:1b")


@pytest.mark.asyncio
async def test_handle_returns_model_response(monkeypatch):
    monkeypatch.setattr(adapter, "_chat", lambda prompt: (prompt.upper(), "gemma3:1b"))

    result, state = await adapter.handle(
        {"type": "command", "payload": {"body": "hello", "skill_id": "reasoning.chat"}},
        MagicMock(),
    )

    assert state == "completed"
    assert result == {
        "body": "HELLO",
        "model": "gemma3:1b",
        "skill_id": "reasoning.chat",
    }


@pytest.mark.asyncio
async def test_handle_rejects_unknown_skill():
    result, state = await adapter.handle(
        {"type": "command", "payload": {"body": "hello", "skill_id": "text.summarize"}},
        MagicMock(),
    )

    assert state == "rejected"
    assert result == {"error": "unknown_skill", "skill_id": "text.summarize"}


@pytest.mark.asyncio
async def test_handle_reports_ollama_failure(monkeypatch):
    def fail(_prompt: str):
        raise adapter.OllamaError("offline")

    monkeypatch.setattr(adapter, "_chat", fail)
    result, state = await adapter.handle(
        {"type": "command", "payload": {"body": "hello"}}, MagicMock()
    )

    assert state == "failed"
    assert result == {"error": "ollama_request_failed", "detail": "offline"}
