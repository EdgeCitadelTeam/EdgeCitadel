"""Live-Ollama integration test for the Gemma adapter.

Skipped unless OLLAMA_URL_TEST is set AND the configured model is loaded.
Pattern mirrors aggregator/tests/test_jetstream_bootstrap.py."""
import os
import pytest
import httpx
from adapters.gemma.adapter import handle
from adapters._common.pull_consumer import Context
from unittest.mock import MagicMock


OLLAMA_URL = os.environ.get("OLLAMA_URL_TEST")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL_TEST", "gemma3:4b")


def _ctx():
    import asyncio
    ctx = Context(agent_id="gemma-1", nc=MagicMock(), js=MagicMock(),
                  msg=MagicMock())
    ctx.in_progress = lambda: asyncio.sleep(0)
    ctx.publish_progress = lambda *a, **k: asyncio.sleep(0)
    return ctx


@pytest.fixture
def ollama_available():
    if not OLLAMA_URL:
        pytest.skip("OLLAMA_URL_TEST not set; skipping live test")
    try:
        with httpx.Client(timeout=2) as client:
            r = client.get(f"{OLLAMA_URL}/api/tags")
            r.raise_for_status()
            names = [m.get("name") for m in r.json().get("models") or []]
            if OLLAMA_MODEL not in names:
                pytest.skip(
                    f"OLLAMA_MODEL_TEST={OLLAMA_MODEL!r} not in {names!r}")
    except Exception as e:
        pytest.skip(f"Ollama not reachable: {e}")


@pytest.mark.asyncio
async def test_handle_against_live_ollama(ollama_available, monkeypatch):
    # Point the adapter's URL builders at the test broker
    parsed = httpx.URL(OLLAMA_URL)
    monkeypatch.setenv("OLLAMA_HOST", parsed.host)
    monkeypatch.setenv("OLLAMA_PORT", str(parsed.port or 11434))
    monkeypatch.setenv("OLLAMA_MODEL", OLLAMA_MODEL)
    # Reload module-level constants in adapter.py
    import importlib
    from adapters.gemma import adapter as adapter_mod
    importlib.reload(adapter_mod)

    env = {"v": 1, "id": "11111111-2222-4333-8444-555555555555",
           "type": "command",
           "sender_id": "tester", "recipient_id": "gemma-1",
           "task_id": "22222222-3333-4444-8555-666666666666",
           "timestamp": "2026-04-24T10:00:00.000Z",
           "payload": {"body": "Reply with exactly the digit 4.",
                       "args": {"timeout_sec": 60}}}

    payload, state = await adapter_mod.handle(env, _ctx())
    assert state == "completed", \
        f"expected completed, got {state}: {payload}"
    assert "body" in payload and payload["body"], \
        f"empty body: {payload}"
    assert payload["model"] == OLLAMA_MODEL
    assert isinstance(payload.get("duration_ms"), int)
