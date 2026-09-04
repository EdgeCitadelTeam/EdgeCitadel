"""Minimal Ollama-backed Managed Agent for a local Gemma model."""

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from edgecitadel_agentd.managed_runtime import ManagedContext, run

CONFIG_PATH = Path(__file__).with_name("config.yaml")
SKILL_ID = "reasoning.chat"
DEFAULT_MODEL = "gemma3:1b"


class OllamaError(RuntimeError):
    """Raised when the local Ollama service cannot complete a chat request."""


def _ollama_url() -> str:
    host = os.environ.get("OLLAMA_HOST", "127.0.0.1")
    port = os.environ.get("OLLAMA_PORT", "11434")
    return f"http://{host}:{port}/api/chat"


def _chat(prompt: str) -> tuple[str, str]:
    model = os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL)
    timeout = float(os.environ.get("OLLAMA_TIMEOUT_SEC", "120"))
    request = urllib.request.Request(
        _ollama_url(),
        data=json.dumps(
            {
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload: Any = json.loads(response.read())
    except (OSError, ValueError, urllib.error.HTTPError) as error:
        raise OllamaError(str(error)) from error

    content = (payload.get("message") or {}).get("content")
    if not isinstance(content, str):
        raise OllamaError("Ollama response did not contain message.content")
    return content, model


async def handle(
    envelope: dict[str, Any], _context: ManagedContext
) -> tuple[dict[str, Any], str]:
    if envelope.get("type") != "command":
        return ({"error": "unsupported_type"}, "rejected")

    payload = envelope.get("payload") or {}
    skill_id = payload.get("skill_id")
    if skill_id not in (None, SKILL_ID):
        return ({"error": "unknown_skill", "skill_id": skill_id}, "rejected")

    prompt = str(payload.get("body") or "").strip()
    if not prompt:
        return ({"error": "empty_prompt"}, "rejected")

    try:
        body, model = await asyncio.to_thread(_chat, prompt)
    except OllamaError as error:
        return ({"error": "ollama_request_failed", "detail": str(error)}, "failed")
    return ({"body": body, "model": model, "skill_id": SKILL_ID}, "completed")


async def main() -> None:
    await run(CONFIG_PATH, handle)
