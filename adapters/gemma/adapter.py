"""EdgeCitadel Gemma adapter — single-shot Ollama-backed reasoner.

Wraps Ollama POST /api/generate. runtime.kind: native; runtime.roles:
[reasoner]. Single skill: reasoning.chat. No streaming, no conversation
memory (deferred to Phase 2.5; see docs/roadmap.md)."""
from __future__ import annotations
import asyncio
import logging
import os
import time
from pathlib import Path

import httpx

from adapters._common.pull_consumer import Context
from adapters._common.template import main as run_adapter

log = logging.getLogger(__name__)

OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "localhost")
OLLAMA_PORT = int(os.environ.get("OLLAMA_PORT", "11434"))
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gemma3:4b")
OLLAMA_TIMEOUT_SEC = int(os.environ.get("OLLAMA_TIMEOUT_SEC", "120"))


def _ollama_url(path: str) -> str:
    return f"http://{OLLAMA_HOST}:{OLLAMA_PORT}{path}"


async def handle(env: dict, ctx: Context) -> tuple[dict, str]:
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")

    body = env["payload"].get("body", "").strip()
    args = env["payload"].get("args") or {}
    if not body:
        return ({"error": "empty_prompt"}, "rejected")

    model = args.get("model") or OLLAMA_MODEL
    timeout_sec = int(args.get("timeout_sec") or OLLAMA_TIMEOUT_SEC)
    options: dict = {}
    if "temperature" in args:
        options["temperature"] = args["temperature"]
    if "max_tokens" in args:
        options["num_predict"] = args["max_tokens"]

    request_body = {"model": model, "prompt": body, "stream": False}
    if options:
        request_body["options"] = options

    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post(_ollama_url("/api/generate"),
                                     json=request_body, timeout=timeout_sec)
    except httpx.ConnectError:
        return ({"error": "ollama_unreachable"}, "failed")
    except (httpx.ReadTimeout, httpx.WriteTimeout, asyncio.TimeoutError):
        return ({"error": "ollama_timeout"}, "failed")
    duration_ms = int((time.monotonic() - started) * 1000)

    if resp.status_code == 404:
        return ({"error": "model_not_loaded"}, "failed")
    if resp.status_code >= 500:
        return ({"error": "ollama_inference_error"}, "failed")
    if resp.status_code != 200:
        return ({"error": "ollama_bad_response",
                 "status": resp.status_code}, "failed")

    try:
        body_json = resp.json()
    except (ValueError, Exception):
        return ({"error": "ollama_bad_response"}, "failed")

    response_text = body_json.get("response", "")
    return ({"body": response_text, "model": model,
             "duration_ms": duration_ms}, "completed")


async def main():
    """Adapter entry point. Handler is injected into the shared template."""
    from adapters._common import template
    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
