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


class PreflightError(RuntimeError):
    """Raised when the adapter cannot start because Ollama is unreachable
    or the configured model is not pulled."""


async def preflight() -> None:
    """Verify Ollama is reachable and OLLAMA_MODEL is in the loaded list.

    Raises PreflightError on failure. Called once at startup before the
    consumer is registered. We intentionally do NOT auto-pull on missing
    models — see docs/superpowers/specs/2026-04-24-gemma-adapter-design.md
    section "Why fail-fast, not auto-pull?"."""
    model = os.environ.get("OLLAMA_MODEL", OLLAMA_MODEL)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(_ollama_url("/api/tags"), timeout=5)
    except httpx.ConnectError as e:
        raise PreflightError(
            f"ollama_unreachable: cannot reach {_ollama_url('/api/tags')}: {e}"
        ) from e
    except (httpx.ReadTimeout, httpx.WriteTimeout):
        raise PreflightError(
            f"ollama_unreachable: timeout reading {_ollama_url('/api/tags')}"
        )

    if resp.status_code != 200:
        raise PreflightError(
            f"ollama_unreachable: /api/tags returned {resp.status_code}"
        )

    try:
        body = resp.json()
    except ValueError as e:
        raise PreflightError(f"ollama_bad_response: {e}") from e

    names = [m.get("name") for m in (body.get("models") or [])]
    if model not in names:
        raise PreflightError(
            f"model_not_loaded: OLLAMA_MODEL={model!r} not in {names!r}; "
            f"run `ollama pull {model}`"
        )


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
    """Adapter entry point. Preflight first, then delegate to template."""
    try:
        await preflight()
    except PreflightError as e:
        log.error("preflight failed: %s", e)
        msg = str(e)
        if msg.startswith("ollama_unreachable"):
            raise SystemExit(1)
        if msg.startswith("model_not_loaded"):
            raise SystemExit(2)
        raise SystemExit(3)

    from adapters._common import template
    template.handle = handle
    config = Path(__file__).resolve().parent / "config.yaml"
    await run_adapter(config)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
