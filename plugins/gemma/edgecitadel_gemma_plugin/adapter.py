"""EdgeCitadel Gemma Managed Agent — multi-skill dispatch + memory + streaming.

Phase 2.5 enhancements over Phase 2:
- Multi-skill dispatch via payload.skill_id (config.yaml-driven).
- Conversational memory via aggregator's memory.turns.* NATS service.
- Token streaming via task.progress envelopes (hybrid 8-tok/100ms flush).
- text.classify uses Ollama format='json' + JSON-schema validation.

Spec: docs/superpowers/specs/2026-04-30-gemma-enhancements-design.md
"""

from __future__ import annotations
import asyncio
import logging
import os
from pathlib import Path

import httpx

from edgecitadel_agentd.managed_runtime import ManagedContext, run as run_managed_agent

from edgecitadel_gemma_plugin.skills import (
    load_skills,
    dispatch_skill,
    validate_structured_output,
    SkillError,
)
from edgecitadel_gemma_plugin.prompt import build_messages
from edgecitadel_gemma_plugin.ollama_client import (
    call_ollama_streaming,
    OLLAMA_MODEL,
    _ollama_url,
)
from edgecitadel_gemma_plugin.memory_client import fetch_memory, persist_memory

log = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"
SKILLS = load_skills(CONFIG_PATH)


class PreflightError(RuntimeError):
    """Ollama unreachable or model not pulled."""


async def preflight() -> None:
    model = os.environ.get("OLLAMA_MODEL", OLLAMA_MODEL)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(_ollama_url("/api/tags"), timeout=5)
    except httpx.ConnectError as e:
        raise PreflightError(f"ollama_unreachable: {e}") from e
    except (httpx.ReadTimeout, httpx.WriteTimeout):
        raise PreflightError("ollama_unreachable: timeout reading /api/tags")
    if resp.status_code != 200:
        raise PreflightError(f"ollama_unreachable: /api/tags status {resp.status_code}")
    try:
        body = resp.json()
    except ValueError as e:
        raise PreflightError(f"ollama_bad_response: {e}") from e
    available = {m["name"] for m in body.get("models", [])}
    # Phase 2: accept bare name when only ":latest" tag is listed
    candidates = {model, f"{model}:latest"} if ":" not in model else {model}
    if not (available & candidates):
        raise PreflightError(
            f"ollama_model_missing: {model} not in {sorted(available)[:5]}..."
        )


async def handle(env: dict, ctx: ManagedContext) -> tuple[dict, str]:
    """Multi-skill dispatcher with memory + streaming."""
    if env["type"] != "command":
        return ({"error": "unsupported_type"}, "rejected")

    payload = env.get("payload") or {}
    body = (payload.get("body") or "").strip()
    args = payload.get("args") or {}
    raw_skill_id = payload.get("skill_id")

    try:
        skill = dispatch_skill(SKILLS, raw_skill_id)
    except SkillError as e:
        return (
            {"error": "unknown_skill", "skill_id": raw_skill_id, "detail": str(e)},
            "rejected",
        )
    skill_id = skill["id"]

    if not body:
        return ({"error": "empty_prompt"}, "rejected")

    context_id = env.get("context_id")
    # 1. Fetch prior history (best-effort)
    history = await fetch_memory(
        ctx.nc, context_id=context_id, agent_id=ctx.agent_id, token_budget=2048
    )

    # 2. Build messages
    messages = build_messages(
        system_prompt=skill["system_prompt"], history=history, user_body=body
    )

    # 3. Determine output shape
    out_shape = skill.get("output_shape", "free_text")
    use_json = isinstance(out_shape, dict) and out_shape.get("type") == "json"

    # 4. Closure for streaming progress publishes (carries skill_id)
    task_id = env.get("task_id")

    async def publish(delta: str) -> None:
        await ctx.publish_progress(
            task_id, body=delta, extra={"delta": delta, "skill_id": skill_id}
        )

    # 5. Stream from Ollama
    try:
        text = await call_ollama_streaming(
            messages=messages,
            skill=skill,
            args=args,
            use_json=use_json,
            publish_progress=publish,
        )
    except httpx.HTTPError as e:
        log.exception("ollama request failed")
        return ({"error": "ollama_request_failed", "detail": str(e)}, "failed")

    # 6. Structured output validation (text.classify path)
    payload_out: dict = {"body": text, "model": OLLAMA_MODEL, "skill_id": skill_id}
    if use_json:
        try:
            structured = validate_structured_output(text, out_shape["schema"])
            payload_out["structured"] = structured
        except SkillError as e:
            return (
                {
                    "error": "structured_output_validation_failed",
                    "raw": text,
                    "detail": str(e),
                    "skill_id": skill_id,
                },
                "failed",
            )

    # 7. Persist user + assistant turns (best-effort)
    await persist_memory(
        ctx.nc,
        context_id=context_id,
        agent_id=ctx.agent_id,
        role="user",
        content=body,
        skill_id=skill_id,
    )
    await persist_memory(
        ctx.nc,
        context_id=context_id,
        agent_id=ctx.agent_id,
        role="assistant",
        content=text,
        skill_id=skill_id,
    )

    return (payload_out, "completed")


async def main():
    await preflight()
    await run_managed_agent(CONFIG_PATH, handle)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(main())
