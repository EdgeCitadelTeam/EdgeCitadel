"""EdgeCitadel Hermes Managed Agent adapter.

Forwards `command` envelopes to a locally-installed Hermes Agent
(`hermes serve --port 8642`) and streams the response back as
`task.progress` envelopes.

Memory is owned upstream — Hermes' own session store under ~/.hermes/
is the source of truth. This Managed Adapter does not use the aggregator's
`memory.turns.*` service.

Architecture: docs/architecture/managed-agents-and-native-plugins.md
"""

from __future__ import annotations
import asyncio
import logging
import os
from pathlib import Path

import httpx

from edgecitadel_agentd.managed_runtime import ManagedContext, run as run_managed_agent
from edgecitadel_hermes_plugin.hermes_client import (
    HERMES_BASE_URL,
    HermesError,
    call_hermes_streaming,
    load_token,
)

log = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class PreflightError(RuntimeError):
    """Hermes server unreachable or bearer token rejected."""


async def preflight() -> None:
    """Check Hermes Agent is reachable on HERMES_BASE_URL with a valid token.
    Calls GET /v1/models — same OpenAI-compat surface, cheap, returns the
    configured model list. Raises PreflightError with a tagged reason."""
    headers = {"Authorization": f"Bearer {load_token()}"}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{HERMES_BASE_URL}/v1/models", headers=headers)
    except httpx.ConnectError as e:
        raise PreflightError(f"hermes_unreachable: {e}") from e
    except (httpx.ReadTimeout, httpx.WriteTimeout):
        raise PreflightError("hermes_unreachable: timeout reading /v1/models")

    if resp.status_code in (401, 403):
        raise PreflightError(
            f"hermes_auth_failed: bearer token rejected (status {resp.status_code})"
        )
    if resp.status_code != 200:
        raise PreflightError(f"hermes_unhealthy: /v1/models status {resp.status_code}")


async def handle(env: dict, ctx: ManagedContext) -> tuple[dict, str]:
    """Translate a `command` envelope into a Hermes Chat Completions call.
    Stream SSE deltas as `task.progress` envelopes; return the joined text
    in a `result`-shaped payload."""
    if env.get("type") != "command":
        return ({"error": "unsupported_type"}, "rejected")

    payload = env.get("payload") or {}
    body = (payload.get("body") or "").strip()
    if not body:
        return ({"error": "empty_prompt"}, "rejected")

    task_id = env.get("task_id") or ""
    context_id = env.get("context_id") or ""

    async def publish_delta(delta: str) -> None:
        if task_id:
            await ctx.publish_progress(
                task_id, body=delta, extra={"upstream": "hermes-agent"}
            )

    try:
        full_text = await call_hermes_streaming(
            prompt=body,
            session_id=context_id or None,
            publish_progress=publish_delta,
        )
    except HermesError as e:
        log.warning("hermes call failed (%s)", e)
        return ({"error": "hermes_request_failed", "detail": str(e)}, "failed")

    return ({"body": full_text, "upstream": "hermes-agent"}, "completed")


async def main() -> None:
    await preflight()
    await run_managed_agent(CONFIG_PATH, handle)


def _entrypoint() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    asyncio.run(main())


if __name__ == "__main__":
    _entrypoint()
