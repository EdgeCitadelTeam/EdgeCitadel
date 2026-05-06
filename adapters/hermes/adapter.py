"""EdgeCitadel Hermes bridge adapter.

Forwards `command` envelopes to a locally-installed Hermes Agent
(`hermes serve --port 8642`) and streams the response back as
`task.progress` envelopes.

Memory is owned upstream — Hermes' own session store under ~/.hermes/
is the source of truth. This adapter does NOT use the aggregator's
`memory.turns.*` service. See ADR-0009.

Spec: docs/superpowers/specs/2026-05-05-hermes-bridge-design.md
"""
from __future__ import annotations
import asyncio
import logging
import os
from pathlib import Path

import httpx

from adapters.hermes.hermes_client import (
    HERMES_BASE_URL, HERMES_TIMEOUT_SEC, _token)

log = logging.getLogger(__name__)
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class PreflightError(RuntimeError):
    """Hermes server unreachable or bearer token rejected."""


async def preflight() -> None:
    """Check Hermes Agent is reachable on HERMES_BASE_URL with a valid token.
    Calls GET /v1/models — same OpenAI-compat surface, cheap, returns the
    configured model list. Raises PreflightError with a tagged reason."""
    headers = {"Authorization": f"Bearer {_token()}"}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{HERMES_BASE_URL}/v1/models",
                                    headers=headers, timeout=5)
    except httpx.ConnectError as e:
        raise PreflightError(f"hermes_unreachable: {e}") from e
    except (httpx.ReadTimeout, httpx.WriteTimeout):
        raise PreflightError("hermes_unreachable: timeout reading /v1/models")

    if resp.status_code == 401:
        raise PreflightError("hermes_auth_failed: bearer token rejected")
    if resp.status_code != 200:
        raise PreflightError(
            f"hermes_unhealthy: /v1/models status {resp.status_code}")
