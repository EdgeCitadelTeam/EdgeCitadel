"""Containment HTTP API, authenticated with the administrator token.

- ``POST /api/containment/alerts``: external alert intake (validator failures or
  passes on a version, version revocation, operator alerts); 202 with the alert id.
- ``GET /api/containment/status``: G_C and G_P sizes, live frontier, last solve,
  chosen actions with per-action timestamps, root-cause ranking, holds in force.
"""

from __future__ import annotations

import os
import secrets
from typing import Annotated, Any

from fastapi import APIRouter, Body, Header, HTTPException

from .model import AlertError


def require_admin(token: str | None) -> None:
    expected = os.environ.get("EDGECITADEL_ADMIN_TOKEN", "")
    if (
        not expected
        or not token
        or not expected.isascii()
        or not token.isascii()
        or not secrets.compare_digest(expected, token)
    ):
        raise HTTPException(401, "invalid administrator credential")


def make_containment_router(service) -> APIRouter:
    router = APIRouter()

    @router.post("/api/containment/alerts", status_code=202)
    async def submit_alert(
        payload: Annotated[Any, Body()],
        x_edgecitadel_admin_token: Annotated[str | None, Header()] = None,
    ):
        require_admin(x_edgecitadel_admin_token)
        try:
            return service.submit_alert(payload)
        except AlertError as error:
            raise HTTPException(400, str(error)) from None

    @router.get("/api/containment/status")
    async def containment_status(
        x_edgecitadel_admin_token: Annotated[str | None, Header()] = None,
    ):
        require_admin(x_edgecitadel_admin_token)
        return service.status()

    return router
