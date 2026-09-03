"""Minimal deterministic Managed Agent used by package and onboarding smoke tests."""

from __future__ import annotations

import asyncio
from pathlib import Path

from edgecitadel_agentd.managed_runtime import ManagedContext, run


async def handle(envelope: dict, _context: ManagedContext) -> tuple[dict, str]:
    if envelope.get("type") != "command":
        return ({"error": "unsupported_type"}, "rejected")
    payload = envelope.get("payload") or {}
    return ({"body": payload.get("body", "")}, "completed")


async def main() -> None:
    await run(Path(__file__).with_name("config.yaml"), handle)


if __name__ == "__main__":
    asyncio.run(main())
