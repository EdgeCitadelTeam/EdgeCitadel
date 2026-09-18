"""Bounded v2 settlement request admission and snapshot work."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from uuid import UUID

from edgecitadel_agentd.trace_settlement_pages import validate_page_request

from .trace_ingest import _object
from .trace_settlement import settlement_page_reply

REQUEST_BYTES = 1024
REQUEST_RATE = 20
QUERY_STEPS = 100_000
QUERY_SECONDS = 0.02


class SettlementResponder:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.tokens = float(REQUEST_RATE)
        self.updated = time.monotonic()

    def reply(self, data: bytes) -> dict | None:
        # Malformed/oversize envelopes are dropped without trusting a reply nonce.
        if len(data) > REQUEST_BYTES:
            return None
        try:
            request = json.loads(data.decode("utf-8"), object_pairs_hook=_object)
            if not isinstance(request, dict):
                return None
            nonce = request.get("request_id")
            if not isinstance(nonce, str) or str(UUID(nonce)) != nonce:
                return None
        except (ValueError, UnicodeError, RecursionError):
            return None
        error = {
            "schema_version": 2,
            "request_id": nonce,
            "status": "error",
            "code": "invalid_request",
            "retry_after_ms": 500,
        }
        if request.get("schema_version") != 2:
            return {**error, "code": "unsupported_version"}
        try:
            validate_page_request(request)
        except ValueError:
            return error
        now = time.monotonic()
        self.tokens = min(
            float(REQUEST_RATE), self.tokens + (now - self.updated) * REQUEST_RATE
        )
        self.updated = now
        if self.tokens < 1:
            return {**error, "code": "rate_limited"}
        self.tokens -= 1
        deadline, steps = now + QUERY_SECONDS, 0

        def interrupt():
            nonlocal steps
            steps += 1000
            return steps >= QUERY_STEPS or time.monotonic() >= deadline

        self.connection.set_progress_handler(interrupt, 1000)
        try:
            return settlement_page_reply(self.connection, request)
        except sqlite3.DatabaseError:
            return {**error, "code": "temporarily_unavailable"}
        finally:
            self.connection.set_progress_handler(None, 0)

    async def __call__(self, message) -> None:
        result = self.reply(message.data)
        if result is not None and message.reply:
            await message.respond(json.dumps(result, separators=(",", ":")).encode())
        await asyncio.sleep(0)
