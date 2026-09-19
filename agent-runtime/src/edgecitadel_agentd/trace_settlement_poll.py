"""Bounded, coalesced v2 control polling; lifecycle and replay scheduling are separate."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from typing import Any

from nats import errors as nats_errors
from nats.aio.client import Client as NATS

from .store import AgentdStore
from .trace_contract import TraceContractError
from .trace_metrics import SourceMetrics
from .trace_settlement_apply import apply_page, page_request
from .trace_settlement_pages import (
    SETTLEMENT_PAGE_SUBJECT,
    validate_page_reply,
    validate_page_request,
)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_reply_key")
        result[key] = value
    return result


class SettlementPoller:
    """One event-loop-owned worker per source/export scope.

    The owner must isolate synchronous SQLite work from command handling. Stop
    cancels network/backoff waits; it never rewinds a committed page application.
    """

    def __init__(
        self, store: AgentdStore, nc: NATS, scope: tuple[str, str, str]
    ) -> None:
        self.store, self.nc, self.scope = store, nc, scope
        self.metrics = SourceMetrics()
        self.state = "idle"
        self.fault: str | None = None
        self.replay_required = False
        self.delay = 0.0
        self._retry_delay = 0.5
        self._next_at = 0.0
        self._inflight: asyncio.Task[str] | None = None

    async def poll(self) -> str:
        """Concurrent triggers share one request/application and its scheduled wait."""
        if self.state == "paused":
            return "paused"
        if self._inflight is None or self._inflight.done():
            self._inflight = asyncio.create_task(self._poll())
        return await asyncio.shield(self._inflight)

    def _schedule(self, delay: float) -> None:
        self.delay = delay
        self._next_at = asyncio.get_running_loop().time() + delay

    def _retry(self, code: str, minimum: float = 0.5) -> str:
        self.state, self.fault = "retrying", code
        self._schedule(min(30.0, max(self._retry_delay, minimum)))
        self._retry_delay = min(30.0, self._retry_delay * 2)
        return code

    def _pause(self, code: str) -> str:
        self.state, self.fault = "paused", code
        return code

    async def _poll(self) -> str:
        await asyncio.sleep(max(0.0, self._next_at - asyncio.get_running_loop().time()))
        self.state, self.fault = "requesting", None
        try:
            request = page_request(self.store, self.scope)
            payload = validate_page_request(request)
        except sqlite3.OperationalError:
            return self._retry("local_storage_unavailable")
        except (TraceContractError, sqlite3.DatabaseError):
            return self._pause("local_settlement_fault")
        self.metrics.note("settlement_requests")
        try:
            message = await self.nc.request(SETTLEMENT_PAGE_SUBJECT, payload, timeout=5)
        except (nats_errors.AuthorizationError, nats_errors.BadSubjectError):
            self.metrics.note("settlement_request_failures")
            return self._pause("settlement_configuration_error")
        except (nats_errors.Error, OSError, TimeoutError):
            self.metrics.note("settlement_request_failures")
            return self._retry("settlement_unavailable")
        try:
            if len(message.data) > 18 * 1024:
                raise ValueError("oversize_reply")
            reply = json.loads(
                message.data.decode("utf-8"), object_pairs_hook=_unique_object
            )
            validate_page_reply(reply, request=request)
        except (ValueError, UnicodeError, RecursionError):
            return self._retry("invalid_settlement_reply")
        if reply["status"] == "error":
            code = str(reply["code"])
            if code in ("unsupported_version", "invalid_request", "collector_changed"):
                self.replay_required = code == "collector_changed"
                return self._pause(code)
            if code == "unknown_source":
                self.replay_required = True
            return self._retry(code, reply["retry_after_ms"] / 1000)
        try:
            outcome = apply_page(self.store, request, reply)
            self.metrics.note("settlement_page_observations")
            with self.store._lock:
                pending = (
                    self.store._connection.execute(
                        "SELECT 1 FROM trace_spool_all WHERE state IN ('pending','broker_acked') "
                        "AND node_id=? AND source_epoch=? AND export_generation=? LIMIT 1",
                        self.scope,
                    ).fetchone()
                    is not None
                )
        except sqlite3.OperationalError:
            return self._retry("local_storage_unavailable")
        except (TraceContractError, sqlite3.DatabaseError):
            return self._pause("local_settlement_fault")
        more = reply["page"]["more"]
        self.state, self.fault = "idle", None
        self.replay_required = pending and not more
        self._retry_delay = 0.5
        self._schedule(0.5 if more else 30.0)
        return outcome

    async def close(self) -> None:
        """Cancel the shielded request before its owner releases the scope."""
        if self._inflight is not None:
            self._inflight.cancel()
            await asyncio.gather(self._inflight, return_exceptions=True)

    async def run(self, stop: asyncio.Event) -> None:
        stopped = asyncio.create_task(stop.wait())
        trigger: asyncio.Task[str] | None = None
        try:
            while not stop.is_set() and self.state != "paused":
                trigger = asyncio.create_task(self.poll())
                done, _ = await asyncio.wait(
                    (trigger, stopped), return_when=asyncio.FIRST_COMPLETED
                )
                if stopped in done:
                    break
                await trigger
        finally:
            tasks = [
                task for task in (trigger, stopped, self._inflight) if task is not None
            ]
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.state != "paused":
                self.state = "stopped"
