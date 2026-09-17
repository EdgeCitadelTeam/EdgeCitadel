"""Best-effort runtime observations; failures never repeat executable work."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .client import AgentdClient, AgentdClientError
from .trace_contract import TraceContractError, validate_rpc_reply

log = logging.getLogger(__name__)


@dataclass
class TraceOperation:
    span_id: str
    input_tokens: int | None = None
    output_tokens: int | None = None


class RuntimeTrace:
    """One handler invocation owns one binding and a bounded loss counter."""

    def __init__(self, client: AgentdClient) -> None:
        self.client = client
        self.binding_id: str | None = None
        self.context_id: str | None = None
        self.session_id: str | None = None
        self.task_id: str | None = None
        self.dropped_observations = 0
        self._producer_id = str(uuid4())
        self._reported_drops = 0
        self._pending_loss: dict[str, Any] | None = None
        self._loss_lock = asyncio.Lock()

    async def _call(
        self, operation: str, params: dict[str, Any], *, count_failure: bool = True
    ) -> dict[str, Any] | None:
        try:
            reply = await asyncio.to_thread(
                self.client.call, f"trace.{operation}", **params
            )
            if not isinstance(reply, dict):
                raise TraceContractError("invalid_reply")
            validate_rpc_reply(
                reply,
                operation=operation,
                request_id=params["observation_id"]
                if operation == "append"
                else params["request_id"],
            )
            if reply["status"] != "ok":
                raise TraceContractError("observation_unavailable")
            return reply["result"]
        except (AgentdClientError, TraceContractError, OSError):
            if count_failure:
                self.dropped_observations = min(
                    self.dropped_observations + 1, 2**31 - 1
                )
            # No exception text, request content, IDs or credentials in this diagnostic.
            log.warning(
                "Runtime trace observation unavailable; dropped=%d",
                self.dropped_observations,
            )
            return None

    async def bind(self, *, session_id: str, task_id: str) -> None:
        result = await self._call(
            "bind",
            {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "session_id": session_id,
                "task_id": task_id,
                "context_id": None,
            },
        )
        if result is not None:
            self.binding_id = result["binding_id"]
            self.context_id = result["context_id"] or task_id
            self.session_id, self.task_id = session_id, task_id

    async def finish(self, state: str) -> None:
        if self.binding_id is None:
            return
        await self.report_loss()
        outcome = {
            "completed": "completed",
            "failed": "failed",
            "cancelled": "canceled",
        }.get(state, "unknown")
        await self._call(
            "finish",
            {
                "schema_version": 1,
                "request_id": str(uuid4()),
                "binding_id": self.binding_id,
                "outcome": outcome,
                "reason": "unknown",
            },
        )

    async def observe(
        self, observation: dict[str, Any], *, observation_id: str
    ) -> None:
        """Stable boundary identity belongs to the producer, never a retry loop."""
        if self.binding_id is None:
            return
        result = await self._call(
            "append",
            {
                "schema_version": 1,
                "binding_id": self.binding_id,
                "observation_id": observation_id,
                "observation": observation,
            },
        )
        if result is not None:
            await self.report_loss()

    async def report_loss(self) -> None:
        """Flush one cumulative report; retries retain identical request bytes."""
        if self.binding_id is None:
            return
        async with self._loss_lock:
            # A pending older snapshot may need one current-count follow-up.
            for _ in range(2):
                if self._pending_loss is None:
                    if self.dropped_observations <= self._reported_drops:
                        return
                    self._pending_loss = {
                        "schema_version": 1,
                        "request_id": str(uuid4()),
                        "binding_id": self.binding_id,
                        "producer_id": self._producer_id,
                        "dropped_observations": self.dropped_observations,
                    }
                result = await self._call(
                    "loss", self._pending_loss, count_failure=False
                )
                if result is None:
                    return
                self._reported_drops = self._pending_loss["dropped_observations"]
                self._pending_loss = None

    @asynccontextmanager
    async def operation(
        self,
        kind: str,
        name: str,
        *,
        parent_span_id: str | None = None,
    ) -> AsyncIterator[TraceOperation]:
        """Measure one real model/tool operation; observation failure cannot rerun it."""
        span = TraceOperation(str(uuid4()))
        attributes: dict[str, Any] = {"name": name}
        if kind == "model":
            attributes.update(
                input_tokens=None,
                output_tokens=None,
                usage_unavailable_reason="not_reported",
            )

        def timestamp() -> str:
            return (
                datetime.now(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )

        base = {
            "schema_version": 1,
            "kind": kind,
            "span_id": span.span_id,
            "parent_span_id": parent_span_id,
        }
        await self.observe(
            {
                **base,
                "phase": "started",
                "occurred_at": timestamp(),
                "duration_ms": None,
                "attributes": attributes,
            },
            observation_id=str(uuid4()),
        )
        started = time.monotonic_ns()
        phase, reason = "finished", None
        try:
            yield span
        except asyncio.CancelledError:
            phase, reason = "interrupted", "unknown"
            raise
        except Exception:
            phase, reason = "failed", "handler_failed"
            raise
        finally:
            duration = max(0, (time.monotonic_ns() - started) // 1_000_000)
            terminal_attributes: dict[str, Any] = {"name": name}
            if reason is not None:
                terminal_attributes["reason"] = reason
            if kind == "model":
                terminal_attributes.update(
                    input_tokens=span.input_tokens,
                    output_tokens=span.output_tokens,
                    usage_unavailable_reason=None
                    if span.input_tokens is not None and span.output_tokens is not None
                    else ("interrupted" if phase == "interrupted" else "not_reported"),
                )
            await self.observe(
                {
                    **base,
                    "phase": phase,
                    "occurred_at": timestamp(),
                    "duration_ms": duration,
                    "attributes": terminal_attributes,
                },
                observation_id=str(uuid4()),
            )
