"""Request-scoped Hermes callbacks; never forward arguments or results."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

from edgecitadel_agentd.trace_producer import RuntimeTrace


@dataclass
class _Call:
    span_id: str
    name: str
    started_ns: int


class HermesToolObserver:
    """Attach to one AIAgent, with its binding and the owning event loop.

    Hermes invokes these callbacks from worker threads. Submission is bounded;
    callback failures cannot change the tool result or cause another invocation.
    """

    def __init__(self, trace: RuntimeTrace, loop: asyncio.AbstractEventLoop) -> None:
        self.trace = trace
        self.loop = loop
        self._calls: dict[str, _Call] = {}
        self._lock = threading.Lock()
        self.dropped_callbacks = 0

    def _drop(self) -> None:
        with self._lock:
            self.dropped_callbacks = min(self.dropped_callbacks + 1, 2**31 - 1)

    def _emit(
        self,
        call: _Call,
        phase: str,
        duration: int | None,
        *,
        kind: str = "tool",
        attributes: dict[str, Any] | None = None,
    ) -> None:
        try:
            if asyncio.get_running_loop() is self.loop:
                self._drop()
                return
        except RuntimeError:
            pass
        observation = {
            "schema_version": 1,
            "kind": kind,
            "phase": phase,
            "span_id": call.span_id,
            "parent_span_id": None,
            "occurred_at": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "duration_ms": duration,
            "attributes": attributes if attributes is not None else {"name": call.name},
        }
        if self.loop.is_closed():
            self._drop()
            return
        coroutine = self.trace.observe(observation, observation_id=str(uuid4()))
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
        except RuntimeError:
            coroutine.close()
            self._drop()
            return
        try:
            future.result(timeout=6)
        except Exception:  # noqa: BLE001 - optional callback failure cannot alter execution
            future.cancel()
            self._drop()

    def started(self, call_id: str, name: str, _arguments: object) -> None:
        if (
            not isinstance(call_id, str)
            or not isinstance(name, str)
            or not 1 <= len(call_id) <= 128
            or not 1 <= len(name) <= 128
        ):
            self._drop()
            return
        with self._lock:
            if call_id in self._calls:
                return
            if len(self._calls) >= 128:
                self.dropped_callbacks = min(self.dropped_callbacks + 1, 2**31 - 1)
                return
            call = _Call(str(uuid4()), name, time.monotonic_ns())
            self._calls[call_id] = call
        self._emit(call, "started", None)
        call.started_ns = time.monotonic_ns()

    def completed(
        self, call_id: str, _name: str, _arguments: object, _result: object
    ) -> None:
        with self._lock:
            call = self._calls.pop(call_id, None)
        if call is None:
            return
        self._emit(
            call,
            "finished",
            max(0, (time.monotonic_ns() - call.started_ns) // 1_000_000),
        )


class HermesModelObserver:
    """Per-agent logical model requests, not SDK-internal transport attempts."""

    def __init__(self, trace: RuntimeTrace, loop: asyncio.AbstractEventLoop) -> None:
        self._emitter = HermesToolObserver(trace, loop)
        self._depth = threading.local()

    @staticmethod
    def _usage(response: Any) -> tuple[int | None, int | None]:
        usage = getattr(response, "usage", None)

        def value(*names: str) -> int | None:
            for name in names:
                item = (
                    usage.get(name)
                    if isinstance(usage, Mapping)
                    else getattr(usage, name, None)
                )
                if type(item) is int and 0 <= item <= 9_007_199_254_740_991:
                    return item
            return None

        return value("input_tokens", "prompt_tokens"), value(
            "output_tokens", "completion_tokens"
        )

    def attach(self, agent: Any) -> None:
        for name in ("_interruptible_api_call", "_interruptible_streaming_api_call"):
            original = getattr(agent, name, None)
            if not callable(original) or getattr(
                original, "_edgecitadel_observed", False
            ):
                continue

            def measured(*args: Any, original: Any = original, **kwargs: Any) -> Any:
                if getattr(self._depth, "active", False):
                    return original(*args, **kwargs)
                self._depth.active = True
                call = _Call(str(uuid4()), str(agent.model), time.monotonic_ns())
                self._emitter._emit(
                    call,
                    "started",
                    None,
                    kind="model",
                    attributes={
                        "name": call.name,
                        "input_tokens": None,
                        "output_tokens": None,
                        "usage_unavailable_reason": "not_reported",
                    },
                )
                started = time.monotonic_ns()
                phase, reason, response = "finished", None, None
                try:
                    response = original(*args, **kwargs)
                    return response
                except BaseException as error:
                    phase = "failed" if isinstance(error, Exception) else "interrupted"
                    reason = "handler_failed" if phase == "failed" else "unknown"
                    raise
                finally:
                    duration = max(0, (time.monotonic_ns() - started) // 1_000_000)
                    try:
                        input_tokens, output_tokens = self._usage(response)
                    except Exception:  # noqa: BLE001 - optional usage cannot alter response
                        input_tokens, output_tokens = None, None
                    attrs = {
                        "name": call.name,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "usage_unavailable_reason": None
                        if input_tokens is not None and output_tokens is not None
                        else (
                            "interrupted" if phase == "interrupted" else "not_reported"
                        ),
                    }
                    if reason is not None:
                        attrs["reason"] = reason
                    self._emitter._emit(
                        call, phase, duration, kind="model", attributes=attrs
                    )
                    self._depth.active = False

            cast(Any, measured)._edgecitadel_observed = True
            setattr(agent, name, measured)
