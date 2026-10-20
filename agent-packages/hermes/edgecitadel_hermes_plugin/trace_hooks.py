"""Request-scoped Hermes callbacks with bounded redacted durable content."""

from __future__ import annotations

import asyncio
import json
import inspect
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

from edgecitadel_agentd.trace_producer import RuntimeTrace
from edgecitadel_agentd.trace_content import bounded_content


@dataclass
class _Call:
    span_id: str
    name: str
    started_ns: int
    parent_span_id: str | None = None


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
        self.model_calls: dict[str, str] = {}

    def _drop(self) -> None:
        with self._lock:
            self.dropped_callbacks = min(self.dropped_callbacks + 1, 2**31 - 1)
            if isinstance(self.trace, RuntimeTrace):
                self.trace.dropped_observations = min(
                    self.trace.dropped_observations + 1, 2**31 - 1
                )

    def _emit(self, *args, **kwargs) -> None:
        try:
            self._emit_unchecked(*args, **kwargs)
        except Exception:  # noqa: BLE001 - telemetry cannot change executable work
            self._drop()

    def _emit_unchecked(
        self,
        call: _Call,
        phase: str,
        duration: int | None,
        *,
        kind: str = "tool",
        attributes: dict[str, Any] | None = None,
        content: dict[str, object] | None = None,
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
            "parent_span_id": call.parent_span_id,
            "occurred_at": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "duration_ms": duration,
            "attributes": attributes if attributes is not None else {"name": call.name},
        }
        if content is not None:
            observation["content"] = bounded_content(content)
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
            call.parent_span_id = self.model_calls.get(call_id)
            self._calls[call_id] = call
        self._emit(
            call,
            "started",
            None,
            attributes={
                "name": name,
                "tool_call_id": call_id,
                "approval_state": "not_reported",
                "execution_target": "hermes_host",
            },
            content={"arguments": _arguments},
        )
        call.started_ns = time.monotonic_ns()

    @staticmethod
    def _failed(result: object) -> bool:
        if isinstance(result, str) and len(result) <= 1_048_576:
            try:
                result = json.loads(result)
            except ValueError:
                return False
        if not isinstance(result, Mapping):
            return False
        return bool(
            result.get("error")
            or result.get("is_error")
            or (type(result.get("exit_code")) is int and result["exit_code"] != 0)
        )

    def completed(
        self, call_id: str, _name: str, _arguments: object, _result: object
    ) -> None:
        with self._lock:
            call = self._calls.pop(call_id, None)
        if call is None:
            return
        self._emit(
            call,
            "failed" if self._failed(_result) else "finished",
            max(0, (time.monotonic_ns() - call.started_ns) // 1_000_000),
            attributes={"name": call.name, "tool_call_id": call_id},
            content={"result": _result},
        )


class HermesModelObserver:
    """Per-agent logical model requests, not SDK-internal transport attempts."""

    def __init__(
        self,
        trace: RuntimeTrace,
        loop: asyncio.AbstractEventLoop,
        tool_observer: HermesToolObserver | None = None,
    ) -> None:
        self._emitter = tool_observer or HermesToolObserver(trace, loop)
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

            has_first_delta = "on_first_delta" in inspect.signature(original).parameters

            def measured(
                *args: Any,
                original: Any = original,
                has_first_delta: bool = has_first_delta,
                **kwargs: Any,
            ) -> Any:
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
                    content={
                        "messages": (
                            args[0] if args and isinstance(args[0], dict) else kwargs
                        ).get("messages", [])
                    },
                )
                started = time.monotonic_ns()
                first_delta_ms = None
                if has_first_delta:
                    previous_delta = kwargs.get("on_first_delta")

                    def first_delta(*values, **options):
                        nonlocal first_delta_ms
                        if first_delta_ms is None:
                            first_delta_ms = max(
                                0, (time.monotonic_ns() - started) // 1_000_000
                            )
                        if previous_delta is not None:
                            return previous_delta(*values, **options)

                    kwargs["on_first_delta"] = first_delta
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
                    if first_delta_ms is not None:
                        attrs["first_token_ms"] = first_delta_ms
                    if reason is not None:
                        attrs["reason"] = reason
                    response_content = {}
                    attrs.update(
                        reasoning_unavailable_reason="not_reported",
                        internal_attempts_unavailable_reason="unsupported",
                    )
                    # Link only explicit provider tool-call IDs, never temporal adjacency.
                    try:
                        usage = getattr(response, "usage", None)
                        for source, target, field in (
                            (
                                "input_tokens_details",
                                "cached_input_tokens",
                                "cached_tokens",
                            ),
                            (
                                "prompt_tokens_details",
                                "cached_input_tokens",
                                "cached_tokens",
                            ),
                            (
                                "output_tokens_details",
                                "reasoning_tokens",
                                "reasoning_tokens",
                            ),
                            (
                                "completion_tokens_details",
                                "reasoning_tokens",
                                "reasoning_tokens",
                            ),
                        ):
                            detail = (
                                usage.get(source)
                                if isinstance(usage, Mapping)
                                else getattr(usage, source, None)
                            )
                            value = (
                                detail.get(field)
                                if isinstance(detail, Mapping)
                                else getattr(detail, field, None)
                            )
                            if (
                                type(value) is int
                                and 0 <= value <= 9_007_199_254_740_991
                            ):
                                attrs[target] = value
                        request_id = getattr(response, "id", None)
                        if isinstance(request_id, str) and len(request_id) <= 256:
                            attrs["provider_request_id"] = request_id
                        # Responses API exposes output items instead of chat choices.
                        for item in getattr(response, "output", None) or []:
                            item_type = getattr(item, "type", None)
                            if item_type == "function_call":
                                tool_id = getattr(item, "call_id", None)
                                if isinstance(tool_id, str) and len(tool_id) <= 128:
                                    with self._emitter._lock:
                                        if len(self._emitter.model_calls) < 4096:
                                            self._emitter.model_calls[tool_id] = (
                                                call.span_id
                                            )
                            elif item_type == "message":
                                texts = [
                                    part.text
                                    for part in getattr(item, "content", [])
                                    if getattr(part, "type", None) == "output_text"
                                    and isinstance(getattr(part, "text", None), str)
                                ]
                                if texts:
                                    response_content["output"] = "\n".join(texts)
                            elif item_type == "reasoning":
                                summaries = [
                                    part.text
                                    for part in getattr(item, "summary", [])
                                    if isinstance(getattr(part, "text", None), str)
                                ]
                                if summaries:
                                    response_content["reasoning_summary"] = "\n".join(
                                        summaries
                                    )
                                    attrs.pop("reasoning_unavailable_reason", None)
                        status = getattr(response, "status", None)
                        if isinstance(status, str):
                            attrs["finish_reason"] = status[:256]
                        for choice in getattr(response, "choices", None) or []:
                            finish = getattr(choice, "finish_reason", None)
                            if isinstance(finish, str) and len(finish) <= 256:
                                attrs["finish_reason"] = finish
                            message = getattr(choice, "message", None)
                            output = getattr(message, "content", None)
                            if isinstance(output, str):
                                response_content["output"] = output
                            summary = getattr(message, "reasoning_summary", None)
                            if isinstance(summary, str):
                                response_content["reasoning_summary"] = summary
                                attrs.pop("reasoning_unavailable_reason", None)
                            for tool in getattr(message, "tool_calls", None) or []:
                                tool_id = getattr(tool, "id", None)
                                if isinstance(tool_id, str) and len(tool_id) <= 128:
                                    with self._emitter._lock:
                                        if len(self._emitter.model_calls) < 4096:
                                            self._emitter.model_calls[tool_id] = (
                                                call.span_id
                                            )
                    except Exception:  # noqa: BLE001 - SDK metadata is optional
                        self._emitter._drop()
                    self._emitter._emit(
                        call,
                        phase,
                        duration,
                        kind="model",
                        attributes=attrs,
                        content=response_content,
                    )
                    self._depth.active = False

            cast(Any, measured)._edgecitadel_observed = True
            setattr(agent, name, measured)
