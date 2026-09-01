"""Compatibility boundary for shared exact-subject JetStream primitives."""

from nats.js import JetStreamContext

from edgecitadel_plugin_runtime.jetstream import (
    STREAM_NAME,
    ensure_consumer,
    ensure_stream as _ensure_stream,
)

SUBJECTS = ["agents.aggregator.inbox"]


async def ensure_stream(js: JetStreamContext, agent_id: str = "aggregator") -> object:
    return await _ensure_stream(js, agent_id)


__all__ = ["STREAM_NAME", "SUBJECTS", "ensure_consumer", "ensure_stream"]
