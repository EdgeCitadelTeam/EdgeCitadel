"""JetStream inbox primitives shared by installed plugin runtimes."""

from __future__ import annotations

from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DiscardPolicy,
    RetentionPolicy,
    StreamConfig,
)
from nats.js.errors import BadRequestError, NotFoundError

STREAM_NAME = "AGENT_INBOX"


async def ensure_stream(js: JetStreamContext, agent_id: str) -> object:
    """Ensure the current JetStream domain owns exactly this destination."""
    subject = f"agents.{agent_id}.inbox"
    subjects = {subject}
    try:
        existing = await js.stream_info(STREAM_NAME)
        configured = set(existing.config.subjects or [])
        if "agents.*.inbox" in configured:
            consumers = await js.consumers_info(STREAM_NAME)
            subjects.update(
                consumer.config.filter_subject
                for consumer in consumers
                if isinstance(consumer.config.filter_subject, str)
                and consumer.config.filter_subject.startswith("agents.")
                and consumer.config.filter_subject.endswith(".inbox")
            )
            configured.discard("agents.*.inbox")
        subjects.update(configured)
    except NotFoundError:
        pass
    config = StreamConfig(
        name=STREAM_NAME,
        subjects=sorted(subjects),
        retention=RetentionPolicy.WORK_QUEUE,
        discard=DiscardPolicy.NEW,
        max_age=24 * 60 * 60,
        max_bytes=1024 * 1024 * 1024,
        max_msg_size=1024 * 1024,
        duplicate_window=5 * 60,
    )
    try:
        return await js.update_stream(config)
    except (NotFoundError, BadRequestError):
        return await js.add_stream(config)


async def ensure_consumer(
    js: JetStreamContext,
    agent_id: str,
    ack_wait_sec: int = 300,
    max_ack_pending: int = 1,
    max_deliver: int = 3,
) -> object:
    """Ensure the destination's explicit-ack durable consumer exists."""
    config = ConsumerConfig(
        durable_name=f"{agent_id}_inbox",
        filter_subject=f"agents.{agent_id}.inbox",
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=ack_wait_sec,
        max_ack_pending=max_ack_pending,
        max_deliver=max_deliver,
    )
    try:
        return await js.add_consumer(STREAM_NAME, config)
    except BadRequestError:
        return await js.consumer_info(STREAM_NAME, f"{agent_id}_inbox")
