"""
Idempotent JetStream bootstrap: creates AGENT_INBOX stream and per-agent
durable consumers. Called on aggregator startup AND lazily by each adapter
when it first connects.
"""
from __future__ import annotations
from nats.js import JetStreamContext
from nats.js.api import (StreamConfig, ConsumerConfig, RetentionPolicy,
                         DiscardPolicy, AckPolicy)
from nats.js.errors import NotFoundError, BadRequestError

STREAM_NAME = "AGENT_INBOX"
SUBJECTS = ["agents.*.inbox"]


async def ensure_stream(js: JetStreamContext):
    # nats-py >=2.9 expects nanosecond ints for max_age and duplicate_window
    # and drops keys whose value is None during JSON serialization for
    # add/update_stream — passing storage=None caused the broker to receive
    # an invalid JSON config (NATS error 10025). Omitting storage uses the
    # server-side default (FILE).
    cfg = StreamConfig(
        name=STREAM_NAME,
        subjects=SUBJECTS,
        retention=RetentionPolicy.WORK_QUEUE,
        discard=DiscardPolicy.NEW,
        max_age=24 * 60 * 60,                    # 24h, seconds (nats-py → ns)
        max_bytes=1 * 1024 * 1024 * 1024,        # 1GB
        max_msg_size=1 * 1024 * 1024,            # 1MB
        duplicate_window=5 * 60,                 # 5min, seconds (nats-py → ns)
    )
    try:
        return await js.update_stream(cfg)
    except (NotFoundError, BadRequestError):
        return await js.add_stream(cfg)


async def ensure_consumer(js: JetStreamContext, agent_id: str,
                          ack_wait_sec: int = 300,
                          max_ack_pending: int = 1,
                          max_deliver: int = 3):
    cfg = ConsumerConfig(
        durable_name=f"{agent_id}_inbox",
        filter_subject=f"agents.{agent_id}.inbox",
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=ack_wait_sec,
        max_ack_pending=max_ack_pending,
        max_deliver=max_deliver,
    )
    try:
        return await js.add_consumer(STREAM_NAME, cfg)
    except BadRequestError:
        # exists; fetch info
        return await js.consumer_info(STREAM_NAME, cfg.durable_name)
