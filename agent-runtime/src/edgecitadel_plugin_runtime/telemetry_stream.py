"""Versioned Core telemetry stream provisioning, separate from command delivery."""

from __future__ import annotations

from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    DiscardPolicy,
    ReplayPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamInfo,
)
from nats.js.errors import NotFoundError

STREAM_NAME = "EDGECITADEL_TELEMETRY_V1"
EVENT_SUBJECT = "edgecitadel.telemetry.v1.*"
SETTLEMENT_SUBJECT = "edgecitadel.telemetry.settlement.v1"
CONSUMER_NAME = "core_trace_ingest_v1"
STREAM_BYTES = 128 * 1024 * 1024


class TelemetryConfigurationError(RuntimeError):
    """Existing telemetry resources differ from the versioned configuration."""


def stream_config() -> StreamConfig:
    return StreamConfig(
        name=STREAM_NAME,
        subjects=[EVENT_SUBJECT],
        retention=RetentionPolicy.LIMITS,
        storage=StorageType.FILE,
        discard=DiscardPolicy.NEW,
        max_bytes=STREAM_BYTES,
        max_msgs=100_000,
        max_msgs_per_subject=-1,
        max_msg_size=18 * 1024,
        max_age=3600,
        duplicate_window=120,
        max_consumers=1,
        num_replicas=1,
    )


def consumer_config() -> ConsumerConfig:
    return ConsumerConfig(
        durable_name=CONSUMER_NAME,
        filter_subject=EVENT_SUBJECT,
        deliver_policy=DeliverPolicy.ALL,
        replay_policy=ReplayPolicy.INSTANT,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30,
        max_ack_pending=128,
        max_waiting=4,
        max_deliver=-1,
    )


async def ensure_telemetry_stream(
    js: JetStreamContext, *, create: bool = True
) -> StreamInfo:
    """Call only on the configured Core JetStream context; never rewrite drift.

    Deployment must reserve this stream's bytes independently of AGENT_INBOX.
    LIMITS retention permits age eviction; only application-level Core settlement,
    not a broker acknowledgement, permits retiring the local replay copy.
    """
    expected = stream_config()
    try:
        info = await js.stream_info(STREAM_NAME)
    except NotFoundError:
        if not create:
            raise
        info = await js.add_stream(expected)
    fields = (
        "name",
        "subjects",
        "retention",
        "storage",
        "discard",
        "max_bytes",
        "max_msgs",
        "max_msg_size",
        "max_age",
        "duplicate_window",
        "max_consumers",
        "max_msgs_per_subject",
        "num_replicas",
        "no_ack",
        "sealed",
        "allow_rollup_hdrs",
        "mirror",
        "sources",
        "republish",
        "subject_transform",
        "discard_new_per_subject",
    )
    if any(getattr(info.config, name) != getattr(expected, name) for name in fields):
        raise TelemetryConfigurationError("telemetry_stream_configuration_mismatch")
    return info


async def ensure_telemetry_consumer(js: JetStreamContext) -> ConsumerInfo:
    """Create the one pull durable; existing cursor and ACK state remain intact."""
    expected = consumer_config()
    try:
        info = await js.consumer_info(STREAM_NAME, CONSUMER_NAME)
    except NotFoundError:
        info = await js.add_consumer(STREAM_NAME, expected)
    fields = (
        "durable_name",
        "filter_subject",
        "ack_policy",
        "ack_wait",
        "max_ack_pending",
        "max_waiting",
        "max_deliver",
        "deliver_policy",
        "replay_policy",
        "deliver_subject",
        "deliver_group",
        "filter_subjects",
        "backoff",
        "opt_start_seq",
        "opt_start_time",
    )
    if any(getattr(info.config, name) != getattr(expected, name) for name in fields):
        raise TelemetryConfigurationError("telemetry_consumer_configuration_mismatch")
    return info
