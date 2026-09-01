"""Supervisor-owned reconciliation for destination inbox stream subjects."""

from __future__ import annotations

import asyncio
import os
import re
import sys

from nats import connect
from nats.js.api import DiscardPolicy, RetentionPolicy, StreamConfig
from nats.js.errors import NotFoundError


STREAM_NAME = "AGENT_INBOX"
_AGENT_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z")


def inbox_subject(agent_id: str) -> str:
    if _AGENT_ID.fullmatch(agent_id) is None:
        raise ValueError("invalid agent id")
    return f"agents.{agent_id}.inbox"


async def ensure_inbox_subjects(js, agent_ids: list[str]):
    desired = {inbox_subject(agent_id) for agent_id in agent_ids}
    try:
        existing = await js.stream_info(STREAM_NAME)
    except NotFoundError:
        existing = None

    subjects = set(desired)
    if existing is not None:
        configured = set(existing.config.subjects or [])
        if "agents.*.inbox" in configured:
            consumers = await js.consumers_info(STREAM_NAME)
            subjects.update(
                consumer.config.filter_subject
                for consumer in consumers
                if isinstance(consumer.config.filter_subject, str)
                and re.fullmatch(
                    r"agents\.[a-z0-9][a-z0-9_-]{0,63}\.inbox",
                    consumer.config.filter_subject,
                )
            )
            configured.discard("agents.*.inbox")
        subjects.update(configured)

    config = StreamConfig(
        name=STREAM_NAME,
        subjects=sorted(subjects),
        retention=RetentionPolicy.WORK_QUEUE,
        discard=DiscardPolicy.NEW,
        max_age=24 * 60 * 60,
        max_bytes=1 * 1024 * 1024 * 1024,
        max_msg_size=1 * 1024 * 1024,
        duplicate_window=5 * 60,
    )
    if existing is None:
        return await js.add_stream(config)
    return await js.update_stream(config)


async def _run() -> None:
    url = os.environ["NATS_URL"]
    token = os.environ["NATS_TOKEN"]
    domain = os.environ.get("NATS_DOMAIN")
    raw_ids = os.environ.get("EDGECITADEL_AGENT_IDS", "")
    agent_ids = [value for value in raw_ids.split(",") if value]
    if not agent_ids:
        raise ValueError("no agent ids supplied")
    client = await connect(
        servers=[url], token=token, name="edgecitadel-stream-reconciler"
    )
    try:
        js = client.jetstream(domain=domain) if domain else client.jetstream()
        await ensure_inbox_subjects(js, agent_ids)
    finally:
        await client.drain()


def main() -> int:
    try:
        asyncio.run(_run())
    except Exception as error:  # noqa: BLE001 - redacted process boundary
        print(
            f"inbox stream reconciliation failed: {type(error).__name__}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
