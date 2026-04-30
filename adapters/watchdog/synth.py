"""Synthesised envelope builder + JetStream publish helper.

Three trigger paths produce the same envelope shape:
- heartbeat_staleness   (primary)
- sticky_offline        (new command to offline agent)
- max_deliveries_advisory (backstop)

Dedup key: Nats-Msg-Id: watchdog-syn-{task_id}. JetStream's 5-min
duplicate_window collapses double-fires across paths.
"""
from __future__ import annotations
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Literal

from nats.aio.client import Client as NATS

log = logging.getLogger(__name__)

WATCHDOG_AGENT_ID = "watchdog-1"
Trigger = Literal["heartbeat_staleness", "sticky_offline", "max_deliveries_advisory"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


def build_synth_envelope(*, original_sender: str, original_task_id: str,
                         original_context_id: str | None,
                         offline_agent_id: str,
                         trigger: Trigger) -> dict:
    """Build a synthesised result envelope. Pure function — no I/O."""
    env: dict = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "result",
        "sender_id": WATCHDOG_AGENT_ID,
        "recipient_id": original_sender,
        "task_id": original_task_id,
        "timestamp": now_iso(),
        "task_state": "failed",
        "payload": {
            "error": "recipient_offline",
            "offline_agent_id": offline_agent_id,
            "detected_at": now_iso(),
            "trigger": trigger,
        },
    }
    if original_context_id:
        env["context_id"] = original_context_id
    return env


async def publish_synth(js: object, nc: NATS, env: dict) -> None:
    """JetStream-publish to the original sender's inbox with the dedup
    header, and mirror to watchdog-1's outbox per ADR-0006.

    Best-effort on the outbox mirror (plain NATS): a publish failure
    here does not block the JetStream durable publish."""
    data = json.dumps(env).encode()
    headers = {"Nats-Msg-Id": f"watchdog-syn-{env['task_id']}"}
    await js.publish(f"agents.{env['recipient_id']}.inbox",
                     data, headers=headers)
    try:
        await nc.publish(f"agents.{WATCHDOG_AGENT_ID}.outbox", data)
    except Exception as e:  # noqa: BLE001
        log.warning("synth outbox mirror failed (%s): %s",
                    type(e).__name__, e)
