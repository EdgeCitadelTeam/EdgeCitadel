"""EdgeCitadel watchdog Plugin runtime.

Subscribes to:
- agents.*.register   — populate declared_interval
- agents.*.heartbeat  — update last_seen, clear offline flag
- agents.*.outbox     — observe in-flight tasks; sticky-offline synth
- $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.> — backstop synth

Holds a durable JetStream consumer on agents.watchdog-1.inbox per spec
convention. The inbox handler rejects all unknown commands.

The staleness check loop runs every check_cadence_sec (default 5s):
identifies offline transitions, synthesises failures for pending tasks,
publishes a status envelope to its own outbox.

Sender identity on synthesised envelopes is `watchdog-1` per spec rev 6.
See ADR-0007 for the trigger-model rationale.
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import signal
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from edgecitadel_plugin_runtime.agent_card import build_card
from edgecitadel_plugin_runtime.pull_consumer import Context, PullConsumer
from edgecitadel_plugin_runtime.template import publish_log
from edgecitadel_plugin_runtime.validator import default_validator, ValidationError

from .state import (
    WatchdogState,
)
from .synth import WATCHDOG_AGENT_ID, build_synth_envelope, now_iso, publish_synth

log = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """Override hook for tests that can't wait the real 25–65 s thresholds.
    Production leaves defaults; tests set WATCHDOG_THRESHOLD_FLOOR_SEC,
    WATCHDOG_TOLERANCE_SEC, WATCHDOG_DEFAULT_INTERVAL_SEC to tune."""
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


async def make_inbox_handler(env: dict, ctx: Context) -> tuple[dict, str]:
    """Reject all commands/delegations to watchdog-1 with unknown_command."""
    if env["type"] not in ("command", "delegation"):
        return ({}, "rejected")
    return ({"reason": "unknown_command"}, "rejected")


class WatchdogApp:
    """Wires the WatchdogState to NATS handlers + the staleness loop."""

    def __init__(self, *, config_path: Path, check_cadence_sec: float = 5.0):
        self.card = build_card(config_path)
        self.agent_id = self.card["name"]
        if self.agent_id != WATCHDOG_AGENT_ID:
            raise ValueError(
                f"watchdog config agent_id must be {WATCHDOG_AGENT_ID}, "
                f"got {self.agent_id}"
            )
        self.check_cadence_sec = check_cadence_sec
        self.state = WatchdogState(
            threshold_floor_sec=_env_int("WATCHDOG_THRESHOLD_FLOOR_SEC", 20),
            tolerance_sec=_env_int("WATCHDOG_TOLERANCE_SEC", 5),
            default_interval_sec=_env_int("WATCHDOG_DEFAULT_INTERVAL_SEC", 30),
        )
        self.validator = default_validator()
        self.nc: NATS | None = None
        self.js = None

    # ---- Handlers ---------------------------------------------------------

    async def on_register(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None or env["type"] != "register":
            return
        try:
            self.validator.validate_register(env)
        except ValidationError as e:
            log.warning(
                "watchdog: drop bad register from %s: %s", env.get("sender_id"), e
            )
            return
        sender = env["sender_id"]
        if sender == self.agent_id:
            return  # don't track ourselves
        interval = env["payload"]["metadata"]["runtime.heartbeat_interval_sec"]
        self.state.record_register(sender, interval_sec=interval)

    async def on_heartbeat(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None or env["type"] != "heartbeat":
            return
        sender = env["sender_id"]
        if sender == self.agent_id:
            return
        ts = _parse_iso(env["timestamp"])
        recovered = self.state.record_heartbeat(sender, ts)
        if recovered:
            await publish_log(
                self.nc,
                self.agent_id,
                level="INFO",
                source="watchdog",
                message=f"agent {sender} back online",
            )

    async def on_outbox(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None:
            return
        env_type = env.get("type")
        if env_type in ("command", "delegation"):
            recipient = env.get("recipient_id")
            task_id = env.get("task_id")
            if not (recipient and task_id):
                return
            sticky = self.state.record_outbox_command(
                recipient_id=recipient, task_id=task_id, sender_id=env["sender_id"]
            )
            if sticky:
                await self._synth(
                    original_sender=env["sender_id"],
                    original_task_id=task_id,
                    original_context_id=env.get("context_id"),
                    offline_agent_id=recipient,
                    trigger="sticky_offline",
                )
        elif env_type == "result":
            worker = env.get("sender_id")
            task_id = env.get("task_id")
            if worker and task_id:
                self.state.record_outbox_result(worker_id=worker, task_id=task_id)

    async def on_advisory(self, msg: Msg) -> None:
        try:
            adv = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        # Subject tail: ...MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>
        parts = msg.subject.split(".")
        offline_agent = parts[-2] if len(parts) >= 2 else "unknown"
        # The advisory does not always carry the original envelope's
        # task_id and sender_id directly. Phase 1 aggregator parses
        # hdrs.get('Original-Sender') / hdrs.get('Task-Id') with a
        # fallback to advisory body fields. Mirror that here.
        hdrs = adv.get("headers") or {}
        task_id = hdrs.get("Task-Id") or adv.get("task_id")
        original_sender = hdrs.get("Original-Sender") or adv.get("sender_id")
        if not (task_id and original_sender):
            log.warning(
                "watchdog: advisory missing task_id/sender for %s", offline_agent
            )
            return
        await self._synth(
            original_sender=original_sender,
            original_task_id=task_id,
            original_context_id=None,
            offline_agent_id=offline_agent,
            trigger="max_deliveries_advisory",
        )

    # ---- Staleness loop ---------------------------------------------------

    async def staleness_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.check_cadence_sec)
                return  # stop.set()
            except asyncio.TimeoutError:
                pass
            now = datetime.now(timezone.utc)
            transitions = self.state.staleness_check(now=now)
            for offline_agent, pending in transitions:
                for task_id, original_sender in pending:
                    await self._synth(
                        original_sender=original_sender,
                        original_task_id=task_id,
                        original_context_id=None,
                        offline_agent_id=offline_agent,
                        trigger="heartbeat_staleness",
                    )
                # Single status envelope mirroring the offline transition
                status = {
                    "v": 1,
                    "id": str(uuid.uuid4()),
                    "type": "status",
                    "sender_id": self.agent_id,
                    "timestamp": now_iso(),
                    "agent_state": "offline",
                    "payload": {
                        "observed_agent_id": offline_agent,
                        "trigger": "heartbeat_staleness",
                    },
                }
                try:
                    await self.nc.publish(
                        f"agents.{self.agent_id}.outbox", json.dumps(status).encode()
                    )
                except Exception as e:  # noqa: BLE001
                    log.warning("watchdog status outbox publish failed: %s", e)

    # ---- Internal helpers -------------------------------------------------

    async def _synth(
        self,
        *,
        original_sender: str,
        original_task_id: str,
        original_context_id: Optional[str],
        offline_agent_id: str,
        trigger: str,
    ) -> None:
        env = build_synth_envelope(
            original_sender=original_sender,
            original_task_id=original_task_id,
            original_context_id=original_context_id,
            offline_agent_id=offline_agent_id,
            trigger=trigger,
        )
        await publish_synth(self.js, self.nc, env)
        await publish_log(
            self.nc,
            self.agent_id,
            level="WARN",
            source="watchdog",
            message=(
                f"synthesised recipient_offline "
                f"(trigger={trigger}, "
                f"offline_agent={offline_agent_id}, "
                f"task_id={original_task_id})"
            ),
        )

    def _parse(self, data: bytes) -> dict | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None


def _parse_iso(ts: str) -> datetime:
    """Parse the canonical .sssZ timestamp."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


async def main(
    config_path: str | Path,
    *,
    _stop_event: Optional[asyncio.Event] = None,
    _check_cadence_sec: Optional[float] = None,
) -> None:
    """Entry point. Test hooks: _stop_event lets fixtures cancel cleanly,
    _check_cadence_sec lets fixtures speed up the loop. Production picks
    WATCHDOG_CHECK_CADENCE_SEC from env if set."""
    cadence = (
        _check_cadence_sec
        if _check_cadence_sec is not None
        else float(_env_int("WATCHDOG_CHECK_CADENCE_SEC", 5))
    )
    app = WatchdogApp(config_path=Path(config_path), check_cadence_sec=cadence)

    nc = NATS()
    await nc.connect(
        servers=[os.environ["NATS_URL"]], token=os.environ.get("NATS_TOKEN")
    )
    app.nc = nc
    domain = os.environ.get("NATS_DOMAIN")
    app.js = nc.jetstream(domain=domain) if domain else nc.jetstream()

    # Publish own register
    reg = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "register",
        "sender_id": app.agent_id,
        "timestamp": now_iso(),
        "payload": app.card,
    }
    await nc.publish(f"agents.{app.agent_id}.register", json.dumps(reg).encode())
    await publish_log(
        nc,
        app.agent_id,
        level="INFO",
        source="lifecycle",
        message=f"watchdog registered as {app.agent_id} "
        f"(check_cadence={app.check_cadence_sec}s)",
    )

    # Subscribe
    await nc.subscribe("agents.*.register", cb=app.on_register)
    await nc.subscribe("agents.*.heartbeat", cb=app.on_heartbeat)
    await nc.subscribe("agents.*.outbox", cb=app.on_outbox)
    await nc.subscribe(
        "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>", cb=app.on_advisory
    )

    # Heartbeat task for own identity
    async def _heartbeat() -> None:
        interval = app.card["metadata"]["runtime.heartbeat_interval_sec"]
        while True:
            hb = {
                "v": 1,
                "id": str(uuid.uuid4()),
                "type": "heartbeat",
                "sender_id": app.agent_id,
                "timestamp": now_iso(),
                "payload": {},
            }
            await nc.publish(
                f"agents.{app.agent_id}.heartbeat", json.dumps(hb).encode()
            )
            await asyncio.sleep(interval)

    hb_task = asyncio.create_task(_heartbeat())

    # Inbox pull consumer (rejects unknowns)
    pc = PullConsumer(
        agent_id=app.agent_id,
        nc=nc,
        handler=make_inbox_handler,
        ack_wait_sec=30,
        max_ack_pending=1,
        max_deliver=3,
    )

    # Stop signal: external (test) or signal handler
    stop = _stop_event or asyncio.Event()
    if _stop_event is None:
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, stop.set)

    consumer_task = asyncio.create_task(pc.run())
    staleness_task = asyncio.create_task(app.staleness_loop(stop))

    await stop.wait()

    # Graceful shutdown: status offline, drain
    off = {
        "v": 1,
        "id": str(uuid.uuid4()),
        "type": "status",
        "sender_id": app.agent_id,
        "timestamp": now_iso(),
        "agent_state": "offline",
        "payload": {"reason": "shutdown"},
    }
    try:
        await nc.publish(f"agents.{app.agent_id}.status", json.dumps(off).encode())
    except Exception:
        pass
    await pc.stop()
    hb_task.cancel()
    consumer_task.cancel()
    staleness_task.cancel()
    try:
        await nc.drain()
    except Exception:
        pass


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    config = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path(__file__).resolve().parent / "config.yaml"
    )
    asyncio.run(main(config))
