"""
Aggregator NATS glue.

Plain NATS subscribers: agents.*.register / .heartbeat / .status / .outbox /
    .log / system.broadcast / tasks.* / $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>

Durable JetStream consumer: agents.aggregator.inbox (for results returned to HTTP callers).

All routing is keyed off the strict envelope schema; malformed envelopes are
dropped silently with a logged reason (preserves aggregator liveness).
"""
from __future__ import annotations
import asyncio, json, logging, os
from datetime import datetime, timezone
from pathlib import Path

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from . import database as db
from .validator import EnvelopeValidator, ValidationError

log = logging.getLogger(__name__)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


class MessageRouter:
    def __init__(self, *, db_path: str,
                 envelope_schema: Path, card_schema: Path):
        self.db_path = db_path
        self.validator = EnvelopeValidator(envelope_schema, card_schema)
        self.cache: dict[str, dict] = {}    # agent_id -> Agent Card
        self.pending_tasks: dict[str, asyncio.Future] = {}  # task_id -> future
        self.nc: NATS | None = None
        self.js = None

    # ---- plain-NATS subscriber handlers ----

    async def on_register(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None: return
        try:
            self.validator.validate_register(env)
        except ValidationError as e:
            log.warning("rejecting register from %s: %s", env.get("sender_id"), e)
            return
        card = env["payload"]
        self.cache[env["sender_id"]] = card
        db.upsert_agent_card(card, timestamp=env["timestamp"])
        log.info("registered %s (kind=%s)", env["sender_id"],
                 card["metadata"]["runtime.kind"])

    async def on_heartbeat(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "heartbeat": return
        db.update_heartbeat(env["sender_id"], env["timestamp"])

    async def on_status(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "status": return
        db.update_agent_state(env["sender_id"], env["agent_state"])
        db.insert_message(env)

    async def on_log(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "log": return
        db.insert_message(env)

    async def on_outbox(self, msg: Msg) -> None:
        """Outbox mirror: authoritative audit path for inbox traffic."""
        env = self._parse_and_validate(msg.data)
        if env is None: return
        # We persist every outbox event so the dashboard has a canonical view
        db.insert_message(env)
        # If this outbox is a result matching an HTTP-driven pending task, resolve it
        if env["type"] == "result":
            f = self.pending_tasks.pop(env.get("task_id", ""), None)
            if f is not None and not f.done():
                f.set_result(env)

    async def on_broadcast(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None: return
        db.insert_message(env)

    async def on_advisory(self, msg: Msg) -> None:
        """$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>."""
        try:
            adv = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        # subject tail: ...MAX_DELIVERIES.AGENT_INBOX.<agent>.<consumer>
        parts = msg.subject.split(".")
        agent = parts[-2] if len(parts) >= 2 else "unknown"
        consumer = parts[-1] if parts else "unknown"
        # Extract original headers if present
        hdrs = (adv.get("headers") or {})
        orig_sender = hdrs.get("Original-Sender") or adv.get("sender_id")
        task_id = hdrs.get("Task-Id") or adv.get("task_id")
        db.insert_poison_event(agent_id=agent, consumer=consumer,
                               task_id=task_id, original_sender=orig_sender,
                               detected_at=now_iso(), advisory=adv)
        log.warning("poison message on %s (consumer=%s, task_id=%s)",
                    agent, consumer, task_id)

    async def on_openclaw_ingress(self, msg: Msg) -> None:
        """Translate openclaw.{session}.command.{target} → agents.{target}.inbox."""
        parts = msg.subject.split(".")
        if len(parts) < 4 or parts[0] != "openclaw":
            return
        session_id, kind = parts[1], parts[2]
        env = self._parse(msg.data)
        if env is None: return
        if kind == "command" and len(parts) == 4:
            target = parts[3]
            # server-set sender_id, do NOT trust browser
            out = {
                "v": 1, "id": env.get("id") or _uuid4_str(),
                "type": "command",
                "sender_id": f"openclaw-{session_id}",
                "recipient_id": target,
                "task_id": env.get("task_id") or _uuid4_str(),
                "timestamp": now_iso(),
                "payload": env.get("payload", {})
            }
            await self.js.publish(f"agents.{target}.inbox",
                                  json.dumps(out).encode(),
                                  headers={"Nats-Msg-Id": out["id"]})
            await self.nc.publish(f"agents.openclaw-{session_id}.outbox",
                                  json.dumps(out).encode())

    # ---- helpers ----

    def _parse(self, data: bytes) -> dict | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            log.warning("non-JSON message dropped")
            return None

    def _parse_and_validate(self, data: bytes) -> dict | None:
        env = self._parse(data)
        if env is None: return None
        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("dropping malformed envelope: %s", e)
            return None
        return env


class AggregatorApp:
    """Wires MessageRouter to NATS subscriptions and durable consumer."""

    def __init__(self, nats_url: str, nats_token: str, db_path: str,
                 envelope_schema: Path, card_schema: Path):
        self.nats_url = nats_url; self.nats_token = nats_token
        self.router = MessageRouter(db_path=db_path,
                                    envelope_schema=envelope_schema,
                                    card_schema=card_schema)

    async def start(self) -> None:
        self.router.nc = NATS()
        await self.router.nc.connect(servers=[self.nats_url],
                                     token=self.nats_token)
        nc = self.router.nc
        self.router.js = nc.jetstream()

        await nc.subscribe("agents.*.register", cb=self.router.on_register)
        await nc.subscribe("agents.*.heartbeat", cb=self.router.on_heartbeat)
        await nc.subscribe("agents.*.status", cb=self.router.on_status)
        await nc.subscribe("agents.*.log", cb=self.router.on_log)
        await nc.subscribe("agents.*.outbox", cb=self.router.on_outbox)
        await nc.subscribe("system.broadcast", cb=self.router.on_broadcast)
        await nc.subscribe(
            "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>",
            cb=self.router.on_advisory)
        await nc.subscribe("openclaw.*.>", cb=self.router.on_openclaw_ingress)

        from .jetstream_bootstrap import ensure_stream, ensure_consumer
        await ensure_stream(self.router.js)
        # aggregator's own inbox: no serial constraint
        await ensure_consumer(self.router.js, "aggregator",
                              ack_wait_sec=60, max_ack_pending=100)
        # subscribe durable consumer to drain results
        psub = await self.router.js.pull_subscribe(
            "agents.aggregator.inbox", durable="aggregator_inbox")
        asyncio.create_task(self._drain_own_inbox(psub))

        await self._publish_self_register()
        await self._broadcast_request_register()

    async def _publish_self_register(self) -> None:
        card = {
            "name": "aggregator", "description": "EdgeCitadel aggregator.",
            "version": "0.1.0",
            "url": "nats://edgecitadel/agents.aggregator.inbox",
            "provider": {"organization": "EdgeCitadel"},
            "capabilities": {"streaming": False},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["aggregator"],
                "runtime.heartbeat_interval_sec": 30}}
        env = {"v": 1, "id": _uuid4(), "type": "register",
               "sender_id": "aggregator", "timestamp": now_iso(),
               "payload": card}
        await self.router.nc.publish("agents.aggregator.register",
                                     json.dumps(env).encode())

    async def _broadcast_request_register(self) -> None:
        env = {"v": 1, "id": _uuid4(), "type": "broadcast",
               "sender_id": "aggregator", "timestamp": now_iso(),
               "payload": {"action": "request_register"}}
        await self.router.nc.publish("system.broadcast",
                                     json.dumps(env).encode())

    async def _drain_own_inbox(self, psub) -> None:
        while True:
            try:
                msgs = await psub.fetch(batch=10, timeout=30)
            except Exception:
                await asyncio.sleep(1); continue
            for m in msgs:
                env = self.router._parse_and_validate(m.data)
                if env:
                    db.insert_message(env)
                    if env["type"] == "result":
                        f = self.router.pending_tasks.pop(
                            env.get("task_id", ""), None)
                        if f is not None and not f.done():
                            f.set_result(env)
                await m.ack()


def _uuid4() -> str:
    import uuid; return str(uuid.uuid4())


def _uuid4_str() -> str:
    import uuid; return str(uuid.uuid4())
