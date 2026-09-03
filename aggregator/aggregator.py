"""
Aggregator NATS glue.

Plain NATS subscribers: agents.*.register / .heartbeat / .status / .outbox /
    .log / system.broadcast / tasks.* / $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.AGENT_INBOX.>

Durable JetStream consumer: agents.aggregator.inbox (for results returned to HTTP callers).

All routing is keyed off the strict envelope schema; malformed envelopes are
dropped silently with a logged reason (preserves aggregator liveness).
"""

from __future__ import annotations
import asyncio
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg

from . import database as db
from .validator import EnvelopeValidator, ValidationError

log = logging.getLogger(__name__)


def now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class MessageRouter:
    def __init__(self, *, db_path: str, envelope_schema: Path, card_schema: Path):
        self.db_path = db_path
        self.validator = EnvelopeValidator(envelope_schema, card_schema)
        self.cache: dict[str, dict] = {}  # agent_id -> Agent Card
        self.pending_tasks: dict[str, asyncio.Future] = {}  # task_id -> future
        self.nc: NATS | None = None
        self.js = None
        # Optional WebSocket fan-out hub. AggregatorApp wires this in
        # before start(); broadcast failures are logged but never break
        # the NATS handler chain.
        self.hub = None  # type: ignore[assignment]

    async def _hub_broadcast(self, env: dict) -> None:
        if self.hub is None:
            return
        try:
            await self.hub.broadcast(env)
        except Exception as e:  # noqa: BLE001
            log.warning("ws envelope broadcast failed: %s", e)

    async def _hub_event(
        self, event: str, data: dict, *, agent_id: str | None = None
    ) -> None:
        if self.hub is None:
            return
        try:
            await self.hub.broadcast_event(event, data, agent_id=agent_id)
        except Exception as e:  # noqa: BLE001
            log.warning("ws %s broadcast failed: %s", event, e)

    # ---- plain-NATS subscriber handlers ----

    async def on_register(self, msg: Msg) -> None:
        env = self._parse(msg.data)
        if env is None:
            return
        try:
            self.validator.validate_register(env)
        except ValidationError as e:
            log.warning("rejecting register from %s: %s", env.get("sender_id"), e)
            return
        card = env["payload"]
        self.cache[env["sender_id"]] = card
        db.upsert_agent_card(card, timestamp=env["timestamp"])
        log.info(
            "registered %s (kind=%s)",
            env["sender_id"],
            card["metadata"]["runtime.kind"],
        )
        await self._hub_event(
            "agent_registered",
            {"agent_id": env["sender_id"], "card": card},
            agent_id=env["sender_id"],
        )

    async def on_heartbeat(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "heartbeat":
            return
        db.update_heartbeat(env["sender_id"], env["timestamp"])

    def _deployment_for(self, env: dict) -> str:
        """Resolve the deployment ('default' | 'test' | ...) for this envelope
        by looking up the sender's cached A2A card. Falls back to recipient,
        then 'default'. Lets the dashboard filter test traffic via the
        existing showTestAgents toggle even for messages sent TO production
        agents from a test runner that registered with runtime.deployment=test."""
        for who in (env.get("sender_id"), env.get("recipient_id")):
            card = self.cache.get(who) if who else None
            dep = (card or {}).get("metadata", {}).get("runtime.deployment")
            if dep:
                return dep
        return "default"

    async def on_status(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "status":
            return
        db.update_agent_state(env["sender_id"], env["agent_state"])
        db.insert_message(env, deployment=self._deployment_for(env))
        await self._hub_event(
            "agent_status_change",
            {"agent_id": env["sender_id"], "agent_state": env["agent_state"]},
            agent_id=env["sender_id"],
        )

    async def on_log(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None or env["type"] != "log":
            return
        db.insert_message(env, deployment=self._deployment_for(env))
        # Forward only WARN/ERROR-level log envelopes — INFO would flood the
        # dashboard's notification stream. Frontend filters on data.level.
        payload = env.get("payload") or {}
        if (payload.get("level") or "").upper() in ("ERROR", "WARN", "WARNING"):
            await self._hub_event(
                "log",
                {
                    "level": payload.get("level"),
                    "message": payload.get("message", ""),
                    "source": payload.get("source"),
                    "agent_id": env["sender_id"],
                },
                agent_id=env["sender_id"],
            )

    async def on_outbox(self, msg: Msg) -> None:
        """Outbox mirror: authoritative audit path for inbox traffic."""
        env = self._parse_and_validate(msg.data)
        if env is None:
            return
        deployment = self._deployment_for(env)
        # We persist every outbox event so the dashboard has a canonical view
        db.insert_message(env, deployment=deployment)
        # Push to subscribed dashboard sockets — global firehose plus the
        # per-agent streams that match either sender or recipient. Carry the
        # resolved deployment on the WS frame so client-side filters
        # (showTestAgents) work on real-time messages identically to
        # historical /api/messages rows.
        await self._hub_broadcast({**env, "deployment": deployment})
        # If this outbox is a result matching an HTTP-driven pending task, resolve it
        if env["type"] == "result":
            f = self.pending_tasks.pop(env.get("task_id", ""), None)
            if f is not None and not f.done():
                f.set_result(env)

    async def on_broadcast(self, msg: Msg) -> None:
        env = self._parse_and_validate(msg.data)
        if env is None:
            return
        db.insert_message(env, deployment=self._deployment_for(env))

    async def on_advisory(self, msg: Msg) -> None:
        """Handle ``...MAX_DELIVERIES.<stream>.<consumer>`` advisories."""
        try:
            adv = json.loads(msg.data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return
        if not isinstance(adv, dict):
            return
        # NATS advisories identify the stream and consumer, not the filtered
        # Agent. The source envelope below is the authoritative recipient.
        parts = msg.subject.split(".")
        consumer = parts[-1] if parts else "unknown"
        subject_stream = parts[-2] if len(parts) >= 2 else None
        diagnostic_agent = (
            consumer.removesuffix("_inbox")
            if consumer.endswith("_inbox")
            else "unknown"
        )

        def record_poison(
            task_id: str | None = None,
            original_sender: str | None = None,
            agent_id: str | None = None,
        ) -> None:
            db.insert_poison_event(
                agent_id=agent_id or diagnostic_agent,
                consumer=consumer,
                task_id=task_id,
                original_sender=original_sender,
                detected_at=now_iso(),
                advisory=adv,
            )

        log.warning(
            "poison message for consumer %s (stream_seq=%s)",
            consumer,
            adv.get("stream_seq"),
        )
        stream = adv.get("stream")
        advisory_consumer = adv.get("consumer")
        stream_seq = adv.get("stream_seq")
        if (
            stream != "AGENT_INBOX"
            or subject_stream != stream
            or advisory_consumer != consumer
            or type(stream_seq) is not int
            or stream_seq <= 0
        ):
            record_poison()
            return
        try:
            info = await self.js.consumer_info(stream, consumer)
        except Exception as error:
            log.warning(
                "could not verify max-delivery consumer state: %s",
                type(error).__name__,
            )
            record_poison()
            return
        config = getattr(info, "config", None)
        delivered = getattr(info, "delivered", None)
        ack_floor = getattr(info, "ack_floor", None)
        max_deliver = getattr(config, "max_deliver", None)
        max_ack_pending = getattr(config, "max_ack_pending", None)
        delivered_stream_seq = getattr(delivered, "stream_seq", None)
        delivered_consumer_seq = getattr(delivered, "consumer_seq", None)
        acked_consumer_seq = getattr(ack_floor, "consumer_seq", None)
        deliveries = adv.get("deliveries")
        if (
            max_ack_pending != 1
            or type(max_deliver) is not int
            or max_deliver <= 0
            or deliveries != max_deliver
            or delivered_stream_seq != stream_seq
            or type(delivered_consumer_seq) is not int
            or type(acked_consumer_seq) is not int
            or delivered_consumer_seq - acked_consumer_seq < max_deliver
        ):
            log.warning("max-delivery advisory did not match consumer state")
            record_poison()
            return
        try:
            original = await self.js.get_msg(stream, seq=stream_seq)
            original_env = json.loads(original.data)
        except Exception as error:  # NATS uses multiple not-found/timeout types.
            log.warning(
                "could not retrieve max-delivery source message: %s",
                type(error).__name__,
            )
            record_poison()
            return
        if not isinstance(original_env, dict):
            record_poison()
            return
        try:
            self.validator.validate_envelope(original_env)
        except ValidationError as error:
            log.warning("max-delivery source envelope is invalid: %s", error)
            record_poison()
            return
        agent = original_env.get("recipient_id")
        if (
            original_env.get("type") not in {"command", "delegation"}
            or not isinstance(agent, str)
            or consumer != f"{agent}_inbox"
        ):
            record_poison()
            return
        orig_sender = original_env["sender_id"]
        task_id = original_env["task_id"]
        record_poison(task_id, orig_sender, agent)
        result = {
            "v": 1,
            "id": _system_result_id(task_id),
            "type": "result",
            "sender_id": "edgecitadel-system",
            "recipient_id": orig_sender,
            "task_id": task_id,
            "task_state": "failed",
            "timestamp": now_iso(),
            "payload": {
                "error": "recipient_unavailable",
                "recipient_id": agent,
                "trigger": "max_deliveries",
            },
        }
        try:
            self.validator.validate_envelope(result)
        except ValidationError as error:
            log.warning("advisory correlation is invalid; no result emitted: %s", error)
            return
        encoded = json.dumps(result).encode()
        ack = await self.js.publish(
            f"agents.{orig_sender}.inbox",
            encoded,
            headers={"Nats-Msg-Id": f"edgecitadel-system-undeliverable-{task_id}"},
        )
        if getattr(ack, "duplicate", False) is True:
            return
        await self.nc.publish("agents.edgecitadel-system.outbox", encoded)

    async def on_task_progress(self, msg: Msg) -> None:
        """agents.*.task_progress.{task_id} — Phase 2.5 streaming. Persist
        and broadcast via WebSocket bridge for live UI rendering."""
        env = self._parse_and_validate(msg.data)
        if env is None or env.get("type") != "task.progress":
            return
        db.insert_message(env, deployment=self._deployment_for(env))
        await self._hub_broadcast(env)

    # ---- helpers ----

    def _parse(self, data: bytes) -> dict | None:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            log.warning("non-JSON message dropped")
            return None

    def _parse_and_validate(self, data: bytes) -> dict | None:
        env = self._parse(data)
        if env is None:
            return None
        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("dropping malformed envelope: %s", e)
            return None
        return env


class AggregatorApp:
    """Wires MessageRouter to NATS subscriptions and durable consumer."""

    def __init__(
        self,
        nats_url: str,
        nats_token: str,
        db_path: str,
        envelope_schema: Path,
        card_schema: Path,
    ):
        self.nats_url = nats_url
        self.nats_token = nats_token
        self.router = MessageRouter(
            db_path=db_path, envelope_schema=envelope_schema, card_schema=card_schema
        )

    async def start(self) -> None:
        self.router.nc = NATS()
        await self.router.nc.connect(servers=[self.nats_url], token=self.nats_token)
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
            cb=self.router.on_advisory,
        )
        await nc.subscribe("agents.*.task_progress.>", cb=self.router.on_task_progress)

        from .jetstream_bootstrap import ensure_stream, ensure_consumer

        await ensure_stream(self.router.js, "aggregator")
        # aggregator's own inbox: no serial constraint
        await ensure_consumer(
            self.router.js, "aggregator", ack_wait_sec=60, max_ack_pending=100
        )
        # subscribe durable consumer to drain results
        psub = await self.router.js.pull_subscribe(
            "agents.aggregator.inbox", durable="aggregator_inbox"
        )
        asyncio.create_task(self._drain_own_inbox(psub))

        await self._publish_self_register()
        await self._broadcast_request_register()

    async def _publish_self_register(self) -> None:
        card = {
            "name": "aggregator",
            "description": "EdgeCitadel aggregator.",
            "version": "0.1.0",
            "url": "nats://edgecitadel/agents.aggregator.inbox",
            "provider": {"organization": "EdgeCitadel"},
            "capabilities": {"streaming": False},
            "securitySchemes": {},
            "metadata": {
                "runtime.kind": "native",
                "runtime.roles": ["aggregator"],
                "runtime.conformance": "L1",
                "runtime.heartbeat_interval_sec": 30,
            },
        }
        env = {
            "v": 1,
            "id": _uuid4(),
            "type": "register",
            "sender_id": "aggregator",
            "timestamp": now_iso(),
            "payload": card,
        }
        await self.router.nc.publish(
            "agents.aggregator.register", json.dumps(env).encode()
        )

    async def _broadcast_request_register(self) -> None:
        env = {
            "v": 1,
            "id": _uuid4(),
            "type": "broadcast",
            "sender_id": "aggregator",
            "timestamp": now_iso(),
            "payload": {"action": "request_register"},
        }
        await self.router.nc.publish("system.broadcast", json.dumps(env).encode())

    async def _drain_own_inbox(self, psub) -> None:
        while True:
            try:
                msgs = await psub.fetch(batch=10, timeout=30)
            except Exception:
                await asyncio.sleep(1)
                continue
            for m in msgs:
                env = self.router._parse_and_validate(m.data)
                if env:
                    db.insert_message(env)
                    if env["type"] == "result":
                        f = self.router.pending_tasks.pop(env.get("task_id", ""), None)
                        if f is not None and not f.done():
                            f.set_result(env)
                await m.ack()


def _uuid4() -> str:
    return str(uuid.uuid4())


def _system_result_id(task_id: str) -> str:
    """Return a stable schema-valid ID for one system reconciliation result."""
    raw = bytearray(
        hashlib.sha256(f"recipient-unavailable:{task_id}".encode()).digest()[:16]
    )
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))
