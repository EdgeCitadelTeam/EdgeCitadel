"""JetStream pull-consumer adapter skeleton.

Contract: handler must produce (result_payload: dict, task_state: str)
for command/delegation/cancel, OR return None for types with no reply
(heartbeat, status, broadcast, log — but those don't land on inbox anyway).

Ack happens only after successful handler return AND successful result publish
(for command/delegation/cancel). in_progress() is called periodically to
extend ack_wait.
"""
from __future__ import annotations
import asyncio, json, logging, uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.js.errors import BadRequestError, NotFoundError

from .validator import default_validator, ValidationError
from aggregator.jetstream_bootstrap import ensure_stream, ensure_consumer

log = logging.getLogger(__name__)

HOP_COUNT_MAX = 8


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


@dataclass
class Context:
    agent_id: str
    nc: NATS
    js: object
    msg: Msg

    async def in_progress(self) -> None:
        await self.msg.in_progress()

    async def publish_progress(self, task_id: str, *, body: str = "",
                               progress: Optional[int] = None,
                               extra: Optional[dict] = None) -> None:
        """Publish a task.progress envelope. `extra` merges into payload
        alongside body/progress (used by Gemma to carry skill_id during
        streaming)."""
        payload: dict = {"message": body}
        if progress is not None:
            payload["progress"] = progress
        if extra:
            payload.update(extra)
        env = {"v": 1, "id": str(uuid.uuid4()), "type": "task.progress",
               "sender_id": self.agent_id,
               "task_id": task_id, "task_state": "working",
               "timestamp": now_iso(), "payload": payload}
        await self.nc.publish(
            f"agents.{self.agent_id}.task_progress.{task_id}",
            json.dumps(env).encode())


Handler = Callable[[dict, Context], Awaitable[tuple[dict, str]]]


class PullConsumer:
    def __init__(self, *, agent_id: str, nc: NATS, handler: Handler,
                 ack_wait_sec: int = 300, max_deliver: int = 3,
                 max_ack_pending: int = 1,
                 sender_allowlist: Optional[set[str]] = None):
        self.agent_id = agent_id
        self.nc = nc
        self.js = nc.jetstream()
        self.handler = handler
        self.ack_wait_sec = ack_wait_sec
        self.max_deliver = max_deliver
        self.max_ack_pending = max_ack_pending
        self.sender_allowlist = sender_allowlist
        self.validator = default_validator()
        self._running = False

    async def run(self) -> None:
        await ensure_stream(self.js)
        await ensure_consumer(self.js, self.agent_id,
                              ack_wait_sec=self.ack_wait_sec,
                              max_ack_pending=self.max_ack_pending,
                              max_deliver=self.max_deliver)
        psub = await self.js.pull_subscribe(
            subject=f"agents.{self.agent_id}.inbox",
            durable=f"{self.agent_id}_inbox")

        self._running = True
        while self._running:
            try:
                msgs = await psub.fetch(batch=1, timeout=30)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.warning("fetch error: %s", e)
                await asyncio.sleep(1); continue
            for m in msgs:
                await self._handle_msg(m)

    async def stop(self) -> None:
        self._running = False

    async def _handle_msg(self, msg: Msg) -> None:
        try:
            env = json.loads(msg.data)
        except json.JSONDecodeError:
            await msg.term(); return

        try:
            self.validator.validate_envelope(env)
        except ValidationError as e:
            log.warning("%s: invalid envelope, terminating: %s", self.agent_id, e)
            await msg.term(); return

        if self.sender_allowlist is not None and \
                env["sender_id"] not in self.sender_allowlist:
            log.warning("%s: bridge_sender_not_allowlisted sender=%s",
                        self.agent_id, env["sender_id"])
            await msg.term(); return

        if env["type"] == "delegation" and env.get("hop_count", 0) >= HOP_COUNT_MAX:
            await self._publish_result(env, task_state="rejected",
                                       error="hop_count_exceeded")
            await msg.ack(); return

        keepalive = asyncio.create_task(self._keepalive(msg))
        try:
            ctx = Context(agent_id=self.agent_id, nc=self.nc, js=self.js,
                          msg=msg)
            result, state = await self.handler(env, ctx)
            await self._publish_result(env, task_state=state, payload=result)
            await msg.ack()
        except Exception as e:
            log.exception("%s: handler failed: %s", self.agent_id, e)
            try:
                await self._publish_result(env, task_state="failed",
                                           error=type(e).__name__)
            except Exception:
                pass
            # Best-effort log envelope so the dashboard's Logs tab surfaces
            # handler crashes alongside successes (LogViewer reads
            # payload.level/source/message).
            try:
                from .template import publish_log
                await publish_log(
                    self.nc, self.agent_id,
                    level="ERROR", source="handler",
                    message=f"handler raised {type(e).__name__}: {e}",
                    extra={"task_id": env.get("task_id"),
                           "envelope_type": env.get("type")})
            except Exception:
                pass
            await msg.nak()
        finally:
            keepalive.cancel()

    async def _keepalive(self, msg: Msg) -> None:
        cadence = max(1.0, self.ack_wait_sec / 3)
        try:
            while True:
                await asyncio.sleep(cadence)
                await msg.in_progress()
        except asyncio.CancelledError:
            return

    async def _publish_result(self, inbound: dict, *, task_state: str,
                              payload: Optional[dict] = None,
                              error: Optional[str] = None) -> None:
        if inbound["type"] not in ("command", "delegation", "cancel"):
            return
        out = {
            "v": 1, "id": str(uuid.uuid4()),
            "type": "result",
            "sender_id": self.agent_id,
            "recipient_id": inbound["sender_id"],
            "task_id": inbound["task_id"],
            "task_state": task_state,
            "timestamp": now_iso(),
            "payload": (payload or {}) | ({"error": error} if error else {}),
        }
        if inbound.get("context_id"):
            out["context_id"] = inbound["context_id"]
        data = json.dumps(out).encode()
        # durable publish to sender's inbox
        await self.js.publish(f"agents.{inbound['sender_id']}.inbox", data,
                              headers={"Nats-Msg-Id": out["id"]})
        # mirror to own outbox (plain NATS; best-effort)
        await self.nc.publish(f"agents.{self.agent_id}.outbox", data)
