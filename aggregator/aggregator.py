import asyncio
import json
import logging
import os
import time
import uuid

import nats
from nats.js.api import RetentionPolicy, StorageType

import database

logger = logging.getLogger(__name__)

SKIP_AGENT_IDS = {"dashboard", "system", "nats-server", "broadcast", ""}

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
NATS_TOKEN = os.environ.get("NATS_TOKEN", "")

# Subject mapping (NATS dot-separated):
#   agents.{name}.heartbeat
#   agents.{name}.register
#   agents.{name}.inbox       <- commands TO agent
#   agents.{name}.outbox      <- results FROM agent
#   agents.{name}.status
#   agents.{name}.log
#   tasks.{id}.assign
#   tasks.{id}.stream
#   tasks.{id}.complete
#   tasks.{id}.failed
#   system.broadcast


class OpenClawAggregator:
    def __init__(self):
        self.nc: nats.NATS | None = None
        self.js = None  # JetStream context
        self.kv = None  # AGENT_STATE K/V bucket
        self.ws_connections: list = []
        self.stream_connections: list = []
        self._seen_msg_keys: dict[str, float] = {}
        self._dedup_max_size = 500
        self._dedup_ttl = 10  # seconds
        self._listener_tasks: list[asyncio.Task] = []

    async def connect(self):
        """Connect to NATS server and start message listeners."""
        connect_opts = {"servers": NATS_URL}
        if NATS_TOKEN:
            connect_opts["token"] = NATS_TOKEN

        self.nc = await nats.connect(**connect_opts)
        logger.info(f"Connected to NATS at {NATS_URL}")

        # Set up JetStream
        self.js = self.nc.jetstream()

        # Create CONVERSATIONS stream for persistent message history
        try:
            await self.js.add_stream(
                name="CONVERSATIONS",
                subjects=["agents.>", "tasks.>", "system.>"],
                retention=RetentionPolicy.LIMITS,
                max_msgs=10000,
                storage=StorageType.FILE,
            )
            logger.info("JetStream stream CONVERSATIONS ready")
        except Exception as e:
            logger.warning(f"JetStream stream setup: {e}")

        # Create AGENT_STATE K/V bucket for live state
        try:
            self.kv = await self.js.create_key_value(bucket="AGENT_STATE")
            logger.info("JetStream K/V bucket AGENT_STATE ready")
        except Exception as e:
            logger.warning(f"JetStream K/V setup: {e}")

        # Subscribe to wildcard subjects and start listener tasks
        sub_agents = await self.nc.subscribe("agents.>")
        sub_tasks = await self.nc.subscribe("tasks.>")
        sub_system = await self.nc.subscribe("system.>")

        self._listener_tasks.append(asyncio.create_task(self._agent_loop(sub_agents)))
        self._listener_tasks.append(asyncio.create_task(self._task_loop(sub_tasks)))
        self._listener_tasks.append(asyncio.create_task(self._system_loop(sub_system)))

    async def _agent_loop(self, sub):
        """Process messages on agents.> subjects."""
        try:
            async for msg in sub.messages:
                subject = msg.subject
                payload = msg.data.decode("utf-8", errors="replace") if msg.data else ""
                try:
                    await self._on_agent_message(subject, payload)
                except Exception as e:
                    logger.error(f"Error processing {subject}: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"NATS agent message loop error: {e}")

    async def _task_loop(self, sub):
        """Process messages on tasks.> subjects."""
        try:
            async for msg in sub.messages:
                subject = msg.subject
                payload = msg.data.decode("utf-8", errors="replace") if msg.data else ""
                try:
                    await self._on_task_message(subject, payload)
                except Exception as e:
                    logger.error(f"Error processing {subject}: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"NATS task message loop error: {e}")

    async def _system_loop(self, sub):
        """Process messages on system.> subjects."""
        try:
            async for msg in sub.messages:
                subject = msg.subject
                payload = msg.data.decode("utf-8", errors="replace") if msg.data else ""
                try:
                    await self._on_system_message(subject, payload)
                except Exception as e:
                    logger.error(f"Error processing {subject}: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning(f"NATS system message loop error: {e}")

    async def _on_agent_message(self, subject: str, payload: str):
        """Handle messages on agents.{name}.{action} subjects."""
        parts = subject.split(".")
        if len(parts) < 3:
            return

        agent_id = parts[1]
        action = parts[2]  # heartbeat, register, inbox, outbox, status, log

        # Store raw episode
        deployment = "local"
        ts = int(time.time())
        try:
            database.insert_episode(deployment, subject, payload, ts)
        except Exception as e:
            logger.error(f"Failed to insert episode: {e}")

        # Parse into structured records
        parsed_msg = self._parse_message(deployment, subject, agent_id, action, payload)

        # Broadcast raw event to /ws
        event = {"deployment": deployment, "topic": subject, "payload": payload, "ts": ts}
        await self._broadcast(event)

        # Broadcast structured event to /ws/stream
        if parsed_msg:
            stream_event = {"event": "message", "data": parsed_msg}
            await self._broadcast_stream(stream_event)

            msg_type = parsed_msg.get("message_type", "")
            evt_agent_id = parsed_msg.get("sender_id", "")
            if msg_type == "register" and evt_agent_id:
                await self._broadcast_stream({
                    "type": "agent_registered", "event": "agent_registered",
                    "agent_id": evt_agent_id, "status": "online",
                    "data": {"agent_id": evt_agent_id, "status": "online"},
                })
            elif msg_type == "heartbeat" and evt_agent_id:
                await self._broadcast_stream({
                    "type": "agent_status_change", "event": "agent_status_change",
                    "agent_id": evt_agent_id, "status": "online",
                    "data": {"agent_id": evt_agent_id, "status": "online"},
                })

        # Update K/V state for agent
        if self.kv and agent_id and agent_id not in SKIP_AGENT_IDS:
            try:
                state = json.dumps({
                    "agent_id": agent_id,
                    "action": action,
                    "last_seen": ts,
                })
                await self.kv.put(agent_id, state.encode())
            except Exception:
                pass

    async def _on_task_message(self, subject: str, payload: str):
        """Handle messages on tasks.{id}.{action} subjects."""
        parts = subject.split(".")
        if len(parts) < 3:
            return

        task_id = parts[1]
        action = parts[2]  # assign, stream, complete, failed, progress

        deployment = "local"
        ts = int(time.time())
        try:
            database.insert_episode(deployment, subject, payload, ts)
        except Exception as e:
            logger.error(f"Failed to insert episode: {e}")

        payload_obj = {}
        try:
            payload_obj = json.loads(payload) if payload else {}
        except (json.JSONDecodeError, TypeError):
            payload_obj = {"raw": payload}

        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Extract agent from payload for structured record storage
        agent_id = ""
        if isinstance(payload_obj, dict):
            agent_id = payload_obj.get("sender_id", payload_obj.get("sender", payload_obj.get("assigned_agent", "")))
        if agent_id and agent_id not in SKIP_AGENT_IDS:
            self._parse_message(deployment, subject, agent_id, action, payload)

        # Handle task lifecycle
        try:
            existing = database.get_task(task_id)
            if action == "assign":
                agent = payload_obj.get("assigned_agent", "")
                if existing:
                    database.update_task(task_id, status="assigned", assigned_agent=agent)
                else:
                    database.insert_task(
                        deployment=deployment,
                        title=payload_obj.get("title", payload_obj.get("prompt", "Task")),
                        description=payload_obj.get("description", ""),
                        assigned_agent=agent,
                        priority=payload_obj.get("priority", "normal"),
                        task_id=task_id,
                    )
            elif action == "progress":
                if existing:
                    database.update_task(task_id, status="running", started_at=now)
            elif action == "complete":
                if existing:
                    database.update_task(task_id, status="completed", completed_at=now,
                                         result=payload_obj.get("result", ""))
            elif action == "failed":
                if existing:
                    database.update_task(task_id, status="failed", completed_at=now,
                                         error_message=payload_obj.get("error", ""))
            elif action == "stream":
                pass
        except Exception as e:
            logger.error(f"Failed to handle task: {e}")

        # Broadcast to WebSocket
        event = {"deployment": deployment, "topic": subject, "payload": payload, "ts": ts}
        await self._broadcast(event)

        stream_event = {
            "event": "task_update" if action != "stream" else "token_stream",
            "data": {
                "task_id": task_id,
                "action": action,
                **payload_obj,
            },
        }
        await self._broadcast_stream(stream_event)

    async def _on_system_message(self, subject: str, payload: str):
        """Handle messages on system.> subjects."""
        deployment = "local"
        ts = int(time.time())
        try:
            database.insert_episode(deployment, subject, payload, ts)
        except Exception as e:
            logger.error(f"Failed to insert episode: {e}")

        # Also parse as a message so broadcasts appear in chat
        try:
            payload_obj = json.loads(payload) if payload else {}
        except (json.JSONDecodeError, TypeError):
            payload_obj = {"raw": payload}

        if isinstance(payload_obj, dict):
            sender_id = payload_obj.get("sender_id", payload_obj.get("from", "system"))
            content = payload_obj.get("message", payload_obj.get("content", ""))
            if content and sender_id not in SKIP_AGENT_IDS:
                self._parse_message("local", subject, sender_id, "broadcast", payload)

        event = {"deployment": deployment, "topic": subject, "payload": payload, "ts": ts}
        await self._broadcast(event)

    def _parse_message(self, deployment: str, subject: str, agent_id: str,
                       action: str, payload: str) -> dict | None:
        """Parse a message and store structured records."""
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        payload_obj = {}
        try:
            payload_obj = json.loads(payload) if payload else {}
        except (json.JSONDecodeError, TypeError):
            payload_obj = {"raw": payload}

        # Map action to message type
        action_type_map = {
            "heartbeat": "heartbeat",
            "register": "register",
            "inbox": "command",
            "outbox": "result",
            "status": "heartbeat",
            "cmd": "command",
            "result": "result",
            "log": "info",
            "logs": "info",
            "assign": "task_assign",
            "progress": "task_progress",
            "complete": "task_complete",
            "failed": "task_failed",
            "broadcast": "broadcast",
            "delegation": "delegation",
        }
        message_type = action_type_map.get(action, "info")

        # For inbox messages that are actually routed replies (type=response/result),
        # use "result" message_type and deduplicate against the outbox copy
        if isinstance(payload_obj, dict):
            payload_msg_type = payload_obj.get("type", payload_obj.get("message_type", ""))
            if action == "inbox" and payload_msg_type in ("response", "result"):
                message_type = "result"

        # Extract fields from payload
        sender_id = ""
        receiver_id = ""
        correlation_id = ""

        if isinstance(payload_obj, dict):
            sender_id = payload_obj.get("sender_id", payload_obj.get("from", payload_obj.get("sender", "")))
            receiver_id = payload_obj.get("receiver_id", payload_obj.get("to", ""))
            correlation_id = payload_obj.get("correlation_id",
                                             payload_obj.get("correlationId",
                                             payload_obj.get("task_id", "")))

        # Determine the stored agent_id
        if action == "inbox" and sender_id:
            stored_agent_id = sender_id
            if not receiver_id:
                receiver_id = agent_id
        elif sender_id and action not in ("inbox",):
            stored_agent_id = sender_id
            if not stored_agent_id or stored_agent_id in SKIP_AGENT_IDS:
                stored_agent_id = agent_id
        else:
            stored_agent_id = agent_id

        # Skip messages with no identifiable agent
        if not agent_id or (agent_id in SKIP_AGENT_IDS and not receiver_id):
            return None
        if agent_id in SKIP_AGENT_IDS and receiver_id and receiver_id not in SKIP_AGENT_IDS:
            agent_id = receiver_id

        # Upsert agent record
        agent_kwargs = {"status": "online", "last_heartbeat": now}
        if isinstance(payload_obj, dict):
            inner = payload_obj.get("payload", {}) if isinstance(payload_obj.get("payload"), dict) else {}
            merged = {**inner, **{k: v for k, v in payload_obj.items() if k != "payload"}}
            if merged.get("status"):
                agent_kwargs["status"] = merged["status"]
            if merged.get("display_name"):
                agent_kwargs["display_name"] = merged["display_name"]
            if merged.get("role"):
                agent_kwargs["role"] = merged["role"]
            if merged.get("device_type"):
                agent_kwargs["device_type"] = merged["device_type"]
            if merged.get("capabilities"):
                caps = merged["capabilities"]
                agent_kwargs["capabilities"] = json.dumps(caps) if isinstance(caps, (list, dict)) else caps
            if merged.get("ip_address"):
                agent_kwargs["ip_address"] = merged["ip_address"]
            if merged.get("model"):
                agent_kwargs["model"] = merged["model"]
            if merged.get("cpu_percent") is not None:
                agent_kwargs["cpu_percent"] = merged["cpu_percent"]
            if merged.get("memory_percent") is not None:
                agent_kwargs["memory_percent"] = merged["memory_percent"]

        if stored_agent_id not in SKIP_AGENT_IDS:
            try:
                database.upsert_agent(agent_id, deployment=deployment, **agent_kwargs)
            except Exception as e:
                logger.error(f"Failed to upsert agent: {e}")

        if receiver_id and receiver_id not in SKIP_AGENT_IDS:
            try:
                database.upsert_agent(receiver_id, deployment=deployment, status="online",
                                      last_heartbeat=now)
            except Exception:
                pass

        # Deduplication with bounded cache
        if correlation_id:
            dedup_key = f"{correlation_id}:{stored_agent_id}:{receiver_id}:{message_type}"
            now_ts = time.time()
            if dedup_key in self._seen_msg_keys and (now_ts - self._seen_msg_keys[dedup_key]) < 5:
                return None
            self._seen_msg_keys[dedup_key] = now_ts
            if len(self._seen_msg_keys) > self._dedup_max_size:
                cutoff = now_ts - self._dedup_ttl
                self._seen_msg_keys = {k: v for k, v in self._seen_msg_keys.items() if v > cutoff}
        elif message_type == "result" and stored_agent_id and receiver_id:
            content_str = ""
            if isinstance(payload_obj, dict):
                content_str = payload_obj.get("content", payload_obj.get("message", ""))
            dedup_key = f"reply:{stored_agent_id}:{receiver_id}:{hash(content_str[:200])}"
            now_ts = time.time()
            if dedup_key in self._seen_msg_keys and (now_ts - self._seen_msg_keys[dedup_key]) < 5:
                return None
            self._seen_msg_keys[dedup_key] = now_ts

        # Unwrap nested payload
        stored_payload = payload_obj
        if isinstance(payload_obj, dict) and "payload" in payload_obj:
            inner = payload_obj["payload"]
            if isinstance(inner, dict):
                stored_payload = inner
            elif isinstance(inner, str):
                stored_payload = {"message": inner}

        # Insert structured message (skip heartbeat/register)
        msg_id = None
        if message_type not in ("heartbeat", "register"):
            try:
                msg_id = database.insert_message(
                    deployment=deployment,
                    sender_id=stored_agent_id,
                    receiver_id=receiver_id,
                    message_type=message_type,
                    payload=stored_payload,
                    correlation_id=correlation_id,
                    timestamp=now,
                )
            except Exception as e:
                logger.error(f"Failed to insert message: {e}")

        # Insert log
        try:
            level = "NATS"
            log_message = payload[:500] if payload else ""
            log_source = subject
            if action in ("log", "logs") and isinstance(payload_obj, dict):
                log_inner = payload_obj.get("payload", {}) if isinstance(payload_obj.get("payload"), dict) else {}
                level = log_inner.get("level", payload_obj.get("level", "INFO")).upper()
                log_message = log_inner.get("message", payload_obj.get("message", log_message))
                log_source = log_inner.get("source", payload_obj.get("source", subject))
            database.insert_log(
                deployment=deployment,
                level=level,
                agent_id=agent_id,
                source=log_source,
                message=log_message[:500] if log_message else "",
                metadata=payload_obj if isinstance(payload_obj, dict) else {"raw": payload},
            )
        except Exception as e:
            logger.error(f"Failed to insert log: {e}")

        # Auto-create tasks from command messages with correlation_id
        if message_type == "command" and correlation_id:
            try:
                existing = database.get_task(correlation_id)
                if not existing:
                    content = ""
                    if isinstance(stored_payload, dict):
                        content = stored_payload.get("message", stored_payload.get("command", ""))
                    database.insert_task(
                        deployment=deployment,
                        title=content[:100] if content else f"Command to {receiver_id}",
                        description=content,
                        assigned_agent=receiver_id or agent_id,
                        priority="normal",
                        task_id=correlation_id,
                    )
                    database.update_task(correlation_id, status="running", started_at=now)
            except Exception as e:
                logger.error(f"Failed to auto-create task from command: {e}")

        # Auto-complete tasks from result messages with correlation_id
        if message_type == "result" and correlation_id:
            try:
                existing = database.get_task(correlation_id)
                if existing and existing["status"] not in ("completed", "failed"):
                    result_data = {}
                    if isinstance(stored_payload, dict):
                        result_data = stored_payload.get("result", stored_payload.get("message", ""))
                    database.update_task(correlation_id, status="completed",
                                         completed_at=now, result=result_data)
            except Exception as e:
                logger.error(f"Failed to auto-complete task from result: {e}")

        return {
            "id": msg_id or str(uuid.uuid4()),
            "deployment": deployment,
            "sender_id": stored_agent_id,
            "receiver_id": receiver_id,
            "message_type": message_type,
            "payload": stored_payload,
            "correlation_id": correlation_id,
            "timestamp": now,
        }

    async def _broadcast(self, event: dict):
        dead = []
        for ws in self.ws_connections:
            try:
                await ws.send_json(event)
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                self.ws_connections.remove(ws)
            except ValueError:
                pass

    async def _broadcast_stream(self, event: dict):
        dead = []
        for ws in self.stream_connections:
            try:
                await ws.send_json(event)
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                self.stream_connections.remove(ws)
            except ValueError:
                pass

    async def publish(self, subject: str, payload: str):
        """Publish a message to a NATS subject.
        Converts slash-separated topics to dot-separated NATS subjects for backward compat.
        """
        if self.nc is None or not self.nc.is_connected:
            raise ConnectionError("Not connected to NATS")
        # Convert MQTT-style slash topics to NATS dot subjects
        nats_subject = subject.replace("/", ".")
        await self.nc.publish(nats_subject, payload.encode())

    async def disconnect(self):
        for task in self._listener_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._listener_tasks.clear()
        if self.nc:
            await self.nc.drain()
            logger.info("Disconnected from NATS")

    async def heartbeat_monitor(self, interval: int = 15, timeout: int = 120):
        """Background task that marks agents offline if no heartbeat received."""
        while True:
            try:
                went_offline = database.mark_stale_agents_offline(timeout)
                for agent_id in went_offline:
                    event = {"type": "agent_status_change", "event": "agent_status_change",
                             "agent_id": agent_id, "status": "offline",
                             "data": {"agent_id": agent_id, "status": "offline"}}
                    await self._broadcast_stream(event)
            except Exception as e:
                logger.error(f"Heartbeat monitor error: {e}")
            await asyncio.sleep(interval)
