import asyncio
import json
import logging
import os
import time
import uuid

import nats
from nats.js.api import StreamConfig, RetentionPolicy

import database

logger = logging.getLogger(__name__)

SKIP_AGENT_IDS = {"dashboard", "system", "mqtt-broker", "broadcast", ""}

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

# Subject mapping (replaces MQTT topic conventions):
#   agents.{name}.heartbeat   <- agents/heartbeat/{name}
#   agents.{name}.register    <- agents/register/{name}
#   agents.{name}.inbox       <- agents/inbox/{name}, openclaw/{d}/{a}/cmd
#   agents.{name}.outbox      <- openclaw/{d}/{a}/result
#   tasks.{id}.assign         <- task assignment
#   tasks.{id}.stream         <- LLM token streaming (new)
#   tasks.{id}.complete       <- task completion
#   system.broadcast           <- agents/broadcast


class OpenClawAggregator:
    def __init__(self):
        self.nc: nats.NATS | None = None
        self.js = None  # JetStream context
        self.ws_connections: list = []
        self.stream_connections: list = []
        self._seen_msg_keys: dict[str, float] = {}
        self._subscriptions: list = []

    async def connect(self):
        """Connect to NATS server and set up subscriptions."""
        self.nc = await nats.connect(
            NATS_URL,
            reconnected_cb=self._reconnected,
            disconnected_cb=self._disconnected,
            error_cb=self._error,
            max_reconnect_attempts=-1,  # reconnect forever
            reconnect_time_wait=1,
        )
        logger.info(f"Connected to NATS at {NATS_URL}")

        # Initialize JetStream
        self.js = self.nc.jetstream()
        await self._setup_streams()

        # Subscribe to all agent subjects (replaces MQTT # wildcard)
        sub_agents = await self.nc.subscribe("agents.>", cb=self._on_agent_message)
        sub_tasks = await self.nc.subscribe("tasks.>", cb=self._on_task_message)
        sub_system = await self.nc.subscribe("system.>", cb=self._on_system_message)
        self._subscriptions = [sub_agents, sub_tasks, sub_system]

    async def _setup_streams(self):
        """Create JetStream streams for conversation persistence."""
        try:
            await self.js.add_stream(
                config=StreamConfig(
                    name="CONVERSATIONS",
                    subjects=["conversations.>"],
                    retention=RetentionPolicy.LIMITS,
                    max_bytes=1024 * 1024 * 512,  # 512MB
                ),
            )
            logger.info("JetStream stream CONVERSATIONS ready")
        except Exception as e:
            if "already in use" in str(e).lower():
                logger.info("JetStream stream CONVERSATIONS already exists")
            else:
                logger.error(f"Failed to create CONVERSATIONS stream: {e}")

        try:
            await self.js.create_key_value(bucket="AGENT_STATE")
            logger.info("JetStream K/V bucket AGENT_STATE ready")
        except Exception as e:
            if "already in use" in str(e).lower():
                logger.info("JetStream K/V bucket AGENT_STATE already exists")
            else:
                logger.error(f"Failed to create AGENT_STATE bucket: {e}")

    async def _reconnected(self):
        logger.info(f"Reconnected to NATS at {self.nc.connected_url.netloc}")

    async def _disconnected(self):
        logger.warning("Disconnected from NATS, will reconnect")

    async def _error(self, e):
        logger.error(f"NATS error: {e}")

    async def _on_agent_message(self, msg):
        """Handle messages on agents.{name}.{action} subjects."""
        subject = msg.subject
        parts = subject.split(".")
        if len(parts) < 3:
            return

        agent_id = parts[1]
        action = parts[2]  # heartbeat, register, inbox, outbox, status

        try:
            payload = msg.data.decode("utf-8", errors="replace")
        except Exception:
            payload = str(msg.data)

        # Store raw episode
        deployment = "local"
        ts = int(time.time())
        try:
            database.insert_episode(deployment, subject, payload, ts)
        except Exception as e:
            logger.error(f"Failed to insert episode: {e}")

        # Parse into structured records
        parsed_msg = self._parse_nats_message(deployment, subject, agent_id, action, payload)

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

    async def _on_task_message(self, msg):
        """Handle messages on tasks.{id}.{action} subjects."""
        subject = msg.subject
        parts = subject.split(".")
        if len(parts) < 3:
            return

        task_id = parts[1]
        action = parts[2]  # assign, stream, complete, failed, progress

        try:
            payload = msg.data.decode("utf-8", errors="replace")
        except Exception:
            payload = str(msg.data)

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
                # Token streaming — broadcast to WebSocket for dashboard
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

    async def _on_system_message(self, msg):
        """Handle messages on system.> subjects."""
        try:
            payload = msg.data.decode("utf-8", errors="replace")
        except Exception:
            payload = str(msg.data)

        deployment = "local"
        ts = int(time.time())
        try:
            database.insert_episode(deployment, msg.subject, payload, ts)
        except Exception as e:
            logger.error(f"Failed to insert episode: {e}")

        event = {"deployment": deployment, "topic": msg.subject, "payload": payload, "ts": ts}
        await self._broadcast(event)

    def _parse_nats_message(self, deployment: str, subject: str, agent_id: str,
                            action: str, payload: str) -> dict | None:
        """Parse a NATS message and store structured records."""
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
        }
        message_type = action_type_map.get(action, "info")

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

        # Deduplication
        if correlation_id:
            dedup_key = f"{correlation_id}:{stored_agent_id}:{receiver_id}:{message_type}"
            now_ts = time.time()
            if dedup_key in self._seen_msg_keys and (now_ts - self._seen_msg_keys[dedup_key]) < 5:
                return None
            self._seen_msg_keys[dedup_key] = now_ts
            if len(self._seen_msg_keys) > 1000:
                cutoff = now_ts - 10
                self._seen_msg_keys = {k: v for k, v in self._seen_msg_keys.items() if v > cutoff}

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
        """Publish a message to a NATS subject."""
        if self.nc is None or not self.nc.is_connected:
            raise ConnectionError("Not connected to NATS")
        await self.nc.publish(subject, payload.encode())

    async def disconnect(self):
        for sub in self._subscriptions:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
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
