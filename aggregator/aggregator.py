import asyncio
import json
import logging
import time
import uuid

import paho.mqtt.client as mqtt

import database

logger = logging.getLogger(__name__)

SKIP_AGENT_IDS = {"dashboard", "system", "mqtt-broker", "broadcast", ""}


class OpenClawAggregator:
    def __init__(self):
        self.clients: dict[str, mqtt.Client] = {}
        self.ws_connections: list = []
        self.stream_connections: list = []  # /ws/stream connections
        self.loop: asyncio.AbstractEventLoop | None = None
        self._seen_msg_keys: dict[str, float] = {}  # dedup: key -> timestamp

    def connect_deployment(self, name: str, host: str, port: int,
                           mqtt_user: str = "", mqtt_pass: str = ""):
        if name in self.clients:
            self.disconnect_deployment(name)

        client = mqtt.Client(client_id=f"edge-citadel-{name}")
        client.reconnect_delay_set(min_delay=1, max_delay=30)
        if mqtt_user:
            client.username_pw_set(mqtt_user, mqtt_pass)
        client.on_message = self._make_handler(name)
        client.on_connect = lambda c, ud, flags, rc: c.subscribe("#")
        client.on_disconnect = lambda c, ud, rc: logger.warning(
            f"Disconnected from {name} (rc={rc}), will reconnect"
        )

        client.connect(host, port, keepalive=60)
        client.loop_start()
        self.clients[name] = client
        logger.info(f"Connected to deployment '{name}' at {host}:{port}")

    def _make_handler(self, deployment: str):
        def on_message(client, userdata, msg):
            try:
                payload = msg.payload.decode("utf-8", errors="replace")
            except Exception:
                payload = str(msg.payload)

            event = {
                "deployment": deployment,
                "topic": msg.topic,
                "payload": payload,
                "ts": int(time.time()),
            }

            try:
                database.insert_episode(deployment, msg.topic, payload, event["ts"])
            except Exception as e:
                logger.error(f"Failed to insert episode: {e}")

            # Parse MQTT message into structured records
            parsed_msg = self._parse_mqtt_message(deployment, msg.topic, payload)

            # Broadcast raw event to /ws connections
            if self.loop:
                asyncio.run_coroutine_threadsafe(self._broadcast(event), self.loop)

            # Broadcast structured event to /ws/stream connections
            if self.loop and parsed_msg:
                stream_event = {"event": "message", "data": parsed_msg}
                asyncio.run_coroutine_threadsafe(
                    self._broadcast_stream(stream_event), self.loop
                )
                # Broadcast agent lifecycle events
                msg_type = parsed_msg.get("message_type", "")
                agent_id = parsed_msg.get("sender_id", "")
                if msg_type == "register" and agent_id:
                    reg_event = {"type": "agent_registered", "event": "agent_registered",
                                 "agent_id": agent_id, "status": "online",
                                 "data": {"agent_id": agent_id, "status": "online"}}
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast_stream(reg_event), self.loop)
                elif msg_type == "heartbeat" and agent_id:
                    hb_event = {"type": "agent_status_change", "event": "agent_status_change",
                                "agent_id": agent_id, "status": "online",
                                "data": {"agent_id": agent_id, "status": "online"}}
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast_stream(hb_event), self.loop)

        return on_message

    def _parse_mqtt_message(self, deployment: str, topic: str, payload: str) -> dict | None:
        """Parse an MQTT message and store structured records (agent, message, log, task)."""
        parts = topic.split("/")
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Try to parse payload as JSON
        payload_obj = {}
        try:
            payload_obj = json.loads(payload) if payload else {}
        except (json.JSONDecodeError, TypeError):
            payload_obj = {"raw": payload}

        # Skip system topics ($SYS/...)
        if topic.startswith("$"):
            return None

        # Extract agent from topic: openclaw/{deployment}/{agent}/...
        agent_id = ""
        message_type = "info"
        receiver_id = ""
        correlation_id = ""

        if len(parts) >= 3 and parts[0] == "openclaw":
            agent_id = parts[2]
        elif len(parts) >= 3 and parts[0] == "agents":
            # agents/register/{name}, agents/heartbeat/{name}, agents/inbox/{name},
            # agents/status/{name}
            agent_id = parts[2]
        elif parts[0] == "agents" and len(parts) == 2:
            if parts[1] == "mirror":
                # agents/mirror is a duplicate of inbox — skip to avoid double-storing
                return None
            # agents/broadcast — agent extracted from payload
            agent_id = ""
        elif len(parts) >= 2 and parts[0] == "swarm":
            agent_id = parts[1]
        else:
            # Unknown topic prefix — skip to avoid phantom agents
            return None

        # Detect special topics
        is_heartbeat = "heartbeat" in topic
        is_register = "register" in topic
        is_status = "status" in parts  # agents/status/{agent}
        is_log = "log" in topic or "logs" in topic
        is_task = "task" in topic
        is_command = "cmd" in topic or "command" in topic
        is_result = ("result" in topic or "response" in topic) and not is_status

        # Determine message type
        if is_command:
            message_type = "command"
        elif is_result:
            message_type = "result"
        elif is_task:
            message_type = "task_assign"
        elif is_log:
            message_type = "info"
        elif is_heartbeat or is_status:
            message_type = "heartbeat"
        elif is_register:
            message_type = "register"
        else:
            message_type = payload_obj.get("message_type", payload_obj.get("type", "info"))

        # Extract fields from payload
        sender_id = ""
        if isinstance(payload_obj, dict):
            sender_id = payload_obj.get("sender_id", payload_obj.get("from", payload_obj.get("sender", "")))
            receiver_from_payload = payload_obj.get("receiver_id", payload_obj.get("to", ""))
            correlation_id = payload_obj.get("correlation_id",
                                            payload_obj.get("correlationId",
                                            payload_obj.get("task_id", "")))
            # For inbox/task topics, agent_id is the target from the topic — don't override
            # with sender. For other topics, sender from payload is more reliable.
            if sender_id and "inbox" not in topic and not is_task:
                agent_id = sender_id
            if receiver_from_payload:
                receiver_id = receiver_from_payload

        # Fill in sender/receiver for inbox topics
        if "inbox" in topic and sender_id:
            # agent_id stays as recipient from topic, sender_id is the sender
            if not receiver_id:
                receiver_id = agent_id
            # For message storage, agent_id is the sender
            stored_agent_id = sender_id
        else:
            stored_agent_id = agent_id

        # Skip messages with no identifiable agent (but allow dashboard-sent commands)
        if not agent_id or (agent_id in SKIP_AGENT_IDS and not receiver_id):
            return None
        # For dashboard-sent messages, use receiver as the primary agent context
        if agent_id in SKIP_AGENT_IDS and receiver_id and receiver_id not in SKIP_AGENT_IDS:
            agent_id = receiver_id

        # Upsert agent record (skip for dashboard/system senders)
        agent_kwargs = {"status": "online", "last_heartbeat": now}
        if isinstance(payload_obj, dict):
            # Some agents nest metadata under a "payload" key; merge both levels
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

        # Also register receiver as agent if present (skip blocklisted IDs)
        if receiver_id and receiver_id not in SKIP_AGENT_IDS:
            try:
                database.upsert_agent(receiver_id, deployment=deployment, status="online",
                                      last_heartbeat=now)
            except Exception:
                pass

        # Deduplicate: skip if same correlation_id + sender + receiver + type seen recently
        # (prevents double-storing when aggregator publishes to multiple MQTT topics)
        if correlation_id:
            dedup_key = f"{correlation_id}:{stored_agent_id}:{receiver_id}:{message_type}"
            now_ts = time.time()
            if dedup_key in self._seen_msg_keys and (now_ts - self._seen_msg_keys[dedup_key]) < 5:
                return None
            self._seen_msg_keys[dedup_key] = now_ts
            # Periodic cleanup
            if len(self._seen_msg_keys) > 1000:
                cutoff = now_ts - 10
                self._seen_msg_keys = {k: v for k, v in self._seen_msg_keys.items() if v > cutoff}

        # Unwrap nested payload: if the MQTT JSON has a "payload" field, use that
        # as the stored message payload (the frontend expects payload.message, not payload.payload.message)
        stored_payload = payload_obj
        if isinstance(payload_obj, dict) and "payload" in payload_obj:
            inner = payload_obj["payload"]
            if isinstance(inner, dict):
                stored_payload = inner
            elif isinstance(inner, str):
                stored_payload = {"message": inner}

        # Insert structured message (skip heartbeat/register for chat)
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

        # Insert log for all MQTT messages
        try:
            level = "MQTT"
            log_message = payload[:500] if payload else ""
            log_source = topic
            if is_log and isinstance(payload_obj, dict):
                # Extract from nested payload or top-level
                log_inner = payload_obj.get("payload", {}) if isinstance(payload_obj.get("payload"), dict) else {}
                level = log_inner.get("level", payload_obj.get("level", "INFO")).upper()
                log_message = log_inner.get("message", payload_obj.get("message", log_message))
                log_source = log_inner.get("source", payload_obj.get("source", topic))
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

        # Handle explicit task messages
        if is_task and isinstance(payload_obj, dict):
            try:
                # Merge nested payload for task fields
                task_inner = payload_obj.get("payload", {}) if isinstance(payload_obj.get("payload"), dict) else {}
                task_data = {**task_inner, **{k: v for k, v in payload_obj.items() if k != "payload"}}
                task_id = task_data.get("task_id", "") or correlation_id
                # Determine task action from topic suffix
                topic_action = parts[-1] if parts else ""
                if task_id:
                    existing = database.get_task(task_id)
                    if existing:
                        updates = {}
                        # Map topic action to status
                        if topic_action == "assign":
                            updates["status"] = "assigned"
                            updates["assigned_agent"] = agent_id
                        elif topic_action == "progress":
                            updates["status"] = "running"
                            updates["started_at"] = now
                        elif topic_action == "complete":
                            updates["status"] = "completed"
                            updates["completed_at"] = now
                            if task_data.get("result"):
                                updates["result"] = task_data["result"]
                        elif topic_action == "failed":
                            updates["status"] = "failed"
                            updates["completed_at"] = now
                            if task_data.get("error_message") or task_data.get("error"):
                                updates["error_message"] = task_data.get("error_message", task_data.get("error", ""))
                        # Also handle explicit status field
                        if task_data.get("status"):
                            updates["status"] = task_data["status"]
                            if task_data["status"] == "running":
                                updates["started_at"] = now
                            elif task_data["status"] in ("completed", "failed"):
                                updates["completed_at"] = now
                        if task_data.get("result") and "result" not in updates:
                            updates["result"] = task_data["result"]
                        if (task_data.get("error") or task_data.get("error_message")) and "error_message" not in updates:
                            updates["error_message"] = task_data.get("error_message", task_data.get("error", ""))
                        if updates:
                            database.update_task(task_id, **updates)
                    else:
                        database.insert_task(
                            deployment=deployment,
                            title=task_data.get("title", task_data.get("command", "Task")),
                            description=task_data.get("description", ""),
                            assigned_agent=task_data.get("assigned_agent", agent_id),
                            priority=task_data.get("priority", "normal"),
                            task_id=task_id,
                        )
                        # If the action is beyond "assign", update status immediately
                        if topic_action == "progress":
                            database.update_task(task_id, status="running", started_at=now)
                        elif topic_action == "complete":
                            database.update_task(task_id, status="completed", completed_at=now)
                        elif topic_action == "failed":
                            database.update_task(task_id, status="failed", completed_at=now)
            except Exception as e:
                logger.error(f"Failed to handle task: {e}")

        # Auto-create tasks from COMMAND messages with correlation_id
        if message_type == "command" and correlation_id and not is_task:
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

        # Auto-complete tasks from RESULT messages with correlation_id
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

        # Return structured message for WebSocket
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

    def disconnect_deployment(self, name: str):
        client = self.clients.pop(name, None)
        if client:
            client.loop_stop()
            client.disconnect()
            logger.info(f"Disconnected from deployment '{name}'")

    def disconnect_all(self):
        for name in list(self.clients.keys()):
            self.disconnect_deployment(name)

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

    def publish(self, deployment: str, topic: str, payload: str):
        client = self.clients.get(deployment)
        if client is None:
            raise KeyError(f"Deployment '{deployment}' not connected")
        client.publish(topic, payload)
