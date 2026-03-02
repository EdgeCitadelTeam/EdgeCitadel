import asyncio
import json
import logging
from datetime import datetime, timezone

import aiomqtt

from config import settings
from database import Agent, Message, SystemLog, Task, async_session, utcnow
from sqlalchemy import select

logger = logging.getLogger(__name__)


class MQTTService:
    def __init__(self, ws_manager):
        self.ws_manager = ws_manager
        self._client = None

    async def start(self):
        while True:
            try:
                async with aiomqtt.Client(
                    hostname=settings.mqtt_host,
                    port=settings.mqtt_port,
                    username=settings.mqtt_user,
                    password=settings.mqtt_pass,
                ) as client:
                    self._client = client
                    logger.info("Connected to MQTT broker at %s:%s", settings.mqtt_host, settings.mqtt_port)
                    await client.subscribe("agents/#")
                    async for msg in client.messages:
                        try:
                            await self._route_message(str(msg.topic), msg.payload.decode())
                        except Exception:
                            logger.exception("Error processing MQTT message on %s", msg.topic)
            except aiomqtt.MqttError as e:
                logger.warning("MQTT connection lost: %s. Reconnecting in 5s...", e)
                self._client = None
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                logger.info("MQTT service cancelled")
                self._client = None
                return
            except Exception:
                logger.exception("Unexpected MQTT error. Reconnecting in 5s...")
                self._client = None
                await asyncio.sleep(5)

    async def publish(self, topic: str, payload: dict):
        if self._client is None:
            raise RuntimeError("MQTT client not connected")
        await self._client.publish(topic, json.dumps(payload))

    async def _route_message(self, topic: str, raw_payload: str):
        try:
            data = json.loads(raw_payload)
        except json.JSONDecodeError:
            data = {"raw": raw_payload}

        sender = data.get("sender") or data.get("agent_id") or self._extract_agent_from_topic(topic)
        receiver = data.get("receiver") or self._extract_receiver(topic)
        msg_type = data.get("type") or data.get("message_type") or self._extract_type_from_topic(topic)
        correlation_id = data.get("correlation_id")
        ts = data.get("timestamp")
        timestamp = datetime.fromisoformat(ts) if ts else utcnow()

        async with async_session() as session:
            msg = Message(
                mqtt_topic=topic,
                sender_id=sender,
                receiver_id=receiver,
                message_type=msg_type,
                correlation_id=correlation_id,
                payload=data,
                raw_payload=raw_payload,
                timestamp=timestamp,
            )
            session.add(msg)
            await session.commit()
            msg_id = msg.id

            ws_message = {
                "event": "message",
                "data": {
                    "id": msg_id,
                    "mqtt_topic": topic,
                    "sender_id": sender,
                    "receiver_id": receiver,
                    "message_type": msg_type,
                    "correlation_id": correlation_id,
                    "payload": data,
                    "timestamp": timestamp.isoformat(),
                },
            }

            # Route by topic category
            topic_parts = topic.split("/")

            if "/heartbeat" in topic:
                await self._handle_heartbeat(session, sender, data)
            elif "/register" in topic:
                await self._handle_register(session, sender, data)
            elif "/status" in topic:
                await self._handle_status(session, sender, data)
            elif "/task/" in topic:
                await self._handle_task(session, topic_parts, data, correlation_id)
            elif "/logs" in topic or "/log" in topic:
                await self._handle_log(session, sender, data)

        await self.ws_manager.broadcast(ws_message)
        if sender:
            await self.ws_manager.send_to_agent_subscribers(sender, ws_message)
        if receiver and receiver != sender:
            await self.ws_manager.send_to_agent_subscribers(receiver, ws_message)

    async def _handle_heartbeat(self, session, agent_id: str, data: dict):
        if not agent_id:
            return
        result = await session.execute(select(Agent).where(Agent.id == agent_id))
        agent = result.scalar_one_or_none()
        now = utcnow()
        if agent:
            agent.status = "online"
            agent.last_heartbeat = now
            agent.cpu_percent = data.get("cpu_percent", agent.cpu_percent)
            agent.memory_percent = data.get("memory_percent", agent.memory_percent)
            agent.ip_address = data.get("ip_address", agent.ip_address)
            agent.updated_at = now
        else:
            agent = Agent(
                id=agent_id,
                display_name=data.get("display_name", agent_id),
                status="online",
                last_heartbeat=now,
                cpu_percent=data.get("cpu_percent"),
                memory_percent=data.get("memory_percent"),
                ip_address=data.get("ip_address"),
            )
            session.add(agent)
        await session.commit()

        await self.ws_manager.broadcast({
            "event": "agent_status_change",
            "data": {"agent_id": agent_id, "status": "online"},
        })

    async def _handle_register(self, session, agent_id: str, data: dict):
        if not agent_id:
            return
        result = await session.execute(select(Agent).where(Agent.id == agent_id))
        agent = result.scalar_one_or_none()
        now = utcnow()
        if agent:
            agent.display_name = data.get("display_name", agent.display_name)
            agent.role = data.get("role", agent.role)
            agent.device_type = data.get("device_type", agent.device_type)
            agent.model = data.get("model", agent.model)
            agent.capabilities = data.get("capabilities", agent.capabilities)
            agent.ip_address = data.get("ip_address", agent.ip_address)
            agent.status = "online"
            agent.last_heartbeat = now
            agent.updated_at = now
        else:
            agent = Agent(
                id=agent_id,
                display_name=data.get("display_name", agent_id),
                role=data.get("role"),
                device_type=data.get("device_type"),
                model=data.get("model"),
                capabilities=data.get("capabilities"),
                ip_address=data.get("ip_address"),
                status="online",
                last_heartbeat=now,
            )
            session.add(agent)
        await session.commit()

        await self.ws_manager.broadcast({
            "event": "agent_registered",
            "data": {"agent_id": agent_id, "status": "online"},
        })

    async def _handle_status(self, session, agent_id: str, data: dict):
        if not agent_id:
            return
        new_status = data.get("status", "online")
        result = await session.execute(select(Agent).where(Agent.id == agent_id))
        agent = result.scalar_one_or_none()
        if agent:
            agent.status = new_status
            agent.updated_at = utcnow()
            await session.commit()

        await self.ws_manager.broadcast({
            "event": "agent_status_change",
            "data": {"agent_id": agent_id, "status": new_status},
        })

    async def _handle_task(self, session, topic_parts: list, data: dict, correlation_id: str | None):
        task_id = data.get("task_id") or correlation_id
        if not task_id:
            return

        action = topic_parts[-1] if topic_parts else ""
        now = utcnow()

        result = await session.execute(select(Task).where(Task.id == task_id))
        task = result.scalar_one_or_none()

        if action == "assign":
            if task:
                task.status = "assigned"
                task.assigned_agent = data.get("assigned_agent")
                task.updated_at = now
            else:
                task = Task(
                    id=task_id,
                    title=data.get("title", "Untitled Task"),
                    description=data.get("description"),
                    status="assigned",
                    assigned_agent=data.get("assigned_agent"),
                    priority=data.get("priority", "normal"),
                    correlation_id=correlation_id,
                )
                session.add(task)
        elif action == "progress":
            if task:
                task.status = "running"
                if not task.started_at:
                    task.started_at = now
        elif action == "complete":
            if task:
                task.status = "completed"
                task.result = data.get("result")
                task.completed_at = now
        elif action == "failed":
            if task:
                task.status = "failed"
                task.error_message = data.get("error") or data.get("error_message")
                task.completed_at = now

        await session.commit()

    async def _handle_log(self, session, agent_id: str | None, data: dict):
        log = SystemLog(
            level=data.get("level", "INFO"),
            agent_id=agent_id,
            source=data.get("source", "agent"),
            message=data.get("message", str(data)),
            metadata_=data.get("metadata"),
        )
        session.add(log)
        await session.commit()

        log_entry = {
            "event": "log",
            "data": {
                "id": log.id,
                "level": log.level,
                "agent_id": log.agent_id,
                "source": log.source,
                "message": log.message,
                "metadata": log.metadata_,
                "timestamp": log.timestamp.isoformat(),
            },
        }
        await self.ws_manager.broadcast_log(log_entry)

    def _extract_agent_from_topic(self, topic: str) -> str | None:
        parts = topic.split("/")
        # patterns: agents/{action}/{agent_name}, agents/{agent_name}/{action}
        if len(parts) >= 3:
            return parts[2] if parts[1] in ("heartbeat", "register", "status", "logs", "log", "inbox") else parts[1]
        return None

    def _extract_receiver(self, topic: str) -> str | None:
        parts = topic.split("/")
        if "inbox" in parts:
            idx = parts.index("inbox")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        if "task" in parts:
            idx = parts.index("task")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        return None

    def _extract_type_from_topic(self, topic: str) -> str | None:
        parts = topic.split("/")
        for keyword in ("heartbeat", "register", "status", "task", "logs", "log", "command", "result", "broadcast", "inbox"):
            if keyword in parts:
                return keyword
        return "unknown"
